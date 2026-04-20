"""
Parallel pipeline orchestrator — runs all 4 phases with concurrency.

Usage:
    python scripts/run_pipeline_parallel.py --scan-dir ./data/reports/uae
    python scripts/run_pipeline_parallel.py --scan-dir ./data/reports/uae --parse-workers 2 --extract-workers 3
    python scripts/run_pipeline_parallel.py --scan-dir ./data/reports/uae --simulate
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

# Make `src/` and `scripts/` importable
_project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root / "scripts"))

from oren_esg.config import settings  # noqa: E402
from oren_esg.logging_config import setup_logging  # noqa: E402

# Phase 1
from pdf_watcher import IngestionDB as WatcherDB, batch_scan  # noqa: E402

# Phase 2 imports (used inside worker init to avoid pickling issues)
from pdf_parser import (  # noqa: E402
    DoclingParser,
    IngestionDB as ParserDB,
    OutputWriter as ParsedOutputWriter,
    ParserConfig,
    process_task as parse_task,
)

# Phase 3
from esg_extractor import (  # noqa: E402
    ESGExtractor,
    ExtractionOutputWriter,
    ExtractorConfig,
    IngestionDB as ExtractorDB,
    LLMClient,
)

# Phase 4
from esg_validator import (  # noqa: E402
    ESGValidatorPipeline,
    FlatKPIExporter,
    IngestionDB as ValidatorDB,
    ValidationOutputWriter,
    ValidatorConfig,
)

log = logging.getLogger("pipeline_parallel")

# Sonnet pricing ($ per million tokens)
SONNET_INPUT_COST = 3.0
SONNET_OUTPUT_COST = 15.0


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def init_wal_mode(db_path: str):
    """Enable WAL mode and busy timeout for concurrent access."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.close()


class PipelineDB:
    """Path-filtered queries for the orchestrator."""

    def __init__(self, db_path: str = "ingestion.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def _prefix(self, scan_dir: str) -> str:
        return str(Path(scan_dir).resolve())

    def get_pending(self, scan_dir: str, limit: int = 0) -> list[sqlite3.Row]:
        prefix = self._prefix(scan_dir)
        sql = "SELECT * FROM pdf_tasks WHERE status='pending' AND pdf_path LIKE ? ORDER BY ingested_at ASC"
        params: list = [prefix + "%"]
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        return self.conn.execute(sql, params).fetchall()

    def get_parsed(self, scan_dir: str) -> list[sqlite3.Row]:
        prefix = self._prefix(scan_dir)
        return self.conn.execute(
            "SELECT * FROM pdf_tasks WHERE status='parsed' AND pdf_path LIKE ? ORDER BY ingested_at ASC",
            (prefix + "%",),
        ).fetchall()

    def get_extracted(self, scan_dir: str) -> list[sqlite3.Row]:
        prefix = self._prefix(scan_dir)
        return self.conn.execute(
            "SELECT * FROM pdf_tasks WHERE status='extracted' AND pdf_path LIKE ? ORDER BY ingested_at ASC",
            (prefix + "%",),
        ).fetchall()

    def get_status_summary(self) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM pdf_tasks GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}

    def close(self):
        self.conn.close()


# ---------------------------------------------------------------------------
# Phase 1 — Scan (unchanged)
# ---------------------------------------------------------------------------
def run_scan(scan_dir: str, db_path: str) -> dict:
    log.info(f"\n{'=' * 55}")
    log.info("  Phase 1 -- SCAN")
    log.info(f"{'=' * 55}")
    watcher_db = WatcherDB(db_path)
    stats = batch_scan([scan_dir], watcher_db)
    log.info(f"  Scanned: {stats['scanned']}, New: {stats['queued']}, Dups: {stats['skipped_duplicate']}")
    return stats


# ---------------------------------------------------------------------------
# Phase 2 — Sequential Parse (Docling doesn't work in child processes on Windows)
# ---------------------------------------------------------------------------
class _DictRow(dict):
    """Make a plain dict behave like sqlite3.Row for subscript access."""
    def __getitem__(self, key):
        return super().__getitem__(key)


def run_parse(tasks: list, db_path: str, workers: int = 1) -> dict:
    log.info(f"\n{'=' * 55}")
    log.info("  Phase 2 -- PARSE (sequential)")
    log.info(f"{'=' * 55}")

    if not tasks:
        log.info("  No pending tasks to parse.")
        return {"success": 0, "failed": 0}

    log.info(f"  Processing {len(tasks)} task(s)")

    task_dicts = [_DictRow(dict(t)) for t in tasks]
    results = {"success": 0, "failed": 0}

    parser_db = ParserDB(db_path)
    docling_parser = DoclingParser()
    writer = ParsedOutputWriter(ParserConfig.OUTPUT_DIR)

    for i, td in enumerate(task_dicts, 1):
        ok = parse_task(td, docling_parser, writer, parser_db)
        results["success" if ok else "failed"] += 1
        log.info(f"  Parse {'OK' if ok else 'FAIL'}: task #{td['id']}  ({i}/{len(tasks)})")

    log.info(f"  Parse done: {results['success']} ok, {results['failed']} failed")
    return results


# ---------------------------------------------------------------------------
# Phase 3 — Parallel Extract (ThreadPoolExecutor)
# ---------------------------------------------------------------------------
# Shared cost tracker (thread-safe)
_cost_lock = threading.Lock()
_total_input_tokens = 0
_total_output_tokens = 0
_total_extract_calls = 0


def _estimate_cost() -> float:
    """Estimate total cost in USD based on accumulated tokens."""
    return (_total_input_tokens / 1_000_000 * SONNET_INPUT_COST +
            _total_output_tokens / 1_000_000 * SONNET_OUTPUT_COST)


def _extract_one_pdf(
    task_dict: dict,
    db_path: str,
    simulate: bool,
    model: str,
    output_dir: str,
    parsed_base: str,
    task_num: int,
    total_tasks: int,
) -> tuple[int, bool]:
    """Extract ESG data from one parsed PDF. Runs in a thread."""
    global _total_input_tokens, _total_output_tokens, _total_extract_calls

    task_id = task_dict["id"]
    file_name = task_dict["file_name"]

    # Each thread gets its own instances
    ext_db = ExtractorDB(db_path)
    llm = LLMClient(model=model, simulate=simulate)
    extractor = ESGExtractor(llm)
    writer = ExtractionOutputWriter(output_dir)

    # Find parsed output folder
    parsed_dir = None
    base = Path(parsed_base)
    if base.exists():
        for d in sorted(base.iterdir()):
            if d.is_dir() and d.name.startswith(f"{task_id:04d}_"):
                parsed_dir = d
                break

    if not parsed_dir:
        log.warning(f"  Task #{task_id}: parsed folder not found, skipping")
        return task_id, False

    log.info(f"  [{task_num}/{total_tasks}] Extracting: {file_name}")
    ext_db.update_status(task_id, "extracting")

    try:
        data = extractor.extract_from_folder(str(parsed_dir), task_id)
        writer.write(data, str(parsed_dir))
        ext_db.update_status(task_id, "extracted")

        # Accumulate token usage
        usage = llm.get_usage_summary()
        with _cost_lock:
            _total_input_tokens += usage.get("total_input_tokens", 0)
            _total_output_tokens += usage.get("total_output_tokens", 0)
            _total_extract_calls += usage.get("total_calls", 0)
            cost = _estimate_cost()

        log.info(
            f"  [{task_num}/{total_tasks}] Done: {file_name} "
            f"(calls: {usage.get('total_calls', 0)}, "
            f"cost so far: ${cost:.2f})"
        )
        return task_id, True

    except Exception as e:
        log.error(f"  Task #{task_id} extraction failed: {e}")
        ext_db.update_status(task_id, "failed", str(e))
        return task_id, False


def run_extract_parallel(
    tasks: list, db_path: str, simulate: bool, model: str, workers: int = 3
) -> dict:
    global _total_input_tokens, _total_output_tokens, _total_extract_calls
    _total_input_tokens = 0
    _total_output_tokens = 0
    _total_extract_calls = 0

    log.info(f"\n{'=' * 55}")
    log.info(f"  Phase 3 -- EXTRACT ({workers} workers)")
    log.info(f"{'=' * 55}")

    if not tasks:
        log.info("  No parsed tasks to extract.")
        return {"success": 0, "failed": 0, "usage": {}}

    log.info(f"  Processing {len(tasks)} task(s)  (simulate={simulate}, model={model})")

    task_dicts = [dict(t) for t in tasks]
    results = {"success": 0, "failed": 0}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for i, td in enumerate(task_dicts, 1):
            fut = pool.submit(
                _extract_one_pdf,
                td, db_path, simulate, model,
                ExtractorConfig.OUTPUT_DIR,
                ExtractorConfig.PARSED_DIR,
                i, len(task_dicts),
            )
            futures[fut] = td

        for future in as_completed(futures):
            try:
                task_id, ok = future.result()
                results["success" if ok else "failed"] += 1
            except Exception as e:
                results["failed"] += 1
                td = futures[future]
                log.error(f"  Extract crashed for task #{td['id']}: {e}")

    cost = _estimate_cost()
    results["usage"] = {
        "total_calls": _total_extract_calls,
        "total_input_tokens": _total_input_tokens,
        "total_output_tokens": _total_output_tokens,
        "estimated_cost_usd": round(cost, 2),
    }
    log.info(
        f"  Extract done: {results['success']} ok, {results['failed']} failed  "
        f"(calls: {_total_extract_calls}, cost: ${cost:.2f})"
    )
    return results


# ---------------------------------------------------------------------------
# Phase 4 — Parallel Validate (ThreadPoolExecutor)
# ---------------------------------------------------------------------------
def _validate_one(task_dict: dict, db_path: str) -> tuple[int, str]:
    """Validate one extracted PDF. Runs in a thread."""
    task_id = task_dict["id"]
    file_name = task_dict["file_name"]

    val_db = ValidatorDB(db_path)
    pipeline = ESGValidatorPipeline()
    writer = ValidationOutputWriter(ValidatorConfig.OUTPUT_DIR)

    # Find extracted folder
    ext_dir = None
    base = Path(ValidatorConfig.EXTRACTED_DIR)
    if base.exists():
        for d in sorted(base.iterdir()):
            if d.is_dir() and d.name.startswith(f"{task_id:04d}_"):
                ext_dir = d
                break

    if not ext_dir:
        log.warning(f"  Task #{task_id}: extracted folder not found")
        return task_id, "error"

    data_path = ext_dir / "esg_data.json"
    if not data_path.exists():
        log.warning(f"  Task #{task_id}: esg_data.json not found")
        return task_id, "error"

    val_db.update_status(task_id, "validating")

    try:
        data = json.loads(data_path.read_text(encoding="utf-8"))
        validated, report = pipeline.validate(data)
        kpi_rows = pipeline.kpi_exporter.export(validated)
        writer.write(validated, report, kpi_rows, str(ext_dir))

        status = "validated" if report.pass_fail != "FAIL" else "review_needed"
        val_db.update_status(task_id, status)
        log.info(f"  Validated: {file_name} -> {report.pass_fail} ({report.overall_score:.1f}/100)")
        return task_id, report.pass_fail.lower()

    except Exception as e:
        log.error(f"  Task #{task_id} validation failed: {e}")
        val_db.update_status(task_id, "failed", str(e))
        return task_id, "error"


def run_validate_parallel(tasks: list, db_path: str, workers: int = 8) -> dict:
    log.info(f"\n{'=' * 55}")
    log.info(f"  Phase 4 -- VALIDATE ({workers} workers)")
    log.info(f"{'=' * 55}")

    if not tasks:
        log.info("  No extracted tasks to validate.")
        return {"pass": 0, "review": 0, "fail": 0, "error": 0}

    log.info(f"  Processing {len(tasks)} task(s)")

    task_dicts = [dict(t) for t in tasks]
    results = {"pass": 0, "review": 0, "fail": 0, "error": 0}

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_validate_one, td, db_path): td for td in task_dicts}
        for future in as_completed(futures):
            try:
                task_id, verdict = future.result()
                results[verdict] = results.get(verdict, 0) + 1
            except Exception as e:
                results["error"] += 1
                td = futures[future]
                log.error(f"  Validate crashed for task #{td['id']}: {e}")

    log.info(
        f"  Validate done: {results['pass']} PASS, {results['review']} REVIEW, "
        f"{results['fail']} FAIL, {results['error']} errors"
    )
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Parallel ESG pipeline: scan -> parse -> extract -> validate",
    )
    parser.add_argument("--scan-dir", default=None, help="Directory to scan for PDFs")
    parser.add_argument("--limit", type=int, default=0, help="Max PDFs to process (0 = all)")
    parser.add_argument("--simulate", action="store_true", help="Simulated LLM (no API)")
    parser.add_argument("--model", default=ExtractorConfig.MODEL, help=f"Claude model (default: {ExtractorConfig.MODEL})")
    parser.add_argument("--db", default="ingestion.db", help="SQLite DB path")
    parser.add_argument("--parse-workers", type=int, default=2, help="Parallel Docling parsers (default: 2)")
    parser.add_argument("--extract-workers", type=int, default=3, help="Concurrent extract threads (default: 3)")
    parser.add_argument("--validate-workers", type=int, default=8, help="Concurrent validate threads (default: 8)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")
    args = parser.parse_args()

    # Logging
    log_level = "DEBUG" if args.verbose else settings.log_level
    setup_logging(log_level=log_level, log_dir=settings.log_dir)

    # Resolve scan dir
    scan_dir = args.scan_dir or os.getenv("REPORT_SCAN_DIR")
    if not scan_dir:
        parser.error("--scan-dir required (or set REPORT_SCAN_DIR)")
    if not Path(scan_dir).exists():
        parser.error(f"Scan directory does not exist: {scan_dir}")

    # API key check
    if not args.simulate:
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key or api_key.startswith("your_"):
            log.error("ANTHROPIC_API_KEY not set. Use --simulate or set it in .env")
            sys.exit(1)

    # Disable Claude Vision fallback — use Gemini only for images
    os.environ["DISABLE_CLAUDE_VISION"] = "1"

    db_path = args.db

    # Enable WAL mode for concurrent DB access
    init_wal_mode(db_path)

    log.info(f"{'=' * 55}")
    log.info(f"  ESG Pipeline (Parallel)")
    log.info(f"{'=' * 55}")
    log.info(f"  Scan dir:        {Path(scan_dir).resolve()}")
    log.info(f"  Limit:           {args.limit or 'all'}")
    log.info(f"  Simulate:        {args.simulate}")
    log.info(f"  Model:           {args.model}")
    log.info(f"  Parse workers:   {args.parse_workers}")
    log.info(f"  Extract workers: {args.extract_workers}")
    log.info(f"  Validate workers:{args.validate_workers}")
    log.info(f"  Claude Vision:   DISABLED (Gemini only)")

    pipeline_start = time.time()
    pdb = PipelineDB(db_path)

    # Phase 1 — Scan
    scan_stats = run_scan(scan_dir, db_path)

    # Phase 2 — Parse (sequential — Docling doesn't work in child processes on Windows)
    pending = pdb.get_pending(scan_dir, limit=args.limit)
    parse_results = run_parse(pending, db_path)

    # Refresh DB connection to see Phase 2 results
    pdb.close()
    pdb = PipelineDB(db_path)

    # Phase 3 — Extract
    parsed = pdb.get_parsed(scan_dir)
    extract_results = run_extract_parallel(parsed, db_path, args.simulate, args.model, workers=args.extract_workers)

    # Refresh DB connection to see Phase 3 results
    pdb.close()
    pdb = PipelineDB(db_path)

    # Phase 4 — Validate
    extracted = pdb.get_extracted(scan_dir)
    validate_results = run_validate_parallel(extracted, db_path, workers=args.validate_workers)

    # Grand summary
    total_time = time.time() - pipeline_start

    print(f"\n{'=' * 55}")
    print(f"  Pipeline Complete")
    print(f"{'=' * 55}")
    print(f"  Scan:       {scan_stats['scanned']} scanned, {scan_stats['queued']} new")
    print(f"  Parse:      {parse_results['success']} ok, {parse_results['failed']} failed")
    print(f"  Extract:    {extract_results['success']} ok, {extract_results['failed']} failed")
    print(
        f"  Validate:   {validate_results['pass']} PASS, "
        f"{validate_results['review']} REVIEW, "
        f"{validate_results['fail']} FAIL, "
        f"{validate_results['error']} errors"
    )
    print(f"  Total time: {total_time:.1f}s ({total_time/60:.1f} min)")

    usage = extract_results.get("usage", {})
    if usage.get("total_calls"):
        print(f"\n  LLM Usage:")
        print(f"    Calls:  {usage['total_calls']}")
        if usage.get("total_input_tokens"):
            print(f"    Tokens: {usage['total_input_tokens']:,} in / {usage['total_output_tokens']:,} out")
        print(f"    Cost:   ${usage.get('estimated_cost_usd', 0):.2f} (estimated)")

    # Queue status
    pdb.close()
    pdb = PipelineDB(db_path)
    summary = pdb.get_status_summary()
    if summary:
        print(f"\n  Queue Status:")
        for status, count in sorted(summary.items()):
            print(f"    {status:<14} {count:>5}")
    print()
    pdb.close()


if __name__ == "__main__":
    main()
