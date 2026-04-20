"""
Full pipeline orchestrator.

Usage:
    python scripts/run_pipeline.py --limit 5
    python scripts/run_pipeline.py --limit 5 --scan-dir ./data/reports/uae
    python scripts/run_pipeline.py --limit 5 --simulate
    python scripts/run_pipeline.py --limit 5 --simulate -v
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Make `src/` and `scripts/` importable
_project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_project_root / "src"))
sys.path.insert(0, str(_project_root / "scripts"))

from oren_esg.config import settings  # noqa: E402  — triggers load_dotenv()
from oren_esg.logging_config import setup_logging  # noqa: E402

# Phase 1 — scan / ingest
from pdf_watcher import IngestionDB as WatcherDB, batch_scan  # noqa: E402

# Phase 2 — parse
from pdf_parser import (  # noqa: E402
    DoclingParser,
    IngestionDB as ParserDB,
    OutputWriter as ParsedOutputWriter,
    ParserConfig,
    process_task as parse_task,
)

# Phase 3 — extract
from esg_extractor import (  # noqa: E402
    ESGExtractor,
    ExtractionOutputWriter,
    ExtractorConfig,
    IngestionDB as ExtractorDB,
    LLMClient,
)

# Phase 4 — validate
from esg_validator import (  # noqa: E402
    ESGValidatorPipeline,
    FlatKPIExporter,
    IngestionDB as ValidatorDB,
    ValidationOutputWriter,
    ValidatorConfig,
)

log = logging.getLogger("run_pipeline")


# ---------------------------------------------------------------------------
# Path-filtered DB helper (orchestrator-only)
# ---------------------------------------------------------------------------
class PipelineDB:
    """Thin wrapper providing path-filtered queries for the orchestrator."""

    def __init__(self, db_path: str = "ingestion.db"):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def _resolve_prefix(self, scan_dir: str) -> str:
        return str(Path(scan_dir).resolve())

    def get_pending_by_path(self, scan_dir: str, limit: int = 0) -> list[sqlite3.Row]:
        prefix = self._resolve_prefix(scan_dir)
        sql = (
            "SELECT * FROM pdf_tasks "
            "WHERE status = 'pending' AND pdf_path LIKE ? "
            "ORDER BY ingested_at ASC"
        )
        params: list = [prefix + "%"]
        if limit > 0:
            sql += " LIMIT ?"
            params.append(limit)
        return self.conn.execute(sql, params).fetchall()

    def get_parsed_by_path(self, scan_dir: str) -> list[sqlite3.Row]:
        prefix = self._resolve_prefix(scan_dir)
        return self.conn.execute(
            "SELECT * FROM pdf_tasks "
            "WHERE status = 'parsed' AND pdf_path LIKE ? "
            "ORDER BY ingested_at ASC",
            (prefix + "%",),
        ).fetchall()

    def get_extracted_by_path(self, scan_dir: str) -> list[sqlite3.Row]:
        prefix = self._resolve_prefix(scan_dir)
        return self.conn.execute(
            "SELECT * FROM pdf_tasks "
            "WHERE status = 'extracted' AND pdf_path LIKE ? "
            "ORDER BY ingested_at ASC",
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
# Phase runners
# ---------------------------------------------------------------------------
def run_scan(scan_dir: str, db_path: str) -> dict:
    """Phase 1: Scan directory for new PDFs and ingest into pdf_tasks."""
    log.info(f"\n{'═' * 55}")
    log.info("  Phase 1 — SCAN")
    log.info(f"{'═' * 55}")
    log.info(f"  Directory: {Path(scan_dir).resolve()}")

    watcher_db = WatcherDB(db_path)
    stats = batch_scan([scan_dir], watcher_db)

    log.info(
        f"  Scanned: {stats['scanned']}, "
        f"New queued: {stats['queued']}, "
        f"Duplicates skipped: {stats['skipped_duplicate']}"
    )
    return stats


def run_parse(tasks: list, db_path: str) -> dict:
    """Phase 2: Parse pending PDFs with Docling."""
    log.info(f"\n{'═' * 55}")
    log.info("  Phase 2 — PARSE")
    log.info(f"{'═' * 55}")

    if not tasks:
        log.info("  No pending tasks to parse.")
        return {"success": 0, "failed": 0}

    log.info(f"  Processing {len(tasks)} task(s)")

    parser_db = ParserDB(db_path)
    docling_parser = DoclingParser()
    writer = ParsedOutputWriter(ParserConfig.OUTPUT_DIR)

    results = {"success": 0, "failed": 0}
    for task in tasks:
        ok = parse_task(task, docling_parser, writer, parser_db)
        results["success" if ok else "failed"] += 1

    log.info(f"  Parse done: {results['success']} ok, {results['failed']} failed")
    return results


def run_extract(
    tasks: list, db_path: str, simulate: bool, model: str
) -> dict:
    """Phase 3: Extract structured ESG data using LLM."""
    log.info(f"\n{'═' * 55}")
    log.info("  Phase 3 — EXTRACT")
    log.info(f"{'═' * 55}")

    if not tasks:
        log.info("  No parsed tasks to extract.")
        return {"success": 0, "failed": 0, "usage": {}}

    log.info(f"  Processing {len(tasks)} task(s)  (simulate={simulate})")

    extractor_db = ExtractorDB(db_path)
    llm = LLMClient(model=model, simulate=simulate)
    extractor = ESGExtractor(llm)
    writer = ExtractionOutputWriter(ExtractorConfig.OUTPUT_DIR)

    results = {"success": 0, "failed": 0}
    parsed_base = Path(ExtractorConfig.PARSED_DIR)

    for task in tasks:
        task_id = task["id"]
        file_name = task["file_name"]

        # Find the parsed output folder
        parsed_dir = None
        if parsed_base.exists():
            for d in sorted(parsed_base.iterdir()):
                if d.is_dir() and d.name.startswith(f"{task_id:04d}_"):
                    parsed_dir = d
                    break

        if not parsed_dir:
            log.warning(f"  Task #{task_id}: parsed output folder not found, skipping")
            results["failed"] += 1
            continue

        log.info(f"  Extracting task #{task_id}: {file_name}")
        extractor_db.update_status(task_id, "extracting")

        try:
            data = extractor.extract_from_folder(str(parsed_dir), task_id)
            writer.write(data, str(parsed_dir))
            extractor_db.update_status(task_id, "extracted")
            results["success"] += 1
        except Exception as e:
            log.error(f"  Task #{task_id} extraction failed: {e}")
            extractor_db.update_status(task_id, "failed", str(e))
            results["failed"] += 1

    usage = llm.get_usage_summary()
    results["usage"] = usage
    log.info(
        f"  Extract done: {results['success']} ok, {results['failed']} failed"
        f"  (LLM calls: {usage['total_calls']})"
    )
    return results


def run_validate(tasks: list, db_path: str) -> dict:
    """Phase 4: Validate and normalize extracted ESG data."""
    log.info(f"\n{'═' * 55}")
    log.info("  Phase 4 — VALIDATE")
    log.info(f"{'═' * 55}")

    if not tasks:
        log.info("  No extracted tasks to validate.")
        return {"pass": 0, "review": 0, "fail": 0, "error": 0}

    log.info(f"  Processing {len(tasks)} task(s)")

    validator_db = ValidatorDB(db_path)
    pipeline = ESGValidatorPipeline()
    writer = ValidationOutputWriter(ValidatorConfig.OUTPUT_DIR)

    results = {"pass": 0, "review": 0, "fail": 0, "error": 0}
    extracted_base = Path(ValidatorConfig.EXTRACTED_DIR)

    for task in tasks:
        task_id = task["id"]
        file_name = task["file_name"]

        # Find the extracted output folder
        ext_dir = None
        if extracted_base.exists():
            for d in sorted(extracted_base.iterdir()):
                if d.is_dir() and d.name.startswith(f"{task_id:04d}_"):
                    ext_dir = d
                    break

        if not ext_dir:
            log.warning(f"  Task #{task_id}: extracted output folder not found")
            results["error"] += 1
            continue

        data_path = ext_dir / "esg_data.json"
        if not data_path.exists():
            log.warning(f"  Task #{task_id}: esg_data.json not found in {ext_dir}")
            results["error"] += 1
            continue

        log.info(f"  Validating task #{task_id}: {file_name}")
        validator_db.update_status(task_id, "validating")

        try:
            data = json.loads(data_path.read_text(encoding="utf-8"))
            validated, report = pipeline.validate(data)
            kpi_rows = pipeline.kpi_exporter.export(validated)
            writer.write(validated, report, kpi_rows, str(ext_dir))

            status = "validated" if report.pass_fail != "FAIL" else "review_needed"
            validator_db.update_status(task_id, status)

            verdict = report.pass_fail
            results[verdict.lower()] = results.get(verdict.lower(), 0) + 1
            log.info(f"    Score: {report.overall_score:.1f}/100 -> {verdict}")

        except Exception as e:
            log.error(f"  Task #{task_id} validation failed: {e}")
            validator_db.update_status(task_id, "failed", str(e))
            results["error"] += 1

    log.info(
        f"  Validate done: {results['pass']} PASS, "
        f"{results['review']} REVIEW, "
        f"{results['fail']} FAIL, "
        f"{results['error']} errors"
    )
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the full ESG extraction pipeline: scan → parse → extract → validate",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python scripts/run_pipeline.py --limit 5
  python scripts/run_pipeline.py --scan-dir ./data/reports/uae --limit 10
  python scripts/run_pipeline.py --limit 5 --simulate
  python scripts/run_pipeline.py --limit 5 --simulate -v
""",
    )
    parser.add_argument(
        "--scan-dir",
        default=None,
        help="Directory to scan for PDFs (overrides REPORT_SCAN_DIR env var)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max PDFs to parse in this run (0 = all pending, default: 0)",
    )
    parser.add_argument(
        "--simulate",
        action="store_true",
        help="Use simulated LLM extraction (no API key needed, lower quality)",
    )
    parser.add_argument(
        "--model",
        default=ExtractorConfig.MODEL,
        help=f"Claude model for extraction (default: {ExtractorConfig.MODEL})",
    )
    parser.add_argument(
        "--db",
        default="ingestion.db",
        help="Path to SQLite database (default: ingestion.db)",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")

    args = parser.parse_args()

    # --- Logging ---
    log_level = "DEBUG" if args.verbose else settings.log_level
    setup_logging(log_level=log_level, log_dir=settings.log_dir)

    # --- Resolve scan directory ---
    scan_dir = args.scan_dir or os.getenv("REPORT_SCAN_DIR")
    if not scan_dir:
        parser.error(
            "--scan-dir is required (or set REPORT_SCAN_DIR in your .env file)"
        )

    scan_path = Path(scan_dir)
    if not scan_path.exists():
        parser.error(f"Scan directory does not exist: {scan_dir}")

    # --- Fail fast: check API key if not simulating ---
    if not args.simulate:
        api_key = os.getenv("ANTHROPIC_API_KEY", "")
        if not api_key or api_key.startswith("your_"):
            log.error(
                "ANTHROPIC_API_KEY is not set. "
                "Set it in .env or use --simulate for testing."
            )
            sys.exit(1)

    db_path = args.db

    log.info(f"{'═' * 55}")
    log.info(f"  ESG Pipeline — Starting")
    log.info(f"{'═' * 55}")
    log.info(f"  Scan dir:  {scan_path.resolve()}")
    log.info(f"  Limit:     {args.limit or 'all pending'}")
    log.info(f"  Simulate:  {args.simulate}")
    log.info(f"  Model:     {args.model}")
    log.info(f"  DB:        {db_path}")

    pipeline_start = time.time()
    pipeline_db = PipelineDB(db_path)

    # ---- Phase 1: SCAN ----
    scan_stats = run_scan(scan_dir, db_path)

    # ---- Phase 2: PARSE ----
    pending = pipeline_db.get_pending_by_path(scan_dir, limit=args.limit)
    parse_results = run_parse(pending, db_path)

    # ---- Phase 3: EXTRACT ----
    parsed = pipeline_db.get_parsed_by_path(scan_dir)
    extract_results = run_extract(parsed, db_path, args.simulate, args.model)

    # ---- Phase 4: VALIDATE ----
    extracted = pipeline_db.get_extracted_by_path(scan_dir)
    validate_results = run_validate(extracted, db_path)

    # ---- Grand Summary ----
    total_time = time.time() - pipeline_start

    print(f"\n{'═' * 55}")
    print(f"  Pipeline Complete")
    print(f"{'═' * 55}")
    print(
        f"  Scan:       {scan_stats['scanned']} scanned, "
        f"{scan_stats['queued']} new queued"
    )
    print(
        f"  Parse:      {parse_results['success']} parsed, "
        f"{parse_results['failed']} failed"
    )
    print(
        f"  Extract:    {extract_results['success']} extracted, "
        f"{extract_results['failed']} failed"
    )
    print(
        f"  Validate:   {validate_results['pass']} PASS, "
        f"{validate_results['review']} REVIEW, "
        f"{validate_results['fail']} FAIL, "
        f"{validate_results['error']} errors"
    )
    print(f"  Total time: {total_time:.1f}s")

    usage = extract_results.get("usage", {})
    if usage.get("total_calls"):
        print(f"  LLM calls:  {usage['total_calls']}")
        if usage.get("total_input_tokens"):
            print(
                f"  Tokens:     {usage['total_input_tokens']:,} in / "
                f"{usage['total_output_tokens']:,} out"
            )
    print()

    # Show remaining queue status
    summary = pipeline_db.get_status_summary()
    if summary:
        print(f"  Queue Status:")
        for status, count in sorted(summary.items()):
            print(f"    {status:<14} {count:>5}")
        print()

    pipeline_db.close()


if __name__ == "__main__":
    main()
