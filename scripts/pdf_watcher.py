"""
PDF Ingestion Watcher for Sustainability Reports
=================================================
Monitors folders for new PDF files, deduplicates via SHA-256,
extracts basic metadata, and queues them for processing.

Usage:
    # Watch mode (real-time monitoring):
    python pdf_watcher.py --watch ./reports

    # Batch mode (scan and queue all unprocessed):
    python pdf_watcher.py --scan ./reports

    # Check queue status:
    python pdf_watcher.py --status

Requirements:
    pip install watchdog

Architecture:
    ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
    │ File watcher │────▶│ Deduplicator │────▶│ Metadata  │
    │ (watchdog)   │    │ (SHA-256)    │     │ tagger      │
    └─────────────┘     └──────────────┘     └──────┬──────┘
                                                    │
                                                    ▼
                                              ┌─────────────┐
                                              │ SQLite queue │
                                              │ (task table) │
                                              └─────────────┘
"""

import os
import sys
import re
import json
import hashlib
import sqlite3
import logging
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional

try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler, FileCreatedEvent

    WATCHDOG_AVAILABLE = True
except ImportError:
    WATCHDOG_AVAILABLE = False

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pdf_watcher")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
class Config:
    """Central configuration — edit these to match your setup."""

    # Database file that tracks every PDF we've seen
    DB_PATH: str = "ingestion.db"

    # Folders to watch (one per country or however you organise them)
    # Overridden by CLI args, but these are the defaults
    WATCH_DIRS: list[str] = ["./reports"]

    # Only process files matching this pattern
    PDF_GLOB: str = "*.pdf"

    # Known filename patterns for metadata extraction
    # Adjust these regexes to match YOUR naming conventions
    # Example filenames the patterns match:
    #   "Acme_Corp_2024_ESG_Report.pdf"
    #   "ACME-2024-sustainability.pdf"
    #   "2024_Acme_Annual_ESG.pdf"
    FILENAME_PATTERNS: list[re.Pattern] = [
        # Pattern 1: CompanyName_Year_*.pdf
        re.compile(
            r"^(?P<company>[A-Za-z0-9_ -]+?)[-_ ]+"
            r"(?P<year>20[1-3]\d)"
            r"[-_ ]+.*\.pdf$",
            re.IGNORECASE,
        ),
        # Pattern 2: Year_CompanyName_*.pdf
        re.compile(
            r"^(?P<year>20[1-3]\d)"
            r"[-_ ]+(?P<company>[A-Za-z0-9_ -]+?)[-_ ]+.*\.pdf$",
            re.IGNORECASE,
        ),
        # Pattern 3: just grab a year anywhere in the name
        re.compile(
            r"(?P<year>20[1-3]\d)",
            re.IGNORECASE,
        ),
    ]

    # Map top-level folder names to countries
    # e.g. reports/india/acme.pdf  →  country = "india"
    COUNTRY_FROM_FOLDER: bool = True


# ---------------------------------------------------------------------------
# Database layer  (SQLite — zero setup, portable, perfect for solo dev)
# ---------------------------------------------------------------------------
class IngestionDB:
    """Thin wrapper around SQLite for the task queue + hash registry."""

    SCHEMA = """
    CREATE TABLE IF NOT EXISTS pdf_tasks (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,

        -- File identity
        pdf_path        TEXT    NOT NULL,
        file_name       TEXT    NOT NULL,
        sha256          TEXT    NOT NULL,
        file_size_bytes INTEGER NOT NULL,

        -- Metadata (populated by the tagger)
        company         TEXT,
        country         TEXT,
        year            INTEGER,
        framework       TEXT,
        page_count      INTEGER,
        extra_meta      TEXT,           -- JSON blob for anything else

        -- Processing state
        status          TEXT    NOT NULL DEFAULT 'pending',
            -- pending → parsing → extracting → validating → done
            -- or → failed  (with error info)
        error_message   TEXT,
        version         INTEGER NOT NULL DEFAULT 1,
        replaced_by_id  INTEGER,        -- points to newer version if replaced

        -- Timestamps
        ingested_at     TEXT    NOT NULL,
        updated_at      TEXT    NOT NULL
    );

    -- Fast lookups
    CREATE INDEX IF NOT EXISTS idx_sha256  ON pdf_tasks(sha256);
    CREATE INDEX IF NOT EXISTS idx_status  ON pdf_tasks(status);
    CREATE INDEX IF NOT EXISTS idx_company_year
        ON pdf_tasks(company, year, country);
    """

    def __init__(self, db_path: str = Config.DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(self.SCHEMA)

    def hash_exists(self, sha256: str) -> Optional[sqlite3.Row]:
        """Check if we've already ingested a file with this hash."""
        return self.conn.execute(
            "SELECT * FROM pdf_tasks WHERE sha256 = ?", (sha256,)
        ).fetchone()

    def find_previous_version(
        self, company: str, year: int, country: str
    ) -> Optional[sqlite3.Row]:
        """Find an existing record for the same company+year+country."""
        return self.conn.execute(
            """SELECT * FROM pdf_tasks
               WHERE company = ? AND year = ? AND country = ?
                 AND replaced_by_id IS NULL
               ORDER BY version DESC LIMIT 1""",
            (company, year, country),
        ).fetchone()

    def insert_task(self, record: dict) -> int:
        """Insert a new PDF task and return its id."""
        now = datetime.now(timezone.utc).isoformat()
        record.setdefault("ingested_at", now)
        record.setdefault("updated_at", now)
        record.setdefault("status", "pending")
        record.setdefault("version", 1)

        cur = self.conn.execute(
            """INSERT INTO pdf_tasks
               (pdf_path, file_name, sha256, file_size_bytes,
                company, country, year, framework, page_count, extra_meta,
                status, version, ingested_at, updated_at)
               VALUES
               (:pdf_path, :file_name, :sha256, :file_size_bytes,
                :company, :country, :year, :framework, :page_count, :extra_meta,
                :status, :version, :ingested_at, :updated_at)""",
            record,
        )
        self.conn.commit()
        return cur.lastrowid

    def mark_replaced(self, old_id: int, new_id: int):
        """Mark an older version as replaced by a newer one."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE pdf_tasks SET replaced_by_id = ?, updated_at = ? WHERE id = ?",
            (new_id, now, old_id),
        )
        self.conn.commit()

    def get_status_summary(self) -> dict:
        """Return counts grouped by status."""
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM pdf_tasks GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}

    def get_pending_tasks(self, limit: int = 50) -> list[sqlite3.Row]:
        """Fetch the next batch of tasks to process."""
        return self.conn.execute(
            """SELECT * FROM pdf_tasks
               WHERE status = 'pending'
               ORDER BY ingested_at ASC
               LIMIT ?""",
            (limit,),
        ).fetchall()

    def update_status(self, task_id: int, status: str, error: str = None):
        """Update a task's processing status."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """UPDATE pdf_tasks
               SET status = ?, error_message = ?, updated_at = ?
               WHERE id = ?""",
            (status, error, now, task_id),
        )
        self.conn.commit()


# ---------------------------------------------------------------------------
# SHA-256 hashing
# ---------------------------------------------------------------------------
def compute_sha256(file_path: str, chunk_size: int = 8192) -> str:
    """Compute SHA-256 hash of a file without loading it all into memory."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


# ---------------------------------------------------------------------------
# Metadata extraction (from filename + folder structure)
# ---------------------------------------------------------------------------
def extract_metadata(file_path: str) -> dict:
    """
    Extract what we can from the filename and folder path.

    Returns a dict with keys: company, country, year, framework.
    Values are None when not detected — the LLM cover-page scanner
    in Phase 3 fills these gaps later.
    """
    path = Path(file_path)
    meta = {
        "company": None,
        "country": None,
        "year": None,
        "framework": None,
        "page_count": None,
        "extra_meta": None,
    }

    # --- Country from folder structure ---
    # e.g. ./reports/india/acme_2024.pdf → country = "india"
    if Config.COUNTRY_FROM_FOLDER:
        parts = path.parts
        # Look for the folder right after the base watch dir
        for i, part in enumerate(parts):
            if part.lower() in ("reports", "data", "pdfs") and i + 1 < len(parts):
                candidate = parts[i + 1]
                # Don't treat the filename as the country
                if candidate != path.name:
                    meta["country"] = candidate.strip().title()
                break

    # --- Company + year from filename patterns ---
    for pattern in Config.FILENAME_PATTERNS:
        match = pattern.search(path.name)
        if match:
            groups = match.groupdict()
            if "company" in groups and groups["company"]:
                # Clean up: "Acme_Corp" → "Acme Corp"
                raw = groups["company"].replace("_", " ").replace("-", " ").strip()
                meta["company"] = raw.title()
            if "year" in groups and groups["year"]:
                meta["year"] = int(groups["year"])
            break

    # --- Framework hints from filename ---
    name_lower = path.name.lower()
    framework_keywords = {
        "gri": "GRI",
        "tcfd": "TCFD",
        "csrd": "CSRD",
        "sasb": "SASB",
        "cdp": "CDP",
        "sdg": "SDG",
    }
    for keyword, framework in framework_keywords.items():
        if keyword in name_lower:
            meta["framework"] = framework
            break

    # --- Page count (quick, no heavy parsing) ---
    try:
        # Attempt a lightweight page count using pypdf if available
        from pypdf import PdfReader

        reader = PdfReader(file_path)
        meta["page_count"] = len(reader.pages)
    except Exception:
        # Not critical — will be filled during Phase 2 parsing
        pass

    return meta


# ---------------------------------------------------------------------------
# Core ingestion logic
# ---------------------------------------------------------------------------
def ingest_pdf(file_path: str, db: IngestionDB) -> Optional[int]:
    """
    Process a single PDF through the ingestion pipeline:
    1. Validate it's a real PDF
    2. Compute hash → deduplicate
    3. Extract metadata
    4. Handle versioning
    5. Insert into task queue

    Returns the task ID if queued, or None if skipped.
    """
    path = Path(file_path)

    # --- Basic validation ---
    if not path.exists():
        log.warning(f"File not found: {file_path}")
        return None

    if not path.suffix.lower() == ".pdf":
        return None

    file_size = path.stat().st_size
    if file_size < 1024:  # Less than 1KB is probably not a real report
        log.warning(f"Skipping tiny file ({file_size}B): {path.name}")
        return None

    # --- Deduplication ---
    sha256 = compute_sha256(file_path)
    existing = db.hash_exists(sha256)

    if existing:
        log.info(f"Duplicate (exact match): {path.name}  ->  skipped")
        return None

    # --- Metadata extraction ---
    meta = extract_metadata(file_path)
    log.info(
        f"Metadata: company={meta['company']}, "
        f"country={meta['country']}, year={meta['year']}, "
        f"framework={meta['framework']}, pages={meta['page_count']}"
    )

    # --- Version detection ---
    version = 1
    if meta["company"] and meta["year"] and meta["country"]:
        prev = db.find_previous_version(meta["company"], meta["year"], meta["country"])
        if prev:
            version = prev["version"] + 1
            log.info(
                f"Version update detected: {meta['company']} {meta['year']} "
                f"v{prev['version']} → v{version}"
            )

    # --- Build the record ---
    record = {
        "pdf_path": str(path.resolve()),
        "file_name": path.name,
        "sha256": sha256,
        "file_size_bytes": file_size,
        "company": meta["company"],
        "country": meta["country"],
        "year": meta["year"],
        "framework": meta["framework"],
        "page_count": meta["page_count"],
        "extra_meta": json.dumps(meta.get("extra_meta")) if meta.get("extra_meta") else None,
        "version": version,
    }

    # --- Insert ---
    task_id = db.insert_task(record)

    # --- If this is a version update, mark the old one as replaced ---
    if version > 1:
        prev = db.find_previous_version(meta["company"], meta["year"], meta["country"])
        # find_previous_version returns the latest unreplaced — which is now
        # the one we just inserted. We need the one before that.
        old = db.conn.execute(
            """SELECT id FROM pdf_tasks
               WHERE company = ? AND year = ? AND country = ?
                 AND id != ? AND replaced_by_id IS NULL
               ORDER BY version DESC LIMIT 1""",
            (meta["company"], meta["year"], meta["country"], task_id),
        ).fetchone()
        if old:
            db.mark_replaced(old["id"], task_id)

    log.info(f"Queued: {path.name}  (id={task_id}, v{version}, status=pending)")
    return task_id


# ---------------------------------------------------------------------------
# Watchdog event handler
# ---------------------------------------------------------------------------
if WATCHDOG_AVAILABLE:

    class PDFEventHandler(FileSystemEventHandler):
        """React to new PDF files appearing in watched directories."""

        def __init__(self, db: IngestionDB):
            super().__init__()
            self.db = db

        def on_created(self, event):
            if event.is_directory:
                return
            if not event.src_path.lower().endswith(".pdf"):
                return

            log.info(f"New file detected: {event.src_path}")

            # Small delay — some systems write files incrementally
            import time
            time.sleep(1)

            ingest_pdf(event.src_path, self.db)

        def on_moved(self, event):
            """Handle files moved INTO the watched directory."""
            if event.is_directory:
                return
            if not event.dest_path.lower().endswith(".pdf"):
                return

            log.info(f"File moved in: {event.dest_path}")
            ingest_pdf(event.dest_path, self.db)


# ---------------------------------------------------------------------------
# Batch scanner (for initial load or catch-up)
# ---------------------------------------------------------------------------
def batch_scan(directories: list[str], db: IngestionDB) -> dict:
    """
    Recursively scan directories for PDFs and ingest any new ones.
    Returns summary stats.
    """
    stats = {"scanned": 0, "queued": 0, "skipped_duplicate": 0, "skipped_other": 0}

    for dir_path in directories:
        base = Path(dir_path)
        if not base.exists():
            log.warning(f"Directory not found: {dir_path}")
            continue

        log.info(f"Scanning: {base.resolve()}")

        for pdf_path in sorted(base.rglob("*.pdf")):
            stats["scanned"] += 1
            result = ingest_pdf(str(pdf_path), db)

            if result is not None:
                stats["queued"] += 1
            else:
                # Distinguish duplicates from other skips
                sha = compute_sha256(str(pdf_path))
                if db.hash_exists(sha):
                    stats["skipped_duplicate"] += 1
                else:
                    stats["skipped_other"] += 1

            # Progress logging every 100 files
            if stats["scanned"] % 100 == 0:
                log.info(f"Progress: {stats['scanned']} scanned, {stats['queued']} queued")

    return stats


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="PDF Ingestion Watcher for Sustainability Reports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pdf_watcher.py --watch ./reports/country_a ./reports/country_b
  python pdf_watcher.py --scan ./reports
  python pdf_watcher.py --status
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--watch",
        nargs="+",
        metavar="DIR",
        help="Watch directories for new PDFs (real-time)",
    )
    group.add_argument(
        "--scan",
        nargs="+",
        metavar="DIR",
        help="Batch scan directories and queue all unprocessed PDFs",
    )
    group.add_argument(
        "--status",
        action="store_true",
        help="Show current queue status",
    )

    parser.add_argument(
        "--db",
        default=Config.DB_PATH,
        help=f"SQLite database path (default: {Config.DB_PATH})",
    )

    args = parser.parse_args()
    db = IngestionDB(args.db)

    # --- Status mode ---
    if args.status:
        summary = db.get_status_summary()
        total = sum(summary.values())
        print(f"\n{'─' * 40}")
        print(f"  Ingestion Queue Status")
        print(f"{'-' * 40}")
        for status, count in sorted(summary.items()):
            bar = "#" * min(count, 40)
            print(f"  {status:<14} {count:>5}  {bar}")
        print(f"{'─' * 40}")
        print(f"  {'total':<14} {total:>5}")
        print()

        # Show next 5 pending
        pending = db.get_pending_tasks(limit=5)
        if pending:
            print("  Next up:")
            for task in pending:
                label = task["file_name"][:40]
                print(f"    #{task['id']:>4}  {label}")
            print()
        return

    # --- Batch scan mode ---
    if args.scan:
        log.info("Starting batch scan...")
        stats = batch_scan(args.scan, db)
        print(f"\nBatch scan complete:")
        print(f"  Scanned:            {stats['scanned']}")
        print(f"  Queued (new):       {stats['queued']}")
        print(f"  Skipped (duplicate):{stats['skipped_duplicate']}")
        print(f"  Skipped (other):    {stats['skipped_other']}")
        return

    # --- Watch mode ---
    if args.watch:
        if not WATCHDOG_AVAILABLE:
            print("Error: watchdog is required for --watch mode")
            print("Install it:  pip install watchdog")
            sys.exit(1)

        # First, do an initial scan to catch anything already there
        log.info("Running initial scan before watching...")
        stats = batch_scan(args.watch, db)
        log.info(
            f"Initial scan: {stats['queued']} new PDFs queued "
            f"from {stats['scanned']} files"
        )

        # Now set up real-time watching
        handler = PDFEventHandler(db)
        observer = Observer()

        for dir_path in args.watch:
            resolved = str(Path(dir_path).resolve())
            observer.schedule(handler, resolved, recursive=True)
            log.info(f"Watching: {resolved}")

        observer.start()
        print("\nWatching for new PDFs... (Ctrl+C to stop)\n")

        try:
            import time
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            log.info("Stopping watcher...")
            observer.stop()
        observer.join()


if __name__ == "__main__":
    main()