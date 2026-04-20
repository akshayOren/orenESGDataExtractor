"""
ADX ESG Report Manifest Builder
=================================
Reads sustainability report records from a local JSON file (data/row.json)
and upserts them into the adx_manifest table in ingestion.db.

Run this before download_reports.py to populate the manifest.

Usage:
    python scripts/fetch_manifest.py
    python scripts/fetch_manifest.py --input data/row.json
    python scripts/fetch_manifest.py --db path/to/ingestion.db

Workflow:
    data/row.json  -->  fetch_manifest.py  -->  ingestion.db (adx_manifest table)
                                                         |
                                             download_reports.py  -->  reports/uae/
"""

from __future__ import annotations

import re
import json
import sqlite3
import argparse
import logging
from pathlib import Path

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
JSON_PATH = Path("data/row.json")
DB_PATH = Path("ingestion.db")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS adx_manifest (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entity          TEXT NOT NULL,
    entity_name_en  TEXT NOT NULL,
    title_en        TEXT,
    url_en          TEXT NOT NULL UNIQUE,
    published_date  TEXT,
    filename        TEXT,
    local_path      TEXT DEFAULT '',
    status          TEXT DEFAULT 'pending',
    error           TEXT DEFAULT '',
    downloaded_at   TEXT DEFAULT '',
    created_at      TEXT DEFAULT (datetime('now'))
)
"""

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_date(raw_date: str) -> str:
    """Extract YYYY-MM-DD from '2024-03-26 00:00:00.0'."""
    return raw_date.strip().split(" ")[0] if raw_date else ""


def sanitize_filename(entity_name: str, published_date: str) -> str:
    """
    Build a safe filesystem filename from entity name and date.

    'Emirates Telecom. Group Company (Etisalat Group) PJSC' + '2024-03-26'
    → 'Emirates_Telecom_Group_Company_Etisalat_Group_PJSC_2024-03-26.pdf'
    """
    clean = re.sub(r"[^\w]", "_", entity_name)
    clean = re.sub(r"_+", "_", clean)
    clean = clean.strip("_")
    clean = clean[:120]
    return f"{clean}_{published_date}.pdf"


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def load_records(json_path: Path) -> list:
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "items", "records", "results"):
            if key in data and isinstance(data[key], list):
                return data[key]
    raise ValueError(f"Unexpected JSON structure in {json_path}: expected a list or dict with a list value")


def unique_filename(conn: sqlite3.Connection, base_filename: str, seen_in_batch: set) -> str:
    """
    Return a filename that doesn't collide with existing DB rows or the current batch.
    If 'Report_2024-03-26.pdf' already exists, tries 'Report_2024-03-26_2.pdf', then _3, etc.
    """
    stem = base_filename[: -len(".pdf")]
    candidate = base_filename
    counter = 2
    while True:
        in_db = conn.execute(
            "SELECT 1 FROM adx_manifest WHERE filename = ?", (candidate,)
        ).fetchone()
        if not in_db and candidate not in seen_in_batch:
            return candidate
        candidate = f"{stem}_{counter}.pdf"
        counter += 1


def upsert_records(conn: sqlite3.Connection, records: list) -> tuple[int, int]:
    """
    Insert new records into adx_manifest. Rows with duplicate url_en are ignored.
    Filename collisions (same entity + date, different URL) are resolved with a _2, _3 suffix.
    Returns (added, skipped).
    """
    added = 0
    skipped = 0
    seen_filenames: set = set()

    for rec in records:
        url_en = rec.get("urlEn", "") or ""
        entity_name = rec.get("entityNameEn", "") or ""
        published_date = parse_date(rec.get("publishedDate", "") or "")

        if not url_en:
            log.warning("Skipping record with no urlEn: entity=%s", rec.get("entity"))
            skipped += 1
            continue

        base_filename = sanitize_filename(entity_name, published_date)
        filename = unique_filename(conn, base_filename, seen_filenames)
        seen_filenames.add(filename)

        cursor = conn.execute(
            """
            INSERT OR IGNORE INTO adx_manifest
                (entity, entity_name_en, title_en, url_en, published_date, filename)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                rec.get("entity", ""),
                entity_name,
                rec.get("titleEn", ""),
                url_en,
                published_date,
                filename,
            ),
        )
        if cursor.rowcount > 0:
            added += 1
        else:
            skipped += 1

    conn.commit()
    return added, skipped


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load ADX ESG report metadata from row.json into ingestion.db"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=JSON_PATH,
        help=f"Path to JSON file (default: {JSON_PATH})",
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help=f"Path to SQLite database (default: {DB_PATH})",
    )
    args = parser.parse_args()

    if not args.input.exists():
        log.error("Input file not found: %s", args.input)
        raise SystemExit(1)

    log.info("Loading records from %s…", args.input)
    try:
        records = load_records(args.input)
    except (json.JSONDecodeError, ValueError) as exc:
        log.error("Failed to parse %s: %s", args.input, exc)
        raise SystemExit(1)

    log.info("Loaded %d records", len(records))

    conn = get_db(args.db)
    try:
        added, skipped = upsert_records(conn, records)
    finally:
        conn.close()

    print(f"\nManifest updated in {args.db} (table: adx_manifest)")
    print(f"  Added:   {added}")
    print(f"  Skipped: {skipped}  (already in manifest or missing urlEn)")
    print(f"  Total in file: {len(records)}")


if __name__ == "__main__":
    main()
