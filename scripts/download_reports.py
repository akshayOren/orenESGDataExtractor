"""
ADX ESG Report Downloader
==========================
Reads the adx_manifest table from ingestion.db and downloads pending PDF reports
into reports/uae/. After each download the manifest is updated in place so a
crashed or interrupted run can be safely resumed.

Run fetch_manifest.py first to populate the manifest.

Usage:
    python scripts/download_reports.py                  # download all pending
    python scripts/download_reports.py --retry          # also retry failed
    python scripts/download_reports.py --dry-run        # preview without downloading
    python scripts/download_reports.py --limit 5        # download at most 5 files

Workflow:
    ingestion.db (adx_manifest)  -->  download_reports.py  -->  reports/uae/*.pdf
                                                                        |
                                                            pdf_watcher.py  -->  ingestion.db (pdf_tasks)
"""

from __future__ import annotations

import hashlib
import sqlite3
import argparse
import logging
from pathlib import Path
from datetime import datetime, timezone

from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError

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
DB_PATH = Path("ingestion.db")
DOWNLOAD_DIR = Path("reports/uae")
CHUNK_SIZE = 8192
REQUEST_TIMEOUT = 60

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
    if not db_path.exists():
        raise FileNotFoundError(
            f"Database not found: {db_path}\n"
            "Run fetch_manifest.py first to create the manifest."
        )
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(CREATE_TABLE_SQL)
    conn.commit()
    return conn


def fetch_pending(conn: sqlite3.Connection, retry: bool) -> list[sqlite3.Row]:
    if retry:
        rows = conn.execute(
            "SELECT * FROM adx_manifest WHERE status IN ('pending', 'failed') ORDER BY published_date DESC"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM adx_manifest WHERE status = 'pending' ORDER BY published_date DESC"
        ).fetchall()
    return rows


def update_row(conn: sqlite3.Connection, row_id: int, **fields) -> None:
    """Update specific columns for a row and commit immediately (crash-safe)."""
    set_clause = ", ".join(f"{col} = ?" for col in fields)
    values = list(fields.values()) + [row_id]
    conn.execute(f"UPDATE adx_manifest SET {set_clause} WHERE id = ?", values)
    conn.commit()


# ---------------------------------------------------------------------------
# File helpers
# ---------------------------------------------------------------------------

def compute_sha256(file_path: Path, chunk_size: int = CHUNK_SIZE) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def download_pdf(session: requests.Session, url: str, dest_path: Path) -> None:
    """
    Stream-download a PDF to dest_path.
    Raises ValueError if the file does not start with the PDF magic bytes (%PDF).
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    resp = session.get(url, stream=True, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    with open(dest_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
            if chunk:
                f.write(chunk)

    # Validate it's actually a PDF
    with open(dest_path, "rb") as f:
        magic = f.read(4)
    if magic != b"%PDF":
        dest_path.unlink(missing_ok=True)
        raise ValueError(f"Downloaded file is not a PDF (magic bytes: {magic!r})")


# ---------------------------------------------------------------------------
# Per-row processing
# ---------------------------------------------------------------------------

def process_row(
    session: requests.Session,
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    download_dir: Path,
    dry_run: bool,
) -> str:
    """
    Download one report. Returns final status: 'downloaded', 'failed', or 'skipped'.
    Mutates the database row in place (committed immediately on success or failure).
    """
    row_id: int = row["id"]
    url: str = row["url_en"] or ""
    filename: str = row["filename"] or ""
    dest = download_dir / filename

    if not url:
        update_row(conn, row_id, status="failed", error="empty url_en")
        return "failed"

    if dest.exists() and row["status"] == "downloaded":
        print(f"  [skip]   {filename}")
        return "skipped"

    if dry_run:
        print(f"  [dry]    {filename}  <-- {url}")
        return "skipped"

    try:
        download_pdf(session, url, dest)
        update_row(
            conn,
            row_id,
            status="downloaded",
            local_path=str(dest),
            downloaded_at=datetime.now(timezone.utc).isoformat(),
            error="",
        )
        print(f"  [done]   {filename}")
        return "downloaded"

    except (RequestsError, OSError, ValueError) as exc:
        msg = f"{type(exc).__name__}: {exc}"
        update_row(conn, row_id, status="failed", error=msg)
        print(f"  [fail]   {filename}  -- {msg}")
        return "failed"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download ADX ESG sustainability reports from the manifest"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help=f"Path to SQLite database (default: {DB_PATH})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DOWNLOAD_DIR,
        help=f"Directory to save PDFs (default: {DOWNLOAD_DIR})",
    )
    parser.add_argument(
        "--retry",
        action="store_true",
        help="Also retry rows with status=failed",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be downloaded without actually downloading",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Download at most N files (0 = no limit)",
    )
    args = parser.parse_args()

    try:
        conn = get_db(args.db)
    except FileNotFoundError as exc:
        log.error("%s", exc)
        raise SystemExit(1)

    rows = fetch_pending(conn, retry=args.retry)
    if args.limit > 0:
        rows = rows[: args.limit]

    total = len(rows)
    if total == 0:
        print("Nothing to download. Run fetch_manifest.py first or use --retry.")
        conn.close()
        return

    args.output.mkdir(parents=True, exist_ok=True)
    mode = "[DRY RUN] " if args.dry_run else ""
    print(f"{mode}Downloading {total} report(s) -> {args.output}/\n")

    session = requests.Session(impersonate="chrome120")
    session.headers.update({
        "adx-gateway-apikey": "1863a94c-582b-46f9-b4f0-0d02c0cc5307",
        "channel-id": "OSS WEB",
        "Origin": "https://www.adx.ae",
        "Referer": "https://www.adx.ae/",
    })

    counts = {"downloaded": 0, "failed": 0, "skipped": 0}

    for n, row in enumerate(rows, 1):
        print(f"[{n}/{total}]", end=" ")
        result = process_row(session, conn, row, args.output, args.dry_run)
        counts[result] += 1

    conn.close()

    print(f"\n{'-' * 40}")
    print(f"Downloaded: {counts['downloaded']}")
    print(f"Failed:     {counts['failed']}")
    print(f"Skipped:    {counts['skipped']}")

    if counts["failed"]:
        print("\nRe-run with --retry to attempt failed downloads again.")


if __name__ == "__main__":
    main()
