"""Bursa Malaysia Annual Report Downloader.

Scrapes Bursa Malaysia's announcement search API for `Annual Report` category
filings in a given date window, then follows each announcement to the disclosure
iframe to locate and download the attached PDF(s).

PDFs land under:
    data/reports/malaysia/<Company_Name>/<Company_Name>_<YYYY-MM-DD>_<title>.pdf

One announcement can contain multiple attachments (Annual Report Part 1, Part 2,
CG Report, etc.) -- all are saved side-by-side under the company folder.

Usage:
    # Preview what would be downloaded (no files written):
    python scripts/download_malaysia_reports.py --dry-run

    # Download everything in the last ~3 years, default window:
    python scripts/download_malaysia_reports.py

    # Cap to first N announcements (handy for a smoke test):
    python scripts/download_malaysia_reports.py --limit 5

    # Custom date window (YYYY-MM-DD):
    python scripts/download_malaysia_reports.py --from 2025-01-01 --to 2026-04-20

    # Only keep reports whose title contains one of these years:
    python scripts/download_malaysia_reports.py --report-years 2024 2025 2026
"""
from __future__ import annotations

import argparse
import html
import logging
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, NamedTuple

from curl_cffi import requests
from curl_cffi.requests.errors import RequestsError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("bursa")

BASE = "https://www.bursamalaysia.com"
DISCLOSURE = "https://disclosure.bursamalaysia.com"
SEARCH_API = f"{BASE}/api/v1/announcements/search"
IFRAME_URL = f"{DISCLOSURE}/FileAccess/viewHtml"

# Bursa's "Annual Report" category value from the filter dropdown.
CATEGORY_ANNUAL_REPORT = "AR,ARCO"

DEFAULT_OUT_DIR = Path("data/reports/malaysia")
DEFAULT_FROM = "2024-01-01"  # wide enough to catch FY2023+ annual reports
DEFAULT_PER_PAGE = 50
REQUEST_TIMEOUT = 60
CHUNK = 16384


class Announcement(NamedTuple):
    ann_id: str
    date: str              # YYYY-MM-DD
    stock_code: str
    company: str           # cleaned human name (upper / as-is)
    title: str


class Attachment(NamedTuple):
    url: str
    filename: str          # original filename as shown on the page


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    })
    return s


def _get(session: requests.Session, url: str, **kw) -> requests.Response:
    kw.setdefault("impersonate", "chrome120")
    kw.setdefault("timeout", REQUEST_TIMEOUT)
    return session.get(url, **kw)


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

_DATE_CELL_RE = re.compile(r">\s*(\d{1,2})\s+([A-Za-z]{3,})\s+(\d{4})\s*<")
_COMPANY_CELL_RE = re.compile(
    r"stock_code=(\d+)[^>]*>([^<]+)</a>", re.IGNORECASE
)
_TITLE_CELL_RE = re.compile(
    r"ann_id=(\d+)[^>]*>([^<]+)</a>", re.IGNORECASE
)
_ATTACHMENT_RE = re.compile(
    r"<a\s+href='([^']+download\?[^']+)'>\s*([^<]+?\.pdf)\s*</a>",
    re.IGNORECASE,
)

MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}


def parse_date_cell(raw: str) -> str:
    m = _DATE_CELL_RE.search(raw)
    if not m:
        return ""
    day, mon_name, year = m.groups()
    mon = MONTHS.get(mon_name[:3].lower())
    if not mon:
        return ""
    return f"{year}-{mon:02d}-{int(day):02d}"


def parse_row(row: list) -> Announcement | None:
    # row format: [n, date_html, company_html, title_html]
    if len(row) < 4:
        return None
    date_str = parse_date_cell(row[1])
    cm = _COMPANY_CELL_RE.search(row[2])
    tm = _TITLE_CELL_RE.search(row[3])
    if not (cm and tm):
        return None
    stock_code, company = cm.group(1), html.unescape(cm.group(2).strip())
    ann_id, title = tm.group(1), html.unescape(tm.group(2).strip())
    return Announcement(ann_id, date_str, stock_code, company, title)


def parse_attachments(iframe_html: str) -> list[Attachment]:
    out: list[Attachment] = []
    for m in _ATTACHMENT_RE.finditer(iframe_html):
        href, fname = m.group(1), html.unescape(m.group(2).strip())
        url = href if href.startswith("http") else DISCLOSURE + href
        out.append(Attachment(url=url, filename=fname))
    return out


# ---------------------------------------------------------------------------
# Filename sanitisation
# ---------------------------------------------------------------------------

_BAD_CHARS = re.compile(r"[^A-Za-z0-9._\- ]+")
_WS = re.compile(r"\s+")


def safe_component(s: str, max_len: int = 80) -> str:
    s = _BAD_CHARS.sub("_", s)
    s = _WS.sub("_", s).strip("._ ")
    return s[:max_len] or "untitled"


def target_path(out_dir: Path, a: Announcement, att: Attachment) -> Path:
    company_dir = out_dir / safe_component(a.company)
    stem = Path(att.filename).stem
    ext = Path(att.filename).suffix or ".pdf"
    filename = f"{safe_component(a.company)}_{a.date}_{safe_component(stem)}{ext}"
    return company_dir / filename


# ---------------------------------------------------------------------------
# Search + detail
# ---------------------------------------------------------------------------

def iter_announcements(
    session: requests.Session,
    *,
    date_from: str,
    date_to: str,
    per_page: int,
    sleep_s: float,
) -> Iterator[Announcement]:
    page = 1
    while True:
        params = {
            "ann_type": "company",
            "company": "",
            "keyword": "",
            "dt_ht": date_from,
            "dt_lt": date_to,
            "cat": CATEGORY_ANNUAL_REPORT,
            "per_page": per_page,
            "page": page,
        }
        log.info("search page=%d...", page)
        r = _get(
            session, SEARCH_API, params=params,
            headers={
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{BASE}/market_information/announcements/company_announcement",
            },
        )
        r.raise_for_status()
        payload = r.json()
        rows = payload.get("data") or []
        total = payload.get("recordsFiltered") or payload.get("recordsTotal") or 0
        log.info("  page %d: %d rows (total filtered=%s)", page, len(rows), total)
        if not rows:
            return
        for raw in rows:
            ann = parse_row(raw)
            if ann:
                yield ann
        if page * per_page >= int(total):
            return
        page += 1
        if sleep_s > 0:
            time.sleep(sleep_s)


def fetch_attachments(
    session: requests.Session, ann: Announcement
) -> list[Attachment]:
    url = f"{IFRAME_URL}?e={ann.ann_id}"
    referer = (
        f"{BASE}/market_information/announcements/company_announcement/"
        f"announcement_details?ann_id={ann.ann_id}"
    )
    r = _get(session, url, headers={"Referer": referer})
    r.raise_for_status()
    return parse_attachments(r.text)


def download_pdf(
    session: requests.Session, att: Attachment, dest: Path
) -> tuple[bool, int]:
    """Download a PDF to `dest`. Returns (downloaded, bytes). Skips if exists."""
    if dest.exists() and dest.stat().st_size > 0:
        return False, dest.stat().st_size
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    r = _get(session, att.url, stream=True)
    if r.status_code != 200:
        raise RequestsError(f"HTTP {r.status_code} for {att.url}")
    size = 0
    with tmp.open("wb") as f:
        for chunk in r.iter_content(chunk_size=CHUNK):
            if chunk:
                f.write(chunk)
                size += len(chunk)
    if size == 0:
        tmp.unlink(missing_ok=True)
        raise RequestsError(f"empty response for {att.url}")
    tmp.replace(dest)
    return True, size


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def valid_date(s: str) -> str:
    datetime.strptime(s, "%Y-%m-%d")
    return s


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    p.add_argument("--from", dest="date_from", default=DEFAULT_FROM,
                   type=valid_date, help="Announcement date >= YYYY-MM-DD")
    p.add_argument("--to", dest="date_to",
                   default=date.today().isoformat(),
                   type=valid_date, help="Announcement date <= YYYY-MM-DD")
    p.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR,
                   help="Where to save PDFs")
    p.add_argument("--per-page", type=int, default=DEFAULT_PER_PAGE)
    p.add_argument("--limit", type=int, default=0,
                   help="Stop after processing this many announcements (0 = no cap)")
    p.add_argument("--sleep", type=float, default=0.5,
                   help="Seconds to pause between HTTP calls")
    p.add_argument("--report-years", nargs="*", default=None,
                   help="Only keep announcements whose title contains one of these years, e.g. 2024 2025 2026")
    p.add_argument("--dry-run", action="store_true",
                   help="List what would be downloaded, don't write files")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    session = make_session()

    year_filter = set(args.report_years) if args.report_years else None

    log.info(
        "Bursa Malaysia scraper: %s .. %s  cat=AnnualReport  out=%s  dry_run=%s",
        args.date_from, args.date_to, args.out_dir, args.dry_run,
    )

    seen = 0
    kept = 0
    dl_files = 0
    dl_bytes = 0
    skipped = 0
    failed = 0

    try:
        for ann in iter_announcements(
            session,
            date_from=args.date_from,
            date_to=args.date_to,
            per_page=args.per_page,
            sleep_s=args.sleep,
        ):
            seen += 1
            if year_filter and not any(y in ann.title for y in year_filter):
                continue
            kept += 1
            log.info(
                "[%d] %s  %s  ann_id=%s  %s",
                kept, ann.date, ann.company, ann.ann_id, ann.title,
            )
            try:
                atts = fetch_attachments(session, ann)
            except Exception as e:
                failed += 1
                log.error("  fetch_attachments failed: %s", e)
                continue
            if not atts:
                log.warning("  no PDF attachments found on detail page")
                continue
            for att in atts:
                dest = target_path(args.out_dir, ann, att)
                if args.dry_run:
                    log.info("  would download: %s  ->  %s", att.filename, dest)
                    continue
                try:
                    did_dl, size = download_pdf(session, att, dest)
                except Exception as e:
                    failed += 1
                    log.error("  download failed [%s]: %s", att.filename, e)
                    continue
                if did_dl:
                    dl_files += 1
                    dl_bytes += size
                    log.info("  saved %s (%.1f KB)", dest.name, size / 1024)
                else:
                    skipped += 1
                    log.info("  skip (exists) %s", dest.name)
                if args.sleep > 0:
                    time.sleep(args.sleep)
            if args.limit and kept >= args.limit:
                log.info("hit --limit, stopping")
                break
    except KeyboardInterrupt:
        log.warning("interrupted by user")

    log.info("=" * 60)
    log.info("done. seen=%d kept=%d downloaded=%d skipped=%d failed=%d bytes=%.1f MB",
             seen, kept, dl_files, skipped, failed, dl_bytes / 1_000_000)
    return 0 if failed == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
