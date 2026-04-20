#!/usr/bin/env python3
"""
export_all_db_csv.py
────────────────────
Combine all db_ready_output into a single CSV with company details.

Usage:
    python scripts/export_all_db_csv.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_READY_DIR = ROOT / "db_ready_output"
VALIDATED_DIR = ROOT / "validated_output"
OUTPUT_CSV = ROOT / "data" / "all_db_records.csv"

COLUMNS = [
    # Company details
    "company_name",
    "country",
    "industry_sector",
    "reporting_year",
    "start_date",
    "end_date",
    "report_title",
    "frameworks_used",
    # Task reference
    "task_folder",
    "source_pdf",
    # Data point
    "data_point_id",
    "extractor_identifier",
    "esg_category",
    "kpi_name",
    "db_description",
    # Values
    "response",
    "unit_found",
    "prev_year_value",
    "prev_year_unit",
    "confidence",
    "page_reference",
    "notes",
]


def load_company_meta(task_folder: str) -> dict:
    """Load company metadata from validated esg_validated.json."""
    val_path = VALIDATED_DIR / task_folder / "esg_validated.json"
    if not val_path.exists():
        return {}
    with open(val_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    meta = data.get("report_metadata", {})
    period = meta.get("reporting_period") or {}
    year = meta.get("reporting_year")
    frameworks = meta.get("frameworks_used", [])
    ext_meta = data.get("extraction_metadata") or {}
    return {
        "company_name": meta.get("company_name", ""),
        "country": meta.get("country", ""),
        "industry_sector": meta.get("industry_sector", ""),
        "reporting_year": year or "",
        "start_date": period.get("start_date", f"{year}-01-01" if year else ""),
        "end_date": period.get("end_date", f"{year}-12-31" if year else ""),
        "report_title": meta.get("report_title", ""),
        "frameworks_used": ", ".join(frameworks) if frameworks else "",
        "source_pdf": ext_meta.get("source_pdf", ""),
    }


def main():
    if not DB_READY_DIR.exists():
        print(f"Error: {DB_READY_DIR} not found")
        return

    all_rows = []
    folders = sorted(f for f in DB_READY_DIR.iterdir() if f.is_dir())

    for folder in folders:
        records_path = folder / "db_records.json"
        if not records_path.exists():
            continue

        with open(records_path, "r", encoding="utf-8") as f:
            records = json.load(f)

        company = load_company_meta(folder.name)

        for dp_id, entry in records.items():
            response = entry.get("response", "")
            if not response:
                continue  # skip empty entries

            row = {
                # Company
                "company_name": company.get("company_name", ""),
                "country": company.get("country", ""),
                "industry_sector": company.get("industry_sector", ""),
                "reporting_year": company.get("reporting_year", ""),
                "start_date": company.get("start_date", ""),
                "end_date": company.get("end_date", ""),
                "report_title": company.get("report_title", ""),
                "frameworks_used": company.get("frameworks_used", ""),
                # Task
                "task_folder": folder.name,
                "source_pdf": company.get("source_pdf", ""),
                # Data point
                "data_point_id": entry.get("data_point_id", dp_id),
                "extractor_identifier": entry.get("extractor_identifier", ""),
                "esg_category": entry.get("esg_category", ""),
                "kpi_name": entry.get("kpi_name", ""),
                "db_description": entry.get("db_description", ""),
                # Values
                "response": response,
                "unit_found": entry.get("unit_found", ""),
                "prev_year_value": entry.get("prev_year_value", ""),
                "prev_year_unit": entry.get("prev_year_unit", ""),
                "confidence": entry.get("confidence", ""),
                "page_reference": entry.get("page_reference", ""),
                "notes": entry.get("notes", ""),
            }
            all_rows.append(row)

    # Write CSV
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(all_rows)

    # Summary
    companies = set(r["company_name"] for r in all_rows)
    print(f"Exported {len(all_rows)} records from {len(folders)} tasks ({len(companies)} companies)")
    print(f"Output: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
