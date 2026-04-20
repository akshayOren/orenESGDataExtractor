"""
Clean all pipeline outputs for a fresh run.

Usage:
    python scripts/clean_pipeline.py --yes
    python scripts/clean_pipeline.py --dry-run
"""

import argparse
import shutil
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def clean(dry_run: bool = False):
    removed = []
    kept = []

    # 1. Delete all versioned output directories
    for pattern in ("parsed_output*", "extracted_output*", "validated_output*"):
        for d in sorted(PROJECT_ROOT.glob(pattern)):
            if d.is_dir():
                if dry_run:
                    print(f"  [DRY] Would remove: {d.name}/")
                else:
                    shutil.rmtree(d, ignore_errors=True)
                    print(f"  Removed: {d.name}/")
                removed.append(d.name)

    # 2. Recreate canonical empty dirs
    for name in ("parsed_output", "extracted_output", "validated_output"):
        d = PROJECT_ROOT / name
        if not dry_run:
            d.mkdir(exist_ok=True)

    # 3. Reset DB
    db_path = PROJECT_ROOT / "ingestion.db"
    if db_path.exists():
        if dry_run:
            conn = sqlite3.connect(str(db_path))
            count = conn.execute("SELECT COUNT(*) FROM pdf_tasks").fetchone()[0]
            print(f"  [DRY] Would delete {count} rows from pdf_tasks")
            conn.close()
        else:
            conn = sqlite3.connect(str(db_path))
            count = conn.execute("SELECT COUNT(*) FROM pdf_tasks").fetchone()[0]
            conn.execute("DELETE FROM pdf_tasks")
            conn.commit()
            conn.execute("VACUUM")
            conn.close()
            print(f"  DB: deleted {count} rows from pdf_tasks")

    # 4. Truncate logs
    log_file = PROJECT_ROOT / "logs" / "pipeline.log"
    if log_file.exists():
        if dry_run:
            print(f"  [DRY] Would truncate {log_file.name}")
        else:
            log_file.write_text("")
            print(f"  Truncated {log_file.name}")

    print(f"\nDone. Removed {len(removed)} directories.")


def main():
    parser = argparse.ArgumentParser(description="Clean all pipeline outputs for a fresh run")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--dry-run", action="store_true", help="Preview without deleting")
    args = parser.parse_args()

    print("=== Pipeline Cleanup ===\n")

    if args.dry_run:
        clean(dry_run=True)
        return

    if not args.yes:
        resp = input("This will delete ALL parsed/extracted/validated outputs and reset the DB.\nContinue? [y/N] ")
        if resp.lower() != "y":
            print("Aborted.")
            return

    clean(dry_run=False)


if __name__ == "__main__":
    main()
