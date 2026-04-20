"""
Reset pdf_tasks status for given task IDs.

Usage:
    python scripts/reset_status.py --ids 2 3 4 5 --to parsed
    python scripts/reset_status.py --ids 2 3 --from validated --to parsed
    python scripts/reset_status.py --ids 2 3 4 5 --to pending --dry-run
"""

import argparse
import sqlite3
import sys
from datetime import datetime, timezone

DB_PATH = "ingestion.db"

VALID_STATUSES = [
    "pending", "parsing", "parsed",
    "extracting", "extracted",
    "validating", "validated",
    "review_needed", "failed",
]


def reset_status(
    db_path: str,
    task_ids: list[int],
    to_status: str,
    from_status: str | None = None,
    dry_run: bool = False,
) -> list[dict]:
    """Change status of given task IDs. Returns list of affected rows."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    placeholders = ",".join("?" for _ in task_ids)
    sql = f"SELECT id, file_name, status FROM pdf_tasks WHERE id IN ({placeholders})"
    rows = conn.execute(sql, task_ids).fetchall()

    if not rows:
        print("No tasks found for the given IDs.")
        conn.close()
        return []

    # Filter by from_status if specified
    to_update = []
    skipped = []
    for r in rows:
        if from_status and r["status"] != from_status:
            skipped.append(dict(r))
        else:
            to_update.append(dict(r))

    if skipped:
        print(f"Skipping {len(skipped)} task(s) not in '{from_status}' status:")
        for t in skipped:
            print(f"  #{t['id']}  {t['file_name']}  (status={t['status']})")

    if not to_update:
        print("No matching tasks to update.")
        conn.close()
        return []

    print(f"\n{'DRY RUN -- ' if dry_run else ''}Updating {len(to_update)} task(s) -> '{to_status}':")
    for t in to_update:
        print(f"  #{t['id']}  {t['file_name']}  {t['status']} -> {to_status}")

    if not dry_run:
        now = datetime.now(timezone.utc).isoformat()
        ids_to_update = [t["id"] for t in to_update]
        placeholders = ",".join("?" for _ in ids_to_update)
        conn.execute(
            f"UPDATE pdf_tasks SET status = ?, error_message = NULL, updated_at = ? "
            f"WHERE id IN ({placeholders})",
            [to_status, now] + ids_to_update,
        )
        conn.commit()
        print("\nDone.")

    conn.close()
    return to_update


def main():
    parser = argparse.ArgumentParser(
        description="Reset pdf_tasks status for given task IDs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  python scripts/reset_status.py --ids 2 3 4 5 --to parsed
  python scripts/reset_status.py --ids 2 3 --from validated --to parsed
  python scripts/reset_status.py --ids 2 3 4 5 --to pending --dry-run
""",
    )
    parser.add_argument("--ids", type=int, nargs="+", required=True, help="Task IDs to update")
    parser.add_argument("--to", required=True, choices=VALID_STATUSES, dest="to_status", help="Target status")
    parser.add_argument("--from", choices=VALID_STATUSES, dest="from_status", default=None, help="Only update tasks currently in this status")
    parser.add_argument("--db", default=DB_PATH, help=f"SQLite DB path (default: {DB_PATH})")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without applying")

    args = parser.parse_args()
    reset_status(args.db, args.ids, args.to_status, args.from_status, args.dry_run)


if __name__ == "__main__":
    main()
