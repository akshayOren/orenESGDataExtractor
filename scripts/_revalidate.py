"""Reset failed tasks that have extracted output back to 'extracted' for revalidation."""
import sqlite3
import os
from datetime import datetime, timezone

conn = sqlite3.connect('ingestion.db')
conn.row_factory = sqlite3.Row
now = datetime.now(timezone.utc).isoformat()

# Find failed tasks that have extracted output folders
failed = conn.execute("SELECT id, file_name FROM pdf_tasks WHERE status='failed'").fetchall()
ext_dir = 'extracted_output'
folders = set(os.listdir(ext_dir)) if os.path.exists(ext_dir) else set()

reset_ids = []
for r in failed:
    prefix = f"{r['id']:04d}_"
    if any(f.startswith(prefix) for f in folders):
        reset_ids.append(r['id'])

if reset_ids:
    placeholders = ','.join('?' for _ in reset_ids)
    conn.execute(
        f"UPDATE pdf_tasks SET status='extracted', error_message=NULL, updated_at=? WHERE id IN ({placeholders})",
        [now] + reset_ids
    )
    conn.commit()
    print(f"Reset {len(reset_ids)} tasks from 'failed' to 'extracted'")

# Also reset review_needed back to extracted for revalidation
conn.execute("UPDATE pdf_tasks SET status='extracted', updated_at=? WHERE status='review_needed'", (now,))
conn.commit()

rows = conn.execute("SELECT status, COUNT(*) FROM pdf_tasks GROUP BY status ORDER BY status").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")
conn.close()
