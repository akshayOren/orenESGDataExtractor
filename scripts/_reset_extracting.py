import sqlite3
from datetime import datetime, timezone
conn = sqlite3.connect('ingestion.db')
now = datetime.now(timezone.utc).isoformat()
# Reset any "extracting" tasks back to "parsed" so they get picked up again
result = conn.execute("UPDATE pdf_tasks SET status='parsed', error_message=NULL, updated_at=? WHERE status='extracting'", (now,))
conn.commit()
print(f"Reset {result.rowcount} tasks from 'extracting' back to 'parsed'")
rows = conn.execute("SELECT status, COUNT(*) FROM pdf_tasks GROUP BY status").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")
conn.close()
