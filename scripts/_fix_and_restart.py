"""Mark the stuck Arabic PDF as failed and reset it to skip."""
import sqlite3
from datetime import datetime, timezone

conn = sqlite3.connect('ingestion.db')
now = datetime.now(timezone.utc).isoformat()

# Mark #392 (Arabic PDF that crashed Docling) as failed
conn.execute(
    "UPDATE pdf_tasks SET status='failed', error_message='Arabic PDF - Docling crash', updated_at=? WHERE id=392",
    (now,)
)
conn.commit()

# Verify
rows = conn.execute("SELECT status, COUNT(*) FROM pdf_tasks GROUP BY status").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")

pending = conn.execute("SELECT id, file_name FROM pdf_tasks WHERE status='pending' ORDER BY id").fetchall()
print(f"\nReady to parse: {len(pending)} PDFs")
for r in pending:
    print(f"  #{r[0]}: {r[1]}")

parsed = conn.execute("SELECT COUNT(*) FROM pdf_tasks WHERE status='parsed'").fetchone()[0]
print(f"Ready to extract: {parsed} PDFs")
conn.close()
