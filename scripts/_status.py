import sqlite3
conn = sqlite3.connect('ingestion.db')
rows = conn.execute("SELECT status, COUNT(*) FROM pdf_tasks GROUP BY status").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]}")
print()
pending = conn.execute("SELECT id, file_name FROM pdf_tasks WHERE status IN ('pending','parsing') ORDER BY id").fetchall()
if pending:
    print("Remaining:")
    for r in pending:
        print(f"  #{r[0]}: {r[1]}")
conn.close()
