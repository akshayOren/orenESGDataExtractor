"""Investigate failed and error tasks."""
import sqlite3

conn = sqlite3.connect('ingestion.db')
conn.row_factory = sqlite3.Row

print("=== FAILED TASKS (extraction failures) ===")
failed = conn.execute("SELECT id, file_name, status, error_message FROM pdf_tasks WHERE status='failed' ORDER BY id").fetchall()
for r in failed:
    err = (r['error_message'] or '')[:120]
    print(f"  #{r['id']} [{r['status']}] {r['file_name']}")
    if err:
        print(f"       Error: {err}")

print(f"\nTotal failed: {len(failed)}")

print("\n=== REVIEW_NEEDED TASKS ===")
review = conn.execute("SELECT id, file_name FROM pdf_tasks WHERE status='review_needed' ORDER BY id").fetchall()
for r in review:
    print(f"  #{r['id']} {r['file_name']}")
print(f"\nTotal review_needed: {len(review)}")

print("\n=== STATUS SUMMARY ===")
rows = conn.execute("SELECT status, COUNT(*) as cnt FROM pdf_tasks GROUP BY status ORDER BY status").fetchall()
for r in rows:
    print(f"  {r['status']:<16} {r['cnt']:>4}")

# Check if extracted output folders exist for failed validation
print("\n=== EXTRACTED FOLDERS CHECK ===")
import os
ext_dir = 'extracted_output'
if os.path.exists(ext_dir):
    folders = sorted(os.listdir(ext_dir))
    print(f"  Extracted folders: {len(folders)}")
    # Check which failed tasks have extracted output
    failed_ids = [r['id'] for r in failed]
    has_output = 0
    no_output = 0
    for fid in failed_ids:
        prefix = f"{fid:04d}_"
        found = any(f.startswith(prefix) for f in folders)
        if found:
            has_output += 1
        else:
            no_output += 1
    print(f"  Failed tasks WITH extracted output: {has_output}")
    print(f"  Failed tasks WITHOUT extracted output: {no_output}")

conn.close()
