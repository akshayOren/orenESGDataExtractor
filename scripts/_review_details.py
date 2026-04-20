"""Show review_needed tasks with validation details."""
import sqlite3
import json
from pathlib import Path

conn = sqlite3.connect('ingestion.db')
conn.row_factory = sqlite3.Row

tasks = conn.execute(
    "SELECT id, file_name FROM pdf_tasks WHERE status='review_needed' ORDER BY id"
).fetchall()

print(f"=== REVIEW NEEDED: {len(tasks)} PDFs ===\n")

for task in tasks:
    tid = task['id']
    fname = task['file_name']

    # Find validated output
    val_dir = None
    base = Path('validated_output')
    if base.exists():
        for d in sorted(base.iterdir()):
            if d.is_dir() and d.name.startswith(f"{tid:04d}_"):
                val_dir = d
                break

    report_path = val_dir / "validation_report.json" if val_dir else None

    print(f"#{tid} {fname}")

    if report_path and report_path.exists():
        report = json.loads(report_path.read_text(encoding="utf-8"))
        score = report.get("overall_score", "?")
        completeness = report.get("completeness_score", "?")
        accuracy = report.get("accuracy_score", "?")
        consistency = report.get("consistency_score", "?")
        verdict = report.get("pass_fail", "?")
        total_fields = report.get("total_fields", 0)
        flagged = report.get("flagged_fields", 0)

        print(f"  Score: {score}/100  (completeness={completeness}, accuracy={accuracy}, consistency={consistency})")
        print(f"  Verdict: {verdict}  |  Fields: {total_fields}  |  Flagged: {flagged}")

        # Show flags
        flags = report.get("flags", [])
        errors = [f for f in flags if f.get("severity") == "error"]
        warnings = [f for f in flags if f.get("severity") == "warning"]

        if errors:
            print(f"  ERRORS ({len(errors)}):")
            for f in errors[:5]:
                print(f"    - [{f.get('check_name')}] {f.get('field_path')}: {f.get('message', '')[:120]}")
            if len(errors) > 5:
                print(f"    ... and {len(errors)-5} more")

        if warnings:
            print(f"  WARNINGS ({len(warnings)}):")
            for f in warnings[:3]:
                print(f"    - [{f.get('check_name')}] {f.get('field_path')}: {f.get('message', '')[:120]}")
            if len(warnings) > 3:
                print(f"    ... and {len(warnings)-3} more")
    else:
        print(f"  No validation report found")

    print()

conn.close()
