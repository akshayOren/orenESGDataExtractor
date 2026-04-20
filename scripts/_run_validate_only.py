"""Run just Phase 4 (validation) on all extracted tasks."""
import sys
import json
import sqlite3
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from oren_esg.config import settings
from oren_esg.logging_config import setup_logging
from esg_validator import (
    ESGValidatorPipeline, ValidationOutputWriter, ValidatorConfig, IngestionDB as ValidatorDB
)

setup_logging(log_level="INFO", log_dir=settings.log_dir)
log = logging.getLogger("revalidate")

db_path = "ingestion.db"
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
tasks = conn.execute("SELECT * FROM pdf_tasks WHERE status='extracted' ORDER BY id").fetchall()
conn.close()

print(f"Revalidating {len(tasks)} tasks...\n")

results = {"pass": 0, "review": 0, "fail": 0, "error": 0}

for task in tasks:
    task_id = task["id"]
    file_name = task["file_name"]

    val_db = ValidatorDB(db_path)
    pipeline = ESGValidatorPipeline()
    writer = ValidationOutputWriter(ValidatorConfig.OUTPUT_DIR)

    ext_dir = None
    base = Path(ValidatorConfig.EXTRACTED_DIR)
    if base.exists():
        for d in sorted(base.iterdir()):
            if d.is_dir() and d.name.startswith(f"{task_id:04d}_"):
                ext_dir = d
                break

    if not ext_dir:
        log.warning(f"  #{task_id}: no extracted folder")
        results["error"] += 1
        continue

    data_path = ext_dir / "esg_data.json"
    if not data_path.exists():
        log.warning(f"  #{task_id}: no esg_data.json")
        results["error"] += 1
        continue

    val_db.update_status(task_id, "validating")

    try:
        data = json.loads(data_path.read_text(encoding="utf-8"))
        validated, report = pipeline.validate(data)
        kpi_rows = pipeline.kpi_exporter.export(validated)
        writer.write(validated, report, kpi_rows, str(ext_dir))

        status = "validated" if report.pass_fail != "FAIL" else "review_needed"
        val_db.update_status(task_id, status)

        verdict = report.pass_fail
        results[verdict.lower()] = results.get(verdict.lower(), 0) + 1
        print(f"  #{task_id} {file_name}: {verdict} ({report.overall_score:.1f}/100)")

    except Exception as e:
        log.error(f"  #{task_id} validation failed: {e}")
        val_db.update_status(task_id, "failed", str(e))
        results["error"] += 1

print(f"\nDone: {results['pass']} PASS, {results['review']} REVIEW, {results['fail']} FAIL, {results['error']} errors")

# Final status
conn2 = sqlite3.connect(db_path)
rows = conn2.execute("SELECT status, COUNT(*) FROM pdf_tasks GROUP BY status ORDER BY status").fetchall()
print("\nFinal status:")
for r in rows:
    print(f"  {r[0]}: {r[1]}")
conn2.close()
