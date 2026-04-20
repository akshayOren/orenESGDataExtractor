#!/usr/bin/env python3
"""
generate_db_json.py
───────────────────
Read validated ESG output and the data-point mapping template,
resolve each extractor_identifier against the nested JSON,
and produce a DB-ready JSON file per company/PDF — analogous
to the benchmarking project's finaljson output.

Usage:
    python scripts/generate_db_json.py --task 0298_A_D_N_H_Catering_PLC_2025-03-17
    python scripts/generate_db_json.py --all
"""

from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

# ── Directories ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
VALIDATED_DIR = ROOT / "validated_output"
MAPPING_PATH = ROOT / "data" / "esg_data_pointmapping.json"
OUTPUT_DIR = ROOT / "db_ready_output"


# ─────────────────────────────────────────────────────────────────────────────
# Path resolver
# ─────────────────────────────────────────────────────────────────────────────

# Regex to match array accessors: [0], [*], [key=value]
_ARRAY_RE = re.compile(r"\[([^\]]+)\]")


def _tokenize_path(path: str) -> list[dict]:
    """
    Tokenize a dot-notation path into segments.
    Examples:
        "a.b.c"                       -> [{name:"a"}, {name:"b"}, {name:"c"}]
        "a.b[0]"                      -> [{name:"a"}, {name:"b", index:0}]
        "a.b[*]"                      -> [{name:"a"}, {name:"b", wildcard:True}]
        "a.b[category_number=1]"      -> [{name:"a"}, {name:"b", filter_key:"category_number", filter_val:"1"}]
    """
    tokens = []
    # Split on dots but not inside brackets
    # We'll handle bracket notation after splitting
    raw_parts = re.split(r"\.(?![^\[]*\])", path)

    for part in raw_parts:
        bracket_match = _ARRAY_RE.search(part)
        if bracket_match:
            name = part[: bracket_match.start()]
            accessor = bracket_match.group(1)

            if accessor == "*":
                tokens.append({"name": name, "wildcard": True})
            elif "=" in accessor:
                fk, fv = accessor.split("=", 1)
                tokens.append({"name": name, "filter_key": fk, "filter_val": fv})
            else:
                try:
                    tokens.append({"name": name, "index": int(accessor)})
                except ValueError:
                    tokens.append({"name": name})
        else:
            tokens.append({"name": part})

    return tokens


def resolve_path(data: dict, path: str):
    """
    Resolve a dot-notation path (with optional array accessors) against
    a nested dict/list structure.

    Returns the resolved value, or None if not found.
    For wildcard [*] paths, returns the list itself.
    """
    tokens = _tokenize_path(path)
    current = data

    for token in tokens:
        if current is None:
            return None

        name = token.get("name", "")
        if name and isinstance(current, dict):
            current = current.get(name)
        elif name:
            return None

        if current is None:
            return None

        # Array index
        if "index" in token:
            if isinstance(current, list) and token["index"] < len(current):
                current = current[token["index"]]
            else:
                return None

        # Array wildcard — return the list
        elif token.get("wildcard"):
            if isinstance(current, list):
                return current
            return None

        # Array filter — find matching item
        elif "filter_key" in token:
            if not isinstance(current, list):
                return None
            fk = token["filter_key"]
            fv = token["filter_val"]
            found = None
            for item in current:
                if isinstance(item, dict):
                    item_val = item.get(fk)
                    if str(item_val) == fv:
                        found = item
                        break
            current = found

    return current


# ─────────────────────────────────────────────────────────────────────────────
# Value extraction
# ─────────────────────────────────────────────────────────────────────────────

def _is_metric_value(obj) -> bool:
    """Check if an object is a metric_value (has value + unit keys)."""
    return isinstance(obj, dict) and "value" in obj


def extract_response(resolved, entry: dict) -> dict:
    """
    Given a resolved object from the JSON, extract response fields.
    Returns a dict with: response, unit_found, prev_year_value, prev_year_unit,
    confidence, page_reference.
    """
    result = {
        "response": "",
        "unit_found": entry.get("default_unit", ""),
        "prev_year_value": "",
        "prev_year_unit": "",
        "confidence": None,
        "page_reference": None,
    }

    if resolved is None:
        return result

    # metric_value object (has "value" key)
    if _is_metric_value(resolved):
        val = resolved.get("value")
        result["response"] = str(val) if val is not None else ""
        result["unit_found"] = resolved.get("unit", entry.get("default_unit", ""))
        result["confidence"] = resolved.get("confidence")
        result["page_reference"] = resolved.get("page_reference")

        pyv = resolved.get("prior_year_value")
        if pyv is not None:
            result["prev_year_value"] = str(pyv)
            result["prev_year_unit"] = resolved.get("unit", "")

        return result

    # Plain scalar (int, float, bool, str)
    if isinstance(resolved, (int, float)):
        result["response"] = str(resolved)
        return result

    if isinstance(resolved, bool):
        result["response"] = str(resolved).lower()
        return result

    if isinstance(resolved, str):
        result["response"] = resolved
        return result

    # Dict that looks like an intensity object {value, unit, denominator}
    if isinstance(resolved, dict) and "value" in resolved:
        val = resolved.get("value")
        result["response"] = str(val) if val is not None else ""
        result["unit_found"] = resolved.get("unit", "")
        return result

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Array expansion
# ─────────────────────────────────────────────────────────────────────────────

def expand_array_entry(data: dict, entry: dict) -> list[dict]:
    """
    For wildcard [*] extractor_identifiers, resolve the array and
    produce one output entry per item.
    """
    path = entry["extractor_identifier"]
    # Get base path (without [*])
    base_path = path.replace("[*]", "")
    arr = resolve_path(data, base_path)

    if not isinstance(arr, list) or len(arr) == 0:
        return []

    label_field = entry.get("array_label_field", "")
    results = []

    for i, item in enumerate(arr):
        expanded = copy.deepcopy(entry)
        expanded["sequence"] = i

        # Build a descriptive label
        label = ""
        if label_field and isinstance(item, dict):
            label = str(item.get(label_field, ""))

        # Extract value from the array item
        # Array items can be: {category_number, category_name, value: metric_value}
        # or: {type/gas/source, value: metric_value}
        value_obj = item.get("value") if isinstance(item, dict) else item

        if isinstance(item, dict) and _is_metric_value(item):
            # Item itself is a metric_value
            value_obj = item
        elif isinstance(item, dict) and "value" in item and _is_metric_value(item["value"]):
            # Item has a nested value that is metric_value
            value_obj = item["value"]
        elif isinstance(item, dict) and "value" in item:
            # Item has a plain value
            value_obj = item

        resp = extract_response(value_obj, expanded)
        expanded.update(resp)
        expanded["kpi_name"] = f"{entry['kpi_name'].replace(' (all items)', '')} [{label}]" if label else f"{entry['kpi_name']} [{i}]"
        expanded["extractor_identifier"] = f"{base_path}[{i}]"
        results.append(expanded)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main generator
# ─────────────────────────────────────────────────────────────────────────────

def generate_db_json(validated_json_path: Path, mapping_path: Path) -> tuple[dict, dict]:
    """
    Generate DB-ready JSON from a validated ESG JSON and the mapping template.

    Returns: (db_records, summary)
    """
    with open(validated_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    with open(mapping_path, "r", encoding="utf-8") as f:
        mapping = json.load(f)

    # Extract report metadata
    meta = data.get("report_metadata", {})
    company = meta.get("company_name", "Unknown")
    year = meta.get("reporting_year")
    period = meta.get("reporting_period") or {}
    start_date = period.get("start_date", f"{year}-01-01" if year else "")
    end_date = period.get("end_date", f"{year}-12-31" if year else "")

    db_records = {}
    fields_mapped = 0
    fields_empty = 0
    confidence_sum = 0.0
    confidence_count = 0

    for dp_id, entry in mapping.items():
        path = entry["extractor_identifier"]

        # Handle array wildcards
        if entry.get("is_array_wildcard") or "[*]" in path:
            expanded = expand_array_entry(data, entry)
            for exp_entry in expanded:
                seq = exp_entry.get("sequence", 0)
                seq_key = f"{dp_id}_seq{seq}"
                db_records[seq_key] = exp_entry
                if exp_entry.get("response"):
                    fields_mapped += 1
                else:
                    fields_empty += 1
                if exp_entry.get("confidence") is not None:
                    confidence_sum += exp_entry["confidence"]
                    confidence_count += 1
            continue

        # Handle filter-based paths (e.g., categories[category_number=1])
        # and simple paths
        resolved = resolve_path(data, path)
        resp = extract_response(resolved, entry)

        record = copy.deepcopy(entry)
        record.update(resp)

        db_records[dp_id] = record

        if record["response"]:
            fields_mapped += 1
        else:
            fields_empty += 1

        if record.get("confidence") is not None:
            confidence_sum += record["confidence"]
            confidence_count += 1

    summary = {
        "source_file": validated_json_path.name,
        "company": company,
        "year": year,
        "start_date": start_date,
        "end_date": end_date,
        "total_mapping_entries": len(mapping),
        "db_records_generated": len(db_records),
        "fields_mapped": fields_mapped,
        "fields_empty": fields_empty,
        "fill_rate_pct": round(100 * fields_mapped / max(fields_mapped + fields_empty, 1), 1),
        "avg_confidence": round(confidence_sum / max(confidence_count, 1), 3),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    return db_records, summary


def process_task(task_folder: Path, mapping_path: Path, output_dir: Path):
    """Process a single task folder."""
    validated_json = task_folder / "esg_validated.json"
    if not validated_json.exists():
        print(f"  SKIP {task_folder.name}: no esg_validated.json")
        return False

    db_records, summary = generate_db_json(validated_json, mapping_path)

    # Write output
    out_folder = output_dir / task_folder.name
    out_folder.mkdir(parents=True, exist_ok=True)

    with open(out_folder / "db_records.json", "w", encoding="utf-8") as f:
        json.dump(db_records, f, indent=2, ensure_ascii=False)
    with open(out_folder / "db_records_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"  OK {task_folder.name}: {summary['fields_mapped']} mapped, "
          f"{summary['fields_empty']} empty ({summary['fill_rate_pct']}% fill)")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Generate DB-ready JSON from validated ESG output"
    )
    parser.add_argument("--task", type=str,
                        help="Task folder name or prefix (e.g., 0298 or full name)")
    parser.add_argument("--all", action="store_true",
                        help="Process all validated_output folders")
    parser.add_argument("--mapping", type=Path, default=MAPPING_PATH,
                        help="Path to esg_data_pointmapping.json")
    parser.add_argument("--output", type=Path, default=OUTPUT_DIR,
                        help="Output directory for DB-ready JSON")
    args = parser.parse_args()

    if not args.task and not args.all:
        parser.error("Specify --task <name> or --all")

    if not args.mapping.exists():
        print(f"Error: mapping file not found at {args.mapping}")
        print("Run: python scripts/build_mapping_template.py")
        sys.exit(1)

    args.output.mkdir(parents=True, exist_ok=True)

    if args.all:
        if not VALIDATED_DIR.exists():
            print(f"Error: validated_output directory not found at {VALIDATED_DIR}")
            sys.exit(1)
        folders = sorted(VALIDATED_DIR.iterdir())
        processed = 0
        for folder in folders:
            if folder.is_dir():
                if process_task(folder, args.mapping, args.output):
                    processed += 1
        print(f"\nDone. Processed {processed}/{len(folders)} tasks -> {args.output}")

    elif args.task:
        # Find matching folder (exact or prefix match)
        if not VALIDATED_DIR.exists():
            print(f"Error: validated_output directory not found at {VALIDATED_DIR}")
            sys.exit(1)
        matches = [f for f in VALIDATED_DIR.iterdir()
                    if f.is_dir() and f.name.startswith(args.task)]
        if not matches:
            print(f"Error: no task folder matching '{args.task}' in {VALIDATED_DIR}")
            sys.exit(1)
        for folder in sorted(matches):
            process_task(folder, args.mapping, args.output)


if __name__ == "__main__":
    main()
