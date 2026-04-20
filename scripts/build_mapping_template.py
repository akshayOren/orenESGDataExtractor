#!/usr/bin/env python3
"""
build_mapping_template.py
─────────────────────────
Walk data/esg_schema.json recursively and auto-generate
data/esg_data_pointmapping.json with placeholder data_point_ids
and extractor_identifier paths for every extractable leaf field.

Usage:
    python scripts/build_mapping_template.py
    python scripts/build_mapping_template.py --validate --sample validated_output/0298_.../esg_validated.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# ── Directories ──────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
SCHEMA_PATH = ROOT / "data" / "esg_schema.json"
OUTPUT_PATH = ROOT / "data" / "esg_data_pointmapping.json"

# ── Unit defaults by keyword ─────────────────────────────────────────────────
UNIT_HINTS = {
    "scope_1": "tCO2e", "scope_2": "tCO2e", "scope_3": "tCO2e",
    "total_emissions": "tCO2e", "biogenic_co2": "tCO2e",
    "emission_intensity": "tCO2e/M$",
    "energy": "MWh", "fuel": "MWh", "electricity": "MWh",
    "water": "m3", "withdrawal": "m3", "discharge": "m3", "recycled": "m3",
    "waste": "tonnes", "hazardous": "tonnes", "non_hazardous": "tonnes",
    "organic_waste": "tonnes", "food_waste": "tonnes",
    "ltifr": "per million hours", "trir": "per million hours",
    "lost_days": "days", "training_hours": "hours",
    "training_investment": "currency", "community_investment": "currency",
    "political_contributions": "currency",
    "pct": "%", "rate": "%",
    "employees": "count", "hires": "count", "contractors": "count",
    "fatalities": "count", "injuries": "count", "incidents": "count",
    "directors": "count", "board_size": "count", "breaches": "count",
    "audits": "count", "suppliers": "count", "cases": "count",
}

# ── Fields to skip (metadata, narrative, arrays of strings, refs) ────────────
SKIP_FIELDS = {
    "framework_refs", "company_aliases", "frameworks_used",
    "scope_3_categories_covered", "certifications",
    "extraction_metadata",
}

SKIP_TYPES = {"string"}  # pure narrative fields — only skip if not in special list
NARRATIVE_ALLOW = {
    "company_name", "country", "industry_sector", "report_title",
    "assurance_provider", "currency", "revenue_unit", "assurance_level",
    "methodology", "methodology_notes",
}


def guess_unit(field_name: str) -> str:
    """Guess a default unit from the field name."""
    for hint, unit in UNIT_HINTS.items():
        if hint in field_name:
            return unit
    return ""


def guess_category(path: str) -> str:
    """Extract top-level ESG category from the path."""
    parts = path.split(".")
    if len(parts) >= 1:
        top = parts[0]
        if top in ("environmental", "social", "governance", "supply_chain",
                    "report_metadata"):
            return top
    return "other"


def walk_schema(
    schema_node: dict,
    path: str,
    entries: dict,
    defs: dict,
    parent_is_array_item: bool = False,
    array_label_field: str | None = None,
):
    """Recursively walk the JSON Schema tree and emit mapping entries."""

    # Resolve $ref
    if "$ref" in schema_node:
        ref_name = schema_node["$ref"].split("/")[-1]
        resolved = defs.get(ref_name, {})
        # This is a metric_value leaf
        if ref_name == "metric_value":
            key = path.replace(".", "_").replace("[", "_").replace("]", "")
            entries[f"PLACEHOLDER_{key}"] = {
                "data_point_id": f"PLACEHOLDER_{key}",
                "extractor_identifier": path,
                "esg_category": guess_category(path),
                "kpi_name": path.split(".")[-1],
                "default_unit": guess_unit(path),
                "response": "",
                "prev_year_value": "",
                "prev_year_unit": "",
                "notes": "",
            }
            return
        # Other $refs — walk the resolved definition
        walk_schema(resolved, path, entries, defs, parent_is_array_item, array_label_field)
        return

    node_type = schema_node.get("type")

    # ── Object node ──────────────────────────────────────────────────────
    if node_type == "object":
        props = schema_node.get("properties", {})
        for prop_name, prop_schema in props.items():
            if prop_name in SKIP_FIELDS:
                continue
            child_path = f"{path}.{prop_name}" if path else prop_name
            walk_schema(prop_schema, child_path, entries, defs)

    # ── Array node ───────────────────────────────────────────────────────
    elif node_type == "array":
        items_schema = schema_node.get("items", {})

        # Determine the label field for array filter syntax
        item_props = items_schema.get("properties", {})
        label_field = None
        for candidate in ("category_number", "gas", "type", "waste_type", "source"):
            if candidate in item_props:
                label_field = candidate
                break

        if label_field:
            # Emit a wildcard entry for dynamic arrays
            wildcard_path = f"{path}[*]"
            key = wildcard_path.replace(".", "_").replace("[", "_").replace("]", "").replace("*", "all")
            entries[f"PLACEHOLDER_{key}"] = {
                "data_point_id": f"PLACEHOLDER_{key}",
                "extractor_identifier": wildcard_path,
                "esg_category": guess_category(path),
                "kpi_name": f"{path.split('.')[-1]} (all items)",
                "default_unit": "",
                "is_array_wildcard": True,
                "array_label_field": label_field,
                "response": "",
                "prev_year_value": "",
                "prev_year_unit": "",
                "notes": "Array: one DB record per item. Script iterates automatically.",
            }

            # For scope_3 categories, also emit entries for categories 1-15
            if "category_number" == label_field and "scope_3" in path:
                cat_names = {
                    1: "Purchased Goods and Services",
                    2: "Capital Goods",
                    3: "Fuel- and energy-related activities",
                    4: "Upstream transportation and distribution",
                    5: "Waste generated in operations",
                    6: "Business travel",
                    7: "Employee commuting",
                    8: "Upstream leased assets",
                    9: "Downstream transportation",
                    10: "Processing of sold products",
                    11: "Use of sold products",
                    12: "End-of-life treatment of sold products",
                    13: "Downstream leased assets",
                    14: "Franchises",
                    15: "Investments",
                }
                for cat_num, cat_name in cat_names.items():
                    filter_path = f"{path}[category_number={cat_num}]"
                    fkey = filter_path.replace(".", "_").replace("[", "_").replace("]", "").replace("=", "_")
                    entries[f"PLACEHOLDER_{fkey}"] = {
                        "data_point_id": f"PLACEHOLDER_{fkey}",
                        "extractor_identifier": filter_path,
                        "esg_category": guess_category(path),
                        "kpi_name": f"Scope 3 Cat {cat_num}: {cat_name}",
                        "default_unit": "tCO2e",
                        "response": "",
                        "prev_year_value": "",
                        "prev_year_unit": "",
                        "notes": "",
                    }
        else:
            # Array without a known label field — walk items generically
            walk_schema(items_schema, path, entries, defs, parent_is_array_item=True)

    # ── Scalar leaf ──────────────────────────────────────────────────────
    elif node_type in ("integer", "number", "boolean"):
        field_name = path.split(".")[-1]
        key = path.replace(".", "_")
        entries[f"PLACEHOLDER_{key}"] = {
            "data_point_id": f"PLACEHOLDER_{key}",
            "extractor_identifier": path,
            "esg_category": guess_category(path),
            "kpi_name": field_name,
            "default_unit": guess_unit(field_name),
            "response": "",
            "prev_year_value": "",
            "prev_year_unit": "",
            "notes": "",
        }

    elif node_type == "string":
        field_name = path.split(".")[-1]
        if field_name in NARRATIVE_ALLOW:
            key = path.replace(".", "_")
            entries[f"PLACEHOLDER_{key}"] = {
                "data_point_id": f"PLACEHOLDER_{key}",
                "extractor_identifier": path,
                "esg_category": guess_category(path),
                "kpi_name": field_name,
                "default_unit": "",
                "response": "",
                "prev_year_value": "",
                "prev_year_unit": "",
                "notes": "Text field",
            }


def build_mapping(schema_path: Path) -> dict:
    """Build the full mapping from the JSON Schema."""
    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    defs = schema.get("$defs", {})
    entries: dict = {}

    # Walk top-level properties (report_metadata, environmental, social, governance, supply_chain)
    for section_name, section_schema in schema.get("properties", {}).items():
        if section_name in SKIP_FIELDS:
            continue
        walk_schema(section_schema, section_name, entries, defs)

    return entries


def validate_mapping(mapping: dict, sample_path: Path):
    """Validate mapping against a sample esg_validated.json — report hit/miss."""
    with open(sample_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    from generate_db_json import resolve_path  # local import

    hits = 0
    misses = 0
    miss_list = []

    for key, entry in mapping.items():
        path = entry["extractor_identifier"]
        if "[*]" in path:
            # wildcard — check if the array exists
            base = path.replace("[*]", "")
            result = resolve_path(data, base)
            if result is not None and isinstance(result, list) and len(result) > 0:
                hits += 1
            else:
                misses += 1
                miss_list.append(path)
        else:
            result = resolve_path(data, path)
            if result is not None:
                hits += 1
            else:
                misses += 1
                miss_list.append(path)

    total = hits + misses
    print(f"\nValidation against: {sample_path.name}")
    print(f"  Total mappings: {total}")
    print(f"  Resolved:       {hits} ({100*hits/total:.1f}%)")
    print(f"  Missing:        {misses} ({100*misses/total:.1f}%)")
    if miss_list:
        print(f"\n  Missing paths:")
        for p in miss_list[:30]:
            print(f"    - {p}")
        if len(miss_list) > 30:
            print(f"    ... and {len(miss_list) - 30} more")


def main():
    parser = argparse.ArgumentParser(description="Build ESG data point mapping template from schema")
    parser.add_argument("--schema", type=Path, default=SCHEMA_PATH, help="Path to esg_schema.json")
    parser.add_argument("--output", type=Path, default=OUTPUT_PATH, help="Output mapping JSON path")
    parser.add_argument("--validate", action="store_true", help="Validate mapping against a sample")
    parser.add_argument("--sample", type=Path, help="Sample esg_validated.json for validation")
    args = parser.parse_args()

    if args.validate:
        if not args.sample:
            print("Error: --validate requires --sample <path to esg_validated.json>")
            sys.exit(1)
        if not args.output.exists():
            print(f"Error: mapping file not found at {args.output}")
            sys.exit(1)
        with open(args.output, "r", encoding="utf-8") as f:
            mapping = json.load(f)
        validate_mapping(mapping, args.sample)
        return

    mapping = build_mapping(args.schema)

    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(mapping, f, indent=2, ensure_ascii=False)

    # Summary
    categories = {}
    for entry in mapping.values():
        cat = entry.get("esg_category", "other")
        categories[cat] = categories.get(cat, 0) + 1

    print(f"Generated {len(mapping)} mapping entries -> {args.output}")
    for cat, count in sorted(categories.items()):
        print(f"  {cat}: {count}")


if __name__ == "__main__":
    main()
