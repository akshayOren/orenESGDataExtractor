"""
Phase 4 — Validation & QA for Extracted ESG Data
==================================================
Validates Phase 3 extraction output, normalizes units,
detects anomalies, scores completeness, and generates
a human review queue.

Usage:
    # Validate a single extraction:
    python esg_validator.py --input ./extracted_output/0007_TestCorp_2024_ESG_GRI

    # Validate all extracted tasks from the queue:
    python esg_validator.py --run

    # Show the human review queue:
    python esg_validator.py --review-queue

    # Export validated data as pipeline-ready CSV:
    python esg_validator.py --input ./extracted_output/0007_TestCorp_2024_ESG_GRI --export-csv

Requirements:
    No extra dependencies — pure Python.

Architecture:
    ┌────────────────┐
    │ esg_data.json  │
    │ (Phase 3)      │
    └───────┬────────┘
            │
            ▼
    ┌────────────────┐     7 validation layers:
    │  Validator      │     1. Schema conformance
    │  pipeline       │     2. Range plausibility
    │                 │     3. Cross-reference checks
    │                 │     4. YoY anomaly detection
    │                 │     5. Unit normalization
    │                 │     6. Completeness scoring
    │                 │     7. Confidence review flags
    └───────┬────────┘
            │
            ▼
    ┌────────────────┐
    │ validated/     │
    │  esg_validated │  ← cleaned + normalized data
    │  .json         │
    │  report.json   │  ← full validation report
    │  review_queue  │  ← items needing human review
    │  .json         │
    │  flat_kpis.csv │  ← DB-ready flat table
    └────────────────┘
"""

import os
import sys
import json
import csv
import logging
import argparse
import sqlite3
import math
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, Any
from dataclasses import dataclass, field, asdict
from copy import deepcopy

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("esg_validator")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
class ValidatorConfig:
    DB_PATH: str = "ingestion.db"
    EXTRACTED_DIR: str = "./extracted_output"
    OUTPUT_DIR: str = "./validated_output"

    # Confidence below this → review queue
    CONFIDENCE_THRESHOLD: float = 0.7

    # YoY change above this % → anomaly flag
    YOY_ANOMALY_THRESHOLD: float = 50.0

    # Completeness: minimum expected fields per pillar
    MIN_FIELDS_ENVIRONMENTAL: int = 3
    MIN_FIELDS_SOCIAL: int = 2
    MIN_FIELDS_GOVERNANCE: int = 1

    DEFAULT_BATCH_SIZE: int = 50


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------
@dataclass
class ValidationFlag:
    """A single validation issue."""
    field_path: str
    check_name: str        # Which validation layer caught this
    severity: str          # error, warning, info
    message: str
    current_value: Any = None
    expected_range: str = None
    suggested_fix: str = None

    def to_dict(self):
        d = asdict(self)
        return {k: v for k, v in d.items() if v is not None}


@dataclass
class ValidationReport:
    """Complete validation results for one extraction."""
    source_file: str
    company: str
    year: int
    timestamp: str

    # Scores (0-100)
    overall_score: float = 0.0
    completeness_score: float = 0.0
    accuracy_score: float = 0.0
    consistency_score: float = 0.0

    # Counts
    total_fields: int = 0
    valid_fields: int = 0
    flagged_fields: int = 0
    normalized_fields: int = 0

    # Flags grouped by severity
    errors: list = field(default_factory=list)
    warnings: list = field(default_factory=list)
    info: list = field(default_factory=list)

    # Review items
    review_queue: list = field(default_factory=list)

    # Completeness detail
    pillar_completeness: dict = field(default_factory=dict)

    @property
    def all_flags(self) -> list:
        return self.errors + self.warnings + self.info

    @property
    def pass_fail(self) -> str:
        if self.errors:
            return "FAIL"
        if self.overall_score >= 70:
            return "PASS"
        return "REVIEW"

    def to_dict(self):
        return {
            "source_file": self.source_file,
            "company": self.company,
            "year": self.year,
            "timestamp": self.timestamp,
            "verdict": self.pass_fail,
            "overall_score": round(self.overall_score, 1),
            "completeness_score": round(self.completeness_score, 1),
            "accuracy_score": round(self.accuracy_score, 1),
            "consistency_score": round(self.consistency_score, 1),
            "total_fields": self.total_fields,
            "valid_fields": self.valid_fields,
            "flagged_fields": self.flagged_fields,
            "normalized_fields": self.normalized_fields,
            "pillar_completeness": self.pillar_completeness,
            "errors": [f.to_dict() for f in self.errors],
            "warnings": [f.to_dict() for f in self.warnings],
            "info": [f.to_dict() for f in self.info],
            "review_queue": [r.to_dict() if hasattr(r, 'to_dict') else r for r in self.review_queue],
        }


# ---------------------------------------------------------------------------
# Layer 1: Schema conformance
# ---------------------------------------------------------------------------
class SchemaConformanceCheck:
    """Verify required fields exist and have correct types."""

    REQUIRED_FIELDS = {
        "report_metadata.company_name": str,
        "report_metadata.reporting_year": (int, float),
    }

    EXPECTED_TYPES = {
        "report_metadata.employee_count": (int, float),
        "report_metadata.reporting_year": (int, float),
    }

    def check(self, data: dict) -> list[ValidationFlag]:
        flags = []

        # Check required fields
        for field_path, expected_type in self.REQUIRED_FIELDS.items():
            val = _get_nested(data, field_path)
            if val is None:
                flags.append(ValidationFlag(
                    field_path=field_path,
                    check_name="schema_conformance",
                    severity="error",
                    message=f"Required field missing: {field_path}",
                ))
            elif not isinstance(val, expected_type):
                flags.append(ValidationFlag(
                    field_path=field_path,
                    check_name="schema_conformance",
                    severity="error",
                    message=f"Wrong type: expected {expected_type.__name__}, got {type(val).__name__}",
                    current_value=val,
                ))

        # Check year is reasonable
        year = _get_nested(data, "report_metadata.reporting_year")
        if year is not None and not (2000 <= year <= 2030):
            flags.append(ValidationFlag(
                field_path="report_metadata.reporting_year",
                check_name="schema_conformance",
                severity="error",
                message=f"Reporting year {year} outside valid range [2000, 2030]",
                current_value=year,
            ))

        return flags


# ---------------------------------------------------------------------------
# Layer 2: Range plausibility
# ---------------------------------------------------------------------------
class RangePlausibilityCheck:
    """Verify numeric values fall within physically possible ranges."""

    # (field_path, min, max, unit_hint)
    RANGES = [
        # GHG emissions (tCO2e)
        ("environmental.ghg_emissions.scope_1.total.value", 0, 500_000_000, "tCO2e"),
        ("environmental.ghg_emissions.scope_2.market_based.value", 0, 200_000_000, "tCO2e"),
        ("environmental.ghg_emissions.scope_2.location_based.value", 0, 200_000_000, "tCO2e"),
        ("environmental.ghg_emissions.scope_3.total.value", 0, 2_000_000_000, "tCO2e"),
        ("environmental.ghg_emissions.total_emissions.value", 0, 3_000_000_000, "tCO2e"),

        # Percentages (must be 0-100)
        ("environmental.energy.renewable_share_pct", 0, 100, "%"),
        ("environmental.waste.recycling_rate_pct", 0, 100, "%"),
        ("environmental.water.water_recycled_pct", 0, 100, "%"),
        ("social.workforce.turnover_rate_pct", 0, 100, "%"),
        ("social.workforce.turnover_voluntary_pct", 0, 100, "%"),
        ("social.diversity_inclusion.women_total_pct", 0, 100, "%"),
        ("social.diversity_inclusion.women_management_pct", 0, 100, "%"),
        ("social.diversity_inclusion.women_board_pct", 0, 100, "%"),
        ("governance.board_composition.independent_pct", 0, 100, "%"),

        # Safety rates
        ("social.health_safety.ltifr", 0, 200, "per M hours"),
        ("social.health_safety.trir", 0, 200, "per M hours"),
        ("social.health_safety.fatalities", 0, 10000, "count"),

        # Board
        ("governance.board_composition.board_size", 1, 50, "count"),
        ("governance.board_composition.independent_directors", 0, 50, "count"),

        # Employees
        ("report_metadata.employee_count", 1, 5_000_000, "count"),
        ("social.workforce.total_employees", 1, 5_000_000, "count"),

        # Training
        ("social.training_development.avg_training_hours", 0, 2000, "hours"),

        # Energy
        ("environmental.energy.total_consumption.within_org.value", 0, 500_000_000, "MWh"),
    ]

    def check(self, data: dict) -> list[ValidationFlag]:
        flags = []

        for field_path, lo, hi, unit_hint in self.RANGES:
            val = _get_nested(data, field_path)
            if val is None:
                continue

            if not isinstance(val, (int, float)):
                continue

            if val < lo or val > hi:
                flags.append(ValidationFlag(
                    field_path=field_path,
                    check_name="range_plausibility",
                    severity="error" if (val < 0 or val > hi * 10) else "warning",
                    message=f"Value {val:,.2f} outside plausible range [{lo:,}, {hi:,}] {unit_hint}",
                    current_value=val,
                    expected_range=f"[{lo:,}, {hi:,}]",
                ))

        return flags


# ---------------------------------------------------------------------------
# Layer 3: Cross-reference checks
# ---------------------------------------------------------------------------
class CrossReferenceCheck:
    """Verify internal consistency — parts sum to totals, related fields agree."""

    def check(self, data: dict) -> list[ValidationFlag]:
        flags = []

        env = data.get("environmental", {})
        ghg = env.get("ghg_emissions", {})

        # --- Scope 1 + 2(market) + 3 ≈ Total ---
        s1 = _extract_metric_value(ghg, "scope_1.total")
        s2m = _extract_metric_value(ghg, "scope_2.market_based")
        s3 = _extract_metric_value(ghg, "scope_3.total")
        total = _extract_metric_value(ghg, "total_emissions")

        if all(v is not None for v in [s1, s2m, s3, total]) and total > 0:
            calc = s1 + s2m + s3
            diff_pct = abs(calc - total) / total * 100
            if diff_pct > 5:
                flags.append(ValidationFlag(
                    field_path="environmental.ghg_emissions.total_emissions",
                    check_name="cross_reference",
                    severity="warning" if diff_pct < 20 else "error",
                    message=f"Scope 1 ({s1:,.0f}) + Scope 2 market ({s2m:,.0f}) + Scope 3 ({s3:,.0f}) = {calc:,.0f}, "
                            f"but reported total is {total:,.0f} (diff: {diff_pct:.1f}%)",
                    current_value=total,
                    suggested_fix=f"Calculated sum: {calc:,.0f}",
                ))

        # --- Scope 2: market ≤ location (typically) ---
        s2m = _extract_metric_value(ghg, "scope_2.market_based")
        s2l = _extract_metric_value(ghg, "scope_2.location_based")
        if s2m is not None and s2l is not None and s2m > s2l * 1.5:
            flags.append(ValidationFlag(
                field_path="environmental.ghg_emissions.scope_2",
                check_name="cross_reference",
                severity="info",
                message=f"Market-based Scope 2 ({s2m:,.0f}) is unusually higher than "
                        f"location-based ({s2l:,.0f}). Verify renewable energy credits.",
                current_value=s2m,
            ))

        # --- Renewable energy share consistency ---
        # Renewable consumption is now nested under electricity_consumption.within_org.by_type
        # with renewable types (solar, wind, etc.).  Skip automated cross-ref for now;
        # renewable_share_pct is still validated as a standalone percentage.

        # --- Employee count consistency ---
        meta_emp = _safe_number(data.get("report_metadata", {}).get("employee_count"))
        social_emp = _safe_number(_get_nested(data, "social.workforce.total_employees"))
        if meta_emp is not None and social_emp is not None and meta_emp != social_emp:
            flags.append(ValidationFlag(
                field_path="social.workforce.total_employees",
                check_name="cross_reference",
                severity="warning",
                message=f"Employee count mismatch: metadata says {meta_emp:,.0f}, "
                        f"social section says {social_emp:,.0f}",
                current_value=social_emp,
            ))

        # --- Independent directors ≤ board size ---
        board = data.get("governance", {}).get("board_composition", {}) or {}
        board_size = _safe_number(board.get("board_size"))
        independent = _safe_number(board.get("independent_directors"))
        if board_size is not None and independent is not None and independent > board_size:
            flags.append(ValidationFlag(
                field_path="governance.board_composition.independent_directors",
                check_name="cross_reference",
                severity="error",
                message=f"Independent directors ({independent:.0f}) exceeds board size ({board_size:.0f})",
                current_value=independent,
            ))

        return flags


# ---------------------------------------------------------------------------
# Layer 4: Year-over-year anomaly detection
# ---------------------------------------------------------------------------
class YoYAnomalyCheck:
    """Flag values that changed dramatically from prior year."""

    THRESHOLD_PCT = ValidatorConfig.YOY_ANOMALY_THRESHOLD

    # Fields to check for YoY anomalies: (path_to_metric, name)
    YOY_FIELDS = [
        ("environmental.ghg_emissions.scope_1.total", "Scope 1 emissions"),
        ("environmental.ghg_emissions.scope_2.market_based", "Scope 2 emissions (market)"),
        ("environmental.ghg_emissions.scope_3.total", "Scope 3 emissions"),
        ("environmental.ghg_emissions.total_emissions", "Total GHG emissions"),
        ("environmental.energy.total_consumption.within_org", "Total energy"),
    ]

    def check(self, data: dict) -> list[ValidationFlag]:
        flags = []

        for field_path, display_name in self.YOY_FIELDS:
            metric = _get_nested(data, field_path)
            if not isinstance(metric, dict):
                continue

            current = _safe_number(metric.get("value"))
            prior = _safe_number(metric.get("prior_year_value"))

            if current is None or prior is None or prior == 0:
                continue

            change_pct = ((current - prior) / abs(prior)) * 100

            if abs(change_pct) > self.THRESHOLD_PCT:
                direction = "increased" if change_pct > 0 else "decreased"
                flags.append(ValidationFlag(
                    field_path=field_path,
                    check_name="yoy_anomaly",
                    severity="warning",
                    message=f"{display_name} {direction} by {abs(change_pct):.1f}% YoY "
                            f"({prior:,.0f} -> {current:,.0f}). Verify this is correct.",
                    current_value=current,
                    expected_range=f"±{self.THRESHOLD_PCT}% of {prior:,.0f}",
                ))

        return flags


# ---------------------------------------------------------------------------
# Layer 5: Unit normalization
# ---------------------------------------------------------------------------
class UnitNormalizer:
    """Normalize units to canonical forms and convert where needed."""

    # Canonical unit mappings: variant → (canonical_unit, multiplier)
    UNIT_MAP = {
        # Emissions
        "tco2e": ("tCO2e", 1),
        "tco2": ("tCO2e", 1),
        "tonnes co2e": ("tCO2e", 1),
        "tonnes co2": ("tCO2e", 1),
        "ktco2e": ("tCO2e", 1000),
        "kt co2e": ("tCO2e", 1000),
        "mtco2e": ("tCO2e", 1_000_000),
        "mt co2e": ("tCO2e", 1_000_000),
        "kgco2e": ("tCO2e", 0.001),
        "kg co2e": ("tCO2e", 0.001),

        # Energy
        "mwh": ("MWh", 1),
        "gwh": ("MWh", 1000),
        "twh": ("MWh", 1_000_000),
        "kwh": ("MWh", 0.001),
        "gj": ("MWh", 0.2778),       # 1 GJ ≈ 0.2778 MWh
        "tj": ("MWh", 277.8),

        # Water
        "ml": ("megalitres", 1),
        "megalitres": ("megalitres", 1),
        "megaliters": ("megalitres", 1),
        "kl": ("megalitres", 0.001),
        "m3": ("megalitres", 0.001),   # 1 m³ = 0.001 ML
        "cubic meters": ("megalitres", 0.001),

        # Waste
        "tonnes": ("tonnes", 1),
        "metric tons": ("tonnes", 1),
        "mt": ("tonnes", 1),           # context dependent — use carefully
        "kt": ("tonnes", 1000),
        "kg": ("tonnes", 0.001),
    }

    def normalize(self, data: dict) -> tuple[dict, list[ValidationFlag]]:
        """
        Normalize units in metric_value objects throughout the data.
        Returns (normalized_data, list of changes made).
        """
        normalized = deepcopy(data)
        changes = []

        self._walk_and_normalize(normalized, "", changes)

        return normalized, changes

    def _walk_and_normalize(self, obj: dict, path: str, changes: list):
        """Recursively find metric_value objects and normalize them."""
        if not isinstance(obj, dict):
            return

        # Check if this looks like a metric_value object
        if "value" in obj and "unit" in obj:
            num_val = _safe_number(obj["value"])
            if num_val is None:
                # Skip non-numeric values
                pass
            else:
                unit_raw = str(obj["unit"]).strip().lower()

                if unit_raw in self.UNIT_MAP:
                    canonical, multiplier = self.UNIT_MAP[unit_raw]

                    if obj["unit"] != canonical or multiplier != 1:
                        old_val = num_val
                        old_unit = obj["unit"]

                        obj["value"] = num_val * multiplier
                        obj["unit"] = canonical

                        # Also convert prior_year_value if present
                        prior_num = _safe_number(obj.get("prior_year_value"))
                        if prior_num is not None:
                            obj["prior_year_value"] = prior_num * multiplier

                        base_num = _safe_number(obj.get("base_year_value"))
                        if base_num is not None:
                            obj["base_year_value"] = base_num * multiplier

                        if multiplier != 1 or old_unit != canonical:
                            changes.append(ValidationFlag(
                                field_path=path,
                                check_name="unit_normalization",
                                severity="info",
                                message=f"Normalized: {old_val:,.2f} {old_unit} -> {obj['value']:,.2f} {canonical}",
                                current_value=obj["value"],
                            ))

        # Recurse into nested dicts
        for key, val in obj.items():
            child_path = f"{path}.{key}" if path else key
            if isinstance(val, dict):
                self._walk_and_normalize(val, child_path, changes)
            elif isinstance(val, list):
                for i, item in enumerate(val):
                    if isinstance(item, dict):
                        self._walk_and_normalize(item, f"{child_path}[{i}]", changes)


# ---------------------------------------------------------------------------
# Layer 6: Completeness scoring
# ---------------------------------------------------------------------------
class CompletenessScorer:
    """Score how complete the extraction is per ESG pillar."""

    # Key fields we'd expect to find in a good sustainability report
    EXPECTED_FIELDS = {
        "environmental": [
            "ghg_emissions.scope_1.total.value",
            "ghg_emissions.scope_2.location_based.value",
            "ghg_emissions.scope_2.market_based.value",
            "ghg_emissions.scope_3.total.value",
            "ghg_emissions.total_emissions.value",
            "ghg_emissions.emission_intensity.value",
            "energy.total_consumption.within_org.value",
            "energy.fuel_consumption.within_org.total.value",
            "energy.electricity_consumption.within_org.total.value",
            "energy.renewable_share_pct",
            "water.total_withdrawal.value",
            "water.total_consumption.value",
            "water.water_recycled.value",
            "waste.total_waste.value",
            "waste.hazardous_waste.value",
            "waste.non_hazardous_waste.value",
            "waste.recycling_rate_pct",
        ],
        "social": [
            "workforce.total_employees",
            "workforce.total_employees_male",
            "workforce.total_employees_female",
            "workforce.turnover_rate_pct",
            "workforce.new_hires",
            "diversity_inclusion.women_total_pct",
            "diversity_inclusion.women_management_pct",
            "health_safety.ltifr",
            "health_safety.trir",
            "health_safety.fatalities",
            "training_development.avg_training_hours",
            "parental_leave.took_leave_male",
            "parental_leave.took_leave_female",
            "human_rights.human_rights_policy",
            "whistleblowing.mechanism_exists",
            "whistleblowing.cases_reported",
        ],
        "governance": [
            "board_composition.board_size",
            "board_composition.independent_directors",
            "board_composition.women_on_board_pct",
            "board_composition.esg_committee",
            "board_composition.esg_linked_compensation",
            "ethics_compliance.anti_corruption_policy",
            "ethics_compliance.confirmed_corruption_incidents",
            "ethics_compliance.whistleblower_mechanism",
            "data_privacy.data_breach_incidents",
        ],
        "report_metadata": [
            "company_name",
            "reporting_year",
            "frameworks_used",
            "employee_count",
        ],
        "supply_chain": [
            "supplier_engagement.total_suppliers",
            "supplier_engagement.suppliers_screened_pct",
            "supplier_engagement.supplier_code_of_conduct",
            "local_procurement.local_spending_pct",
        ],
    }

    def score(self, data: dict) -> dict:
        """
        Returns per-pillar completeness scores and an overall score.
        """
        results = {}

        for pillar, expected_fields in self.EXPECTED_FIELDS.items():
            pillar_data = data.get(pillar, {})
            found = 0
            missing = []

            for field_path in expected_fields:
                val = _get_nested(pillar_data, field_path)
                if val is not None:
                    found += 1
                else:
                    missing.append(field_path)

            total = len(expected_fields)
            pct = (found / total * 100) if total > 0 else 0

            results[pillar] = {
                "score": round(pct, 1),
                "found": found,
                "expected": total,
                "missing": missing,
            }

        # Overall weighted score
        weights = {"environmental": 0.35, "social": 0.25, "governance": 0.15, "report_metadata": 0.25}
        overall = sum(
            results[p]["score"] * weights.get(p, 0.25)
            for p in results
        )

        results["overall"] = round(overall, 1)
        return results


# ---------------------------------------------------------------------------
# Layer 7: Confidence review flags
# ---------------------------------------------------------------------------
class ConfidenceReviewCheck:
    """Flag low-confidence values for the human review queue."""

    THRESHOLD = ValidatorConfig.CONFIDENCE_THRESHOLD

    def check(self, data: dict) -> list[ValidationFlag]:
        flags = []
        self._walk(data, "", flags)
        return flags

    def _walk(self, obj, path: str, flags: list):
        if not isinstance(obj, dict):
            return

        if "confidence" in obj and "value" in obj:
            conf = _safe_number(obj["confidence"])
            if conf is not None and conf < self.THRESHOLD:
                flags.append(ValidationFlag(
                    field_path=path,
                    check_name="confidence_review",
                    severity="warning",
                    message=f"Low extraction confidence ({conf:.2f}) — needs human verification",
                    current_value=obj["value"],
                ))

        for key, val in obj.items():
            child = f"{path}.{key}" if path else key
            if isinstance(val, dict):
                self._walk(val, child, flags)
            elif isinstance(val, list):
                for i, item in enumerate(val):
                    if isinstance(item, dict):
                        self._walk(item, f"{child}[{i}]", flags)


# ---------------------------------------------------------------------------
# Flat KPI exporter (for DB ingestion)
# ---------------------------------------------------------------------------
class FlatKPIExporter:
    """
    Flatten the nested ESG JSON into a DB-ready table:
    one row per KPI with standardized columns.
    """

    def export(self, data: dict) -> list[dict]:
        """Returns a list of flat KPI rows."""
        rows = []
        company = data.get("report_metadata", {}).get("company_name", "Unknown")
        year = data.get("report_metadata", {}).get("reporting_year")

        # Walk through all metric_value objects
        self._extract_rows(data, "", company, year, rows)
        return rows

    def _extract_rows(self, obj, path: str, company: str, year: int, rows: list):
        if not isinstance(obj, dict):
            return

        # Is this a metric_value?
        if "value" in obj and "unit" in obj:
            rows.append({
                "company": company,
                "year": year,
                "kpi_path": path,
                "kpi_name": path.split(".")[-2] if "." in path else path,
                "value": obj.get("value"),
                "unit": obj.get("unit"),
                "prior_year_value": obj.get("prior_year_value"),
                "prior_year": obj.get("prior_year"),
                "base_year_value": obj.get("base_year_value"),
                "base_year": obj.get("base_year"),
                "confidence": obj.get("confidence"),
                "page_reference": obj.get("page_reference"),
                "notes": obj.get("notes"),
            })
            return

        # Is this a simple numeric KPI (like women_total_pct: 44.2)?
        for key, val in obj.items():
            child = f"{path}.{key}" if path else key
            if isinstance(val, (int, float)) and not key.startswith("_"):
                # Simple scalar KPI
                if any(kw in key for kw in ["pct", "rate", "count", "hours", "size",
                                              "directors", "employees", "fatalities",
                                              "ltifr", "trir", "breaches"]):
                    rows.append({
                        "company": company,
                        "year": year,
                        "kpi_path": child,
                        "kpi_name": key,
                        "value": val,
                        "unit": "%" if "pct" in key else "count",
                        "prior_year_value": None,
                        "prior_year": None,
                        "base_year_value": None,
                        "base_year": None,
                        "confidence": None,
                        "page_reference": None,
                        "notes": None,
                    })
            elif isinstance(val, dict):
                self._extract_rows(val, child, company, year, rows)
            elif isinstance(val, list):
                for i, item in enumerate(val):
                    if isinstance(item, dict):
                        self._extract_rows(item, f"{child}[{i}]", company, year, rows)


# ---------------------------------------------------------------------------
# Master validator pipeline
# ---------------------------------------------------------------------------
class ESGValidatorPipeline:
    """Runs all 7 validation layers and produces a report."""

    def __init__(self):
        self.schema_check = SchemaConformanceCheck()
        self.range_check = RangePlausibilityCheck()
        self.cross_ref_check = CrossReferenceCheck()
        self.yoy_check = YoYAnomalyCheck()
        self.normalizer = UnitNormalizer()
        self.completeness = CompletenessScorer()
        self.confidence_check = ConfidenceReviewCheck()
        self.kpi_exporter = FlatKPIExporter()

    def validate(self, data: dict) -> tuple[dict, ValidationReport]:
        """
        Run the full validation pipeline.
        Returns (validated_data, report).
        """
        company = data.get("report_metadata", {}).get("company_name", "Unknown")
        year = data.get("report_metadata", {}).get("reporting_year", 0)
        source = data.get("extraction_metadata", {}).get("source_pdf", "unknown")

        report = ValidationReport(
            source_file=source,
            company=company,
            year=year,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        # Count total fields
        report.total_fields = _count_leaf_values(data)

        # Layer 1: Schema conformance
        log.info("  [1/7] Schema conformance...")
        flags = self.schema_check.check(data)
        self._add_flags(report, flags)

        # Layer 2: Range plausibility
        log.info("  [2/7] Range plausibility...")
        flags = self.range_check.check(data)
        self._add_flags(report, flags)

        # Layer 3: Cross-reference
        log.info("  [3/7] Cross-reference checks...")
        flags = self.cross_ref_check.check(data)
        self._add_flags(report, flags)

        # Layer 4: YoY anomaly detection
        log.info("  [4/7] Year-over-year anomaly detection...")
        flags = self.yoy_check.check(data)
        self._add_flags(report, flags)

        # Layer 5: Unit normalization
        log.info("  [5/7] Unit normalization...")
        validated_data, norm_flags = self.normalizer.normalize(data)
        report.normalized_fields = len(norm_flags)
        self._add_flags(report, norm_flags)

        # Layer 6: Completeness scoring
        log.info("  [6/7] Completeness scoring...")
        completeness = self.completeness.score(validated_data)
        report.pillar_completeness = completeness
        report.completeness_score = completeness.get("overall", 0)

        # Generate info flags for missing fields
        for pillar, details in completeness.items():
            if pillar == "overall":
                continue
            for missing_field in details.get("missing", []):
                self._add_flags(report, [ValidationFlag(
                    field_path=f"{pillar}.{missing_field}",
                    check_name="completeness",
                    severity="info",
                    message=f"Expected field not found in report",
                )])

        # Layer 7: Confidence review flags
        log.info("  [7/7] Confidence review flags...")
        flags = self.confidence_check.check(validated_data)
        self._add_flags(report, flags)

        # Build review queue (errors + warnings)
        report.review_queue = [f for f in report.all_flags if f.severity in ("error", "warning")]

        # Calculate scores
        report.flagged_fields = len(report.errors) + len(report.warnings)
        report.valid_fields = report.total_fields - report.flagged_fields
        report.accuracy_score = max(0, (1 - len(report.errors) / max(report.total_fields, 1)) * 100)
        report.consistency_score = max(0, (1 - len(report.warnings) / max(report.total_fields, 1)) * 100)
        report.overall_score = (
            report.completeness_score * 0.4 +
            report.accuracy_score * 0.35 +
            report.consistency_score * 0.25
        )

        # Stamp validated data
        validated_data.setdefault("extraction_metadata", {})
        validated_data["extraction_metadata"]["validation_date"] = report.timestamp
        validated_data["extraction_metadata"]["validation_score"] = round(report.overall_score, 1)
        validated_data["extraction_metadata"]["validation_verdict"] = report.pass_fail
        validated_data["extraction_metadata"]["human_reviewed"] = False

        return validated_data, report

    def _add_flags(self, report: ValidationReport, flags: list[ValidationFlag]):
        for f in flags:
            if f.severity == "error":
                report.errors.append(f)
            elif f.severity == "warning":
                report.warnings.append(f)
            else:
                report.info.append(f)


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------
class ValidationOutputWriter:

    def __init__(self, base_dir: str = ValidatorConfig.OUTPUT_DIR):
        self.base_dir = Path(base_dir)

    def write(self, validated_data: dict, report: ValidationReport,
              kpi_rows: list[dict], input_dir: str) -> Path:
        """Write all validation outputs."""
        out_dir = self.base_dir / Path(input_dir).name
        out_dir.mkdir(parents=True, exist_ok=True)

        # Validated ESG data (cleaned + normalized)
        with open(out_dir / "esg_validated.json", "w", encoding="utf-8") as f:
            json.dump(validated_data, f, indent=2, ensure_ascii=False, default=str)

        # Validation report
        with open(out_dir / "validation_report.json", "w", encoding="utf-8") as f:
            json.dump(report.to_dict(), f, indent=2, ensure_ascii=False, default=str)

        # Review queue (items needing human attention)
        if report.review_queue:
            with open(out_dir / "review_queue.json", "w", encoding="utf-8") as f:
                json.dump(
                    [r.to_dict() for r in report.review_queue],
                    f, indent=2, ensure_ascii=False,
                )

        # Flat KPI CSV (DB-ready)
        if kpi_rows:
            csv_path = out_dir / "flat_kpis.csv"
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=kpi_rows[0].keys())
                writer.writeheader()
                writer.writerows(kpi_rows)

        log.info(f"Output written: {out_dir}")
        return out_dir


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _get_nested(data: dict, path: str) -> Any:
    """Get a value from a nested dict using dot-separated path."""
    keys = path.split(".")
    obj = data
    for key in keys:
        if isinstance(obj, dict) and key in obj:
            obj = obj[key]
        else:
            return None
    return obj


def _safe_number(obj) -> Optional[float]:
    """Unwrap any value to a plain float, or None if not numeric."""
    if obj is None:
        return None
    if isinstance(obj, dict) and "value" in obj:
        obj = obj["value"]
    if isinstance(obj, (int, float)):
        return float(obj)
    if isinstance(obj, str):
        try:
            return float(obj.replace(",", ""))
        except (ValueError, TypeError):
            return None
    return None


def _extract_metric_value(data: dict, path: str) -> Optional[float]:
    """Get the numeric value from a metric_value object at the given path."""
    obj = _get_nested(data, path)
    return _safe_number(obj)


def _count_leaf_values(obj, depth=0) -> int:
    """Count non-null leaf values in a nested dict."""
    if depth > 15:
        return 0
    count = 0
    if isinstance(obj, dict):
        for v in obj.values():
            if isinstance(v, (dict, list)):
                count += _count_leaf_values(v, depth + 1)
            elif v is not None:
                count += 1
    elif isinstance(obj, list):
        for item in obj:
            count += _count_leaf_values(item, depth + 1)
    return count


# ---------------------------------------------------------------------------
# DB interface
# ---------------------------------------------------------------------------
class IngestionDB:
    def __init__(self, db_path: str = ValidatorConfig.DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def get_extracted_tasks(self, limit: int = 50) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM pdf_tasks WHERE status = 'extracted' ORDER BY ingested_at ASC LIMIT ?",
            (limit,),
        ).fetchall()

    def get_task_by_id(self, task_id: int):
        return self.conn.execute(
            "SELECT * FROM pdf_tasks WHERE id = ?", (task_id,)
        ).fetchone()

    def update_status(self, task_id: int, status: str, error: str = None):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE pdf_tasks SET status = ?, error_message = ?, updated_at = ? WHERE id = ?",
            (status, error, now, task_id),
        )
        self.conn.commit()

    def get_status_summary(self) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM pdf_tasks GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Phase 4 — Validate & QA extracted ESG data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python esg_validator.py --input ./extracted_output/0007_TestCorp_2024_ESG_GRI
  python esg_validator.py --run
  python esg_validator.py --review-queue
  python esg_validator.py --input ./extracted_output/0007_TestCorp_2024_ESG_GRI --export-csv
        """,
    )

    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument("--input", type=str, help="Validate a single extracted folder")
    group.add_argument("--run", action="store_true", help="Validate all extracted tasks")
    group.add_argument("--review-queue", action="store_true", help="Show all items needing review")

    parser.add_argument("--export-csv", action="store_true", help="Export flat KPI CSV")
    parser.add_argument("--output-dir", default=ValidatorConfig.OUTPUT_DIR)
    parser.add_argument("--db", default=ValidatorConfig.DB_PATH)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument(
        "--resync",
        action="store_true",
        help="Scan extracted_output/ and mark tasks with esg_data.json as 'extracted' in DB",
    )

    args = parser.parse_args()
    if not args.resync and not args.input and not args.run and not args.review_queue:
        parser.error("one of the arguments --input --run --review-queue --resync is required")
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ValidatorConfig.OUTPUT_DIR = args.output_dir
    ValidatorConfig.DB_PATH = args.db

    pipeline = ESGValidatorPipeline()
    writer = ValidationOutputWriter(args.output_dir)

    # --- Resync mode: update DB status from filesystem ---
    if args.resync:
        db = IngestionDB(args.db)
        base = Path(ValidatorConfig.EXTRACTED_DIR)
        synced = 0
        if base.exists():
            for d in sorted(base.iterdir()):
                if not d.is_dir():
                    continue
                parts = d.name.split("_", 1)
                if not parts[0].isdigit():
                    continue
                task_id = int(parts[0])
                data_path = d / "esg_data.json"
                if not data_path.exists():
                    continue
                task = db.get_task_by_id(task_id)
                if task and task["status"] == "parsed":
                    db.update_status(task_id, "extracted")
                    print(f"  Synced task #{task_id} -> 'extracted'  ({d.name})")
                    synced += 1
        print(f"  Resync complete: {synced} task(s) updated.")
        if not args.run:
            return

    # --- Single folder mode ---
    if args.input:
        input_path = Path(args.input)
        data_path = input_path / "esg_data.json"

        if not data_path.exists():
            print(f"Error: {data_path} not found")
            sys.exit(1)

        data = json.loads(data_path.read_text(encoding="utf-8"))

        log.info(f"Validating: {input_path.name}")
        validated, report = pipeline.validate(data)

        # Export flat KPIs
        kpi_rows = pipeline.kpi_exporter.export(validated)

        out_dir = writer.write(validated, report, kpi_rows, args.input)

        # Print summary
        print(f"\n{'─' * 55}")
        print(f"  Validation Report: {report.company} ({report.year})")
        print(f"{'─' * 55}")
        print(f"  Verdict:        {report.pass_fail}")
        print(f"  Overall score:  {report.overall_score:.1f}/100")
        print(f"  Completeness:   {report.completeness_score:.1f}/100")
        print(f"  Accuracy:       {report.accuracy_score:.1f}/100")
        print(f"  Consistency:    {report.consistency_score:.1f}/100")
        print(f"  Total fields:   {report.total_fields}")
        print(f"  Normalized:     {report.normalized_fields}")
        print(f"  KPIs exported:  {len(kpi_rows)} rows")

        print(f"\n  Pillar completeness:")
        for pillar in ["report_metadata", "environmental", "social", "governance"]:
            if pillar in report.pillar_completeness:
                p = report.pillar_completeness[pillar]
                bar = "#" * int(p["score"] / 5) + "-" * (20 - int(p["score"] / 5))
                print(f"    {pillar:<22} {bar} {p['score']:>5.1f}%  ({p['found']}/{p['expected']})")

        if report.errors:
            print(f"\n  Errors ({len(report.errors)}):")
            for f in report.errors:
                print(f"    [X] {f.field_path}: {f.message}")

        if report.warnings:
            print(f"\n  Warnings ({len(report.warnings)}):")
            for f in report.warnings:
                print(f"    [!] {f.field_path}: {f.message}")

        if report.review_queue:
            print(f"\n  Review queue: {len(report.review_queue)} items need human review")

        print(f"\n  Output: {out_dir}")
        print()
        return

    # --- Batch mode ---
    db = IngestionDB(args.db)

    if args.review_queue:
        # Scan all validated output for review items
        base = Path(args.output_dir)
        total_reviews = 0
        if base.exists():
            for d in sorted(base.iterdir()):
                rq_path = d / "review_queue.json"
                if rq_path.exists():
                    items = json.loads(rq_path.read_text())
                    if items:
                        print(f"\n  {d.name}: {len(items)} items")
                        for item in items:
                            sev = "[X]" if item["severity"] == "error" else "[!]"
                            print(f"    {sev} {item['field_path']}: {item['message']}")
                        total_reviews += len(items)

        if total_reviews == 0:
            print("  No items in review queue.")
        else:
            print(f"\n  Total: {total_reviews} items need review")
        print()
        return

    if args.run:
        extracted = db.get_extracted_tasks(limit=ValidatorConfig.DEFAULT_BATCH_SIZE)
        if not extracted:
            print("No extracted tasks to validate.")
            return

        log.info(f"Validating {len(extracted)} tasks")
        results = {"pass": 0, "review": 0, "fail": 0}

        for task in extracted:
            task_id = task["id"]
            file_name = task["file_name"]

            # Find extracted output folder
            ext_dir = None
            base = Path(ValidatorConfig.EXTRACTED_DIR)
            if base.exists():
                for d in sorted(base.iterdir()):
                    if d.is_dir() and d.name.startswith(f"{task_id:04d}_"):
                        ext_dir = d
                        break

            if not ext_dir:
                log.warning(f"Task #{task_id}: extracted output not found")
                continue

            data_path = ext_dir / "esg_data.json"
            if not data_path.exists():
                log.warning(f"Task #{task_id}: esg_data.json not found in {ext_dir}")
                continue

            log.info(f"{'─' * 60}")
            log.info(f"Validating task #{task_id}: {file_name}")
            db.update_status(task_id, "validating")

            try:
                data = json.loads(data_path.read_text(encoding="utf-8"))
                validated, report = pipeline.validate(data)
                kpi_rows = pipeline.kpi_exporter.export(validated)
                writer.write(validated, report, kpi_rows, str(ext_dir))

                status = "validated" if report.pass_fail != "FAIL" else "review_needed"
                db.update_status(task_id, status)
                results[report.pass_fail.lower()] = results.get(report.pass_fail.lower(), 0) + 1

                log.info(f"  Score: {report.overall_score:.1f}/100 -> {report.pass_fail}")

            except Exception as e:
                log.error(f"Task #{task_id} validation failed: {e}")
                db.update_status(task_id, "failed", str(e))

        print(f"\n{'═' * 55}")
        print(f"  Validation Batch Complete")
        print(f"{'═' * 55}")
        for k, v in results.items():
            print(f"  {k.upper():<10} {v}")
        print()


if __name__ == "__main__":
    main()