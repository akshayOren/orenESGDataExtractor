"""
Phase 3 — LLM-based ESG Data Extraction
=========================================
Reads parsed output from Phase 2 (markdown + tables), constructs
structured prompts against the ESG schema, and uses an LLM (Claude)
to extract structured sustainability data.

Usage:
    # Extract from a single parsed output folder:
    python esg_extractor.py --input ./parsed_output/0007_TestCorp_2024_ESG_GRI

    # Process all parsed tasks from the queue:
    python esg_extractor.py --run

    # Dry run with simulated LLM (no API key needed):
    python esg_extractor.py --input ./parsed_output/0007_TestCorp_2024_ESG_GRI --simulate

    # Use a specific model:
    python esg_extractor.py --run --model claude-sonnet-4-20250514

Requirements:
    pip install anthropic   # For Claude API
    # OR set --simulate for testing without API

Architecture:
    ┌──────────────┐     ┌────────────────┐     ┌──────────────┐
    │ parsed_output│────▶│  Prompt builder │────▶│ Claude API   │
    │ full.md      │     │  (per-section   │     │ (structured  │
    │ tables/*.csv │     │   chunked)      │     │  JSON output)│
    └──────────────┘     └────────────────┘     └──────┬───────┘
                                                        │
                                                        ▼
                                                 ┌──────────────┐
                                                 │ Validator    │
                                                 │ + merger     │
                                                 └──────┬───────┘
                                                        │
                                                        ▼
                                                 ┌──────────────┐
                                                 │ extracted/   │
                                                 │  esg_data.json
                                                 │  summary.json│
                                                 │  review_flags│
                                                 └──────────────┘
"""

import os
import sys
import json
import time
import logging
import argparse
import traceback
import sqlite3
import re
import base64
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from copy import deepcopy

# Load .env from the project root so ANTHROPIC_API_KEY is available
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from oren_esg.config import settings  # noqa: F401  triggers load_dotenv()

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("esg_extractor")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
class ExtractorConfig:
    DB_PATH: str = "ingestion.db"
    PARSED_DIR: str = "./parsed_output"
    OUTPUT_DIR: str = "./extracted_output"
    SCHEMA_PATH: str = "./data/esg_schema.json"

    # LLM settings
    MODEL: str = "claude-sonnet-4-20250514"
    MAX_TOKENS: int = 8192
    TEMPERATURE: float = 0.0          # Deterministic for data extraction

    # Chunking: max chars per LLM call (fallback when section files unavailable)
    # Set high enough to cover entire report — Claude handles 200K context
    MAX_CHUNK_CHARS: int = 120000

    # Confidence threshold below which values get flagged for review
    REVIEW_THRESHOLD: float = 0.9

    DEFAULT_BATCH_SIZE: int = 10


# ---------------------------------------------------------------------------
# Ingestion DB interface (same as Phase 1/2)
# ---------------------------------------------------------------------------
class IngestionDB:
    def __init__(self, db_path: str = ExtractorConfig.DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def get_parsed_tasks(self, limit: int = 50) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT * FROM pdf_tasks
               WHERE status = 'parsed'
               ORDER BY ingested_at ASC LIMIT ?""",
            (limit,),
        ).fetchall()

    def get_task_by_id(self, task_id: int) -> Optional[sqlite3.Row]:
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
# Prompt templates
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are an expert ESG data analyst. Your job is to extract structured sustainability data from corporate ESG/sustainability reports.

RULES:
1. Extract ONLY data that is explicitly stated in the report text. Never infer or calculate values.
2. For every numeric value, include the exact unit as stated in the report.
3. If a value is not found or not disclosed, omit that field entirely — do NOT guess.
4. Include a confidence score (0.0-1.0) for each extracted value:
   - 1.0 = value clearly stated in a structured table with a header row
   - 0.95 = value clearly stated in prose with explicit number and unit
   - 0.9 = value clearly stated but minor ambiguity (e.g., unit inferred from context)
   - 0.8 = value requires some interpretation (e.g., derived from chart description, or unit unclear)
   - < 0.8 = uncertain — OMIT the field instead of extracting with low confidence
   Values below 0.9 will be flagged for manual review. When in doubt, OMIT rather than guess.
5. Include the page number where each value was found when visible in the text.
6. For year-over-year data, extract BOTH current year AND prior year values when available.
   The "year" field MUST be the report's reporting year. "prior_year" MUST be reporting_year - 1.
   If a metric only has data for years before the reporting year (e.g., due to lag), OMIT it entirely.
7. Respond with ONLY valid JSON. No markdown, no explanation, no preamble.
8. When the same data appears in both a summary/highlights/milestones section AND a detailed section (tables, data pages), ALWAYS prefer the value from the detailed section. Summary sections may contain rounded or approximate figures.
9. For gender-disaggregated data, always extract male and female values separately when reported.
10. For percentage values, extract the raw percentage number (e.g., 15.36 not 0.1536)."""


def build_extraction_prompt(section: str, content: str, detected_metrics: list, detected_frameworks: list) -> str:
    """
    Build a targeted extraction prompt for a specific ESG section.
    The prompt focuses on metrics the Phase 2 detector already found,
    making extraction more accurate and token-efficient.
    """

    metrics_hint = ""
    if detected_metrics:
        metrics_hint = f"\n\nThe following metrics were detected in this report: {', '.join(detected_metrics)}. Focus extraction on these."

    frameworks_hint = ""
    if detected_frameworks:
        frameworks_hint = f"\nThe report aligns with: {', '.join(detected_frameworks)}."

    if section == "environmental":
        return f"""Extract ALL environmental sustainability data from this report content.
{metrics_hint}{frameworks_hint}

IMPORTANT RULES:
- CRITICAL — DO NOT FABRICATE DATA: Only extract values where the EXACT number appears in the text or table.
  If a section discusses a topic (e.g., "GHG emissions increased" or "waste was managed safely") but gives
  NO specific numeric value, you MUST return null for that field. Do NOT estimate, infer, round, or generate
  plausible values. Do NOT extrapolate numbers from narrative descriptions of trends.
  "<!-- image -->" markers mean the numeric data is in a chart/image that you cannot see — do NOT guess what
  those charts might contain. The Vision API handles chart data separately.
  TEST: Can you point to the exact characters in the text where this number appears? If NO → omit the field.
- SKIP any "Key Milestones", "Highlights", or summary/overview sections that present high-level numbers.
  Extract data ONLY from the detailed sections (e.g., "Energy Efficiency", "Emissions Management", structured tables).
  If the same metric appears in both a summary and a detailed section, use the detailed section value.
- CRITICAL: Do NOT confuse emission REDUCTION/ABATEMENT values with actual emissions.
  Tables showing "emissions reduced as a result of abatement initiatives" or "emission reductions" are NOT actual Scope 1/2/3 values.
  Only extract Scope 1/2/3 from tables/sections that report actual total emissions (e.g., "Total GHG emissions", "Scope 1 emissions").
  If the report does not explicitly state actual Scope 1/2/3 values, leave those fields null rather than using reduction figures.
- CRITICAL YEAR RULE: The "year" field must ALWAYS be the report's reporting year (from the reporting period).
  The "prior_year" must be reporting_year - 1. For a report covering 01-01-2024 to 31-12-2024:
    - "year" = 2024, "prior_year" = 2023
    - "value" = the value for 2024, "prior_year_value" = the value for 2023
  If a metric has NO value for the reporting year (e.g., the 2024 column shows "***", "N/A", "-", or is blank),
  OMIT that field entirely — even if older year data exists. Do NOT fall back to the 2023 column as "value"
  and 2022 as "prior_year_value". Example: if a table has columns 2022|2023|2024 and Scope 3 row shows
  1,836,100|1,993,689|*** then OMIT scope_3 because there is no 2024 value.
- For water: Use the EXACT terminology from the report. If the report says "Water Consumption" (not "withdrawal"),
  map it to total_consumption, NOT total_withdrawal. "total withdrawal" is water taken FROM sources (groundwater,
  municipal, seawater, etc.) — only use this if the report explicitly says "withdrawal" or "abstraction".
  "total consumption" = total water used/consumed by the organization.
  "water recycled/reclaimed" is a SEPARATE field — do NOT put recycled volume as total_withdrawal or total_consumption.
- Always extract BOTH current year and prior year values when both are presented (e.g., CY2023 and CY2024).
- For energy: extract fuel consumption, electricity consumption, and total energy SEPARATELY using within_org/outside_org structure.
  If not explicitly stated whether consumption is within or outside the organization, default to within_org.
  Natural gas is a fuel type — extract it under fuel_consumption.within_org.by_type, NOT as a standalone field.
  Grid electricity goes under electricity_consumption.within_org.by_type with type "grid".
  Individual fuel types (petrol/gasoline, diesel, LPG, natural gas) go under fuel_consumption.within_org.by_type.
  Keep original units from the PDF (liters, kWh, m3) — do NOT convert to GJ unless the PDF itself reports in GJ.
- For Scope 1: extract BOTH the total AND the breakdown by source if given:
  stationary_combustion (fuel), fugitive_refrigerants, mobile_combustion, process_emissions.
  Put each under scope_1.by_source array.
- For Scope 2: extract BOTH the total AND the breakdown by source if given:
  purchased_electricity (grid), district_cooling, district_heating, solar, wind, hydro, or any other
  type unique to the report. Put each under scope_2.by_source array.
- For Scope 3: extract BOTH the total AND the category-wise breakdown (categories 1-15) if available.
- For waste: extract total waste AND waste by type if available.

Return a JSON object with this structure (include only fields that have data):
{{
  "ghg_emissions": {{
    "scope_1": {{
      "total": {{ "value": <number>, "unit": "tCO2e", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
      "by_source": [
        {{ "source": "<stationary_combustion|fugitive_refrigerants|mobile_combustion|process_emissions>", "value": {{ "value": <number>, "unit": "tCO2e", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }} }}
      ],
      "percentage_of_total": <number>
    }},
    "scope_2": {{
      "location_based": {{ "value": <number>, "unit": "tCO2e", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
      "market_based": {{ "value": <number>, "unit": "tCO2e", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
      "methodology": "<location-based|market-based|both>",
      "by_source": [
        {{ "source": "<purchased_electricity|district_cooling|district_heating|solar|wind|hydro|other>", "value": {{ "value": <number>, "unit": "tCO2e", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }} }}
      ],
      "percentage_of_total": <number>
    }},
    "scope_3": {{
      "total": {{ "value": <number>, "unit": "tCO2e", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
      "percentage_of_total": <number>,
      "categories": [
        {{
          "category_number": <1-15>,
          "category_name": "<e.g., Purchased Goods and Services>",
          "value": <number>,
          "unit": "tCO2e",
          "year": <year>,
          "prior_year_value": <number>,
          "prior_year": <year>,
          "confidence": <0-1>
        }}
      ]
    }},
    "total_emissions": {{ "value": <number>, "unit": "tCO2e", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "emission_intensity": {{ "value": <number>, "unit": "<unit>", "denominator": "<what>", "year": <year> }},
    "reduction_targets": [{{ "target_description": "...", "target_year": <year>, "base_year": <year>, "reduction_pct": <number>, "scope_covered": "..." }}],
    "methodology_notes": "<GHG accounting methodology, emission factors used>"
  }},
  "energy": {{
    "total_consumption": {{
      "within_org": {{ "value": <number>, "unit": "<GJ or kWh — use the unit from the PDF>", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
      "outside_org": {{ "value": <number>, "unit": "<unit>", "year": <year>, "confidence": <0-1> }}
    }},
    "fuel_consumption": {{
      "within_org": {{
        "total": {{ "value": <number>, "unit": "<unit>", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
        "by_type": [
          {{ "type": "<diesel|gasoline|petrol|LPG|natural_gas|propane|etc>", "value": {{ "value": <number>, "unit": "<liters|m3|GJ — original unit from PDF>", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }} }}
        ]
      }},
      "outside_org": {{ "value": <number>, "unit": "<unit>", "year": <year>, "confidence": <0-1> }}
    }},
    "electricity_consumption": {{
      "within_org": {{
        "total": {{ "value": <number>, "unit": "<kWh or GJ — original unit from PDF>", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
        "by_type": [
          {{ "type": "<grid|steam|heat|captive|solar|wind|etc>", "value": {{ "value": <number>, "unit": "<unit>", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }} }}
        ]
      }},
      "outside_org": {{ "value": <number>, "unit": "<unit>", "year": <year>, "confidence": <0-1> }}
    }},
    "renewable_share_pct": <number>,
    "energy_intensity": {{ "value": <number>, "unit": "<unit>", "denominator": "<what>", "year": <year> }}
  }},
  "water": {{
    "total_withdrawal": {{ "value": <number>, "unit": "m3", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "total_consumption": {{ "value": <number>, "unit": "m3", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "total_discharge": {{ "value": <number>, "unit": "m3", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "water_recycled": {{ "value": <number>, "unit": "m3", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "water_recycled_pct": <number>,
    "withdrawal_by_source": [
      {{ "source": "<e.g., municipal, tanker, groundwater, desalinated, seawater>", "value": <number>, "unit": "m3", "year": <year> }}
    ],
    "water_intensity": {{ "value": <number>, "unit": "<e.g., m3/employee, m3/M$ revenue>", "denominator": "<what>" }}
  }},
  "waste": {{
    "total_waste": {{ "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "hazardous_waste": {{ "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "non_hazardous_waste": {{ "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "organic_waste": {{ "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "food_waste": {{ "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "waste_to_landfill": {{ "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "waste_diverted_from_disposal": {{ "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "recycling_rate_pct": <number>,
    "waste_by_type": [
      {{ "waste_type": "<e.g., organic, plastic, paper, glass, metal, e-waste>", "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number> }}
    ]
  }},
  "air_emissions": {{
    "by_type": [
      {{ "type": "<NOx|SOx|VOC|particulate_matter|CO|H2S|ozone_depleting_substances>", "value": {{ "value": <number>, "unit": "tonnes", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }} }}
    ]
  }}
}}

REPORT CONTENT:
---
{content}
---"""

    elif section == "social":
        return f"""Extract ALL social sustainability data from this report content.
{metrics_hint}{frameworks_hint}

IMPORTANT RULES:
- Extract gender-disaggregated data (male/female) wherever reported.
- Extract age-group breakdowns wherever reported (typically <30, 30-50, >50).
- Always extract BOTH current year and prior year values when both are presented.
- CRITICAL YEAR RULE: The "year" field must always be the report's reporting year (from the reporting period).
  The "prior_year" must be reporting_year - 1. If a metric only has data for years BEFORE the reporting
  year, OMIT that field entirely. Do NOT shift years down.
- For percentages, extract the raw number (e.g., 15.36 not 0.1536).
- Look for data in tables, charts, and prose text across all social/workforce sections.
- CRITICAL: Whistleblowing data (cases reported, concerns raised through ethics hotline/speak-up channels)
  is NOT human rights data. Extract whistleblowing under "whistleblowing", not under "human_rights".
  Human rights incidents are specifically about forced labor, child labor, discrimination complaints,
  indigenous rights violations, etc.
- SUBSIDIARY DATA RULE: Always prefer parent/group-level consolidated data. If data is ONLY reported
  per subsidiary (not at parent level), you may SUM absolute numbers (headcount, total hours, total
  investment) across subsidiaries to get the parent total — but subtract 0.1 from the confidence score.
  NEVER sum or average percentages, ratios, rates, or per-employee averages across subsidiaries — that
  is mathematically incorrect. If only subsidiary-level percentages exist, OMIT the field.

Return a JSON object with this structure (include only fields that have data):
{{
  "workforce": {{
    "total_employees": <number>,
    "total_employees_male": <number>,
    "total_employees_female": <number>,
    "permanent_employees": <number>,
    "permanent_employees_male": <number>,
    "permanent_employees_female": <number>,
    "temporary_employees": <number>,
    "temporary_employees_male": <number>,
    "temporary_employees_female": <number>,
    "full_time_employees": <number>,
    "full_time_employees_male": <number>,
    "full_time_employees_female": <number>,
    "part_time_employees": <number>,
    "part_time_employees_male": <number>,
    "part_time_employees_female": <number>,
    "contractors": <number>,
    "new_hires": <number>,
    "new_hires_male": <number>,
    "new_hires_female": <number>,
    "turnover_rate_pct": <number>,
    "turnover_voluntary_pct": <number>,
    "turnover_male_pct": <number>,
    "turnover_female_pct": <number>,
    "turnover_under_30_pct": <number>,
    "turnover_30_50_pct": <number>,
    "turnover_over_50_pct": <number>,
    "local_employees_pct": {{ "value": <number>, "unit": "%", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "age_distribution": {{
      "under_30": <number>,
      "under_30_male": <number>,
      "under_30_female": <number>,
      "between_30_50": <number>,
      "between_30_50_male": <number>,
      "between_30_50_female": <number>,
      "over_50": <number>,
      "over_50_male": <number>,
      "over_50_female": <number>
    }}
  }},
  "diversity_inclusion": {{
    "women_total_pct": <number>,
    "women_management_pct": <number>,
    "women_board_pct": <number>,
    "gender_pay_gap_pct": <number>,
    "diversity_policy": "<summary if mentioned>"
  }},
  "parental_leave": {{
    "entitled_male": <number>,
    "entitled_female": <number>,
    "took_leave_male": <number>,
    "took_leave_female": <number>,
    "returned_after_leave_male": <number>,
    "returned_after_leave_female": <number>,
    "return_rate_male_pct": <number>,
    "return_rate_female_pct": <number>
  }},
  "health_safety": {{
    "fatalities": <number>,
    "fatalities_contractors": <number>,
    "ltifr": {{ "value": <number>, "unit": "per million hours worked", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "trir": {{ "value": <number>, "unit": "per million hours worked", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "lost_days": {{ "value": <number>, "unit": "days", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "high_consequence_injuries": <number>,
    "other_occupational_injuries": <number>,
    "near_miss_incidents": <number>,
    "safety_training_hours": <number>
  }},
  "training_development": {{
    "total_training_hours": <number>,
    "avg_training_hours": <number>,
    "training_hours_male": <number>,
    "training_hours_female": <number>,
    "training_investment": {{ "value": <number>, "unit": "<currency>", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1> }},
    "training_investment_per_employee": {{ "value": <number>, "unit": "<currency>", "year": <year>, "confidence": <0-1> }}
  }},
  "performance_reviews": {{
    "total_reviewed_pct": <number>,
    "reviewed_male_pct": <number>,
    "reviewed_female_pct": <number>
  }},
  "human_rights": {{
    "human_rights_policy": <boolean>,
    "grievance_mechanisms": <boolean>,
    "incidents_reported": <number>,
    "due_diligence_process": "<summary if mentioned>"
  }},
  "whistleblowing": {{
    "mechanism_exists": <boolean>,
    "cases_reported": {{ "value": <number>, "unit": "cases", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "cases_resolved": {{ "value": <number>, "unit": "cases", "year": <year>, "confidence": <0-1> }},
    "cases_substantiated": {{ "value": <number>, "unit": "cases", "year": <year>, "confidence": <0-1> }}
  }},
  "community": {{
    "community_investment": {{ "value": <number>, "unit": "<currency>", "year": <year>, "confidence": <0-1> }},
    "local_hiring_pct": <number>
  }}
}}

REPORT CONTENT:
---
{content}
---"""

    elif section == "governance":
        return f"""Extract ALL governance data from this report content.
{metrics_hint}{frameworks_hint}

IMPORTANT RULES:
- CRITICAL YEAR RULE: The "year" field must always be the report's reporting year. If a metric only has
  data for years BEFORE the reporting year, OMIT that field entirely. Do NOT shift years down.
- "confirmed_corruption_incidents" means ONLY confirmed cases of corruption or bribery. Do NOT count
  whistleblowing reports, ethics hotline cases, or general grievance/compliance concerns as corruption
  incidents. Those belong in the social section under whistleblowing.
- SUBSIDIARY DATA: Prefer parent/group-level data. If only subsidiary data exists, you may sum absolute
  numbers (subtract 0.1 from confidence), but NEVER sum percentages or ratios.

Return a JSON object with this structure (include only fields that have data):
{{
  "board_composition": {{
    "board_size": <number>,
    "independent_directors": <number>,
    "independent_pct": <number>,
    "women_on_board": <number>,
    "board_diversity_policy": <boolean>,
    "esg_committee": <boolean>,
    "esg_linked_compensation": <boolean>,
    "esg_compensation_details": "<description of ESG-linked KPIs for executive pay>"
  }},
  "ethics_compliance": {{
    "anti_corruption_policy": <boolean>,
    "corruption_training_pct": <number>,
    "ethics_training_employees": <number>,
    "whistleblower_mechanism": <boolean>,
    "confirmed_corruption_incidents": <number>,
    "political_contributions": <number or null>
  }},
  "data_privacy": {{
    "data_breaches": <number>,
    "customers_affected": <number>,
    "privacy_policy": <boolean>
  }}
}}

REPORT CONTENT:
---
{content}
---"""

    elif section == "report_metadata":
        return f"""Extract report-level metadata from this content.

Return a JSON object:
{{
  "company_name": "<legal or trading name>",
  "company_aliases": ["<any other names, ticker symbol, parent company>"],
  "country": "<headquarters country>",
  "industry_sector": "<sector>",
  "reporting_year": <year>,
  "reporting_period": {{
    "start_date": "<YYYY-MM-DD>",
    "end_date": "<YYYY-MM-DD>"
  }},
  "report_title": "<full title>",
  "frameworks_used": ["GRI", "TCFD", "CSRD_ESRS", "SASB", "CDP", "ISSB_IFRS", "SDG", "GHG_Protocol", "TNFD", "UN_Global_Compact", "ADX_ESG"],
  "assurance_level": "none|limited|reasonable|unknown",
  "assurance_provider": "<firm name or null>",
  "currency": "<ISO 4217 code>",
  "revenue": <number or null>,
  "revenue_unit": "<unit>",
  "employee_count": <number or null>,
  "scope_3_categories_covered": [
    {{ "category_number": <1-15>, "category_name": "<name per GHG Protocol>" }}
  ]
}}

IMPORTANT:
- For reporting_period, look for phrases like "1 January 2024 to 31 December 2024", "for the year ended", "fifteen-month period ended", or similar date ranges.
- For scope_3_categories_covered, list which Scope 3 categories (1-15 per GHG Protocol) the report states it covers or considers relevant. Look for sections describing scope boundaries.
- Extract the ISO currency code if financial figures are presented (e.g., AED, USD, SAR).
- For frameworks_used, include ALL frameworks/standards mentioned (GRI, ADX ESG, TCFD, etc.).

REPORT CONTENT:
---
{content}
---"""

    elif section == "supply_chain":
        return f"""Extract ALL supply chain and procurement sustainability data from this report content.
{metrics_hint}{frameworks_hint}

IMPORTANT RULES:
- "Local" refers to suppliers/spend within the country of operations (e.g., UAE for UAE-based companies).
- Extract both current and prior year values where available.
- CRITICAL YEAR RULE: The "year" field must always be the report's reporting year. If a metric only has
  data for years BEFORE the reporting year, OMIT that field entirely. Do NOT shift years down.
- Sustainable packaging percentage = spend on sustainable packaging / total packaging materials spend.
- Look for ICV (In-Country Value) scores if reported.

Return a JSON object with this structure (include only fields that have data):
{{
  "local_procurement": {{
    "local_spend_pct": {{ "value": <number>, "unit": "%", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "local_suppliers_pct": {{ "value": <number>, "unit": "%", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "icv_score": {{ "value": <number>, "unit": "%", "year": <year>, "confidence": <0-1> }}
  }},
  "sustainable_packaging": {{
    "sustainable_packaging_spend_pct": {{ "value": <number>, "unit": "%", "year": <year>, "prior_year_value": <number>, "prior_year": <year>, "confidence": <0-1>, "page_reference": <page> }},
    "certifications": ["<e.g., RSPO, FSC, Rainforest Alliance>"],
    "certified_materials_pct": {{ "value": <number>, "unit": "%", "year": <year>, "confidence": <0-1> }}
  }},
  "supplier_engagement": {{
    "total_suppliers": <number>,
    "suppliers_screened_pct": <number>,
    "supplier_code_of_conduct": <boolean>,
    "supplier_code_adherence_pct": <number>,
    "supplier_audits": <number>,
    "supplier_risk_assessments": <boolean>
  }}
}}

REPORT CONTENT:
---
{content}
---"""

    else:
        return f"""Extract any ESG-related data from this content as a JSON object.
{content}"""


# ---------------------------------------------------------------------------
# LLM client (Claude API or simulated)
# ---------------------------------------------------------------------------
class LLMClient:
    """Wrapper for Claude API calls with retry logic."""

    def __init__(self, model: str = ExtractorConfig.MODEL, simulate: bool = False):
        self.model = model
        self.simulate = simulate
        self._client = None
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.total_calls = 0

    def _init_client(self):
        if self._client or self.simulate:
            return
        try:
            import anthropic
            self._client = anthropic.Anthropic()  # Uses ANTHROPIC_API_KEY env var
            log.info(f"Claude API client initialized (model: {self.model})")
        except ImportError:
            log.error("anthropic package not installed. pip install anthropic")
            raise

    # Progressive rate-limit backoff: each consecutive 429 adds more wait
    _rate_limit_backoff = [65, 90, 120]

    def extract(self, system_prompt: str, user_prompt: str, max_retries: int = 3) -> dict:
        """Call the LLM and return parsed JSON."""

        if self.simulate:
            return self._simulate_extraction(user_prompt)

        self._init_client()

        for attempt in range(1, max_retries + 1):
            try:
                response = self._client.messages.create(
                    model=self.model,
                    max_tokens=ExtractorConfig.MAX_TOKENS,
                    temperature=ExtractorConfig.TEMPERATURE,
                    system=system_prompt,
                    messages=[{"role": "user", "content": user_prompt}],
                )

                # Track usage
                self.total_calls += 1
                if hasattr(response, 'usage'):
                    self.total_input_tokens += response.usage.input_tokens
                    self.total_output_tokens += response.usage.output_tokens

                # Extract text content
                text = response.content[0].text.strip()

                # Clean up: remove markdown code fences if present
                if text.startswith("```"):
                    text = re.sub(r'^```(?:json)?\s*', '', text)
                    text = re.sub(r'\s*```$', '', text)

                return json.loads(text)

            except json.JSONDecodeError as e:
                log.warning(f"JSON parse failed (attempt {attempt}): {e}")
                if attempt == max_retries:
                    log.error(f"Failed to parse LLM response after {max_retries} attempts")
                    return {}

            except Exception as e:
                is_rate_limit = "429" in str(e) or "rate_limit" in str(e).lower()
                log.warning(f"API call failed (attempt {attempt}): {e}")
                if attempt < max_retries:
                    if is_rate_limit:
                        wait = self._rate_limit_backoff[min(attempt - 1, len(self._rate_limit_backoff) - 1)]
                    else:
                        wait = 2 ** attempt
                    log.info(f"Retrying in {wait}s{'  (rate limit cooldown)' if is_rate_limit else ''}...")
                    time.sleep(wait)
                else:
                    log.error(f"API call failed after {max_retries} attempts: {e}")
                    return {}

        return {}

    def _simulate_extraction(self, user_prompt: str) -> dict:
        """
        Simulate LLM extraction by parsing the report content with regex.
        This lets you test the full pipeline without an API key.
        """
        self.total_calls += 1
        content = user_prompt.split("REPORT CONTENT:\n---\n")[-1].rstrip("\n---")

        # Check only the instruction header (first 200 chars) to determine section
        instruction = user_prompt[:200].lower()
        result = {}

        # --- Environmental simulation ---
        if "environmental" in instruction:
            result = {"ghg_emissions": {}, "energy": {}}

            # Extract Scope 1
            m = re.search(r'Scope 1.*?(\d[\d,]*\.?\d*)\s', content)
            if m:
                val = float(m.group(1).replace(",", ""))
                result["ghg_emissions"]["scope_1"] = {
                    "total": {"value": val, "unit": "tCO2e", "year": 2024, "confidence": 0.95}
                }

            # Extract Scope 2 market-based
            m = re.search(r'Scope 2.*?market.*?(\d[\d,]*\.?\d*)\s', content)
            if m:
                val = float(m.group(1).replace(",", ""))
                result["ghg_emissions"]["scope_2"] = {
                    "market_based": {"value": val, "unit": "tCO2e", "year": 2024, "confidence": 0.95}
                }

            # Extract Scope 2 location-based
            m = re.search(r'Scope 2.*?location.*?(\d[\d,]*\.?\d*)\s', content)
            if m:
                val = float(m.group(1).replace(",", ""))
                result["ghg_emissions"]["scope_2"] = result["ghg_emissions"].get("scope_2", {})
                result["ghg_emissions"]["scope_2"]["location_based"] = {
                    "value": val, "unit": "tCO2e", "year": 2024, "confidence": 0.95
                }

            # Scope 3
            m = re.search(r'Scope 3.*?(\d[\d,]*\.?\d*)\s', content)
            if m:
                val = float(m.group(1).replace(",", ""))
                result["ghg_emissions"]["scope_3"] = {
                    "total": {"value": val, "unit": "tCO2e", "year": 2024, "confidence": 0.90}
                }

            # Total emissions
            m = re.search(r'Total.*?market.*?(\d[\d,]*\.?\d*)\s', content)
            if m:
                val = float(m.group(1).replace(",", ""))
                result["ghg_emissions"]["total_emissions"] = {
                    "value": val, "unit": "tCO2e", "year": 2024, "confidence": 0.95
                }

            # Carbon intensity
            m = re.search(r'Carbon intensity.*?(\d+\.?\d*)', content)
            if m:
                result["ghg_emissions"]["emission_intensity"] = {
                    "value": float(m.group(1)), "unit": "tCO2e/M$ rev", "denominator": "million USD revenue"
                }

            # Energy
            m = re.search(r'Total energy consumption.*?(\d[\d,]*\.?\d*)', content)
            if m:
                result["energy"]["total_consumption"] = {
                    "within_org": {"value": float(m.group(1).replace(",", "")), "unit": "MWh", "year": 2024, "confidence": 0.95}
                }

            m = re.search(r'Renewable energy share.*?(\d+\.?\d*)', content)
            if m:
                result["energy"]["renewable_share_pct"] = float(m.group(1))

            m = re.search(r'Natural gas.*?(\d[\d,]*\.?\d*)', content)
            if m:
                result["energy"]["fuel_consumption"] = {
                    "within_org": {
                        "by_type": [{"type": "natural_gas", "value": {"value": float(m.group(1).replace(",", "")), "unit": "MWh", "year": 2024, "confidence": 0.95}}]
                    }
                }

        # --- Social simulation ---
        elif "social" in instruction:
            result = {"workforce": {}, "diversity_inclusion": {}, "health_safety": {}, "training_development": {}}

            m = re.search(r'Total employees.*?(\d[\d,]*)', content)
            if m:
                result["workforce"]["total_employees"] = int(m.group(1).replace(",", ""))

            m = re.search(r'Employee turnover rate.*?(\d+\.?\d*)', content)
            if m:
                result["workforce"]["turnover_rate_pct"] = float(m.group(1))

            m = re.search(r'Women in workforce.*?(\d+\.?\d*)', content)
            if m:
                result["diversity_inclusion"]["women_total_pct"] = float(m.group(1))

            m = re.search(r'Women in management.*?(\d+\.?\d*)', content)
            if m:
                result["diversity_inclusion"]["women_management_pct"] = float(m.group(1))

            m = re.search(r'LTIFR.*?(\d+\.?\d*)', content)
            if m:
                result["health_safety"]["ltifr"] = float(m.group(1))

            m = re.search(r'Training hours.*?(\d+)', content)
            if m:
                result["training_development"]["avg_training_hours"] = float(m.group(1))

        # --- Governance simulation ---
        elif "governance" in instruction:
            result = {"board_composition": {}, "ethics_compliance": {}}
            if "csrd" in content.lower():
                result["ethics_compliance"]["anti_corruption_policy"] = True
            if "cdp" in content.lower():
                result["board_composition"]["esg_committee"] = True

        # --- Report metadata simulation ---
        elif "metadata" in instruction:
            result = {
                "company_name": "TestCorp International",
                "reporting_year": 2024,
                "report_title": "Sustainability Report 2024",
                "frameworks_used": ["GRI", "TCFD", "SDG"],
                "assurance_level": "unknown",
                "employee_count": 8450,
            }
            m = re.search(r'(\d[\d,]+)\s*employees', content)
            if m:
                result["employee_count"] = int(m.group(1).replace(",", ""))

        return result

    def extract_from_image(self, image_path: str, context: str = "") -> dict:
        """
        Send an image to Gemini 2.5 Flash to extract ESG data points.
        Falls back to Claude Vision if GOOGLE_API_KEY is not set.
        Returns parsed JSON with any ESG metrics found in the image.
        """
        if self.simulate:
            self.total_calls += 1
            return {"image_source": image_path, "data_points": []}

        google_key = os.environ.get("GOOGLE_API_KEY")
        if google_key:
            return self._extract_image_gemini(image_path, context, google_key)
        else:
            return self._extract_image_claude(image_path, context)

    def _extract_image_gemini(self, image_path: str, context: str, api_key: str) -> dict:
        """Extract ESG data from image using Gemini 2.5 Flash."""
        import urllib.request

        try:
            with open(image_path, "rb") as f:
                image_data = base64.standard_b64encode(f.read()).decode("utf-8")

            ext = Path(image_path).suffix.lower()
            media_type = {
                ".png": "image/png",
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".gif": "image/gif",
                ".webp": "image/webp",
            }.get(ext, "image/png")

            context_hint = ""
            if context:
                context_hint = f"\n\nAdditional context from the report: {context}"

            prompt_text = f"""You are analyzing a page from a corporate sustainability/ESG report.{context_hint}

PRIORITY 1 — CHARTS AND GRAPHS (extract these FIRST):
Look for bar charts, line charts, and pie charts on this page. Many pages have BOTH text and charts —
you MUST extract the numeric data from the charts, not just the surrounding text.
- For BAR CHARTS: Extract the numeric value of EACH bar. Read the y-axis scale carefully.
  Include the label (e.g., "Scope 1", "Scope 2", "Hazardous Waste") and the year for each bar.
  If values are written on/above the bars, use those exact numbers.
- For GHG/emissions charts: Extract Scope 1, Scope 2, Scope 3, and Total emissions separately with their year.
- For water charts: Extract total consumption/withdrawal values with year.
- For waste charts: Extract total waste, hazardous waste, and non-hazardous waste separately with year.
- For energy charts: Extract total energy consumption, electricity, fuel values with year.
- For intensity metrics: Include the FULL unit with denominator (e.g., "kg CO2e per patient", "kWh per m2", "m3 per employee").
- Extract ALL years shown (e.g., if chart shows 2023, 2024, 2025 bars, extract values for each year separately).

PRIORITY 2 — TEXT AND INFOGRAPHICS:
After extracting all chart data, also extract any ESG metrics from text, tables, and infographics on the page.

Return a JSON object with this structure:
{{
  "data_points": [
    {{
      "metric_name": "<name of the metric>",
      "value": <numeric value or string>,
      "unit": "<unit of measurement — include denominator for intensity metrics>",
      "year": <year if visible>,
      "category": "environmental|social|governance",
      "confidence": <0.0-1.0>,
      "source_type": "chart|infographic|diagram|table_in_image|text"
    }}
  ],
  "chart_type": "<bar|pie|line|infographic|diagram|other>",
  "description": "<brief description of what the image shows>"
}}

If no ESG data is found, return {{"data_points": [], "description": "<what the image shows>"}}.
Respond with ONLY valid JSON. No markdown fences."""

            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
            body = json.dumps({
                "contents": [{"parts": [
                    {"inline_data": {"mime_type": media_type, "data": image_data}},
                    {"text": prompt_text},
                ]}],
                "generationConfig": {"temperature": 0.0},
            }).encode("utf-8")

            req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
            resp = urllib.request.urlopen(req, timeout=60)
            result = json.loads(resp.read().decode("utf-8"))

            self.total_calls += 1
            # Track Gemini token usage if available
            usage = result.get("usageMetadata", {})
            self.total_input_tokens += usage.get("promptTokenCount", 0)
            self.total_output_tokens += usage.get("candidatesTokenCount", 0)

            text = result["candidates"][0]["content"]["parts"][0]["text"].strip()
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\s*', '', text)
                text = re.sub(r'\s*```$', '', text)

            return json.loads(text)

        except FileNotFoundError:
            log.warning(f"Image not found: {image_path}")
            return {"data_points": []}
        except json.JSONDecodeError as e:
            log.warning(f"Gemini vision JSON parse failed for {image_path}: {e}")
            return {"data_points": []}
        except Exception as e:
            log.warning(f"Gemini vision extraction failed for {image_path}: {e}")
            return {"data_points": []}

    def _extract_image_claude(self, image_path: str, context: str) -> dict:
        """Fallback: Extract ESG data from image using Claude Vision API."""
        if os.environ.get("DISABLE_CLAUDE_VISION"):
            log.warning(f"Claude Vision disabled, skipping {image_path}")
            return {"data_points": []}
        self._init_client()

        try:
            with open(image_path, "rb") as f:
                image_data = base64.standard_b64encode(f.read()).decode("utf-8")

            ext = Path(image_path).suffix.lower()
            media_type = {
                ".png": "image/png",
                ".jpg": "image/jpeg",
                ".jpeg": "image/jpeg",
                ".gif": "image/gif",
                ".webp": "image/webp",
            }.get(ext, "image/png")

            context_hint = ""
            if context:
                context_hint = f"\n\nAdditional context from the report: {context}"

            user_content = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": media_type,
                        "data": image_data,
                    },
                },
                {
                    "type": "text",
                    "text": f"""Extract ALL ESG/sustainability data points visible in this image.
This image is from a corporate sustainability/ESG report.{context_hint}

Return a JSON object with this structure:
{{
  "data_points": [
    {{
      "metric_name": "<name of the metric>",
      "value": <numeric value or string>,
      "unit": "<unit of measurement>",
      "year": <year if visible>,
      "category": "environmental|social|governance",
      "confidence": <0.0-1.0>,
      "source_type": "chart|infographic|diagram|table_in_image"
    }}
  ],
  "chart_type": "<bar|pie|line|infographic|diagram|other>",
  "description": "<brief description of what the image shows>"
}}

If no ESG data is found, return {{"data_points": [], "description": "<what the image shows>"}}.
Respond with ONLY valid JSON.""",
                },
            ]

            response = self._client.messages.create(
                model=self.model,
                max_tokens=ExtractorConfig.MAX_TOKENS,
                temperature=ExtractorConfig.TEMPERATURE,
                system="You are an expert ESG data analyst extracting sustainability metrics from report images.",
                messages=[{"role": "user", "content": user_content}],
            )

            self.total_calls += 1
            if hasattr(response, 'usage'):
                self.total_input_tokens += response.usage.input_tokens
                self.total_output_tokens += response.usage.output_tokens

            text = response.content[0].text.strip()
            if text.startswith("```"):
                text = re.sub(r'^```(?:json)?\s*', '', text)
                text = re.sub(r'\s*```$', '', text)

            return json.loads(text)

        except FileNotFoundError:
            log.warning(f"Image not found: {image_path}")
            return {"data_points": []}
        except json.JSONDecodeError as e:
            log.warning(f"Vision extraction JSON parse failed for {image_path}: {e}")
            return {"data_points": []}
        except Exception as e:
            log.warning(f"Vision extraction failed for {image_path}: {e}")
            return {"data_points": []}

    def get_usage_summary(self) -> dict:
        return {
            "total_calls": self.total_calls,
            "total_input_tokens": self.total_input_tokens,
            "total_output_tokens": self.total_output_tokens,
        }


# ---------------------------------------------------------------------------
# Validator — checks extracted data for anomalies
# ---------------------------------------------------------------------------
class ESGValidator:
    """Post-extraction validation and review flagging."""

    # Physical plausibility ranges for common metrics
    RANGES = {
        "scope_1_total": (0, 500_000_000),          # tCO2e
        "scope_2_total": (0, 200_000_000),
        "scope_3_total": (0, 2_000_000_000),
        "renewable_share_pct": (0, 100),
        "women_total_pct": (0, 100),
        "women_management_pct": (0, 100),
        "turnover_rate_pct": (0, 100),
        "ltifr": (0, 100),
        "recycling_rate_pct": (0, 100),
        "local_spend_pct": (0, 100),
        "local_suppliers_pct": (0, 100),
        "sustainable_packaging_spend_pct": (0, 100),
        "local_employees_pct": (0, 100),
        "return_rate_male_pct": (0, 100),
        "return_rate_female_pct": (0, 100),
        "total_reviewed_pct": (0, 100),
        "reviewed_male_pct": (0, 100),
        "reviewed_female_pct": (0, 100),
        "near_miss_incidents": (0, 1_000_000),
    }

    def validate(self, data: dict) -> list[dict]:
        """
        Validate extracted data and return a list of review flags.
        Each flag: { field, issue, severity, value }
        """
        flags = []

        # Check confidence scores
        flags.extend(self._check_confidence(data, ""))

        # Check physical plausibility
        flags.extend(self._check_ranges(data))

        # Cross-reference checks
        flags.extend(self._check_cross_refs(data))

        return flags

    def _check_confidence(self, obj: dict, path: str) -> list[dict]:
        """Flag any values with low confidence."""
        flags = []
        if isinstance(obj, dict):
            if "confidence" in obj and "value" in obj:
                if obj["confidence"] < ExtractorConfig.REVIEW_THRESHOLD:
                    flags.append({
                        "field": path,
                        "issue": f"Low confidence ({obj['confidence']:.2f})",
                        "severity": "warning",
                        "value": obj["value"],
                    })
            for key, val in obj.items():
                flags.extend(self._check_confidence(val, f"{path}.{key}" if path else key))
        return flags

    def _check_ranges(self, data: dict) -> list[dict]:
        """Check if values fall within plausible physical ranges."""
        flags = []

        # Helper to safely get nested values
        def get_val(d, *keys):
            for k in keys:
                if isinstance(d, dict) and k in d:
                    d = d[k]
                else:
                    return None
            return d

        checks = [
            ("renewable_share_pct", get_val(data, "environmental", "energy", "renewable_share_pct")),
            ("women_total_pct", get_val(data, "social", "diversity_inclusion", "women_total_pct")),
            ("women_management_pct", get_val(data, "social", "diversity_inclusion", "women_management_pct")),
            ("turnover_rate_pct", get_val(data, "social", "workforce", "turnover_rate_pct")),
            ("ltifr", get_val(data, "social", "health_safety", "ltifr")),
            ("recycling_rate_pct", get_val(data, "environmental", "waste", "recycling_rate_pct")),
            ("local_spend_pct", get_val(data, "supply_chain", "local_procurement", "local_spend_pct")),
            ("local_suppliers_pct", get_val(data, "supply_chain", "local_procurement", "local_suppliers_pct")),
            ("sustainable_packaging_spend_pct", get_val(data, "supply_chain", "sustainable_packaging", "sustainable_packaging_spend_pct")),
            ("local_employees_pct", get_val(data, "social", "workforce", "local_employees_pct")),
            ("total_reviewed_pct", get_val(data, "social", "performance_reviews", "total_reviewed_pct")),
        ]

        for name, val in checks:
            if val is not None and name in self.RANGES:
                # Unwrap metric_value dicts (e.g. {"value": 50, "unit": "%"})
                if isinstance(val, dict):
                    val = val.get("value")
                if val is None or not isinstance(val, (int, float)):
                    continue
                lo, hi = self.RANGES[name]
                if not (lo <= val <= hi):
                    flags.append({
                        "field": name,
                        "issue": f"Value {val} outside plausible range [{lo}, {hi}]",
                        "severity": "error",
                        "value": val,
                    })

        return flags

    def _check_cross_refs(self, data: dict) -> list[dict]:
        """Cross-reference checks (e.g., Scope 1 + 2 + 3 ≈ Total)."""
        flags = []

        env = data.get("environmental", {})
        ghg = env.get("ghg_emissions", {})

        # Check Scope 1 + 2 (market) + 3 ≈ Total
        s1 = self._extract_value(ghg.get("scope_1", {}).get("total"))
        s2 = self._extract_value(ghg.get("scope_2", {}).get("market_based"))
        s3 = self._extract_value(ghg.get("scope_3", {}).get("total"))
        total = self._extract_value(ghg.get("total_emissions"))

        if all(v is not None for v in [s1, s2, s3, total]):
            calculated = s1 + s2 + s3
            if total > 0:
                diff_pct = abs(calculated - total) / total * 100
                if diff_pct > 5:
                    flags.append({
                        "field": "ghg_emissions.total_emissions",
                        "issue": f"Scope 1+2+3 ({calculated:,.0f}) differs from reported total ({total:,.0f}) by {diff_pct:.1f}%",
                        "severity": "warning",
                        "value": total,
                    })

        # Check permanent + temporary ≈ total employees
        soc = data.get("social", {})
        wf = soc.get("workforce", {})
        total_emp = self._extract_value(wf.get("total_employees"))
        perm = self._extract_value(wf.get("permanent_employees"))
        temp = self._extract_value(wf.get("temporary_employees"))

        if all(v is not None for v in [total_emp, perm, temp]):
            calc_total = perm + temp
            if total_emp > 0:
                diff_pct = abs(calc_total - total_emp) / total_emp * 100
                if diff_pct > 5:
                    flags.append({
                        "field": "workforce.total_employees",
                        "issue": f"Permanent ({perm:,.0f}) + Temporary ({temp:,.0f}) = {calc_total:,.0f} differs from total ({total_emp:,.0f}) by {diff_pct:.1f}%",
                        "severity": "warning",
                        "value": total_emp,
                    })

        return flags

    def _extract_value(self, obj) -> Optional[float]:
        if isinstance(obj, dict) and "value" in obj:
            return obj["value"]
        if isinstance(obj, (int, float)):
            return float(obj)
        return None


# ---------------------------------------------------------------------------
# Main extraction orchestrator
# ---------------------------------------------------------------------------
class ESGExtractor:
    """Orchestrates the full extraction pipeline for one document."""

    SECTIONS = ["report_metadata", "environmental", "social", "governance", "supply_chain"]

    # Maps Vision API metric_name patterns (regex, case-insensitive) to
    # dot-paths in the structured result.  Order matters: first match wins,
    # so put more specific patterns (e.g. "female") before broader ones
    # (e.g. "male") to avoid "female" matching the male rule.
    VISION_METRIC_MAP = [
        # --- Environmental: Energy ---
        (r"total energy consumption increase",          None),  # derived %, skip
        (r"energy.*outside.*org",                       "environmental.energy.total_consumption.outside_org"),
        (r"total energy consumption|total.*energy.*consum",
                                                        "environmental.energy.total_consumption.within_org"),
        (r"fuel consum",                                "environmental.energy.fuel_consumption.within_org.total"),
        (r"natural gas\b",                              "environmental.energy.fuel_consumption.within_org.total"),
        (r"diesel consum|petrol consum|gasoline consum|lpg consum",
                                                        "environmental.energy.fuel_consumption.within_org.total"),
        (r"total electricity|electricity consum",       "environmental.energy.electricity_consumption.within_org.total"),
        (r"grid electricity",                           "environmental.energy.electricity_consumption.within_org.total"),
        (r"renewable energy|solar energy|clean energy", "environmental.energy.electricity_consumption.within_org.total"),
        (r"energy intensity",                           "environmental.energy.energy_intensity"),
        (r"energy.*efficien|energy.*saving|energy.*reduc",
                                                        "environmental.energy.energy_reduction_initiatives"),

        # --- Environmental: Emission reductions / targets / percentages (SKIP — not actual emissions) ---
        (r"emission.*reduc|reduc.*emission|abat.*emission|emission.*abat|ghg.*reduc.*result|reduc.*abatement",
                                                        None),
        (r"scope.*target|scope.*offset|net zero.*target|net zero.*scope|decarboni",
                                                        None),
        (r"scope \d.*upstream|scope \d.*downstream|scope \d.*purchas|scope \d.*capital|scope \d.*fuel.*energy|scope \d.*invest|scope \d.*use of",
                                                        None),

        # --- Environmental: Emissions ---
        (r"scope 1(?!.*2)(?!.*3)",                      "environmental.ghg_emissions.scope_1.total"),
        (r"scope 2.*location",                          "environmental.ghg_emissions.scope_2.location_based"),
        (r"scope 2.*market",                            "environmental.ghg_emissions.scope_2.market_based"),
        (r"scope 2(?!.*location|.*market)",             "environmental.ghg_emissions.scope_2.location_based"),
        (r"scope 3.*ghg.*emission|scope 3.*total.*emission|^scope 3 emission",
                                                        "environmental.ghg_emissions.scope_3.total"),
        (r"total.*ghg|total.*emission|total.*co2|carbon.*footprint",
                                                        "environmental.ghg_emissions.total_emissions"),
        (r"emission.*intensity|carbon.*intensity|ghg.*intensity",
                                                        "environmental.ghg_emissions.emission_intensity"),
        (r"financed emission",                          "environmental.ghg_emissions.financed_emissions"),

        # --- Environmental: Water/Waste reduction targets (SKIP — not actual values) ---
        (r"water.*(?:reduc|sav|target|goal|offset|conserv).*(?:consum|usage|volume|m³|m3)",
                                                        None),
        (r"(?:reduc|sav|target|goal|offset|conserv).*water",
                                                        None),
        (r"waste.*(?:reduc|recycl|divert|recover|segregat).*(?:percent|%|\bpct\b)",
                                                        None),
        (r"(?:percent|%).*waste.*(?:recycl|divert|recover|incinerat)",
                                                        None),

        # --- Environmental: Water ---
        (r"total water.*consumption|water consum|water.*usage|water.*use\b",
                                                        "environmental.water.total_consumption"),
        (r"water withdrawal|water.*withdraw",           "environmental.water.total_withdrawal"),
        (r"water discharge",                            "environmental.water.total_discharge"),
        (r"water recycl|water.*reuse|water.*reclaim",    "environmental.water.water_recycled"),

        # --- Environmental: Waste ---
        (r"total waste|waste generat|total.*waste.*generat", "environmental.waste.total_waste"),
        (r"medical waste(?!.*inciner|.*safe)",            "environmental.waste.hazardous_waste"),
        (r"hazardous waste",                            "environmental.waste.hazardous_waste"),
        (r"non.hazardous waste",                        "environmental.waste.non_hazardous_waste"),
        (r"organic waste|food waste",                   "environmental.waste.organic_waste"),
        (r"e.waste|electronic waste",                   "environmental.waste.electronic_waste"),
        (r"waste.*landfill",                            "environmental.waste.waste_to_landfill"),
        (r"waste.*divert|waste.*recov",                 "environmental.waste.waste_diverted_from_disposal"),
        (r"recycling rate|diversion rate",              "environmental.waste.recycling_rate_pct"),
        (r"paper.*consum|paper.*usage",                 "environmental.waste.paper_consumption"),

        # --- Environmental: Air Emissions ---
        (r"nox\b|nitrogen oxide",                       "environmental.air_emissions.nox"),
        (r"sox\b|sulfur oxide|sulphur oxide|so2\b",     "environmental.air_emissions.sox"),
        (r"\bvoc\b|volatile organic",                   "environmental.air_emissions.voc"),
        (r"particulate matter|\bpm\b.*emission|pm10|pm2\.5",
                                                        "environmental.air_emissions.particulate_matter"),
        (r"carbon monoxide|\bco\b.*emission",           "environmental.air_emissions.co"),
        (r"\bh2s\b|hydrogen sulfide",                   "environmental.air_emissions.h2s"),
        (r"ozone.deplet|ods\b",                         "environmental.air_emissions.ods"),
        (r"flaring|flare.*volume|flare.*gas",           "environmental.air_emissions.flaring"),
        (r"methane.*emission|ch4.*emission|fugitive.*emission|methane.*leak",
                                                        "environmental.air_emissions.methane"),

        # --- Social: Parental Leave ---
        (r"parental leave.*female|maternity leave",     "social.parental_leave.took_leave_female"),
        (r"parental leave.*male|paternity leave",       "social.parental_leave.took_leave_male"),

        # --- Social: Performance Reviews ---
        (r"performance.*review.*female",                "social.performance_reviews.reviewed_female_count"),
        (r"performance.*review.*male",                  "social.performance_reviews.reviewed_male_count"),
        (r"performance.*review.*entry",                 "social.performance_reviews.reviewed_entry_level"),
        (r"performance.*review.*mid",                   "social.performance_reviews.reviewed_mid_level"),
        (r"performance.*review.*(senior|executive)",    "social.performance_reviews.reviewed_senior_level"),
        (r"performance.*review.*total|total.*review.*pct|employees.*receiv.*review",
                                                        "social.performance_reviews.total_reviewed_pct"),

        # --- Social: Training ---
        (r"training hours.*female|total training.*female|learning hours.*female",
                                                        "social.training_development.training_hours_female"),
        (r"training hours.*male|total training.*male|learning hours.*male",
                                                        "social.training_development.training_hours_male"),
        (r"training hours.*entry",                      "social.training_development.training_hours_entry_level"),
        (r"training hours.*mid",                        "social.training_development.training_hours_mid_level"),
        (r"training hours.*(senior|executive)",         "social.training_development.training_hours_senior_level"),
        (r"avg.*training|average.*training.*hour",      "social.training_development.avg_training_hours"),
        (r"company.*wide training|total.*l&d|total learning.*development|total.*training hour|employee learning hours",
                                                        "social.training_development.total_training_hours"),
        (r"amount spent.*training|training.*investment|training.*spend|training.*cost|training.*budget",
                                                        "social.training_development.training_investment"),

        # --- Social: Health & Safety ---
        (r"ltifr|lost.time injury freq",                "social.health_safety.ltifr"),
        (r"work.?days lost|lost days|days lost.*injury","social.health_safety.lost_days"),
        (r"other occupational injur",                   "social.health_safety.other_occupational_injuries"),
        (r"work.related injur|workplace injur|injur.*report|total.*injur",
                                                        "social.health_safety.work_related_injuries"),
        (r"near.miss",                                  "social.health_safety.near_miss_incidents"),
        (r"fatalit",                                    "social.health_safety.fatalities"),
        (r"total recordable.*rate|trir",                "social.health_safety.trir"),
        (r"safety.*training",                           "social.health_safety.safety_training_hours"),

        # --- Social: Workforce ---
        (r"gender.*distribut.*female|employee.*distribut.*female|total employee.*female|female.*headcount|female.*employee.*\d|women.*workforce",
                                                        "social.workforce.total_employees_female"),
        (r"gender.*distribut.*male|employee.*distribut.*male|total employee.*male|male.*headcount|male.*employee.*\d|men.*workforce",
                                                        "social.workforce.total_employees_male"),
        (r"total.*employee.*count|total.*headcount|total.*workforce.*size",
                                                        "social.workforce.total_employees"),
        (r"age.*distribut.*under.?25|age.*distribut.*<.?25|under.?25.*year",
                                                        "social.workforce.age_distribution.under_30"),
        (r"age.*distribut.*25.?[-–].?34",               "social.workforce.age_distribution.between_25_34"),
        (r"age.*distribut.*35.?[-–].?45",               "social.workforce.age_distribution.between_35_45"),
        (r"age.*distribut.*(over.?45|>.?45|above.?45|45.?[-–])",
                                                        "social.workforce.age_distribution.over_45"),
        (r"new hire.*female|female.*new hire",          "social.workforce.new_hires_female"),
        (r"new hire.*male|male.*new hire",              "social.workforce.new_hires_male"),
        (r"total new hire|new hire.*total|new employee","social.workforce.new_hires_total"),
        (r"turnover.*female|attrition.*female",         "social.workforce.turnover_female"),
        (r"turnover.*male|attrition.*male",             "social.workforce.turnover_male"),
        (r"total.*turnover|turnover.*total|employee turnover|attrition rate|turnover rate",
                                                        "social.workforce.turnover_rate"),
        (r"nationali.*represented|nationali.*count",    "social.workforce.nationalities_count"),
        (r"emirat.*rate|emirat.*represent|emirat.*national|nationali[sz]ation|local.*national",
                                                        "social.workforce.local_employees_pct"),
        (r"gender.?pay|pay.*gap|pay.*ratio|pay.*equity","social.diversity_inclusion.gender_pay_ratio"),
        (r"grievance.*filed|grievance.*report|grievance.*resolved",
                                                        "social.grievances_reported"),

        # --- Social: Customer ---
        (r"net promoter|nps\b",                         "social.customer_nps"),
        (r"customer satisfaction|csat\b",               "social.customer_satisfaction_score"),
        (r"customer complaint",                         "social.customer_complaints"),

        # --- Governance ---
        (r"board.*size|board.*member|number.*director", "governance.board_composition.board_size"),
        (r"independent.*director|independent.*pct",     "governance.board_composition.independent_pct"),
        (r"women.*board|female.*board|board.*female|board.*gender|board.*divers",
                                                        "governance.board_composition.women_on_board_pct"),
        (r"ethics.*training|compliance.*training|anti.*corruption.*training",
                                                        "governance.ethics_compliance.ethics_training_employees"),
        (r"data.*breach|cyber.*incident|information.*security.*incident",
                                                        "governance.data_privacy.data_breaches"),
        (r"whistleblow",                                "governance.ethics_compliance.whistleblower_cases"),

        # --- Supply Chain ---
        (r"local.*spend|local.*procur|local.*supplier.*pct|local.*sourc",
                                                        "supply_chain.local_procurement.local_spend_pct"),
        (r"sustain.*procur|green.*procur|esg.*supplier|supplier.*esg",
                                                        "supply_chain.sustainable_procurement"),

        # --- Sustainable Finance (banking-specific) ---
        (r"green bond|green.*bond.*issued",             "environmental.sustainable_finance.green_bonds"),
        (r"sustainable finance.*portf|sustainable financ.*total|sustainable.*financ.*asset",
                                                        "environmental.sustainable_finance.total_portfolio"),
        (r"sustainable finance.*lend|green.*lend",      "environmental.sustainable_finance.lending"),
        (r"sustainable finance.*invest|esg.*invest",    "environmental.sustainable_finance.investments"),
        (r"sustainable finance.*facilit",               "environmental.sustainable_finance.facilitation"),
        (r"sustainable finance.*target|sustainable finance.*commit",
                                                        "environmental.sustainable_finance.target"),
    ]

    # Keywords to match parsed section filenames to extraction sections.
    # Each extraction section gets content from section files whose names
    # contain any of the listed keywords.
    SECTION_KEYWORDS = {
        "report_metadata": [
            "introduction", "about", "alignment", "scope", "2024", "2023",
            "table_of_contents", "report",
        ],
        "environmental": [
            "environment", "stewardship", "energy", "emission", "water",
            "waste", "climate", "sustainable_meals", "footprint", "plastic",
            "paperless",
        ],
        "supply_chain": [
            "supply_chain", "supplier", "packaging", "procurement",
            "spend_on_local", "palm_oil", "resilient_supply",
        ],
        "social": [
            "society", "catalyst", "employee", "health_safety", "training",
            "customer", "workforce", "parental", "diversity", "occupational",
            "safety", "people", "human_rights", "labour", "labor",
            "performance_review", "local_employee",
        ],
        "governance": [
            "governance", "board", "committee", "conduct", "ethics",
            "risk_management", "continuity", "compliance", "audit",
            "remuneration", "nomination",
        ],
    }

    def __init__(self, llm: LLMClient, config: ExtractorConfig = None):
        self.llm = llm
        self.config = config or ExtractorConfig()
        self.validator = ESGValidator()

    def extract_from_folder(self, input_dir: str, task_id: int = None) -> dict:
        """
        Run full extraction on a parsed output folder.
        Returns the complete ESG data JSON.
        """
        input_path = Path(input_dir)

        # Load parsed content
        md_path = input_path / "full.md"
        meta_path = input_path / "meta.json"

        if not md_path.exists():
            raise FileNotFoundError(f"No full.md found in {input_dir}")

        content = md_path.read_text(encoding="utf-8")
        meta = {}
        if meta_path.exists():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))

        detected_metrics = meta.get("detected_metrics", [])
        detected_frameworks = meta.get("detected_frameworks", [])
        file_name = meta.get("file_name", input_path.name)

        log.info(f"Extracting ESG data from: {file_name}")
        log.info(f"  Content length: {len(content):,} chars")
        log.info(f"  Detected metrics: {len(detected_metrics)}")
        log.info(f"  Detected frameworks: {len(detected_frameworks)}")

        # Load any table CSVs for context
        tables_content = self._load_tables(input_path)
        if tables_content:
            content += "\n\n--- EXTRACTED TABLES ---\n" + tables_content
            log.info(f"  Appended {len(tables_content):,} chars of table data")

        # Load chart CSVs (data extracted from bar/pie/line charts)
        charts_content = self._load_charts(input_path)
        if charts_content:
            content += "\n\n--- EXTRACTED CHART DATA ---\n" + charts_content
            log.info(f"  Appended {len(charts_content):,} chars of chart data")

        # Extract each section
        full_result = {}
        extraction_start = time.time()

        for section in self.SECTIONS:
            log.info(f"  Extracting: {section}...")

            # Use section-aware content routing: send only relevant
            # parsed sections to each extraction prompt
            section_content = self._build_section_content(input_path, section)
            if not section_content:
                # Fallback to full content (truncated) if no section files
                section_content = content[:self.config.MAX_CHUNK_CHARS]
                log.info(f"    No section files matched, using full content fallback")

            prompt = build_extraction_prompt(
                section=section,
                content=section_content,
                detected_metrics=detected_metrics,
                detected_frameworks=detected_frameworks,
            )

            section_data = self.llm.extract(SYSTEM_PROMPT, prompt)
            full_result[section] = section_data

            fields_found = self._count_fields(section_data)
            log.info(f"    -> {fields_found} fields extracted")

        # --- Vision LLM extraction for chart images ---
        image_data_points = self._extract_from_images(input_path)
        vision_merged = 0
        vision_unmapped = []
        if image_data_points:
            log.info(f"  Vision extraction: {len(image_data_points)} data points from images")

            # Merge Vision data into the structured sections
            reporting_year = (
                (full_result.get("report_metadata") or {}).get("reporting_year")
                or meta.get("reporting_year")
            )
            if reporting_year:
                vision_merged, vision_unmapped = self._merge_vision_data(
                    full_result, image_data_points, reporting_year
                )
                log.info(
                    f"  Vision merge: {vision_merged} fields merged, "
                    f"{len(vision_unmapped)} unmapped"
                )
            else:
                log.warning("  Cannot merge Vision data: reporting_year unknown")
                vision_unmapped = image_data_points

            # Keep unmapped data points for review (much smaller than full list)
            if vision_unmapped:
                full_result["vision_unmapped"] = vision_unmapped

        # Post-merge: validate waste totals
        self._fix_waste_totals(full_result)

        extraction_time = time.time() - extraction_start

        # Add extraction metadata
        full_result["extraction_metadata"] = {
            "extraction_date": datetime.now(timezone.utc).isoformat(),
            "extraction_model": self.llm.model if not self.llm.simulate else "simulated",
            "extraction_version": "1.0.0",
            "source_pdf": file_name,
            "source_pages": meta.get("page_count", 0),
            "extraction_time_seconds": round(extraction_time, 2),
            "fields_extracted": self._count_fields(full_result),
            "image_data_points": len(image_data_points),
            "vision_merged": vision_merged,
            "vision_unmapped": len(vision_unmapped),
            "human_reviewed": False,
        }

        # Validate
        flags = self.validator.validate(full_result)
        if flags:
            full_result["extraction_metadata"]["review_flags"] = flags
            full_result["extraction_metadata"]["needs_review"] = True
            log.info(f"  Validation: {len(flags)} flag(s)")
            for f in flags:
                log.info(f"    [{f['severity']}] {f['field']}: {f['issue']}")
        else:
            full_result["extraction_metadata"]["needs_review"] = False
            log.info(f"  Validation: passed (no flags)")

        log.info(
            f"  Extraction complete: {self._count_fields(full_result)} total fields "
            f"in {extraction_time:.1f}s"
        )

        return full_result

    def _build_section_content(self, input_path: Path, section: str) -> str:
        """
        Build targeted content for a specific extraction section by loading
        only the relevant parsed section files (keyword-matched).
        Falls back to full.md if no section files exist.
        """
        sections_dir = input_path / "sections"
        if not sections_dir.exists():
            return ""

        keywords = self.SECTION_KEYWORDS.get(section, [])
        if not keywords:
            return ""

        matched_files = []
        for md_file in sorted(sections_dir.glob("*.md")):
            name_lower = md_file.stem.lower()
            # Strip leading numeric prefix (e.g., "38_climate_change..." -> "climate_change...")
            name_stripped = "_".join(name_lower.split("_")[1:]) if "_" in name_lower else name_lower
            if any(kw in name_stripped for kw in keywords):
                matched_files.append(md_file)

        if not matched_files:
            return ""

        parts = []
        for f in matched_files:
            try:
                text = f.read_text(encoding="utf-8").strip()
                if text:
                    parts.append(text)
            except Exception:
                pass

        content = "\n\n".join(parts)

        # Also append relevant table CSVs
        tables_content = self._load_tables(input_path)
        if tables_content:
            content += "\n\n--- EXTRACTED TABLES ---\n" + tables_content

        # Token-aware trimming: cap at ~60K chars (~15K tokens) to stay within
        # rate limits. Prioritize tables over narrative by trimming from the top
        # (section text) rather than the bottom (tables).
        max_chars = 60_000
        if len(content) > max_chars:
            tables_marker = content.find("--- EXTRACTED TABLES ---")
            if tables_marker > 0:
                tables_part = content[tables_marker:]
                text_budget = max_chars - len(tables_part)
                if text_budget > 0:
                    content = content[:text_budget] + "\n\n" + tables_part
                else:
                    content = tables_part[:max_chars]
            else:
                content = content[:max_chars]
            log.info(f"    Trimmed section content to {len(content):,} chars (was over {max_chars:,})")

        log.info(f"    Section content for '{section}': {len(content):,} chars from {len(matched_files)} section files")
        return content

    def _load_tables(self, input_path: Path) -> str:
        """Load extracted table CSVs and format as text for the LLM."""
        tables_dir = input_path / "tables"
        if not tables_dir.exists():
            return ""

        parts = []
        for csv_path in sorted(tables_dir.glob("*.csv")):
            try:
                text = csv_path.read_text(encoding="utf-8")
                parts.append(f"\n[Table: {csv_path.stem}]\n{text}")
            except Exception:
                pass

        return "\n".join(parts)

    def _load_charts(self, input_path: Path) -> str:
        """Load chart CSVs extracted by Docling's chart extraction."""
        charts_dir = input_path / "charts"
        if not charts_dir.exists():
            return ""

        parts = []

        # Load chart index for context
        index_path = charts_dir / "charts_index.json"
        chart_meta = {}
        if index_path.exists():
            try:
                chart_list = json.loads(index_path.read_text(encoding="utf-8"))
                chart_meta = {f"chart_{c['index']:02d}": c for c in chart_list}
            except Exception:
                pass

        for csv_path in sorted(charts_dir.glob("*.csv")):
            try:
                text = csv_path.read_text(encoding="utf-8")
                meta = chart_meta.get(csv_path.stem, {})
                chart_type = meta.get("chart_type", "unknown")
                caption = meta.get("caption", "")
                header = f"\n[Chart: {csv_path.stem} (type: {chart_type})]"
                if caption:
                    header += f"\nCaption: {caption}"
                parts.append(f"{header}\n{text}")
            except Exception:
                pass

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Vision data merge
    # ------------------------------------------------------------------
    def _merge_vision_data(
        self, full_result: dict, data_points: list[dict], reporting_year: int
    ) -> tuple[int, list[dict]]:
        """
        Merge flat Vision API data points into the structured ESG sections.

        Strategy:
        - Map each data point's metric_name to a schema path via VISION_METRIC_MAP
        - Group current-year vs prior-year values
        - Fill missing fields; add prior_year_value where absent
        - Never overwrite existing values that have equal or higher confidence

        Returns (merged_count, unmapped_points).
        """
        merged_count = 0
        unmapped = []

        # Group data points by (schema_path, year) for prior-year pairing
        path_groups: dict[str, list[dict]] = {}

        for dp in data_points:
            name = (dp.get("metric_name") or "").lower()
            schema_path = None

            for pattern, path in self.VISION_METRIC_MAP:
                if path is None:
                    # Explicitly skipped metric (derived %, etc.)
                    if re.search(pattern, name, re.IGNORECASE):
                        schema_path = "__skip__"
                        break
                    continue
                if re.search(pattern, name, re.IGNORECASE):
                    schema_path = path
                    break

            if schema_path is None:
                unmapped.append(dp)
                continue
            if schema_path == "__skip__":
                continue

            path_groups.setdefault(schema_path, []).append(dp)

        # Process each mapped group
        for schema_path, points in path_groups.items():
            parts = schema_path.split(".")
            current_year_dp = None
            prior_year_dp = None

            for dp in points:
                yr = dp.get("year")
                # Coerce year to int — Gemini may return strings like "2024"
                if yr is not None:
                    try:
                        yr = int(yr)
                    except (ValueError, TypeError):
                        yr = None
                if yr == reporting_year:
                    # Keep the highest-confidence one if duplicates
                    if current_year_dp is None or dp.get("confidence", 0) > current_year_dp.get("confidence", 0):
                        current_year_dp = dp
                elif yr is not None and yr < reporting_year:
                    if prior_year_dp is None or dp.get("year", 0) > prior_year_dp.get("year", 0):
                        prior_year_dp = dp
                elif yr is not None and yr > reporting_year:
                    # Future year — unusual, skip
                    continue
                else:
                    # No year — treat as current
                    if current_year_dp is None:
                        current_year_dp = dp

            # Navigate to the parent in full_result, creating dicts as needed
            node = full_result
            for part in parts[:-1]:
                if part not in node or not isinstance(node[part], dict):
                    node[part] = {}
                node = node[part]

            field_name = parts[-1]
            existing = node.get(field_name)

            # Determine if this is a metric_value field (dict with value/unit)
            # vs. a plain scalar field
            is_metric_value = isinstance(existing, dict) and "value" in existing

            if existing is None or existing == {}:
                # Field is empty — populate from Vision, but only if data looks valid
                # Skip if Vision unit is % but the schema path expects absolute values (emissions, energy, water, waste)
                abs_paths = ("ghg_emissions.", "total_consumption", "fuel_consumption", "electricity_consumption",
                             "total_withdrawal", "total_waste", "hazardous_waste", "non_hazardous_waste")
                dp_unit = (current_year_dp or prior_year_dp or {}).get("unit", "")
                if isinstance(dp_unit, str) and "%" in dp_unit and any(p in schema_path for p in abs_paths):
                    unmapped.append(current_year_dp or prior_year_dp)
                    continue
                # Reject non-numeric values — Vision sometimes returns strings like "first", "N/A"
                best_dp = current_year_dp or prior_year_dp
                if best_dp and not isinstance(best_dp.get("value"), (int, float)):
                    unmapped.append(best_dp)
                    continue
                if current_year_dp:
                    new_val = self._vision_dp_to_field(current_year_dp, prior_year_dp)
                    node[field_name] = new_val
                    merged_count += 1
                elif prior_year_dp:
                    # Only have prior year data, still useful
                    new_val = self._vision_dp_to_field(prior_year_dp, None)
                    node[field_name] = new_val
                    merged_count += 1

            elif is_metric_value:
                # Field exists as metric_value — enrich or overwrite with Vision data
                changed = False

                # Vision overwrite: replace text-extracted values when either:
                # (a) Text confidence is below 0.9 AND vision has chart data, OR
                # (b) Text page_reference is a vague section name AND vision has chart
                #     data with confidence >= 0.9.
                # This handles hallucinated values (the prompt tells text LLM to omit,
                # but when it doesn't, confidence is typically < 0.9).
                vision_source = (current_year_dp or {}).get("source_type", "")
                vision_conf = (current_year_dp or {}).get("confidence", 0)
                text_conf = existing.get("confidence", 0.9)
                text_ref = str(existing.get("page_reference", ""))
                chart_sources = ("chart", "table_in_image", "table")
                if (
                    current_year_dp
                    and isinstance(current_year_dp.get("value"), (int, float))
                    and vision_source in chart_sources
                    and (
                        text_conf < 0.9
                        or (vision_conf >= 0.9 and "section" in text_ref.lower()
                            and not text_ref.startswith("table_"))
                    )
                ):
                    new_val = self._vision_dp_to_field(current_year_dp, prior_year_dp)
                    node[field_name] = new_val
                    merged_count += 1
                    continue

                # Add prior_year_value if missing
                if (
                    prior_year_dp
                    and existing.get("prior_year_value") is None
                ):
                    existing["prior_year_value"] = prior_year_dp["value"]
                    existing["prior_year"] = prior_year_dp.get("year")
                    changed = True

                # Add source_type if from Vision
                if changed:
                    existing.setdefault("vision_supplemented", True)
                    merged_count += 1

            elif isinstance(existing, (int, float)):
                # Plain scalar — only fill if zero/null
                pass  # text extraction already got it

            # else: existing is something else, don't touch

        return merged_count, unmapped

    @staticmethod
    def _vision_dp_to_field(dp: dict, prior_dp: dict = None) -> dict:
        """Convert a Vision data point to a metric_value-style dict."""
        field = {
            "value": dp.get("value"),
            "year": dp.get("year"),
            "confidence": dp.get("confidence", 0.8),
            "source": "vision",
            "page_reference": dp.get("page"),
        }
        unit = dp.get("unit")
        if unit:
            field["unit"] = unit

        if prior_dp:
            field["prior_year_value"] = prior_dp.get("value")
            field["prior_year"] = prior_dp.get("year")

        # Strip None values for cleanliness
        return {k: v for k, v in field.items() if v is not None}

    # Number of parallel Gemini Vision calls (keep modest to avoid rate limits)
    VISION_WORKERS = 4

    def _extract_from_images(self, input_path: Path) -> list[dict]:
        """
        Send chart/infographic images to Vision API for ESG data extraction.
        Uses Gemini 2.5 Flash if GOOGLE_API_KEY is set, else falls back to Claude Vision.
        Processes images in parallel (VISION_WORKERS threads) for speed.
        Returns a flat list of extracted data points.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        vision_backend = "Gemini 2.5 Flash" if os.environ.get("GOOGLE_API_KEY") else "Claude Vision"
        log.info(f"  Vision backend: {vision_backend}")
        images_dir = input_path / "images"
        if not images_dir.exists():
            return []

        index_path = images_dir / "images_index.json"
        if not index_path.exists():
            return []

        try:
            image_list = json.loads(index_path.read_text(encoding="utf-8"))
        except Exception:
            return []

        if not image_list:
            return []

        # Build task list
        tasks = []
        for img_info in image_list:
            image_file = img_info.get("image_file")
            if not image_file:
                continue
            image_path = images_dir / image_file
            if not image_path.exists():
                image_path = input_path / "images" / image_file
                if not image_path.exists():
                    log.debug(f"  Image file not found: {image_file}")
                    continue
            context = img_info.get("description", "") or ""
            classification = img_info.get("classification", "")
            if classification:
                context = f"Image type: {classification}. {context}"
            tasks.append((str(image_path), image_file, classification, context, img_info))

        def _process_image(task):
            img_path, image_file, classification, context, img_info = task
            log.info(f"  Vision extracting: {image_file} ({classification or 'unclassified'})")
            result = self.llm.extract_from_image(img_path, context=context)
            data_points = result.get("data_points", [])
            for dp in data_points:
                dp["source_image"] = image_file
                dp["page"] = img_info.get("page")
                # Clamp confidence to [0, 1]
                conf = dp.get("confidence")
                if isinstance(conf, (int, float)) and conf > 1.0:
                    dp["confidence"] = min(conf / (10 if conf <= 10 else 100), 1.0)
                # Normalize source_type
                if dp.get("source_type") == "table":
                    dp["source_type"] = "table_in_image"
            return data_points

        all_data_points = []
        workers = min(self.VISION_WORKERS, len(tasks))
        if workers <= 1:
            # Sequential fallback for single image
            for task in tasks:
                all_data_points.extend(_process_image(task))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(_process_image, t): t for t in tasks}
                for future in as_completed(futures):
                    try:
                        all_data_points.extend(future.result())
                    except Exception as e:
                        task = futures[future]
                        log.warning(f"  Vision failed for {task[1]}: {e}")

        return all_data_points

    @staticmethod
    def _fix_waste_totals(full_result: dict):
        """
        Post-merge fix: if hazardous + non_hazardous waste are both present
        and their sum doesn't match total_waste, recalculate.
        Also: if total_waste looks like it's only one of the sub-types, fix it.
        """
        waste = full_result.get("environmental", {}).get("waste", {})
        if not waste:
            return

        def get_val(obj):
            if isinstance(obj, dict) and "value" in obj:
                return obj["value"]
            return obj if isinstance(obj, (int, float)) else None

        haz = get_val(waste.get("hazardous_waste"))
        non_haz = get_val(waste.get("non_hazardous_waste"))
        total = get_val(waste.get("total_waste"))

        # Case 1: haz and non_haz both present, total missing or wrong
        if haz is not None and non_haz is not None:
            expected_total = haz + non_haz
            if total is None:
                waste["total_waste"] = {
                    "value": expected_total,
                    "unit": "tonnes",
                    "confidence": 0.9,
                    "source": "calculated",
                    "page_reference": "sum of hazardous + non-hazardous",
                }
            elif abs(total - expected_total) > 1 and abs(total - haz) < 1:
                # total_waste == hazardous (misclassified) -> fix
                waste["total_waste"]["value"] = expected_total
                waste["total_waste"]["source"] = "recalculated"
            elif abs(total - expected_total) > 1 and abs(total - non_haz) < 1:
                # total_waste == non_hazardous (misclassified) -> fix
                waste["total_waste"]["value"] = expected_total
                waste["total_waste"]["source"] = "recalculated"

        # Case 2: total and one sub-type present -> derive the other
        elif total is not None and haz is not None and non_haz is None and total > haz:
            waste["non_hazardous_waste"] = {
                "value": total - haz,
                "unit": "tonnes",
                "confidence": 0.85,
                "source": "calculated",
                "page_reference": "total minus hazardous",
            }
        elif total is not None and non_haz is not None and haz is None and total > non_haz:
            waste["hazardous_waste"] = {
                "value": total - non_haz,
                "unit": "tonnes",
                "confidence": 0.85,
                "source": "calculated",
                "page_reference": "total minus non-hazardous",
            }

    def _count_fields(self, obj, depth=0) -> int:
        """Count non-null leaf values in a nested dict."""
        if depth > 10:
            return 0
        count = 0
        if isinstance(obj, dict):
            for v in obj.values():
                if isinstance(v, (dict, list)):
                    count += self._count_fields(v, depth + 1)
                elif v is not None:
                    count += 1
        elif isinstance(obj, list):
            for item in obj:
                count += self._count_fields(item, depth + 1)
        return count


# ---------------------------------------------------------------------------
# Output writer
# ---------------------------------------------------------------------------
class ExtractionOutputWriter:
    """Saves extraction results to disk."""

    def __init__(self, base_dir: str = ExtractorConfig.OUTPUT_DIR):
        self.base_dir = Path(base_dir)

    def write(self, data: dict, input_dir: str) -> Path:
        """Write extraction results alongside or into an output folder."""
        input_name = Path(input_dir).name
        out_dir = self.base_dir / input_name
        out_dir.mkdir(parents=True, exist_ok=True)

        # Main ESG data file
        data_path = out_dir / "esg_data.json"
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False, default=str)

        # Summary (lighter file for quick review)
        summary = {
            "source": data.get("extraction_metadata", {}).get("source_pdf"),
            "company": data.get("report_metadata", {}).get("company_name"),
            "year": data.get("report_metadata", {}).get("reporting_year"),
            "frameworks": data.get("report_metadata", {}).get("frameworks_used"),
            "fields_extracted": data.get("extraction_metadata", {}).get("fields_extracted"),
            "needs_review": data.get("extraction_metadata", {}).get("needs_review"),
            "review_flags": data.get("extraction_metadata", {}).get("review_flags", []),
            "extraction_time": data.get("extraction_metadata", {}).get("extraction_time_seconds"),
        }
        summary_path = out_dir / "summary.json"
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        # Review flags file (if any)
        flags = data.get("extraction_metadata", {}).get("review_flags", [])
        if flags:
            flags_path = out_dir / "review_flags.json"
            with open(flags_path, "w", encoding="utf-8") as f:
                json.dump(flags, f, indent=2, ensure_ascii=False)

        log.info(f"Output written: {out_dir}")
        return out_dir


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Phase 3 — Extract structured ESG data using LLM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python esg_extractor.py --input ./parsed_output/0007_TestCorp_2024_ESG_GRI --simulate
  python esg_extractor.py --run --simulate
  python esg_extractor.py --run --model claude-sonnet-4-20250514
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--input", type=str, help="Path to a parsed output folder")
    group.add_argument("--run", action="store_true", help="Process all parsed tasks from queue")
    group.add_argument("--dry-run", action="store_true", help="Show parsed tasks without processing")

    parser.add_argument("--simulate", action="store_true", help="Simulate LLM (no API key needed)")
    parser.add_argument("--model", default=ExtractorConfig.MODEL, help="Claude model to use")
    parser.add_argument("--output-dir", default=ExtractorConfig.OUTPUT_DIR)
    parser.add_argument("--db", default=ExtractorConfig.DB_PATH)
    parser.add_argument("--batch-size", type=int, default=ExtractorConfig.DEFAULT_BATCH_SIZE)
    parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ExtractorConfig.OUTPUT_DIR = args.output_dir
    ExtractorConfig.DB_PATH = args.db
    ExtractorConfig.MODEL = args.model

    llm = LLMClient(model=args.model, simulate=args.simulate)
    extractor = ESGExtractor(llm)
    writer = ExtractionOutputWriter(args.output_dir)

    # --- Single folder mode ---
    if args.input:
        try:
            data = extractor.extract_from_folder(args.input)
            out_dir = writer.write(data, args.input)

            print(f"\n{'-' * 55}")
            print(f"  ESG Extraction Results")
            print(f"{'-' * 55}")
            company = data.get("report_metadata", {}).get("company_name", "Unknown")
            year = data.get("report_metadata", {}).get("reporting_year", "?")
            print(f"  Company:    {company}")
            print(f"  Year:       {year}")
            print(f"  Fields:     {data['extraction_metadata']['fields_extracted']}")
            print(f"  Time:       {data['extraction_metadata']['extraction_time_seconds']:.1f}s")
            print(f"  Review:     {'YES -- flags found' if data['extraction_metadata']['needs_review'] else 'Clean'}")
            print(f"  Output:     {out_dir}")

            flags = data.get("extraction_metadata", {}).get("review_flags", [])
            if flags:
                print(f"\n  Review flags ({len(flags)}):")
                for f in flags:
                    print(f"    [{f['severity']}] {f['field']}: {f['issue']}")

            usage = llm.get_usage_summary()
            print(f"\n  LLM calls:  {usage['total_calls']}")
            if usage['total_input_tokens']:
                print(f"  Tokens:     {usage['total_input_tokens']:,} in / {usage['total_output_tokens']:,} out")
            print()

        except Exception as e:
            log.error(f"Extraction failed: {e}")
            if args.verbose:
                traceback.print_exc()
            sys.exit(1)
        return

    # --- DB modes ---
    db = IngestionDB(args.db)

    if args.dry_run:
        parsed = db.get_parsed_tasks(limit=100)
        summary = db.get_status_summary()
        print(f"\n{'-' * 50}")
        print(f"  Queue Status")
        print(f"{'-' * 50}")
        for status, count in sorted(summary.items()):
            print(f"  {status:<14} {count:>5}")
        if parsed:
            print(f"\n  Next {len(parsed)} parsed tasks ready for extraction:")
            for t in parsed:
                print(f"    #{t['id']:>4}  {t['file_name'][:40]}")
        print()
        return

    # --- Batch run ---
    if args.run:
        parsed_tasks = db.get_parsed_tasks(limit=args.batch_size)
        if not parsed_tasks:
            print("No parsed tasks ready for extraction.")
            return

        log.info(f"Starting extraction batch: {len(parsed_tasks)} tasks")
        results = {"success": 0, "failed": 0}
        batch_start = time.time()

        for task in parsed_tasks:
            task_id = task["id"]
            file_name = task["file_name"]

            # Find the parsed output folder
            parsed_dir = None
            base = Path(ExtractorConfig.PARSED_DIR)

            if base.exists():
                for d in sorted(base.iterdir()):
                    if d.is_dir() and d.name.startswith(f"{task_id:04d}_"):
                        parsed_dir = d
                        break

            if not parsed_dir:
                log.warning(f"Task #{task_id}: parsed output folder not found, skipping")
                results["failed"] += 1
                continue

            log.info(f"{'-' * 60}")
            log.info(f"Extracting task #{task_id}: {file_name}")
            db.update_status(task_id, "extracting")

            try:
                data = extractor.extract_from_folder(str(parsed_dir), task_id)
                writer.write(data, str(parsed_dir))
                db.update_status(task_id, "extracted")
                results["success"] += 1

            except Exception as e:
                log.error(f"Task #{task_id} failed: {e}")
                db.update_status(task_id, "failed", str(e))
                results["failed"] += 1

        batch_time = time.time() - batch_start
        usage = llm.get_usage_summary()

        print(f"\n{'═' * 55}")
        print(f"  Extraction Batch Complete")
        print(f"{'═' * 55}")
        print(f"  Processed:  {results['success'] + results['failed']}")
        print(f"  Succeeded:  {results['success']}")
        print(f"  Failed:     {results['failed']}")
        print(f"  Total time: {batch_time:.1f}s")
        print(f"  LLM calls:  {usage['total_calls']}")
        if usage["total_input_tokens"]:
            print(f"  Tokens:     {usage['total_input_tokens']:,} in / {usage['total_output_tokens']:,} out")
        print()


if __name__ == "__main__":
    main()