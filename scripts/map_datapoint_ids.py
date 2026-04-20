#!/usr/bin/env python3
"""
map_datapoint_ids.py
────────────────────
Replace placeholder data_point_ids in esg_data_pointmapping.json with
real CUIDs from the DB data_point CSVs (exported from the data_point table).

Matching is done by semantic correspondence between our extractor_identifier
paths and the CSV `description` field.

Usage:
    python scripts/map_datapoint_ids.py
    python scripts/map_datapoint_ids.py --dry-run
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ESG_MAPPING_PATH = ROOT / "data" / "esg_data_pointmapping.json"
CSV_FILES = [
    ROOT / "data" / "turnover_and_website_202604161632.csv",
    ROOT / "data" / "turnover_and_website_202604161634.csv",
]

# ─────────────────────────────────────────────────────────────────────────────
# Semantic mapping: our placeholder key → DB description (from CSV)
#
# These are hand-verified conceptual matches between the ADX/UAE ESG schema
# fields and the data_point descriptions in the Oren DB.
# ─────────────────────────────────────────────────────────────────────────────

SEMANTIC_MAP = {
    # ── Report Metadata ──────────────────────────────────────────────────
    "PLACEHOLDER_report_metadata_company_name": "Legal-name",
    "PLACEHOLDER_report_metadata_country": "Countries-of-operation",
    "PLACEHOLDER_report_metadata_industry_sector": "Organization-activities",
    "PLACEHOLDER_report_metadata_reporting_year": "Reporting-Period",
    "PLACEHOLDER_report_metadata_report_title": None,
    "PLACEHOLDER_report_metadata_assurance_level": None,
    "PLACEHOLDER_report_metadata_assurance_provider": None,
    "PLACEHOLDER_report_metadata_currency": None,
    "PLACEHOLDER_report_metadata_revenue": "Direct-economic-value-generated-revenues",
    "PLACEHOLDER_report_metadata_revenue_unit": None,
    "PLACEHOLDER_report_metadata_employee_count": "Total-employees",

    # ── GHG Emissions ────────────────────────────────────────────────────
    "PLACEHOLDER_environmental_ghg_emissions_scope_1_total": "Total-Scope-1-Emissions",
    "PLACEHOLDER_environmental_ghg_emissions_scope_1_percentage_of_total": None,
    "PLACEHOLDER_environmental_ghg_emissions_scope_1_biogenic_co2": "Biogenic-CO2-emissions-in-metric-tons-of-CO2-equivalent",
    "PLACEHOLDER_environmental_ghg_emissions_scope_2_location_based": "Scope-2-GHG-Emissions-Current-FY",
    "PLACEHOLDER_environmental_ghg_emissions_scope_2_market_based": "Scope-2-GHG-Emissions-Market",
    "PLACEHOLDER_environmental_ghg_emissions_scope_2_percentage_of_total": None,
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_total": "Scope-3-Emissions",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_percentage_of_total": None,
    "PLACEHOLDER_environmental_ghg_emissions_total_emissions": None,

    # Scope 3 categories
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_1": "Total-Purchased-Emissions",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_2": "Total-capital-goodsEmissions",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_3": "Emission in Fuel & Energy",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_4": "Total Transportation & Distribution Upstream",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_5": "Total-Waste-Emissions",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_6": "Total Business Travel",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_7": "Total-Employee-Commute-Emissions",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_8": None,
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_9": "Total Transportation & Distribution Downstream",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_10": "Total-Purchased-Emissions-Processing-of-sold-products",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_11": "Total-Purchased-Emissions-Use-of-sold-products",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_12": "Total-Purchased-Emissions-End-of-life-treatment-of-sold-products",
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_13": None,
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_14": None,
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_category_number_15": None,

    # Emission intensity & reduction
    "PLACEHOLDER_environmental_ghg_emissions_emission_intensity_value": None,
    "PLACEHOLDER_environmental_ghg_emissions_reduction_targets_target_year": None,
    "PLACEHOLDER_environmental_ghg_emissions_reduction_targets_base_year": None,
    "PLACEHOLDER_environmental_ghg_emissions_reduction_targets_reduction_pct": None,
    "PLACEHOLDER_environmental_ghg_emissions_reduction_targets_sbti_validated": None,
    "PLACEHOLDER_environmental_ghg_emissions_methodology_notes": "Scope-1-Methodology",

    # ── Energy ───────────────────────────────────────────────────────────
    "PLACEHOLDER_environmental_energy_total_consumption_within_org": "Total-Energy-Consumption",
    "PLACEHOLDER_environmental_energy_total_consumption_outside_org": "Energy-Consumption-Outside-Org-SCOPE3",
    "PLACEHOLDER_environmental_energy_fuel_consumption_within_org_total": None,
    "PLACEHOLDER_environmental_energy_fuel_consumption_outside_org": None,
    "PLACEHOLDER_environmental_energy_electricity_consumption_within_org_total": None,
    "PLACEHOLDER_environmental_energy_electricity_consumption_outside_org": None,
    "PLACEHOLDER_environmental_energy_renewable_share_pct": None,
    "PLACEHOLDER_environmental_energy_energy_intensity_value": "Energy-Intensity-Optional-metric",
    "PLACEHOLDER_environmental_energy_energy_reduction_initiatives": None,

    # ── Water ────────────────────────────────────────────────────────────
    "PLACEHOLDER_environmental_water_total_withdrawal": "Total-Water-Withdrawal",
    "PLACEHOLDER_environmental_water_total_consumption": None,
    "PLACEHOLDER_environmental_water_total_discharge": "Total-water-discharged-Current-FY",
    "PLACEHOLDER_environmental_water_water_stress_areas": None,
    "PLACEHOLDER_environmental_water_water_recycled": None,
    "PLACEHOLDER_environmental_water_water_recycled_pct": None,
    "PLACEHOLDER_environmental_water_water_intensity_value": None,

    # ── Waste ────────────────────────────────────────────────────────────
    "PLACEHOLDER_environmental_waste_total_waste": "Total-Waste-Generated",
    "PLACEHOLDER_environmental_waste_hazardous_waste": None,
    "PLACEHOLDER_environmental_waste_non_hazardous_waste": None,
    "PLACEHOLDER_environmental_waste_waste_diverted_from_disposal": "306-4-Waste-Diverted-From-Disposal",
    "PLACEHOLDER_environmental_waste_waste_to_landfill": None,
    "PLACEHOLDER_environmental_waste_recycling_rate_pct": None,
    "PLACEHOLDER_environmental_waste_organic_waste": None,
    "PLACEHOLDER_environmental_waste_food_waste": None,

    # ── Air Emissions ────────────────────────────────────────────────────
    # (wildcard handled separately)

    # ── Biodiversity ─────────────────────────────────────────────────────
    "PLACEHOLDER_environmental_biodiversity_operations_near_protected_areas": "Biodiversity-Protected-Areas",

    # ── Climate Risk ─────────────────────────────────────────────────────
    "PLACEHOLDER_environmental_climate_risk_scenario_analysis": None,
    "PLACEHOLDER_environmental_climate_risk_internal_carbon_price_value": None,

    # ── Social: Workforce ────────────────────────────────────────────────
    "PLACEHOLDER_social_workforce_total_employees": "Total-employees",
    "PLACEHOLDER_social_workforce_total_employees_male": "Employees-at-start-of-period-Total-Male",
    "PLACEHOLDER_social_workforce_total_employees_female": "Employees-at-start-of-period-Total-Female",
    "PLACEHOLDER_social_workforce_permanent_employees": "Total-permanent-employees-Current-Year",
    "PLACEHOLDER_social_workforce_permanent_employees_male": "Male-permanent-employees",
    "PLACEHOLDER_social_workforce_permanent_employees_female": "Female-permanent-employees",
    "PLACEHOLDER_social_workforce_temporary_employees": "Total-temporary-employees",
    "PLACEHOLDER_social_workforce_temporary_employees_male": "Male-temporary-employees",
    "PLACEHOLDER_social_workforce_temporary_employees_female": "Female-temporary-employees",
    "PLACEHOLDER_social_workforce_full_time_employees": None,
    "PLACEHOLDER_social_workforce_full_time_employees_male": None,
    "PLACEHOLDER_social_workforce_full_time_employees_female": None,
    "PLACEHOLDER_social_workforce_part_time_employees": None,
    "PLACEHOLDER_social_workforce_part_time_employees_male": None,
    "PLACEHOLDER_social_workforce_part_time_employees_female": None,
    "PLACEHOLDER_social_workforce_contractors": None,
    "PLACEHOLDER_social_workforce_new_hires": "Number-of-Employees-hired-Total-Total",
    "PLACEHOLDER_social_workforce_new_hires_male": "Number-of-Employees-hired-Total-Male",
    "PLACEHOLDER_social_workforce_new_hires_female": "Number-of-Employees-hired-Total-Female",
    "PLACEHOLDER_social_workforce_turnover_rate_pct": "Employee-Turnover-Rate-Total-Total",
    "PLACEHOLDER_social_workforce_turnover_voluntary_pct": None,
    "PLACEHOLDER_social_workforce_turnover_male_pct": "Employee-Turnover-Rate-Total-Male",
    "PLACEHOLDER_social_workforce_turnover_female_pct": "Employee-Turnover-Rate-Total-Female",
    "PLACEHOLDER_social_workforce_turnover_under_30_pct": "Employee-Turnover-Rate-Under-30-years-old-Total",
    "PLACEHOLDER_social_workforce_turnover_30_50_pct": "Employee-Turnover-Rate-30-50-years-Total",
    "PLACEHOLDER_social_workforce_turnover_over_50_pct": "Employee-Turnover-Rate-Over-50-years-old-Total",
    "PLACEHOLDER_social_workforce_local_employees_pct": "Local-procurement-percentage",
    "PLACEHOLDER_social_workforce_age_distribution_under_30": "Employees-at-start-of-period-Under-30-years-old-Total",
    "PLACEHOLDER_social_workforce_age_distribution_under_30_male": "Employees-at-start-of-period-Under-30-years-old-Male",
    "PLACEHOLDER_social_workforce_age_distribution_under_30_female": "Employees-at-start-of-period-Under-30-years-old-Female",
    "PLACEHOLDER_social_workforce_age_distribution_between_30_50": "Employees-at-start-of-period-30-50-years-Total",
    "PLACEHOLDER_social_workforce_age_distribution_between_30_50_male": "Employees-at-start-of-period-30-50-years-Male",
    "PLACEHOLDER_social_workforce_age_distribution_between_30_50_female": "Employees-at-start-of-period-30-50-years-Female",
    "PLACEHOLDER_social_workforce_age_distribution_over_50": "Employees-at-start-of-period-Over-50-years-old-Total",
    "PLACEHOLDER_social_workforce_age_distribution_over_50_male": "Employees-at-start-of-period-Over-50-years-old-Male",
    "PLACEHOLDER_social_workforce_age_distribution_over_50_female": "Employees-at-start-of-period-Over-50-years-old-Female",

    # ── Social: Diversity ────────────────────────────────────────────────
    "PLACEHOLDER_social_diversity_inclusion_women_total_pct": "Female-employees-percentage",
    "PLACEHOLDER_social_diversity_inclusion_women_management_pct": None,
    "PLACEHOLDER_social_diversity_inclusion_women_board_pct": None,
    "PLACEHOLDER_social_diversity_inclusion_gender_pay_gap_pct": None,

    # ── Social: Health & Safety ──────────────────────────────────────────
    "PLACEHOLDER_social_health_safety_fatalities": "Fatalities-from-work-related-injuries-Employees",
    "PLACEHOLDER_social_health_safety_fatalities_contractors": "Fatalities-from-work-related-injuries-Workers",
    "PLACEHOLDER_social_health_safety_ltifr": None,
    "PLACEHOLDER_social_health_safety_trir": None,
    "PLACEHOLDER_social_health_safety_lost_days": None,
    "PLACEHOLDER_social_health_safety_high_consequence_injuries": "High-consequence-work-related-injuries-Employees",
    "PLACEHOLDER_social_health_safety_other_occupational_injuries": "Recordable-work-related-injuries-Employees",
    "PLACEHOLDER_social_health_safety_near_miss_incidents": None,
    "PLACEHOLDER_social_health_safety_safety_training_hours": None,

    # ── Social: Training ─────────────────────────────────────────────────
    "PLACEHOLDER_social_training_development_total_training_hours": "Total-Permanent-Training-Hours",
    "PLACEHOLDER_social_training_development_avg_training_hours": "Board-Average-Training-Hours",
    "PLACEHOLDER_social_training_development_training_hours_male": "Total-Male-Permanent-Training",
    "PLACEHOLDER_social_training_development_training_hours_female": "Total-Female-Permanent-Training",
    "PLACEHOLDER_social_training_development_training_hours_by_gender_male": "Total-Male-Permanent-Training",
    "PLACEHOLDER_social_training_development_training_hours_by_gender_female": "Total-Female-Permanent-Training",
    "PLACEHOLDER_social_training_development_training_investment": None,
    "PLACEHOLDER_social_training_development_training_investment_per_employee": None,

    # ── Social: Parental Leave ───────────────────────────────────────────
    "PLACEHOLDER_social_parental_leave_entitled_male": "Employee-Parental-Leave-0-0",
    "PLACEHOLDER_social_parental_leave_entitled_female": "Employee-Parental-Leave-0-1",
    "PLACEHOLDER_social_parental_leave_took_leave_male": "Male-Employees-that-took-Parental-Leave",
    "PLACEHOLDER_social_parental_leave_took_leave_female": "Female-Employees-that-took-Parental-Leave",
    "PLACEHOLDER_social_parental_leave_returned_after_leave_male": "Male-Employees-that-returned-from-Parental-Leave",
    "PLACEHOLDER_social_parental_leave_returned_after_leave_female": "Female-Employees-that-returned-from-Parental-Leave",
    "PLACEHOLDER_social_parental_leave_return_rate_male_pct": "Employee-Parental-Leave-5-0",
    "PLACEHOLDER_social_parental_leave_return_rate_female_pct": "Employee-Parental-Leave-5-1",

    # ── Social: Performance Reviews ──────────────────────────────────────
    "PLACEHOLDER_social_performance_reviews_total_reviewed_pct": None,
    "PLACEHOLDER_social_performance_reviews_reviewed_male_pct": None,
    "PLACEHOLDER_social_performance_reviews_reviewed_female_pct": None,

    # ── Social: Human Rights ─────────────────────────────────────────────
    "PLACEHOLDER_social_human_rights_human_rights_policy": None,
    "PLACEHOLDER_social_human_rights_grievance_mechanisms": None,
    "PLACEHOLDER_social_human_rights_incidents_reported": None,

    # ── Social: Whistleblowing ───────────────────────────────────────────
    "PLACEHOLDER_social_whistleblowing_mechanism_exists": None,

    # ── Social: Community ────────────────────────────────────────────────
    "PLACEHOLDER_social_community_community_investment": "Community-Investments",
    "PLACEHOLDER_social_community_local_hiring_pct": "Local-procurement-percentage",

    # ── Social: Supply Chain (under social) ──────────────────────────────
    "PLACEHOLDER_social_supply_chain_suppliers_screened_pct": None,
    "PLACEHOLDER_social_supply_chain_supplier_code_of_conduct": None,
    "PLACEHOLDER_social_supply_chain_supplier_audits": None,

    # ── Governance: Board ────────────────────────────────────────────────
    "PLACEHOLDER_governance_board_composition_board_size": None,
    "PLACEHOLDER_governance_board_composition_independent_directors": None,
    "PLACEHOLDER_governance_board_composition_independent_pct": None,
    "PLACEHOLDER_governance_board_composition_women_on_board": "Board-of-Directors-Female",
    "PLACEHOLDER_governance_board_composition_board_diversity_policy": None,
    "PLACEHOLDER_governance_board_composition_esg_committee": None,
    "PLACEHOLDER_governance_board_composition_esg_linked_compensation": None,

    # ── Governance: Ethics ───────────────────────────────────────────────
    "PLACEHOLDER_governance_ethics_compliance_anti_corruption_policy": None,
    "PLACEHOLDER_governance_ethics_compliance_corruption_training_pct": "Percentage-of-corruption-assessed-operations",
    "PLACEHOLDER_governance_ethics_compliance_confirmed_corruption_incidents": None,
    "PLACEHOLDER_governance_ethics_compliance_ethics_training_employees": None,
    "PLACEHOLDER_governance_ethics_compliance_whistleblower_mechanism": None,
    "PLACEHOLDER_governance_ethics_compliance_political_contributions": "Political-Contribution-Country",

    # ── Governance: Data Privacy ─────────────────────────────────────────
    "PLACEHOLDER_governance_data_privacy_data_breaches": "substantiated-complaints-concerning-breaches-of-customer-privacy-received-from-outside-parties-and-substantiated-by-the-organization-",
    "PLACEHOLDER_governance_data_privacy_customers_affected": "substantiated-complaints-concerning-breaches-of-customer-privacy-received-from-regulatory-bodies",
    "PLACEHOLDER_governance_data_privacy_privacy_policy": None,

    # ── Governance: Tax ──────────────────────────────────────────────────
    "PLACEHOLDER_governance_tax_transparency_tax_strategy_published": "Has-Tax-Strategy",
    "PLACEHOLDER_governance_tax_transparency_country_by_country_reporting": None,

    # ── Supply Chain ─────────────────────────────────────────────────────
    "PLACEHOLDER_supply_chain_local_procurement_local_spend_pct": "Local-procurement-percentage",
    "PLACEHOLDER_supply_chain_local_procurement_local_suppliers_pct": None,
    "PLACEHOLDER_supply_chain_local_procurement_icv_score": None,
    "PLACEHOLDER_supply_chain_sustainable_packaging_sustainable_packaging_spend_pct": None,
    "PLACEHOLDER_supply_chain_sustainable_packaging_certified_materials_pct": None,
    "PLACEHOLDER_supply_chain_supplier_engagement_total_suppliers": "Number-of-suppliers-assessed-for-environmental-impacts.",
    "PLACEHOLDER_supply_chain_supplier_engagement_suppliers_screened_pct": None,
    "PLACEHOLDER_supply_chain_supplier_engagement_supplier_code_of_conduct": None,
    "PLACEHOLDER_supply_chain_supplier_engagement_supplier_code_adherence_pct": None,
    "PLACEHOLDER_supply_chain_supplier_engagement_supplier_audits": None,
    "PLACEHOLDER_supply_chain_supplier_engagement_supplier_risk_assessments": None,

    # ── Array wildcards (skip — dynamically resolved) ────────────────────
    "PLACEHOLDER_environmental_ghg_emissions_scope_1_by_gas_all": None,
    "PLACEHOLDER_environmental_ghg_emissions_scope_3_categories_all": None,
    "PLACEHOLDER_environmental_energy_fuel_consumption_within_org_by_type_all": None,
    "PLACEHOLDER_environmental_energy_electricity_consumption_within_org_by_type_all": None,
    "PLACEHOLDER_environmental_water_withdrawal_by_source_all": None,
    "PLACEHOLDER_environmental_waste_waste_by_type_all": None,
    "PLACEHOLDER_environmental_air_emissions_by_type_all": None,
    "PLACEHOLDER_environmental_ghg_emissions_scope_2_methodology": None,
}


def load_csv_lookup(csv_paths: list[Path]) -> dict[str, str]:
    """Build description -> id lookup from CSV files."""
    lookup = {}
    for csv_path in csv_paths:
        if not csv_path.exists():
            print(f"  WARN: CSV not found: {csv_path}")
            continue
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                desc = row.get("description", "").strip()
                dp_id = row.get("id", "").strip()
                if desc and dp_id:
                    # Store by exact description
                    lookup[desc] = dp_id
                    # Also store normalized (lowercase, no hyphens)
                    lookup[desc.lower().replace("-", " ").replace("_", " ")] = dp_id
    return lookup


def find_dp_id(lookup: dict, search_desc: str) -> str | None:
    """Find data_point_id by description, trying exact then normalized match."""
    # Exact match
    if search_desc in lookup:
        return lookup[search_desc]
    # Normalized match
    norm = search_desc.lower().replace("-", " ").replace("_", " ")
    if norm in lookup:
        return lookup[norm]
    return None


def main():
    parser = argparse.ArgumentParser(description="Map placeholder data_point_ids to real CUIDs from DB CSV")
    parser.add_argument("--dry-run", action="store_true", help="Print mapping without modifying file")
    args = parser.parse_args()

    # Load DB lookup from CSVs
    lookup = load_csv_lookup(CSV_FILES)
    print(f"Loaded {len(lookup) // 2} data_point descriptions from CSVs\n")

    # Load our mapping
    with open(ESG_MAPPING_PATH, "r", encoding="utf-8") as f:
        esg_mapping = json.load(f)

    matched = 0
    no_equivalent = 0
    not_found_in_csv = 0
    not_in_map = 0
    matched_list = []
    unmatched_list = []

    new_mapping = {}

    for placeholder_key, entry in esg_mapping.items():
        search_desc = SEMANTIC_MAP.get(placeholder_key, "NOT_IN_MAP")

        if search_desc == "NOT_IN_MAP":
            not_in_map += 1
            unmatched_list.append((placeholder_key, entry["extractor_identifier"], "not in semantic map"))
            new_mapping[placeholder_key] = entry
            continue

        if search_desc is None:
            no_equivalent += 1
            unmatched_list.append((placeholder_key, entry["extractor_identifier"], "no DB equivalent"))
            new_mapping[placeholder_key] = entry
            continue

        real_dp_id = find_dp_id(lookup, search_desc)
        if real_dp_id is None:
            not_found_in_csv += 1
            unmatched_list.append((placeholder_key, entry["extractor_identifier"], f"'{search_desc}' not found in CSV"))
            new_mapping[placeholder_key] = entry
            continue

        # Match found
        matched += 1
        matched_list.append((placeholder_key, search_desc, real_dp_id))

        updated = dict(entry)
        updated["data_point_id"] = real_dp_id
        updated["db_description"] = search_desc  # keep for reference
        new_mapping[real_dp_id] = updated

    # Summary
    total = len(esg_mapping)
    still_need = no_equivalent + not_found_in_csv + not_in_map
    print(f"Total mapping entries:     {total}")
    print(f"Matched (real dp_id):      {matched}")
    print(f"No DB equivalent:          {no_equivalent}")
    print(f"Not found in CSV:          {not_found_in_csv}")
    print(f"Not in semantic map:       {not_in_map}")
    print(f"Still need IDs:            {still_need}")
    print()

    if matched_list:
        print("MATCHED:")
        for pk, desc, dp in matched_list:
            print(f"  {pk}")
            print(f"    -> '{desc}' => {dp}")
        print()

    if not args.dry_run:
        with open(ESG_MAPPING_PATH, "w", encoding="utf-8") as f:
            json.dump(new_mapping, f, indent=2, ensure_ascii=False)
        print(f"Updated {ESG_MAPPING_PATH}")
    else:
        print("[DRY RUN] No file changes made")

    if not_found_in_csv > 0:
        print(f"\nNOT FOUND IN CSV ({not_found_in_csv} — description didn't match any row):")
        for pk, path, reason in unmatched_list:
            if "not found in CSV" in reason:
                print(f"  {pk}  |  {reason}")

    print(f"\nSTILL NEED IDs ({still_need} entries):")
    for pk, path, reason in unmatched_list:
        print(f"  {pk}")
        print(f"    path: {path}  |  reason: {reason}")


if __name__ == "__main__":
    main()
