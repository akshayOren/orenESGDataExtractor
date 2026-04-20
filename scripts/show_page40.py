"""Show Vision API extraction results for page 40."""
import json

d = json.load(open("extracted_output_v7/A_D_N_H_Catering_PLC_2025-03-17/esg_data.json"))
pts = d.get("image_extracted_data", [])
print(f"Total Vision data points: {len(pts)}\n")

print("=== PAGE 40 (Parental Leave + Performance Reviews) ===")
for p in pts:
    if p.get("page") == 40:
        name = p.get("metric_name", "?")
        val = p.get("value", "?")
        year = p.get("year", "?")
        cat = p.get("category", "?")
        print(f"  {name}: {val} (year={year}, category={cat})")

print("\n=== PAGE 44 (Training) ===")
for p in pts:
    if p.get("page") == 44:
        name = p.get("metric_name", "?")
        val = p.get("value", "?")
        year = p.get("year", "?")
        print(f"  {name}: {val} (year={year})")

print("\n=== PAGE 48 (Safety) ===")
for p in pts:
    if p.get("page") == 48:
        name = p.get("metric_name", "?")
        val = p.get("value", "?")
        year = p.get("year", "?")
        print(f"  {name}: {val} (year={year})")
