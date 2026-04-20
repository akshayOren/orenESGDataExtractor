"""Trim images_index to key chart pages for testing."""
import json, shutil
from pathlib import Path

src = Path("parsed_output_v7/A_D_N_H_Catering_PLC_2025-03-17")
dst = Path("parsed_output_v7_test/A_D_N_H_Catering_PLC_2025-03-17")

if dst.exists():
    shutil.rmtree(dst)
shutil.copytree(src, dst)

keep_pages = {20, 25, 40, 44, 48}
idx = json.load(open(dst / "images" / "images_index.json"))
filtered = [e for e in idx if e.get("page") in keep_pages]
for i, e in enumerate(filtered):
    e["index"] = i
json.dump(filtered, open(dst / "images" / "images_index.json", "w"), indent=2)
print(f"Trimmed to {len(filtered)} pages: {[e['page'] for e in filtered]}")
