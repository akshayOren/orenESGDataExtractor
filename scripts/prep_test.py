"""Prepare test dataset: render key chart pages and create trimmed index."""
import json
import shutil
from pathlib import Path
import pypdfium2

src = Path("parsed_output_v7/A_D_N_H_Catering_PLC_2025-03-17")
dst = Path("parsed_output_v7_test/A_D_N_H_Catering_PLC_2025-03-17")
pdf_path = "data/reports/uae/A_D_N_H_Catering_PLC_2025-03-17.pdf"

# Copy base output
if dst.exists():
    shutil.rmtree(dst)
shutil.copytree(src, dst)

# Render specific chart pages
key_pages = [20, 25, 40, 44, 48]
images_dir = dst / "images"
images_dir.mkdir(exist_ok=True)

pdf = pypdfium2.PdfDocument(pdf_path)
index = []
for i, page_num in enumerate(key_pages):
    page = pdf[page_num - 1]
    bitmap = page.render(scale=150 / 72)
    img = bitmap.to_pil().convert("RGB")
    fname = f"chart_page_{page_num:03d}.jpg"
    img.save(str(images_dir / fname), "JPEG", quality=85)
    index.append({
        "index": i,
        "page": page_num,
        "classification": "chart_page",
        "description": f"Page {page_num} containing chart/graph with ESG data",
        "image_file": fname,
    })
    print(f"Rendered page {page_num}")
pdf.close()

json.dump(index, open(images_dir / "images_index.json", "w"), indent=2)
print(f"\nReady: {len(index)} chart pages for Vision API extraction")
