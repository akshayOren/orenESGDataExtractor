"""Quick script to add page 40 image to v7 parsed output."""
import pypdfium2
import json

pdf = pypdfium2.PdfDocument("data/reports/uae/A_D_N_H_Catering_PLC_2025-03-17.pdf")
page = pdf[39]  # 0-based
bitmap = page.render(scale=200 / 72)
img = bitmap.to_pil()
img.save("parsed_output_v7/A_D_N_H_Catering_PLC_2025-03-17/images/chart_page_040.png", "PNG")

idx_path = "parsed_output_v7/A_D_N_H_Catering_PLC_2025-03-17/images/images_index.json"
index = json.load(open(idx_path))
index.append({
    "index": len(index),
    "page": 40,
    "classification": "chart_page",
    "description": "Page 40 containing chart/graph with ESG data",
    "image_file": "chart_page_040.png",
})
json.dump(index, open(idx_path, "w"), indent=2)
print(f"Added page 40. Total images: {len(index)}")
pdf.close()
