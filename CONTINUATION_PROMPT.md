# ESG Data Extractor — Continuation Prompt

Copy everything below this line and paste it as your first message in a new conversation.

---

## Project Context

I'm building an ESG data extraction pipeline that processes corporate sustainability PDF reports → parses them → extracts structured ESG data using Claude API. The project is at `C:\Users\Akshay\Desktop\akshayOren\orenESGDataExtractor`.

## What was done in the previous session

We reviewed the ADNH Catering 2024 PDF extraction and found 17 categories of missing data. We made extensive changes across 3 files to fix 3 root causes:

### Root Cause 1: Parser truncation (FIXED)
**File: `scripts/pdf_parser.py`**
- Docling ran out of memory on large PDFs (50+ pages), silently dropping pages
- **Fix**: Added chunked PDF parsing — splits large PDFs into 15-page chunks via pypdf, parses each chunk with Docling separately, merges results
- Config: `CHUNK_PAGES = 15` in `ParserConfig`
- Also added page-count validation after conversion (compares Docling result vs actual PDF page count)

### Root Cause 2: Content truncation at extraction (FIXED)
**File: `scripts/esg_extractor.py`**
- All 4 section prompts received the same first 12K chars of content
- **Fix**: Added section-aware content routing via `_build_section_content()` method that maps each extraction section to relevant parsed section files using keyword matching in `SECTION_KEYWORDS` dict
- `MAX_CHUNK_CHARS` raised to 120K as fallback, `MAX_TOKENS` raised to 8192

### Root Cause 3: Prompt gaps (FIXED)
**File: `scripts/esg_extractor.py`**
- Prompts didn't ask for many data points
- **Fix**: Completely rewrote all section prompts:
  - `report_metadata`: Added reporting_period, scope_3_categories_covered, company_aliases
  - `environmental`: Added fuel/grid electricity breakdown, Scope 1/2/3 with prior year & percentage_of_total, Scope 3 category-wise emissions, waste by type, methodology notes
  - `social`: Added gender-disaggregated workforce (male/female), age distribution, parental leave, performance reviews, training by gender, training investment, near-miss incidents, other occupational injuries
  - `governance`: Added independent_pct, board_diversity_policy, esg_compensation_details, ethics_training_employees
  - **NEW `supply_chain` section**: local_spend_pct, local_suppliers_pct, icv_score, sustainable_packaging_spend_pct, certifications, supplier engagement
- Updated SYSTEM_PROMPT with rules 8 (prefer detailed sections over summaries), 9 (gender-disaggregated), 10 (raw percentages)
- Added `supply_chain` to `SECTIONS` list

### Root Cause 4: Chart/bar graph data (FIXED)
**File: `scripts/pdf_parser.py`**
- Bar charts in PDFs (e.g., parental leave, performance reviews on page 40) can't be read by text parsers — Docling marks them as `<!-- image -->`, pypdf garbles the numbers
- **Fix**: Added `_render_chart_pages()` method to `OutputWriter.write()` that:
  1. Scans `full.md` for `<!-- image -->` tags near data-related headings (using `DATA_HEADING_KEYWORDS`)
  2. Estimates PDF page number from line position in markdown
  3. Renders those pages as JPEG (150 DPI, quality 85) using pypdfium2
  4. Saves to `images/` directory with `images_index.json`
  5. The extractor's existing `_extract_from_images()` picks them up automatically and sends to Claude Vision API
- This is fully automated — works for any PDF at scale

### Schema updates (DONE)
**File: `data/esg_schema.json`**
- Added ~25 new fields across all sections
- New top-level `supply_chain` section (local_procurement, sustainable_packaging, supplier_engagement)
- New `social.parental_leave` and `social.performance_reviews` sub-sections
- Promoted `ltifr` and `lost_days` to metric_value type (supports prior_year)
- Added waste_by_type, organic_waste, food_waste, electricity_by_type, fuel_consumption, grid_electricity
- Gender-disaggregated workforce fields (total_employees_male/female, etc.)

### Validator updates (DONE)
**File: `scripts/esg_extractor.py`** (ESGValidator class)
- Added range checks for new percentage fields (local_spend_pct, sustainable_packaging_spend_pct, etc.)
- Added cross-reference: permanent + temporary ≈ total employees

### Other fixes
- Fixed Unicode print errors (`─` and `═` characters) causing exit code 1 on Windows cp1252
- Fixed dataclass field ordering in `ParsedDocument` (non-default after default)
- Installed: `pypdf`, `python-dotenv`, `anthropic`, `docling`, `pypdfium2` (comes with docling)

## Results achieved

ADNH Catering 2024 extraction:
- **Original**: 32 fields, 59% completeness, no social data, no supply chain
- **Final (v7)**: 652 fields, Vision API correctly extracted all chart data including parental leave (Female: CY2023=7, CY2024=34; Male: CY2023=6, CY2024=28) and performance reviews

## What needs to be done next

### 1. Chart page detection needs tuning
The current `DATA_HEADING_KEYWORDS` + line-position-based page estimation produces too many false positives (28 pages rendered for ADNH when ~10-15 would suffice). The ±2 neighbor range also adds pages. Options:
- Tighten keywords further
- Use Docling's document structure (provenance/page info) instead of line position estimation
- Skip pages where Docling already extracted substantial text (charts are where text is sparse)

### 2. Merge Vision API data into structured JSON
Currently Vision API results go into `image_extracted_data` as a flat list of data points. They need to be merged into the proper structured sections (social.parental_leave, social.performance_reviews, etc.) so the final `esg_data.json` has everything in one place. This could be a post-processing step or a merge prompt.

### 3. Test on diverse industry PDFs
We planned to test on 10 diverse PDFs (one per industry):
1. ✅ ADNH Catering 2024 (Food) — done
2. ⬜ Abu Dhabi Commercial Bank (Banking)
3. ⬜ ADNOC Gas (Oil & Gas)
4. ⬜ Emirates Telecom/Etisalat (Telecom)
5. ⬜ Aldar Properties (Real Estate)
6. ⬜ Burjeel Holdings (Healthcare)
7. ⬜ LULU Retail (Retail)
8. ⬜ Fujairah Building Industries (Construction)
9. ⬜ Al Ain Alahlia Insurance (Insurance)
10. ⬜ Abu Dhabi Ports (Logistics)

For each: parse → extract → I manually review PDF → tell you gaps → you update prompts.

### 4. Pipeline robustness
- The extractor's `_extract_from_images` sends each image individually to Vision API. For efficiency, batch multiple pages into single calls.
- Page 25 JPEG compression fixed the 5MB limit issue, but validate this holds across all PDFs.
- The `process_single_file` function in pdf_parser.py has a minor bug: the summary print tries to join table headers that may be integers (causes `TypeError: sequence item 0: expected str instance, int found`). Low priority.

### 5. Validated output pipeline
The `esg_validator.py` (Phase 4) may need updates to handle the new supply_chain section and the expanded social fields.

## Key files to read first
- `scripts/esg_extractor.py` — extraction prompts (lines ~140-450), section routing (ESGExtractor class), Vision API extraction
- `scripts/pdf_parser.py` — chunked parsing, chart page detection/rendering
- `data/esg_schema.json` — the schema contract
- `extracted_output_v7/0002_A_D_N_H_Catering_PLC_2025-03-17/esg_data.json` — latest extraction output

## How to run

```bash
# Parse a PDF (uses Docling chunked + chart page rendering)
python scripts/pdf_parser.py --file "data/reports/uae/SOME_COMPANY.pdf" --output-dir "./parsed_output"

# Extract ESG data (uses Claude API + Vision API for charts)  
python scripts/esg_extractor.py --input "./parsed_output/FOLDER_NAME" --output-dir "./extracted_output"

# Simulate extraction (no API key needed, uses regex)
python scripts/esg_extractor.py --input "./parsed_output/FOLDER_NAME" --simulate
```

## Environment
- Python 3.13.5 (pyenv on Windows 11)
- Packages: docling, pypdf, pypdfium2, anthropic, python-dotenv
- API: ANTHROPIC_API_KEY in .env file, model: claude-sonnet-4-20250514
