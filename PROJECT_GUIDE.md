# Oren ESG Data Extractor -- New Joiner Guide

Welcome! This document explains **what this project does**, **how it works end-to-end**, and **how each piece fits together**. By the time you finish reading, you should be able to run the pipeline, understand every phase, and know where to look when something breaks.

---

## Table of Contents

1. [What Does This Project Do?](#1-what-does-this-project-do)
2. [The Big Picture -- Pipeline Overview](#2-the-big-picture----pipeline-overview)
3. [Getting Started -- Setup](#3-getting-started----setup)
4. [Phase 0 -- Report Sourcing (Manifest + Download)](#4-phase-0----report-sourcing-manifest--download)
5. [Phase 1 -- PDF Ingestion (Watcher)](#5-phase-1----pdf-ingestion-watcher)
6. [Phase 2 -- PDF Parsing (Docling)](#6-phase-2----pdf-parsing-docling)
7. [Phase 3 -- LLM Extraction (Claude)](#7-phase-3----llm-extraction-claude)
8. [Phase 4 -- Validation & QA](#8-phase-4----validation--qa)
9. [Running the Full Pipeline](#9-running-the-full-pipeline)
10. [Project Structure -- File by File](#10-project-structure----file-by-file)
11. [Data Flow -- What Goes Where](#11-data-flow----what-goes-where)
12. [The ESG Schema](#12-the-esg-schema)
13. [The SQLite Database (ingestion.db)](#13-the-sqlite-database-ingestiondb)
14. [Configuration & Environment Variables](#14-configuration--environment-variables)
15. [Dependencies](#15-dependencies)
16. [Common Tasks & Troubleshooting](#16-common-tasks--troubleshooting)

---

## 1. What Does This Project Do?

Companies listed on the **Abu Dhabi Securities Exchange (ADX)** publish annual **ESG (Environmental, Social, Governance) sustainability reports** as PDF files. These PDFs contain critical data like greenhouse gas emissions, water usage, employee diversity, and board governance metrics -- but the data is locked inside unstructured text, tables, charts, and infographics.

**This project automatically:**

1. Downloads ESG report PDFs from the ADX website
2. Parses them into structured text, tables, chart data, and images using AI-powered document understanding (IBM Docling)
3. Extracts specific ESG metrics using a Large Language Model (Claude by Anthropic)
4. Validates the extracted data for accuracy, completeness, and plausibility
5. Outputs clean, structured JSON and CSV files ready for analysis or database loading

**In simple terms:** PDF in --> structured ESG data out.

---

## 2. The Big Picture -- Pipeline Overview

The pipeline has **5 phases** (Phase 0-4), each handled by a dedicated script. They run sequentially, and each phase reads from the previous phase's output:

```
Phase 0: SOURCING           Phase 1: INGESTION         Phase 2: PARSING
+-------------------+       +-------------------+       +-------------------+
| fetch_manifest.py |       | pdf_watcher.py    |       | pdf_parser.py     |
| download_reports  |  -->  | Scans folders     |  -->  | Docling AI engine |
|   .py             |       | Deduplicates      |       | Tables, charts,   |
| Downloads PDFs    |       | Queues to SQLite  |       | images, sections  |
+-------------------+       +-------------------+       +-------------------+
                                                                  |
                                                                  v
Phase 4: VALIDATION          Phase 3: EXTRACTION
+-------------------+       +-------------------+
| esg_validator.py  |       | esg_extractor.py  |
| 7-layer QA checks |  <--  | Claude LLM reads  |
| Unit normalization|       | parsed content     |
| Completeness score|       | + chart data       |
| flat_kpis.csv     |       | + image Vision AI  |
+-------------------+       | Outputs JSON       |
                             +-------------------+
```

**One script to rule them all:** `run_pipeline.py` orchestrates all phases in sequence.

---

## 3. Getting Started -- Setup

### Prerequisites

- Python 3.11+ (we use 3.13)
- Git
- An Anthropic API key (for Claude LLM extraction) -- or use `--simulate` mode for testing

### First-Time Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd orenESGDataExtractor

# 2. Create a virtual environment
python -m venv venv

# 3. Activate it
# Windows (Git Bash / PowerShell):
source venv/Scripts/activate
# macOS/Linux:
source venv/bin/activate

# 4. Install dependencies
pip install -r requirements-dev.txt

# 5. Install Docling (the PDF parsing engine)
pip install docling

# 6. Set up your environment
cp .env.example .env
# Then edit .env and fill in:
#   ANTHROPIC_API_KEY=sk-ant-...
#   REPORT_SCAN_DIR=data/reports/uae
#   ADX_COOKIE=<from browser DevTools>
```

### Quick Smoke Test

```bash
# Run the full pipeline on 2 PDFs in simulation mode (no API key needed):
python scripts/run_pipeline.py --scan-dir ./data/reports/uae --limit 2 --simulate
```

This will:
- Scan the `data/reports/uae/` folder for PDFs
- Parse them with Docling
- Run a simulated (regex-based) extraction instead of calling Claude
- Validate the output
- Produce files in `parsed_output/`, `extracted_output/`, and `validated_output/`

---

## 4. Phase 0 -- Report Sourcing (Manifest + Download)

**Goal:** Get the PDF files onto your local machine.

**Scripts:** `fetch_manifest.py` then `download_reports.py`

### How it works:

**Step 1: Build the manifest**

```bash
python scripts/fetch_manifest.py
```

- Reads `data/row.json` -- a JSON file containing metadata about all ADX-listed company sustainability reports (company name, report title, download URL, published date)
- Creates/updates the `adx_manifest` table in `ingestion.db` (SQLite)
- Each report gets a row with status `pending`

**Where does `row.json` come from?** It was captured from the ADX (Abu Dhabi Securities Exchange) API. The API returns a list of all published sustainability/ESG disclosure documents for listed companies.

**Step 2: Download the PDFs**

```bash
python scripts/download_reports.py               # download all pending
python scripts/download_reports.py --limit 10     # download 10 at a time
python scripts/download_reports.py --dry-run      # preview without downloading
python scripts/download_reports.py --retry        # also retry previously failed
```

- Reads pending rows from the `adx_manifest` table
- Downloads each PDF into `data/reports/uae/` (or configured directory)
- Uses `curl_cffi` (not `requests`) because the ADX website requires browser-like TLS fingerprinting
- Updates the manifest row with `status = 'downloaded'` and the local file path
- Safe to interrupt and resume -- already-downloaded files are skipped

### Key files:

| File | Purpose |
|---|---|
| `data/row.json` | Raw metadata from ADX API (input to manifest builder) |
| `scripts/fetch_manifest.py` | Parses row.json --> inserts into `adx_manifest` table |
| `scripts/download_reports.py` | Downloads PDFs from URLs in `adx_manifest` table |
| `data/reports/uae/*.pdf` | The downloaded PDF reports |

---

## 5. Phase 1 -- PDF Ingestion (Watcher)

**Goal:** Discover PDF files, deduplicate them, extract basic metadata, and queue them for processing.

**Script:** `pdf_watcher.py`

### How it works:

```bash
# Batch mode -- scan a folder and queue all new PDFs:
python scripts/pdf_watcher.py --scan ./data/reports/uae

# Watch mode -- monitor folder in real-time for new files:
python scripts/pdf_watcher.py --watch ./data/reports/uae

# Check queue status:
python scripts/pdf_watcher.py --status
```

1. **Scans** the specified directory recursively for `.pdf` files
2. **Deduplicates** using SHA-256 hash -- if the exact same file was already queued, it's skipped
3. **Extracts basic metadata** from the filename:
   - Company name (parsed from filename pattern: `CompanyName_YYYY-MM-DD.pdf`)
   - Year (from the date in the filename)
   - File size
4. **Inserts** a new row into the `pdf_tasks` table in `ingestion.db` with status `pending`

### The `pdf_tasks` table:

This is the **central queue** that tracks every PDF through the entire pipeline:

| Column | Example | Description |
|---|---|---|
| `id` | 5 | Auto-incrementing task ID |
| `file_name` | `ADNOC_GAS_PLC_2025-07-09.pdf` | Original filename |
| `pdf_path` | `/full/path/to/file.pdf` | Absolute path on disk |
| `sha256` | `a1b2c3...` | File hash for deduplication |
| `company` | `ADNOC GAS PLC` | Parsed from filename |
| `year` | `2025` | Parsed from filename |
| `status` | `pending` | Current pipeline stage (see below) |
| `page_count` | `48` | Filled in during Phase 2 |
| `error_message` | `null` | Error details if failed |

### Status lifecycle:

```
pending --> parsing --> parsed --> extracting --> extracted --> validating --> validated
                |                      |                           |
                v                      v                           v
              failed                 failed                   review_needed
```

---

## 6. Phase 2 -- PDF Parsing (Docling)

**Goal:** Convert raw PDFs into structured, machine-readable text, tables, chart data, and images.

**Script:** `pdf_parser.py`

### How it works:

```bash
# Process all pending tasks:
python scripts/pdf_parser.py --run

# Process a specific task:
python scripts/pdf_parser.py --task-id 5

# Parse a single file directly (no queue):
python scripts/pdf_parser.py --file ./data/reports/uae/some_report.pdf

# Preview what would be processed:
python scripts/pdf_parser.py --dry-run

# Enable OCR for scanned documents:
python scripts/pdf_parser.py --run --ocr
```

### What is Docling?

[Docling](https://github.com/DS4SD/docling) is an open-source AI-powered document understanding library by IBM. It:

- Understands PDF layout (headings, paragraphs, columns)
- Extracts tables with high accuracy using **TableFormer** (97.9% cell accuracy on ESG reports)
- Exports to Markdown (for LLM input) and JSON (for structured access)
- Generates a table of contents from document structure

### Chart & Image Extraction (NEW)

ESG reports often embed key data in charts and infographics. The parser now handles these:

**Chart extraction** (enabled by default):
- Uses IBM's **Granite Vision** model to understand bar charts, pie charts, and line charts
- Converts visual chart data into structured CSV files (just like tables)
- Example: A pie chart showing "Energy Mix: 40% Natural Gas, 35% Solar, 25% Grid" becomes a CSV with those values

**Picture classification**:
- Classifies every image in the PDF: chart, infographic, diagram, logo, signature, photo
- Logos and signatures are automatically skipped (no ESG data)
- Infographics and diagrams are saved for Vision LLM extraction in Phase 3

**Picture description**:
- Generates text captions for images using Granite Vision
- These captions help the LLM in Phase 3 understand what the image shows

### Output structure:

For each PDF, the parser creates a folder in `parsed_output/`:

```
parsed_output/0005_Abu_Dhabi_Commercial_Bank_2024-04-26/
|
|-- meta.json              # Summary: page count, frameworks detected, metrics found
|-- full.md                # Complete document as Markdown (LLM-ready)
|-- full.json              # Complete Docling JSON structure
|
|-- sections/              # Document split by headings
|   |-- 00_introduction.md
|   |-- 01_environment.md
|   |-- 02_emissions_management.md
|   +-- ...
|
|-- tables/                # Every table extracted as CSV + Markdown
|   |-- table_00.csv       # CSV (for programmatic use)
|   |-- table_00.md        # Markdown (for human review)
|   |-- table_01.csv
|   +-- tables_index.json  # Index: page number, headers, row count
|
|-- charts/                # Data extracted from charts (NEW)
|   |-- chart_00.csv       # Bar/pie/line chart data as CSV
|   |-- chart_01.csv
|   +-- charts_index.json  # Index: chart type, caption, page
|
+-- images/                # Non-chart images for Vision LLM (NEW)
    +-- images_index.json  # Index: classification, description, page
```

### Framework & Metric Detection

The parser does a lightweight keyword scan of the text to detect:

- **Frameworks used**: GRI, TCFD, CSRD, SASB, CDP, SDG, ISSB
- **Metrics present**: Scope 1/2/3 emissions, energy consumption, water withdrawal, waste, employee count, gender diversity, safety incidents, carbon intensity, renewable energy

This helps Phase 3 know what to focus on when extracting data.

---

## 7. Phase 3 -- LLM Extraction (Claude)

**Goal:** Read the parsed content and extract structured ESG metrics using AI.

**Script:** `esg_extractor.py`

### How it works:

```bash
# Extract from a specific parsed folder:
python scripts/esg_extractor.py --input ./parsed_output/0005_Abu_Dhabi_Commercial_Bank_2024-04-26

# Process all parsed tasks:
python scripts/esg_extractor.py --run

# Simulation mode (no API key, uses regex-based extraction):
python scripts/esg_extractor.py --run --simulate

# Use a specific Claude model:
python scripts/esg_extractor.py --run --model claude-sonnet-4-20250514
```

### The extraction process:

1. **Loads** the parsed `full.md`, all table CSVs, and all chart CSVs
2. **Builds targeted prompts** for 4 sections:
   - `report_metadata` -- company name, year, frameworks, assurance level
   - `environmental` -- GHG emissions (Scope 1/2/3), energy, water, waste
   - `social` -- workforce, diversity, health & safety, training
   - `governance` -- board composition, ethics, data privacy
3. **Calls Claude** with a strict system prompt that enforces:
   - Extract ONLY explicitly stated data (never infer)
   - Include confidence scores (1.0 = from a table, 0.6 = from chart description)
   - Include units and page references
   - Respond with valid JSON only
4. **Vision LLM extraction** (NEW): For non-chart images (infographics, diagrams), sends each image to Claude Vision API to extract any visible ESG data points
5. **Validates** the extracted data with basic range/cross-reference checks
6. **Writes** the output

### How the LLM prompt works (example):

For the environmental section, the prompt says:

> "Extract ALL environmental sustainability data from this report content. Return a JSON object with this structure: `{ ghg_emissions: { scope_1: { total: { value: <number>, unit: "<unit>", year: <year>, confidence: <0-1> } } }, energy: { ... }, water: { ... }, waste: { ... } }`"

The LLM then reads the markdown content + table CSVs + chart CSVs and fills in the JSON structure with actual values from the report.

### Simulation mode:

If you don't have an Anthropic API key, `--simulate` mode uses **regex-based extraction** instead of calling the LLM. It's less accurate but lets you test the full pipeline without any API costs. The regex patterns look for common patterns like "Scope 1...12,345 tCO2e".

### Output structure:

```
extracted_output/0005_Abu_Dhabi_Commercial_Bank_2024-04-26/
|
|-- esg_data.json          # Complete extracted ESG data (all 4 sections)
|-- summary.json           # Quick overview: company, year, field count, flags
+-- review_flags.json      # Items needing human review (if any)
```

### Example `esg_data.json` (simplified):

```json
{
  "report_metadata": {
    "company_name": "Abu Dhabi Commercial Bank",
    "reporting_year": 2023,
    "frameworks_used": ["GRI", "TCFD", "SDG"],
    "assurance_level": "limited"
  },
  "environmental": {
    "ghg_emissions": {
      "scope_1": {
        "total": {
          "value": 12345,
          "unit": "tCO2e",
          "year": 2023,
          "prior_year_value": 13000,
          "prior_year": 2022,
          "confidence": 1.0,
          "page_reference": 42
        }
      }
    },
    "energy": {
      "total_consumption": { "value": 250000, "unit": "MWh", "confidence": 0.95 },
      "renewable_share_pct": 15.2
    }
  },
  "social": { ... },
  "governance": { ... },
  "image_extracted_data": [
    {
      "metric_name": "Water Consumption by Region",
      "value": 1200,
      "unit": "megalitres",
      "year": 2023,
      "category": "environmental",
      "confidence": 0.7,
      "source_type": "infographic",
      "source_image": "picture_03.png",
      "page": 28
    }
  ],
  "extraction_metadata": {
    "extraction_model": "claude-sonnet-4-20250514",
    "fields_extracted": 47,
    "image_data_points": 3,
    "needs_review": false
  }
}
```

---

## 8. Phase 4 -- Validation & QA

**Goal:** Validate the extracted data for accuracy, normalize units, score completeness, and flag items needing human review.

**Script:** `esg_validator.py`

### How it works:

```bash
# Validate a single extraction:
python scripts/esg_validator.py --input ./extracted_output/0005_Abu_Dhabi_Commercial_Bank_2024-04-26

# Validate all extracted tasks:
python scripts/esg_validator.py --run

# Show human review queue:
python scripts/esg_validator.py --review-queue

# Export as flat CSV:
python scripts/esg_validator.py --input <folder> --export-csv
```

### The 7 validation layers:

| Layer | What it checks | Example |
|---|---|---|
| 1. Schema conformance | Fields match the ESG schema structure | Is `scope_1.total.value` a number? |
| 2. Range plausibility | Values fall within physically possible ranges | Scope 1 emissions between 0 and 500M tCO2e |
| 3. Cross-reference | Internal consistency between related values | Scope 1 + 2 + 3 should roughly equal Total |
| 4. YoY anomaly detection | Year-over-year change > 50% is flagged | Emissions jumped 200%? Flag for review |
| 5. Unit normalization | Standardizes units across reports | "thousand tonnes CO2" --> tCO2e |
| 6. Completeness scoring | Scores how many expected fields are present | Environmental: 8/12 fields = 67% |
| 7. Confidence review | Flags low-confidence extractions | Confidence < 0.7 --> needs human review |

### Output structure:

```
validated_output/0005_Abu_Dhabi_Commercial_Bank_2024-04-26/
|
|-- esg_validated.json     # Cleaned + normalized data
|-- validation_report.json # Full QA report: score, pass/fail, all flags
|-- review_queue.json      # Items needing human review (if any)
+-- flat_kpis.csv          # Flat table ready for database import
```

### The `flat_kpis.csv` format:

This is the **final deliverable** -- a simple flat table with one row per KPI:

| company | year | pillar | category | metric | value | unit | confidence | page |
|---|---|---|---|---|---|---|---|---|
| ADCB | 2023 | environmental | ghg_emissions | scope_1_total | 12345 | tCO2e | 1.0 | 42 |
| ADCB | 2023 | environmental | energy | renewable_share_pct | 15.2 | % | 0.95 | 38 |
| ADCB | 2023 | social | diversity | women_management_pct | 28.5 | % | 0.9 | 55 |

---

## 9. Running the Full Pipeline

The **easiest way** to run everything is the orchestrator script:

```bash
# Full pipeline (real LLM extraction):
python scripts/run_pipeline.py --scan-dir ./data/reports/uae --limit 5

# Full pipeline (simulated, no API key):
python scripts/run_pipeline.py --scan-dir ./data/reports/uae --limit 5 --simulate

# Full pipeline (verbose logging):
python scripts/run_pipeline.py --scan-dir ./data/reports/uae --limit 5 --simulate -v
```

`run_pipeline.py` runs all 4 phases in order:

```
Phase 1: SCAN     -- discovers and queues PDFs from the scan directory
Phase 2: PARSE    -- runs Docling on all pending PDFs
Phase 3: EXTRACT  -- runs Claude LLM on all parsed PDFs
Phase 4: VALIDATE -- validates all extracted data
```

### What the output looks like:

```
===============================================
  Pipeline Complete
===============================================
  Scan:       95 scanned, 5 new queued
  Parse:      5 parsed, 0 failed
  Extract:    5 extracted, 0 failed
  Validate:   4 PASS, 1 REVIEW, 0 FAIL, 0 errors
  Total time: 342.1s
  LLM calls:  20
  Tokens:     45,230 in / 12,890 out

  Queue Status:
    extracted          5
    parsed            10
    pending           80
    validated          4
    review_needed      1
```

---

## 10. Project Structure -- File by File

```
orenESGDataExtractor/
|
|-- .env                        # Your local secrets (never committed)
|-- .env.example                # Template -- copy to .env, fill in values
|-- .gitignore                  # Git ignore rules
|-- requirements.txt            # Production Python dependencies
|-- requirements-dev.txt        # Dev/test dependencies (includes production)
|-- pytest.ini                  # Pytest configuration
|-- ingestion.db                # SQLite database (the pipeline queue)
|-- PROJECT_GUIDE.md            # This file
|
|-- data/
|   |-- row.json                # ADX report metadata (input to manifest)
|   |-- esg_schema.json         # The ESG data schema (contract for extraction)
|   +-- reports/
|       +-- uae/                # Downloaded PDF reports (one per company/date)
|           |-- ADNOC_GAS_PLC_2025-07-09.pdf
|           |-- Abu_Dhabi_Commercial_Bank_2024-04-26.pdf
|           +-- ... (~95 PDFs)
|
|-- scripts/                    # All pipeline scripts
|   |-- fetch_manifest.py       # Phase 0a: row.json --> adx_manifest table
|   |-- download_reports.py     # Phase 0b: Downloads PDFs from ADX
|   |-- pdf_watcher.py          # Phase 1: Scans folders, deduplicates, queues PDFs
|   |-- pdf_parser.py           # Phase 2: Docling parsing (text, tables, charts, images)
|   |-- esg_extractor.py        # Phase 3: Claude LLM extraction + Vision AI
|   |-- esg_validator.py        # Phase 4: 7-layer validation + flat CSV export
|   |-- run_pipeline.py         # Orchestrator: runs phases 1-4 in sequence
|   +-- reset_status.py         # Utility: reset task statuses for re-processing
|
|-- src/oren_esg/               # Shared Python package
|   |-- __init__.py             # Package init + version
|   |-- config.py               # Loads .env, exposes `settings` singleton
|   |-- logging_config.py       # Console + file logging setup
|   |-- pipeline.py             # Generic ETL pipeline base (extract->transform->load)
|   |-- extractors/             # Base classes for data source extractors
|   |-- transformers/           # Base classes for data transformations
|   +-- loaders/                # Base classes for data destinations
|
|-- parsed_output/              # Phase 2 output (one folder per PDF)
|   +-- 0005_Abu_Dhabi_Commercial_Bank_2024-04-26/
|       |-- meta.json, full.md, full.json
|       |-- sections/, tables/, charts/, images/
|
|-- extracted_output/           # Phase 3 output (one folder per PDF)
|   +-- 0005_Abu_Dhabi_Commercial_Bank_2024-04-26/
|       |-- esg_data.json, summary.json
|
|-- validated_output/           # Phase 4 output (one folder per PDF)
|   +-- 0005_Abu_Dhabi_Commercial_Bank_2024-04-26/
|       |-- esg_validated.json, validation_report.json, flat_kpis.csv
|
+-- tests/                      # Unit tests
    |-- conftest.py             # Shared fixtures
    +-- test_extractors/, test_transformers/, test_loaders/
```

---

## 11. Data Flow -- What Goes Where

Here's exactly what data moves between phases:

```
row.json (ADX metadata)
    |
    v
[fetch_manifest.py] --> ingestion.db:adx_manifest table
    |
    v
[download_reports.py] --> data/reports/uae/*.pdf
    |
    v
[pdf_watcher.py] --> ingestion.db:pdf_tasks table (status: pending)
    |
    v
[pdf_parser.py] --> parsed_output/{id}_{company}/
    |                   |-- full.md (text)
    |                   |-- tables/*.csv (table data)
    |                   |-- charts/*.csv (chart data)    <-- NEW
    |                   |-- images/ (infographics)       <-- NEW
    |                   +-- meta.json (frameworks, metrics detected)
    |
    v
[esg_extractor.py] --> extracted_output/{id}_{company}/
    |                   |-- esg_data.json (structured ESG metrics)
    |                   +-- summary.json
    |
    v
[esg_validator.py] --> validated_output/{id}_{company}/
                        |-- esg_validated.json (cleaned data)
                        |-- validation_report.json (QA report)
                        +-- flat_kpis.csv (DB-ready flat table)
```

---

## 12. The ESG Schema

The file `data/esg_schema.json` defines the **contract** between the parser and the extractor. It specifies every field the LLM should try to extract.

### Top-level structure:

```
esg_schema.json
|
|-- report_metadata        # Who wrote the report, what year, what frameworks
|-- environmental          # All environmental KPIs
|   |-- ghg_emissions      # Scope 1, 2, 3, total, intensity, targets
|   |-- energy             # Consumption, renewable share, intensity
|   |-- water              # Withdrawal, consumption, recycling
|   |-- waste              # Total, hazardous, recycling rate
|   +-- biodiversity       # Land use, protected areas
|
|-- social                 # All social KPIs
|   |-- workforce          # Headcount, hires, turnover
|   |-- diversity           # Gender percentages, pay gap
|   |-- health_safety      # Fatalities, LTIFR, TRIR
|   |-- training           # Hours per employee
|   +-- human_rights       # Policies, grievance mechanisms
|
+-- governance             # All governance KPIs
    |-- board_composition  # Size, independence, diversity
    |-- ethics_compliance  # Anti-corruption, whistleblower
    +-- data_privacy       # Breaches
```

### The `metric_value` pattern:

Every numeric KPI follows the same structure:

```json
{
  "value": 12345,
  "unit": "tCO2e",
  "year": 2023,
  "prior_year_value": 13000,
  "prior_year": 2022,
  "confidence": 1.0,
  "page_reference": 42,
  "methodology_note": "GHG Protocol, operational control"
}
```

This ensures consistency -- every number always has its unit, year, and confidence level.

### Supported ESG frameworks:

The schema is **framework-agnostic** but maps to:

| Framework | Full Name | What it covers |
|---|---|---|
| GRI | Global Reporting Initiative | Comprehensive ESG reporting standards |
| TCFD | Task Force on Climate-related Financial Disclosures | Climate risk and opportunity |
| CSRD/ESRS | Corporate Sustainability Reporting Directive | EU mandatory sustainability reporting |
| SASB | Sustainability Accounting Standards Board | Industry-specific ESG metrics |
| CDP | Carbon Disclosure Project | Climate, water, forests questionnaires |
| ISSB/IFRS S1-S2 | International Sustainability Standards Board | Global sustainability disclosure baseline |
| SDG | UN Sustainable Development Goals | 17 global goals |
| GHG Protocol | Greenhouse Gas Protocol | Standard for emissions accounting |

---

## 13. The SQLite Database (ingestion.db)

`ingestion.db` is a lightweight SQLite database that acts as the **pipeline's task queue and state tracker**. It has two main tables:

### `adx_manifest` (Phase 0)

Tracks report downloads from ADX:

| Column | Purpose |
|---|---|
| `entity` | Company identifier |
| `entity_name_en` | Company name in English |
| `url_en` | Download URL |
| `filename` | Generated filename |
| `local_path` | Path after download |
| `status` | pending / downloaded / failed |

### `pdf_tasks` (Phases 1-4)

Tracks each PDF through the entire pipeline:

| Column | Purpose |
|---|---|
| `id` | Unique task ID (used in folder names: `0005_...`) |
| `file_name` | Original PDF filename |
| `pdf_path` | Absolute path to the PDF |
| `sha256` | File hash (for deduplication) |
| `company` | Company name (parsed from filename) |
| `year` | Report year (parsed from filename) |
| `status` | Current pipeline stage |
| `page_count` | Number of pages (set during parsing) |
| `error_message` | Error details if any phase failed |
| `ingested_at` | When the PDF was first queued |
| `updated_at` | Last status change |

### Useful queries:

```sql
-- See pipeline progress:
SELECT status, COUNT(*) FROM pdf_tasks GROUP BY status;

-- Find failed tasks:
SELECT id, file_name, error_message FROM pdf_tasks WHERE status = 'failed';

-- Reset a failed task for re-processing:
UPDATE pdf_tasks SET status = 'pending', error_message = NULL WHERE id = 5;
```

You can browse the database with any SQLite tool (DB Browser for SQLite, `sqlite3` CLI, or the `reset_status.py` utility script).

---

## 14. Configuration & Environment Variables

All configuration comes from environment variables loaded via `.env`:

| Variable | Required | Purpose |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes (or use `--simulate`) | API key for Claude LLM |
| `REPORT_SCAN_DIR` | Yes (or pass `--scan-dir`) | Folder to scan for PDFs |
| `ADX_COOKIE` | For downloads only | Browser cookie for ADX API auth |
| `LOG_LEVEL` | No (default: `INFO`) | Logging verbosity: DEBUG, INFO, WARNING |
| `LOG_DIR` | No (default: `logs`) | Where log files are written |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | For PostgreSQL export only | PostgreSQL connection details |

### How config is loaded:

```python
# In src/oren_esg/config.py:
from dotenv import load_dotenv
load_dotenv()   # reads .env file

# Then in any script:
from oren_esg.config import settings
print(settings.log_level)  # "INFO"
```

---

## 15. Dependencies

### Production (`requirements.txt`)

| Package | Why |
|---|---|
| `python-dotenv` | Loads `.env` file into environment variables |
| `requests` | HTTP client (general purpose) |
| `curl_cffi` | HTTP client with TLS fingerprinting (needed for ADX downloads) |
| `pandas` | DataFrame operations (table/chart data processing) |
| `SQLAlchemy` | SQL toolkit (for potential PostgreSQL export) |
| `psycopg2-binary` | PostgreSQL driver |

### Additional (install separately)

| Package | Why |
|---|---|
| `docling` | PDF parsing engine (IBM) -- AI-powered layout analysis, table extraction, chart extraction |
| `anthropic` | Claude API client -- for LLM-based ESG data extraction |
| `watchdog` | (Optional) File system monitoring for real-time PDF watching |

### Development (`requirements-dev.txt`)

| Package | Why |
|---|---|
| `pytest` | Test runner |
| `pytest-cov` | Code coverage |
| `pytest-mock` | Mocking utilities |

---

## 16. Common Tasks & Troubleshooting

### "I want to re-process a specific PDF"

```bash
# Reset its status to pending:
python scripts/reset_status.py --task-id 5

# Or manually:
sqlite3 ingestion.db "UPDATE pdf_tasks SET status='pending', error_message=NULL WHERE id=5;"

# Then re-run the pipeline:
python scripts/run_pipeline.py --scan-dir ./data/reports/uae --limit 1
```

### "The parser is slow"

Docling downloads AI models on first run (~2-3 GB). Subsequent runs are much faster. Parsing time scales linearly: ~1 second per page for text+tables, slower with chart extraction enabled.

To disable chart/image extraction for faster processing, set in `pdf_parser.py`:

```python
ParserConfig.ENABLE_CHART_EXTRACTION = False
ParserConfig.ENABLE_PICTURE_CLASSIFICATION = False
ParserConfig.ENABLE_PICTURE_DESCRIPTION = False
```

### "I don't have an API key"

Use simulation mode: `--simulate`. It uses regex patterns instead of Claude and works without any API key. The extracted data will be less accurate but the pipeline runs end-to-end.

### "How do I add a new country?"

1. Create a folder: `data/reports/new_country/`
2. Place PDFs in it (or update `download_reports.py` to download from a new source)
3. Run: `python scripts/run_pipeline.py --scan-dir ./data/reports/new_country --limit 5`

### "How do I check extraction quality?"

Look at `validated_output/{folder}/validation_report.json` -- it contains:
- Overall score (0-100)
- Pass/Review/Fail verdict
- Detailed flags for every issue found
- Completeness breakdown by pillar (environmental, social, governance)

### "What data is being lost from charts/images?"

Check `parsed_output/{folder}/charts/` and `parsed_output/{folder}/images/images_index.json`. The charts folder contains CSV data extracted from charts. The images index shows which images were classified and sent for Vision LLM extraction.

### Key log locations:

| Log | Location |
|---|---|
| Pipeline log | `logs/pipeline.log` |
| Console output | Standard output (all scripts log to console) |
| Per-task errors | `ingestion.db` --> `pdf_tasks.error_message` |
| Validation details | `validated_output/{folder}/validation_report.json` |

---

## Quick Reference -- Running Each Phase Independently

| Phase | Script | Command |
|---|---|---|
| 0a. Build manifest | `fetch_manifest.py` | `python scripts/fetch_manifest.py` |
| 0b. Download PDFs | `download_reports.py` | `python scripts/download_reports.py --limit 10` |
| 1. Ingest | `pdf_watcher.py` | `python scripts/pdf_watcher.py --scan ./data/reports/uae` |
| 2. Parse | `pdf_parser.py` | `python scripts/pdf_parser.py --run` |
| 3. Extract | `esg_extractor.py` | `python scripts/esg_extractor.py --run --simulate` |
| 4. Validate | `esg_validator.py` | `python scripts/esg_validator.py --run` |
| All at once | `run_pipeline.py` | `python scripts/run_pipeline.py --scan-dir ./data/reports/uae --limit 5 --simulate` |
