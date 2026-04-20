"""
Phase 2 — PDF Parser for Sustainability Reports
=================================================
Picks up pending tasks from the ingestion queue (pdf_watcher.py),
parses them through Docling for layout analysis + table extraction,
and stores structured output for Phase 3 LLM extraction.

Usage:
    # Process all pending PDFs:
    python pdf_parser.py --run

    # Process a specific task by ID:
    python pdf_parser.py --task-id 3

    # Process a single PDF file directly (no queue):
    python pdf_parser.py --file ./reports/acme_2024_esg.pdf

    # Dry run — show what would be processed:
    python pdf_parser.py --dry-run

    # Set batch size (default 10):
    python pdf_parser.py --run --batch-size 20

Requirements:
    pip install docling

Architecture:
    ┌─────────────┐     ┌──────────────┐     ┌─────────────────┐
    │ SQLite queue │────▶│   Docling     │────▶│ Structured      │
    │ (pending)    │     │   pipeline    │     │ output          │
    └─────────────┘     └──────────────┘     └─────────────────┘
                              │                       │
                         ┌────┴────┐           ┌──────┴──────┐
                         │ Layout  │           │ • full.json │
                         │ analysis│           │ • full.md   │
                         │   +     │           │ • tables/   │
                         │ Table-  │           │   ├ 01.csv  │
                         │ Former  │           │   ├ 02.csv  │
                         │         │           │   └ ...     │
                         └─────────┘           │ • meta.json │
                                               └─────────────┘
"""

import os
import sys
import json
import csv
import re
import time
import logging
import argparse
import traceback
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from dataclasses import dataclass, field, asdict

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("pdf_parser")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
class ParserConfig:
    """Central configuration for the parsing phase."""

    # Where the ingestion database lives (same as pdf_watcher.py)
    DB_PATH: str = "ingestion.db"

    # Base directory for parsed output
    # Structure: OUTPUT_DIR / {task_id}_{company}_{year} / ...
    OUTPUT_DIR: str = "./parsed_output"

    # How many tasks to process per batch run
    DEFAULT_BATCH_SIZE: int = 10

    # Docling settings
    ENABLE_OCR: bool = False          # Enable for scanned PDFs (slower)
    OCR_ENGINE: str = "easyocr"       # easyocr, tesseract, rapidocr
    TABLE_STRUCTURE: bool = True      # Enable TableFormer for table structure
    IMAGE_RESOLUTION: int = 300       # DPI for image export (figures/charts)

    # Chart & image enrichment settings
    # Chart extraction uses Granite Vision (3B model) — requires ~6GB RAM and
    # GPU for acceptable speed. Keep disabled for CPU-only environments.
    # Chart data is instead extracted via Claude Vision API in Phase 3.
    ENABLE_CHART_EXTRACTION: bool = False
    ENABLE_PICTURE_CLASSIFICATION: bool = False
    ENABLE_PICTURE_DESCRIPTION: bool = False

    # PDF chunking to prevent Docling memory issues on large PDFs
    # Splits PDF into N-page chunks, parses each separately, merges results
    CHUNK_PAGES: int = 15             # Max pages per Docling chunk (0 = no chunking)

    # Chunking settings for LLM-ready output
    CHUNK_BY_SECTION: bool = True     # Split markdown by section headings
    MAX_CHUNK_CHARS: int = 4000       # Max chars per chunk (for LLM context)


# ---------------------------------------------------------------------------
# Import the ingestion DB from Phase 1
# ---------------------------------------------------------------------------
# We reuse the IngestionDB class from pdf_watcher.py
# In production, you'd put this in a shared module.
# For now, we import it or recreate the minimal interface needed.

import sqlite3


class IngestionDB:
    """Minimal interface to the ingestion database (mirrors pdf_watcher.py)."""

    def __init__(self, db_path: str = ParserConfig.DB_PATH):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def get_pending_tasks(self, limit: int = 50) -> list[sqlite3.Row]:
        return self.conn.execute(
            """SELECT * FROM pdf_tasks
               WHERE status = 'pending'
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
            """UPDATE pdf_tasks
               SET status = ?, error_message = ?, updated_at = ?
               WHERE id = ?""",
            (status, error, now, task_id),
        )
        self.conn.commit()

    def update_page_count(self, task_id: int, page_count: int):
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE pdf_tasks SET page_count = ?, updated_at = ? WHERE id = ?",
            (page_count, now, task_id),
        )
        self.conn.commit()

    def get_status_summary(self) -> dict:
        rows = self.conn.execute(
            "SELECT status, COUNT(*) as cnt FROM pdf_tasks GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}


# ---------------------------------------------------------------------------
# Data structures for parsed output
# ---------------------------------------------------------------------------
@dataclass
class ParsedTable:
    """A single table extracted from the PDF."""
    table_index: int
    page_number: Optional[int]
    num_rows: int
    num_cols: int
    headers: list[str]
    data: list[list[str]]           # rows of cell values
    markdown: str                    # markdown representation
    caption: Optional[str] = None    # table caption if detected nearby
    confidence: Optional[float] = None


@dataclass
class ParsedChart:
    """A chart whose data was extracted by Docling's chart extraction."""
    chart_index: int
    page_number: Optional[int]
    chart_type: Optional[str]       # bar, pie, line, unknown
    data: list[list[str]]           # extracted tabular data (rows)
    headers: list[str]
    caption: Optional[str] = None
    image_file: Optional[str] = None  # filename of the saved chart image


@dataclass
class ParsedImage:
    """A non-chart image (infographic, diagram, etc.) saved for Vision LLM."""
    image_index: int
    page_number: Optional[int]
    classification: Optional[str]   # e.g. "infographic", "diagram", "logo", "photo"
    description: Optional[str]      # Docling-generated caption
    image_file: str                 # filename of the saved image


@dataclass
class ParsedSection:
    """A logical section of the document (heading + content)."""
    heading: str
    level: int                       # 1 = top-level, 2 = subsection, etc.
    content: str                     # text content (markdown)
    page_start: Optional[int] = None
    tables_in_section: list[int] = field(default_factory=list)  # table indices


@dataclass
class ParsedDocument:
    """Complete parsed output for one PDF."""
    task_id: Optional[int]
    file_name: str
    pdf_path: str
    page_count: int
    parse_time_seconds: float

    # Full document
    full_markdown: str
    full_json: dict

    # Structured sections
    sections: list[ParsedSection]

    # Extracted tables
    tables: list[ParsedTable]

    # Metadata enrichment from content
    detected_frameworks: list[str]   # GRI, TCFD, CSRD found in text
    detected_metrics: list[str]      # "Scope 1", "Water usage" etc. found

    # Extracted charts (data from bar/pie/line charts)
    charts: list[ParsedChart] = field(default_factory=list)

    # Non-chart images saved for Vision LLM extraction
    images: list[ParsedImage] = field(default_factory=list)

    def to_summary(self) -> dict:
        """Summary for logging and the meta.json file."""
        return {
            "task_id": self.task_id,
            "file_name": self.file_name,
            "page_count": self.page_count,
            "parse_time_seconds": round(self.parse_time_seconds, 2),
            "num_sections": len(self.sections),
            "num_tables": len(self.tables),
            "num_charts": len(self.charts),
            "num_images": len(self.images),
            "detected_frameworks": self.detected_frameworks,
            "detected_metrics": self.detected_metrics,
            "section_headings": [s.heading for s in self.sections],
        }


# ---------------------------------------------------------------------------
# Framework & metric detection (lightweight keyword scan)
# ---------------------------------------------------------------------------
FRAMEWORK_KEYWORDS = {
    "GRI": ["global reporting initiative", "gri standards", "gri index",
             "gri content index", "gri 302", "gri 305", "gri 303"],
    "TCFD": ["tcfd", "task force on climate", "climate-related financial"],
    "CSRD": ["csrd", "corporate sustainability reporting directive",
             "european sustainability reporting"],
    "SASB": ["sasb", "sustainability accounting standards"],
    "CDP": ["cdp", "carbon disclosure project"],
    "SDG": ["sustainable development goals", "sdg"],
    "ISSB": ["issb", "international sustainability standards board",
             "ifrs s1", "ifrs s2"],
}

METRIC_KEYWORDS = {
    "Scope 1 emissions": ["scope 1", "direct emissions", "direct ghg"],
    "Scope 2 emissions": ["scope 2", "indirect emissions", "purchased electricity"],
    "Scope 3 emissions": ["scope 3", "value chain emissions"],
    "Total GHG emissions": ["total emissions", "total ghg", "total greenhouse"],
    "Energy consumption": ["energy consumption", "energy usage", "total energy",
                           "electricity consumption", "mwh", "gwh"],
    "Water withdrawal": ["water withdrawal", "water consumption", "water usage",
                         "megalitres", "cubic meters of water"],
    "Waste generated": ["waste generated", "total waste", "hazardous waste",
                        "non-hazardous waste"],
    "Employee count": ["total employees", "headcount", "workforce", "fte"],
    "Gender diversity": ["gender diversity", "women in management",
                         "female representation", "gender ratio"],
    "Safety incidents": ["lost time injury", "ltifr", "trir",
                         "recordable incidents", "fatalities"],
    "Carbon intensity": ["carbon intensity", "emission intensity",
                         "co2 per revenue", "tco2e per"],
    "Renewable energy": ["renewable energy", "renewables", "solar",
                         "wind energy", "clean energy share"],
}


def detect_frameworks(text: str) -> list[str]:
    """Scan text for ESG framework mentions."""
    text_lower = text.lower()
    found = []
    for framework, keywords in FRAMEWORK_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            found.append(framework)
    return sorted(found)


def detect_metrics(text: str) -> list[str]:
    """Scan text for common sustainability metric mentions."""
    text_lower = text.lower()
    found = []
    for metric, keywords in METRIC_KEYWORDS.items():
        if any(kw in text_lower for kw in keywords):
            found.append(metric)
    return sorted(found)


# ---------------------------------------------------------------------------
# Section splitter (chunks markdown by headings)
# ---------------------------------------------------------------------------
def split_into_sections(markdown: str) -> list[ParsedSection]:
    """
    Split markdown into logical sections by heading.
    Returns a list of ParsedSection objects.
    """
    import re

    sections = []
    # Split on markdown headings (# , ## , ### )
    pattern = r'^(#{1,4})\s+(.+)$'
    lines = markdown.split('\n')

    current_heading = "Introduction"
    current_level = 1
    current_lines = []

    for line in lines:
        match = re.match(pattern, line)
        if match:
            # Save the previous section
            content = '\n'.join(current_lines).strip()
            if content or current_heading != "Introduction":
                sections.append(ParsedSection(
                    heading=current_heading,
                    level=current_level,
                    content=content,
                ))

            # Start new section
            current_level = len(match.group(1))
            current_heading = match.group(2).strip()
            current_lines = []
        else:
            current_lines.append(line)

    # Don't forget the last section
    content = '\n'.join(current_lines).strip()
    if content:
        sections.append(ParsedSection(
            heading=current_heading,
            level=current_level,
            content=content,
        ))

    return sections


# ---------------------------------------------------------------------------
# Docling parser wrapper
# ---------------------------------------------------------------------------
class DoclingParser:
    """
    Wraps the Docling DocumentConverter with sustainability-specific
    configuration and output formatting.
    """

    def __init__(self, config: ParserConfig = None):
        self.config = config or ParserConfig()
        self._converter = None

    def _init_converter(self):
        """Lazy-initialize the Docling converter (model download on first use)."""
        if self._converter is not None:
            return

        try:
            from docling.document_converter import DocumentConverter, PdfFormatOption
            from docling.datamodel.pipeline_options import PdfPipelineOptions
            from docling.datamodel.base_models import InputFormat

            pipeline_options = PdfPipelineOptions()
            pipeline_options.do_ocr = self.config.ENABLE_OCR
            pipeline_options.do_table_structure = self.config.TABLE_STRUCTURE

            # Chart & image enrichments
            if self.config.ENABLE_CHART_EXTRACTION:
                pipeline_options.do_chart_extraction = True
                pipeline_options.generate_page_images = True
                pipeline_options.generate_picture_images = True
                log.info("Chart extraction enabled (Granite Vision)")

            if self.config.ENABLE_PICTURE_CLASSIFICATION:
                pipeline_options.do_picture_classification = True
                log.info("Picture classification enabled")

            if self.config.ENABLE_PICTURE_DESCRIPTION:
                pipeline_options.do_picture_description = True
                try:
                    from docling.datamodel.pipeline_options import granite_picture_description
                    pipeline_options.picture_description_options = granite_picture_description
                    log.info("Picture description enabled (Granite Vision)")
                except ImportError:
                    log.warning("granite_picture_description not available, using default")

            if self.config.ENABLE_OCR:
                from docling.datamodel.pipeline_options import OcrOptions
                pipeline_options.ocr_options = OcrOptions(
                    engine=self.config.OCR_ENGINE
                )

            self._converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(
                        pipeline_options=pipeline_options
                    )
                }
            )
            log.info("Docling converter initialized (models loaded)")

        except ImportError:
            log.warning(
                "Docling not installed. Install with: pip install docling\n"
                "Falling back to basic text extraction (pypdf)."
            )
            self._converter = "fallback"

    def parse(self, pdf_path: str, task_id: int = None) -> ParsedDocument:
        """
        Parse a PDF through Docling and return structured output.
        Falls back to basic pypdf extraction if Docling is not available.
        """
        self._init_converter()

        start_time = time.time()
        path = Path(pdf_path)

        if not path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        if self._converter == "fallback":
            return self._parse_fallback(pdf_path, task_id, start_time)

        # Use chunked parsing for large PDFs to prevent Docling memory issues
        if self.config.CHUNK_PAGES > 0:
            try:
                from pypdf import PdfReader
                total_pages = len(PdfReader(pdf_path).pages)
            except Exception:
                total_pages = 0

            if total_pages > self.config.CHUNK_PAGES:
                log.info(
                    f"Large PDF ({total_pages} pages) — using chunked parsing "
                    f"({self.config.CHUNK_PAGES} pages per chunk)"
                )
                return self._parse_docling_chunked(pdf_path, task_id, start_time, total_pages)

        return self._parse_docling(pdf_path, task_id, start_time)

    def _parse_docling_chunked(
        self, pdf_path: str, task_id: int, start_time: float, total_pages: int
    ) -> ParsedDocument:
        """
        Split a large PDF into page chunks, parse each with Docling,
        and merge the results. Prevents memory exhaustion on large PDFs.
        """
        import tempfile
        from pypdf import PdfReader, PdfWriter

        chunk_size = self.config.CHUNK_PAGES
        reader = PdfReader(pdf_path)

        all_markdown_parts = []
        all_tables = []
        all_sections = []
        table_offset = 0

        for chunk_start in range(0, total_pages, chunk_size):
            chunk_end = min(chunk_start + chunk_size, total_pages)
            chunk_num = chunk_start // chunk_size + 1
            log.info(
                f"  Chunk {chunk_num}: pages {chunk_start + 1}-{chunk_end} "
                f"({chunk_end - chunk_start} pages)"
            )

            # Write chunk PDF to a temp file
            writer = PdfWriter()
            for page_idx in range(chunk_start, chunk_end):
                writer.add_page(reader.pages[page_idx])

            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                writer.write(tmp)
                tmp_path = tmp.name

            try:
                # Parse chunk with Docling
                result = self._converter.convert(tmp_path)
                doc = result.document

                chunk_md = doc.export_to_markdown()

                # Add page offset markers to help LLM identify page numbers
                chunk_md = f"\n<!-- Pages {chunk_start + 1}-{chunk_end} -->\n{chunk_md}"
                all_markdown_parts.append(chunk_md)

                # Extract tables with adjusted page numbers
                try:
                    for idx, table_item in enumerate(doc.tables):
                        try:
                            df = table_item.export_to_dataframe()
                            headers = list(df.columns)
                            data = df.values.tolist()
                            data_str = [
                                [str(cell) if cell is not None else "" for cell in row]
                                for row in data
                            ]

                            try:
                                table_md = table_item.export_to_markdown()
                            except Exception:
                                table_md = df.to_markdown(index=False)

                            # Adjust page number by chunk offset
                            page_num = None
                            try:
                                page_num = table_item.prov[0].page_no if table_item.prov else None
                                if page_num is not None:
                                    page_num += chunk_start
                            except (AttributeError, IndexError):
                                pass

                            all_tables.append(ParsedTable(
                                table_index=table_offset + idx,
                                page_number=page_num,
                                num_rows=len(data_str),
                                num_cols=len(headers),
                                headers=headers,
                                data=data_str,
                                markdown=table_md,
                            ))
                            log.info(
                                f"    Table {table_offset + idx}: "
                                f"{len(data_str)} rows x {len(headers)} cols"
                                f"{f' (page {page_num})' if page_num else ''}"
                            )
                        except Exception as e:
                            log.warning(f"    Table {idx} extraction failed: {e}")

                    table_offset += len(list(doc.tables)) if hasattr(doc, 'tables') else 0
                except AttributeError:
                    pass

                processed = len(result.pages) if hasattr(result, 'pages') else (chunk_end - chunk_start)
                log.info(f"    Chunk {chunk_num}: {processed} pages processed")

            except Exception as e:
                log.warning(f"    Chunk {chunk_num} failed: {e} — skipping")
            finally:
                try:
                    os.unlink(tmp_path)
                except Exception:
                    pass

        # Merge all chunks
        full_markdown = "\n\n".join(all_markdown_parts)
        sections = split_into_sections(full_markdown)

        # Detect frameworks and metrics from merged content
        text_for_scan = full_markdown[:50000]
        frameworks = detect_frameworks(text_for_scan)
        metrics = detect_metrics(text_for_scan)

        parse_time = time.time() - start_time

        log.info(
            f"  Chunked parse complete: {total_pages} pages, "
            f"{len(all_tables)} tables, {len(sections)} sections in {parse_time:.1f}s"
        )

        return ParsedDocument(
            task_id=task_id,
            file_name=Path(pdf_path).name,
            pdf_path=str(Path(pdf_path).resolve()),
            page_count=total_pages,
            parse_time_seconds=parse_time,
            full_markdown=full_markdown,
            full_json={"pages": total_pages, "chunked": True, "chunk_size": chunk_size},
            sections=sections,
            tables=all_tables,
            detected_frameworks=frameworks,
            detected_metrics=metrics,
        )

    def _parse_docling(
        self, pdf_path: str, task_id: int, start_time: float
    ) -> ParsedDocument:
        """Full Docling parsing pipeline."""
        log.info(f"Parsing with Docling: {Path(pdf_path).name}")

        result = self._converter.convert(pdf_path)
        doc = result.document

        # --- Export full document ---
        full_markdown = doc.export_to_markdown()
        full_json = doc.export_to_dict()
        page_count = len(result.pages) if hasattr(result, 'pages') else 0

        # Try to get page count from the document structure
        if page_count == 0:
            try:
                page_count = doc.num_pages
            except AttributeError:
                # Count from the JSON structure
                try:
                    page_count = full_json.get("num_pages", 0)
                except (AttributeError, TypeError):
                    pass

        # --- Validate page count against actual PDF ---
        try:
            from pypdf import PdfReader
            actual_pages = len(PdfReader(pdf_path).pages)
            if page_count < actual_pages:
                skipped = actual_pages - page_count
                log.warning(
                    f"Docling processed {page_count}/{actual_pages} pages "
                    f"({skipped} pages skipped — possible memory issue). "
                    f"Consider disabling heavy features if this persists."
                )
                page_count = actual_pages  # Report actual count in metadata
        except ImportError:
            pass
        except Exception as e:
            log.debug(f"Could not validate page count: {e}")

        # --- Extract tables ---
        tables = []
        try:
            for idx, table_item in enumerate(doc.tables):
                try:
                    # Export to dataframe for structured access
                    df = table_item.export_to_dataframe()

                    headers = list(df.columns)
                    data = df.values.tolist()

                    # Convert all values to strings for CSV compatibility
                    data_str = [
                        [str(cell) if cell is not None else "" for cell in row]
                        for row in data
                    ]

                    # Get markdown representation
                    try:
                        table_md = table_item.export_to_markdown()
                    except Exception:
                        table_md = df.to_markdown(index=False)

                    # Try to get page number
                    page_num = None
                    try:
                        page_num = table_item.prov[0].page_no if table_item.prov else None
                    except (AttributeError, IndexError):
                        pass

                    tables.append(ParsedTable(
                        table_index=idx,
                        page_number=page_num,
                        num_rows=len(data_str),
                        num_cols=len(headers),
                        headers=headers,
                        data=data_str,
                        markdown=table_md,
                    ))

                    log.info(
                        f"  Table {idx}: {len(data_str)} rows × {len(headers)} cols"
                        f"{f' (page {page_num})' if page_num else ''}"
                    )

                except Exception as e:
                    log.warning(f"  Table {idx} extraction failed: {e}")

        except AttributeError:
            log.info("  No tables attribute found in document")

        # --- Extract charts and images from pictures ---
        charts = []
        images = []
        try:
            for idx, picture in enumerate(doc.pictures):
                # Get page number
                page_num = None
                try:
                    page_num = picture.prov[0].page_no if picture.prov else None
                except (AttributeError, IndexError):
                    pass

                # Get classification (chart type, infographic, logo, etc.)
                classification = None
                try:
                    classification = getattr(picture, 'classification', None)
                    if classification and hasattr(classification, 'label'):
                        classification = classification.label
                    elif isinstance(classification, str):
                        pass
                    else:
                        classification = str(classification) if classification else None
                except Exception:
                    pass

                # Get description/caption
                description = None
                try:
                    description = getattr(picture, 'description', None)
                    if description and hasattr(description, 'text'):
                        description = description.text
                except Exception:
                    pass

                # Check if this picture has chart data extracted
                chart_data = None
                try:
                    chart_data = getattr(picture, 'chart_data', None)
                    if chart_data is None:
                        # Try export_to_dataframe for chart items
                        df = picture.export_to_dataframe()
                        if df is not None and not df.empty:
                            chart_data = df
                except Exception:
                    pass

                # Use word-boundary matching to avoid false positives
                # (e.g., "sidebar" matching "bar", "timeline" matching "line")
                _cls_lower = str(classification).lower() if classification else ""
                is_chart = (
                    chart_data is not None
                    or bool(re.search(
                        r'\b(bar\s*chart|pie\s*chart|line\s*chart|histogram|'
                        r'(?<!side)bar\s*graph|pie\s*graph|line\s*graph|'
                        r'scatter\s*plot|box\s*plot|'
                        r'\bchart\b|\bgraph\b|\bplot\b)',
                        _cls_lower
                    ))
                )

                # Save the image
                image_file = None
                try:
                    img = getattr(picture, 'image', None)
                    if img is not None:
                        pil_img = img.pil_image if hasattr(img, 'pil_image') else img
                        if hasattr(pil_img, 'save'):
                            image_file = f"picture_{idx:02d}.png"
                except Exception:
                    pass

                if is_chart and chart_data is not None:
                    try:
                        import pandas as pd
                        if isinstance(chart_data, pd.DataFrame):
                            headers = list(chart_data.columns)
                            data_rows = [
                                [str(c) if c is not None else "" for c in row]
                                for row in chart_data.values.tolist()
                            ]
                        else:
                            headers = []
                            data_rows = []

                        charts.append(ParsedChart(
                            chart_index=len(charts),
                            page_number=page_num,
                            chart_type=str(classification).lower() if classification else "unknown",
                            data=data_rows,
                            headers=headers,
                            caption=description,
                            image_file=image_file,
                        ))
                        log.info(
                            f"  Chart {len(charts)-1}: {classification or 'unknown'} "
                            f"({len(data_rows)} rows)"
                            f"{f' (page {page_num})' if page_num else ''}"
                        )
                    except Exception as e:
                        log.warning(f"  Chart {idx} data extraction failed: {e}")
                else:
                    # Non-chart image — save for Vision LLM extraction
                    # Skip logos and signatures as they don't contain ESG data
                    if classification and any(
                        skip in str(classification).lower()
                        for skip in ["logo", "signature", "decoration"]
                    ):
                        log.debug(f"  Skipping {classification} image (idx {idx})")
                        continue

                    images.append(ParsedImage(
                        image_index=len(images),
                        page_number=page_num,
                        classification=str(classification) if classification else None,
                        description=description,
                        image_file=image_file or f"picture_{idx:02d}.png",
                    ))
                    log.info(
                        f"  Image {len(images)-1}: {classification or 'unclassified'}"
                        f"{f' (page {page_num})' if page_num else ''}"
                        f"{f' — {description[:60]}...' if description and len(description) > 60 else f' — {description}' if description else ''}"
                    )

        except AttributeError:
            log.info("  No pictures attribute found in document")
        except Exception as e:
            log.warning(f"  Picture extraction failed: {e}")

        # --- Split into sections ---
        sections = split_into_sections(full_markdown)

        # --- Detect frameworks and metrics ---
        text_for_scan = full_markdown[:50000]  # First ~50K chars is enough
        frameworks = detect_frameworks(text_for_scan)
        metrics = detect_metrics(text_for_scan)

        parse_time = time.time() - start_time

        return ParsedDocument(
            task_id=task_id,
            file_name=Path(pdf_path).name,
            pdf_path=str(Path(pdf_path).resolve()),
            page_count=page_count,
            parse_time_seconds=parse_time,
            full_markdown=full_markdown,
            full_json=full_json,
            sections=sections,
            tables=tables,
            charts=charts,
            images=images,
            detected_frameworks=frameworks,
            detected_metrics=metrics,
        )

    def _parse_fallback(
        self, pdf_path: str, task_id: int, start_time: float
    ) -> ParsedDocument:
        """
        Basic fallback using pypdf when Docling is not installed.
        Extracts text only — no layout analysis or table structure.
        """
        log.info(f"Parsing with pypdf (fallback): {Path(pdf_path).name}")

        try:
            from pypdf import PdfReader
        except ImportError:
            raise RuntimeError(
                "Neither docling nor pypdf is installed. "
                "Install one: pip install docling  OR  pip install pypdf"
            )

        reader = PdfReader(pdf_path)
        page_count = len(reader.pages)

        # Extract text from all pages
        all_text = []
        for i, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            all_text.append(f"<!-- Page {i + 1} -->\n{text}")

        full_markdown = "\n\n".join(all_text)
        sections = split_into_sections(full_markdown)

        text_for_scan = full_markdown[:50000]
        frameworks = detect_frameworks(text_for_scan)
        metrics = detect_metrics(text_for_scan)

        parse_time = time.time() - start_time

        return ParsedDocument(
            task_id=task_id,
            file_name=Path(pdf_path).name,
            pdf_path=str(Path(pdf_path).resolve()),
            page_count=page_count,
            parse_time_seconds=parse_time,
            full_markdown=full_markdown,
            full_json={"pages": len(reader.pages), "fallback": True},
            sections=sections,
            tables=[],   # No table extraction in fallback mode
            detected_frameworks=frameworks,
            detected_metrics=metrics,
        )


# ---------------------------------------------------------------------------
# Output writer — saves parsed results to disk
# ---------------------------------------------------------------------------
class OutputWriter:
    """Saves ParsedDocument results to a structured folder."""

    def __init__(self, base_dir: str = ParserConfig.OUTPUT_DIR):
        self.base_dir = Path(base_dir)

    def _make_output_dir(self, parsed: ParsedDocument) -> Path:
        """Create a unique output directory for this task."""
        # Build folder name: {task_id}_{company}_{year} or {task_id}_{filename}
        parts = []
        if parsed.task_id:
            parts.append(f"{parsed.task_id:04d}")

        stem = Path(parsed.file_name).stem
        # Sanitize for filesystem
        safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in stem)
        parts.append(safe_name[:60])

        dir_name = "_".join(parts)
        out_dir = self.base_dir / dir_name
        out_dir.mkdir(parents=True, exist_ok=True)
        return out_dir

    def write(self, parsed: ParsedDocument) -> Path:
        """
        Write all parsed output to disk.

        Creates:
            {output_dir}/
            ├── meta.json          # Summary + detected frameworks/metrics
            ├── full.md            # Complete markdown (LLM-ready)
            ├── full.json          # Complete Docling JSON structure
            ├── sections/
            │   ├── 01_introduction.md
            │   ├── 02_environment.md
            │   └── ...
            └── tables/
                ├── table_00.csv
                ├── table_01.csv
                └── tables_index.json
        """
        out_dir = self._make_output_dir(parsed)

        # --- meta.json ---
        meta_path = out_dir / "meta.json"
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(parsed.to_summary(), f, indent=2, ensure_ascii=False)

        # --- full.md ---
        md_path = out_dir / "full.md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(parsed.full_markdown)

        # --- full.json ---
        json_path = out_dir / "full.json"
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(parsed.full_json, f, indent=2, ensure_ascii=False,
                      default=str)

        # --- sections/ ---
        if parsed.sections:
            sections_dir = out_dir / "sections"
            sections_dir.mkdir(exist_ok=True)

            for i, section in enumerate(parsed.sections):
                safe_heading = "".join(
                    c if c.isalnum() or c in "-_ " else ""
                    for c in section.heading
                )[:50].strip().replace(" ", "_").lower()

                section_path = sections_dir / f"{i:02d}_{safe_heading}.md"
                with open(section_path, "w", encoding="utf-8") as f:
                    f.write(f"# {section.heading}\n\n")
                    f.write(section.content)

        # --- tables/ ---
        if parsed.tables:
            tables_dir = out_dir / "tables"
            tables_dir.mkdir(exist_ok=True)

            tables_index = []
            for table in parsed.tables:
                # CSV file
                csv_path = tables_dir / f"table_{table.table_index:02d}.csv"
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(table.headers)
                    writer.writerows(table.data)

                # Markdown file
                md_path = tables_dir / f"table_{table.table_index:02d}.md"
                with open(md_path, "w", encoding="utf-8") as f:
                    f.write(table.markdown)

                tables_index.append({
                    "index": table.table_index,
                    "page": table.page_number,
                    "rows": table.num_rows,
                    "cols": table.num_cols,
                    "headers": table.headers,
                    "csv_file": csv_path.name,
                    "caption": table.caption,
                })

            # Table index
            index_path = tables_dir / "tables_index.json"
            with open(index_path, "w", encoding="utf-8") as f:
                json.dump(tables_index, f, indent=2, ensure_ascii=False)

        # --- charts/ ---
        if parsed.charts:
            charts_dir = out_dir / "charts"
            charts_dir.mkdir(exist_ok=True)

            charts_index = []
            for chart in parsed.charts:
                # CSV file with chart data
                csv_path = charts_dir / f"chart_{chart.chart_index:02d}.csv"
                with open(csv_path, "w", newline="", encoding="utf-8") as f:
                    writer_csv = csv.writer(f)
                    if chart.headers:
                        writer_csv.writerow(chart.headers)
                    writer_csv.writerows(chart.data)

                charts_index.append({
                    "index": chart.chart_index,
                    "page": chart.page_number,
                    "chart_type": chart.chart_type,
                    "rows": len(chart.data),
                    "headers": chart.headers,
                    "csv_file": csv_path.name,
                    "caption": chart.caption,
                    "image_file": chart.image_file,
                })

            index_path = charts_dir / "charts_index.json"
            with open(index_path, "w", encoding="utf-8") as f:
                json.dump(charts_index, f, indent=2, ensure_ascii=False)

        # --- images/ (non-chart images for Vision LLM) ---
        if parsed.images:
            images_dir = out_dir / "images"
            images_dir.mkdir(exist_ok=True)

            images_index = []
            for img in parsed.images:
                images_index.append({
                    "index": img.image_index,
                    "page": img.page_number,
                    "classification": img.classification,
                    "description": img.description,
                    "image_file": img.image_file,
                })

            index_path = images_dir / "images_index.json"
            with open(index_path, "w", encoding="utf-8") as f:
                json.dump(images_index, f, indent=2, ensure_ascii=False)

        # --- Render chart pages as images for Vision API extraction ---
        chart_images = self._render_chart_pages(parsed, out_dir)
        total_images = len(parsed.images) + len(chart_images)

        log.info(
            f"Output written: {out_dir}  "
            f"({len(parsed.sections)} sections, {len(parsed.tables)} tables, "
            f"{len(parsed.charts)} charts, {total_images} images)"
        )
        return out_dir

    def _render_chart_pages(self, parsed: ParsedDocument, out_dir: Path) -> list[dict]:
        """
        Detect pages with chart/image placeholders in parsed markdown,
        render those pages as PNG images, and save to images/ directory
        for Vision API extraction in Phase 3.

        Detection: Scans full.md for <!-- image --> tags near data-related
        headings (e.g., "Details of...", "Total...", chart titles).
        """
        # Heading keywords that suggest the HEADING ITSELF describes a chart.
        # Must appear in the ## heading directly before/after <!-- image -->,
        # not just anywhere in the surrounding text. Kept narrow to avoid
        # matching text-heavy pages with incidental images (logos, photos).
        DATA_HEADING_KEYWORDS = [
            "detail", "breakdown", "distribution", "percentage",
            "proportion", "by gender", "by age", "by job level",
            "by type", "by source", "by category",
            "parental leave", "performance review", "career development",
            "training hour", "new hire", "employee turnover",
            "total employee", "workforce composition",
            "scope 1", "scope 2", "scope 3", "emission",
            "energy consumption", "fuel consum", "grid electric",
            "water consumption", "water footprint",
            "waste manage", "organic waste", "recycl",
            "local supplier", "spend on", "sustainable packaging",
            "safety", "ltifr", "near miss", "incident",
            "customer satisfaction",
        ]

        if not parsed.pdf_path or parsed.page_count == 0:
            return []

        import re

        # Scan markdown to find <!-- image --> tags near data headings.
        # Estimate the PDF page number from the line position in the markdown.
        lines = parsed.full_markdown.split("\n")
        total_lines = len(lines)
        chart_pages = set()

        for i, line in enumerate(lines):
            if "<!-- image -->" not in line:
                continue

            # Only check the HEADING immediately before this image (within 3 lines).
            # This avoids matching decorative images in text-heavy sections.
            heading_context = ""
            for j in range(i - 1, max(0, i - 4), -1):
                stripped = lines[j].strip()
                if stripped.startswith("#"):
                    heading_context = stripped.lower()
                    break
                # Also check the heading after the image (chart title below)
            if not heading_context:
                for j in range(i + 1, min(len(lines), i + 3)):
                    stripped = lines[j].strip()
                    if stripped.startswith("#"):
                        heading_context = stripped.lower()
                        break

            if not heading_context:
                continue

            if not any(kw in heading_context for kw in DATA_HEADING_KEYWORDS):
                continue

            # Estimate PDF page from line position in markdown.
            # Position ratio in markdown ≈ position ratio in PDF (±2 pages).
            position_ratio = i / max(total_lines, 1)
            estimated_page = max(1, round(position_ratio * parsed.page_count))

            # Render estimated page and ±2 neighbors for safety
            for offset in [-2, -1, 0, 1, 2]:
                p = estimated_page + offset
                if 1 <= p <= parsed.page_count:
                    chart_pages.add(p)

        if not chart_pages:
            return []

        log.info(f"  Chart page detection: {len(chart_pages)} pages to render")

        if not chart_pages:
            return []

        # Render chart pages as images using pypdfium2
        chart_images = []
        try:
            import pypdfium2
            pdf_doc = pypdfium2.PdfDocument(parsed.pdf_path)

            images_dir = out_dir / "images"
            images_dir.mkdir(exist_ok=True)

            # Load existing images_index if present
            index_path = images_dir / "images_index.json"
            existing_index = []
            if index_path.exists():
                try:
                    existing_index = json.loads(index_path.read_text(encoding="utf-8"))
                except Exception:
                    pass

            img_offset = len(existing_index)

            for page_num in sorted(chart_pages):
                page_idx = page_num - 1  # 0-based
                if page_idx < 0 or page_idx >= len(pdf_doc):
                    continue

                try:
                    page = pdf_doc[page_idx]
                    # Render at 150 DPI — sufficient for chart text readability
                    bitmap = page.render(scale=150 / 72)
                    pil_image = bitmap.to_pil().convert("RGB")

                    image_file = f"chart_page_{page_num:03d}.jpg"
                    image_path = images_dir / image_file
                    pil_image.save(str(image_path), "JPEG", quality=85)

                    entry = {
                        "index": img_offset + len(chart_images),
                        "page": page_num,
                        "classification": "chart_page",
                        "description": f"Page {page_num} containing chart/graph with ESG data",
                        "image_file": image_file,
                    }
                    chart_images.append(entry)
                    log.info(f"  Rendered chart page {page_num} as {image_file}")

                except Exception as e:
                    log.warning(f"  Failed to render page {page_num}: {e}")

            pdf_doc.close()

            # Write/update images_index.json
            all_images = existing_index + chart_images
            with open(index_path, "w", encoding="utf-8") as f:
                json.dump(all_images, f, indent=2, ensure_ascii=False)

            log.info(f"  Chart page detection: {len(chart_images)} pages rendered from {len(chart_pages)} candidates")

        except ImportError:
            log.warning("pypdfium2 not installed — skipping chart page rendering")
        except Exception as e:
            log.warning(f"Chart page rendering failed: {e}")

        return chart_images


# ---------------------------------------------------------------------------
# Main processing loop
# ---------------------------------------------------------------------------
def process_task(
    task: sqlite3.Row,
    parser: DoclingParser,
    writer: OutputWriter,
    db: IngestionDB,
) -> bool:
    """
    Process a single task from the queue.
    Returns True on success, False on failure.
    """
    task_id = task["id"]
    pdf_path = task["pdf_path"]
    file_name = task["file_name"]

    log.info(f"{'-' * 60}")
    log.info(f"Processing task #{task_id}: {file_name}")

    # Mark as parsing
    db.update_status(task_id, "parsing")

    try:
        # Parse the PDF
        parsed = parser.parse(pdf_path, task_id=task_id)

        # Update page count if we got it
        if parsed.page_count > 0:
            db.update_page_count(task_id, parsed.page_count)

        # Write output files
        out_dir = writer.write(parsed)

        # Mark as parsed (ready for Phase 3 extraction)
        db.update_status(task_id, "parsed")

        summary = parsed.to_summary()
        log.info(
            f"Completed task #{task_id}: "
            f"{parsed.page_count} pages, "
            f"{len(parsed.tables)} tables, "
            f"{len(parsed.charts)} charts, "
            f"{len(parsed.images)} images, "
            f"{len(parsed.sections)} sections "
            f"in {parsed.parse_time_seconds:.1f}s"
        )
        if parsed.detected_frameworks:
            log.info(f"  Frameworks: {', '.join(parsed.detected_frameworks)}")
        if parsed.detected_metrics:
            log.info(f"  Metrics found: {', '.join(parsed.detected_metrics)}")

        return True

    except FileNotFoundError as e:
        log.error(f"Task #{task_id}: File not found — {e}")
        db.update_status(task_id, "failed", str(e))
        return False

    except Exception as e:
        error_detail = f"{type(e).__name__}: {e}"
        log.error(f"Task #{task_id}: Parse failed — {error_detail}")
        log.debug(traceback.format_exc())
        db.update_status(task_id, "failed", error_detail)
        return False


def process_single_file(
    pdf_path: str,
    parser: DoclingParser,
    writer: OutputWriter,
) -> bool:
    """Process a single PDF file directly (no queue involvement)."""
    log.info(f"{'-' * 60}")
    log.info(f"Direct parse: {pdf_path}")

    try:
        parsed = parser.parse(pdf_path)
        out_dir = writer.write(parsed)

        summary = parsed.to_summary()
        log.info(
            f"Completed: "
            f"{parsed.page_count} pages, "
            f"{len(parsed.tables)} tables, "
            f"{len(parsed.sections)} sections "
            f"in {parsed.parse_time_seconds:.1f}s"
        )
        log.info(f"Output: {out_dir}")

        # Print a nice summary
        print(f"\n{'-' * 50}")
        print(f"  Parse Results: {Path(pdf_path).name}")
        print(f"{'-' * 50}")
        print(f"  Pages:          {parsed.page_count}")
        print(f"  Sections:       {len(parsed.sections)}")
        print(f"  Tables:         {len(parsed.tables)}")
        print(f"  Charts:         {len(parsed.charts)}")
        print(f"  Images:         {len(parsed.images)}")
        print(f"  Parse time:     {parsed.parse_time_seconds:.1f}s")
        if parsed.detected_frameworks:
            print(f"  Frameworks:     {', '.join(str(f) for f in parsed.detected_frameworks)}")
        if parsed.detected_metrics:
            print(f"  Metrics found:  {', '.join(str(m) for m in parsed.detected_metrics)}")
        print(f"  Output dir:     {out_dir}")
        print()

        if parsed.tables:
            print("  Extracted tables:")
            for t in parsed.tables:
                page_info = f" (page {t.page_number})" if t.page_number else ""
                print(f"    [{t.table_index}] {t.num_rows}×{t.num_cols}{page_info}")
                if t.headers:
                    preview = ", ".join(t.headers[:5])
                    if len(t.headers) > 5:
                        preview += f" ... (+{len(t.headers) - 5} more)"
                    print(f"        Headers: {preview}")
            print()

        return True

    except Exception as e:
        log.error(f"Parse failed: {type(e).__name__}: {e}")
        log.debug(traceback.format_exc())
        return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Phase 2 — Parse sustainability PDFs with Docling",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python pdf_parser.py --run                  # Process all pending
  python pdf_parser.py --run --batch-size 50  # Process 50 at a time
  python pdf_parser.py --task-id 3            # Process specific task
  python pdf_parser.py --file report.pdf      # Parse a single file
  python pdf_parser.py --dry-run              # Preview pending tasks
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--run",
        action="store_true",
        help="Process all pending tasks from the queue",
    )
    group.add_argument(
        "--task-id",
        type=int,
        help="Process a specific task by its ID",
    )
    group.add_argument(
        "--file",
        type=str,
        help="Parse a single PDF file directly (no queue)",
    )
    group.add_argument(
        "--dry-run",
        action="store_true",
        help="Show pending tasks without processing them",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=ParserConfig.DEFAULT_BATCH_SIZE,
        help=f"Max tasks per run (default: {ParserConfig.DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--output-dir",
        default=ParserConfig.OUTPUT_DIR,
        help=f"Output directory (default: {ParserConfig.OUTPUT_DIR})",
    )
    parser.add_argument(
        "--db",
        default=ParserConfig.DB_PATH,
        help=f"Ingestion database path (default: {ParserConfig.DB_PATH})",
    )
    parser.add_argument(
        "--ocr",
        action="store_true",
        help="Enable OCR for scanned PDFs (slower)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Update config from CLI args
    ParserConfig.OUTPUT_DIR = args.output_dir
    ParserConfig.DB_PATH = args.db
    if args.ocr:
        ParserConfig.ENABLE_OCR = True

    # Initialize components
    docling_parser = DoclingParser()
    writer = OutputWriter(args.output_dir)

    # --- Direct file mode ---
    if args.file:
        success = process_single_file(args.file, docling_parser, writer)
        sys.exit(0 if success else 1)

    # --- Modes that need the DB ---
    db = IngestionDB(args.db)

    # --- Dry run ---
    if args.dry_run:
        pending = db.get_pending_tasks(limit=100)
        summary = db.get_status_summary()

        print(f"\n{'-' * 50}")
        print(f"  Queue Status")
        print(f"{'-' * 50}")
        for status, count in sorted(summary.items()):
            print(f"  {status:<14} {count:>5}")
        print()

        if pending:
            print(f"  Next {len(pending)} pending tasks:")
            for t in pending:
                company = t["company"] or "?"
                year = t["year"] or "?"
                print(f"    #{t['id']:>4}  {t['file_name'][:40]:<42} {company} ({year})")
        else:
            print("  No pending tasks.")
        print()
        return

    # --- Process specific task ---
    if args.task_id:
        task = db.get_task_by_id(args.task_id)
        if not task:
            print(f"Error: Task #{args.task_id} not found.")
            sys.exit(1)
        if task["status"] not in ("pending", "failed"):
            print(
                f"Warning: Task #{args.task_id} has status '{task['status']}'. "
                f"Processing anyway."
            )
        success = process_task(task, docling_parser, writer, db)
        sys.exit(0 if success else 1)

    # --- Batch run ---
    if args.run:
        pending = db.get_pending_tasks(limit=args.batch_size)

        if not pending:
            print("No pending tasks to process.")
            return

        log.info(
            f"Starting batch: {len(pending)} tasks "
            f"(batch_size={args.batch_size})"
        )

        results = {"success": 0, "failed": 0}
        batch_start = time.time()

        for task in pending:
            ok = process_task(task, docling_parser, writer, db)
            results["success" if ok else "failed"] += 1

        batch_time = time.time() - batch_start

        # Final summary
        print(f"\n{'=' * 50}")
        print(f"  Batch Complete")
        print(f"{'=' * 50}")
        print(f"  Processed:  {results['success'] + results['failed']}")
        print(f"  Succeeded:  {results['success']}")
        print(f"  Failed:     {results['failed']}")
        print(f"  Total time: {batch_time:.1f}s")
        if results["success"] > 0:
            print(f"  Avg time:   {batch_time / (results['success'] + results['failed']):.1f}s per PDF")
        print()

        # Show updated queue status
        summary = db.get_status_summary()
        remaining = summary.get("pending", 0)
        if remaining > 0:
            print(f"  {remaining} tasks still pending. Run again to continue.")
        print()


if __name__ == "__main__":
    main()