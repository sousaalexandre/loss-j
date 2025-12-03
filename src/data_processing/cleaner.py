import re
import fitz
import difflib
import src.settings as settings

from bs4 import BeautifulSoup
from pylatexenc.latex2text import LatexNodes2Text
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from src.logger import log
from src.services.llm_generator import get_llm


def apply_cleaning(pdf_path: str, markdown_content: str) -> list:
    """
    Applies full cleaning pipeline: hierarchy rebalancing, HTML table conversion, and LaTeX cleaning.
    """

    if settings.LOADER_TYPE == "pdfloader":
        log("PDFLoader selected, skipping cleaning steps.", level="info")
        return markdown_content
    

    # Step 1: Hierarchy Rebalancing (if enabled)
    if settings.ENABLE_HIERARCHY_REBUILDING:

        lines = markdown_content.split('\n')

        if settings.HIERARCHY_REBUILDING_MODE == "llm":
            log("Rebuilding hierarchy using LLM...", level="info")
            header_map: List[Dict[str, Any]] = []
            
            for i, line in enumerate(lines):
                if line.strip().startswith("#"):
                    header_map.append({"index": i, "text": line.strip()})
            
            if header_map:
                flat_headers = [h["text"] for h in header_map]
                new_headers = _rebuild_headers_with_llm(flat_headers)
                
                for i, corrected_header in enumerate(new_headers):
                    original_index = header_map[i]["index"]
                    lines[original_index] = corrected_header
        else:
            log("Rebuilding hierarchy using Font Detection...", level="info")
            rebalanced = _rebuild_headers_with_font(pdf_path, lines)
            if rebalanced:
                lines = rebalanced
        
        markdown_content = '\n'.join(lines)


    # Step 2: HTML Table Cleaning (if enabled)    
    if settings.ENABLE_HTML_CLEANING:
        log("Converting HTML tables to Markdown...", level="info")
        def replace_html_table(match):
            html_table = match.group(0)
            try:
                return _convert_html_table_to_markdown(html_table)
            except Exception as e:
                log(f"HTML table conversion failed: {e}", level="warning")
                return html_table
        
        markdown_content = re.sub(
            r'<table.*?</table>', 
            replace_html_table, 
            markdown_content, 
            flags=re.DOTALL | re.IGNORECASE
        )
    
    # Step 3: LaTeX Cleaning (if enabled)
    if settings.ENABLE_LATEX_CLEANING:
        log("Cleaning LaTeX expressions...", level="info")
        def replace_latex(match):
            latex_str = match.group(1)
            try:
                return _convert_latex_to_text(f"${latex_str}$")
            except Exception as e:
                log(f"LaTeX cleaning failed for '{latex_str}': {e}", level="warning")
                return match.group(0)
        
        markdown_content = re.sub(r'\$(.*?)\$', replace_latex, markdown_content)
    
    
    log("Cleaning pipeline complete", level="info")
    return markdown_content




## HIERARCHY REBALANCING UTILITIES
# rebalance using font detection
def _rebuild_headers_with_font(pdf_path, md):
    # --- PHASE 1: Build Font Size Map from PDF ---
    log(f"Reading PDF layout: {pdf_path}...")
    doc = fitz.open(pdf_path)
    
    # Map: "Cleaned Text" -> Font Size
    pdf_font_map = {}
    
    for page in doc:
        blocks = page.get_text("dict")["blocks"]
        for b in blocks:
            if b['type'] == 0:  # Text block
                for line in b["lines"]:
                    for span in line["spans"]:
                        text = span["text"].strip()
                        size = round(span["size"], 1)
                        
                        # Store max size found for this text (handles splitting issues)
                        if len(text) > 1:
                            if text not in pdf_font_map or size > pdf_font_map[text]:
                                pdf_font_map[text] = size

    # --- PHASE 2: Identify Markdown Headers & Get Sizes ---
    md_lines = md

    header_entries = [] # Stores (line_index, text, font_size)
    found_sizes = set()

    log("Analyzing Markdown headers...")
    for idx, line in enumerate(md_lines):
        stripped = line.strip()
        
        if stripped.startswith("#"):
            # Remove existing hashtags and cleanup
            clean_text = stripped.lstrip("#").strip()
            
            # Find size in PDF
            size = pdf_font_map.get(clean_text)
            
            # Fuzzy match fallback (OCR sometimes adds/removes spaces)
            if not size:
                matches = difflib.get_close_matches(clean_text, pdf_font_map.keys(), n=1, cutoff=0.85)
                if matches:
                    size = pdf_font_map[matches[0]]
            
            if size:
                header_entries.append({
                    "index": idx,
                    "text": clean_text,
                    "size": size
                })
                found_sizes.add(size)

    if not found_sizes:
        log("No headers matched in PDF. Check file names.", level="warning")
        return

    # --- PHASE 3: Cluster Sizes into Levels ---
    # Sort unique sizes high to low
    sorted_sizes = sorted(list(found_sizes), reverse=True)
    
    # Cluster logic: Group sizes within 1.0pt of each other
    clusters = []
    if sorted_sizes:
        current_cluster = [sorted_sizes[0]]
        for i in range(1, len(sorted_sizes)):
            if abs(sorted_sizes[i] - current_cluster[-1]) < 1.0:
                current_cluster.append(sorted_sizes[i])
            else:
                clusters.append(current_cluster)
                current_cluster = [sorted_sizes[i]]
        clusters.append(current_cluster)

    # Map Size -> Header Level (1, 2, 3...)
    size_to_level = {}
    for level_idx, cluster in enumerate(clusters):
        for s in cluster:
            # Level 1 = H1, Level 2 = H2, etc.
            size_to_level[s] = level_idx + 1

    log(f"Detected {len(clusters)} header levels based on font size.")
    log(f"Mappings: {size_to_level}")

    # --- PHASE 4: Rewrite Markdown ---
    new_lines = md_lines[:]
    
    for entry in header_entries:
        idx = entry["index"]
        size = entry["size"]
        text = entry["text"]
        
        level = size_to_level.get(size, 2) # Default to H2 if unknown
        
        # Limit to H6 (Markdown standard)
        if level > 6: level = 6
        
        # Create new header line
        new_prefix = "#" * level
        new_lines[idx] = f"{new_prefix} {text}\n"

    return new_lines



# --- LLM Structure Definition ---
class HeaderAnalysis(BaseModel):
    """Analysis for a single header line."""
    original_id: int = Field(..., description="The exact ID provided in the input.")
    suggested_prefix: str = Field(..., description="The correct markdown prefix (e.g., '#', '##', '###').")

class DocumentStructure(BaseModel):
    """The collection of analyzed headers."""
    headers: List[HeaderAnalysis] = Field(..., description="List of analyzed headers.")


def _rebuild_headers_with_llm(flat_headers: List[str]) -> List[str]:
    """Sends flat headers to LLM to infer hierarchy."""
    numbered_input = [
        {"id": i, "text": text} 
        for i, text in enumerate(flat_headers)
    ]
    input_block = str(numbered_input) # Convert list of dicts to string representation
    
    llm = get_llm()
    llm = llm.bind(temperature=0)
    llm_with_schema = llm.with_structured_output(DocumentStructure)

    prompt_template = """
    You are an expert document structurer specializing in European Portuguese texts.
    
    ### Task
    I will provide a JSON list of headers with IDs and Text.
    Your task is to determine the correct Markdown hierarchy level (#, ##, ###) for each ID based on context.
    
    ### Universal Logic
    1. **Level 1 (#):** Macro structure (Titles, Capítulos, Anexos).
    2. **Level 2 (##):** Meso structure (Specific Entities, Articles, Roles).
    3. **Level 3+ (###):** Micro structure (Attributes, Details, Recurring items like 'Local', 'Requisitos').

    ### Constraints
    - **Output ONLY the ID and the new Prefix.** Do not return the text.
    - **Strict ID Matching:** You must return an entry for every single ID in the input.
    - **No Hallucinations:** Do not invent IDs that do not exist.

    ### Input Data
    {input_data}
    """

    try:
        response = llm_with_schema.invoke(
            prompt_template.format(input_data=input_block)
        )
        
        # 3. Reconstruct the List
        # Create a map for O(1) lookup: {id: prefix}
        id_to_prefix = {item.original_id: item.suggested_prefix for item in response.headers}
        
        corrected_list = []
        for i, original_text in enumerate(flat_headers):
            # Clean the original text (remove existing # and spaces)
            clean_text = original_text.lstrip('#').strip()
            
            # Get the new prefix from LLM, default to '#' if ID missing (fallback)
            new_prefix = id_to_prefix.get(i, "#")
            
            # Rebuild string
            corrected_list.append(f"{new_prefix} {clean_text}")
            
        return corrected_list

    except Exception as e:
        print(f"Structure inference failed: {e}")
        return flat_headers





### HTML TABLE TO MARKDOWN UTILITIES
def _parse_html_table_structure(table):
    rows = table.find_all("tr")
    
    # First pass: determine grid dimensions
    max_cols = 0
    for row in rows:
        cells = row.find_all(["td", "th"])
        colspan_sum = sum(int(cell.get("colspan", 1)) for cell in cells)
        max_cols = max(max_cols, colspan_sum)
    
    # Second pass: build grid with proper cell tracking
    grid = []
    rowspan_map = {}  # Maps (row, col) to (value, remaining_rows)
    
    for r, row in enumerate(rows):
        grid_row = [None] * max_cols  # Use None to track unfilled cells
        cells = row.find_all(["td", "th"])
        col_index = 0
        
        # Fill cells that are covered by rowspans from previous rows
        for c in range(max_cols):
            if (r, c) in rowspan_map:
                value, remaining = rowspan_map[(r, c)]
                grid_row[c] = value
                if remaining > 1:
                    rowspan_map[(r, c)] = (value, remaining - 1)
                else:
                    del rowspan_map[(r, c)]
        
        # Process cells in current row
        for cell in cells:
            # Skip to next available column
            while col_index < max_cols and grid_row[col_index] is not None:
                col_index += 1
            
            if col_index >= max_cols:
                break
            
            rowspan = int(cell.get("rowspan", 1))
            colspan = int(cell.get("colspan", 1))
            value = " ".join(cell.stripped_strings)  # Can be empty string
            
            # Fill colspan cells with the same value
            for offset in range(colspan):
                if col_index + offset < max_cols:
                    grid_row[col_index + offset] = value
                    
                    # Register rowspan for future rows
                    if rowspan > 1:
                        for future_row in range(1, rowspan):
                            rowspan_map[(r + future_row, col_index + offset)] = (value, rowspan - future_row)
            
            col_index += colspan
        
        # Fill any remaining None values with empty strings
        grid_row = [cell if cell is not None else "" for cell in grid_row]
        grid.append(grid_row)
    
    return grid


def _format_table_as_markdown(table_matrix):
    md = []
    headers = table_matrix[0]
    md.append("| " + " | ".join(headers) + " |")
    md.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in table_matrix[1:]:
        md.append("| " + " | ".join(row) + " |")

    return "\n".join(md)


def _convert_html_table_to_markdown(html_table: str) -> str:
    soup = BeautifulSoup(html_table, "html.parser")
    table = soup.find("table")
    parsed = _parse_html_table_structure(table)
    markdown = _format_table_as_markdown(parsed)
    
    return markdown



## LATEX RELATED UTILITIES
def _convert_latex_to_text(latex):
    text = LatexNodes2Text().latex_to_text(latex)
    text = re.sub(r"\^\s*∘", "°", text)
    return text


if __name__ == "__main__":
    # with open('../../docs/test1.md', 'r', encoding='utf-8') as f:
    #     md = f.readlines()
    # new_lines = rebalance_headers('../../docs/test1_origin.pdf', md)
    # with open('../../docs/test1_hierarchical.md', 'w', encoding='utf-8') as f:
    #     f.writelines(new_lines)
    exit(0)