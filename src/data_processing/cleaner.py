import re
import fitz
import difflib
from bs4 import BeautifulSoup
from pylatexenc.latex2text import LatexNodes2Text
from src.logger import log


def apply_cleaning(pdf_path: str, markdown_content: str) -> list:
    """
    Applies full cleaning pipeline: hierarchy rebalancing, HTML table conversion, and LaTeX cleaning.
    """
    # Step 1: Rebalance headers (expects list, so split)
    lines = markdown_content.split('\n')
    new_lines = rebalance_headers(pdf_path, lines)
    if not isinstance(new_lines, list):
        log("rebalance_headers returned non-list. Using original.", level="warning")
        new_lines = lines
    
    # Step 2: Convert HTML tables to Markdown tables
    # Join lines into a single string for easier regex processing
    markdown_content = '\n'.join(new_lines)
    
    # Replace HTML table blocks with Markdown tables
    def replace_html_table(match):
        html_table = match.group(0)
        try:
            return markdown_table_from_html(html_table)
        except Exception as e:
            log(f"HTML table conversion failed: {e}", level="warning")
            return html_table
    
    markdown_content = re.sub(
        r'<table.*?</table>', 
        replace_html_table, 
        markdown_content, 
        flags=re.DOTALL | re.IGNORECASE
    )
    
    # Step 3: Clean LaTeX expressions
    def replace_latex(match):
        latex_str = match.group(1)
        try:
            return latex_to_clean_text(f"${latex_str}$")
        except Exception as e:
            log(f"LaTeX cleaning failed for '{latex_str}': {e}", level="warning")
            return match.group(0)
    
    markdown_content = re.sub(r'\$(.*?)\$', replace_latex, markdown_content)
    
    return markdown_content




## HIERARCHY REBALANCING UTILITIES
def rebalance_headers(pdf_path, md):
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




### HTML TABLE TO MARKDOWN UTILITIES
def _parse_html_table(table):
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


def _table_to_markdown(table_matrix):
    md = []
    headers = table_matrix[0]
    md.append("| " + " | ".join(headers) + " |")
    md.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in table_matrix[1:]:
        md.append("| " + " | ".join(row) + " |")

    return "\n".join(md)


def markdown_table_from_html(html_table: str) -> str:
    soup = BeautifulSoup(html_table, "html.parser")
    table = soup.find("table")
    parsed = _parse_html_table(table)
    markdown = _table_to_markdown(parsed)
    
    return markdown


## LATEX RELATED UTILITIES
def latex_to_clean_text(latex):
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