# Sample Excel files

This directory uses generated Excel workbooks to highlight common layouts the data-frame tool should handle. Generated files are ignored in version control; run the helper script to create them locally.

## Files
- `multi_sheet_jan.xlsx`: Two-sheet workbook with `Orders` (order id, region, product, quantity, unit price) and `Adjustments` (order-level adjustments). Useful for verifying sheet selection and multi-sheet ingestion.
- `consistent_schema_feb.xlsx`: Single-sheet February workbook that mirrors the `Orders` schema from January so you can test combining files with matching headers.
- `offset_header.xlsx`: `Departments` sheet with two descriptive banner rows above the header; table headers begin on row 4. Use this to confirm the tool can skip context rows before reading column names.
- `merged_header.xlsx`: A sheet with merged year header over split month row (e.g., `2020` merged over Jan/Feb/Mar). Tests merged-cell header expansion.
- `split_year_month.xlsx`: Year in one row and localized month names below (Finnish months) to test year+month combining and localization.

## Regenerating samples
1. Ensure `pandas` with the `openpyxl` engine is available in your environment.
2. From the repository root, run `python samples/generate_samples.py`.
3. Five `.xlsx` files will be written into `samples/` (they remain locally and are ignored by git).

Quick header QA (prints guessed header rows/columns):

```bash
python samples/qa_headers.py
python samples/test_harness.py
```

## How to use
1. Launch the data-frame tool and use the file picker to select a generated workbook from this folder.
2. For the January/February files, choose the `Orders` sheet to load rows with matching headers across both workbooks.
3. For the offset-header sample, point the tool at the `Departments` sheet and configure it to ignore the first three rows so the header row is detected correctly.
4. Inspect the parsed output to ensure columns and row counts match expectations for each scenario.

## Ignore strategy
Generated artifacts from the CLI (templates and cleaned/combined CSVs) and ad-hoc sample exports are not checked in. The root `.gitignore` omits `samples/*.xls*`, `samples/*.csv`, `*.template.json`, `*_template.*`, `*_cleaned.csv`, `combined.csv`, and even an optional `/outputs/` export directory so only source scripts and documentation stay tracked.
