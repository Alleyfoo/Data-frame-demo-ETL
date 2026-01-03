# Quick Start Guide

## 1. I have a messy Excel file.

- Go to Import Data (Tkinter GUI) or Upload (Streamlit).
- Select your file.
- Look at the Data Preview pane.
- If the data starts on the wrong row, adjust the Header Row.
- Click Auto-Suggest to map columns to standard names.

## 2. I have multiple files to combine.

- Run the Batch Processor:
  - `python -m src.cli run --target-dir "data/input"`
- Cleaned files appear in `data/output`.
- Then run the Combine Reports tool to merge them into a master report.

## 3. I want to analyze my data.

- Go to Query Builder (Streamlit).
- Select a cleaned file from `data/output` or load a source directly.
- Use the Source Canvas and Operator Palette to build a filter query (e.g., `sales_amount > 1000`).

## 4. Something went wrong.

- Check the `data/quarantine` folder for failed files.
- Open the `.error.log` file to see why validation failed.
