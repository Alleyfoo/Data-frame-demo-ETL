# Quick Start Guide (Data Frame Tool)

## 1) Install
```bash
python -m venv .venv
.venv\Scripts\activate  # or source .venv/bin/activate
pip install -r requirements.txt
```

## 2) Run the Streamlit demo
```bash
streamlit run app.py
```
Use the pages in `webapp/pages` to explore uploads, mappings, query builder, and diagnostics.

## 3) CLI basics (canonical form)
- Batch process files: `python -m src.cli run --target-dir data/input --output-fmt xlsx`
- Combine cleaned outputs: `python -m src.cli combine --input-dir data/output --pattern "*.xlsx" --output Master_Sales_Report.xlsx`
- YouTube ETL:
  ```bash
  setx YOUTUBE_API_KEY "<your-key>"  # or export YOUTUBE_API_KEY=...
  python -m src.cli youtube --playlist-id <PLAYLIST_ID> --max-results 50 --summary-output data/output/youtube_summary.xlsx
  ```

## 4) Troubleshooting
- Failed batch files land in `data/quarantine` with an error log.
- Missing columns or schema drift are logged during processing.
- For YouTube, ensure the API key is set or pass `--api-key` explicitly.
