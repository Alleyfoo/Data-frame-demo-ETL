# Data Frame Tool (YouTube ETL Demo)

Lightweight ETL demo for pulling YouTube metadata, inspecting it in Streamlit, and exporting cleaned outputs with summaries.

## Install

```bash
python -m venv .venv
.venv\Scripts\activate  # or source .venv/bin/activate
pip install -r requirements.txt
```

## Run

- Streamlit UI (canonical UI): `streamlit run app.py`
- CLI (canonical): `python -m src.cli <command> [...]`
  - Batch files: `python -m src.cli run --target-dir data/input`
  - Combine cleaned outputs: `python -m src.cli combine --input-dir data/output --pattern "*.xlsx" --output Master_Sales_Report.xlsx`
  - YouTube ETL: see below

`main.py` remains as a thin shim to `src.cli` for compatibility; prefer the `python -m src.cli` form above.

## YouTube API demo (happy path)

Use the YouTube Data API to pull channel uploads or playlist items directly into the pipeline.

1) Set an API key: `setx YOUTUBE_API_KEY "<your-key>"` (Windows) or `export YOUTUBE_API_KEY=...`.

2) Fetch one or more sources, add engagement metrics, and emit a summary workbook:
```bash
python -m src.cli youtube ^
  --playlist-id PL123 ^
  --playlist-id PL456 ^
  --channel-id UC789 ^
  --max-results 50 ^
  --output data/output/youtube_detail.xlsx ^
  --summary-output data/output/youtube_summary.xlsx ^
  --top-n 15
```
Outputs:
- Detail file (`--output`): includes `engagement_rate`, `engagement_rate_pct`, and `source`.
- Summary workbook (`--summary-output`): sheets `top_videos`, `per_channel`, `per_year`, plus the full detail sheet.

Sample demo data is already checked in under `data/output/` for offline walkthroughs.

## Project structure (trimmed for the demo)

- `app.py` — Streamlit launcher (uses `webapp/pages`).
- `src/cli.py` — canonical CLI (`run`, `combine`, `youtube`).
- `src/youtube.py` — YouTube API client + summaries.
- `data/output/` — demo outputs (detail + summary).
- `webapp/` — Streamlit pages.
- `legacy/` (optional) — place old Tkinter assets here if needed; GUI command removed from CLI.

## Notes

- Dependencies are consolidated in `requirements.txt` (includes Streamlit and dev tools like pytest/ruff).
- Combine reports via the CLI `combine` subcommand (the old `combine-reports.py` script was removed).
- Tkinter GUI is no longer exposed via CLI; Streamlit is the primary UI.
- `Makefile` targets: `lint`, `test`, `run-ui`, `demo-youtube`.
