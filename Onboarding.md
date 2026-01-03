# Onboarding (Data Frame Tool)

## What this demo does
- Streamlit UI for exploring cleaned data and template flows.
- CLI for batch processing and YouTube ingestion.
- Pre-baked YouTube outputs in `data/output/` to demo without live calls.

## Run paths
- UI: `streamlit run app.py` (uses `webapp/pages`).
- CLI (canonical): `python -m src.cli <command>`
  - `run` — batch process files in `data/input`.
  - `combine` — merge cleaned outputs.
  - `youtube` — fetch playlists/channels, add engagement metrics, and write detail + summary outputs.

## YouTube quick demo
```bash
setx YOUTUBE_API_KEY "<your-key>"  # or export YOUTUBE_API_KEY=...
python -m src.cli youtube --playlist-id <PLAYLIST_ID> --max-results 50 --summary-output data/output/youtube_summary.xlsx
```
Outputs land in `data/output/` (detail + summary workbook).

## Legacy notes
- Tkinter GUI command is removed from the CLI; Streamlit is the primary UI.
- The old `combine-reports.py` script was removed—use `python -m src.cli combine`.

## Old → New CLI mapping
- `python main.py --batch ...` → `python -m src.cli run ...`
- `python combine-reports.py` → `python -m src.cli combine ...`
- `python main.py gui` → not exposed (Tkinter UI is legacy-only)
