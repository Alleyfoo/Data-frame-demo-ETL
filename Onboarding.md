Usage
1) Onboarding a new file (GUI mode)
- Run `python main.py`.
- Select the raw Excel/CSV file.
- Use the sheet list to pick one or many sheets; toggle "Combine selected sheets" to stack them (a `source_sheet` column is added automatically).
- The info bar shows approximate rows/columns; hit "Reload Preview" after changing header/skip rows, or "Reset View" to start fresh.
- Click "Auto-Suggest" to guess column mappings.
- If the data is wide (months as columns), check "Unpivot".
- (Optional) Set "Group by" to aggregate by one or more canonical fields (comma-separated), e.g., `order_id` to roll products into the same order or `order_id, article_sku` to keep per-product lines.
- (Optional) Cleanup options: trim text, drop empty rows, drop sparse columns (set a non-null ratio), and dedupe on keys.
- (Optional) Cleanup: strip thousands separators in text columns to help numeric parsing.
- (Optional) Connections: use the Connection Manager to store connection info (in-memory) if you plan to pull from SQL/azure later; local file import remains available.
- SQL (preview): add a SQL connection, provide a table or query, click “Use Connection” to preview and map. Requires SQLAlchemy + driver installed; connections saved to `connections.yaml`.
- Credentials tip: leave password blank and set an environment variable `<NAME>_PASSWORD` (uppercased connection name) to avoid storing secrets on disk.
- SQL Server tip: driver example `mssql+pyodbc`; install Microsoft ODBC Driver 18 plus `pip install sqlalchemy pyodbc`.
- Examples:
  - Postgres: driver `postgresql+psycopg2`, host `your-host`, port `5432`, database `your_db`, user `your_user`.
  - SQL Server: driver `mssql+pyodbc`, host `your-host`, port `1433`, database `your_db`, user `your_user`.
- Click "Save Template". A df-template file appears next to the data file.

2) Processing data (batch mode)
- Run `python main.py --batch --target-dir "data/input"` (schedule it if needed).
- Success: cleaned data goes to output/, source goes to archive/.
- Failure: source goes to quarantine/ with a .log file explaining the error.

Configuration
- Base synonyms live in `src/config.yaml`.
- New header names you map are auto-added to `src/config.user.yaml` when you save a template. Keep `src/config.yaml` for shared defaults; clear `src/config.user.yaml` to reset learned hints.
- To add new target columns (e.g., Cost Center): update `src/schema.py` and add synonyms for it in `src/config.yaml`.

Consuming the data
- Run `python combine-reports.py` to stack everything from data/output once templates are in place.
