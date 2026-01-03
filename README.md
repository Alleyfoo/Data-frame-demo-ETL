# Provider Data Ingestor

A robust, schema-validated pipeline for standardizing Excel/CSV data from multiple providers.

## 📂 Project Structure

- **`main.py`**: The entry point. Runs the GUI or the Batch Processor.
- **`config.yaml`**: Defines synonyms for auto-mapping columns.
- **`src/app.py`**: The UI for creating mapping templates.
- **`src/pipeline.py`**: The logic that cleans, unpivots, and validates data.
- **`src/schema.py`**: Defines the "Contract" (Data Types) required for output.
- **`data/`**:
    - `input/`: Drop raw files here.
    - `output/`: Clean, standardized files appear here.
    - `schemas/`: Place canonical/target output examples or schema files here to keep `output/` tidy.
    - `archive/`: Successfully processed source files are moved here.
    - `quarantine/`: Failed files are moved here with an error log.

## 🚀 Installation

1. Install Python 3.10+
2. Install dependencies:
   ```bash
   pip install pandas openpyxl pyyaml pandera sqlalchemy psycopg2-binary
   ```

## SQL Connection Examples

- Postgres: driver `postgresql+psycopg2`, host `your-host`, port `5432`, database `your_db`, user `your_user`. Leave password empty and set env var `<NAME>_PASSWORD` to avoid storing secrets.
- SQL Server: driver `mssql+pyodbc`, host `your-host`, port `1433`, database `your_db`, user `your_user`. Install Microsoft ODBC Driver 18 and `pip install sqlalchemy pyodbc`. Password can also come from `<NAME>_PASSWORD`.
- Quick SQL test without a DB: install `duckdb` and run ad-hoc SQL over files, e.g.:
  ```bash
  pip install duckdb
  python - <<'PY'
  import duckdb
  df = duckdb.query("SELECT * FROM read_excel('data/input/sample.xlsx') LIMIT 5").df()
  print(df.head())
  PY
  ```

## Streamlit UI

- Run: `streamlit run app.py`
- Pages: Dashboard, Upload, Mapping, Query Builder, Diagnostics, Template Library, Combine & Export.
-
- See `QUICKSTART.md` for a guided first run.

## Query Builder

The Query Builder page provides a visual SQL-like builder using a Source Canvas,
operator palette, and a generated SQL preview. See `GUIDE_QUERY_BUILDER.md` for
usage details.

## Template Library

The Template Library page lists `.df-template.json` files, shows the JSON
contents, and supports batch processing over an input directory. Use it to
duplicate, inspect, and run templates without writing code.

## YouTube API Data Flow

Use the YouTube Data API to pull channel uploads or playlist items directly into
the pipeline:

1. Create an API key in the Google Cloud console and set it locally:  
   `setx YOUTUBE_API_KEY "<your-key>"` (Windows) or `export YOUTUBE_API_KEY=...`.
2. Fetch uploads into the standard output folder (xlsx by default):  
   `python main.py youtube --channel-id <CHANNEL_ID> --max-results 50`
3. To target a playlist instead of a channel:  
   `python main.py youtube --playlist-id <PLAYLIST_ID> --output data/output/youtube_videos.parquet --output-fmt parquet`
4. Combine multiple sources and add engagement metrics:  
   `python main.py youtube --playlist-id PL123 --playlist-id PL456 --channel-id UC789 --max-results 50`
   - Outputs include `engagement_rate` and `engagement_rate_pct` plus a `source`
     column noting which playlist/channel each video came from.

Outputs include video metadata (title, duration, publish date) and engagement
stats (views, likes, comments), making it easy to experiment with the ETL
pipeline against API-driven data.
