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
