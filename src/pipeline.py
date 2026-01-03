"""Pipeline implementation for provider data ingestion.

This module handles the heavy lifting:
1. Reading the file (Ingest)
2. Renaming columns (Normalize)
3. Unpivoting/Melting (Transform)
4. checking data quality (Validate)
5. Saving the result (Load)
"""

from __future__ import annotations

import logging
import shutil
import traceback
from pathlib import Path
from typing import Optional

import pandas as pd
import pandera as pa

from .api.v1 import engine
from .connectors import read_sql_with_template
from .templates import Template, read_excel_with_template


def ingest(source: Path, template: Template) -> pd.DataFrame:
    """Read a CSV/Excel source using the provided template settings."""
    if template.source_type == "sql":
        return read_sql_with_template(template)
    return read_excel_with_template(source, template)


def normalize(df: pd.DataFrame, template: Template) -> pd.DataFrame:
    """Backward-compatible wrapper for engine.normalize."""
    return engine.normalize(df, template)


def transform(df: pd.DataFrame, template: Template) -> tuple[pd.DataFrame, dict]:
    """Backward-compatible wrapper for engine.transform."""
    return engine.transform(df, template)


def warn_on_schema_diff(
    df: pd.DataFrame, template: Template, source: Path | None = None
) -> tuple[list[str], list[str]]:
    """Log missing/extra columns relative to template expectations and return them."""
    ctx = source.name if source is not None else "current file"
    tpl_name = template.provider_name or template.source_file or ""
    context_label = f"{ctx}::{tpl_name}" if tpl_name else ctx
    return engine.warn_on_schema_diff(df, template, context_label=context_label)


def validate_data(
    df: pd.DataFrame, template: Template, validation_level: str = "coerce"
) -> pd.DataFrame:
    """Backward-compatible wrapper for engine.validate."""
    return engine.validate(df, template, validation_level=validation_level)


def save_quarantine(
    df: pd.DataFrame,
    source: Path,
    quarantine_dir: Path,
    error_msg: str,
    validation_report: str | None = None,
) -> None:
    """Save failed data and a log file to the quarantine folder."""
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    dest_file = quarantine_dir / source.name
    try:
        shutil.copy2(source, dest_file)
    except Exception:
        pass

    log_path = quarantine_dir / f"{source.name}.error.log"
    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"Validation Failed for {source.name}\n")
        f.write("-" * 50 + "\n")
        f.write(error_msg)
        if validation_report:
            f.write("\n\n")
            f.write(validation_report)


def _build_validation_report(
    source: Path,
    raw_rows: int,
    raw_cols: int,
    clean_df: pd.DataFrame,
    metrics: dict,
    missing: list[str],
    extra: list[str],
    validation_level: str,
    template: Template,
) -> str:
    lines: list[str] = []
    lines.append(f"Source: {source.name}")
    lines.append(f"Validation level: {validation_level.upper()}")
    lines.append(f"Rows before/after: {raw_rows} -> {len(clean_df)}")
    lines.append(f"Columns before/after: {raw_cols} -> {len(clean_df.columns)}")
    if template.unpivot:
        before = metrics.get("unpivot_before", (raw_rows, raw_cols))
        after = metrics.get("unpivot_after", clean_df.shape)
        lines.append(f"Unpivot shape: rows {before[0]}->{after[0]}, cols {before[1]}->{after[1]}")
    if metrics.get("dedupe_dropped"):
        lines.append(f"Dedupe dropped rows: {metrics['dedupe_dropped']}")
    lines.append(f"Date parse failures: {metrics.get('date_parse_failures', 0)}")
    lines.append(f"Numeric parse failures: {metrics.get('numeric_parse_failures', 0)}")
    if missing:
        lines.append(f"Missing vs template: {', '.join(missing)}")
    if extra:
        lines.append(f"Extra vs template: {', '.join(extra)}")
    if template.required_fields:
        lines.append(f"Required fields: {', '.join(template.required_fields)}")
    return "\n".join(lines)


def run_pipeline(
    file_path: Path,
    template: Template,
    output_path: Path,
    quarantine_dir: Optional[Path] = None,
    fail_on_missing: bool = False,
    fail_on_extra: bool = False,
    validation_level: str = "coerce",
) -> bool:
    """
    Orchestrate the ETL process for a single file.
    Returns True if successful, False if quarantined.
    """
    try:
        logging.info(f"Pipeline started for {file_path.name}")

        raw_df = ingest(file_path, template) if template.source_type != "sql" else ingest(Path(""), template)
        raw_rows, raw_cols = raw_df.shape

        norm_df = normalize(raw_df, template)

        clean_df, metrics = transform(norm_df, template)

        missing, extra = warn_on_schema_diff(clean_df, template, source=file_path if file_path else None)
        if (fail_on_missing and missing) or (fail_on_extra and extra):
            logging.error(
                "Schema drift enforced failure: missing=%s extra=%s", ",".join(missing), ",".join(extra)
            )
            if quarantine_dir:
                report = _build_validation_report(
                    file_path, raw_rows, raw_cols, clean_df, metrics, missing, extra, validation_level, template
                )
                save_quarantine(clean_df, file_path, quarantine_dir, f"Missing: {missing} | Extra: {extra}", report)
            return False

        try:
            valid_df = validate_data(clean_df, template, validation_level=validation_level)
        except pa.errors.SchemaErrors as err:
            logging.error(f"Schema Validation Failed: {err}")
            if quarantine_dir:
                report = _build_validation_report(
                    file_path, raw_rows, raw_cols, clean_df, metrics, missing, extra, validation_level, template
                )
                save_quarantine(clean_df, file_path, quarantine_dir, str(err.failure_cases), report)
            return False

        output_path.parent.mkdir(parents=True, exist_ok=True)
        excel_path = output_path.with_suffix(".xlsx")
        valid_df.to_excel(excel_path, index=False)

        report = _build_validation_report(
            file_path, raw_rows, raw_cols, valid_df, metrics, missing, extra, validation_level, template
        )
        report_path = excel_path.with_suffix(excel_path.suffix + ".validation.txt")
        report_path.write_text(report, encoding="utf-8")

        logging.info(f"Pipeline finished. Saved to {excel_path}")
        return True

    except Exception as e:
        logging.error(f"Critical Pipeline Error: {e}")
        traceback.print_exc()
        if quarantine_dir:
            save_quarantine(pd.DataFrame(), file_path, quarantine_dir, traceback.format_exc())
        return False
