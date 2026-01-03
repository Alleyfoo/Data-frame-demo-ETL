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

from .schema import OutputSchema
from .templates import Template, read_excel_with_template
from .connectors import read_sql_with_template


def _coerce_field_types(df: pd.DataFrame, type_map: dict[str, str]) -> tuple[pd.DataFrame, list[dict]]:
    """Attempt to coerce columns to declared types; return failures for reporting."""
    failures: list[dict] = []
    for col, spec in type_map.items():
        if col not in df.columns:
            continue
        target = str(spec).lower()
        series = df[col]
        try:
            if target in {"date", "datetime"}:
                non_null = series.notna().sum()
                converted = pd.to_datetime(series, errors="coerce")
                failed = max(non_null - converted.notna().sum(), 0)
                df[col] = converted
                if failed:
                    failures.append({"column": col, "failure": f"{failed} datetime parse failures"})
            elif target in {"int", "integer"}:
                non_null = series.notna().sum()
                converted = pd.to_numeric(series, errors="coerce").astype("Int64")
                failed = max(non_null - converted.notna().sum(), 0)
                df[col] = converted
                if failed:
                    failures.append({"column": col, "failure": f"{failed} integer parse failures"})
            elif target in {"float", "number", "numeric"}:
                non_null = series.notna().sum()
                converted = pd.to_numeric(series, errors="coerce")
                failed = max(non_null - converted.notna().sum(), 0)
                df[col] = converted
                if failed:
                    failures.append({"column": col, "failure": f"{failed} numeric parse failures"})
            elif target in {"str", "string", "text"}:
                df[col] = series.astype(str)
        except Exception:
            failures.append({"column": col, "failure": f"coercion to {target} failed"})
    return df, failures


def ingest(source: Path, template: Template) -> pd.DataFrame:
    """Read a CSV/Excel source using the provided template settings."""
    if template.source_type == "sql":
        return read_sql_with_template(template)
    # This helper from templates.py handles header normalization and skiprows
    return read_excel_with_template(source, template)


def normalize(df: pd.DataFrame, template: Template) -> pd.DataFrame:
    """Rename columns to canonical names defined in the template."""
    # Note: read_excel_with_template already handles mapping,
    # but we double check here if specific post-processing is needed.
    return df


def transform(df: pd.DataFrame, template: Template) -> tuple[pd.DataFrame, dict]:
    """Apply structural transformations (unpivot) and clean types, returning metrics."""

    metrics: dict = {
        "unpivot_before": df.shape,
        "unpivot_after": df.shape,
        "dedupe_dropped": 0,
        "date_parse_failures": 0,
        "numeric_parse_failures": 0,
    }

    # 1. UNPIVOT (MELT) LOGIC
    if template.unpivot:
        # If unpivoting, the columns we mapped in the UI are the "Identifier Variables"
        # (e.g., SKU, Provider Name). Everything else is data (Months).

        # We need to find which columns in the current DF correspond to the IDs
        # Since the DF headers are already renamed to the Targets, we use the Targets as IDs.
        id_vars = list(template.column_mappings.values())

        # Ensure these columns actually exist in the DF
        available_ids = [c for c in id_vars if c in df.columns]

        if not available_ids:
            logging.warning("Unpivot requested but no identifier columns found.")
        else:
            before_rows, before_cols = df.shape
            df = df.melt(
                id_vars=available_ids,
                var_name=template.var_name,  # e.g., "report_date"
                value_name=template.value_name,  # e.g., "sales_amount"
            )
            metrics["unpivot_before"] = (before_rows, before_cols)
            metrics["unpivot_after"] = df.shape

    # 2. ADD METADATA
    if template.provider_name:
        df["provider_id"] = template.provider_name
    else:
        # Fallback to filename if not set
        df["provider_id"] = template.source_file

    # 3. TYPE COERCION & CLEANING
    if template.drop_empty_rows:
        df = df.dropna(how="all")

    # Drop sparse columns if threshold provided (fraction of non-nulls required)
    if template.drop_null_columns_threshold is not None:
        frac = template.drop_null_columns_threshold
        keep_cols: list[str] = []
        for col in df.columns:
            if df[col].size == 0:
                continue
            if df[col].notna().mean() >= frac:
                keep_cols.append(col)
        df = df[keep_cols] if keep_cols else df

    # Clean up text fields
    if template.trim_strings:
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.strip()

    if template.strip_thousands:
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype(str).str.replace(r"[,\s]", "", regex=True)

    # Convert Date
    if "report_date" in df.columns:
        non_null = df["report_date"].notna().sum()
        converted = pd.to_datetime(df["report_date"], errors="coerce")
        metrics["date_parse_failures"] = max(non_null - converted.notna().sum(), 0)
        df["report_date"] = converted
        # Optional: Drop rows where date parsing failed completely
        df = df.dropna(subset=["report_date"])

    # Convert Numeric
    if "sales_amount" in df.columns:
        non_null = df["sales_amount"].notna().sum()
        converted_num = pd.to_numeric(df["sales_amount"], errors="coerce")
        metrics["numeric_parse_failures"] = max(non_null - converted_num.notna().sum(), 0)
        df["sales_amount"] = converted_num.fillna(
            0.0
        )

    # Optional aggregation by a canonical field (e.g., product name)
    if template.combine_on:
        keys = [k for k in template.combine_on if k in df.columns]
        if not keys:
            logging.warning("combine_on keys not found in columns; skipping aggregation.")
        else:
            group_cols: list[str] = list(keys)
            if template.unpivot and template.var_name in df.columns:
                group_cols.append(template.var_name)
            if "provider_id" in df.columns and "provider_id" not in group_cols:
                group_cols.append("provider_id")

            numeric_cols = [
                col
                for col in df.columns
                if col not in group_cols and pd.api.types.is_numeric_dtype(df[col])
            ]
            if numeric_cols:
                df = (
                    df.groupby(group_cols, as_index=False)[numeric_cols]
                    .sum(min_count=1)
                    .copy()
                )
            else:
                logging.warning(
                    "combine_on=%s requested but no numeric columns to aggregate.",
                    ",".join(keys),
                )

    # Deduplication
    if template.dedupe_on:
        keys = [k for k in template.dedupe_on if k in df.columns]
        if keys:
            before = len(df)
            df = df.drop_duplicates(subset=keys, keep="first")
            metrics["dedupe_dropped"] = before - len(df)
        else:
            logging.warning("dedupe_on keys not found in columns; skipping dedupe.")

    return df, metrics


def _expected_headers(template: Template) -> set[str]:
    """Best-effort expected headers based on template mappings/headers."""
    if template.headers:
        return {h.alias or h.name for h in template.headers}
    if template.column_mappings:
        return set(template.column_mappings.values())
    if template.columns:
        return set(template.columns)
    return set()


def warn_on_schema_diff(
    df: pd.DataFrame, template: Template, source: Path | None = None
) -> tuple[list[str], list[str]]:
    """Log missing/extra columns relative to template expectations and return them."""
    expected = _expected_headers(template)
    if not expected:
        return [], []
    cols = set(df.columns)
    missing = sorted(expected - cols)
    extra = sorted(cols - expected)
    ctx = source.name if source is not None else "current file"
    tpl_name = template.provider_name or template.source_file or ""
    if missing:
        context_label = f"{ctx}::{tpl_name}" if tpl_name else ctx
        logging.warning("[%s] Missing columns vs template: %s", context_label, ", ".join(missing))
    if extra:
        context_label = f"{ctx}::{tpl_name}" if tpl_name else ctx
        logging.warning("[%s] Extra columns vs template: %s", context_label, ", ".join(extra))
    return missing, extra


def validate_data(
    df: pd.DataFrame, template: Template, validation_level: str = "coerce"
) -> pd.DataFrame:
    """Validate data against the schema contract."""
    level = (validation_level or "coerce").lower()
    if level == "off":
        return df

    if level == "contract":
        missing_required = [f for f in template.required_fields if f not in df.columns]
        if missing_required:
            raise pa.errors.SchemaErrors(
                schema=OutputSchema,
                data=df,
                failure_cases=pd.DataFrame(
                    {"column": missing_required, "failure": "missing required column"}
                ),
            )
        if template.field_types:
            df, failures = _coerce_field_types(df, template.field_types)
            if failures:
                raise pa.errors.SchemaErrors(
                    schema=OutputSchema,
                    data=df,
                    failure_cases=pd.DataFrame(failures),
                )

    # This raises pa.errors.SchemaErrors if data is bad
    return OutputSchema.validate(df, lazy=True)


def save_quarantine(
    df: pd.DataFrame,
    source: Path,
    quarantine_dir: Path,
    error_msg: str,
    validation_report: str | None = None,
) -> None:
    """Save failed data and a log file to the quarantine folder."""
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    # 1. Move the original source file
    dest_file = quarantine_dir / source.name
    try:
        shutil.copy2(source, dest_file)
    except Exception:
        pass  # File might already be there

    # 2. Save the error log
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

        # 1. Ingest
        raw_df = ingest(file_path, template) if template.source_type != "sql" else ingest(Path(""), template)
        raw_rows, raw_cols = raw_df.shape

        # 2. Normalize (Renaming handled in ingest via template, this is a placeholder)
        norm_df = normalize(raw_df, template)

        # 3. Transform
        clean_df, metrics = transform(norm_df, template)

        # 3b. Warn if schema drift vs template
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

        # 4. Validate
        try:
            valid_df = validate_data(clean_df, template, validation_level=validation_level)
        except pa.errors.SchemaErrors as err:
            logging.error(f"Schema Validation Failed: {err}")
            if quarantine_dir:
                report = _build_validation_report(
                    file_path, raw_rows, raw_cols, clean_df, metrics, missing, extra, validation_level, template
                )
                save_quarantine(
                    clean_df, file_path, quarantine_dir, str(err.failure_cases), report
                )
            return False

        # 5. Load
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
            save_quarantine(
                pd.DataFrame(), file_path, quarantine_dir, traceback.format_exc()
            )
        return False
