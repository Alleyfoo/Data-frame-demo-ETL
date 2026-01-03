"""Headless engine for template-driven transformations."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import pandera as pa

from .endpoints import ProcessResult, TransformRequest, ValidationConfig, ValidationResponse
from ...combine_runner import run_combine as _run_combine
from ...connectors import read_sql_with_template
from ...schema import OutputSchema
from ...templates import Template, read_excel_with_template


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


def validate(df: pd.DataFrame, template: Template, validation_level: str = "coerce") -> pd.DataFrame:
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

    return OutputSchema.validate(df, lazy=True)


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
    df: pd.DataFrame, template: Template, context_label: str | None = None
) -> tuple[list[str], list[str]]:
    """Log missing/extra columns relative to template expectations and return them."""
    expected = _expected_headers(template)
    if not expected:
        return [], []
    cols = set(df.columns)
    missing = sorted(expected - cols)
    extra = sorted(cols - expected)
    if missing:
        prefix = f"[{context_label}] " if context_label else ""
        logging.warning("%sMissing columns vs template: %s", prefix, ", ".join(missing))
    if extra:
        prefix = f"[{context_label}] " if context_label else ""
        logging.warning("%sExtra columns vs template: %s", prefix, ", ".join(extra))
    return missing, extra


class DataEngine:
    """Headless engine for running ETL without UI dependencies."""

    def read_source(self, source_path: Path, template: Template) -> pd.DataFrame:
        """Read from an input source path using a template."""
        if template.source_type == "sql":
            return read_sql_with_template(template)
        return read_excel_with_template(source_path, template)

    def ingest(self, df: pd.DataFrame, template: Template) -> pd.DataFrame:
        """Validate incoming DataFrame shape for the engine."""
        if not isinstance(df, pd.DataFrame):
            raise ValueError("Engine ingest expects a pandas DataFrame.")
        if not isinstance(template, Template):
            raise ValueError("Engine ingest expects a Template.")
        return df.copy()

    def normalize_data(self, df: pd.DataFrame, template: Template) -> pd.DataFrame:
        """Rename columns to canonical names defined in the template."""
        return df

    def transform_data(self, df: pd.DataFrame, template: Template) -> tuple[pd.DataFrame, dict]:
        """Apply structural transformations (unpivot) and clean types, returning metrics."""
        metrics: dict = {
            "unpivot_before": df.shape,
            "unpivot_after": df.shape,
            "dedupe_dropped": 0,
            "date_parse_failures": 0,
            "numeric_parse_failures": 0,
        }

        if template.unpivot:
            id_vars = list(template.column_mappings.values())
            available_ids = [c for c in id_vars if c in df.columns]

            if not available_ids:
                logging.warning("Unpivot requested but no identifier columns found.")
            else:
                before_rows, before_cols = df.shape
                df = df.melt(
                    id_vars=available_ids,
                    var_name=template.var_name,
                    value_name=template.value_name,
                )
                metrics["unpivot_before"] = (before_rows, before_cols)
                metrics["unpivot_after"] = df.shape

        if template.provider_name:
            df["provider_id"] = template.provider_name
        else:
            df["provider_id"] = template.source_file

        if template.drop_empty_rows:
            df = df.dropna(how="all")

        if template.drop_null_columns_threshold is not None:
            frac = template.drop_null_columns_threshold
            keep_cols: list[str] = []
            for col in df.columns:
                if df[col].size == 0:
                    continue
                if df[col].notna().mean() >= frac:
                    keep_cols.append(col)
            df = df[keep_cols] if keep_cols else df

        if template.trim_strings:
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.strip()

        if template.strip_thousands:
            for col in df.select_dtypes(include=["object"]).columns:
                df[col] = df[col].astype(str).str.replace(r"[,\s]", "", regex=True)

        if "report_date" in df.columns:
            non_null = df["report_date"].notna().sum()
            converted = pd.to_datetime(df["report_date"], errors="coerce")
            metrics["date_parse_failures"] = max(non_null - converted.notna().sum(), 0)
            df["report_date"] = converted
            df = df.dropna(subset=["report_date"])

        if "sales_amount" in df.columns:
            non_null = df["sales_amount"].notna().sum()
            converted_num = pd.to_numeric(df["sales_amount"], errors="coerce")
            metrics["numeric_parse_failures"] = max(non_null - converted_num.notna().sum(), 0)
            df["sales_amount"] = converted_num.fillna(0.0)

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
                    df = df.groupby(group_cols, as_index=False)[numeric_cols].sum(min_count=1).copy()
                else:
                    logging.warning(
                        "combine_on=%s requested but no numeric columns to aggregate.",
                        ",".join(keys),
                    )

        if template.dedupe_on:
            keys = [k for k in template.dedupe_on if k in df.columns]
            if keys:
                before = len(df)
                df = df.drop_duplicates(subset=keys, keep="first")
                metrics["dedupe_dropped"] = before - len(df)
            else:
                logging.warning("dedupe_on keys not found in columns; skipping dedupe.")

        return df, metrics

    def validate_data(
        self, df: pd.DataFrame, template: Template, config: ValidationConfig
    ) -> ValidationResponse:
        """Validate data against the schema contract."""
        try:
            validate(df, template, validation_level=config.level)
            return ValidationResponse(is_valid=True, errors=[], row_count=len(df))
        except pa.errors.SchemaErrors as exc:
            errors = exc.failure_cases.to_dict(orient="records") if exc.failure_cases is not None else []
            return ValidationResponse(is_valid=False, errors=errors, row_count=len(df))
        except Exception as exc:
            return ValidationResponse(
                is_valid=False, errors=[{"failure": str(exc)}], row_count=len(df)
            )

    def run_full_process(
        self,
        source_path: Path,
        template: Template,
        output_path: Path,
        validation_level: str = "coerce",
    ) -> tuple[ProcessResult, pd.DataFrame | None]:
        """
        Orchestrates the full ETL pipeline: Ingest -> Normalize -> Transform -> Validate.
        Note: This does NOT handle file movement (Archive/Quarantine).
        """
        try:
            raw_df = self.read_source(source_path, template)
            norm_df = self.normalize_data(raw_df, template)
            clean_df, metrics = self.transform_data(norm_df, template)
            config = ValidationConfig(level=validation_level)
            validation = self.validate_data(clean_df, template, config)

            if not validation.is_valid:
                return ProcessResult(
                    success=False,
                    message="Validation failed.",
                    row_count=validation.row_count,
                    metrics=metrics,
                ), clean_df

            return ProcessResult(
                success=True,
                message="Processing successful.",
                output_path=str(output_path),
                row_count=len(clean_df),
                metrics=metrics,
            ), clean_df

        except Exception as exc:
            logging.exception("Pipeline processing failed")
            return ProcessResult(
                success=False,
                message=str(exc),
                row_count=0,
                metrics={},
            ), None

    def run_combine(
        self,
        input_dir: Path,
        pattern: str = "*.xlsx",
        mode: str = "concat",
        keys: list[str] | None = None,
        how: str = "inner",
        strict_schema: bool = False,
    ) -> pd.DataFrame:
        """Combine multiple outputs using existing combine runner logic."""
        return _run_combine(
            input_dir=input_dir,
            pattern=pattern,
            mode=mode,
            keys=keys or [],
            how=how,
            strict_schema=strict_schema,
        )


_ENGINE = DataEngine()


def ingest(df: pd.DataFrame, template: Template) -> pd.DataFrame:
    """Compatibility wrapper for engine ingestion."""
    return _ENGINE.ingest(df, template)


def normalize(df: pd.DataFrame, template: Template) -> pd.DataFrame:
    """Compatibility wrapper for engine normalization."""
    return _ENGINE.normalize_data(df, template)


def transform(df: pd.DataFrame, template: Template) -> tuple[pd.DataFrame, dict]:
    """Compatibility wrapper for engine transforms."""
    return _ENGINE.transform_data(df, template)


def validate_data(
    df: pd.DataFrame, template: Template, validation_level: str = "coerce"
) -> ValidationResponse:
    """Compatibility wrapper for engine validation response."""
    return _ENGINE.validate_data(df, template, ValidationConfig(level=validation_level))


def run_engine(
    df: pd.DataFrame, template: Template, validation_level: str = "coerce"
) -> tuple[pd.DataFrame, dict, list[str], list[str]]:
    """Run the full engine pipeline on a DataFrame."""
    ingested = ingest(df, template)
    normalized = normalize(ingested, template)
    transformed, metrics = transform(normalized, template)
    missing, extra = warn_on_schema_diff(transformed, template)
    validated = validate(transformed, template, validation_level=validation_level)
    return validated, metrics, missing, extra


__all__ = [
    "DataEngine",
    "ProcessResult",
    "TransformRequest",
    "ValidationResponse",
    "ValidationConfig",
    "ingest",
    "normalize",
    "transform",
    "validate",
    "validate_data",
    "warn_on_schema_diff",
    "run_engine",
]
