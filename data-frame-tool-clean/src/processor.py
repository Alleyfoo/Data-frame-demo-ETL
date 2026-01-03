"""Processing module for applying saved templates to Excel or CSV files.

Legacy: This module is kept for reference only and is not wired into the
current CLI entrypoints, which rely on ``src/pipeline.py`` instead.
"""
from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

from .templates import (
    COMMON_FIELDS_HELP,
    Template,
    locate_template,
    load_template,
    read_excel_with_template,
)

EXCEL_SUFFIXES = {".xls", ".xlsx", ".xlsm"}
CSV_SUFFIXES = {".csv"}
SUPPORTED_SUFFIXES = {".xls", ".xlsx", ".xlsm", ".csv"}


@dataclass
class TemplateConfig:
    """Configuration describing how to extract data from Excel or CSV files.

    Attributes:
        source_type: Indicates whether the template targets Excel or CSV files.
        sheet_name: Name or index of the sheet to read (Excel only).
        header_row: Zero-based row index to use as the header when reading.
        delimiter: Field delimiter for CSV sources.
        encoding: Optional file encoding for CSV sources.
        use_columns: Optional list of columns to retain. Columns may be names
            or letter/index notation accepted by ``pandas.read_excel`` or
            labels accepted by ``pandas.read_csv``.
    """

    source_type: str = "excel"
    sheet_name: str | int | None = 0
    header_row: int = 0
    delimiter: str | None = ","
    encoding: str | None = None
    delimiter: Optional[str] = ","
    encoding: Optional[str] = "utf-8"
    use_columns: Optional[List[str | int]] = None

    @classmethod
    def from_dict(cls, payload: dict) -> "TemplateConfig":
        sheet_name = payload.get("sheet", payload.get("sheet_name"))
        header_row = payload.get("header", payload.get("header_row", 0))
        use_columns = payload.get("columns")
        source_type = payload.get("source_type", "excel")
        delimiter = payload.get("delimiter", ",")
        encoding = payload.get("encoding")
        delimiter = payload.get("delimiter", ",")
        encoding = payload.get("encoding", "utf-8")

        if header_row is None:
            header_row = 0
        if not isinstance(header_row, int) or header_row < 0:
            raise ValueError("Template header must be a non-negative integer")
        if use_columns is not None and not isinstance(use_columns, list):
            raise ValueError("Template 'columns' must be a list when provided")
        if source_type not in {"excel", "csv"}:
            raise ValueError("source_type must be either 'excel' or 'csv'")

        return cls(
            source_type=source_type,

        return cls(
            sheet_name=sheet_name,
            header_row=header_row,
            delimiter=delimiter,
            encoding=encoding,
            use_columns=use_columns,
        )


def load_template(path: Path) -> TemplateConfig:
    """Load and validate a template file.

    Args:
        path: Path to a JSON file containing template metadata.

    Raises:
        FileNotFoundError: if the template file does not exist.
        ValueError: for invalid JSON or missing/invalid fields.
    """

    if not path.exists():
        raise FileNotFoundError(f"Template not found: {path}")

    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive logging
        raise ValueError(f"Template is not valid JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise ValueError("Template file must contain a JSON object")

    return TemplateConfig.from_dict(payload)


def discover_sources(directory: Path, source_type: str) -> Iterable[Path]:
    """Yield source files from the provided directory."""

    suffixes = CSV_SUFFIXES if source_type == "csv" else EXCEL_SUFFIXES
    for path in sorted(directory.iterdir()):
        if path.suffix.lower() in suffixes and path.is_file():
            yield path


def read_with_template(path: Path, template: TemplateConfig) -> pd.DataFrame:
    """Read a file using template settings."""

    logging.info("Reading %s", path.name)
    try:
        if template.source_type == "csv":
            df = pd.read_csv(
                path,
                sep=template.delimiter or ",",
                encoding=template.encoding,
                header=template.header_row,
                usecols=template.use_columns,
def read_excel_with_template(path: Path, template: TemplateConfig) -> pd.DataFrame:
    """Read an Excel or CSV file using template settings."""

    logging.info("Reading %s", path.name)
    try:
        if path.suffix.lower() == ".csv":
            df = pd.read_csv(
                path,
                header=template.header_row,
                usecols=template.use_columns,
                sep=template.delimiter or ",",
                encoding=template.encoding or None,
            )
        else:
            df = pd.read_excel(
                path,
                sheet_name=template.sheet_name if template.sheet_name is not None else 0,
                header=template.header_row,
                usecols=template.use_columns,
            )
    except ValueError as exc:  # includes sheet errors and usecols mismatches
        raise ValueError(f"Failed to read {path.name}: {exc}") from exc

    # Basic normalization: drop fully empty rows/columns.
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    return df


def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False)


def build_output_name(base: str, original: Path, aggregate: bool) -> str:
    if aggregate:
        return base if base.endswith(".xlsx") else f"{base}.xlsx"
    if "{stem}" in base:
        filename = base.format(stem=original.stem)
    else:
        filename = f"{original.stem}_{base}"
    return filename if filename.endswith(".xlsx") else f"{filename}.xlsx"


def process_directory(
    directory: Path,
    template: Template,
    output_dir: Path,
    aggregate: bool = False,
    output_name: str = "cleaned",
    include_source_column: bool = True,
) -> Path | List[Path]:
    """Apply a template to all files in a directory.

    Returns the path(s) to created file(s).
    """

    template = load_template(template_path)

    sources = list(discover_sources(directory, template.source_type))
    if not sources:
        raise FileNotFoundError(f"No {template.source_type.upper()} files found in {directory}")
    excels = list(discover_excels(directory))
    if not excels:
        raise FileNotFoundError(f"No Excel files found in {directory}")

    if aggregate:
        frames: List[pd.DataFrame] = []
        for source in sources:
            try:
                df = read_with_template(source, template)
            except ValueError as exc:
                logging.error(exc)
                continue
            if include_source_column:
                df["source_file"] = source.name
            frames.append(df)

        if not frames:
            raise RuntimeError("No data could be aggregated; see logs for details")

        combined = pd.concat(frames, ignore_index=True)
        output_file = output_dir / build_output_name(output_name, sources[0], aggregate=True)
        save_dataframe(combined, output_file)
        logging.info("Aggregated data written to %s", output_file)
        return output_file

    created: List[Path] = []
    for source in sources:
        try:
            df = read_with_template(source, template)
        except ValueError as exc:
            logging.error(exc)
            continue

        output_file = output_dir / build_output_name(output_name, source, aggregate=False)
        save_dataframe(df, output_file)
        logging.info("Cleaned workbook written to %s", output_file)
        created.append(output_file)

    if not created:
        raise RuntimeError("No workbooks were processed successfully; see logs for details")

    return created


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Normalize spreadsheet or CSV files using a saved template")
    parser.add_argument("directory", type=Path, help="Directory containing Excel or CSV files")
    parser = argparse.ArgumentParser(
        description=(
            "Normalize Excel files using a saved template. "
            + COMMON_FIELDS_HELP
        )
    )
    parser.add_argument("directory", type=Path, help="Directory containing Excel files")
    parser.add_argument(
        "--template",
        type=Path,
        default=None,
        help=(
            "Path to template JSON/YAML. Defaults to the first <name>.df-template.* "
            "file in the directory; legacy '*_template.*' names will emit a migration hint."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help=(
            "Directory for output files. Defaults to the template's output_dir if "
            "present, otherwise the input directory"
        ),
    )
    parser.add_argument(
        "--aggregate",
        action="store_true",
        help="Aggregate all normalized data into a single workbook",
    )
    parser.add_argument(
        "--output-name",
        default="cleaned",
        help="Filename or pattern for outputs. Use {stem} for original stem when not aggregating.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level",
    )
    parser.add_argument(
        "--no-source-column",
        action="store_true",
        help="Do not add a source_file column when aggregating",
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=getattr(logging, args.log_level), format="%(levelname)s: %(message)s")

    directory = args.directory
    template_path = args.template or locate_template(directory)

    template = load_template(template_path)
    output_dir = args.output_dir or Path(template.output_dir or directory)

    try:
        result = process_directory(
            directory=directory,
            template=template,
            output_dir=output_dir,
            aggregate=args.aggregate,
            output_name=args.output_name,
            include_source_column=not args.no_source_column,
        )
    except (FileNotFoundError, ValueError, RuntimeError) as exc:
        logging.error(exc)
        return 1

    if isinstance(result, list):
        logging.info("Processed %d files", len(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
