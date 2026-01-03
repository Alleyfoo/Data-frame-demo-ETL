"""LEGACY / UNUSED ENTRYPOINT.

This prototype predates the current pipeline and GUI. It is kept only for
reference and is not wired into supported entrypoints. Prefer
``src/pipeline.py`` (batch) or ``main.py`` (GUI/batch) instead.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd

from .templates import (
    COMMON_FIELDS_HELP,
    Template,
    default_template_path,
    load_template,
    read_excel_with_template,
    save_template,
)

# Supported Excel extensions for discovery when applying a template.
EXCEL_EXTENSIONS = (".xlsx", ".xls", ".xlsm", ".xlsb")
# Supported tabular extensions for discovery when applying a template.
DATA_EXTENSIONS = (".xlsx", ".xls", ".xlsm", ".xlsb", ".csv")


def list_sheets(excel_path: Path) -> List[str]:
    """Return sheet names from an Excel workbook."""
    if excel_path.suffix.lower() == ".csv":
        return []
    workbook = pd.ExcelFile(excel_path)
    return workbook.sheet_names


def prompt_for_sheet(sheet_names: Sequence[str], chosen: str | None) -> str | None:
    """Return a sheet name, optionally prompting the user to choose."""
    if not sheet_names:
        return None
    if chosen and chosen in sheet_names:
        return chosen

    print("Available sheets:")
    for idx, name in enumerate(sheet_names):
        print(f"[{idx}] {name}")

    while True:
        response = input("Select sheet index (default 0): ").strip()
        if not response:
            return sheet_names[0]
        if response.isdigit():
            index = int(response)
            if 0 <= index < len(sheet_names):
                return sheet_names[index]
        print("Invalid selection, try again.")


def prompt_for_header_row(default_header: int | None) -> int:
    """Prompt for a header row index (zero-based)."""
    if default_header is not None:
        prompt = f"Header row index (default {default_header}): "
    else:
        prompt = "Header row index: "

    while True:
        response = input(prompt).strip()
        if not response and default_header is not None:
            return default_header
        if response.isdigit():
            return int(response)
        print("Please provide a numeric header row index (zero-based).")


def prompt_for_columns(columns: Sequence[str], preselected: Sequence[str] | None) -> List[str]:
    """Prompt the user to choose columns by index or name."""
    if preselected:
        chosen = [col for col in preselected if col in columns]
        if chosen:
            return chosen

    print("Columns in the sheet:")
    for idx, name in enumerate(columns):
        print(f"[{idx}] {name}")

    while True:
        response = input(
            "Enter column numbers or names separated by commas (blank keeps all): "
        ).strip()
        if not response:
            return list(columns)

        selections = [item.strip() for item in response.split(",") if item.strip()]
        resolved: list[str] = []
        for item in selections:
            if item.isdigit():
                index = int(item)
                if 0 <= index < len(columns):
                    resolved.append(columns[index])
            elif item in columns:
                resolved.append(item)
        if resolved:
            return resolved
        print("No valid selections detected; try again.")


def build_template(
    excel_path: Path,
    sheet_name: str | None,
    header_row: int,
    selected_headers: Sequence[str],
    delimiter: str | None,
    encoding: str | None,
    output_dir: Path | None = None,
    delimiter: str = ",",
    encoding: str = "utf-8",
) -> dict:
    """Construct a template dictionary for JSON serialization."""
    parent_dir = source_path.parent
    template_path = (
        parent_dir / output_dir if output_dir else parent_dir
    ) / f"{source_path.stem}.template.json"

    return {
        "template_version": 1,
        "source_file": source_path.name,
        "source_type": "csv" if source_path.suffix.lower() in CSV_EXTENSIONS else "excel",
        "sheet_name": sheet_name,
        "header_row": header_row,
        "delimiter": delimiter,
        "encoding": encoding,
        "selected_headers": list(selected_headers),
        "output_dir": str(template_path.parent.resolve()),
    }


def save_template(template: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(template, fp, indent=2)
    print(f"Template saved to {path}")


def load_template(template_path: Path) -> dict:
    with template_path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def discover_excel_files(folder: Path) -> List[Path]:
    """Return supported data files in the folder (non-recursive)."""
    return sorted(
        path
        for path in folder.iterdir()
        if path.is_file() and path.suffix.lower() in DATA_EXTENSIONS
    )


def apply_template_to_file(file_path: Path, template: Template) -> pd.DataFrame:
    """Apply a template to a single file and return the cleaned DataFrame."""
    if file_path.suffix.lower() == ".csv":
        df = pd.read_csv(
            file_path,
            header=template.get("header_row", 0),
            sep=template.get("delimiter", ","),
            encoding=template.get("encoding", "utf-8"),
        )
    else:
        df = pd.read_excel(
            file_path,
            sheet_name=template.get("sheet_name") or 0,
            header=template.get("header_row", 0),
        )

    cleaned = read_excel_with_template(file_path, template)
    cleaned.insert(0, "source_file", file_path.name)
    return cleaned


def handle_create_template(args: argparse.Namespace) -> None:
    source_path = Path(args.source).expanduser().resolve()
    delimiter = args.delimiter
    encoding = args.encoding

    if source_path.suffix.lower() in CSV_EXTENSIONS:
        sheet = None
        header_row = prompt_for_header_row(
            args.header_row if args.header_row is not None else 0
        )
        df_preview = pd.read_csv(
            source_path,
            sep=delimiter,
            encoding=encoding,
            header=header_row,
            nrows=5,
        )
    else:
        sheet_names = list_sheets(source_path)
        sheet = prompt_for_sheet(sheet_names, args.sheet)
        header_row = prompt_for_header_row(
            args.header_row if args.header_row is not None else 0
        )
        df_preview = pd.read_excel(
            source_path, sheet_name=sheet, header=header_row, nrows=5
        )

    header_row = prompt_for_header_row(args.header_row if args.header_row is not None else 0)

    if excel_path.suffix.lower() == ".csv":
        df_preview = pd.read_csv(
            excel_path,
            nrows=5,
            header=header_row,
            sep=args.delimiter,
            encoding=args.encoding,
        )
    else:
        df_preview = pd.read_excel(excel_path, sheet_name=sheet, header=header_row, nrows=5)
    selected_headers = prompt_for_columns(df_preview.columns.tolist(), args.columns)

    template = Template(
        sheet=sheet,
        header_row=header_row,
        columns=list(selected_headers),
        source_file=excel_path.name,
        output_dir=str(Path(args.output_dir).expanduser().resolve()) if args.output_dir else None,
    )

    template_target = default_template_path(
        Path(args.output_dir).expanduser().resolve() / excel_path.name
        if args.output_dir
        else excel_path
    )
    save_template(template, template_target)
    print(
        f"Template saved to {template_target}\n"
        f"Fields: {COMMON_FIELDS_HELP}"
        selected_headers=selected_headers,
        delimiter=delimiter,
        encoding=encoding,
        output_dir=Path(args.output_dir).expanduser().resolve() if args.output_dir else None,
        delimiter=args.delimiter,
        encoding=args.encoding,
    )


def handle_apply_template(args: argparse.Namespace) -> None:
    template_path = Path(args.template).expanduser().resolve()
    template = load_template(template_path)
    source_type = template.get("source_type", "excel")

    target_dir = Path(args.folder).expanduser().resolve() if args.folder else template_path.parent
    output_dir = Path(args.output_dir).expanduser().resolve()
    if not args.output_dir:
        output_dir = Path(template.output_dir) if template.output_dir else template_path.parent
    output_dir.mkdir(parents=True, exist_ok=True)

    files = discover_files(target_dir, source_type)
    if not files:
        print(f"No {source_type.upper()} files found in {target_dir}.")
        return

    combined_frames: list[pd.DataFrame] = []
    for file_path in files:
        print(f"Processing {file_path.name}...")
        cleaned = apply_template_to_file(file_path, template)
        combined_frames.append(cleaned)

        cleaned_path = output_dir / f"{file_path.stem}_cleaned.csv"
        cleaned.to_csv(cleaned_path, index=False)
        print(f"  -> Saved cleaned data to {cleaned_path}")

    combined_df = pd.concat(combined_frames, ignore_index=True)
    combined_path = output_dir / "combined.csv"
    combined_df.to_csv(combined_path, index=False)
    print(f"Combined dataset saved to {combined_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Prototype Excel-to-DataFrame workflow using the shared template format. "
            + COMMON_FIELDS_HELP
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser(
        "create-template",
        help=(
            "Inspect an Excel file and save a df-template JSON/YAML with sheet, "
            "header row, and column selections"
        ),
    )
    create.add_argument("source", help="Path to the Excel or CSV file")
    create.add_argument("--sheet", help="Sheet name to use (defaults to prompt)")
    create.add_argument(
        "--header-row",
        type=int,
        help="Zero-based row index containing headers (defaults to prompt)",
    )
    create.add_argument(
        "--delimiter",
        default=",",
        help="Delimiter to use when reading CSV files",
    )
    create.add_argument(
        "--encoding",
        default="utf-8",
        help="Encoding to use when reading CSV files",
    )
    create.add_argument(
        "--columns",
        nargs="+",
        help="Headers to keep (defaults to prompt; accepts multiple values)",
    )
    create.add_argument(
        "--output-dir",
        help=(
            "Directory for the generated template (defaults to the Excel directory). "
            "Templates are saved as <workbook>.df-template.json"
        ),
    )
    create.set_defaults(func=handle_create_template)

    apply = subparsers.add_parser(
        "apply-template",
        help=(
            "Apply a saved template to Excel files in a folder. "
            "Accepts JSON/YAML templates created by any tool in this repo."
        ),
    )
    apply.add_argument(
        "template",
        help="Path to the saved template JSON/YAML (df-template.* or legacy *_template.*)",
    )
    apply.add_argument(
        "--folder",
        help="Folder containing source files (defaults to the template's directory)",
    )
    apply.add_argument(
        "--output-dir",
        help="Where to write cleaned and combined CSVs (defaults to the template's output_dir)",
    )
    apply.set_defaults(func=handle_apply_template)

    return parser


def main(argv: Iterable[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(args=argv)
    args.func(args)


if __name__ == "__main__":
    main()
