"""Unified CLI entrypoint for Data Frame Tool."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
import tkinter as tk
from pathlib import Path

import pandas as pd

from .app import ExcelTemplateApp
from .connectors import check_sqlalchemy_available
from .pipeline import run_pipeline
from .templates import Template, load_template, locate_template
from .combine_runner import run_combine

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_INPUT = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
ARCHIVE_DIR = DATA_DIR / "archive"
QUARANTINE_DIR = DATA_DIR / "quarantine"


def setup_logging(log_to_file: bool = False) -> None:
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    if log_to_file:
        logging.basicConfig(filename="pipeline.log", level=logging.INFO, format=log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)


def _iter_files(input_path: Path):
    return list(input_path.glob("*.xlsx")) + list(input_path.glob("*.csv"))


def run_batch_process(
    target_dir: str,
    output_fmt: str = "xlsx",
    fail_on_missing: bool = False,
    fail_on_extra: bool = False,
    validation_level: str = "coerce",
) -> None:
    """Scans input folder (and optional company subfolders), applies templates, moves files."""

    input_path = Path(target_dir)

    def process_directory(dir_path: Path, company: str | None = None) -> None:
        out_dir = OUTPUT_DIR / company if company else OUTPUT_DIR
        arc_dir = ARCHIVE_DIR / company if company else ARCHIVE_DIR
        qua_dir = QUARANTINE_DIR / company if company else QUARANTINE_DIR
        for d in [out_dir, arc_dir, qua_dir]:
            d.mkdir(parents=True, exist_ok=True)

        logging.info(f"Scanning directory: {dir_path.resolve()}{' (company '+company+')' if company else ''}")

        sql_ran = False
        try:
            tpl_path = locate_template(dir_path)
            template = load_template(tpl_path)
            if template.source_type == "sql":
                check_sqlalchemy_available()
                output_path = out_dir / f"sql_clean.{output_fmt}"
                success = run_pipeline(
                    Path(""),
                    template,
                    output_path,
                    qua_dir,
                    fail_on_missing=fail_on_missing,
                    fail_on_extra=fail_on_extra,
                    validation_level=validation_level,
                )
                if success:
                    logging.info(f"SQL template processed. Output at {output_path}")
                else:
                    logging.warning("SQL template failed; see logs/quarantine.")
                sql_ran = True
        except FileNotFoundError:
            pass
        except Exception as exc:
            logging.error(f"Error checking for SQL template: {exc}")

        files = _iter_files(dir_path)
        if not files and not sql_ran:
            logging.info("No files found in input directory.")
            return

        for file_path in files:
            logging.info(f"Processing {file_path.name}...")
            try:
                try:
                    tpl_path = locate_template(file_path.parent, stem=file_path.stem)
                    template = load_template(tpl_path)
                except FileNotFoundError:
                    logging.warning(f"No template found for {file_path.name}. Skipping.")
                    continue

                ext = ".parquet" if output_fmt == "parquet" else ".xlsx"
                output_path = out_dir / f"{file_path.stem}_clean{ext}"

                success = run_pipeline(
                    file_path,
                    template,
                    output_path,
                    qua_dir,
                    fail_on_missing=fail_on_missing,
                    fail_on_extra=fail_on_extra,
                    validation_level=validation_level,
                )

                if success:
                    dest = arc_dir / file_path.name
                    if dest.exists():
                        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
                        dest = arc_dir / f"{file_path.stem}_{timestamp}{file_path.suffix}"
                    shutil.move(str(file_path), str(dest))
                    logging.info(f"Archived source file to {dest}")
                else:
                    dest = qua_dir / file_path.name
                    if file_path.exists():
                        shutil.move(str(file_path), str(dest))
                    logging.warning(f"Quarantined source file to {dest}")
            except Exception as e:
                logging.error(f"Critical error on {file_path.name}: {e}")

    subdirs = [p for p in input_path.iterdir() if p.is_dir()]
    if subdirs:
        for sub in subdirs:
            process_directory(sub, company=sub.name)
    else:
        process_directory(input_path, company=None)


def run_gui() -> None:
    root = tk.Tk()
    ExcelTemplateApp(root)
    root.mainloop()


def run_combine_cli(input_dir: str, pattern: str, mode: str, keys: list[str], how: str, strict: bool, output: str) -> None:
    df = run_combine(
        input_dir=Path(input_dir),
        pattern=pattern,
        mode=mode,
        keys=keys,
        how=how,
        strict_schema=strict,
    )
    out_path = Path(output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.suffix.lower() == ".parquet":
        df.to_parquet(out_path, index=False)
    else:
        df.to_excel(out_path, index=False)
    logging.info("Combined %d rows using mode=%s. Saved to %s", len(df), mode, out_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Data Frame Tool")
    sub = parser.add_subparsers(dest="command")

    gui = sub.add_parser("gui", help="Launch the GUI.")
    gui.set_defaults(command="gui")

    run = sub.add_parser("run", help="Process files in batch mode.")
    run.add_argument("--target-dir", type=str, default=str(DEFAULT_INPUT), help="Directory to scan for files")
    run.add_argument("--output-fmt", choices=["xlsx", "parquet"], default="xlsx")
    run.add_argument("--fail-on-missing", action="store_true")
    run.add_argument("--fail-on-extra", action="store_true")
    run.add_argument("--validation-level", choices=["off", "coerce", "contract"], default="coerce")

    combine = sub.add_parser("combine", help="Combine cleaned outputs.")
    combine.add_argument("--input-dir", type=str, default="data/output")
    combine.add_argument("--pattern", type=str, default="*.xlsx")
    combine.add_argument("--mode", choices=["concat", "merge"], default="concat")
    combine.add_argument("--keys", type=str, default="")
    combine.add_argument("--how", choices=["inner", "outer", "left", "right"], default="inner")
    combine.add_argument("--strict-schema", action="store_true")
    combine.add_argument("--output", type=str, default="Master_Sales_Report.xlsx")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 1

    setup_logging(log_to_file=args.command == "run")

    if args.command == "gui":
        run_gui()
        return 0

    if args.command == "run":
        run_batch_process(
            args.target_dir,
            output_fmt=args.output_fmt,
            fail_on_missing=args.fail_on_missing,
            fail_on_extra=args.fail_on_extra,
            validation_level=args.validation_level,
        )
        return 0

    if args.command == "combine":
        keys = [k.strip() for k in args.keys.split(",") if k.strip()]
        run_combine_cli(
            input_dir=args.input_dir,
            pattern=args.pattern,
            mode=args.mode,
            keys=keys,
            how=args.how,
            strict=args.strict_schema,
            output=args.output,
        )
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
