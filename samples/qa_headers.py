"""
Quick QA helper to exercise header detection on sample workbooks.

Runs the bundled sample generator if needed, then prints guessed header rows
and detected columns for each sample file.
"""

from __future__ import annotations

import pathlib
import sys
from typing import Iterable

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.core import guess_header_row  # type: ignore
from src.templates import normalize_excel_headers  # type: ignore
from samples.generate_samples import main as generate_samples  # type: ignore

SAMPLES = [
    "multi_sheet_jan.xlsx",
    "consistent_schema_feb.xlsx",
    "offset_header.xlsx",
    "merged_header.xlsx",
    "split_year_month.xlsx",
]


def ensure_samples(root: pathlib.Path) -> None:
    missing = [f for f in SAMPLES if not (root / f).exists()]
    if missing:
        print("Generating missing samples:", ", ".join(missing))
        generate_samples()


def _print_headers(headers: Iterable[str]) -> str:
    return ", ".join(str(h) for h in headers)


def check_excel(path: pathlib.Path) -> None:
    try:
        xf = pd.ExcelFile(path)
    except Exception as exc:
        print(f"[{path.name}] failed to open: {exc}")
        return

    for sheet in xf.sheet_names:
        try:
            preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=10)
        except Exception as exc:
            print(f"[{path.name}::{sheet}] failed to read preview: {exc}")
            continue

        guessed = guess_header_row(preview)
        try:
            normalized, merged = normalize_excel_headers(
                path=path, sheet=sheet, header_row=guessed, skiprows=None
            )
        except Exception:
            normalized, merged = [], False

        try:
            df = pd.read_excel(path, sheet_name=sheet, header=guessed, nrows=5)
            detected = list(df.columns)
        except Exception:
            detected = []

        print(f"[{path.name}::{sheet}] header_row_guess={guessed}, merged={merged}")
        if normalized:
            print("  normalized:", _print_headers(normalized))
        if detected:
            print("  detected :", _print_headers(detected))


def main() -> None:
    root = pathlib.Path(__file__).resolve().parent
    ensure_samples(root)
    for sample in SAMPLES:
        path = root / sample
        if not path.exists():
            print(f"Skipping missing {sample}")
            continue
        check_excel(path)


if __name__ == "__main__":
    main()
