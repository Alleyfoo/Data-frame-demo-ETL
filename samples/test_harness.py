"""
Lightweight QA harness for header detection heuristics.

- Regenerates sample workbooks if missing
- Reads expected headers from samples/expected.json
- Runs the same header guessing + normalization logic as the app
- Prints PASS/FAIL with diffs
"""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Dict, List, Tuple

import pandas as pd

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from samples.generate_samples import main as generate_samples  # type: ignore
from src.core import guess_header_row  # type: ignore
from src.templates import normalize_excel_headers  # type: ignore

EXPECTED_PATH = pathlib.Path(__file__).parent / "expected.json"
SAMPLES_DIR = pathlib.Path(__file__).parent


def load_expected() -> Dict[str, Dict[str, object]]:
    if not EXPECTED_PATH.exists():
        raise FileNotFoundError(f"expected.json not found at {EXPECTED_PATH}")
    return json.loads(EXPECTED_PATH.read_text(encoding="utf-8"))


def ensure_samples(expected: Dict[str, Dict[str, object]]) -> None:
    needed = {entry.split("::")[0] for entry in expected.keys()}
    missing = [fname for fname in needed if not (SAMPLES_DIR / fname).exists()]
    if missing:
        print("Generating missing samples:", ", ".join(missing))
        generate_samples()


def derive_headers(path: pathlib.Path, sheet: str | int) -> Tuple[List[str], int]:
    """Guess header row, expand merged headers, return headers and guessed row."""
    preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=12)
    guessed = guess_header_row(preview)
    headers: List[str] = []
    try:
        normalized, _merged = normalize_excel_headers(
            path=path, sheet=sheet, header_row=guessed, skiprows=None
        )
        headers = [h for h in normalized if str(h).strip()]
    except Exception:
        headers = []
    if not headers:
        df = pd.read_excel(path, sheet_name=sheet, header=guessed, nrows=1)
        headers = [str(col) for col in df.columns if str(col).strip()]
    return headers, guessed


def diff_headers(expected: List[str], actual: List[str]) -> Tuple[List[str], List[str]]:
    exp_set = set(expected)
    act_set = set(actual)
    missing = sorted(list(exp_set - act_set))
    extra = sorted(list(act_set - exp_set))
    return missing, extra


def run_tests() -> int:
    expected = load_expected()
    ensure_samples(expected)
    failures = 0

    for key, meta in expected.items():
        fname, sheet = key.split("::", 1)
        expected_headers = [str(h) for h in meta.get("expected_headers", [])]
        expected_header_row = meta.get("expected_header_row")
        path = SAMPLES_DIR / fname
        if not path.exists():
            print(f"[SKIP] {key}: sample missing at {path}")
            continue
        headers, guessed_row = derive_headers(path, sheet)
        missing, extra = diff_headers(expected_headers, headers)
        ok = not missing and not extra

        if expected_header_row is not None and guessed_row != expected_header_row:
            ok = False
            extra_note = f", header_row expected={expected_header_row} got={guessed_row}"
        else:
            extra_note = ""

        status = "PASS" if ok else "FAIL"
        print(f"[{status}] {key}")
        print(f"  expected: {', '.join(expected_headers)}")
        print(f"  actual  : {', '.join(headers)}")
        if missing:
            print(f"  missing : {', '.join(missing)}")
        if extra:
            print(f"  extra   : {', '.join(extra)}")
        if extra_note:
            print(f"  note    : {extra_note}")

        if not ok:
            failures += 1

    print(f"\nCompleted {len(expected)} checks. Failures: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(run_tests())
