from pathlib import Path

import pandas as pd
import pytest

from src.core import guess_header_row
from src.templates import normalize_excel_headers


SAMPLES_DIR = Path("samples")
EXPECTED_PATH = SAMPLES_DIR / "expected.json"


def load_expected():
    import json

    return json.loads(EXPECTED_PATH.read_text(encoding="utf-8"))


@pytest.mark.parametrize("key,meta", load_expected().items())
def test_sample_headers(key, meta):
    fname, sheet = key.split("::", 1)
    path = SAMPLES_DIR / fname
    if not path.exists():
        pytest.skip(f"missing sample {fname}")

    expected_headers = [str(h) for h in meta.get("expected_headers", [])]
    expected_header_row = meta.get("expected_header_row")

    preview = pd.read_excel(path, sheet_name=sheet, header=None, nrows=12)
    guessed = guess_header_row(preview)
    normalized, _merged = normalize_excel_headers(
        path=path, sheet=sheet, header_row=guessed, skiprows=None
    )
    headers = [h for h in normalized if str(h).strip()]

    missing = sorted(set(expected_headers) - set(headers))
    extra = sorted(set(headers) - set(expected_headers))

    assert not missing and not extra
    if expected_header_row is not None:
        assert guessed == expected_header_row
