"""Header detection and normalization helpers with light caching."""

from __future__ import annotations

from functools import lru_cache
import zipfile
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import pandas as pd
from openpyxl.utils.exceptions import InvalidFileException

from ..templates import apply_normalized_headers, normalize_excel_headers


def guess_header_row(df_preview: pd.DataFrame) -> int:
    """Heuristically guess the header row index from an unlabelled preview."""
    for idx, (_, row) in enumerate(df_preview.iterrows()):
        non_null = row.dropna()
        if non_null.empty:
            continue
        str_ratio = sum(isinstance(val, str) for val in non_null) / len(non_null)
        width_ratio = len(non_null) / df_preview.shape[1] if df_preview.shape[1] else 0
        if str_ratio > 0.8 and width_ratio > 0.5:
            return idx
    return 0


def _header_cache_key(
    path: Path, sheet: str | int | None, header_row: int, skiprows: Sequence[int] | None
) -> Tuple[str, float | None, str | int | None, int, Tuple[int, ...]]:
    resolved = Path(path).resolve()
    try:
        mtime = resolved.stat().st_mtime
    except OSError:
        mtime = None
    sheet_key: str | int | None = sheet
    skip_key: Tuple[int, ...] = tuple(skiprows or [])
    return str(resolved), mtime, sheet_key, int(header_row), skip_key


@lru_cache(maxsize=64)
def _cached_normalized_headers(
    path_str: str, _mtime: float | None, sheet: str | int | None, header_row: int, skiprows: Tuple[int, ...]
) -> Tuple[List[str], bool]:
    headers, merged = normalize_excel_headers(
        path=Path(path_str), sheet=sheet, header_row=header_row, skiprows=list(skiprows)
    )
    return headers, merged


def get_normalized_headers(
    path: Path, sheet: str | int | None, header_row: int, skiprows: Sequence[int] | None = None
) -> Tuple[List[str], bool]:
    """Return normalized headers for a worksheet, cached by file + sheet + offsets."""
    key = _header_cache_key(path, sheet, header_row, skiprows)
    try:
        headers, merged = _cached_normalized_headers(*key)
        return list(headers), merged
    except (zipfile.BadZipFile, InvalidFileException):
        # Fallback: handle legacy XLS or mislabeled CSV that pandas can still read.
        df = pd.read_excel(
            path,
            sheet_name=sheet if sheet is not None else 0,
            header=header_row,
            skiprows=list(skiprows or []),
            nrows=1,
        )
        return [str(c) for c in df.columns], False


def apply_headers(df: pd.DataFrame, headers: Iterable[str]) -> pd.DataFrame:
    """Apply normalized headers to a dataframe, aligning lengths."""
    return apply_normalized_headers(df, list(headers))


__all__ = [
    "apply_headers",
    "get_normalized_headers",
    "guess_header_row",
]
