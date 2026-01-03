"""I/O helpers with lightweight caching for preview reads."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Iterable, List, Sequence

import pandas as pd


def _file_sig(path: Path) -> tuple[str, float | None]:
    resolved = Path(path).resolve()
    try:
        mtime = resolved.stat().st_mtime
    except OSError:
        mtime = None
    return str(resolved), mtime


def _skip_key(skiprows: Sequence[int] | None) -> tuple[int, ...]:
    return tuple(skiprows or ())


@lru_cache(maxsize=32)
def _cached_excel_preview(
    path_str: str,
    _mtime: float | None,
    sheet: str | int | None,
    header_row: int | None,
    skiprows: tuple[int, ...],
    nrows: int | None,
    usecols: tuple[str, ...] | None,
) -> pd.DataFrame:
    return pd.read_excel(
        Path(path_str),
        sheet_name=sheet if sheet is not None else 0,
        header=header_row,
        skiprows=list(skiprows),
        nrows=nrows,
        usecols=list(usecols) if usecols is not None else None,
    )


@lru_cache(maxsize=32)
def _cached_csv_preview(
    path_str: str,
    _mtime: float | None,
    header_row: int | None,
    skiprows: tuple[int, ...],
    nrows: int | None,
    delimiter: str,
    encoding: str,
) -> pd.DataFrame:
    return pd.read_csv(
        Path(path_str),
        header=header_row,
        skiprows=list(skiprows),
        nrows=nrows,
        sep=delimiter,
        encoding=encoding,
    )


def read_preview_frame(
    path: Path,
    source_type: str,
    sheet: str | int | None,
    header_row: int | None,
    skiprows: Sequence[int] | None,
    nrows: int | None,
    delimiter: str = ",",
    encoding: str = "utf-8",
    usecols: Iterable[str] | None = None,
) -> pd.DataFrame:
    """Read a small preview DataFrame with caching to avoid repeated I/O."""
    sig = _file_sig(path)
    if source_type == "csv":
        df = _cached_csv_preview(
            sig[0],
            sig[1],
            header_row,
            _skip_key(skiprows),
            nrows,
            delimiter,
            encoding,
        )
    else:
        usecols_key = tuple(usecols) if usecols is not None else None
        try:
            df = _cached_excel_preview(
                sig[0],
                sig[1],
                sheet,
                header_row,
                _skip_key(skiprows),
                nrows,
                usecols_key,
            )
        except (ValueError, OSError) as exc:
            # Gracefully handle corrupted/mis-labeled Excel files.
            msg = str(exc).lower()
            if (
                "zipfile" in msg
                or "not a zip file" in msg
                or "file format cannot be determined" in msg
            ):
                df = pd.read_csv(
                    path,
                    header=header_row,
                    skiprows=list(skiprows or []),
                    nrows=nrows,
                    encoding=encoding,
                    sep=delimiter,
                )
            else:
                raise
    return df.copy()


@lru_cache(maxsize=16)
def get_sheet_names(path_str: str, _mtime: float | None) -> List[str]:
    """Cached wrapper to fetch sheet names from a workbook."""
    with pd.ExcelFile(Path(path_str)) as xf:
        return list(xf.sheet_names)


def sheet_names(path: Path) -> List[str]:
    sig = _file_sig(path)
    try:
        return get_sheet_names(*sig)
    except Exception:
        return []


__all__ = ["read_preview_frame", "sheet_names"]
