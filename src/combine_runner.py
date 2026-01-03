"""Programmatic wrapper around combine-reports logic for UI use."""

from __future__ import annotations

from pathlib import Path
from typing import List

import pandas as pd


def read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xls", ".xlsx"}:
        return pd.read_excel(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {path.suffix}")


def concat_frames(files: List[Path], strict_schema: bool) -> pd.DataFrame:
    frames = []
    base_cols: list[str] | None = None
    for f in files:
        df = read_frame(f)
        if strict_schema:
            if base_cols is None:
                base_cols = list(df.columns)
            elif list(df.columns) != base_cols:
                raise ValueError(f"Schema mismatch in {f.name}")
        frames.append(df)
    return pd.concat(frames, ignore_index=True, sort=False)


def merge_frames(files: List[Path], keys: List[str], how: str) -> pd.DataFrame:
    if not keys:
        raise ValueError("Merge mode requires at least one key.")
    frames = [read_frame(f) for f in files]
    merged = frames[0]
    for idx, df in enumerate(frames[1:], start=2):
        missing_left = [k for k in keys if k not in merged.columns]
        missing_right = [k for k in keys if k not in df.columns]
        if missing_left or missing_right:
            raise ValueError(
                f"Missing merge keys. Left missing {missing_left}, right missing {missing_right}."
            )
        merged = merged.merge(df, on=keys, how=how, suffixes=("", f"_{idx}"))
    return merged


def run_combine(
    input_dir: Path,
    pattern: str = "*.xlsx",
    mode: str = "concat",
    keys: List[str] | None = None,
    how: str = "inner",
    strict_schema: bool = False,
) -> pd.DataFrame:
    files = sorted(input_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No files found in {input_dir} with pattern {pattern}")
    if mode == "concat":
        return concat_frames(files, strict_schema=strict_schema)
    return merge_frames(files, keys or [], how=how)
