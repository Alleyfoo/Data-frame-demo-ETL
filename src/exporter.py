"""Dataset export utilities with manifest and basic quality metrics."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd


def _null_pct(series: pd.Series) -> float:
    total = len(series)
    if total == 0:
        return 0.0
    return round(series.isna().sum() * 100.0 / total, 2)


def _dtype_map(df: pd.DataFrame) -> dict[str, str]:
    return {col: str(dtype) for col, dtype in df.dtypes.items()}


def _metrics(df: pd.DataFrame) -> dict:
    dup_count = df.duplicated().sum() if not df.empty else 0
    return {
        "rows": int(df.shape[0]),
        "columns": int(df.shape[1]),
        "dtypes": _dtype_map(df),
        "null_pct": {col: _null_pct(df[col]) for col in df.columns},
        "duplicates": int(dup_count),
    }


def _write_excel(df: pd.DataFrame, meta: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path) as writer:
        df.to_excel(writer, sheet_name="data", index=False)
        meta_rows = pd.DataFrame([{"key": k, "value": v} for k, v in meta.items()])
        meta_rows.to_excel(writer, sheet_name="meta", index=False)

        # Basic formatting: freeze header row and apply autofilter
        data_sheet = writer.sheets["data"]
        # Some engines (like openpyxl) use attributes, not callables
        if hasattr(data_sheet, "freeze_panes"):
            try:
                data_sheet.freeze_panes = data_sheet["B2"]  # freeze first row
            except Exception:
                pass
        if not df.empty and hasattr(data_sheet, "auto_filter"):
            try:
                last_col = df.shape[1]
                last_row = df.shape[0]
                data_sheet.auto_filter.ref = f"A1:{chr(ord('A') + last_col - 1)}{last_row+1}"
            except Exception:
                pass
        # Ensure column order is the DataFrame order (default behavior already)


def _write_jsonl(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            f.write(json.dumps(row.to_dict(), ensure_ascii=False))
            f.write("\n")


def export_dataset(
    df: pd.DataFrame,
    out_dir: str | Path,
    formats: Iterable[str] = ("xlsx", "jsonl"),
    meta: Mapping | None = None,
) -> dict[str, Path]:
    """
    Export a dataset to the given formats and write a manifest.json.

    Args:
        df: DataFrame to export.
        out_dir: Output directory.
        formats: Iterable of formats, any of {"xlsx","jsonl","parquet"}.
        meta: Additional manifest fields (must include run/usage info).
    """
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    manifest: dict = {
        "run_id": str(uuid.uuid4()),
        "run_started_at": datetime.now(timezone.utc).isoformat(),
        "formats": sorted(set(formats)),
    }
    if meta:
        manifest.update(meta)

    manifest["metrics"] = _metrics(df)

    written: dict[str, Path] = {}
    for fmt in manifest["formats"]:
        fmt_lower = fmt.lower()
        if fmt_lower == "xlsx":
            target = out_path / "data.xlsx"
            _write_excel(df, manifest, target)
        elif fmt_lower == "jsonl":
            target = out_path / "data.jsonl"
            _write_jsonl(df, target)
        elif fmt_lower == "parquet":
            target = out_path / "data.parquet"
            target.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(target, index=False)
        else:
            continue
        written[fmt_lower] = target

    manifest_path = out_path / "manifest.json"
    manifest["run_completed_at"] = datetime.now(timezone.utc).isoformat()
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    written["manifest"] = manifest_path
    return written


__all__ = ["export_dataset"]
