"""Heuristics for ranking schema/header candidates separate from the UI."""

from __future__ import annotations

from typing import Dict, Iterable, List, Mapping, Sequence, Tuple

import pandas as pd


def numeric_ratio(series: pd.Series) -> float:
    try:
        return pd.to_numeric(series, errors="coerce").notna().mean()
    except Exception:
        return 0.0


def is_year_like(series: pd.Series) -> bool:
    try:
        vals = pd.to_numeric(series, errors="coerce")
        clean = vals.dropna()
        if clean.empty:
            return False
        return (clean.between(1900, 2100)).mean() > 0.6
    except Exception:
        return False


def is_numeric_col(series: pd.Series) -> bool:
    return numeric_ratio(series) > 0.6 and not is_year_like(series)


def is_texty_col(series: pd.Series) -> bool:
    return series.fillna("").astype(str).map(len).mean() > 12 and numeric_ratio(series) < 0.3


def find_numeric_blocks(df: pd.DataFrame) -> List[Dict[str, object]]:
    """Identify contiguous numeric column blocks that are not year-like."""
    numeric_flags: List[bool] = []
    for col in df.columns:
        series = df[col]
        flag = is_numeric_col(series)
        numeric_flags.append(flag)

    blocks: List[List[int]] = []
    current: List[int] = []
    for idx, is_num in enumerate(numeric_flags):
        if is_num:
            current.append(idx)
        else:
            if current:
                blocks.append(current)
            current = []
    if current:
        blocks.append(current)

    results: List[Dict[str, object]] = []
    for block in blocks:
        cols = [df.columns[i] for i in block]
        if not cols:
            continue
        results.append(
            {
                "columns": cols,
                "start_idx": block[0],
                "end_idx": block[-1],
            }
        )
    return results


def _normalize_month(token: str) -> str | None:
    month_map = {
        "tammikuu": "jan",
        "helmikuu": "feb",
        "maaliskuu": "mar",
        "huhtikuu": "apr",
        "toukokuu": "may",
        "kesäkuu": "jun",
        "heinäkuu": "jul",
        "elokuu": "aug",
        "syyskuu": "sep",
        "lokakuu": "oct",
        "marraskuu": "nov",
        "joulukuu": "dec",
        "januaari": "jan",
        "january": "jan",
        "february": "feb",
        "march": "mar",
        "april": "apr",
        "may": "may",
        "june": "jun",
        "july": "jul",
        "august": "aug",
        "september": "sep",
        "october": "oct",
        "november": "nov",
        "december": "dec",
        "januari": "jan",
        "februari": "feb",
        "mars": "mar",
        "maj": "may",
        "juni": "jun",
        "juli": "jul",
        "augusti": "aug",
        "oktober": "oct",
        "maerz": "mar",
        "märz": "mar",
        "mai": "may",
        "dezember": "dec",
    }
    lower = token.lower()
    if lower in month_map:
        return month_map[lower]
    for eng in ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]:
        if eng in lower:
            return eng
    return None


def schema_diff(headers: Sequence[str], target_fields: Iterable[str] | None) -> Tuple[List[str], List[str]]:
    current = set(target_fields or [])
    proposed = set(headers)
    missing = sorted(list(current - proposed))
    extra = sorted(list(proposed - current))
    return missing, extra


def build_schema_candidates(
    df: pd.DataFrame,
    headers: Sequence[str],
    data_type: str = "generic",
    target_fields: Iterable[str] | None = None,
) -> List[Dict[str, object]]:
    """Return ranked header candidates with heuristic scores and diff annotations."""
    candidates: List[Dict[str, object]] = []

    def add_candidate(label: str, headers_in: List[str], score: float, note: str) -> None:
        candidates.append({"label": label, "headers": headers_in, "score": score, "note": note})

    add_candidate("As detected", list(headers), 0.20, "Headers as read from file.")

    numeric_cols = [col for col in df.columns if is_numeric_col(df[col])]
    text_cols = [col for col in df.columns if is_texty_col(df[col])]

    combined_headers: List[str] = []
    combined_changed = False
    for h in headers:
        parts = str(h).replace("/", " ").replace("-", " ").split()
        year = next((p for p in parts if p.isdigit() and len(p) == 4), None)
        month = next((_normalize_month(p) for p in parts if _normalize_month(p)), None)
        if year and month:
            combined_headers.append(f"{year}-{month}")
            combined_changed = True
        else:
            combined_headers.append(str(h))

    if combined_changed:
        add_candidate("Combined year+month headers", combined_headers, 0.35, "Merged year + month tokens into single period labels.")

    block_info = find_numeric_blocks(df)
    for block in block_info:
        cols = block["columns"]
        start_idx = block["start_idx"]
        note = f"Numeric block cols {start_idx}-{start_idx+len(cols)-1} (size {len(cols)})"
        key_col = None
        if start_idx > 0:
            left_col = df.columns[start_idx - 1]
            if left_col in text_cols:
                key_col = left_col
        ordered = list(cols)
        if key_col and key_col not in ordered:
            ordered = [key_col] + ordered
            note += f"; key column '{key_col}' on the left."
            score = 0.6 + 0.05 * len(cols)
        else:
            score = 0.5 + 0.05 * len(cols)
        add_candidate("Numeric block ordering", ordered, min(score, 0.9), note)

    if data_type == "product_sales":
        key_col = text_cols[0] if text_cols else None
        if key_col and numeric_cols:
            ordered = [key_col] + [c for c in df.columns if c in numeric_cols]
            add_candidate(
                "Product key + numeric measures",
                ordered,
                0.55 + 0.05 * len(numeric_cols),
                f"Text key '{key_col}' with numeric measures.",
            )

    if data_type == "product_descriptions":
        key_col = text_cols[0] if text_cols else None
        if key_col:
            ordered = [key_col] + [c for c in df.columns if c != key_col]
            add_candidate(
                "Description-first ordering",
                ordered,
                0.45,
                f"Longest text column '{key_col}' first.",
            )

    if data_type == "sales":
        if numeric_cols:
            ordered = numeric_cols + [c for c in df.columns if c not in numeric_cols]
            add_candidate(
                "Numeric-first (sales) ordering",
                ordered,
                0.5 + 0.05 * len(numeric_cols),
                "Prioritized numeric columns (likely amounts/quantities).",
            )

    filtered: List[Dict[str, object]] = []
    for cand in candidates:
        if cand["label"] != "As detected" and cand.get("score", 0) < 0.25:
            continue
        filtered.append(cand)

    annotated: List[Dict[str, object]] = []
    for cand in filtered:
        hdrs = [str(h) for h in cand.get("headers", [])]
        missing, extra = schema_diff(hdrs, target_fields)
        note = cand.get("note", "")
        if missing or extra:
            miss_txt = (
                f" missing vs current schema: {', '.join(missing[:5])}" + ("..." if len(missing) > 5 else "")
                if missing
                else ""
            )
            extra_txt = (
                f" extra: {', '.join(extra[:5])}" + ("..." if len(extra) > 5 else "")
                if extra
                else ""
            )
            note = f"{note} |{miss_txt} {extra_txt}".strip()
        annotated.append({**cand, "note": note, "missing": missing, "extra": extra})

    return annotated


__all__ = [
    "build_schema_candidates",
    "find_numeric_blocks",
    "is_numeric_col",
    "is_texty_col",
    "numeric_ratio",
    "schema_diff",
]
