from __future__ import annotations

import re
from difflib import SequenceMatcher

import pandas as pd
import streamlit as st
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype, is_numeric_dtype

from src.core.config_loader import load_synonyms
from src.core.state import SessionState
from src.core.streamlit_io import read_uploaded_dataframe
from src.templates import parse_skiprows


DEFAULTS = {
    "uploaded_name": None,
    "uploaded_bytes": None,
    "header_row": 0,
    "skiprows": "",
    "delimiter": ",",
    "encoding": "utf-8",
    "sheet_name": None,
    "selected_column": None,
    "mappings": {},
}


def _normalize(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def _best_target(source: str, synonyms: dict[str, list[str]]) -> str | None:
    source_norm = _normalize(source)
    best_score = 0.0
    best_target = None

    for target, terms in synonyms.items():
        candidates = [target] + list(terms)
        for term in candidates:
            score = SequenceMatcher(None, source_norm, _normalize(term)).ratio()
            if score > best_score:
                best_score = score
                best_target = target

    return best_target if best_score >= 0.6 else None


def _infer_type(series: pd.Series) -> str:
    if is_datetime64_any_dtype(series):
        return "Date"
    if is_numeric_dtype(series):
        return "Number"
    if is_bool_dtype(series):
        return "Boolean"
    return "Text"


def _ensure_selectbox_state(key: str, value: str) -> None:
    if key not in st.session_state:
        st.session_state[key] = value


def render() -> None:
    state = SessionState(DEFAULTS)

    header_left, header_right = st.columns([4, 1])
    with header_left:
        st.title("Mapping")
        st.caption("Step 2 of 3: Map source columns to target fields.")
    with header_right:
        if st.button("Reset", use_container_width=True):
            state.reset()
            st.rerun()

    def _notify(message: str) -> None:
        if hasattr(st, "toast"):
            st.toast(message)
        else:
            st.info(message)

    if not state.uploaded_bytes or not state.uploaded_name:
        st.info("Upload a file on the Upload page to start mapping.")
        return

    try:
        skiprows = parse_skiprows(state.skiprows)
        df = read_uploaded_dataframe(
            state.uploaded_bytes,
            state.uploaded_name,
            int(state.header_row),
            skiprows,
            state.delimiter,
            state.encoding,
            state.sheet_name,
            nrows=200,
        )
    except Exception as exc:
        st.error(f"Unable to parse file: {exc}")
        return

    synonyms = load_synonyms()
    targets = sorted(synonyms.keys())
    if not targets:
        st.warning("No target fields found in src/config.yaml.")
        return

    top_row = st.columns([1, 3])
    with top_row[0]:
        if st.button("Auto-Suggest", use_container_width=True):
            for col in df.columns:
                suggestion = _best_target(str(col), synonyms)
                key = f"map_{col}"
                if suggestion:
                    st.session_state[key] = suggestion
                    state.mappings[col] = suggestion
                else:
                    st.session_state[key] = "(unmapped)"
                    state.mappings[col] = None
            _notify("Auto-suggest applied.")
            st.rerun()

    with top_row[1]:
        st.caption("Click a column on the Upload page to highlight it here.")

    if df.empty:
        st.warning("Preview is empty. Adjust header row or delimiter settings.")
        return

    grid = st.columns(2, gap="large")
    for idx, col in enumerate(df.columns):
        target_key = f"map_{col}"
        current = state.mappings.get(col) or "(unmapped)"
        _ensure_selectbox_state(target_key, current)

        with grid[idx % 2]:
            st.markdown(f"**{col}**")
            if state.selected_column == col:
                st.success("Selected from preview")

            dtype_label = _infer_type(df[col])
            st.caption(f"Detected type: {dtype_label}")

            selected = st.selectbox(
                "Target field",
                options=["(unmapped)"] + targets,
                key=target_key,
                label_visibility="collapsed",
            )

            state.mappings[col] = None if selected == "(unmapped)" else selected
            st.divider()
