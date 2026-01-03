from __future__ import annotations

import pandas as pd
import streamlit as st

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
    "query_text": "",
    "selected_columns": [],
    "filters": [
        {"column": "", "operator": "=", "value": ""},
    ],
    "last_selected_column": None,
    "auto_sync": True,
}


def _build_sql(selected_cols: list[str], filters: list[dict]) -> str:
    select_clause = ", ".join(selected_cols) if selected_cols else "*"
    where_parts: list[str] = []
    for item in filters:
        col = str(item.get("column", "")).strip()
        op = str(item.get("operator", "")).strip() or "="
        val = str(item.get("value", "")).strip()
        if not col or not val:
            continue
        if op.lower() == "contains":
            where_parts.append(f"{col} LIKE '%{val}%'")
        else:
            where_parts.append(f"{col} {op} '{val}'")
    where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""
    return f"SELECT {select_clause} FROM data{where_clause};"


def _apply_filters(df: pd.DataFrame, filters: list[dict]) -> pd.DataFrame:
    filtered = df.copy()
    for item in filters:
        col = str(item.get("column", "")).strip()
        op = str(item.get("operator", "")).strip() or "="
        raw_val = str(item.get("value", "")).strip()
        if not col or col not in filtered.columns or raw_val == "":
            continue
        series = filtered[col]
        val: object = raw_val
        if pd.api.types.is_numeric_dtype(series):
            try:
                val = float(raw_val)
            except ValueError:
                continue
        if op == "=":
            filtered = filtered[series == val]
        elif op == "!=":
            filtered = filtered[series != val]
        elif op == ">":
            filtered = filtered[series > val]
        elif op == ">=":
            filtered = filtered[series >= val]
        elif op == "<":
            filtered = filtered[series < val]
        elif op == "<=":
            filtered = filtered[series <= val]
        elif op.lower() == "contains":
            filtered = filtered[series.astype(str).str.contains(str(val), na=False)]
    return filtered


def render() -> None:
    state = SessionState(DEFAULTS)

    header_left, header_right = st.columns([4, 1])
    with header_left:
        st.title("Query Builder")
        st.caption("Step 3 of 3: Build a query or filter preview data.")
    with header_right:
        if st.button("Reset", use_container_width=True):
            state.reset()
            st.rerun()

    if not state.uploaded_bytes or not state.uploaded_name:
        st.info("Upload a file on the Upload page to use the Query Builder.")
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
            nrows=500,
        )
    except Exception as exc:
        st.error(f"Unable to parse file: {exc}")
        return

    if df.empty:
        st.warning("Preview is empty. Adjust header row or delimiter settings.")
        return

    left, right = st.columns([3, 2], gap="large")

    with left:
        st.subheader("Source Preview")
        st.dataframe(df, use_container_width=True)

    with right:
        st.subheader("Source Canvas")

        column_df = pd.DataFrame({"column": list(df.columns)})
        selected_col = None
        try:
            st.data_editor(
                column_df,
                use_container_width=True,
                selection_mode="single-row",
                on_select="rerun",
                key="source_columns",
            )
            selection = st.session_state.get("source_columns", {}).get("selection", {})
            selected_rows = selection.get("rows", [])
            if selected_rows:
                selected_col = column_df.iloc[selected_rows[0]]["column"]
        except TypeError:
            selected_col = st.selectbox(
                "Select a column",
                options=list(df.columns),
                index=0,
            )

        if selected_col and selected_col != state.last_selected_column:
            state.last_selected_column = selected_col
            if selected_col not in state.selected_columns:
                state.selected_columns = state.selected_columns + [selected_col]

        st.subheader("Query Canvas")
        state.selected_columns = st.multiselect(
            "Select columns",
            options=list(df.columns),
            default=state.selected_columns,
        )

        filter_df = pd.DataFrame(state.filters)
        edited = st.data_editor(
            filter_df,
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "column": st.column_config.SelectboxColumn(
                    "Column",
                    options=[""] + list(df.columns),
                ),
                "operator": st.column_config.SelectboxColumn(
                    "Operator",
                    options=["=", "!=", ">", ">=", "<", "<=", "contains"],
                ),
            },
        )
        state.filters = edited.to_dict(orient="records")

        state.auto_sync = st.checkbox(
            "Auto-sync SQL with selections",
            value=state.auto_sync,
        )
        if state.auto_sync:
            state.query_text = _build_sql(state.selected_columns, state.filters)

        state.query_text = st.text_area(
            "Generated SQL",
            value=state.query_text,
            height=120,
        )
        st.caption("Copy the SQL text above into your query tool.")

        palette = st.columns(6)
        buttons = ["=", "!=", ">", "<", ">=", "<="]
        for idx, token in enumerate(buttons):
            if palette[idx].button(token, use_container_width=True):
                state.query_text = f"{state.query_text} {token}".strip()
                st.rerun()

        if st.button("Apply Filters Preview", use_container_width=True):
            preview = _apply_filters(df, state.filters)
            st.dataframe(preview, use_container_width=True)
