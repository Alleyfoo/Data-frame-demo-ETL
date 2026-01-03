from __future__ import annotations

import streamlit as st

from src.core.state import SessionState
from src.core.streamlit_io import list_excel_sheets, read_uploaded_dataframe
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
    "metadata_cells": [],
    "meta_row_idx": None,
    "meta_col_idx": None,
    "meta_target": "",
    "meta_type": "metadata",
}


def _render_dataframe_selection(df):
    selection = None
    try:
        selection = st.dataframe(
            df,
            use_container_width=True,
            selection_mode="single-column",
            on_select="rerun",
        )
    except TypeError:
        st.dataframe(df, use_container_width=True)
    return selection


def _extract_selected_column(selection) -> str | None:
    if selection is None:
        return None
    if hasattr(selection, "selection"):
        columns = selection.selection.get("columns", [])
        return columns[0] if columns else None
    if isinstance(selection, dict):
        columns = selection.get("columns") or selection.get("selection", {}).get("columns")
        if columns:
            return columns[0]
    return None


def render() -> None:
    state = SessionState(DEFAULTS)

    header_left, header_right = st.columns([4, 1])
    with header_left:
        st.title("Upload & Preview")
        st.caption("Step 1 of 3: Upload a file and preview columns.")
    with header_right:
        if st.button("Reset", use_container_width=True):
            state.reset()
            st.rerun()

    uploaded = st.file_uploader(
        "Upload a CSV or Excel file",
        type=["csv", "xlsx", "xls"],
    )

    if uploaded is None:
        st.info("Upload a file to begin.")
        return

    if uploaded.name != state.uploaded_name:
        state.uploaded_name = uploaded.name
        state.uploaded_bytes = uploaded.getvalue()
        state.sheet_name = None
        state.selected_column = None
        state.mappings = {}
        if hasattr(st, "toast"):
            st.toast("File uploaded.")
        else:
            st.info("File uploaded.")

    if not state.uploaded_bytes:
        st.warning("Uploaded file is empty.")
        return

    is_excel = uploaded.name.lower().endswith((".xlsx", ".xls"))
    left, right = st.columns([3, 2], gap="large")

    with right:
        st.subheader("Settings")
        header_row = st.number_input(
            "Header row (0-indexed)",
            min_value=0,
            value=int(state.header_row),
        )
        state.header_row = int(header_row)

        skiprows_text = st.text_input(
            "Skip rows (comma-separated)",
            value=state.skiprows,
        )
        state.skiprows = skiprows_text

        if is_excel:
            sheets = list_excel_sheets(state.uploaded_bytes)
            if not sheets:
                sheets = ["Sheet1"]
            if state.sheet_name not in sheets:
                state.sheet_name = sheets[0]
            state.sheet_name = st.selectbox("Sheet", sheets, index=sheets.index(state.sheet_name))
        else:
            delimiter = st.text_input("Delimiter", value=state.delimiter)
            encoding = st.text_input("Encoding", value=state.encoding)
            state.delimiter = delimiter
            state.encoding = encoding
            state.sheet_name = None

    with left:
        st.subheader("Preview")
        progress = st.progress(0)
        read_ok = False
        try:
            progress.progress(20)
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
            progress.progress(80)
            read_ok = True
        except Exception as exc:
            progress.empty()
            st.error(f"Unable to parse file: {exc}")
            return
        finally:
            if read_ok:
                progress.progress(100)
                progress.empty()

        tab_data, tab_meta = st.tabs(
            ["Table Columns (Data)", "Metadata Cells (Titles/Dates)"]
        )

        with tab_data:
            selection = _render_dataframe_selection(df)
            selected = _extract_selected_column(selection)

            if selected:
                state.selected_column = selected
            elif state.selected_column not in df.columns:
                state.selected_column = None

            if state.selected_column:
                st.success(f"Selected column: {state.selected_column}")
            else:
                fallback = st.selectbox(
                    "Select a column to map",
                    options=["(none)"] + list(df.columns),
                )
                state.selected_column = None if fallback == "(none)" else fallback

        with tab_meta:
            st.subheader("Metadata Cells")
            st.caption("Select a cell to capture titles, dates, or other metadata.")

            selected_row_idx = None
            selected_col_idx = None
            try:
                raw_df = read_uploaded_dataframe(
                    state.uploaded_bytes,
                    state.uploaded_name,
                    header_row=None,
                    skiprows=[],
                    delimiter=state.delimiter,
                    encoding=state.encoding,
                    sheet_name=state.sheet_name,
                    nrows=200,
                )
                display_df = raw_df.copy()
                display_df.columns = [f"Col {idx}" for idx in range(len(raw_df.columns))]
                meta_event = st.dataframe(
                    display_df,
                    use_container_width=True,
                    on_select="rerun",
                    selection_mode=["single-row", "single-column"],
                    height=240,
                )
                if meta_event.selection.rows:
                    selected_row_idx = int(meta_event.selection.rows[0])
                if meta_event.selection.columns:
                    col_name = meta_event.selection.columns[0]
                    if col_name in display_df.columns:
                        selected_col_idx = int(display_df.columns.get_loc(col_name))
            except TypeError:
                st.caption("Selection API not available; using dropdowns.")
                raw_df = read_uploaded_dataframe(
                    state.uploaded_bytes,
                    state.uploaded_name,
                    header_row=None,
                    skiprows=[],
                    delimiter=state.delimiter,
                    encoding=state.encoding,
                    sheet_name=state.sheet_name,
                    nrows=200,
                )
                selected_row_idx = st.selectbox(
                    "Row", options=list(range(len(raw_df))), index=0 if len(raw_df) else 0
                )
                selected_col_idx = st.selectbox(
                    "Column",
                    options=list(range(len(raw_df.columns))),
                    format_func=lambda idx: f"Col {idx}",
                    index=0 if len(raw_df.columns) else 0,
                )

            cell_value = None
            if selected_row_idx is not None and selected_col_idx is not None:
                state.meta_row_idx = selected_row_idx
                state.meta_col_idx = selected_col_idx
                col_name = f"Col {selected_col_idx}"
                cell_value = raw_df.iat[selected_row_idx, selected_col_idx]
                st.write(
                    f"Selected cell: Row {selected_row_idx}, Column {col_name} -> `{cell_value}`"
                )
            else:
                st.info("Select a row and column to capture metadata.")

            state.meta_target = st.text_input(
                "Metadata target name", value=state.meta_target
            )
            state.meta_type = st.selectbox(
                "Metadata type",
                options=["metadata", "title", "date", "header_def"],
                index=["metadata", "title", "date", "header_def"].index(state.meta_type),
            )

            if cell_value is None and state.meta_row_idx is not None and state.meta_col_idx is not None:
                try:
                    cell_value = raw_df.iat[int(state.meta_row_idx), int(state.meta_col_idx)]
                except Exception:
                    cell_value = None

            add_disabled = (
                state.meta_row_idx is None
                or state.meta_col_idx is None
                or not state.meta_target.strip()
            )
            if st.button("Add Metadata Field", disabled=add_disabled):
                entry = {
                    "row": int(state.meta_row_idx),
                    "col": int(state.meta_col_idx),
                    "value": "" if cell_value is None else str(cell_value),
                    "target": state.meta_target.strip(),
                    "metadata_type": state.meta_type,
                }
                state.metadata_cells = state.metadata_cells + [entry]
                state.meta_target = ""
                st.toast("Metadata field added.") if hasattr(st, "toast") else st.success(
                    "Metadata field added."
                )

            if state.metadata_cells:
                st.dataframe(state.metadata_cells, use_container_width=True)
                if st.button("Clear Metadata Fields"):
                    state.metadata_cells = []

        st.caption(f"{len(df)} rows x {len(df.columns)} columns (preview)")
        if df.empty:
            st.warning("Preview is empty. Adjust header row or delimiter settings.")
