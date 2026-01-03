from __future__ import annotations

import io

import pandas as pd
import streamlit as st

from src.core.state import SessionState
from src.templates import parse_skiprows


DEFAULTS = {
    "uploaded_name": None,
    "uploaded_bytes": None,
    "header_row": 0,
    "skiprows": "",
    "delimiter": ",",
    "encoding": "utf-8",
    "sheet_name": None,
}


@st.cache_data(show_spinner=False)
def _list_excel_sheets(data: bytes) -> list[str]:
    with pd.ExcelFile(io.BytesIO(data)) as xf:
        return list(xf.sheet_names)


@st.cache_data(show_spinner=False)
def _read_dataframe(
    data: bytes,
    filename: str,
    header_row: int,
    skiprows: list[int],
    delimiter: str,
    encoding: str,
    sheet_name: str | int | None,
    nrows: int,
) -> pd.DataFrame:
    if filename.lower().endswith((".xlsx", ".xls")):
        return pd.read_excel(
            io.BytesIO(data),
            sheet_name=sheet_name or 0,
            header=header_row,
            skiprows=skiprows,
            nrows=nrows,
        )
    return pd.read_csv(
        io.BytesIO(data),
        header=header_row,
        skiprows=skiprows,
        sep=delimiter,
        encoding=encoding,
        nrows=nrows,
    )


def render() -> None:
    state = SessionState(DEFAULTS)

    st.title("Schema Upload")
    st.caption("Upload a file and tune parsing settings while previewing the data.")

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
            sheets = _list_excel_sheets(state.uploaded_bytes)
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
        try:
            skiprows = parse_skiprows(state.skiprows)
            df = _read_dataframe(
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

        st.dataframe(df, use_container_width=True)
        st.caption(f"{len(df)} rows x {len(df.columns)} columns (preview)")
