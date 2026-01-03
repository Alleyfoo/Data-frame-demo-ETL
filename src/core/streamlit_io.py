"""Streamlit-specific I/O helpers for uploaded files."""

from __future__ import annotations

import io

import pandas as pd
import streamlit as st


@st.cache_data(show_spinner=False)
def list_excel_sheets(data: bytes) -> list[str]:
    with pd.ExcelFile(io.BytesIO(data)) as xf:
        return list(xf.sheet_names)


@st.cache_data(show_spinner=False)
def read_uploaded_dataframe(
    data: bytes,
    filename: str,
    header_row: int | None,
    skiprows: list[int],
    delimiter: str,
    encoding: str,
    sheet_name: str | int | None,
    nrows: int = 200,
) -> pd.DataFrame:
    """Read a preview DataFrame from uploaded bytes."""
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


__all__ = ["list_excel_sheets", "read_uploaded_dataframe"]
