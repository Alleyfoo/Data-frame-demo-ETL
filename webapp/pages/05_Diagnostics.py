from __future__ import annotations

import pandas as pd
import streamlit as st

from src.api.v1 import engine
from src.core.state import SessionState
from src.core.streamlit_io import read_uploaded_dataframe
from src.templates import Template, parse_skiprows


DEFAULTS = {
    "uploaded_name": None,
    "uploaded_bytes": None,
    "header_row": 0,
    "skiprows": "",
    "delimiter": ",",
    "encoding": "utf-8",
    "sheet_name": None,
    "mappings": {},
    "validation_level": "coerce",
    "output_fmt": "xlsx",
    "target_dir": "data/input",
    "use_streamlit_templates": True,
}


def _build_template(df: pd.DataFrame, mappings: dict) -> Template:
    column_mappings = {
        str(source): str(target)
        for source, target in mappings.items()
        if target not in (None, "", "(unmapped)")
    }
    return Template(
        sheet="Sheet1",
        header_row=0,
        columns=[str(col) for col in df.columns],
        column_mappings=column_mappings,
    )


def _generate_cli(
    target_dir: str, validation_level: str, output_fmt: str, use_streamlit_templates: bool
) -> str:
    cmd = (
        f'python -m src.cli run --target-dir "{target_dir}" '
        f'--validation-level "{validation_level}" --output-fmt "{output_fmt}"'
    )
    if use_streamlit_templates:
        cmd = f"{cmd} --use-streamlit-templates"
    return cmd


def render() -> None:
    state = SessionState(DEFAULTS)

    header_left, header_right = st.columns([4, 1])
    with header_left:
        st.title("Diagnostics")
        st.caption("Data quality checks, validation status, and CLI command export.")
    with header_right:
        if st.button("Reset", use_container_width=True):
            state.reset()
            st.rerun()

    if not state.uploaded_bytes or not state.uploaded_name:
        st.info("Upload a file on the Upload page to run diagnostics.")
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
            nrows=1000,
        )
    except Exception as exc:
        st.error(f"Unable to parse file: {exc}")
        return

    if df.empty:
        st.warning("Preview is empty. Adjust header row or delimiter settings.")
        return

    template = _build_template(df, state.mappings)

    row_count = len(df)
    col_count = len(df.columns)
    null_count = int(df.isna().sum().sum())

    metrics = st.columns(3)
    metrics[0].metric("Rows", f"{row_count}")
    metrics[1].metric("Columns", f"{col_count}")
    metrics[2].metric("Null Cells", f"{null_count}")

    validation_msg = "Not evaluated"
    try:
        engine.validate(df.copy(), template, validation_level=state.validation_level)
        validation_msg = "Pass"
    except Exception as exc:
        validation_msg = f"Fail: {type(exc).__name__}"

    st.metric("Validation", validation_msg)

    numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
    chart_col = st.selectbox(
        "Chart column",
        options=numeric_cols if numeric_cols else list(df.columns),
    )
    st.bar_chart(df[chart_col].value_counts().head(25))

    st.subheader("Code Generator")
    state.validation_level = st.selectbox(
        "Validation level",
        options=["off", "coerce", "contract"],
        index=["off", "coerce", "contract"].index(state.validation_level),
    )
    state.output_fmt = st.selectbox(
        "Output format",
        options=["xlsx", "parquet"],
        index=["xlsx", "parquet"].index(state.output_fmt),
    )
    state.target_dir = st.text_input("Target directory", value=state.target_dir)
    state.use_streamlit_templates = st.checkbox(
        "Use Streamlit templates only",
        value=state.use_streamlit_templates,
    )

    command = _generate_cli(
        state.target_dir, state.validation_level, state.output_fmt, state.use_streamlit_templates
    )
    st.text_area("Generated CLI Command", value=command, height=100)
    st.caption("Use the copy button in the text area to copy the command.")
