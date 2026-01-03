from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.combine_runner import concat_frames, merge_frames
from src.core.state import SessionState


DEFAULTS = {
    "combine_mode": "concat",
    "combine_keys": "",
    "combine_how": "inner",
    "strict_schema": False,
    "selected_files": [],
}


def _list_output_files(base_dir: Path) -> list[Path]:
    patterns = ["*.xlsx", "*.xls", "*.parquet"]
    files: list[Path] = []
    for pattern in patterns:
        files.extend(sorted(base_dir.glob(pattern)))
    return files


def render() -> None:
    state = SessionState(DEFAULTS)

    header_left, header_right = st.columns([4, 1])
    with header_left:
        st.title("Combine & Export")
        st.caption("Combine multiple output files using concat or merge.")
    with header_right:
        if st.button("Reset", use_container_width=True):
            state.reset()
            st.rerun()

    output_dir = Path("data/output")
    files = _list_output_files(output_dir)
    if not files:
        st.info("No output files found in data/output.")
        return

    file_labels = {f"{f.name}": f for f in files}
    state.selected_files = st.multiselect(
        "Select files to combine",
        options=list(file_labels.keys()),
        default=state.selected_files,
    )

    if not state.selected_files:
        st.info("Select at least one file to begin.")
        return

    selected_paths = [file_labels[name] for name in state.selected_files]

    state.combine_mode = st.selectbox(
        "Combine mode",
        options=["concat", "merge"],
        index=["concat", "merge"].index(state.combine_mode),
    )
    state.strict_schema = st.checkbox(
        "Strict schema (concat only)",
        value=state.strict_schema,
    )
    state.combine_keys = st.text_input(
        "Merge keys (comma-separated)",
        value=state.combine_keys,
    )
    state.combine_how = st.selectbox(
        "Merge type",
        options=["inner", "left", "right", "outer"],
        index=["inner", "left", "right", "outer"].index(state.combine_how),
    )

    if st.button("Run Combine", use_container_width=True):
        try:
            if state.combine_mode == "concat":
                combined = concat_frames(selected_paths, strict_schema=state.strict_schema)
            else:
                keys = [k.strip() for k in state.combine_keys.split(",") if k.strip()]
                combined = merge_frames(selected_paths, keys=keys, how=state.combine_how)
        except Exception as exc:
            st.error(f"Combine failed: {exc}")
            return

        st.dataframe(combined, use_container_width=True)
        csv_bytes = combined.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv_bytes,
            file_name="combined_output.csv",
            mime="text/csv",
        )
