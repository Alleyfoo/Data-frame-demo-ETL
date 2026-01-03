from __future__ import annotations

import json
from pathlib import Path

import streamlit as st

from src.api.v1.engine import DataEngine
from src.core.state import SessionState
from src.templates import load_template


DEFAULTS = {
    "selected_template": None,
    "input_dir": "data/input",
    "output_dir": "data/output",
    "last_message": "",
    "combine_mode": "concat",
    "combine_keys": "",
    "combine_how": "inner",
    "combine_strict": False,
}


def _find_templates() -> list[Path]:
    roots = [Path("data"), Path("data/schemas"), Path("data/input")]
    templates: list[Path] = []
    for root in roots:
        if root.exists():
            templates.extend(sorted(root.glob("*.df-template.json")))
    return templates


def _template_card(name: str, description: str, selected: bool) -> None:
    style = "✅" if selected else "🗂️"
    st.markdown(f"{style} **{name}**")
    st.caption(description)


def render() -> None:
    state = SessionState(DEFAULTS)
    engine = DataEngine()

    header_left, header_right = st.columns([4, 1])
    with header_left:
        st.title("Template Library")
        st.caption("Browse, inspect, and batch-run templates without writing code.")
    with header_right:
        if st.button("Reset", use_container_width=True):
            state.reset()
            st.rerun()

    templates = _find_templates()
    if not templates:
        st.info("No templates found. Save a .df-template.json to data/ or data/schemas.")
        return

    grid = st.columns(3, gap="large")
    for idx, path in enumerate(templates):
        with grid[idx % 3]:
            selected = str(path) == state.selected_template
            _template_card(path.name, str(path.parent), selected)
            if st.button("Select", key=f"select_{path.name}"):
                state.selected_template = str(path)
                st.rerun()

    if not state.selected_template:
        st.info("Select a template to inspect details.")
        return

    selected_path = Path(state.selected_template)
    st.divider()
    st.subheader(f"Template: {selected_path.name}")

    try:
        template = load_template(selected_path)
    except Exception as exc:
        st.error(f"Failed to load template: {exc}")
        return

    with st.expander("View JSON", expanded=True):
        st.code(json.dumps(template.to_dict(), indent=2), language="json")

    action_left, action_right = st.columns([2, 1])
    with action_left:
        state.input_dir = st.text_input("Input directory", value=state.input_dir)
        state.output_dir = st.text_input("Output directory", value=state.output_dir)
    with action_right:
        if st.button("Duplicate Template", use_container_width=True):
            duplicate_path = selected_path.with_name(f"{selected_path.stem}_copy.json")
            duplicate_path.write_text(
                json.dumps(template.to_dict(), indent=2), encoding="utf-8"
            )
            st.success(f"Duplicated to {duplicate_path.name}")
        if st.button("Delete Template", use_container_width=True):
            selected_path.unlink(missing_ok=True)
            state.selected_template = None
            st.warning("Template deleted.")
            st.rerun()

    st.subheader("Batch Operations")
    state.combine_mode = st.selectbox(
        "Combine mode",
        options=["concat", "merge"],
        index=["concat", "merge"].index(state.combine_mode),
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
    state.combine_strict = st.checkbox(
        "Strict schema (concat only)",
        value=state.combine_strict,
    )

    if st.button("Combine All Outputs", use_container_width=True):
        try:
            keys = [k.strip() for k in state.combine_keys.split(",") if k.strip()]
            combined = engine.run_combine(
                input_dir=Path(state.output_dir),
                pattern="*.xlsx",
                mode=state.combine_mode,
                keys=keys,
                how=state.combine_how,
                strict_schema=state.combine_strict,
            )
            out_path = Path(state.output_dir) / "Master_Combined_Output.xlsx"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            combined.to_excel(out_path, index=False)
            st.success(f"Combined output saved to {out_path}")
        except Exception as exc:
            st.error(f"Combine failed: {exc}")

    if st.button("Process Input Directory", use_container_width=True):
        input_dir = Path(state.input_dir)
        output_dir = Path(state.output_dir)
        files = sorted(list(input_dir.glob("*.xlsx")) + list(input_dir.glob("*.csv")))
        if not files:
            st.warning("No input files found.")
            return
        output_dir.mkdir(parents=True, exist_ok=True)

        results = []
        for file_path in files:
            result, df = engine.run_full_process(
                source_path=file_path,
                template=template,
                output_path=output_dir / f"{file_path.stem}_clean.xlsx",
            )
            if result.success and df is not None:
                out_path = output_dir / f"{file_path.stem}_clean.xlsx"
                df.to_excel(out_path, index=False)
                results.append((file_path.name, "ok"))
            else:
                results.append((file_path.name, "failed"))

        st.success("Batch processing complete.")
        st.dataframe(results, use_container_width=True)
