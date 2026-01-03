from __future__ import annotations

import subprocess
import sys
from datetime import datetime
from pathlib import Path

import streamlit as st

from src.api.v1.engine import DataEngine
from src.templates import load_template, locate_template


def _latest_mtime(paths: list[Path]) -> float | None:
    if not paths:
        return None
    return max(paths, key=lambda p: p.stat().st_mtime).stat().st_mtime


def _format_time(timestamp: float | None) -> str:
    if timestamp is None:
        return "Never"
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")


def _scan_files(directory: Path, pattern: str = "*") -> list[Path]:
    if not directory.exists():
        return []
    return sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)


def _run_batch() -> tuple[bool, str]:
    cmd = [sys.executable, "-m", "src.cli", "run", "--target-dir", "data/input"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return True, "Batch run completed."
    return False, result.stderr or result.stdout or "Batch run failed."


def render() -> None:
    st.title("Dashboard")
    st.caption("Quick actions and system health at a glance.")

    output_files = _scan_files(Path("data/output"))
    archive_files = _scan_files(Path("data/archive"))
    quarantine_files = _scan_files(Path("data/quarantine"))

    last_output = _latest_mtime(output_files)
    last_archive = _latest_mtime(archive_files)

    metrics = st.columns(3)
    metrics[0].metric("Last Output Update", _format_time(last_output))
    metrics[1].metric("Last Archive Update", _format_time(last_archive))
    metrics[2].metric("Quarantine Files", str(len(quarantine_files)))

    st.subheader("Quick Actions")
    action_cols = st.columns(3)

    if action_cols[0].button("New Import", use_container_width=True):
        st.info("Go to Upload in the sidebar to start a new import.")

    if action_cols[1].button("Run Batch", use_container_width=True):
        ok, msg = _run_batch()
        if ok:
            st.success(msg)
        else:
            st.error(msg)

    if action_cols[2].button("View Templates", use_container_width=True):
        st.info("Open Template Library from the sidebar.")

    st.subheader("Recent Errors")
    if not quarantine_files:
        st.success("No recent errors in quarantine.")
        return

    engine = DataEngine()
    for path in quarantine_files[:3]:
        row = st.columns([3, 1])
        row[0].write(path.name)
        if row[1].button("Retry", key=f"retry_{path.name}"):
            try:
                try:
                    tpl_path = locate_template(Path("data/input"), stem=path.stem)
                except FileNotFoundError:
                    tpl_path = locate_template(Path("data/schemas"), stem=path.stem)
                template = load_template(tpl_path)
                output_path = Path("data/output") / f"{path.stem}_clean.xlsx"
                result, df = engine.run_full_process(path, template, output_path)
                if result.success and df is not None:
                    output_path.parent.mkdir(parents=True, exist_ok=True)
                    df.to_excel(output_path, index=False)
                    archive_path = Path("data/archive") / path.name
                    archive_path.parent.mkdir(parents=True, exist_ok=True)
                    path.replace(archive_path)
                    st.success(f"Retried {path.name} successfully.")
                else:
                    st.warning(f"Retry failed for {path.name}.")
            except Exception as exc:
                st.error(f"Retry error: {exc}")
