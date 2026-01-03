from __future__ import annotations

from pathlib import Path

import importlib.util
import streamlit as st
import sys

REPO_ROOT = Path(__file__).resolve().parent
SRC_PATH = REPO_ROOT / "src"
PAGES_DIR = REPO_ROOT / "webapp" / "pages"


def _load_page(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load page module: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    repo_root = str(REPO_ROOT.resolve())
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    if str(SRC_PATH) not in sys.path:
        sys.path.insert(0, str(SRC_PATH))

    st.set_page_config(
        page_title="Data Frame Tool",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.sidebar.title("Navigation")

    pages = {
        "Dashboard": PAGES_DIR / "00_Dashboard.py",
        "Upload": PAGES_DIR / "01_Upload.py",
        "Mapping": PAGES_DIR / "02_Mapping.py",
        "Query Builder": PAGES_DIR / "04_Query_Builder.py",
        "Diagnostics": PAGES_DIR / "05_Diagnostics.py",
        "Template Library": PAGES_DIR / "06_Template_Library.py",
        "Combine & Export": PAGES_DIR / "07_Combine.py",
        "YouTube Demo": PAGES_DIR / "08_YouTube_Demo.py",
    }
    available = {name: path for name, path in pages.items() if path.exists()}

    if not available:
        st.info("No Streamlit pages found yet.")
        return

    selection = st.sidebar.radio("Page", list(available.keys()))
    page = _load_page(available[selection])

    if hasattr(page, "render"):
        page.render()
    elif hasattr(page, "main"):
        page.main()
    else:
        st.error(f"Page {available[selection].name} is missing a render() function.")


if __name__ == "__main__":
    main()
