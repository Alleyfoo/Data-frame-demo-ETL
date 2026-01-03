# Streamlit UI Modernization Roadmap

Status legend: pending, in-progress, done, blocked.

## Executive Summary
Refactor the UI into a modular Streamlit app that reuses the backend (src/pipeline.py, src/templates.py) while keeping a clean entrypoint and CLI parity.

## Phase 1: Foundation & Architecture Setup
Status: done
Actions:
- done: Create webapp/pages/ for multipage UI.
- done: Create src/core/ for shared UI utilities.
- done: Consolidate dependencies into requirements.txt (includes Streamlit + data deps).
- done: Create root app.py with Streamlit routing (no sys.path hacks).
- done: Move Streamlit config to .streamlit/config.toml at repo root.

## Phase 2: MVP - Visual Schema Builder
Status: done
Actions:
- done: Build pages/01_Upload.py with interactive preview + selection state.
- done: Build pages/02_Mapping.py with card layout + target mapping dropdowns.
- done: Add fuzzy auto-suggest for target fields.

## Phase 3: Polish & UX
Status: done
Actions:
- done: Extend src/core/state.py with reset flows used by pages.
- done: Add toasts, warnings, and progress indicators.
- done: Add step indicators and helper captions.

## Phase 4A: Headless Backend Refactor
Status: done
Actions:
- done: Create src/api/v1 scaffolding with Pydantic schemas.
- done: Move transform/validate logic into src/api/v1/engine.py.
- done: Update src/pipeline.py to call the engine.
- done: Add unit tests for engine behaviors.

## Phase 4B: Query Builder UI
Status: done
Actions:
- done: Create webapp/pages/04_Query_Builder.py.
- done: Add query canvas and operator palette interactions.

## Phase 4C: Diagnostics & Code Generation
Status: done
Actions:
- done: Create webapp/pages/05_Diagnostics.py.
- done: Add CLI command generator for current settings.

## Phase 4D: Multi-Source & Collaboration
Status: done
Actions:
- done: Add Combine & Export Streamlit page using combine_runner.

## Open Issues
- Metadata cell selection in the upload preview is a placeholder tab.
