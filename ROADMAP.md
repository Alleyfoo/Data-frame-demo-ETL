# Streamlit UI Modernization Roadmap

Status legend: pending, in-progress, done, blocked.

## Executive Summary
Refactor the monolithic schema builder UI into a modular Streamlit app that
reuses the existing backend (src/pipeline.py, src/templates.py). The plan
emphasizes a visual, multi-page workflow without duplicating ETL logic.

## Phase 1: Foundation & Architecture Setup
Goal: Separate UI from logic and establish a stable entry point.

Status: done

Actions:
- done: Create streamlit/pages/ for multipage UI.
- done: Create src/core/ for shared UI utilities.
- done: Add requirements-streamlit.txt with Streamlit + data deps.
- done: Create root app.py with Streamlit routing.
- done: Create streamlit/.streamlit/ for theme config.
- done: Add streamlit/.streamlit/config.toml baseline config.

Notes:
- app.py strips repo root from sys.path to avoid local streamlit/ shadowing the
  Streamlit package.

## Phase 2: MVP - Visual Schema Builder
Goal: Replicate the schema builder workflow with a modern UI.

Status: done

Actions:
- done: Build pages/01_Upload.py with interactive preview + selection state.
- done: Build pages/02_Mapping.py with card layout + target mapping dropdowns.
- done: Add fuzzy auto-suggest for target fields.

## Phase 3: Polish & UX
Goal: Make the UI feel guided and resilient.

Status: done

Actions:
- done: Extend src/core/state.py with reset flows used by pages.
- done: Add toasts, warnings, and progress indicators.
- done: Add step indicators and helper captions.

## Phase 4A: Headless Backend Refactor
Goal: Decouple processing logic from UI for API/CLI reuse.

Status: done

Actions:
- done: Create src/api/v1 scaffolding with Pydantic schemas.
- done: Move transform/validate logic into src/api/v1/engine.py.
- done: Update src/pipeline.py to call the engine.
- done: Add unit tests for engine behaviors.

## Phase 4B: Query Builder UI
Goal: Build a "Lansa-style" query builder in Streamlit.

Status: done

Actions:
- done: Create streamlit/pages/04_Query_Builder.py.
- done: Add query canvas and operator palette interactions.

## Phase 4C: Diagnostics & Code Generation
Goal: Provide validation visibility and CLI command generation.

Status: done

Actions:
- done: Create streamlit/pages/05_Diagnostics.py.
- done: Add CLI command generator for current settings.

## Phase 4D: Multi-Source & Collaboration
Goal: Allow multi-source preview and joining.

Status: done

Actions:
- done: Add Combine & Export Streamlit page using combine_runner.

## Phase 5: Advanced Features - Query Tool
Goal: Optional menu-driven preview/query capabilities.

Status: done

Actions:
- done: Implement Query Builder page with source selection and SQL preview.

## Phase 6: Power User Features - Template Library
Goal: Manage and batch-run templates without code.

Status: done

Actions:
- done: Implement Template Library page with gallery and batch processing.

## Open Issues
- Metadata cell selection in the upload preview is a placeholder tab.
