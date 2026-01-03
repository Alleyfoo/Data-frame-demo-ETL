"""File handler wrapper for template load/save operations."""

from __future__ import annotations

from pathlib import Path

from ..templates import Template, load_template as _load_template, save_template as _save_template


def load_template(path: Path) -> Template:
    """Load a Template from a JSON/YAML file."""
    return _load_template(path)


def save_template(template: Template, path: Path) -> None:
    """Save a Template to a JSON/YAML file."""
    _save_template(template, path)
