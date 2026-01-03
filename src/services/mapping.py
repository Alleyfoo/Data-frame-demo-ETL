"""Mapping- and schema-related helpers separated from the UI layer."""

from __future__ import annotations

from pathlib import Path
from typing import Mapping, MutableMapping, Tuple

from ..core import (
    TARGET_SCHEMA,
    auto_map_columns as _auto_map_columns,
    describe_schema as _describe_schema,
    learn_synonyms_from_mapping as _learn_synonyms_from_mapping,
    load_master_config,
    load_target_schema as _load_target_schema,
    snake_case as _snake_case,
    user_override_path,
)

# Thin wrappers allow the UI to import from a stable service module while keeping
# the implementations in core.py for backwards compatibility.


def load_target_schema(path: Path | None = None, master_config_path: Path | None = None):
    return _load_target_schema(path=path, master_config_path=master_config_path)


def auto_map_columns(file_headers, target_schema):
    return _auto_map_columns(file_headers, target_schema)


def describe_schema(target_schema):
    return _describe_schema(target_schema)


def learn_synonyms_from_mapping(mapping: Mapping[str, str], master_config_path: Path | None = None) -> Tuple[int, Path]:
    return _learn_synonyms_from_mapping(mapping, master_config_path=master_config_path)


def snake_case(text: str) -> str:
    return _snake_case(text)


__all__ = [
    "TARGET_SCHEMA",
    "auto_map_columns",
    "describe_schema",
    "learn_synonyms_from_mapping",
    "load_master_config",
    "load_target_schema",
    "snake_case",
    "user_override_path",
]
