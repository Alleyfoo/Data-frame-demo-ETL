"""Target schema configuration and helper utilities."""

from __future__ import annotations

import difflib
import json
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Tuple

import pandas as pd
import yaml

from .services.header_detection import guess_header_row as _guess_header_row

# Default synonyms if no config is found
TARGET_SCHEMA: Dict[str, List[str]] = {
    "provider_id": ["provider", "vendor", "supplier", "source", "partner"],
    "article_sku": ["sku", "item", "material", "product"],
    "report_date": ["date", "period", "month", "time", "year"],
    "sales_qty": ["qty", "quantity", "units", "volume"],
    "sales_amount": ["amount", "total", "revenue", "sales", "net", "gross"],
    "order_id": ["order", "po number", "reference"],
    "region": ["region", "area", "location"],
    "unit_price": ["unit_price", "price", "unit cost", "rate"],
}

# Where to look for config.yaml (first match wins)
CONFIG_CANDIDATES: Tuple[Path, ...] = (
    Path("config.yaml"),
    Path(__file__).with_name("config.yaml"),
    Path(__file__).parent / "config.yaml",
)

# Canonical location for user-provided schema definitions to keep data/output tidy
SCHEMA_DIR = Path("data") / "schemas"
SCHEMA_FILE = "schema.json"
DEFAULT_SCHEMA_CANDIDATES: Tuple[Path, ...] = (
    SCHEMA_DIR / SCHEMA_FILE,
    Path(SCHEMA_FILE),
)


# --- Config helpers ----------------------------------------------------


def resolve_config_path(path: Path | None = None) -> Path:
    """Return the config path to use, preferring an existing file."""
    if path:
        return Path(path)

    for candidate in CONFIG_CANDIDATES:
        if candidate.exists():
            return candidate

    # Default to the first candidate even if missing so callers can create it
    return CONFIG_CANDIDATES[0]


def user_override_path(config_path: Path) -> Path:
    """Location for user-learned overrides that should not overwrite the base file."""
    return config_path.with_name(f"{config_path.stem}.user{config_path.suffix}")


def _read_yaml(path: Path) -> Dict[str, object]:
    if not path.exists():
        return {}
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (yaml.YAMLError, OSError):
        return {}
    return data if isinstance(data, dict) else {}


def _normalize_synonyms(
    synonyms: MutableMapping[str, Iterable[str]],
) -> Dict[str, List[str]]:
    normalized: Dict[str, List[str]] = {}
    for key, values in synonyms.items():
        if isinstance(values, Iterable) and not isinstance(values, (str, bytes)):
            normalized[str(key)] = [str(item) for item in values if item is not None]
    return normalized


def _merge_synonym_maps(
    base: Mapping[str, Iterable[str]], new_items: Mapping[str, Iterable[str]]
) -> Dict[str, List[str]]:
    merged: Dict[str, List[str]] = {
        str(key): [str(v) for v in values] for key, values in base.items()
    }
    for key, values in new_items.items():
        canon = str(key)
        existing = merged.setdefault(canon, [])
        seen = {val.lower() for val in existing}
        for value in values:
            val_str = str(value)
            if val_str.lower() not in seen:
                existing.append(val_str)
                seen.add(val_str.lower())
    return merged


def _merge_configs(base: Dict[str, object], override: Dict[str, object]) -> Dict[str, object]:
    """Merge base config with user overrides; synonyms lists are merged."""
    if not override:
        return base

    merged: Dict[str, object] = dict(base)

    if "synonyms" in base or "synonyms" in override:
        base_syn = (
            _normalize_synonyms(base.get("synonyms", {}))  # type: ignore[arg-type]
            if isinstance(base, Mapping)
            else {}
        )
        override_syn = (
            _normalize_synonyms(override.get("synonyms", {}))  # type: ignore[arg-type]
            if isinstance(override, Mapping)
            else {}
        )
        merged["synonyms"] = _merge_synonym_maps(base_syn, override_syn)

    for key, value in override.items():
        if key == "synonyms":
            continue
        merged[key] = value

    return merged


def load_master_config(path: Path | None = None) -> Dict[str, object]:
    """Load config.yaml with an optional <name>.user.yaml overlay."""
    base_path = resolve_config_path(path)
    base_cfg = _read_yaml(base_path)
    user_cfg = _read_yaml(user_override_path(base_path))
    return _merge_configs(base_cfg, user_cfg)


def load_target_schema(
    path: Path | None = None, master_config_path: Path | None = None
) -> Dict[str, List[str]]:
    """
    Load synonyms from config.yaml, falling back to a schema file or defaults.

    Priority:
    1. Caller-provided path
    2. data/schemas/schema.json or ./schema.json
    3. config.yaml synonyms
    4. Built-in TARGET_SCHEMA
    """
    if not SCHEMA_DIR.exists():
        SCHEMA_DIR.mkdir(parents=True, exist_ok=True)

    # 1/2: explicit path or schema files on disk
    schema_candidates: list[Path] = []
    if path:
        schema_candidates.append(Path(path))
    else:
        schema_candidates.extend([c for c in DEFAULT_SCHEMA_CANDIDATES if c.exists()])

    for candidate in schema_candidates:
        if not candidate.exists():
            continue
        try:
            data = json.loads(candidate.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if isinstance(data, Mapping):
            normalized: Dict[str, List[str]] = {}
            for key, synonyms in data.items():
                if isinstance(synonyms, Iterable) and not isinstance(
                    synonyms, (str, bytes)
                ):
                    normalized[str(key)] = [str(item) for item in synonyms]
            if normalized:
                return normalized

    # 3: config.yaml synonyms
    master_config = load_master_config(master_config_path)
    if isinstance(master_config, Mapping) and "synonyms" in master_config:
        synonyms_cfg = master_config["synonyms"]
        if isinstance(synonyms_cfg, MutableMapping):
            normalized_cfg = _normalize_synonyms(synonyms_cfg)
            if normalized_cfg:
                return normalized_cfg

    # 4: fallback to built-ins
    return dict(TARGET_SCHEMA)


def learn_synonyms_from_mapping(
    mapping: Mapping[str, str], master_config_path: Path | None = None
) -> Tuple[int, Path]:
    """Persist unseen header names into <config>.user.yaml for future auto-mapping."""
    if not mapping:
        path = user_override_path(resolve_config_path(master_config_path))
        return 0, path

    base_path = resolve_config_path(master_config_path)
    user_path = user_override_path(base_path)

    combined_config = load_master_config(base_path)
    known_synonyms = _normalize_synonyms(
        combined_config.get("synonyms", {})  # type: ignore[arg-type]
        if isinstance(combined_config, Mapping)
        else {}
    )

    additions: Dict[str, List[str]] = {}
    for source_header, target_field in mapping.items():
        canonical = str(target_field).strip()
        header_text = str(source_header).strip()
        if not canonical or not header_text:
            continue
        current = [s.lower() for s in known_synonyms.get(canonical, [])]
        if header_text.lower() not in current:
            additions.setdefault(canonical, []).append(header_text)
            known_synonyms.setdefault(canonical, []).append(header_text)

    if not additions:
        return 0, user_path

    user_cfg = _read_yaml(user_path)
    if not isinstance(user_cfg, dict):
        user_cfg = {}

    existing_user_syn = _normalize_synonyms(user_cfg.get("synonyms", {}))  # type: ignore[arg-type]
    merged = _merge_synonym_maps(existing_user_syn, additions)
    user_cfg["synonyms"] = merged

    user_path.parent.mkdir(parents=True, exist_ok=True)
    user_path.write_text(
        yaml.safe_dump(user_cfg, sort_keys=False, allow_unicode=False), encoding="utf-8"
    )
    added_count = sum(len(vals) for vals in additions.values())
    return added_count, user_path


# --- Mapping helpers ---------------------------------------------------


def guess_header_row(df_preview: pd.DataFrame) -> int:
    """Heuristically guess the header row index from an unlabelled preview."""
    return _guess_header_row(df_preview)


def snake_case(text: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in text)
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_").lower()


def auto_map_columns(
    file_headers: List[str], target_schema: Mapping[str, List[str]]
) -> Dict[str, str]:
    """Return a best-effort mapping from file headers to canonical fields."""
    mapping: Dict[str, str] = {}
    used_targets: set[str] = set()

    for header in file_headers:
        header_lower = header.lower().strip()
        best_match = None
        for target_field, synonyms in target_schema.items():
            if target_field in used_targets:
                continue
            pool = [target_field] + list(synonyms)
            for candidate in pool:
                candidate_lower = candidate.lower()
                if candidate_lower and candidate_lower in header_lower:
                    best_match = target_field
                    break
            if best_match:
                break
            matches = difflib.get_close_matches(header_lower, pool, n=1, cutoff=0.82)
            if matches:
                best_match = target_field
        if best_match:
            mapping[header] = best_match
            used_targets.add(best_match)
        elif header_lower not in mapping:
            mapping[header] = snake_case(header)
    return mapping


def describe_schema(target_schema: Mapping[str, List[str]]) -> str:
    pairs = [f"{key}: {', '.join(vals)}" for key, vals in target_schema.items()]
    return "\n".join(pairs)
