import json
from pathlib import Path

import pandas as pd

from src.exporter import export_dataset


def test_exporter_writes_files_and_manifest(tmp_path: Path):
    df = pd.DataFrame({"a": [1, 2, None], "b": ["x", None, "z"]})
    meta = {
        "run_id": "abc",
        "estimated_quota_units": 2,
        "endpoint_call_counts": {"videos": 1},
    }
    paths = export_dataset(df, tmp_path, formats=["xlsx", "jsonl"], meta=meta)

    assert (tmp_path / "data.xlsx").exists()
    assert (tmp_path / "data.jsonl").exists()
    manifest_path = paths["manifest"]
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "metrics" in manifest
    assert manifest["metrics"]["rows"] == 3
    assert manifest["metrics"]["columns"] == 2
    assert manifest["estimated_quota_units"] == 2
    assert manifest["endpoint_call_counts"] == {"videos": 1}


def test_exporter_handles_parquet_and_nulls(tmp_path: Path):
    df = pd.DataFrame({"count": [1, 2, None], "label": ["a", "b", "c"]})
    paths = export_dataset(df, tmp_path, formats=["parquet"], meta={})
    parquet_path = tmp_path / "data.parquet"
    assert parquet_path.exists()
    manifest = json.loads(paths["manifest"].read_text(encoding="utf-8"))
    assert manifest["metrics"]["dtypes"]["count"]
    # null pct should be present and numeric
    assert "count" in manifest["metrics"]["null_pct"]
    assert isinstance(manifest["metrics"]["null_pct"]["count"], (int, float))
