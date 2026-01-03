from pathlib import Path
import json

from src.templates import Template


def test_template_roundtrip(tmp_path: Path):
    payload = {
        "sheet": "Sheet1",
        "header_row": 2,
        "columns": ["A", "B"],
        "column_mappings": {"A": "col_a", "B": "col_b"},
        "skiprows": [0, 1],
        "trim_strings": False,
        "drop_empty_rows": True,
        "unpivot": True,
        "id_columns": ["A"],
        "var_name": "period",
        "value_name": "amount",
        "required_fields": ["col_a", "period"],
        "field_types": {"period": "date", "amount": "numeric"},
        "provider_name": "acme",
        "template_version": 3,
    }
    template = Template.from_dict(payload)
    path = tmp_path / "sample.df-template.json"
    path.write_text(json.dumps(template.to_dict(), indent=2), encoding="utf-8")

    loaded = Template.from_dict(json.loads(path.read_text(encoding="utf-8")))
    assert loaded.required_fields == template.required_fields
    assert loaded.field_types == template.field_types
    assert loaded.var_name == "period"
    assert loaded.value_name == "amount"
