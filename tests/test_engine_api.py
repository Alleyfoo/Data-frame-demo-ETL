import pandas as pd
import pytest

from src.api.v1 import engine
from src.templates import Template


def test_engine_transform_unpivot_metrics():
    df = pd.DataFrame(
        {
            "article_sku": ["s1", "s2"],
            "Jan": [1, 2],
            "Feb": [3, 4],
        }
    )
    template = Template(
        sheet="Sheet1",
        header_row=0,
        columns=["article_sku", "Jan", "Feb"],
        column_mappings={"article_sku": "article_sku"},
        unpivot=True,
        var_name="period",
        value_name="sales_amount",
        provider_name="acme",
    )

    clean_df, metrics = engine.transform(df, template)
    assert set(clean_df.columns) >= {"article_sku", "period", "sales_amount", "provider_id"}
    assert metrics["unpivot_after"][1] == 3
    assert len(clean_df) == 4


def test_engine_warn_on_schema_diff():
    template = Template(sheet="Sheet1", header_row=0, columns=["a", "b", "c"])
    df = pd.DataFrame({"a": [1], "b": [2], "extra": [3]})
    missing, extra = engine.warn_on_schema_diff(df, template)
    assert missing == ["c"]
    assert extra == ["extra"]


def test_engine_validate_contract_missing_required():
    df = pd.DataFrame(
        {
            "provider_id": ["p1"],
            "article_sku": ["sku"],
            "report_date": ["2024-01-01"],
            "sales_amount": [1.0],
        }
    )
    template = Template(
        sheet="Sheet1",
        header_row=0,
        required_fields=["article_sku"],
    )
    result = engine.validate(df, template, validation_level="contract")
    assert "article_sku" in result.columns

    template_missing = Template(
        sheet="Sheet1",
        header_row=0,
        required_fields=["missing_field"],
    )
    with pytest.raises(Exception):
        engine.validate(df, template_missing, validation_level="contract")
