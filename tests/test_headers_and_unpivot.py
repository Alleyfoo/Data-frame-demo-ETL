from pathlib import Path

import pandas as pd

from src.pipeline import transform, warn_on_schema_diff
from src.templates import Template, normalize_excel_headers


def test_merged_header_normalization():
    sample = Path("samples/merged_header.xlsx")
    preview = pd.read_excel(sample, sheet_name="Sales", header=None, nrows=12)
    guessed = 0
    # emulate harness guess
    for idx, (_, row) in enumerate(preview.iterrows()):
        non_null = row.dropna()
        if non_null.empty:
            continue
        str_ratio = sum(isinstance(val, str) for val in non_null) / len(non_null)
        width_ratio = len(non_null) / preview.shape[1] if preview.shape[1] else 0
        if str_ratio > 0.8 and width_ratio > 0.5:
            guessed = idx
            break
    headers, _merged = normalize_excel_headers(sample, sheet="Sales", header_row=guessed, skiprows=None)
    assert {"Jan", "Feb", "Mar"}.issubset(set(headers))


def test_unpivot_transform_metrics():
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

    clean_df, metrics = transform(df, template)
    assert set(clean_df.columns) >= {"article_sku", "period", "sales_amount", "provider_id"}
    assert metrics["unpivot_after"][1] == 3  # columns after melt
    assert len(clean_df) == 4


def test_warn_on_schema_diff():
    template = Template(
        sheet="Sheet1",
        header_row=0,
        columns=["a", "b", "c"],
    )
    df = pd.DataFrame({"a": [1], "b": [2], "extra": [3]})
    missing, extra = warn_on_schema_diff(df, template)
    assert missing == ["c"]
    assert extra == ["extra"]
