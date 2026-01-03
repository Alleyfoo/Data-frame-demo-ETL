"""Bridge helpers between Streamlit recipes and backend templates."""

from __future__ import annotations

from typing import Any

from ..templates import HeaderCell, Template


def recipe_to_template(recipe: dict[str, Any]) -> Template:
    """Convert a Streamlit recipe payload into a Template."""
    if not isinstance(recipe, dict):
        raise ValueError("Recipe must be a dictionary.")

    mappings = recipe.get("mappings", {}) or {}
    column_mappings = {
        str(source): str(target)
        for source, target in mappings.items()
        if target not in (None, "", "(unmapped)")
    }

    headers_payload = recipe.get("headers", []) or []
    headers: list[HeaderCell] = []
    for item in headers_payload:
        if isinstance(item, dict):
            headers.append(HeaderCell.from_dict(item))

    columns = recipe.get("columns", []) or list(column_mappings.keys())
    columns = [str(col) for col in columns if col not in (None, "")]

    return Template(
        sheet=recipe.get("sheet"),
        sheets=recipe.get("sheets", []) or [],
        header_row=int(recipe.get("header_row", 0) or 0),
        skiprows=recipe.get("skiprows", []) or [],
        delimiter=recipe.get("delimiter", ",") or ",",
        encoding=recipe.get("encoding", "utf-8") or "utf-8",
        source_type=recipe.get("source_type", "excel") or "excel",
        source_file=recipe.get("source_file"),
        output_dir=recipe.get("output_dir"),
        provider_name=recipe.get("provider_name"),
        columns=columns,
        column_mappings=column_mappings,
        headers=headers,
        unpivot=bool(recipe.get("unpivot", False)),
        id_columns=recipe.get("id_columns", []) or [],
        var_name=recipe.get("var_name", "report_date") or "report_date",
        value_name=recipe.get("value_name", "sales_amount") or "sales_amount",
        combine_sheets=bool(recipe.get("combine_sheets", False)),
        combine_on=recipe.get("combine_on", []) or [],
        connection_name=recipe.get("connection_name"),
        sql_table=recipe.get("sql_table"),
        sql_query=recipe.get("sql_query"),
        trim_strings=bool(recipe.get("trim_strings", True)),
        drop_empty_rows=bool(recipe.get("drop_empty_rows", False)),
        drop_null_columns_threshold=recipe.get("drop_null_columns_threshold"),
        dedupe_on=recipe.get("dedupe_on", []) or [],
        strip_thousands=bool(recipe.get("strip_thousands", False)),
        required_fields=recipe.get("required_fields", []) or [],
        field_types=recipe.get("field_types", {}) or {},
    )


__all__ = ["recipe_to_template"]
