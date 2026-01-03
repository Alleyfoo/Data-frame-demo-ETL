"""Generate representative Excel workbooks for the data-frame tool samples.

This script creates three Excel files under the `samples/` directory:
- multi_sheet_jan.xlsx: multiple sheets with related schemas
- offset_header.xlsx: header begins after descriptive banner rows
- consistent_schema_feb.xlsx: consistent schema across months/files

Requires pandas with the openpyxl engine available.
"""

from __future__ import annotations

import pathlib

import pandas as pd


def _ensure_samples_dir() -> pathlib.Path:
    root = pathlib.Path(__file__).resolve().parent
    root.mkdir(parents=True, exist_ok=True)
    return root


def _write_multi_sheet(path: pathlib.Path) -> None:
    january_orders = pd.DataFrame(
        [
            {"order_id": 101, "region": "North", "product": "Widget", "quantity": 7, "unit_price": 12.0},
            {"order_id": 102, "region": "West", "product": "Gadget", "quantity": 4, "unit_price": 19.5},
        ]
    )
    adjustments = pd.DataFrame(
        [
            {"order_id": 101, "adjustment": "Discount", "amount": -5.0},
            {"order_id": 102, "adjustment": "Rush shipping", "amount": 12.0},
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        january_orders.to_excel(writer, sheet_name="Orders", index=False)
        adjustments.to_excel(writer, sheet_name="Adjustments", index=False)


def _write_offset_header(path: pathlib.Path) -> None:
    details = pd.DataFrame(
        [
            {"department": "Finance", "owner": "L. Singh", "active": True, "budget": 125000},
            {"department": "Marketing", "owner": "C. Wang", "active": True, "budget": 98000},
        ]
    )

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        # Reserve top rows for metadata to simulate exported systems that offset headers
        details.to_excel(writer, sheet_name="Departments", index=False, startrow=3)
        worksheet = writer.sheets["Departments"]
        worksheet["A1"] = "Department roster export"
        worksheet["A2"] = "Headers start on row 4"


def _write_consistent_schema(path: pathlib.Path) -> None:
    february_orders = pd.DataFrame(
        [
            {"order_id": 201, "region": "North", "product": "Widget", "quantity": 6, "unit_price": 12.0},
            {"order_id": 202, "region": "West", "product": "Gadget", "quantity": 5, "unit_price": 19.5},
        ]
    )

    february_orders.to_excel(path, sheet_name="Orders", index=False)


def _write_merged_headers(path: pathlib.Path) -> None:
    df = pd.DataFrame(
        [
            {"2020 Jan": 10, "2020 Feb": 12, "2020 Mar": 8},
            {"2020 Jan": 14, "2020 Feb": 7, "2020 Mar": 9},
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Sales", index=False, startrow=1)
        ws = writer.sheets["Sales"]
        ws.merge_cells("A1:C1")
        ws["A1"] = "2020"
        ws["A2"] = "Jan"
        ws["B2"] = "Feb"
        ws["C2"] = "Mar"


def _write_split_year_month(path: pathlib.Path) -> None:
    df = pd.DataFrame(
        [
            {"SKU": "A1", "2020": 10, "2021": 12},
            {"SKU": "B2", "2020": 14, "2021": 7},
        ]
    )
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Split", index=False, startrow=2)
        ws = writer.sheets["Split"]
        ws["A1"] = "SKU"
        ws["B1"] = "2020"
        ws["B2"] = "tammikuu"
        ws["C1"] = "2021"
        ws["C2"] = "helmikuu"


def main() -> None:
    root = _ensure_samples_dir()
    _write_multi_sheet(root / "multi_sheet_jan.xlsx")
    _write_offset_header(root / "offset_header.xlsx")
    _write_consistent_schema(root / "consistent_schema_feb.xlsx")
    _write_merged_headers(root / "merged_header.xlsx")
    _write_split_year_month(root / "split_year_month.xlsx")
    print("Generated sample Excel workbooks in", root)


if __name__ == "__main__":
    main()
