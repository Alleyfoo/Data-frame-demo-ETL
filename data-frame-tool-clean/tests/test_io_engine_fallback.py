from pathlib import Path

import pandas as pd

from src.services.io import read_preview_frame, sheet_names


def test_mislabeled_csv_with_xlsx_extension(tmp_path: Path):
    mislabeled = tmp_path / "fake.xlsx"
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    df.to_csv(mislabeled, index=False)

    names = sheet_names(mislabeled)
    # Excel engine should fail and sheet_names should return empty gracefully
    assert names == []

    preview = read_preview_frame(
        path=mislabeled,
        source_type="excel",
        sheet=None,
        header_row=0,
        skiprows=[],
        nrows=2,
    )
    assert list(preview.columns) == ["a", "b"]
    assert len(preview) == 2
