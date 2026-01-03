import argparse
import sys
from pathlib import Path

import pandas as pd


def read_frame(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xls", ".xlsx"}:
        return pd.read_excel(path)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {path.suffix}")


def concat_frames(files: list[Path], strict_schema: bool) -> pd.DataFrame:
    frames = []
    base_cols: list[str] | None = None
    for f in files:
        df = read_frame(f)
        if strict_schema:
            if base_cols is None:
                base_cols = list(df.columns)
            elif list(df.columns) != base_cols:
                raise ValueError(f"Schema mismatch in {f.name}")
        frames.append(df)
    return pd.concat(frames, ignore_index=True, sort=False)


def merge_frames(files: list[Path], keys: list[str], how: str) -> pd.DataFrame:
    if not keys:
        raise ValueError("Merge mode requires at least one key (use --keys a,b).")
    frames = [read_frame(f) for f in files]
    merged = frames[0]
    for idx, df in enumerate(frames[1:], start=2):
        missing_left = [k for k in keys if k not in merged.columns]
        missing_right = [k for k in keys if k not in df.columns]
        if missing_left or missing_right:
            raise ValueError(
                f"Missing merge keys. Left missing {missing_left}, right missing {missing_right}."
            )
        merged = merged.merge(df, on=keys, how=how, suffixes=("", f"_{idx}"))
    return merged


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Combine cleaned files into one report with concat or key-based merge."
    )
    parser.add_argument(
        "--input-dir",
        default="data/output",
        help="Directory with cleaned files (default: data/output).",
    )
    parser.add_argument(
        "--pattern",
        default="*.xlsx",
        help="Glob pattern to read (default: *.xlsx). Supports parquet too.",
    )
    parser.add_argument(
        "--mode",
        choices=["concat", "merge"],
        default="concat",
        help="Concat stacks rows; merge joins on keys.",
    )
    parser.add_argument(
        "--keys",
        default="",
        help="Comma-separated merge keys (required for merge mode). Example: order_id,article_sku",
    )
    parser.add_argument(
        "--how",
        choices=["inner", "outer", "left", "right"],
        default="inner",
        help="Join type for merge mode.",
    )
    parser.add_argument(
        "--strict-schema",
        action="store_true",
        help="Fail concat if column order/names differ.",
    )
    parser.add_argument(
        "--output",
        default="Master_Sales_Report.xlsx",
        help="Output file name (xlsx or parquet).",
    )

    args = parser.parse_args(argv)

    output_dir = Path(args.input_dir)
    all_files = sorted(output_dir.glob(args.pattern))
    if not all_files:
        print("No data found!")
        return 1

    print(f"Found {len(all_files)} files. Combining with mode={args.mode}...")

    try:
        if args.mode == "concat":
            master_df = concat_frames(all_files, strict_schema=args.strict_schema)
        else:
            keys = [k.strip() for k in args.keys.split(",") if k.strip()]
            master_df = merge_frames(all_files, keys=keys, how=args.how)
    except Exception as exc:
        print(f"Combine failed: {exc}")
        return 1

    print(f"Total Rows: {len(master_df)}")
    if "provider_id" in master_df.columns and "sales_amount" in master_df.columns:
        try:
            print(master_df.groupby("provider_id")["sales_amount"].sum())
        except Exception:
            pass

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".parquet":
        master_df.to_parquet(output_path, index=False)
    else:
        master_df.to_excel(output_path, index=False)
    print(f"Saved to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
