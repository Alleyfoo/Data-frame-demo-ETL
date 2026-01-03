"""Unified CLI entrypoint for Data Frame Tool."""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

import pandas as pd

from .api.v1.engine import DataEngine, warn_on_schema_diff
from .connectors import check_sqlalchemy_available
from .templates import Template, load_template, locate_template, locate_streamlit_template
from .youtube import (
    YouTubeAuthError,
    add_engagement_metrics,
    build_summaries,
    fetch_videos_dataframe,
)

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
DEFAULT_INPUT = DATA_DIR / "input"
OUTPUT_DIR = DATA_DIR / "output"
ARCHIVE_DIR = DATA_DIR / "archive"
QUARANTINE_DIR = DATA_DIR / "quarantine"
YOUTUBE_DEFAULT_OUTPUT = OUTPUT_DIR / "youtube_videos.xlsx"


def setup_logging(log_to_file: bool = False) -> None:
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    if log_to_file:
        logging.basicConfig(filename="pipeline.log", level=logging.INFO, format=log_format)
    else:
        logging.basicConfig(level=logging.INFO, format=log_format)


def _iter_files(input_path: Path):
    return list(input_path.glob("*.xlsx")) + list(input_path.glob("*.csv"))


def _save_output(df: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".parquet":
        df.to_parquet(output_path, index=False)
        return output_path
    excel_path = output_path.with_suffix(".xlsx")
    df.to_excel(excel_path, index=False)
    return excel_path


def run_batch_process(
    target_dir: str,
    output_fmt: str = "xlsx",
    fail_on_missing: bool = False,
    fail_on_extra: bool = False,
    validation_level: str = "coerce",
    use_streamlit_templates: bool = False,
) -> None:
    """Scans input folder (and optional company subfolders), applies templates, moves files."""

    input_path = Path(target_dir)
    engine = DataEngine()

    def process_directory(dir_path: Path, company: str | None = None) -> None:
        out_dir = OUTPUT_DIR / company if company else OUTPUT_DIR
        arc_dir = ARCHIVE_DIR / company if company else ARCHIVE_DIR
        qua_dir = QUARANTINE_DIR / company if company else QUARANTINE_DIR
        for d in [out_dir, arc_dir, qua_dir]:
            d.mkdir(parents=True, exist_ok=True)

        logging.info(f"Scanning directory: {dir_path.resolve()}{' (company '+company+')' if company else ''}")

        sql_ran = False
        try:
            tpl_path = (
                locate_streamlit_template(dir_path)
                if use_streamlit_templates
                else locate_template(dir_path)
            )
            template = load_template(tpl_path)
            if template.source_type == "sql":
                check_sqlalchemy_available()
                output_path = out_dir / f"sql_clean.{output_fmt}"
                result, output_df = engine.run_full_process(
                    source_path=Path(""),
                    template=template,
                    output_path=output_path,
                    validation_level=validation_level,
                )
                if result.success and output_df is not None:
                    saved_path = _save_output(output_df, output_path)
                    logging.info(f"SQL template processed. Output at {saved_path}")
                else:
                    logging.warning("SQL template failed; see logs/quarantine.")
                sql_ran = True
        except FileNotFoundError:
            pass
        except Exception as exc:
            logging.error(f"Error checking for SQL template: {exc}")

        files = _iter_files(dir_path)
        if not files and not sql_ran:
            logging.info("No files found in input directory.")
            return

        for file_path in files:
            logging.info(f"Processing {file_path.name}...")
            try:
                try:
                    tpl_path = (
                        locate_streamlit_template(file_path.parent, stem=file_path.stem)
                        if use_streamlit_templates
                        else locate_template(file_path.parent, stem=file_path.stem)
                    )
                    template = load_template(tpl_path)
                except FileNotFoundError:
                    logging.warning(f"No template found for {file_path.name}. Skipping.")
                    continue

                ext = ".parquet" if output_fmt == "parquet" else ".xlsx"
                output_path = out_dir / f"{file_path.stem}_clean{ext}"

                result, output_df = engine.run_full_process(
                    source_path=file_path,
                    template=template,
                    output_path=output_path,
                    validation_level=validation_level,
                )

                missing, extra = ([], [])
                if output_df is not None:
                    missing, extra = warn_on_schema_diff(output_df, template, context_label=file_path.name)
                if (fail_on_missing and missing) or (fail_on_extra and extra):
                    result = result.model_copy(
                        update={"success": False, "message": "Schema drift failure"}
                    )

                if result.success and output_df is not None:
                    _save_output(output_df, output_path)
                    dest = arc_dir / file_path.name
                    if dest.exists():
                        timestamp = pd.Timestamp.now().strftime("%Y%m%d%H%M%S")
                        dest = arc_dir / f"{file_path.stem}_{timestamp}{file_path.suffix}"
                    shutil.move(str(file_path), str(dest))
                    logging.info(f"Archived source file to {dest}")
                else:
                    dest = qua_dir / file_path.name
                    if file_path.exists():
                        shutil.move(str(file_path), str(dest))
                    logging.warning(f"Quarantined source file to {dest}")
            except Exception as e:
                logging.error(f"Critical error on {file_path.name}: {e}")

    subdirs = [p for p in input_path.iterdir() if p.is_dir()]
    if subdirs:
        for sub in subdirs:
            process_directory(sub, company=sub.name)
    else:
        process_directory(input_path, company=None)


def run_combine_cli(input_dir: str, pattern: str, mode: str, keys: list[str], how: str, strict: bool, output: str) -> None:
    engine = DataEngine()
    df = engine.run_combine(
        input_dir=Path(input_dir),
        pattern=pattern,
        mode=mode,
        keys=keys,
        how=how,
        strict_schema=strict,
    )
    out_path = Path(output)
    _save_output(df, out_path)
    logging.info("Combined %d rows using mode=%s. Saved to %s", len(df), mode, out_path)


def run_youtube_cli(
    channel_ids: list[str] | None,
    playlist_ids: list[str] | None,
    max_results: int,
    api_key: str | None,
    output: str,
    output_fmt: str,
    summary_output: str | None,
    top_n: int,
) -> None:
    if not channel_ids and not playlist_ids:
        raise ValueError("Provide at least one --channel-id or --playlist-id.")

    frames: list[pd.DataFrame] = []

    for pid in playlist_ids or []:
        df = fetch_videos_dataframe(
            playlist_id=pid,
            max_results=max_results,
            api_key=api_key,
        )
        df["source"] = f"playlist:{pid}"
        frames.append(df)

    for cid in channel_ids or []:
        df = fetch_videos_dataframe(
            channel_id=cid,
            max_results=max_results,
            api_key=api_key,
        )
        df["source"] = f"channel:{cid}"
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=[])
    if not combined.empty:
        combined = combined.drop_duplicates(subset=["video_id"])
        combined = add_engagement_metrics(combined)
        combined = combined.sort_values(by=["view_count", "like_count"], ascending=False)
        summaries = build_summaries(combined, top_n=top_n)
    else:
        summaries = {"detail": combined}

    out_path = Path(output)
    if output_fmt == "parquet":
        out_path = out_path.with_suffix(".parquet")
    saved = _save_output(combined, out_path)

    if summary_output and summaries:
        summary_path = Path(summary_output)
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(summary_path) as writer:
            for name, frame in summaries.items():
                frame.to_excel(writer, sheet_name=name[:31], index=False)
        logging.info("Wrote summary workbook to %s", summary_path)

    logging.info(
        "Fetched %d YouTube videos and saved to %s",
        len(combined),
        saved,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Data Frame Tool")
    sub = parser.add_subparsers(dest="command")

    run = sub.add_parser("run", help="Process files in batch mode.")
    run.add_argument("--target-dir", type=str, default=str(DEFAULT_INPUT), help="Directory to scan for files")
    run.add_argument("--output-fmt", choices=["xlsx", "parquet"], default="xlsx")
    run.add_argument("--fail-on-missing", action="store_true")
    run.add_argument("--fail-on-extra", action="store_true")
    run.add_argument("--validation-level", choices=["off", "coerce", "contract"], default="coerce")
    run.add_argument(
        "--use-streamlit-templates",
        action="store_true",
        help="Only load .df-template.json files generated by Streamlit.",
    )

    combine = sub.add_parser("combine", help="Combine cleaned outputs.")
    combine.add_argument("--input-dir", type=str, default="data/output")
    combine.add_argument("--pattern", type=str, default="*.xlsx")
    combine.add_argument("--mode", choices=["concat", "merge"], default="concat")
    combine.add_argument("--keys", type=str, default="")
    combine.add_argument("--how", choices=["inner", "outer", "left", "right"], default="inner")
    combine.add_argument("--strict-schema", action="store_true")
    combine.add_argument("--output", type=str, default="Master_Sales_Report.xlsx")

    yt = sub.add_parser("youtube", help="Fetch a YouTube channel or playlist into a DataFrame output.")
    yt.add_argument("--channel-id", type=str, action="append", dest="channel_ids", help="YouTube channel ID to pull uploads from (repeatable)")
    yt.add_argument("--playlist-id", type=str, action="append", dest="playlist_ids", help="Playlist ID to pull from (repeatable)")
    yt.add_argument("--api-key", type=str, help="YouTube Data API key; defaults to YOUTUBE_API_KEY env var")
    yt.add_argument("--max-results", type=int, default=25, help="Maximum videos to fetch")
    yt.add_argument(
        "--output",
        type=str,
        default=str(YOUTUBE_DEFAULT_OUTPUT),
        help="Output path for the dataset (xlsx or parquet).",
    )
    yt.add_argument("--output-fmt", choices=["xlsx", "parquet"], default="xlsx")
    yt.add_argument(
        "--summary-output",
        type=str,
        default=str(OUTPUT_DIR / "youtube_summary.xlsx"),
        help="Optional summary workbook path (xlsx with multiple sheets).",
    )
    yt.add_argument("--top-n", type=int, default=10, help="Top N videos to include in summary.")

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 1

    setup_logging(log_to_file=args.command == "run")

    if args.command == "run":
        run_batch_process(
            args.target_dir,
            output_fmt=args.output_fmt,
            fail_on_missing=args.fail_on_missing,
            fail_on_extra=args.fail_on_extra,
            validation_level=args.validation_level,
            use_streamlit_templates=args.use_streamlit_templates,
        )
        return 0

    if args.command == "combine":
        keys = [k.strip() for k in args.keys.split(",") if k.strip()]
        run_combine_cli(
            input_dir=args.input_dir,
            pattern=args.pattern,
            mode=args.mode,
            keys=keys,
            how=args.how,
            strict=args.strict_schema,
            output=args.output,
        )
        return 0

    if args.command == "youtube":
        try:
            run_youtube_cli(
                channel_ids=args.channel_ids,
                playlist_ids=args.playlist_ids,
                max_results=args.max_results,
                api_key=args.api_key,
                output=args.output,
                output_fmt=args.output_fmt,
                summary_output=args.summary_output,
                top_n=args.top_n,
            )
            return 0
        except YouTubeAuthError as exc:
            logging.error(str(exc))
            return 1
        except Exception as exc:  # pragma: no cover - CLI passthrough
            logging.error("YouTube fetch failed: %s", exc)
            return 1

    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
