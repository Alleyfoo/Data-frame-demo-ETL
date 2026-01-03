from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import streamlit as st

from src.youtube import YouTubeAuthError, add_engagement_metrics, build_summaries, fetch_videos_dataframe

DETAIL_PATH = Path("data/output/youtube_detail.xlsx")
SUMMARY_PATH = Path("data/output/youtube_summary.xlsx")


def _display_existing() -> None:
    st.subheader("Current demo data")
    if DETAIL_PATH.exists():
        df = pd.read_excel(DETAIL_PATH)
        st.success(f"Detail file found: {DETAIL_PATH} ({len(df)} rows)")
        st.dataframe(df.head(5), use_container_width=True, hide_index=True)
    else:
        st.info("No detail file yet. Run the fetch form below.")

    if SUMMARY_PATH.exists():
        try:
            xl = pd.ExcelFile(SUMMARY_PATH)
            for name in ["top_videos", "per_channel", "per_year"]:
                if name in xl.sheet_names:
                    st.caption(f"Summary: {name}")
                    st.dataframe(xl.parse(name).head(10), use_container_width=True, hide_index=True)
        except Exception as exc:  # pragma: no cover - UI guardrail
            st.warning(f"Could not read summary workbook: {exc}")
    else:
        st.info("No summary workbook yet.")


def _fetch_and_save(playlists: list[str], channels: list[str], max_results: int, top_n: int, api_key: str | None) -> dict[str, pd.DataFrame]:
    frames: list[pd.DataFrame] = []

    for pid in playlists:
        df = fetch_videos_dataframe(playlist_id=pid, max_results=max_results, api_key=api_key)
        df["source"] = f"playlist:{pid}"
        frames.append(df)

    for cid in channels:
        df = fetch_videos_dataframe(channel_id=cid, max_results=max_results, api_key=api_key)
        df["source"] = f"channel:{cid}"
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=[])
    if combined.empty:
        return {"detail": combined, "top_videos": pd.DataFrame(), "per_channel": pd.DataFrame(), "per_year": pd.DataFrame()}

    combined = combined.drop_duplicates(subset=["video_id"])
    combined = add_engagement_metrics(combined)
    combined = combined.sort_values(by=["view_count", "like_count"], ascending=False)

    summaries = build_summaries(combined, top_n=top_n)
    DETAIL_PATH.parent.mkdir(parents=True, exist_ok=True)
    combined.to_excel(DETAIL_PATH, index=False)

    with pd.ExcelWriter(SUMMARY_PATH) as writer:
        for name, frame in summaries.items():
            frame.to_excel(writer, sheet_name=name[:31], index=False)

    return summaries | {"detail": combined}


def render() -> None:
    st.title("YouTube ETL Demo")
    st.write("Fetch playlists or channels, compute engagement, and view summaries.")

    _display_existing()

    st.subheader("Fetch fresh data")
    col1, col2 = st.columns(2)
    playlists_text = col1.text_input("Playlist IDs (comma-separated)", value="")
    channels_text = col2.text_input("Channel IDs (comma-separated)", value="")
    max_results = int(st.number_input("Max results per source (<=50)", min_value=1, max_value=50, value=10))
    top_n = int(st.number_input("Top N videos for summary", min_value=1, max_value=50, value=10))
    api_key = st.text_input("API key (optional; uses YOUTUBE_API_KEY if empty)", value=os.getenv("YOUTUBE_API_KEY", ""), type="password")

    if st.button("Fetch YouTube data"):
        playlist_ids = [p.strip() for p in playlists_text.split(",") if p.strip()]
        channel_ids = [c.strip() for c in channels_text.split(",") if c.strip()]

        if not playlist_ids and not channel_ids:
            st.warning("Add at least one playlist or channel ID.")
            return

        with st.spinner("Fetching..."):
            try:
                summaries = _fetch_and_save(playlist_ids, channel_ids, max_results, top_n, api_key or None)
            except YouTubeAuthError as exc:
                st.error(str(exc))
                return
            except Exception as exc:  # pragma: no cover - UI guardrail
                st.error(f"Fetch failed: {exc}")
                return

        st.success(f"Saved detail to {DETAIL_PATH} and summary to {SUMMARY_PATH}")
        st.metric("Sources requested", len(playlist_ids) + len(channel_ids))
        if summaries["detail"].empty:
            st.info("No videos returned.")
            return

        st.caption("Top videos")
        st.dataframe(summaries["top_videos"].head(top_n), use_container_width=True, hide_index=True)

        st.caption("Per-channel summary")
        st.dataframe(summaries["per_channel"], use_container_width=True, hide_index=True)

        st.caption("Per-year summary")
        st.dataframe(summaries["per_year"], use_container_width=True, hide_index=True)


main = render
