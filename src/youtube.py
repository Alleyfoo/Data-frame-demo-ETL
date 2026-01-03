"""Lightweight YouTube Data API client that returns pandas DataFrames."""

from __future__ import annotations

import logging
import os
import re
from typing import Iterable, Iterator, List, Optional

import pandas as pd
import requests

API_BASE = "https://www.googleapis.com/youtube/v3"
DEFAULT_TIMEOUT = 30
OUTPUT_COLUMNS = [
    "video_id",
    "title",
    "description",
    "channel_id",
    "channel_title",
    "published_at",
    "duration",
    "duration_seconds",
    "view_count",
    "like_count",
    "comment_count",
    "tags",
    "thumbnail_url",
]
DERIVED_COLUMNS = [
    "source",
    "engagement_rate",
    "engagement_rate_pct",
]
SUMMARY_SHEETS = ["detail", "top_videos", "per_channel", "per_year"]


class YouTubeAuthError(RuntimeError):
    """Raised when an API key is missing."""


def _get_api_key(explicit: str | None = None) -> str:
    api_key = explicit or os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        raise YouTubeAuthError(
            "Provide a YouTube Data API key via --api-key or the YOUTUBE_API_KEY environment variable."
        )
    return api_key


def _request(endpoint: str, params: dict, api_key: str) -> dict:
    payload = dict(params or {})
    payload["key"] = api_key
    url = f"{API_BASE}/{endpoint}"
    resp = requests.get(url, params=payload, timeout=DEFAULT_TIMEOUT)
    try:
        resp.raise_for_status()
    except requests.HTTPError as exc:  # pragma: no cover - passthrough detail
        detail = resp.text
        raise RuntimeError(f"YouTube API error for {endpoint}: {detail}") from exc
    return resp.json()


def _parse_iso8601_duration(duration: str | None) -> int:
    """
    Convert ISO-8601 duration (e.g., PT1H2M3S) to seconds.
    Returns 0 when parsing fails or duration is missing.
    """
    if not duration:
        return 0
    pattern = re.compile(
        r"P(?:(?P<days>\d+)D)?"
        r"(?:T"
        r"(?:(?P<hours>\d+)H)?"
        r"(?:(?P<minutes>\d+)M)?"
        r"(?:(?P<seconds>\d+)S)?"
        r")?$"
    )
    match = pattern.match(duration)
    if not match:
        return 0
    parts = {k: int(v) if v is not None else 0 for k, v in match.groupdict().items()}
    return parts["seconds"] + parts["minutes"] * 60 + parts["hours"] * 3600 + parts["days"] * 86400


def _chunked(items: Iterable[str], size: int) -> Iterator[List[str]]:
    bucket: List[str] = []
    for item in items:
        bucket.append(item)
        if len(bucket) >= size:
            yield bucket
            bucket = []
    if bucket:
        yield bucket


def _uploads_playlist_id(channel_id: str, api_key: str) -> str:
    data = _request(
        "channels",
        {"part": "contentDetails", "id": channel_id, "maxResults": 1},
        api_key=api_key,
    )
    items = data.get("items") or []
    if not items:
        raise ValueError(f"Channel '{channel_id}' not found.")
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]


def _fetch_playlist_video_ids(playlist_id: str, max_results: int, api_key: str) -> list[str]:
    videos: list[str] = []
    page_token: Optional[str] = None

    while True:
        remaining = max_results - len(videos)
        if remaining <= 0:
            break

        data = _request(
            "playlistItems",
            {
                "part": "contentDetails",
                "playlistId": playlist_id,
                "maxResults": min(50, remaining),
                "pageToken": page_token,
            },
            api_key=api_key,
        )
        for item in data.get("items", []):
            vid = item.get("contentDetails", {}).get("videoId")
            if vid:
                videos.append(vid)

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    return videos[:max_results]


def _pick_thumbnail(snippet: dict) -> str | None:
    thumbs = snippet.get("thumbnails") or {}
    for key in ("standard", "high", "medium", "default"):
        if key in thumbs and isinstance(thumbs[key], dict):
            return thumbs[key].get("url")
    return None


def fetch_videos_dataframe(
    *,
    channel_id: str | None = None,
    playlist_id: str | None = None,
    max_results: int = 25,
    api_key: str | None = None,
) -> pd.DataFrame:
    """
    Fetch videos for a channel or playlist and return a tidy DataFrame.

    Args:
        channel_id: YouTube channel ID to pull uploads from.
        playlist_id: Playlist ID to pull from (overrides channel_id when provided).
        max_results: Maximum number of videos to return (API caps at 50 per page).
        api_key: Explicit API key (otherwise reads YOUTUBE_API_KEY env var).
    """
    key = _get_api_key(api_key)
    target_playlist = playlist_id if playlist_id else (_uploads_playlist_id(channel_id, key) if channel_id else None)
    if not target_playlist:
        raise ValueError("Provide a channel_id or playlist_id to fetch videos.")

    video_ids = _fetch_playlist_video_ids(target_playlist, max_results=max(1, max_results), api_key=key)
    if not video_ids:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    records: list[dict] = []
    for chunk in _chunked(video_ids, 50):
        data = _request(
            "videos",
            {
                "part": "snippet,contentDetails,statistics",
                "id": ",".join(chunk),
                "maxResults": len(chunk),
            },
            api_key=key,
        )
        for item in data.get("items", []):
            snippet = item.get("snippet", {}) or {}
            stats = item.get("statistics", {}) or {}
            content = item.get("contentDetails", {}) or {}
            records.append(
                {
                    "video_id": item.get("id"),
                    "title": snippet.get("title"),
                    "description": snippet.get("description"),
                    "channel_id": snippet.get("channelId"),
                    "channel_title": snippet.get("channelTitle"),
                    "published_at": snippet.get("publishedAt"),
                    "duration": content.get("duration"),
                    "duration_seconds": _parse_iso8601_duration(content.get("duration")),
                    "view_count": int(stats.get("viewCount", 0) or 0),
                    "like_count": int(stats.get("likeCount", 0) or 0),
                    "comment_count": int(stats.get("commentCount", 0) or 0),
                    "tags": ", ".join(snippet.get("tags", [])) if snippet.get("tags") else "",
                    "thumbnail_url": _pick_thumbnail(snippet),
                }
            )

    df = pd.DataFrame(records, columns=OUTPUT_COLUMNS)
    logging.info("Fetched %d videos from playlist %s", len(df), target_playlist)
    return df


def add_engagement_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Add engagement_rate metrics; avoids division by zero."""
    if df.empty:
        return df
    base = df.copy()
    likes = base["like_count"] if "like_count" in base else pd.Series(0, index=base.index)
    comments = base["comment_count"] if "comment_count" in base else pd.Series(0, index=base.index)
    views = base["view_count"] if "view_count" in base else pd.Series(0, index=base.index)
    engagement = (likes.fillna(0) + comments.fillna(0)).astype(float)
    denom = pd.to_numeric(views.replace({0: None}), errors="coerce")
    rate = engagement.divide(denom).fillna(0.0)
    base["engagement_rate"] = rate
    base["engagement_rate_pct"] = (rate * 100).round(2)
    return base


def build_summaries(df: pd.DataFrame, top_n: int = 10) -> dict[str, pd.DataFrame]:
    """Create summary DataFrames for reporting/demo."""
    if df.empty:
        return {
            "detail": df,
            "top_videos": pd.DataFrame(columns=OUTPUT_COLUMNS + DERIVED_COLUMNS),
            "per_channel": pd.DataFrame(
                columns=["channel_title", "video_count", "views", "likes", "comments", "avg_engagement_pct"]
            ),
            "per_year": pd.DataFrame(columns=["year", "video_count", "views", "likes", "comments"]),
        }

    detail = df.copy()
    detail["published_dt"] = pd.to_datetime(detail["published_at"], errors="coerce")
    top_videos = detail.sort_values(by=["view_count", "like_count"], ascending=False).head(top_n)

    per_channel = (
        detail.groupby("channel_title", dropna=False)
        .agg(
            video_count=("video_id", "count"),
            views=("view_count", "sum"),
            likes=("like_count", "sum"),
            comments=("comment_count", "sum"),
            avg_engagement_pct=("engagement_rate_pct", "mean"),
        )
        .reset_index()
        .sort_values(by="views", ascending=False)
    )

    per_year = (
        detail.assign(year=detail["published_dt"].dt.year)
        .groupby("year", dropna=False)
        .agg(
            video_count=("video_id", "count"),
            views=("view_count", "sum"),
            likes=("like_count", "sum"),
            comments=("comment_count", "sum"),
        )
        .reset_index()
        .sort_values(by="year")
    )

    return {
        "detail": detail.drop(columns=["published_dt"]),
        "top_videos": top_videos.drop(columns=["published_dt"]),
        "per_channel": per_channel,
        "per_year": per_year,
    }


__all__ = ["fetch_videos_dataframe", "YouTubeAuthError", "add_engagement_metrics", "build_summaries"]
