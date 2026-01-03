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
    target_playlist = playlist_id or _uploads_playlist_id(channel_id, key) if channel_id else None
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


__all__ = ["fetch_videos_dataframe", "YouTubeAuthError"]
