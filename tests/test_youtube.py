from src.youtube import _parse_iso8601_duration, add_engagement_metrics


def test_parse_iso8601_duration_parses_components():
    assert _parse_iso8601_duration("PT1H2M3S") == 3723
    assert _parse_iso8601_duration("PT5M") == 300
    assert _parse_iso8601_duration("PT45S") == 45
    assert _parse_iso8601_duration("P1DT1H") == 90000  # 1 day + 1 hour


def test_parse_iso8601_duration_handles_invalid():
    assert _parse_iso8601_duration(None) == 0
    assert _parse_iso8601_duration("") == 0
    assert _parse_iso8601_duration("not-a-duration") == 0


def test_add_engagement_metrics_handles_zero_views():
    import pandas as pd

    df = pd.DataFrame(
        {
            "video_id": ["a", "b"],
            "view_count": [0, 100],
            "like_count": [10, 5],
            "comment_count": [5, 5],
        }
    )
    out = add_engagement_metrics(df)
    assert "engagement_rate" in out.columns
    assert out.loc[out["video_id"] == "a", "engagement_rate"].iat[0] == 0
    assert out.loc[out["video_id"] == "b", "engagement_rate"].iat[0] == (5 + 5) / 100
