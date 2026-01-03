from src.youtube import _parse_iso8601_duration


def test_parse_iso8601_duration_parses_components():
    assert _parse_iso8601_duration("PT1H2M3S") == 3723
    assert _parse_iso8601_duration("PT5M") == 300
    assert _parse_iso8601_duration("PT45S") == 45
    assert _parse_iso8601_duration("P1DT1H") == 90000  # 1 day + 1 hour


def test_parse_iso8601_duration_handles_invalid():
    assert _parse_iso8601_duration(None) == 0
    assert _parse_iso8601_duration("") == 0
    assert _parse_iso8601_duration("not-a-duration") == 0
