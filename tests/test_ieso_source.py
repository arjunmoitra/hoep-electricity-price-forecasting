import datetime as dt

import pytest

from src.ingestion.ieso_source import (
    key_to_delivery,
    list_remote_files,
    parse_directory_listing,
    parse_filename,
)

APACHE_LISTING_SAMPLE = """
<html><head><title>Index of /public/RealtimeOntarioZonalPrice/</title></head>
<body>
<h1>Index of /public/RealtimeOntarioZonalPrice/</h1>
<table>
<tr><td><a href="/public/">Parent Directory</a></td></tr>
<tr><td><a href="PUB_RealtimeOntarioZonalPrice.xml">PUB_RealtimeOntarioZonalPrice.xml</a></td><td>24-Nov-2026 15:42</td></tr>
<tr><td><a href="PUB_RealtimeOntarioZonalPrice_2026112414.xml">...</a></td><td>24-Nov-2026 14:56</td></tr>
<tr><td><a href="PUB_RealtimeOntarioZonalPrice_2026112414_v1.xml">...</a></td><td>24-Nov-2026 14:02</td></tr>
<tr><td><a href="PUB_RealtimeOntarioZonalPrice_2026112414_v12.xml">...</a></td><td>24-Nov-2026 14:56</td></tr>
</table>
</body></html>
"""


def test_parse_directory_listing_skips_parent_and_dirs():
    files = parse_directory_listing(APACHE_LISTING_SAMPLE)
    assert "/public/" not in files
    assert "PUB_RealtimeOntarioZonalPrice.xml" in files
    assert "PUB_RealtimeOntarioZonalPrice_2026112414_v12.xml" in files
    assert len(files) == 4


@pytest.mark.parametrize(
    "filename,expected",
    [
        (
            "PUB_RealtimeOntarioZonalPrice_2026112414.xml",
            {"prefix": "PUB_RealtimeOntarioZonalPrice", "key": "2026112414", "version": None, "ext": "xml"},
        ),
        (
            "PUB_RealtimeOntarioZonalPrice_2026112414_v12.xml",
            {"prefix": "PUB_RealtimeOntarioZonalPrice", "key": "2026112414", "version": 12, "ext": "xml"},
        ),
        (
            "PUB_RealtimeOntarioZonalPrice.xml",
            {"prefix": "PUB_RealtimeOntarioZonalPrice", "key": None, "version": None, "ext": "xml"},
        ),
        (
            "PUB_Demand_2026.csv",
            {"prefix": "PUB_Demand", "key": "2026", "version": None, "ext": "csv"},
        ),
    ],
)
def test_parse_filename(filename, expected):
    assert parse_filename(filename) == expected


def test_parse_filename_rejects_unrelated_names():
    assert parse_filename("robots.txt") is None
    assert parse_filename("not_a_pub_file.xml") is None


def test_key_to_delivery_date_hour():
    result = key_to_delivery("date_hour", "2026112414")
    assert result == {"delivery_date": dt.date(2026, 11, 24), "delivery_hour": 14}


def test_key_to_delivery_date_only():
    result = key_to_delivery("date", "20260826")
    assert result == {"delivery_date": dt.date(2026, 8, 26), "delivery_hour": None}


def test_key_to_delivery_year():
    result = key_to_delivery("year", "2026")
    assert result == {"delivery_date": None, "delivery_hour": None, "year": 2026}


def test_key_to_delivery_rejects_bad_length():
    with pytest.raises(ValueError):
        key_to_delivery("date_hour", "202611")


def test_list_remote_files_filters_to_matching_prefix_and_skips_bare_rolling_file():
    files = list_remote_files("realtime_ontario_zonal_price", http_get=lambda url: APACHE_LISTING_SAMPLE)
    # the bare "PUB_RealtimeOntarioZonalPrice.xml" (no key) must be excluded
    assert all(f.key is not None for f in files)
    assert len(files) == 3
    versions = sorted((f.version for f in files), key=lambda v: (v is not None, v))
    assert versions == [None, 1, 12]


def test_list_remote_files_builds_correct_url():
    files = list_remote_files("demand", http_get=lambda url: "<html></html>")
    assert files == []  # empty listing -> empty result, no crash
