"""
Pure-Python client for the IESO public reports site (reports-public.ieso.ca).

Deliberately has no Spark/Databricks dependency so it can be unit tested with
mocked HTTP responses in plain pytest, and imported unchanged by the
Databricks job entrypoint (bronze_job.py) that actually runs it.

Design notes (see PLAN.md section 2 for how these were determined):
  - Each report folder is an Apache-style autoindex directory listing.
  - Files are named `PUB_<ReportName>_<key>.xml` (or `.csv` for Demand),
    optionally with a `_vN` version suffix. A file with no suffix is the
    "final" value for that key; `_vN` files are the interim/revision history
    and are never overwritten once published.
  - The live site only retains a rolling window (90 days for Realtime,
    30 days for Predispatch per IESO's own Data Directory page), so bronze
    ingestion here is only ever "catch up on what's new since last run" --
    it is not meant to backfill full history.
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import re
import urllib.request
from html.parser import HTMLParser

IESO_BASE_URL = "https://reports-public.ieso.ca/public"

# One entry per report we ingest. `key_pattern` extracts the date/hour key
# IESO encodes in the filename; `key_format` says how to parse it into a
# (delivery_date, delivery_hour) pair. Demand is yearly-key, not date/hour.
REPORT_CONFIGS = {
    "realtime_ontario_zonal_price": {
        "folder": "RealtimeOntarioZonalPrice",
        "prefix": "PUB_RealtimeOntarioZonalPrice",
        "ext": "xml",
        "key_kind": "date_hour",  # YYYYMMDDHH, HH = 01-24
    },
    "predispatch_hourly_ontario_zonal_price": {
        "folder": "PredispHourlyOntarioZonalPrice",
        "prefix": "PUB_PredispHourlyOntarioZonalPrice",
        "ext": "xml",
        "key_kind": "date",  # YYYYMMDD, file itself contains all 24 hours
    },
    "dayahead_hourly_ontario_zonal_price": {
        "folder": "DAHourlyOntarioZonalPrice",
        "prefix": "PUB_DAHourlyOntarioZonalPrice",
        "ext": "xml",
        "key_kind": "date",
    },
    "demand": {
        "folder": "Demand",
        "prefix": "PUB_Demand",
        "ext": "csv",
        "key_kind": "year",  # YYYY, file is appended to all year
    },
}


@dataclasses.dataclass(frozen=True)
class RemoteFile:
    report: str
    filename: str
    url: str
    key: str  # the date/hour/year token IESO encodes in the name
    version: int | None  # None = unversioned ("final") file
    last_modified: dt.datetime | None  # from the directory listing, UTC-naive


class _DirectoryListingParser(HTMLParser):
    """Minimal parser for Apache's default autoindex HTML. We only need the
    href values; last-modified comes as the row's free text next to the
    link, which Apache does not mark up distinctly, so filenames are the
    reliable part and we parse timestamps defensively (None if unparsable).
    """

    def __init__(self) -> None:
        super().__init__()
        self.hrefs: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for name, value in attrs:
            if name.lower() == "href" and value:
                self.hrefs.append(value)


def parse_directory_listing(html: str) -> list[str]:
    """Return every filename linked from an Apache autoindex page, excluding
    the parent-directory link and non-file hrefs.
    """
    parser = _DirectoryListingParser()
    parser.feed(html)
    out = []
    for href in parser.hrefs:
        if href in ("../", "/"):
            continue
        if href.endswith("/"):
            continue
        out.append(href)
    return out


_FILENAME_RE = re.compile(
    r"^(?P<prefix>PUB_[A-Za-z]+)"
    r"(?:_(?P<key>\d+))?"
    r"(?:_v(?P<version>\d+))?"
    r"\.(?P<ext>xml|csv)$"
)


def parse_filename(filename: str) -> dict | None:
    """Break a PUB_* filename into prefix/key/version/ext. Returns None for
    filenames that don't match the expected pattern (defensive -- the
    directory listing can contain other report-unrelated noise in theory).
    """
    m = _FILENAME_RE.match(filename)
    if not m:
        return None
    d = m.groupdict()
    return {
        "prefix": d["prefix"],
        "key": d["key"],  # may be None for the bare "PUB_X.xml" rolling file
        "version": int(d["version"]) if d["version"] else None,
        "ext": d["ext"],
    }


def key_to_delivery(key_kind: str, key: str) -> dict:
    """Turn the filename key into delivery_date / delivery_hour fields.

    IESO hour-ending convention: HH runs 01-24, where hour 24 is 23:00-24:00
    of the given date (not hour 0 of the next day). We keep delivery_hour as
    the raw 1-24 integer and leave timezone/interval alignment to the silver
    layer, which is where the modeling-relevant "as-of" semantics live.
    """
    if key_kind == "date_hour":
        if len(key) != 10:
            raise ValueError(f"expected YYYYMMDDHH, got {key!r}")
        return {
            "delivery_date": dt.date(int(key[0:4]), int(key[4:6]), int(key[6:8])),
            "delivery_hour": int(key[8:10]),
        }
    if key_kind == "date":
        if len(key) != 8:
            raise ValueError(f"expected YYYYMMDD, got {key!r}")
        return {
            "delivery_date": dt.date(int(key[0:4]), int(key[4:6]), int(key[6:8])),
            "delivery_hour": None,
        }
    if key_kind == "year":
        return {"delivery_date": None, "delivery_hour": None, "year": int(key)}
    raise ValueError(f"unknown key_kind {key_kind!r}")


def list_remote_files(report: str, *, http_get=None) -> list[RemoteFile]:
    """List every file currently published for a report folder.

    `http_get` is injectable for tests; defaults to a real HTTPS GET. Must be
    a callable taking a URL and returning the response body as str.
    """
    cfg = REPORT_CONFIGS[report]
    url = f"{IESO_BASE_URL}/{cfg['folder']}/"
    html = (http_get or _real_http_get)(url)
    out = []
    for filename in parse_directory_listing(html):
        parsed = parse_filename(filename)
        if parsed is None or parsed["prefix"] != cfg["prefix"]:
            continue
        if parsed["key"] is None:
            # the bare "PUB_X.xml" rolling-latest file -- not useful for
            # bronze (no stable key to dedupe on), skip it.
            continue
        out.append(
            RemoteFile(
                report=report,
                filename=filename,
                url=f"{url}{filename}",
                key=parsed["key"],
                version=parsed["version"],
                last_modified=None,  # see docstring -- not reliably parseable
            )
        )
    return out


def fetch_file(url: str, *, http_get=None) -> str:
    return (http_get or _real_http_get)(url)


def _real_http_get(url: str, timeout: int = 30) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as resp:  # noqa: S310
        return resp.read().decode("utf-8", errors="replace")
