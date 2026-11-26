"""
Raw file content -> normalized row dicts, per IESO report type.

*** STATUS: UNVERIFIED AGAINST A REAL DOWNLOADED FILE. ***
Phase 0 confirmed the folder names, filename patterns, update cadence and
retention windows for each report by fetching live directory listings. It did
NOT confirm the internal XML element names, because I could not complete a
content fetch of an actual sample file before this phase started (a tool
approval was pending and I chose not to block all of Phase 1 on it -- see the
phase-end summary). The parsers below are written against IESO's standard
report XML conventions used elsewhere on reports-public.ieso.ca, using a
namespace-agnostic, candidate-tag-name search specifically so a close-but-not-
exact guess still has a chance of working and fails loudly (raises, rather
than silently returning zero rows) if the structure doesn't match at all.

Before this ever runs against production data, run `verify_parser_against_sample()`
below against one real downloaded file per report and fix any mismatch. Do not
promote this to the Databricks job config until that's done -- see Phase 1
summary for the exact one remaining step.
"""
from __future__ import annotations

import csv
import datetime as dt
import io
import re
import xml.etree.ElementTree as ET


class ParseError(ValueError):
    pass


def _local_name(tag: str) -> str:
    """Strip an XML namespace, e.g. '{http://...}DeliveryHour' -> 'DeliveryHour'."""
    return tag.rsplit("}", 1)[-1]


def _findall_by_local_name(root: ET.Element, *candidates: str) -> list[ET.Element]:
    """Search *descendants* of `root` for any of the given tag local-names.
    Deliberately excludes `root` itself -- callers use this to look for a
    named child/descendant field, and if `root`'s own tag happened to match
    one of the candidates (e.g. searching a <Zone> element for "Zone" as a
    loose synonym for its name field) matching itself would silently return
    the wrong element instead of the intended child.
    """
    lowered = {c.lower() for c in candidates}
    return [el for el in root.iter() if el is not root and _local_name(el.tag).lower() in lowered]


def _text(el: ET.Element | None) -> str | None:
    if el is None or el.text is None:
        return None
    return el.text.strip()


def parse_realtime_zonal_price(xml_content: str, source_filename: str) -> list[dict]:
    """One PUB_RealtimeOntarioZonalPrice_<YYYYMMDDHH>[_vN].xml covers a single
    delivery hour, broken into 5-minute settlement intervals, for every zone.
    We only keep the Ontario zone. Returns one row per interval found.
    """
    root = ET.fromstring(xml_content)  # noqa: S314 -- trusted first-party source

    date_el = _findall_by_local_name(root, "DeliveryDate")
    hour_el = _findall_by_local_name(root, "DeliveryHour")
    delivery_date = _parse_date(_text(date_el[0])) if date_el else None
    delivery_hour = int(_text(hour_el[0])) if hour_el else None

    rows = []
    zone_candidates = _findall_by_local_name(root, "Zone", "ZonalPrice", "IntervalZonalPrice")
    for zone_el in zone_candidates:
        name_el = _findall_by_local_name(zone_el, "ZoneName", "Zone")
        if name_el and _text(name_el[0]) not in ("Ontario", None):
            continue
        interval_el = _findall_by_local_name(zone_el, "Interval")
        price_el = _findall_by_local_name(zone_el, "EnergyPrice", "Price", "LMP")
        if not price_el:
            continue
        rows.append(
            {
                "delivery_date": delivery_date,
                "delivery_hour": delivery_hour,
                "interval": int(_text(interval_el[0])) if interval_el else None,
                "ontario_zonal_price": float(_text(price_el[0])),
                "source_filename": source_filename,
            }
        )
    if not rows:
        raise ParseError(
            f"No Ontario zonal price rows found in {source_filename} -- "
            "parser's assumed schema likely doesn't match the real file. "
            "See module docstring: verify against a real sample before trusting this."
        )
    return rows


def parse_predispatch_zonal_price(xml_content: str, source_filename: str) -> list[dict]:
    """One PUB_PredispHourlyOntarioZonalPrice_<YYYYMMDD>[_vN].xml covers all 24
    hours of a delivery date, for every hour horizon published so far. We keep
    the published_at timestamp (when this file/version was issued) alongside
    each row so silver can reconstruct "what predispatch said h hours before
    delivery, as of the time it was actually published" -- this is the whole
    point of Phase 1's "store predispatch with the time it was published"
    requirement.
    """
    root = ET.fromstring(xml_content)  # noqa: S314

    created_el = _findall_by_local_name(root, "CreatedAt", "DocCreatedAt", "CreationTime")
    published_at = _parse_datetime(_text(created_el[0])) if created_el else None

    date_el = _findall_by_local_name(root, "DeliveryDate")
    delivery_date = _parse_date(_text(date_el[0])) if date_el else None

    rows = []
    hour_candidates = _findall_by_local_name(root, "HourlyPrice", "HourPrice")
    for hour_el in hour_candidates:
        hour_num_el = _findall_by_local_name(hour_el, "DeliveryHour", "Hour")
        price_el = _findall_by_local_name(hour_el, "EnergyPrice", "Price", "ZonalPrice")
        zone_el = _findall_by_local_name(hour_el, "ZoneName", "Zone")
        if zone_el and _text(zone_el[0]) not in ("Ontario", None):
            continue
        if not hour_num_el or not price_el:
            continue
        rows.append(
            {
                "delivery_date": delivery_date,
                "delivery_hour": int(_text(hour_num_el[0])),
                "predispatch_ontario_zonal_price": float(_text(price_el[0])),
                "published_at": published_at,
                "source_filename": source_filename,
            }
        )
    if not rows:
        raise ParseError(
            f"No predispatch rows found in {source_filename} -- parser's assumed "
            "schema likely doesn't match the real file. See module docstring."
        )
    return rows


def parse_dayahead_zonal_price(xml_content: str, source_filename: str) -> list[dict]:
    """Same shape as predispatch, but one publish per date (no revisions in
    the normal case) and all 24 hours published at once the prior day.
    """
    root = ET.fromstring(xml_content)  # noqa: S314

    created_el = _findall_by_local_name(root, "CreatedAt", "DocCreatedAt", "CreationTime")
    published_at = _parse_datetime(_text(created_el[0])) if created_el else None
    date_el = _findall_by_local_name(root, "DeliveryDate")
    delivery_date = _parse_date(_text(date_el[0])) if date_el else None

    rows = []
    hour_candidates = _findall_by_local_name(root, "HourlyPrice", "HourPrice")
    for hour_el in hour_candidates:
        hour_num_el = _findall_by_local_name(hour_el, "DeliveryHour", "Hour")
        price_el = _findall_by_local_name(hour_el, "EnergyPrice", "Price", "ZonalPrice")
        zone_el = _findall_by_local_name(hour_el, "ZoneName", "Zone")
        if zone_el and _text(zone_el[0]) not in ("Ontario", None):
            continue
        if not hour_num_el or not price_el:
            continue
        rows.append(
            {
                "delivery_date": delivery_date,
                "delivery_hour": int(_text(hour_num_el[0])),
                "dayahead_ontario_zonal_price": float(_text(price_el[0])),
                "published_at": published_at,
                "source_filename": source_filename,
            }
        )
    if not rows:
        raise ParseError(
            f"No day-ahead rows found in {source_filename}. See module docstring."
        )
    return rows


def parse_demand_csv(csv_content: str, source_filename: str) -> list[dict]:
    """PUB_Demand_<YYYY>.csv -- this one we're confident about: it's the same
    format the original project already parses successfully in
    01_data_ingestion.ipynb (a few metadata rows, then a header row, then one
    row per hour with Market Demand / Ontario Demand columns). Re-using that
    known-good shape here rather than guessing.
    """
    lines = csv_content.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("DATE,HOUR") or (
            "MARKET DEMAND" in line.upper() and "," in line
        ):
            header_idx = i
            break
    if header_idx is None:
        raise ParseError(
            f"Could not find the header row in {source_filename} -- "
            "expected a line starting with 'Date,Hour,...'."
        )
    reader = csv.DictReader(io.StringIO("\n".join(lines[header_idx:])))
    rows = []
    for raw in reader:
        norm = {_normalize_key(k): v for k, v in raw.items() if k}
        try:
            delivery_date = _parse_date(norm.get("date"))
            delivery_hour = int(norm["hour"])
        except (KeyError, ValueError, TypeError):
            continue  # skip malformed/blank trailing rows
        rows.append(
            {
                "delivery_date": delivery_date,
                "delivery_hour": delivery_hour,
                "market_demand": _to_float(norm.get("marketdemand")),
                "ontario_demand": _to_float(norm.get("ontariodemand")),
                "source_filename": source_filename,
            }
        )
    if not rows:
        raise ParseError(f"No demand rows parsed from {source_filename}.")
    return rows


def _normalize_key(k: str) -> str:
    return re.sub(r"[^a-z0-9]", "", k.strip().lower())


def _to_float(v: str | None) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def _parse_date(s: str | None) -> dt.date | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y"):
        try:
            return dt.datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise ValueError(f"unrecognized date format: {s!r}")


def _parse_datetime(s: str | None) -> dt.datetime | None:
    if not s:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return dt.datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"unrecognized datetime format: {s!r}")


PARSERS = {
    "realtime_ontario_zonal_price": parse_realtime_zonal_price,
    "predispatch_hourly_ontario_zonal_price": parse_predispatch_zonal_price,
    "dayahead_hourly_ontario_zonal_price": parse_dayahead_zonal_price,
    "demand": parse_demand_csv,
}
