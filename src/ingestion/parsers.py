"""
Raw file content -> normalized row dicts, per IESO report type.

*** Schema verified against real downloaded files and IESO's own report help
files on 2026-11-26/28 (reports-public.ieso.ca/docrefs/helpfile/*.pdf). ***
Earlier drafts of this module guessed at field names; that guess was wrong in
several ways (see git history on this file for what changed). What's below is
sourced, not guessed:

  - `RealtimeOntarioZonalPrice_h1.pdf` and `PredispHourlyOntarioZonalPrice_h1.pdf`
    both define the report's price field with the same formula:
    "Zonal Price (Capped) = Energy Reference Price at Reference Location +
    Energy Loss Price + Energy Congestion Price". That is, the single field
    IESO calls "Zonal Price" is **already the complete, final price** --
    the formula describes how IESO computed it internally, not something we
    need to re-sum. The accompanying Energy Loss Price / Energy Congestion
    Price fields are a transparency breakdown, not additional components to
    add on top. Getting this backwards (summing all three) was the bug in
    the previous draft and would have silently doubled the loss/congestion
    contribution in every price -- exactly the kind of error Phase 2's
    honesty-about-evaluation goal exists to catch before it reaches a model.

  - The realtime report's actual XML tag for this field is `LmpCap` (not a
    literal "ZonalPrice" tag -- that name is used instead for the *container*
    element wrapping each 5-minute interval). The predispatch and day-ahead
    reports do use a literal `<ZonalPrice>` leaf tag. Three different XML
    shapes for conceptually the same "Zonal Price (Capped)" field -- confirmed
    directly from one real file per report, not inferred from naming
    consistency across reports.

  - The realtime report additionally provides a pre-computed `<AveragePrice>`
    block (LmpCap/LossPriceCap/CongPriceCap averaged over the hour's 12
    intervals) alongside the 12 raw `<ZonalPrice>` interval containers. We use
    AveragePrice.LmpCap directly as the hour's price rather than averaging the
    12 intervals ourselves, since IESO's own help file describes the hourly
    value as load-distribution-weighted, not a simple mean -- re-deriving it
    from the interval rows without the load weights would not reproduce it.
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


def _child_by_local_name(root: ET.Element, *candidates: str) -> ET.Element | None:
    """First direct-or-nested descendant of `root` (never `root` itself) whose
    tag local-name matches one of `candidates`.
    """
    lowered = {c.lower() for c in candidates}
    for el in root.iter():
        if el is root:
            continue
        if _local_name(el.tag).lower() in lowered:
            return el
    return None


def _children_by_local_name(root: ET.Element, *candidates: str) -> list[ET.Element]:
    lowered = {c.lower() for c in candidates}
    return [el for el in root if _local_name(el.tag).lower() in lowered]


def _text(el: ET.Element | None) -> str | None:
    if el is None or el.text is None:
        return None
    return el.text.strip()


def _float_text(el: ET.Element | None) -> float | None:
    t = _text(el)
    return float(t) if t is not None else None


def parse_realtime_zonal_price(xml_content: str, source_filename: str) -> list[dict]:
    """One PUB_RealtimeOntarioZonalPrice_<YYYYMMDDHH>[_vN].xml covers a single
    delivery hour. Returns exactly one row: the hour's load-weighted average
    price, taken directly from the file's <AveragePrice> block.
    """
    root = ET.fromstring(xml_content)  # noqa: S314 -- trusted first-party source

    delivery_date = _parse_date(_text(_child_by_local_name(root, "DeliveryDate")))
    hour_text = _text(_child_by_local_name(root, "DeliveryHour"))
    if delivery_date is None or hour_text is None:
        raise ParseError(f"Missing DeliveryDate/DeliveryHour in {source_filename}")

    avg_el = _child_by_local_name(root, "AveragePrice")
    if avg_el is None:
        raise ParseError(
            f"No <AveragePrice> block found in {source_filename} -- schema may "
            "have changed since this parser was verified (see module docstring)."
        )
    price = _float_text(_child_by_local_name(avg_el, "LmpCap"))
    if price is None:
        raise ParseError(f"<AveragePrice> in {source_filename} has no LmpCap value")

    return [
        {
            "delivery_date": delivery_date,
            "delivery_hour": int(hour_text),
            "ontario_zonal_price": price,
            "loss_price": _float_text(_child_by_local_name(avg_el, "LossPriceCap")),
            "cong_price": _float_text(_child_by_local_name(avg_el, "CongPriceCap")),
            "source_filename": source_filename,
        }
    ]


def _parse_hourly_components_report(
    xml_content: str, source_filename: str, price_field_name: str
) -> list[dict]:
    """Shared logic for Predispatch and Day-Ahead: both report 24
    <HourlyPriceComponents> entries per file, each with PricingHour,
    ZonalPrice, LossPriceCapped, CongestionPriceCapped (Day-Ahead also has a
    Flag we don't currently use).
    """
    root = ET.fromstring(xml_content)  # noqa: S314

    created_el = _child_by_local_name(root, "CreatedAt")
    published_at = _parse_datetime(_text(created_el))
    delivery_date = _parse_date(_text(_child_by_local_name(root, "DeliveryDate")))

    doc_body = _child_by_local_name(root, "DocBody")
    search_root = doc_body if doc_body is not None else root
    rows = []
    for comp_el in _children_by_local_name(search_root, "HourlyPriceComponents"):
        hour_text = _text(_child_by_local_name(comp_el, "PricingHour"))
        price = _float_text(_child_by_local_name(comp_el, "ZonalPrice"))
        if hour_text is None or price is None:
            continue
        rows.append(
            {
                "delivery_date": delivery_date,
                "delivery_hour": int(hour_text),
                price_field_name: price,
                "loss_price": _float_text(_child_by_local_name(comp_el, "LossPriceCapped")),
                "cong_price": _float_text(_child_by_local_name(comp_el, "CongestionPriceCapped")),
                "published_at": published_at,
                "source_filename": source_filename,
            }
        )
    if not rows:
        raise ParseError(
            f"No <HourlyPriceComponents> rows found in {source_filename} -- "
            "schema may have changed since this parser was verified."
        )
    return rows


def parse_predispatch_zonal_price(xml_content: str, source_filename: str) -> list[dict]:
    return _parse_hourly_components_report(
        xml_content, source_filename, "predispatch_ontario_zonal_price"
    )


def parse_dayahead_zonal_price(xml_content: str, source_filename: str) -> list[dict]:
    return _parse_hourly_components_report(
        xml_content, source_filename, "dayahead_ontario_zonal_price"
    )


def parse_demand_csv(csv_content: str, source_filename: str) -> list[dict]:
    """PUB_Demand_<YYYY>.csv. Confirmed directly (2026-11-26) against the live
    file: title line, 'Created at ...' line, blank line, then a header row
    reading exactly 'Date,Hour,Market Demand,Ontario Demand' followed by one
    row per hour. Matches the shape the original project's own ingestion
    notebook already parses successfully.
    """
    lines = csv_content.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        if line.strip().upper().startswith("DATE,HOUR"):
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
