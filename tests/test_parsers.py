"""
These tests check that the parser code is internally consistent (round-trips
a fixture built to match its own assumed schema) and handles malformed input
safely. They do NOT prove the parser matches IESO's real XML schema -- see the
big warning at the top of src/ingestion/parsers.py. Treat a pass here as
"the code doesn't have bugs given its assumptions", not "the assumptions are
correct".
"""
import datetime as dt

import pytest

from src.ingestion.parsers import (
    ParseError,
    parse_dayahead_zonal_price,
    parse_demand_csv,
    parse_predispatch_zonal_price,
    parse_realtime_zonal_price,
)

REALTIME_FIXTURE = """<?xml version="1.0"?>
<Document>
  <DocBody>
    <DeliveryDate>2026-11-24</DeliveryDate>
    <DeliveryHour>14</DeliveryHour>
    <Zone>
      <ZoneName>Ontario</ZoneName>
      <Interval>1</Interval>
      <EnergyPrice>23.45</EnergyPrice>
    </Zone>
    <Zone>
      <ZoneName>Ontario</ZoneName>
      <Interval>2</Interval>
      <EnergyPrice>24.10</EnergyPrice>
    </Zone>
    <Zone>
      <ZoneName>Manitoba</ZoneName>
      <Interval>1</Interval>
      <EnergyPrice>19.00</EnergyPrice>
    </Zone>
  </DocBody>
</Document>
"""

PREDISPATCH_FIXTURE = """<?xml version="1.0"?>
<Document>
  <DocHeader>
    <CreatedAt>2026-11-11T07:13:00</CreatedAt>
  </DocHeader>
  <DocBody>
    <DeliveryDate>2026-11-11</DeliveryDate>
    <HourlyPrice>
      <ZoneName>Ontario</ZoneName>
      <DeliveryHour>1</DeliveryHour>
      <EnergyPrice>30.5</EnergyPrice>
    </HourlyPrice>
    <HourlyPrice>
      <ZoneName>Ontario</ZoneName>
      <DeliveryHour>2</DeliveryHour>
      <EnergyPrice>28.2</EnergyPrice>
    </HourlyPrice>
  </DocBody>
</Document>
"""

DAYAHEAD_FIXTURE = PREDISPATCH_FIXTURE  # same assumed shape

DEMAND_CSV_FIXTURE = """Hourly Demand Report
Created at 2026-11-24
For 2026-11-24

Date,Hour,Market Demand,Ontario Demand
2026-11-24,1,15234,14890
2026-11-24,2,14900,14500
"""


def test_parse_realtime_zonal_price_keeps_only_ontario():
    rows = parse_realtime_zonal_price(REALTIME_FIXTURE, "PUB_RealtimeOntarioZonalPrice_2026112414.xml")
    assert len(rows) == 2
    assert all(r["delivery_date"] == dt.date(2026, 11, 24) for r in rows)
    assert all(r["delivery_hour"] == 14 for r in rows)
    prices = sorted(r["ontario_zonal_price"] for r in rows)
    assert prices == [23.45, 24.10]


def test_parse_realtime_zonal_price_raises_on_empty():
    with pytest.raises(ParseError):
        parse_realtime_zonal_price("<Document><DocBody></DocBody></Document>", "bad.xml")


def test_parse_predispatch_zonal_price():
    rows = parse_predispatch_zonal_price(
        PREDISPATCH_FIXTURE, "PUB_PredispHourlyOntarioZonalPrice_20261111_v12.xml"
    )
    assert len(rows) == 2
    assert rows[0]["published_at"] == dt.datetime(2026, 11, 11, 7, 13, 0)
    assert rows[0]["delivery_date"] == dt.date(2026, 11, 11)
    hours = {r["delivery_hour"]: r["predispatch_ontario_zonal_price"] for r in rows}
    assert hours == {1: 30.5, 2: 28.2}


def test_parse_dayahead_zonal_price():
    rows = parse_dayahead_zonal_price(DAYAHEAD_FIXTURE, "PUB_DAHourlyOntarioZonalPrice_20261111.xml")
    assert len(rows) == 2


def test_parse_demand_csv():
    rows = parse_demand_csv(DEMAND_CSV_FIXTURE, "PUB_Demand_2026.csv")
    assert len(rows) == 2
    assert rows[0]["delivery_date"] == dt.date(2026, 11, 24)
    assert rows[0]["delivery_hour"] == 1
    assert rows[0]["market_demand"] == 15234.0
    assert rows[0]["ontario_demand"] == 14890.0


def test_parse_demand_csv_raises_without_header():
    with pytest.raises(ParseError):
        parse_demand_csv("just,some,junk\n1,2,3\n", "bad.csv")


def test_parse_demand_csv_skips_trailing_malformed_rows():
    content = DEMAND_CSV_FIXTURE + "\n,,,\n"
    rows = parse_demand_csv(content, "PUB_Demand_2026.csv")
    assert len(rows) == 2  # trailing blank row silently skipped, not a crash
