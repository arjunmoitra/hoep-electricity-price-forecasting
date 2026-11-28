"""
Fixtures below are built from the real XML structure confirmed against live
IESO files on 2026-11-26/28 (see src/ingestion/parsers.py docstring for the
sourced field definitions). Values are made up for readability but the
element names and nesting are the real ones.
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
<Document docID="RealtimeOntarioZonalPrice">
  <DocHeader>
    <DocTitle>Realtime 5-min Ontario Zonal Energy Price Report</DocTitle>
    <DocRevision>1</DocRevision>
    <CreatedAt>2026-11-24T14:56:00</CreatedAt>
  </DocHeader>
  <DocBody>
    <DeliveryDate>2026-11-24</DeliveryDate>
    <DeliveryHour>14</DeliveryHour>
    <ZonalPrice>
      <Interval>1</Interval>
      <Flag>DSO-RD</Flag>
      <LmpCap>54.95</LmpCap>
      <LossPriceCap>0.08</LossPriceCap>
      <CongPriceCap>0.00</CongPriceCap>
    </ZonalPrice>
    <ZonalPrice>
      <Interval>2</Interval>
      <Flag>DSO-RD</Flag>
      <LmpCap>42.15</LmpCap>
      <LossPriceCap>0.06</LossPriceCap>
      <CongPriceCap>0.00</CongPriceCap>
    </ZonalPrice>
    <AveragePrice>
      <LmpCap>49.33</LmpCap>
      <LossPriceCap>0.07</LossPriceCap>
      <CongPriceCap>0.00</CongPriceCap>
    </AveragePrice>
  </DocBody>
</Document>
"""

HOURLY_COMPONENTS_FIXTURE = """<?xml version="1.0"?>
<Document docID="PredispHourlyOntarioZonalPrice">
  <DocHeader>
    <DocTitle>Predispatch Hourly Ontario Zonal Energy Price Report</DocTitle>
    <DocRevision>1</DocRevision>
    <CreatedAt>2026-11-11T07:13:00</CreatedAt>
  </DocHeader>
  <DocBody>
    <DeliveryDate>2026-11-11</DeliveryDate>
    <HourlyPriceComponents>
      <PricingHour>1</PricingHour>
      <ZonalPrice>38.01</ZonalPrice>
      <LossPriceCapped>0.25</LossPriceCapped>
      <CongestionPriceCapped>2.13</CongestionPriceCapped>
    </HourlyPriceComponents>
    <HourlyPriceComponents>
      <PricingHour>2</PricingHour>
      <ZonalPrice>24.23</ZonalPrice>
      <LossPriceCapped>0.26</LossPriceCapped>
      <CongestionPriceCapped>0.63</CongestionPriceCapped>
    </HourlyPriceComponents>
  </DocBody>
</Document>
"""

DAYAHEAD_FIXTURE = HOURLY_COMPONENTS_FIXTURE.replace(
    "Predispatch Hourly Ontario Zonal Energy Price Report",
    "Day-Ahead Hourly Ontario Zonal Energy Price Report",
).replace(
    "</HourlyPriceComponents>\n  </DocBody>",
    "  <Flag>DSO-RD</Flag>\n    </HourlyPriceComponents>\n  </DocBody>",
)

DEMAND_CSV_FIXTURE = """Hourly Demand Report
Created at 2026-11-24
For 2026-11-24

Date,Hour,Market Demand,Ontario Demand
2026-11-24,1,15234,14890
2026-11-24,2,14900,14500
"""


def test_parse_realtime_zonal_price_uses_average_block():
    rows = parse_realtime_zonal_price(REALTIME_FIXTURE, "PUB_RealtimeOntarioZonalPrice_2026112414.xml")
    assert len(rows) == 1
    row = rows[0]
    assert row["delivery_date"] == dt.date(2026, 11, 24)
    assert row["delivery_hour"] == 14
    # Must come from <AveragePrice>, NOT a sum of the per-interval LmpCap values
    # (49.33 != 54.95 + 42.15, and must not equal LmpCap+LossPriceCap+CongPriceCap either)
    assert row["ontario_zonal_price"] == 49.33
    assert row["loss_price"] == 0.07
    assert row["cong_price"] == 0.00


def test_parse_realtime_zonal_price_raises_without_average_block():
    broken = REALTIME_FIXTURE.replace("AveragePrice", "SomethingElse")
    with pytest.raises(ParseError):
        parse_realtime_zonal_price(broken, "bad.xml")


def test_parse_predispatch_zonal_price_does_not_sum_components():
    rows = parse_predispatch_zonal_price(
        HOURLY_COMPONENTS_FIXTURE, "PUB_PredispHourlyOntarioZonalPrice_20261111_v12.xml"
    )
    assert len(rows) == 2
    assert rows[0]["published_at"] == dt.datetime(2026, 11, 11, 7, 13, 0)
    assert rows[0]["delivery_date"] == dt.date(2026, 11, 11)
    # ZonalPrice is already the final capped price -- must NOT be
    # 38.01 + 0.25 + 2.13. Getting this right is the whole point of the
    # schema-verification fix (see module docstring).
    assert rows[0]["predispatch_ontario_zonal_price"] == 38.01
    assert rows[0]["loss_price"] == 0.25
    assert rows[0]["cong_price"] == 2.13
    assert rows[1]["predispatch_ontario_zonal_price"] == 24.23


def test_parse_dayahead_zonal_price():
    rows = parse_dayahead_zonal_price(DAYAHEAD_FIXTURE, "PUB_DAHourlyOntarioZonalPrice_20261111.xml")
    assert len(rows) == 2
    assert rows[0]["dayahead_ontario_zonal_price"] == 38.01


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
