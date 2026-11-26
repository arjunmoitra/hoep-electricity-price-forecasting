"""
Pure-Python data quality checks for silver hourly price/demand series.

No Spark dependency so these run fast in CI against plain dicts/lists. The
Databricks silver job (merge.py) calls these against a collected/small slice
of each day's data and writes the result to `data_quality_log`.

Price bounds: Ontario's wholesale market under the Market Renewal Program has
a stated price floor/ceiling (commonly cited as roughly -$2,000 to $4,000/MWh
for Ontario zonal price, consistent with HOEP-era bounds carried into MRP).
*** This number is carried over from general market-design knowledge, not
re-confirmed against the current IESO market rules document for Phase 1 ***
-- treat DEFAULT_PRICE_BOUNDS as a sanity check that catches gross parsing
errors (e.g. a price off by 100x, or a null coerced to 0), not as a precise
regulatory constant. Tighten or loosen it once real data is flowing and you
can see the actual observed range.
"""
from __future__ import annotations

import dataclasses

DEFAULT_PRICE_BOUNDS = (-2000.0, 4000.0)
EXPECTED_HOURS = set(range(1, 25))  # IESO hour-ending convention, 1-24


@dataclasses.dataclass
class QualityReport:
    delivery_date: object
    rows_checked: int
    missing_hours: list[int]
    duplicate_hours: list[int]
    out_of_range_rows: list[dict]

    @property
    def is_clean(self) -> bool:
        return not (self.missing_hours or self.duplicate_hours or self.out_of_range_rows)


def check_day(
    rows: list[dict],
    *,
    delivery_date,
    hour_key: str = "delivery_hour",
    price_key: str = "ontario_zonal_price",
    price_bounds: tuple[float, float] = DEFAULT_PRICE_BOUNDS,
) -> QualityReport:
    """Run all three checks (missing/duplicate/impossible) for one delivery
    date's worth of rows. `rows` should already be filtered to that date.
    """
    hours = [r[hour_key] for r in rows if r.get(hour_key) is not None]

    seen = set()
    duplicates = []
    for h in hours:
        if h in seen:
            duplicates.append(h)
        seen.add(h)

    missing = sorted(EXPECTED_HOURS - seen)

    lo, hi = price_bounds
    out_of_range = [
        r
        for r in rows
        if r.get(price_key) is not None and not (lo <= r[price_key] <= hi)
    ]

    return QualityReport(
        delivery_date=delivery_date,
        rows_checked=len(rows),
        missing_hours=missing,
        duplicate_hours=sorted(set(duplicates)),
        out_of_range_rows=out_of_range,
    )


def group_by_date(rows: list[dict], date_key: str = "delivery_date") -> dict:
    out: dict = {}
    for r in rows:
        out.setdefault(r.get(date_key), []).append(r)
    return out


def check_all_days(
    rows: list[dict],
    *,
    hour_key: str = "delivery_hour",
    price_key: str = "ontario_zonal_price",
    date_key: str = "delivery_date",
    price_bounds: tuple[float, float] = DEFAULT_PRICE_BOUNDS,
) -> list[QualityReport]:
    grouped = group_by_date(rows, date_key=date_key)
    return [
        check_day(
            day_rows,
            delivery_date=date,
            hour_key=hour_key,
            price_key=price_key,
            price_bounds=price_bounds,
        )
        for date, day_rows in sorted(grouped.items(), key=lambda kv: (kv[0] is None, kv[0]))
    ]
