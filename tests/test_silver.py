import datetime as dt

from src.silver.dedupe import pick_latest_per_key
from src.silver.quality import check_all_days, check_day

D = dt.date(2026, 11, 24)


def test_pick_latest_per_key_prefers_highest_version():
    rows = [
        {"delivery_date": D, "delivery_hour": 1, "version": None, "price": 10.0},
        {"delivery_date": D, "delivery_hour": 1, "version": 1, "price": 11.0},
        {"delivery_date": D, "delivery_hour": 1, "version": 12, "price": 12.0},
        {"delivery_date": D, "delivery_hour": 2, "version": 3, "price": 20.0},
    ]
    result = pick_latest_per_key(rows, ("delivery_date", "delivery_hour"))
    by_hour = {(r["delivery_date"], r["delivery_hour"]): r["price"] for r in result}
    assert by_hour[(D, 1)] == 12.0
    assert by_hour[(D, 2)] == 20.0
    assert len(result) == 2


def test_pick_latest_per_key_is_idempotent_on_rerun():
    rows = [{"delivery_date": D, "delivery_hour": 1, "version": 5, "price": 1.0}]
    once = pick_latest_per_key(rows, ("delivery_date", "delivery_hour"))
    twice = pick_latest_per_key(once + once, ("delivery_date", "delivery_hour"))
    assert len(twice) == 1


def _full_day_rows(missing=(), duplicate=(), prices=None):
    rows = []
    prices = prices or {}
    for h in range(1, 25):
        if h in missing:
            continue
        rows.append({"delivery_date": D, "delivery_hour": h, "ontario_zonal_price": prices.get(h, 20.0)})
        if h in duplicate:
            rows.append({"delivery_date": D, "delivery_hour": h, "ontario_zonal_price": prices.get(h, 20.0)})
    return rows


def test_check_day_clean():
    report = check_day(_full_day_rows(), delivery_date=D)
    assert report.is_clean
    assert report.rows_checked == 24


def test_check_day_detects_missing_hours():
    report = check_day(_full_day_rows(missing=(5, 17)), delivery_date=D)
    assert report.missing_hours == [5, 17]
    assert not report.is_clean


def test_check_day_detects_duplicate_hours():
    report = check_day(_full_day_rows(duplicate=(3,)), delivery_date=D)
    assert report.duplicate_hours == [3]
    assert not report.is_clean


def test_check_day_detects_impossible_prices():
    report = check_day(_full_day_rows(prices={9: 99999.0, 10: -99999.0}), delivery_date=D)
    assert len(report.out_of_range_rows) == 2
    assert not report.is_clean


def test_check_all_days_groups_correctly():
    d2 = dt.date(2026, 11, 25)
    rows = _full_day_rows() + [
        {"delivery_date": d2, "delivery_hour": 1, "ontario_zonal_price": 15.0}
    ]
    reports = check_all_days(rows)
    assert len(reports) == 2
    dates = {r.delivery_date for r in reports}
    assert dates == {D, d2}
