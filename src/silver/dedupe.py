"""
Pure-Python "pick the row to keep" logic for collapsing multiple bronze
versions of the same (delivery_date, delivery_hour) down to one silver row.

This is the testable contract for what the Spark MERGE in merge.py is supposed
to do (there via a window function ordered the same way) -- kept here as plain
Python so the actual tie-breaking rule is covered by fast unit tests instead of
only being implicit in a block of Spark SQL.
"""
from __future__ import annotations


def pick_latest_per_key(
    rows: list[dict],
    key_fields: tuple[str, ...],
    *,
    version_field: str = "version",
) -> list[dict]:
    """Keep exactly one row per distinct value of `key_fields`: the one with
    the highest `version_field`, treating a missing/None version as lower
    than any numbered version (None = the oldest style of file, superseded by
    any versioned revision; IESO's own files are never un-versioned then
    re-versioned, so this ordering matches how revisions actually arrive).
    """
    best: dict[tuple, dict] = {}
    for row in rows:
        key = tuple(row[f] for f in key_fields)
        version = row.get(version_field)
        current = best.get(key)
        if current is None:
            best[key] = row
            continue
        current_version = current.get(version_field)
        if _rank(version) > _rank(current_version):
            best[key] = row
    return list(best.values())


def _rank(version) -> int:
    return -1 if version is None else version
