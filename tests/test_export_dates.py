"""Public export: dates come from listing_dates, one row per listing."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
from export_public_listings import apply_dates  # noqa: E402

REC_A = {"listing_key": "a.com|u:a.com/1", "row_ids": ["r1", "r2"], "first_seen": "2026-05-01",
         "last_seen": "2026-10-08", "listed_on_basis": "observed", "listed_after": "2026-04-28"}


def rows():
    return [
        {"id": "r1", "first_seen": "2026-06-01T10:00:00+00:00", "last_seen": "2026-10-08T10:00:00+00:00"},
        {"id": "r2", "first_seen": "2026-09-01T10:00:00+00:00", "last_seen": "2026-10-08T10:00:00+00:00"},
        {"id": "r3", "first_seen": "2026-10-09T10:00:00+00:00", "last_seen": "2026-10-09T11:00:00+00:00"},
    ]


def test_dates_from_listing_dates_and_duplicates_merged():
    out = apply_dates(rows(), {"r1": REC_A, "r2": REC_A})
    assert [r["id"] for r in out] == ["r1", "r3"]
    assert out[0]["first_seen"] == "2026-05-01"
    assert out[0]["listed_on_basis"] == "observed"
    assert out[0]["listed_after"] == "2026-04-28"


def test_row_without_dates_record_is_floor():
    out = apply_dates(rows(), {"r1": REC_A, "r2": REC_A})
    new = out[1]
    assert new["listed_on_basis"] == "floor"
    assert new["listed_after"] is None
    assert new["first_seen"] == "2026-10-09"
