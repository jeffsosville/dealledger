"""
optional_columns.py — one mechanism for listings_direct columns a writer sends
only with evidence, and that may not exist yet (2026-11-12).

Two writers fill listings_direct: dealledger_scraper_v6.SupabaseWriter.upsert
and run_specialized.upsert_listings. W14 (price_not_disclosed /
broker_price_text) introduced the pattern; j3 (broker_status_text /
broker_status_seen_at) is the second user. Both column groups go through the
code below, so the two writers handle every optional column the same way
(principle 15) instead of growing one retry path per feature:

  * evidence-only: a field builder (price_disclosure.disclosure_fields,
    status_label_fields below) returns the fields to send, or None. None means
    the keys are NOT put on the row at all, so PostgREST's merge-duplicates
    leaves an existing value alone (principle 14: a row that simply has no
    label must never overwrite a stored label with null).
  * uniform batches: PostgREST rejects a POST whose objects don't share one key
    set, so rows are grouped by their key set (group_by_keys) before chunking.
  * column not there yet: a migration may land after the code. PostgREST says
    400 PGRST204 "Could not find the 'x' column of 'listings_direct' in the
    schema cache"; Postgres says 42703 column "x" ... does not exist. The writer
    finds the group that error names (missing_group), strips that group's keys,
    logs ONCE per process, retries, and keeps counting what it could not write
    (principle 17: a drop is reported, never silent).
"""
from __future__ import annotations

import re
from typing import Iterable, Optional


class OptionalColumns:
    """A named group of optional listings_direct columns plus the per-process
    memory 'these columns are not there yet'. One instance per group per
    writer module; the writer looks its instances up at call time."""

    def __init__(self, keys: Iterable[str], migration: str = ""):
        self.keys = tuple(keys)
        self.migration = migration
        self.missing = False
        self.unwritten = 0          # rows whose fields could not be written

    @property
    def name(self) -> str:
        return "/".join(self.keys)

    def matches(self, err) -> bool:
        """True when a PostgREST/Postgres error says one of OUR keys is not a
        column (an unrelated PGRST204 about another column is not ours)."""
        s = str(err or "")
        if not any(k in s for k in self.keys):
            return False
        return ("PGRST204" in s or "42703" in s
                or "schema cache" in s or "does not exist" in s)

    def strip(self, rows: list[dict]) -> int:
        """Remove this group's keys in place; returns how many rows carried any."""
        n = 0
        for r in rows:
            had = False
            for k in self.keys:
                if k in r:
                    r.pop(k)
                    had = True
            n += had
        return n

    def strip_if_missing(self, rows: list[dict]) -> int:
        """Before a write: once the columns are known missing, stop sending
        them, but count the rows that carried them."""
        if not self.missing:
            return 0
        n = self.strip(rows)
        self.unwritten += n
        return n

    def note_missing(self, log, rows_affected: int) -> None:
        self.unwritten += rows_affected
        if not self.missing:
            self.missing = True
            where = f" ({self.migration} not applied)" if self.migration else ""
            log(f"listings_direct has no {self.name} column yet{where} - writing rows "
                f"without them; count of rows that carried them is reported at the end of the run")

    def summary(self) -> Optional[str]:
        if self.missing and self.unwritten:
            return f"{self.name} not written for {self.unwritten} row(s) so far: column missing"
        return None


def missing_group(err, groups: Iterable[OptionalColumns]) -> Optional[OptionalColumns]:
    """The first not-yet-missing group whose column the error names, or None
    (None = a real error: the caller handles it as before)."""
    for g in groups:
        if not g.missing and g.matches(err):
            return g
    return None


def group_by_keys(rows: list[dict]) -> list[list[dict]]:
    """PostgREST requires every object in one POST to have the same keys. Rows
    that carry optional fields and rows that don't go in separate batches."""
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        groups.setdefault(tuple(sorted(r.keys())), []).append(r)
    return list(groups.values())


# ── broker status label (j3, decision 6) ─────────────────────────────────────
# listing_status.apply_status (and the per-path readers built on it: W4
# WP-REST, W11 businessusa, W12 ninbb, W15 healthcarebizsales, j3 FacetWP,
# j3 V6 generic cards) put the broker's own words on the row as
# `broker_status_text` ("Under Contract", "Sale Pending", "SOLD!"). This is
# what reaches listings_direct.broker_status_text, with the time it was read.

STATUS_LABEL_KEYS = ("broker_status_text", "broker_status_seen_at")
MAX_STATUS_LABEL = 120   # matches the column's CHECK (j3-broker_status_text.sql)


def status_label_fields(row: dict, now: str) -> Optional[dict]:
    """The two status-label columns to send for this scraped row, or None to
    send neither. Evidence = a non-empty label on the row; whitespace is
    collapsed, case and punctuation kept, cut to MAX_STATUS_LABEL. No label ->
    None: the stored label (and its seen_at) stays as it is, so a reader can
    tell a label not re-read on the latest crawl by broker_status_seen_at <
    last_seen."""
    t = row.get("broker_status_text")
    if not isinstance(t, str):
        return None
    t = re.sub(r"\s+", " ", t.replace("\xa0", " ")).strip()[:MAX_STATUS_LABEL].strip()
    if not t:
        return None
    return {"broker_status_text": t, "broker_status_seen_at": now}


def new_status_label_columns() -> OptionalColumns:
    return OptionalColumns(STATUS_LABEL_KEYS, "j3-broker_status_text.sql")
