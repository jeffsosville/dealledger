"""
Per-broker crawl run logging -> public.crawl_run

Why this exists
---------------
v_dom_direct computes days-on-market as a RANGE (dom_days_min / dom_days_max)
and grades each listing observed / bounded / unknown. It does that by asking:
"when did we last successfully crawl this broker BEFORE this listing appeared?"

That answer lives in public.crawl_run. The table currently holds ONE row (a
synthetic backfill marker), so the lateral join finds nothing and every one of
the 27,667 active listings grades 'unknown'. The view is correct; it is starving.

Write one row per broker per crawl and the view starts working.

Two rules, both learned the hard way in this repo:
  1. Write INLINE, per broker. dealledger_scraper_v6.py:3133 documents runs that
     were killed by CI before the end-of-run crawl_failures flush ("none recorded
     12-16 Sep"). An end-of-run batch for crawl_run would lose the same days.
  2. NEVER let logging break a scrape. Every call here swallows its own errors.

Table is `crawl_run` (SINGULAR). There is also an orphaned `crawl_runs` table
with zero rows — writing there does nothing and the view will not see it.
"""

import os
import logging

import requests

log = logging.getLogger(__name__)

def _url() -> str:
    return os.environ.get("SUPABASE_URL", "https://kqckuedsyyosmccushyd.supabase.co")


def _key() -> str:
    """Read at CALL time, not import time.

    dealledger_scraper_v6.py calls load_dotenv() *after* its import block, so a
    module-level read here would capture an empty key and silently log nothing.
    The three scrapers also disagree on the variable name — accept either.
    """
    return (
        os.environ.get("SUPABASE_SERVICE_KEY")
        or os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        or ""
    )


# Must match crawl_run_status_check.
_VALID_STATUS = {"running", "ok", "failed", "partial", "backfill"}


def _headers(key: str) -> dict:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }


def start_run(source: str, broker_domain: str | None) -> int | None:
    """Open a run row. Returns its id, or None if logging is unavailable.

    `source` identifies the scraper: 'v6', 'specialized', 'v7'.
    Status starts as 'running'; finish_run() closes it.
    A row left at 'running' means the process died — that is useful signal,
    not a bug, and v_dom_direct ignores it because it filters status='ok'.
    """
    key = _key()
    if not key:
        return None
    try:
        r = requests.post(
            f"{_url()}/rest/v1/crawl_run",
            headers={**_headers(key), "Prefer": "return=representation"},
            json={
                "source": source,
                "broker_domain": broker_domain,
                "status": "running",
            },
            timeout=15,
        )
        if r.ok and r.json():
            return r.json()[0]["id"]
        log.warning("crawl_run start HTTP %s: %s", r.status_code, r.text[:200])
    except Exception as e:                                  # never raise
        log.warning("crawl_run start failed: %s", e)
    return None


def finish_run(
    run_id: int | None,
    status: str = "ok",
    urls_fetched: int = 0,
    listings_seen: int = 0,
    error: str | None = None,
    displayed_count: int | None = None,
    displayed_source: str | None = None,
    displayed_note: str | None = None,
    compared_count: int | None = None,
) -> None:
    """Close a run row.

    status MUST be 'ok' on success — v_dom_direct filters on exactly that
    string. Use 'failed' for an exception or a crawl that returned nothing
    (error='EMPTY'). Allowed values are _VALID_STATUS; there is no 'empty'.

    displayed_count / displayed_source ('stated' | 'counted') / displayed_note:
    the broker's own count of its for-sale listings (scrapers/displayed_count.py).
    compared_count is what gets compared to it; default listings_seen (the
    specialized runner passes the active-status count, since its listings_seen
    is rows upserted, sold ones included). An 'ok' crawl outside 90-110% of a
    STATED count closes 'partial' (an allowed status the absence rule and
    v_dom_direct already ignore); count_complete records the verdict either way.
    """
    key = _key()
    if run_id is None or not key:
        return
    if status == "ok":
        status, error = _truncation_check(run_id, int(listings_seen or 0), key, error)
    compared = int(listings_seen or 0) if compared_count is None else int(compared_count)
    status, error, count_complete = _count_check(
        status, error, compared, displayed_count, displayed_source)
    if status not in _VALID_STATUS:
        # crawl_run_status_check rejects anything else, and a rejected PATCH
        # leaves the row 'running' forever (96 zombie rows by 2026-09-24,
        # all from status='empty'). Coerce rather than lose the close.
        log.warning("crawl_run: status %r not allowed, closing as 'failed'", status)
        error = error or f"status:{status}"
        status = "failed"
    body = {
        "status": status,
        "finished_at": "now()",
        "urls_fetched": int(urls_fetched or 0),
        "listings_seen": int(listings_seen or 0),
        "error": (error or None) and str(error)[:500],
    }
    extra = _displayed_fields(displayed_count, displayed_source,
                              displayed_note, count_complete)
    try:
        r = requests.patch(
            f"{_url()}/rest/v1/crawl_run",
            headers=_headers(key),
            params={"id": f"eq.{run_id}"},
            json={**body, **extra},
            timeout=15,
        )
        if not r.ok and extra:
            # The displayed-count columns may not exist yet (the SQL is a
            # separate, later step). A rejected PATCH would leave the row
            # 'running' forever (principle 17), so close it without them
            # and say so in the log.
            log.warning("crawl_run finish HTTP %s with displayed-count fields for "
                        "id=%s (%s); retrying without them",
                        r.status_code, run_id, r.text[:200])
            r = requests.patch(
                f"{_url()}/rest/v1/crawl_run",
                headers=_headers(key),
                params={"id": f"eq.{run_id}"},
                json=body,
                timeout=15,
            )
        if not r.ok:
            log.warning("crawl_run finish HTTP %s for id=%s: %s",
                        r.status_code, run_id, r.text[:200])
    except Exception as e:                                  # never raise
        log.warning("crawl_run finish failed: %s", e)


# Displayed-count completeness (2026-11-11, owner spec).
# commercialroutesforsale closed 'ok' seeing 30 of 61, vikingmergers 411 of 62,
# peakbusinessbrokersllc 1 of 33. A crawl is complete only when what we saw is
# within 90-110% of the broker's own count. Outside that, a STATED count closes
# the run 'partial' (in crawl_run_status_check; absence_candidates() requires
# 'ok', so a partial run is never read as "the whole index was seen"). A
# COUNTED figure (our heuristic) only sets count_complete=false — see
# displayed_count.COUNTED_MARKS_PARTIAL.
_DISPLAYED_SOURCES = {"stated", "counted"}


def _count_check(status: str, error: str | None, compared: int,
                 displayed: int | None, source: str | None
                 ) -> tuple[str, str | None, bool | None]:
    """Return (status, error, count_complete). Pure; never raises."""
    try:
        from displayed_count import count_verdict, COUNTED_MARKS_PARTIAL
    except Exception:                                       # never raise
        return status, error, None
    if source not in _DISPLAYED_SOURCES:
        return status, error, None
    complete = count_verdict(compared, displayed)
    if complete is False and status == "ok" and (
            source == "stated" or COUNTED_MARKS_PARTIAL):
        log.warning("crawl_run: saw %d vs broker-displayed %s (%s), closing as partial",
                    compared, displayed, source)
        return "partial", f"COUNT_MISMATCH: saw {compared}, broker displays {displayed} ({source})", False
    return status, error, complete


def _displayed_fields(displayed: int | None, source: str | None,
                      note: str | None, complete: bool | None) -> dict:
    """crawl_run columns from sql/2026-11-11_crawl_run_displayed_count.sql.
    Empty when there is nothing to record, so the PATCH is unchanged."""
    if displayed is None or source not in _DISPLAYED_SOURCES:
        return {}
    return {
        "broker_displayed_count": int(displayed),
        "broker_displayed_count_source": source,
        "broker_displayed_note": (note or None) and str(note)[:500],
        "count_complete": complete,
    }


# Truncation guard (2026-10-26, Jeff approved).
# reputedbrokerage closed two runs as 'ok' after seeing 2 listings, down from
# 333. An 'ok' run is read as "the broker's full index was seen" by
# v_dom_direct, v_stale_check_queue and public_stats, so a run that sees a
# small fraction of the recent normal count closes as 'partial' instead.
# 'partial' is an allowed status and none of those views treat it as coverage.
TRUNCATION_RATIO = 0.20     # below 20% of the recent normal count
TRUNCATION_MIN_BASELINE = 20  # ignore brokers that normally list fewer
TRUNCATION_LOOKBACK = "14 days"


def _norm_domain(d: str | None) -> str:
    d = (d or "").strip().lower()
    return d[4:] if d.startswith("www.") else d


def is_truncated(listings_seen: int, recent_ok_counts: list[int]) -> tuple[bool, int]:
    """Pure check. Baseline = max of the last 3 'ok' runs' listings_seen."""
    recent = [int(c or 0) for c in recent_ok_counts[:3]]
    baseline = max(recent) if recent else 0
    if baseline < TRUNCATION_MIN_BASELINE:
        return False, baseline
    return listings_seen < TRUNCATION_RATIO * baseline, baseline


def _truncation_check(run_id: int, listings_seen: int, key: str,
                      error: str | None) -> tuple[str, str | None]:
    """Return ('partial', reason) for a truncated crawl, else ('ok', error).
    Any lookup failure keeps 'ok' (the old behaviour). Never raises."""
    try:
        h = _headers(key)
        r = requests.get(f"{_url()}/rest/v1/crawl_run", headers=h,
                         params={"id": f"eq.{run_id}", "select": "broker_domain"},
                         timeout=15)
        rows = r.json() if r.ok else []
        dom = _norm_domain(rows[0].get("broker_domain") if rows else None)
        if not dom:
            return "ok", error
        r = requests.get(
            f"{_url()}/rest/v1/crawl_run", headers=h,
            params={
                "select": "listings_seen",
                "status": "eq.ok",
                "id": f"neq.{run_id}",
                "broker_domain": f"in.({dom},www.{dom})",
                "started_at": f"gt.{_lookback_iso()}",
                "order": "started_at.desc",
                "limit": "3",
            },
            timeout=15,
        )
        if not r.ok:
            return "ok", error
        counts = [row.get("listings_seen") or 0 for row in r.json()]
        truncated, baseline = is_truncated(listings_seen, counts)
        if truncated:
            log.warning("crawl_run %s: %s saw %d vs recent %d, closing as partial",
                        run_id, dom, listings_seen, baseline)
            return "partial", f"TRUNCATED: saw {listings_seen}, recent ok max {baseline}"
    except Exception as e:                                  # never raise
        log.warning("crawl_run truncation check failed: %s", e)
    return "ok", error


def _lookback_iso() -> str:
    from datetime import datetime, timedelta, timezone
    days = int(TRUNCATION_LOOKBACK.split()[0])
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def expire_stale_runs(max_age: str = "6 hours") -> int:
    """Mark crawl_run rows still 'running' after `max_age` as failed/timeout.

    Calls public.expire_stale_crawl_runs() (pg_cron also runs it hourly).
    Returns the number of rows closed; 0 on any error. Never raises.
    """
    key = _key()
    if not key:
        return 0
    try:
        r = requests.post(
            f"{_url()}/rest/v1/rpc/expire_stale_crawl_runs",
            headers=_headers(key),
            json={"max_age": max_age},
            timeout=30,
        )
        if r.ok:
            return int(r.json() or 0)
        log.warning("expire_stale_crawl_runs HTTP %s: %s", r.status_code, r.text[:200])
    except Exception as e:                                  # never raise
        log.warning("expire_stale_crawl_runs failed: %s", e)
    return 0


# Per-domain rows for multi-domain brokers (2026-11-12).
# A specialized run opens ONE crawl_run row under its registry domain
# (BROKERS[...]["domain"]), but some brokers' listings sit on other hosts:
# Bodner (registry bodnergroup.com) writes every row under execbb.com, VR
# (vrbusinessbrokers.com) under ~30 office domains. v_dom_direct, absence
# candidates and v_listing_dates all join crawl_run on broker_domain, so
# those domains had no 'ok' crawl since 2026-09-24 (ff046fe switched the
# row to the registry domain and dropped the per-listing domain; the "one
# extra ok row per additional domain" CLAUDE.md describes was never built).
#
# Each extra row is written CLOSED in a single insert (status, finished_at,
# listings_seen in the same POST), so a failure can never leave it 'running'.
# The registry domain is never given a second row. The registry row keeps the
# whole-run total, so the extra rows are a per-domain breakdown of it: do not
# sum listings_seen across one run's rows.
def domain_counts_excluding(domain_counts: dict, registry_domain: str | None) -> dict:
    """Pure. {bare domain: count} for every domain other than the registry
    domain, www./case variants merged, zero counts dropped."""
    reg = _norm_domain(registry_domain)
    out: dict = {}
    for d, n in (domain_counts or {}).items():
        dom = _norm_domain(d)
        if not dom or dom == reg or int(n or 0) <= 0:
            continue
        out[dom] = out.get(dom, 0) + int(n)
    return out


def _recent_ok_counts(dom: str, key: str) -> list[int] | None:
    """listings_seen of the last 3 'ok' runs for dom; None if unreadable."""
    try:
        r = requests.get(
            f"{_url()}/rest/v1/crawl_run", headers=_headers(key),
            params={
                "select": "listings_seen",
                "status": "eq.ok",
                "broker_domain": f"in.({dom},www.{dom})",
                "started_at": f"gt.{_lookback_iso()}",
                "order": "started_at.desc",
                "limit": "3",
            },
            timeout=15,
        )
        if r.ok:
            return [row.get("listings_seen") or 0 for row in r.json()]
    except Exception as e:                                  # never raise
        log.warning("crawl_run recent counts for %s failed: %s", dom, e)
    return None


def record_domain_run(source: str, broker_domain: str, listings_seen: int,
                      urls_fetched: int | None = None) -> int | None:
    """Insert one already-closed crawl_run row for broker_domain.

    'ok', or 'partial' when the truncation rule says so. Returns the new id,
    or None (logged at ERROR) if the insert was refused or raised. Never raises.
    """
    from datetime import datetime, timezone
    key = _key()
    if not key:
        return None
    dom = _norm_domain(broker_domain)
    seen = int(listings_seen or 0)
    status, error = "ok", None
    recent = _recent_ok_counts(dom, key)
    if recent is not None:
        truncated, baseline = is_truncated(seen, recent)
        if truncated:
            log.warning("crawl_run: %s saw %d vs recent %d, recording as partial",
                        dom, seen, baseline)
            status, error = "partial", f"TRUNCATED: saw {seen}, recent ok max {baseline}"
    body = {
        "source": source,
        "broker_domain": dom,
        "status": status,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "urls_fetched": int(seen if urls_fetched is None else urls_fetched),
        "listings_seen": seen,
        "error": error,
    }
    try:
        r = requests.post(
            f"{_url()}/rest/v1/crawl_run",
            headers={**_headers(key), "Prefer": "return=representation"},
            json=body,
            timeout=15,
        )
        rows = r.json() if r.ok else None
        if rows and rows[0].get("id") is not None:
            return rows[0]["id"]
        log.error("crawl_run domain row NOT written for %s (%d listings): HTTP %s %s",
                  dom, seen, r.status_code, r.text[:200])
    except Exception as e:                                  # never raise
        log.error("crawl_run domain row NOT written for %s (%d listings): %s",
                  dom, seen, e)
    return None


def record_domain_runs(source: str, registry_domain: str | None,
                       domain_counts: dict) -> tuple[dict, list[str]]:
    """One closed row per non-registry domain in domain_counts.
    Returns ({domain: id}, [domains whose row failed]). Never raises."""
    written, failed = {}, []
    for dom, n in sorted(domain_counts_excluding(domain_counts, registry_domain).items()):
        rid = record_domain_run(source, dom, n)
        if rid is None:
            failed.append(dom)
        else:
            written[dom] = rid
    return written, failed
