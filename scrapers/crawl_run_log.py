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
) -> None:
    """Close a run row.

    status MUST be 'ok' on success — v_dom_direct filters on exactly that
    string. Use 'failed' for an exception or a crawl that returned nothing
    (error='EMPTY'). Allowed values are _VALID_STATUS; there is no 'empty'.
    """
    key = _key()
    if run_id is None or not key:
        return
    if status == "ok":
        status, error = _truncation_check(run_id, int(listings_seen or 0), key, error)
    if status not in _VALID_STATUS:
        # crawl_run_status_check rejects anything else, and a rejected PATCH
        # leaves the row 'running' forever (96 zombie rows by 2026-09-24,
        # all from status='empty'). Coerce rather than lose the close.
        log.warning("crawl_run: status %r not allowed, closing as 'failed'", status)
        error = error or f"status:{status}"
        status = "failed"
    try:
        r = requests.patch(
            f"{_url()}/rest/v1/crawl_run",
            headers=_headers(key),
            params={"id": f"eq.{run_id}"},
            json={
                "status": status,
                "finished_at": "now()",
                "urls_fetched": int(urls_fetched or 0),
                "listings_seen": int(listings_seen or 0),
                "error": (error or None) and str(error)[:500],
            },
            timeout=15,
        )
        if not r.ok:
            log.warning("crawl_run finish HTTP %s for id=%s: %s",
                        r.status_code, run_id, r.text[:200])
    except Exception as e:                                  # never raise
        log.warning("crawl_run finish failed: %s", e)


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
