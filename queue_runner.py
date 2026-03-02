#!/usr/bin/env python3
"""
DealLedger Queue Runner
========================
Pulls jobs from scrape_jobs, executes them, records results.
Multi-worker safe via Postgres FOR UPDATE SKIP LOCKED.

Run:
  python3 queue_runner.py                     # run forever
  python3 queue_runner.py --once              # one batch then exit
  python3 queue_runner.py --workers 4         # threaded (4 workers)

Env vars:
  SUPABASE_URL, SUPABASE_KEY
  QUEUE_BATCH_SIZE     (default 20)
  QUEUE_POLL_SECONDS   (default 10)
  QUEUE_LOCK_MINUTES   (default 10)
  AUTO_REDISCOVER_AFTER_FAILURES (default 2)
  WORKER_ID            (default: hostname-random)
"""

import os
import sys
import json
import time
import uuid
import socket
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

from supabase import create_client

# ── Import your existing runners ────────────────────────────────────────────
from discovery_v2 import discover as run_discovery
from scraper_agent_v5 import scrape_url as run_scrape

# ── Config ───────────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

BATCH_SIZE   = int(os.environ.get("QUEUE_BATCH_SIZE", "20"))
POLL_SECONDS = int(os.environ.get("QUEUE_POLL_SECONDS", "10"))
LOCK_MINUTES = int(os.environ.get("QUEUE_LOCK_MINUTES", "10"))
AUTO_REDISCOVER_AFTER_FAILURES = int(os.environ.get("AUTO_REDISCOVER_AFTER_FAILURES", "2"))

# Exponential backoff in minutes: attempt 1→2min, 2→5min, 3→15min, 4→1hr, 5→4hr, 6+→daily
BACKOFF_MINUTES = [2, 5, 15, 60, 240, 1440]


# ── Supabase ─────────────────────────────────────────────────────────────────
def get_sb():
    if not (SUPABASE_URL and SUPABASE_KEY):
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY env vars")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def get_worker_id():
    return os.environ.get("WORKER_ID") or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"


# ── Queue operations ──────────────────────────────────────────────────────────
def claim_jobs(sb, worker_id: str, limit: int) -> list:
    res = sb.rpc("claim_scrape_jobs", {
        "p_worker_id": worker_id,
        "p_limit": limit,
        "p_lock_minutes": LOCK_MINUTES
    }).execute()
    return res.data or []


def mark_done(sb, job_id: int):
    sb.table("scrape_jobs").update({
        "status": "done",
        "locked_by": None,
        "lock_expires_at": None,
    }).eq("id", job_id).execute()


def mark_failed(sb, job: dict, error_code: str, error_msg: str, http_status: int = None):
    attempts = int(job.get("attempts") or 0) + 1
    max_attempts = int(job.get("max_attempts") or 5)

    if attempts >= max_attempts:
        status = "failed"
        run_after = job.get("run_after")
    else:
        status = "queued"
        idx = max(0, min(attempts - 1, len(BACKOFF_MINUTES) - 1))
        run_after = (datetime.now(timezone.utc) + timedelta(minutes=BACKOFF_MINUTES[idx])).isoformat()

    sb.table("scrape_jobs").update({
        "status": status,
        "attempts": attempts,
        "run_after": run_after,
        "last_error_code": (error_code or "")[:80],
        "last_error": (error_msg or "")[:500],
        "last_http_status": http_status,
        "locked_by": None,
        "lock_expires_at": None,
    }).eq("id", job["id"]).execute()


def enqueue_job(sb, domain: str, url: str, job_type="scrape", priority=100,
                run_after=None, max_attempts=5) -> bool:
    try:
        sb.table("scrape_jobs").insert({
            "domain": domain,
            "url": url,
            "job_type": job_type,
            "priority": priority,
            "status": "queued",
            "attempts": 0,
            "max_attempts": max_attempts,
            "run_after": run_after or now_iso(),
            "scheduled_at": now_iso(),
        }).execute()
        return True
    except Exception:
        return False  # likely duplicate due to unique_active index


def log_event(sb, job_id: int, domain: str, job_type: str,
              status: str, message: str, meta: dict = None):
    try:
        sb.table("scrape_job_events").insert({
            "job_id": job_id,
            "domain": domain,
            "job_type": job_type,
            "status": status,
            "message": message,
            "meta": meta or {},
        }).execute()
    except Exception:
        pass


# ── Health tracking ───────────────────────────────────────────────────────────
def bump_health(sb, domain: str, success: bool, error_code: str = None):
    try:
        rows = sb.table("broker_discovery").select(
            "domain, failure_streak, success_streak"
        ).eq("domain", domain).execute().data

        cur = rows[0] if rows else {}
        fs = int(cur.get("failure_streak") or 0)
        ss = int(cur.get("success_streak") or 0)

        if success:
            upd = {
                "last_success_at": now_iso(),
                "failure_streak": 0,
                "success_streak": ss + 1,
                "last_error": None,
            }
        else:
            upd = {
                "last_error_at": now_iso(),
                "failure_streak": fs + 1,
                "success_streak": 0,
                "last_error": (error_code or "error")[:120],
            }

        sb.table("broker_discovery").update(upd).eq("domain", domain).execute()
        return fs + (0 if success else 1)  # return new failure streak

    except Exception:
        return 0


# ── Job runner ────────────────────────────────────────────────────────────────
def run_job(sb, job: dict):
    job_id   = job["id"]
    domain   = job["domain"]
    url      = job.get("url")
    job_type = job.get("job_type", "scrape")

    log_event(sb, job_id, domain, job_type, "running", "Started", {"url": url})

    try:
        # ── Discovery / rediscovery ──────────────────────────────────────────
        if job_type in ("discover", "rediscover", "playwright_capture"):
            if not url:
                raise RuntimeError("missing_url")

            result = run_discovery(url, verbose=False)
            status = result.get("status")

            if status == "ok":
                mark_done(sb, job_id)
                log_event(sb, job_id, domain, job_type, "done", "Discovery ok",
                          {"method": result.get("method"), "sample_count": result.get("sample_count")})
                bump_health(sb, domain, success=True)
                # Auto-enqueue scrape after successful discovery
                enqueue_job(sb, domain, url, job_type="scrape", priority=100)
                return

            # Discovery returned non-ok (js_only, failed, dead, etc.)
            mark_failed(sb, job,
                        error_code=status or "discover_failed",
                        error_msg=json.dumps(result)[:400])
            log_event(sb, job_id, domain, job_type, "failed", "Discovery failed",
                      {"status": status, "method": result.get("method")})
            bump_health(sb, domain, success=False, error_code=status)
            return

        # ── Scrape ───────────────────────────────────────────────────────────
        if not url:
            raise RuntimeError("missing_url")

        result = run_scrape(url, verbose=False)

        if result.get("success"):
            mark_done(sb, job_id)
            log_event(sb, job_id, domain, job_type, "done", "Scrape ok",
                      {"count": result.get("count"), "method": result.get("method")})
            bump_health(sb, domain, success=True)
            return

        # Scrape failed
        err = result.get("error", "scrape_failed")
        mark_failed(sb, job, error_code=str(err)[:80], error_msg=str(err))
        log_event(sb, job_id, domain, job_type, "failed", "Scrape failed",
                  {"error": err, "method": result.get("method")})

        new_fs = bump_health(sb, domain, success=False, error_code=str(err))

        # Auto-rediscover after N consecutive failures
        if new_fs >= AUTO_REDISCOVER_AFTER_FAILURES:
            enqueued = enqueue_job(sb, domain, url, job_type="rediscover",
                                   priority=50, max_attempts=3)
            if enqueued:
                log_event(sb, job_id, domain, job_type, "info",
                          f"Auto-rediscover enqueued after {new_fs} failures")

    except Exception as e:
        mark_failed(sb, job, error_code="exception", error_msg=str(e)[:500])
        log_event(sb, job_id, domain, job_type, "failed", "Exception",
                  {"error": str(e)[:300]})
        bump_health(sb, domain, success=False, error_code="exception")


# ── Main loop ─────────────────────────────────────────────────────────────────
def run_loop(sb, worker_id: str, batch_size: int, run_once: bool, num_workers: int):
    print(f"[queue_runner] worker={worker_id} batch={batch_size} poll={POLL_SECONDS}s "
          f"threads={num_workers}")

    while True:
        jobs = claim_jobs(sb, worker_id, batch_size)

        if not jobs:
            if run_once:
                print("[queue_runner] No jobs. Exiting (--once).")
                break
            time.sleep(POLL_SECONDS)
            continue

        print(f"[queue_runner] Claimed {len(jobs)} jobs")

        if num_workers > 1:
            with ThreadPoolExecutor(max_workers=num_workers) as ex:
                futures = {ex.submit(run_job, sb, job): job for job in jobs}
                for future in as_completed(futures):
                    job = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        print(f"  [ERROR] {job['domain']}: {e}")
        else:
            for job in jobs:
                run_job(sb, job)

        if run_once:
            break


def main():
    parser = argparse.ArgumentParser(description="DealLedger Queue Runner")
    parser.add_argument("--once", action="store_true",
                        help="Process one batch then exit")
    parser.add_argument("--workers", type=int, default=1,
                        help="Number of parallel threads (default: 1)")
    parser.add_argument("--batch", type=int, default=BATCH_SIZE,
                        help=f"Jobs per batch (default: {BATCH_SIZE})")
    args = parser.parse_args()

    sb = get_sb()
    wid = get_worker_id()

    run_loop(sb, wid,
             batch_size=args.batch,
             run_once=args.once,
             num_workers=args.workers)


if __name__ == "__main__":
    main()
