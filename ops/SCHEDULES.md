# Schedules — the one list

Rule 1: **Supabase pg_cron is the only clock.** Every scheduled GitHub workflow is started by a pg_cron `gh-*` job via `dispatch_workflow()`. GitHub's own `schedule:` lines are backups only.
Rule 2: **The Mac runs only `data/mac_only_brokers.txt`** (brokers that fail from GitHub), via `run_specialized_daily.sh` from the Mac crontab at 08:00 ET, with `pmset` waking the Mac at 07:55. `FULL=1 bash run_specialized_daily.sh` runs every specialized broker (catch-up only).
Rule 3: **Nothing leaves the Mac until it has run ok from GitHub two days in a row.** Check `crawl_run` for the domain before deleting a line from the list or a cron entry.
Rule 4: **Every change is watched.** When a scraper patch lands, add its domains to `private.change_watch` (domain, change_ref, landed_at). Health alerts go CRITICAL if a watched domain has no ok crawl 30 h after landing.

## Mac (crontab on Jeff's Mac)
```
0 8 * * * /bin/bash /Users/jeffsosville/dealledger-repo/run_specialized_daily.sh >> /Users/jeffsosville/dealledger-repo/logs/cron.log 2>&1
```
Wake: `sudo pmset repeat wakeorpoweron MTWRFSU 07:55:00`. Verify: `crontab -l`, `pmset -g sched`.
Lost once already: crontab was empty from ~Nov 18 to Nov 21 2026; Sunbelt (825 listings) went 3 days uncrawled.

## pg_cron (UTC) — Supabase `kqckuedsyyosmccushyd`
| Job | Schedule | What |
|---|---|---|
| gh-broker-discovery / -pm | 06:23 / 18:23 | discovery workflow |
| gh-specialized-scrape | 07:41 | specialized_scrape.yml (→ stale_check.yml on completion) |
| gh-sheet-sync | 09:47 | sheet_sync.yml |
| gh-broker-scrape | 10:23 | broker_scrape.yml (V6, 4 shards) |
| coverage_snapshot_daily | 11:15 | coverage_snapshots |
| recompute-days-on-market | 14:40 | |
| refresh-listing-dates | 14:50 | |
| removal-rule-daily | 14:55 | absence-based removals |
| bridge-direct-daily | 15:00 | listings_direct → listings |
| snapshot-dom-daily | 15:10 | |
| detect-price-changes | */15 14–21 | detector v7 |
| refresh-homepage-after-bridge | 15:25 | |
| refresh-homepage-v2 | 15:32 | |
| refresh-broker-registry | 15:35 | |
| refresh_seo_listings | 15:45 | |
| gh-export-public / health-alerts-daily | 15:55 | export + health_alerts.yml |
| gh-daily-report | 16:07 | daily_report.yml |
| judge-broker-trials | 21:05 | |
| sync-blocked-brokers | 21:11 | |
| promote-producing-brokers | 21:17 | |
| expire-stale-crawl-runs | :37 hourly | |

## Health alerts (`public.health_alerts_report()`, daily → GitHub issue `health-alert`, run fails on CRITICAL = email)
- CRITICAL: pipeline missing, cron failed, broker not crawled ok within 36 h (specialized) / 9 d (V6) **that was ok in the last 7 days** (a regression), Mac-only broker not crawled in 30 h, watched change not verified.
- WARN: drops, second schedule failing, stuck runs, blocked domain crawled.
- BACKLOG: brokers with no ok crawl for 7+ days. The conductor's work queue — not an alarm.

## The daily report (Nov 21)
One report, posted as the `Today — date` issue at 16:07 UTC (pg_cron `gh-daily-report`), assigned to Jeff:
1. What broke (CRITICAL health alerts, marked new / open since)
2. Fixed and verified (change_watch verified + brokers back to OK)
3. Core numbers vs yesterday / 7 days (brokers OK 7 d, brokers whose count matches, listings on site, price changes 7 d) + fix-queue size
4. Needs your decision (`private.decisions_pending`, open rows)

Built by `public.daily_ops_report()`; history in `private.daily_metrics`. Anything that needs Jeff
(approval, credentials, a business call) goes in as a row in `private.decisions_pending`; his answer
closes it (`closed_on`, `answer`). Nothing else interrupts him. The hand-written daily triage is retired.

## Claude Code job runner (Nov 22)
Mac crontab: `30 8 * * * /bin/zsh -lc "$HOME/dealledger-repo/ops/nightly_conductor.sh"` — after the 8:00 scraper run.
Takes the top line of `claude/staging/queue.txt`, runs it headless (Opus conductor, Sonnet workers),
Supabase keys blanked, git commit/push blocked. Writes `claude/staging/<date>/SUMMARY_<job>.md`; the job
leaves the queue only when that summary exists. Log: `logs/conductor_<date>.log`. Jeff reviews and pushes;
chat applies SQL and inserts change_watch rows.
