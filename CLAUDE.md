# DealLedger — Mission & Operating Context

**Read this first, every session.**
Last updated: 1 September 2026 (evening) · Target: 1 September 2027

---

## The goal, stated plainly

**In twelve months, DealLedger holds every business listed for sale in the United States, and can compare any of them against BizBuySell and BizQuest dynamically.**

Not a sample. Not an aggregation of aggregators. The primary index — sourced directly from the brokers who list the businesses, refreshed daily, with provenance on every row.

Everything else follows from that. The vertical marketplaces, the brokerage, the valuation tools, the outbound — all of them are worth more when the data underneath is the one nobody else has.

### Why this is winnable

BizBuySell and BizQuest are the same company (CoStar) sharing one sequential listing-number pool. They are an advertising business: sellers pay to be seen, and the platform has no incentive to tell you a listing has been sitting for 400 days or has been relisted four times. That asymmetry is the whole opening.

We can compute what they won't publish — true days on market, relist history, view velocity, and whether a listing is real at all. Our data is CC0 and free. Theirs is a paywall around a number they'd rather you didn't check.

### What "taking over the ecosystem" actually means

Not displacing them. Becoming the layer everyone checks first — the reference the trade press cites, the number a broker quotes to a seller, the source an SBA lender pulls before underwriting. Then the transactions follow the trust, not the other way around.

---

## Where we actually are

Be honest in every session. Overstating readiness produces bad decisions.

| | 1 Sep 2026 | Needed |
|---|---|---|
| Broker-direct listings (active) | ~28,000 | Every active US listing |
| Brokers producing | 155 of 740 crawled | 740, plus the long tail beyond |
| Brokers known but never crawled | **1,171** (~10,000 listings) | Zero |
| Untagged by vertical | ~21,800 | Near zero, continuously |
| DOM methodology | Anchored, piecewise, published | Same, validated against closed sales |
| BBS/BQ comparison | Manual, single-listing lookup | Dynamic, whole-index |

**Two gaps, and the second is the surprise.**

BizBuySell's index is ~133,000. Direct broker coverage is ~28,000 active. `NO_PATTERN` on roughly half of successful fetches is the biggest lever on depth.

But `broker_master` holds ~1,505 distinct broker domains and only ~334 appear in `listings_direct`. **1,171 brokers we already know about have never been crawled**, carrying roughly 10,000 active listings. Discovery was never the bottleneck — consumption was. `v_broker_crawl_candidates` is the queue; `agents/discover_backlog.py` works it.

---

## Distribution: Twitter and SEO. Nothing else.

**Twitter only.** Not LinkedIn, not Instagram, not TikTok, not YouTube. The SMB acquisition world lives on Twitter and nowhere else. An SMB attorney built one of the fastest-growing firms in the country on it, with a running Pizza Friday bit as the hook — the consistency and the recognizable ritual mattered as much as the substance.

What works there:
- A specific, checkable number nobody else has
- The same thing, repeatedly, until it's associated with us
- Being useful before being promotional
- Findings that correct a widely held assumption

What does not:
- Announcements about ourselves
- Threads that could have been one sentence
- Anything that reads like marketing

**SEO is the second channel, and it is seller-intent, not geo.** We do not have the inventory to win "cleaning business for sale in Ohio." We can own "what is my business worth" — because we can answer it with real comps and nobody else will.

**The recurring publishable finding:** businesses that actually sell sit ~103 days. Advertised listings average 17. The market looks three times faster than it is, because the slow ones accumulate invisibly. That correction is the kind of thing trade press picks up, and it's ours.

---

## Operating principles

These are not abstractions. Every one of them came from a specific failure.

### 1. Silent failure is the enemy, not visible failure

The vertical classifier stopped writing on 15 July 2026. Nothing noticed for six weeks. Jobs exited 0. Builds went green. Pages rendered. By the time anyone looked, 21,871 rows were untagged and VendingExits was showing 25 listings out of a far larger real pool.

**Every job asserts something. A job that only reports success reports nothing.**

The check is almost always two numbers that should match: rows inserted vs rows tagged, brokers in the list vs brokers producing, distinct titles vs total rows.

### 2. Verify the whole chain, not the first link

The ATMExits sell form saved to the database and sent a magic link, so it looked like it worked. It never called the endpoint that notifies anyone. Two real seller leads sat unread — one a 375-machine route at $145K/month — because the notification didn't fail, it never existed.

**"The row saved" is not "the workflow completed."**

### 3. Check the call path before theorizing

On 27 August, an hour went into DNS records, then a suppressed mailbox, then an API key — before anyone checked whether the endpoint was called at all. It wasn't. One line in the log answered what three hypotheses hadn't.

**Read the log. Grep the caller. Then theorize.**

### 4. Never put mutable data in an identity hash

Row IDs were `sha256(title | asking_price | url)`. A price that parsed differently between runs produced a new ID, so the upsert never matched. 316 real listings became 11,243 rows in one night.

**Identity is what a thing *is*, not what it currently costs.**

### 5. Title and category are signal. Description is noise.

A backfill matched on description and tagged an amusement park as vending, because its listing mentioned vending machines. That one listing then rendered 42 times on VendingExits.

**Related: "route" alone means nothing.** Bread routes, FedEx routes, and vending routes are three different businesses. No keyword list separates them. That is what the model is for.

### 6. Nothing gets deleted

A Supabase project that looked abandoned held the entire subscriber list. A "dead" marketplace app turned out to be running in production with real seller submissions in it.

**Archive, deactivate, flag. Never delete.** The cost of keeping something useless is near zero. The cost of deleting something load-bearing is not.

### 7. More rows is not more data

Every scraper failure so far has been the same shape wearing a new disguise:

- aria.net wrote paginated index URLs as listings — 343 rows for 3 listings
- quietlight got a different id each run because the id hashed `asking_price` — 316 listings became 11,243 rows
- quietlight again, where pagination never advanced and the same page counted as new 50 times — 11,440 cards
- a nav menu scored better than a listing grid, because menus repeat cleanly and listings do not

Each was fixed specifically. The general rule is the one that matters: **a broker that suddenly yields ten times its usual count has broken, not grown.** Check yield against history before writing, not after.

### 8. Test a new filter against known-good input, not just the bad case

Twice in one day a guard written to reject junk rejected real listings instead. A chrome-detection rule matched `elementor-widget-container` and `card-header`, which wrap genuine listing cards on WordPress and Bootstrap — NO_PATTERN went from 49% to 78% in a single run. A stricter page validator rejected every JS-rendered site, including franchises with 100+ listings.

**A filter's false-reject rate is invisible in a way its false-accept rate is not.** Nothing looks wrong; there is simply less. Every new filter gets tested against input that must pass.

### 9. Ask the site before guessing at it

Path-guessing tried 21 URLs against a domain before ever loading its homepage. That is slow, it reads as a scan, and Cloudflare starts returning 403 partway through — after which every probe fails for reasons unrelated to whether the page exists. 18 of 25 brokers reported "no listings page" that way.

Reading the homepage nav found `sunbeltmidwest.com/buy-a-business/search-businesses-for-sale` on the first try. **The site will tell you where its listings are.**

### 10. Crawl what works before crawling what doesn't

Ordering the daily run by staleness put 155 never-scraped brokers at the front of every batch, so each run spent itself on the cohort that has never once succeeded while 40,000 known-good listings went unrefreshed. Sort by proven yield; sweep the dark ones as a separate job with a different success bar.

### 11. A source that goes quiet is the failure nobody sees

tworld.com sat at ~977 rows from 16 July until 2 September, then jumped to 4,535 in a single run. Nothing was broken — the specialized scraper simply had not completed a full pass in six weeks, and nothing anywhere said so. vestedbb.com, the second-largest source at 3,586 listings, last updated 23 August.

A source that stops producing looks identical to a source with nothing new. The index quietly ages and every downstream number ages with it.

**Per-source staleness is a monitor, not a spot check**: any broker whose newest row is more than three days old, ranked by how many listings it holds.

### 12. Publish the correction, not the claim

Our credibility is that we say what the data shows, including when it undercuts us. Vending is 25 listings, not 200. Asking multiples are not sold multiples, and we label them as asking. Every honest caveat we publish is one a competitor can't copy without also being honest.

### 13. Never replace a whole file from a stale clone

On 1–2 Sep, full-file versions of `dealledger_scraper_v6.py` were pasted in from a clone at commit `637947d`, silently reverting three later commits and costing a reconciliation session.

**This is not a one-off.** The same clone reverted `scrapers/run_specialized.py`'s `listing_key()` (commit `12f1b16`), which held execbb.com and vestedbb.com's id stability — the revert alone produced 1,058 duplicate rows on the next run, three weeks later, with nothing pointing at the cause until someone noticed the uniqueness rate. Same mistake, same source, second file, second incident.

**Any file handed over from outside the repo gets applied as a patch and verified with `git log --oneline <base>..HEAD -- <file>` first** — never dropped in whole. Surgical patches only, applied to whatever is actually in the working tree.

### 14. A scrape must never undo a manual correction

Rows retired by hand were flipped back to active by the next run, because the upsert overwrote `status` unconditionally — no specialized scraper has ever reported a status, so every row got `"active"` by default, every time, regardless of what a human had just set it to. Restoring `listing_key()`'s id stability made this worse, not better: once an id reliably matched its original row again, the upsert found that row every run and stamped over the correction every time.

**Any field a human can set, a scraper must not blindly overwrite — it needs positive evidence, not a default.** The fix mirrors the `first_seen` guard: an existing row keeps whatever value it has unless this run's own data says otherwise.

---

## What "better every day" means concretely

Not vague improvement. Four measurable things:

**Coverage.** Brokers producing, weekly. 155 today. The `NO_PATTERN` failures are the biggest available gain — those brokers fetch fine and extract nothing.

**Freshness.** Rows with `last_seen` inside 7 days, as a share of active. Anything stale is a lie by omission on a live site.

**Correctness.** Untagged share, duplicate rate, junk rate. All three should trend to zero and be monitored, not spot-checked.

**Reach.** Citations, backlinks, inbound. Growing without paid acquisition.

If a week passes where none of these moved, that week didn't count.

---

## Standing constraints

- **CC0, always.** The data is free and unrestricted. That's what makes it citable, and citation is the distribution.
- **Never inject our own listings into the warehouse.** DealLedger is a neutral registry. Sosville-brokered inventory lives in the app database and is clearly marked on the sites.
- **Direct broker sourcing is the moat.** BizBuySell scraping is permanently dead (Akamai). BizQuest is a pass-through with the same listing numbers and is the path for cross-reference.
- **Every published number has a stated method.** DOM comes from anchored interpolation, and the anchors are public. If we can't explain how we got a number, we don't publish it.
- **Reading GitHub is free, writing is a decision.** `gh` is installed and authenticated. Don't push to main, edit or disable a workflow, trigger a run (`gh workflow run` — the broker scrape takes hours and burns Actions minutes), create or modify secrets, open PRs/issues/releases, or touch repo settings/visibility without asking first. `gh repo delete` is never acceptable. Commit locally, describe what's ready, wait for it to be pushed.

---

## For any agent picking this up

Assume the system is more broken than it looks, and that the breakage is silent. Before building anything new:

1. Is the thing you're about to extend actually running? Check the data, not the code.
2. Does anything assert that it's running? If not, that's the first build.
3. Has someone already built this in another repo or another project? There are five Supabase projects and several overlapping tables. Look before creating.

And when something doesn't work: **find the call path before forming a theory.** Most of the time the answer is that a function nobody suspected is never invoked at all.

**Check whether the job ran before reasoning about why it didn't.** `gh run list --workflow=broker_scrape.yml --limit 10` tells you if the nightly scrape actually ran and passed; `gh run view <id> --log` (or `--log-failed`) reads the real output instead of guessing at it; `gh workflow list` shows what's scheduled and enabled; `gh secret list` shows which secrets exist (names only). Most of this week's mysteries — tworld sitting at 977 rows for six weeks, `crawl_failures` staying empty — would have been answered in one command by looking at run history instead of reasoning about the code.

**Always `git pull origin main` at the start of a session.** The local tree drifts from origin, and changes get pushed from elsewhere between sessions.
