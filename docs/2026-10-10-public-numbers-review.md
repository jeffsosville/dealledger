# Public numbers review — 2026-10-10

Session handoff, not a state table. **CLAUDE.md is the source of truth**
for mission, state and pipeline; read it first. This file records what an
outside review found, what was changed on 2026-10-10, and what is still open.

## The finding

An outside review of dealledger.org found four coverage figures that disagree:
broker registry 475 brokers / 21,522 listings; dataset metadata 535 domains /
24,820 rows; methodology ~770 broker websites; "The Market Has No Memory"
1,700+ broker websites plus marketplace data.

Most of the disagreement is copy, not data. The live site agrees with itself
within ~40 listings. The four numbers are different pipeline stages described
with the same words, and most were typed by hand.

## What the site actually reads

| Page | Source | 2026-10-10 |
|---|---|---|
| Homepage table, URL checker | `mv_listings_page` (bridged `listings`, `source='broker_direct'`) | 21,281 listings |
| /brokers, /broker/[slug] | `broker_directory` (built on `listings`) | 476 sites, 21,318 listings |
| CSV/JSON download | `scripts/export_public_listings.py` → `listings_direct` active + published | written daily |

`listings` is the serving layer, not legacy: per CLAUDE.md the site shows only
rows that pass `bridge_direct_to_listings()`.

## Changed on 2026-10-10

- **`public_stats` materialized view** (`sql/2026-10-10_public_stats.sql`): one
  row of figures for public pages. `listings_on_site` and `broker_sites_on_site`
  count what the site shows; `broker_sites_in_pipeline`, `_producing`,
  `_crawled_7d` describe the funnel; plus `last_successful_crawl_at`, `as_of`.
  Refreshed inside `refresh_broker_registry()` (cron 15:35 UTC), right after
  `broker_directory`. Readable by anon. Nothing reads it yet.
- **`broker_registry` dropped.** Superseded draft, already marked "safe to drop"
  in `sql/2026-10-08_broker_registry.sql`. Nothing referenced it.
- **Blocked-domain listings unpublished.** 84 `listings_direct` rows (15 URLs)
  from 7 `broker_block` domains were still `published`; set
  `status='quarantined_blocked'`. Nothing deleted. `published` is recomputed by
  `trg_listings_direct_set_published`, and V6 skips blocked domains, so they
  should stay out. **Verify after the 2026-10-11 scrape** (principle 14).
- **RLS enabled** on `wayback_anchors`, `listings_active_backup_2026_10_03`,
  `_fs_backfill_20261008` (were anon-writable). The two workflows that read
  `wayback_anchors` use the service key; `dl_lookup` is SECURITY DEFINER.

## Open — decisions, not yet changes

1. **Point public pages at `public_stats`.** Homepage proof line ("listings on the
   record since March 2026") and the /brokers header. Homepage is
   `public/index.html`, filled client-side; counts show "—" until JS runs.
   Server-rendering it is the real fix.
2. **Fix the hand-typed numbers.** `public/methodology.html`, `docs/METHODOLOGY.md`,
   `public/why.html`. The 1,700 figure is the broker-coverage *goal*; label it as
   a goal, not coverage. The essay's marketplace-data description is out of date
   with broker-direct-only (CLAUDE.md, 24 Sep).
3. **Record start date.** Site code says March 24, 2026
   (`lib/brokerRegistry.ts` `OBSERVATION_START_LABEL`); `listing_sighting` and
   2,512 published listings start 2026-02-18. Decide which is true and why.
4. **34 listing URLs attributed to two broker domains.** Dedup question.
5. **`listings_direct.status = 'sold'`.** Find what writes it. If inferred from
   disappearance, it contradicts the methodology (disappeared ≠ sold).
6. **Price history is not publishable yet.** Of ~1,179 URLs with more than one
   price in `listings_direct`, roughly 80% are extraction errors
   (e.g. $40M → $170K). `listing_observation` stopped writing 2026-09-08;
   `listing_sighting` is current but has no price. Keep price-cut views off the
   site until extraction is fixed.
7. **Relist flag.** `mv_listings_page.relisted` is true on 165 broker-direct rows,
   but METHODOLOGY says broker-direct relist matching hasn't shipped. Check what
   sets it before showing it.
8. **`mv_listings_page.signal`** (hot / gem / overpriced / dead) is a judgment,
   not an observation. Keep it off public pages.

## Language rules carried from the review

Disappeared is not sold: "no longer observed", never "sold" or "time to sell",
except a broker's own label quoted as theirs. Every chart and report about
removals says so.
