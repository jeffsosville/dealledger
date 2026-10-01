# Contributing to DealLedger

DealLedger is an open record of every business for sale in the US, read
directly from broker websites. The code is MIT, the data is CC0.

The most useful thing you can do is **adopt a broker**: make sure one
broker's listings come through complete. Most tasks take an afternoon and
need nothing more than Python and your browser's dev tools.

You do **not** need database access or API keys for any of this.

---

## Pick a task

Browse issues labeled [`good first issue`](https://github.com/jeffsosville/dealledger/labels/good%20first%20issue).
There are two kinds:

**1. Fix a field (most common).** We crawl the broker, but one field comes
through empty on every listing. For example, the site shows revenue but we
capture it on 0 of 4,960 listings. Each issue says which field, how many
listings, and gives a sample URL.

**2. Add a broker.** The broker isn't covered yet, or its scraper returns
nothing. See [adding-a-broker.md](docs/adding-a-broker.md) and
[WANTED_BROKERS.md](docs/WANTED_BROKERS.md).

Comment on the issue to claim it, so two people don't do the same work. If
you go quiet for a week we'll free it up for someone else.

---

## Set up

```bash
git clone https://github.com/jeffsosville/dealledger
cd dealledger
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

---

## Run a broker locally

Brokers are read by one of three paths. The issue tells you which.

| Path | Where the code lives | How to run it |
|---|---|---|
| Generic crawler | `scrapers/dealledger_scraper_v6.py` | `python3 scrapers/dealledger_scraper_v6.py --broker https://broker-site.com --test` |
| Specialized scraper | `BROKERS` in `scrapers/run_specialized.py` | `python3 scrapers/run_specialized.py --dry-run --brokers <key>` |
| WordPress REST | one row in `data/wp_rest_brokers.csv`, parsed by `WPRestScraper` | `python3 scrapers/run_specialized.py --dry-run --brokers wp_<domain>` |

Both commands are safe. A `--broker` run never writes to the database unless
you pass `--write` (maintainers only), and `--dry-run` never writes at all.

The generic crawler prints a field summary at the end:

```
Listings:  38 total
  price:   38
  cf:      31
  state:   36
```

That summary is your before-and-after. Run it before your change, fix the
parser, run it again.

---

## What "done" looks like

- The field is captured on the listings where the broker's site actually
  shows it. If the site hides revenue on half its listings, half is correct.
  Say so in the PR.
- Nothing else got worse. Paste the before and after field summary in the PR.
- Numbers are numbers: `$1.2M` becomes `1200000`, and "Call for price" or
  "Confidential" becomes empty, not `0`.
- Be polite to broker sites. Keep the existing request delays, and don't
  crawl a site more than you need to test.

---

## Open a pull request

- Title: `Fix: revenue for tworld.com` or `Add broker: Example Business Brokers`
- Link the issue (`Closes #123`).
- Include the before and after field summary.
- Keep it to one broker or one parser. Small PRs get merged fast.

We aim to review every PR within a few days.

---

## Other ways to help

- **Report a data error.** Open an issue with the listing URL, what's wrong,
  and what the broker's site actually says.
- **Suggest a broker.** Open an issue with the website. Most brokers need no
  code at all; our discovery job finds their listings page.
- **Challenge the methodology.** If you think how we date, dedupe or classify
  listings is wrong, open an issue with your reasoning.
  See [METHODOLOGY.md](docs/METHODOLOGY.md).
- **Build on the data.** Daily snapshots are CC0. Tell us what you make.

---

## Credit

Every merged contribution is credited in [CONTRIBUTORS.md](CONTRIBUTORS.md)
and in the weekly DealLedger pulse. Fix a broker and your name goes on that
broker's record.

---

## Conduct

Be respectful, assume good intent, and focus on the work. Harassment of any
kind isn't tolerated.

Questions? Open an issue, or find [@JeffSosville](https://x.com/JeffSosville) on X.
