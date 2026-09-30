# How DealLedger crawls

DealLedger records listings that brokers publish on their own public
websites. This page says what we read, how often, and how a broker can
opt out.

## What we read

- Public listing pages only: no logins, no member areas, no paywalls.
- The listing facts a broker publishes: title, asking price, cash flow and
  revenue where shown, location, and the listing URL.
- We do not collect buyer or seller contact details, and we do not send
  anyone email or calls from this data.

## How often

- Each broker site is crawled at most once a day, and each producing broker
  at least once a week.
- Most sites are read one page at a time with a pause between pages. A few
  large franchise networks are read with a small number of parallel requests.
- If a site returns errors, we back off and try again on a later day.

## Opting out

If you are a broker and don't want your site crawled, open an issue using
the **Broker opt-out** template. We will stop crawling the site and mark it
opted out in the broker registry. Listings already recorded stay in the
history, as the record of what was public at the time.

## Problems

If our crawler is causing trouble for your site, open an issue and we will
slow it down or stop it the same day we see it.
