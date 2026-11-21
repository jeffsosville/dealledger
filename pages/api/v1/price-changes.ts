// GET /api/v1/price-changes — reviewed asking-price changes, filtered and paged in the database.
import { route, str, wantsCsv } from '../../../lib/intelligence/http';
import { searchPriceChanges } from '../../../lib/intelligence';

const CSV_HEADER = [
  'listing_number', 'dealledger_url', 'title', 'city', 'state', 'business_type', 'from_price', 'to_price', 'pct_change',
  'changed_after', 'changed_on_or_before', 'window_days', 'broker_site',
];

export default route('price-changes', async (req) => {
  const q = req.query;
  const csv = wantsCsv(req);
  const page = await searchPriceChanges(
    {
      state: str(q.state), type: str(q.type), direction: str(q.direction), min_pct: str(q.min_pct), since: str(q.since),
      limit: str(q.limit) ?? (csv ? '5000' : undefined), offset: str(q.offset),
    },
    csv ? 5000 : 100
  );
  return {
    body: page,
    csv: {
      name: ['dealledger-price-changes', str(q.state), str(q.type)].filter(Boolean).join('-'),
      header: CSV_HEADER,
      rows: page.results.map((r: any) => [
        r.listing_number, r.dealledger_url, r.title, r.city, r.state, r.business_type, r.from_price, r.to_price, r.pct_change,
        r.changed_after, r.changed_on_or_before, r.window_days, r.broker_site,
      ]),
    },
  };
});
