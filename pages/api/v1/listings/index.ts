// GET /api/v1/listings — search the public listing index (same set the site shows).
import { route, str, wantsCsv } from '../../../../lib/intelligence/http';
import { searchListings } from '../../../../lib/intelligence';

const CSV_HEADER = [
  'listing_number', 'dealledger_url', 'title', 'city', 'state', 'business_type', 'asking_price', 'cash_flow', 'revenue',
  'price_to_cash_flow', 'age_status', 'observed_days', 'listed_after', 'listed_on_or_before', 'age_basis', 'last_seen',
  'relist_signal', 'broker_site', 'broker_page',
];

export default route('listings', async (req) => {
  const q = req.query;
  const csv = wantsCsv(req);
  const page = await searchListings(
    {
      state: str(q.state), type: str(q.type), q: str(q.q), broker: str(q.broker),
      min_price: str(q.min_price), max_price: str(q.max_price), min_cash_flow: str(q.min_cash_flow),
      min_age_days: str(q.min_age_days), relisted: str(q.relisted), sort: str(q.sort),
      limit: str(q.limit) ?? (csv ? '5000' : undefined), offset: str(q.offset),
    },
    csv ? 5000 : 100
  );
  return {
    body: page,
    csv: {
      name: ['dealledger-listings', str(q.state), str(q.type)].filter(Boolean).join('-'),
      header: CSV_HEADER,
      rows: page.results.map((r: any) => [
        r.listing_number, r.dealledger_url, r.title, r.city, r.state, r.business_type, r.asking_price, r.cash_flow, r.revenue,
        r.price_to_cash_flow, r.observed_age.status, r.observed_age.observed_days, r.observed_age.listed_after,
        r.observed_age.listed_on_or_before, r.observed_age.basis, r.last_seen, r.relist_signal, r.broker_site, r.broker_page,
      ]),
    },
  };
});
