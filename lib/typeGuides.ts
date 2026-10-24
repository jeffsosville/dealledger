// lib/typeGuides.ts — buyer-side context for national type pages.
//
// Search Console (Oct 2026) shows these two type pages sitting at positions
// 11–16 for their head terms with real impressions. The pages were a table and
// one sentence of numbers; this adds the substance a buyer actually searches
// for. Keep it factual and general: every figure on the page comes from the
// live stats, never from this file.

export type TypeGuide = {
  title: string;        // <title>, with {n} replaced by the listing count
  h1?: string;
  sections: { heading: string; body: string[] }[];
  faqs: { q: string; a: string }[];
};

export const TYPE_GUIDES: Record<string, TypeGuide> = {
  laundromat: {
    title: 'Laundromats for Sale: {n} Laundromat Businesses Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a laundromat',
        body: [
          'Most laundromats change hands on a multiple of seller’s discretionary earnings, and the spread between a cheap one and an expensive one usually comes down to three things: the lease, the equipment, and how much of the work the owner does.',
          'The lease matters more here than in almost any other small business. Water, sewer and venting are expensive to move, so a laundromat is worth what its remaining lease term lets a buyer earn. Ten or more years including options is the usual comfort level for lenders.',
          'Equipment age sets the capital a buyer has to plan for. Washers and dryers that are near the end of their life, or still coin-only, get priced as a near-term replacement cost.',
          'Attended stores with wash-and-fold or pickup and delivery earn more but take more labor. A fully unattended store is closer to a passive investment and is valued that way.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Ask for twelve months of water and utility bills alongside the profit and loss statement. Water usage is the most reliable way to confirm reported revenue in a cash-heavy business.',
          'Get the lease, every option to renew, and the landlord’s willingness to assign it. Then get an equipment list with model years and any service records.',
          'Walk the store on a weekend and a weekday. Turns per machine per day, competition within a couple of miles and parking tell you more than the listing does.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are laundromats for sale priced?',
        a: 'Usually as a multiple of seller’s discretionary earnings, adjusted for remaining lease term and equipment age. The median asking price and price-to-cash-flow ratio at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Can I buy a laundromat with an SBA loan?',
        a: 'Laundromats are a common SBA 7(a) acquisition. Lenders generally want a lease that runs at least as long as the loan, verified revenue, and some industry or management experience from the buyer.',
      },
      {
        q: 'Where do these laundromat listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so days on market reflect what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  hvac: {
    title: 'HVAC Businesses for Sale: {n} HVAC Companies Listed by Brokers | DealLedger',
    h1: 'HVAC Businesses for Sale',
    sections: [
      {
        heading: 'What sets the price of an HVAC company',
        body: [
          'Buyers pay most for revenue they can count on next year. Service agreements and maintenance memberships are the clearest version of that, so the share of revenue that is recurring service rather than one-off installs moves the multiple more than total sales do.',
          'The split between residential service and new-construction work matters too. Construction-heavy revenue rises and falls with building activity, and buyers price that risk in.',
          'Technicians are the constraint in this trade. A company with licensed techs who are staying, a service manager, and an owner who is not the only person quoting jobs is worth more than one that depends on the seller.',
          'Private equity has been buying HVAC companies, which has pushed up prices for larger, well-run operators. Most broker listings are owner-operated companies below that size.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Ask for revenue split by service, replacement and installation, and for the count of active maintenance agreements with renewal rates.',
          'Confirm whose license the company operates under. If it is the seller’s personal license, plan for how that transfers or who replaces it.',
          'Review the fleet and equipment list, technician tenure, and any manufacturer dealer agreements, which may not transfer automatically.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are HVAC businesses for sale valued?',
        a: 'Usually as a multiple of seller’s discretionary earnings or EBITDA, higher for companies with a large base of recurring service agreements and lower for those dependent on new construction or on the owner. The median figures at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Can I finance an HVAC acquisition with an SBA loan?',
        a: 'Yes, HVAC companies are frequently financed with SBA 7(a) loans. Lenders look closely at licensing, so plan for a licensed qualifier if you do not hold one yourself.',
      },
      {
        q: 'Where do these HVAC listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so days on market reflect what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },
};
