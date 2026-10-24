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

  cleaning: {
    title: 'Cleaning Businesses for Sale: {n} Janitorial, Maid & Commercial Cleaning Companies | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a cleaning business',
        body: [
          'Owner-operated cleaning companies are priced on seller’s discretionary earnings. Where a business lands in the range depends mostly on what kind of revenue it has: contracted commercial accounts, recurring residential clients, or one-off jobs.',
          'Commercial and janitorial companies are valued on their contracts. Written, assignable agreements spread across many customers are worth more than handshake accounts, and one customer above roughly a quarter of revenue usually lowers the price.',
          'Residential and maid services are valued on recurring frequency and retention. A book of weekly and biweekly clients billed automatically is worth more than the same revenue from move-outs and deep cleans.',
          'Labor is the risk buyers price hardest. Properly classified crews, low turnover and a supervisor between the owner and the customers all move a business up the range.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Get revenue by customer for the last two to three years, and check how many of today’s accounts were there a year ago.',
          'Read every large contract for term, termination notice and whether it can be assigned to a new owner.',
          'Ask how cleaners are paid and classified, and what payroll looks like as a share of revenue. Misclassified 1099 crews are a liability that transfers with the business.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are cleaning businesses for sale priced?',
        a: 'Usually as a multiple of seller’s discretionary earnings, higher for contracted commercial revenue and recurring residential clients, lower for one-off work or businesses that depend on the owner. The median asking price and price-to-cash-flow ratio at the top of this page come from the listings below.',
      },
      {
        q: 'Is a commercial or residential cleaning business a better buy?',
        a: 'Commercial businesses usually have larger, contracted accounts and more concentration risk; residential businesses have many small clients and more churn. Which is better depends on whether you would rather manage customer relationships or a high volume of scheduling.',
      },
      {
        q: 'Where do these cleaning business listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so days on market reflect what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  roofing: {
    title: 'Roofing Companies for Sale: {n} Roofing Businesses Listed by Brokers | DealLedger',
    h1: 'Roofing Companies for Sale',
    sections: [
      {
        heading: 'What sets the price of a roofing company',
        body: [
          'Roofing revenue is project-based, so buyers pay most for a business that wins work without the owner. A sales process with estimators, a steady lead source and a backlog of signed jobs is worth more than an owner who sells every roof personally.',
          'The mix of work matters. Residential re-roofing, commercial maintenance contracts and insurance or storm work each carry different risk. Storm-driven revenue can make one or two years look far better than normal, and buyers will average it out.',
          'Licensing, insurance and warranty exposure carry over. A clean claims history and manageable warranty obligations make a company easier to finance and easier to sell.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Revenue by year and by type of work, with storm and insurance jobs broken out, so you can see the underlying run rate.',
          'Whose license the company operates under, and how it transfers or who replaces it.',
          'Outstanding warranties, open claims, crew structure (employees or subcontractors), and any manufacturer certifications that may not transfer automatically.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are roofing companies valued?',
        a: 'As a multiple of seller’s discretionary earnings for owner-operated companies, or EBITDA for larger ones with managers in place. Buyers normalize storm-heavy years and look closely at how much of the sales process depends on the owner.',
      },
      {
        q: 'Can I buy a roofing company with an SBA loan?',
        a: 'Yes. Lenders look for verified earnings across several years, a licensed qualifier, and insurance and warranty exposure they can understand.',
      },
      {
        q: 'Where do these roofing listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so days on market reflect what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  amusement: {
    title: 'Entertainment Businesses for Sale: {n} Family Entertainment Centers, Arcades & More | DealLedger',
    sections: [
      {
        heading: 'What sets the price of an entertainment business',
        body: [
          'Family entertainment centers, arcades, trampoline parks, bowling alleys and similar businesses are capital-heavy and location-bound, so the lease and the equipment shape the price as much as the earnings do.',
          'Revenue is seasonal and event-driven. Birthday parties, group bookings and memberships smooth it out; a business that lives on walk-in weekend traffic is riskier and priced that way.',
          'Equipment and attractions wear out and go out of fashion. Buyers model the cost of refreshing them in the first few years, so the age and condition of the attractions come straight off the price.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'The lease: remaining term, options, rent as a share of revenue, and the landlord’s willingness to assign it.',
          'Revenue by month and by stream (admissions, parties, food and beverage, games), so you can see seasonality and margin.',
          'An equipment list with ages and maintenance records, plus insurance history and any required inspections for rides or attractions.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are entertainment businesses valued?',
        a: 'Usually as a multiple of seller’s discretionary earnings or EBITDA, adjusted for lease term and the capital needed to keep attractions current. The median figures at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Is a family entertainment center a good business to buy?',
        a: 'It can be, but it is closer to running a venue than a service business: high fixed costs, seasonal swings and ongoing capital spending. The strongest ones have party and group revenue that keeps weekdays busy.',
      },
      {
        q: 'Where do these listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so days on market reflect what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  restaurant: {
    title: 'Restaurants for Sale: {n} Restaurants Listed by Business Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a restaurant',
        body: [
          'Many restaurants for sale are priced closer to the value of the space and equipment than to earnings, because margins are thin and a new owner often changes the concept. Profitable, established restaurants with clean books are the ones that sell on a multiple of earnings.',
          'The lease is often the most valuable thing being sold: location, remaining term, rent as a share of sales, and whether the landlord will assign it.',
          'Franchise resales are priced differently from independents. The franchisor’s transfer fee, approval process and any required remodel all affect what a buyer can pay.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Point-of-sale reports and sales tax filings alongside the profit and loss statement. In a cash-heavy business they are the most reliable check on reported sales.',
          'Food, labor and occupancy costs as a share of sales, compared month by month.',
          'The liquor license, if any, and how it transfers in that state; health inspection history; and the age of major kitchen equipment.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are restaurants for sale priced?',
        a: 'Profitable restaurants usually sell on a multiple of seller’s discretionary earnings. Restaurants with little or no profit are priced on the value of the lease, build-out and equipment. The median figures at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Is it better to buy an existing restaurant or a franchise resale?',
        a: 'An independent gives you control of the concept; a franchise resale gives you a proven system with fees, rules and a transfer process. Either way, verify sales from point-of-sale and tax records rather than the listing.',
      },
      {
        q: 'Where do these restaurant listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so days on market reflect what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  landscaping: {
    title: 'Landscaping Businesses for Sale: {n} Lawn Care & Landscaping Companies | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a landscaping business',
        body: [
          'Recurring maintenance contracts are what buyers pay for. A book of weekly mowing and maintenance accounts, especially commercial ones under contract, is worth more than the same revenue from one-off installation projects.',
          'Equipment is a large part of the deal. Buyers look at the age and condition of trucks, trailers and mowers and price in what will need replacing soon.',
          'Labor and seasonality are the main risks. Crews that come back every season, and snow removal or year-round work in colder climates, make earnings steadier and the business easier to finance.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Revenue split between maintenance contracts, installation projects and snow or seasonal work, for at least three years.',
          'Customer concentration and contract terms for commercial accounts, including whether they can be assigned.',
          'An equipment list with ages, titles and liens, and how crews are hired, paid and classified.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are landscaping businesses valued?',
        a: 'As a multiple of seller’s discretionary earnings, higher for recurring maintenance contracts and lower for project-based installation work. Equipment value and condition are considered alongside earnings.',
      },
      {
        q: 'Can I buy a lawn care business with an SBA loan?',
        a: 'Yes. Lenders want verified earnings, and they will look at equipment as collateral and at how dependent the business is on the seller.',
      },
      {
        q: 'Where do these landscaping listings come from?',
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
