// lib/nicheGuides.ts — buyer context for the largest niche pages (lib/niches.ts).
// Same rules as lib/typeGuides.ts: factual and general; every number on the page
// comes from the live stats, never from this file.
import type { TypeGuide } from './typeGuides';

const SOURCE_FAQ = (what: string) => ({
  q: `Where do these ${what} listings come from?`,
  a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
});

export const NICHE_GUIDES: Record<string, TypeGuide> = {
  'franchise-resales': {
    title: 'Franchises for Sale: {n} Franchise Resales Listed by Brokers | DealLedger',
    h1: 'Franchise Resales for Sale',
    sections: [
      {
        heading: 'Why buy an existing franchise',
        body: [
          'A resale is an operating location with a sales history, staff and customers, instead of a new territory you have to build from zero. You pay for that: resales usually cost more than a new unit’s startup budget, but the risk is easier to measure because the numbers already exist.',
          'The franchise agreement comes with the business. The remaining term, the renewal conditions, royalty and marketing fees, and any required remodels all carry over to you and belong in the price.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'The franchisor has to approve you. Expect an application, a financial review, training, and a transfer fee, and ask the franchisor early how long approval takes.',
          'Read the current Franchise Disclosure Document, especially Item 20 on unit openings and closings and Item 19 if it shows financial performance. Then compare the listing’s numbers to the system average.',
          'Ask the franchisor and the seller whether any remodel, equipment or technology upgrade is due. A required refresh can change what the business is worth to you.',
        ],
      },
    ],
    faqs: [
      { q: 'Does the franchisor have to approve the sale?', a: 'Almost always. Franchise agreements typically require the franchisor to approve the buyer and charge a transfer fee, and some give the franchisor a right of first refusal. Build the approval timeline into your offer.' },
      { q: 'Can I use an SBA loan to buy a franchise?', a: 'Many franchise resales are financed with SBA 7(a) loans. Lenders look at the location’s own history and often at whether the brand appears in the SBA Franchise Directory.' },
      SOURCE_FAQ('franchise'),
    ],
  },

  'bars-and-taverns': {
    title: 'Bars for Sale: {n} Bars, Taverns & Pubs Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a bar',
        body: [
          'A bar is valued on its earnings, but two assets often matter as much: the liquor license and the lease. In states that cap the number of licenses, the license alone can carry significant value; in others it transfers for little more than the filing cost.',
          'Many listings are asset sales where most of the price is the license, the build-out and the location, especially when the current owner reports modest profit.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Confirm the exact license type, that it can be transferred to you, and how long the state or local transfer takes. Make the purchase contingent on approval.',
          'Compare reported sales to distributor and point-of-sale records and sales tax filings. Cash-heavy businesses deserve a second source for every number.',
          'Check the lease term, noise and occupancy limits, and any history of violations with the local licensing authority.',
        ],
      },
    ],
    faqs: [
      { q: 'Does the liquor license come with the bar?', a: 'It depends on the state. Some licenses transfer with the business after approval, some are tied to the location, and some must be applied for new. The listing should say which, and the purchase should be contingent on the transfer.' },
      { q: 'How are bars for sale priced?', a: 'On earnings where the bar is profitable, and on the value of the license, lease and build-out where it is not. The median asking price at the top of this page is calculated from the listings below.' },
      SOURCE_FAQ('bar'),
    ],
  },

  pizzerias: {
    title: 'Pizzerias for Sale: {n} Pizza Restaurants Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a pizzeria',
        body: [
          'Pizzerias trade on owner’s earnings, and the mix of business decides how durable those earnings are: delivery and takeout through the shop’s own phone and website, third-party delivery apps, dine-in, and catering.',
          'Ovens, hood systems and walk-ins are expensive to replace, so their age and condition move the price. So does the lease, because a good pizza location is hard to recreate.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Get point-of-sale reports by channel and compare them to bank deposits and sales tax returns. Look at what share of sales comes through delivery apps and what those apps charge.',
          'Ask how much of the cooking and management the owner does personally, and whether the recipes, phone number, website and social accounts transfer with the sale.',
          'If it is a franchise location, the franchisor’s approval and transfer terms apply.',
        ],
      },
    ],
    faqs: [
      { q: 'How are pizzerias for sale priced?', a: 'Usually as a multiple of seller’s discretionary earnings, adjusted for lease term, equipment condition and how dependent the business is on the owner. The figures at the top of this page come from the listings below.' },
      { q: 'Can I buy a pizzeria with an SBA loan?', a: 'Restaurants are financed with SBA 7(a) loans regularly. Lenders want verified cash flow, a lease that covers the loan term, and restaurant experience from the buyer or a manager.' },
      SOURCE_FAQ('pizzeria'),
    ],
  },

  'delis-and-sandwich-shops': {
    title: 'Delis for Sale: {n} Delis, Bagel & Sandwich Shops Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a deli',
        body: [
          'Delis and sandwich shops are small, high-volume operations, so price follows owner’s earnings and how many hours the owner works to produce them. A shop that runs on a manager is worth more than one that depends on the owner behind the counter.',
          'Many delis also sell grocery items, lottery, tobacco or beer. Those lines add traffic but carry thin margins and their own licenses, so look at them separately from food sales.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Compare point-of-sale data with supplier invoices: food cost as a share of sales is the quickest test of whether reported revenue is real.',
          'Confirm which licenses transfer (food service, beer and wine, lottery, tobacco) and how long each takes.',
          'Check the lease and the breakfast and lunch traffic that keeps the business alive: nearby offices, schools and commuter routes.',
        ],
      },
    ],
    faqs: [
      { q: 'How are delis for sale priced?', a: 'As a multiple of seller’s discretionary earnings, usually at the lower end for owner-operated shops. The median asking price at the top of this page is calculated from the listings below.' },
      { q: 'What licenses does a deli need?', a: 'A food service permit at minimum. Delis that sell beer, lottery or tobacco need separate licenses, and each one has its own transfer process.' },
      SOURCE_FAQ('deli'),
    ],
  },

  'coffee-shops': {
    title: 'Coffee Shops for Sale: {n} Cafés & Coffee Shops Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a coffee shop',
        body: [
          'Independent coffee shops often sell for close to the value of their equipment and build-out, because many earn modest profit for the hours involved. Shops with strong morning traffic, drive-thru windows or wholesale and catering accounts earn more and sell on earnings.',
          'Espresso machines and grinders are the major equipment items. Their age, and whether they are owned or leased from a roaster, belong in the price.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Look at sales by hour and day. A coffee shop’s value lives in its morning rush; afternoons and weekends tell you whether there is room to grow.',
          'Check supplier agreements with the roaster, any equipment that comes with them, and whether they transfer.',
          'Read the lease for the drive-thru, signage and outdoor seating rights the business depends on.',
        ],
      },
    ],
    faqs: [
      { q: 'How are coffee shops for sale priced?', a: 'Profitable shops sell as a multiple of earnings; smaller ones often sell for the value of the equipment, build-out and lease. The figures at the top of this page come from the listings below.' },
      { q: 'Is a franchise coffee shop different?', a: 'Yes. A franchise resale needs the franchisor’s approval, carries royalties and brand standards, and may require remodels. It is also easier to compare against other units in the system.' },
      SOURCE_FAQ('coffee shop'),
    ],
  },

  'auto-repair-shops': {
    title: 'Auto Repair Shops for Sale: {n} Mechanic & Auto Service Businesses | DealLedger',
    sections: [
      {
        heading: 'What sets the price of an auto repair shop',
        body: [
          'Repair shops sell on owner’s earnings, and the biggest question is who does the work. A shop where the owner is the lead technician is worth less to a buyer who is not a mechanic than one run by a team of certified techs.',
          'Customer base matters next: fleet and commercial accounts, a long list of repeat retail customers, and online reviews all make revenue more durable. Lifts, diagnostic equipment and any real estate included round out the price.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Get shop-management-system reports for car count, average repair order and parts versus labor margins. Compare them to tax returns.',
          'Meet the technicians and ask which ones are staying. Their certifications and tenure are the business.',
          'Check environmental compliance for waste oil, fluids and any underground storage, especially if the property is part of the sale.',
        ],
      },
    ],
    faqs: [
      { q: 'Do I need to be a mechanic to buy an auto repair shop?', a: 'No, but you need technicians who stay after the sale and a service manager who can run the floor. Lenders will ask how the shop runs without the seller.' },
      { q: 'How are auto repair shops priced?', a: 'As a multiple of seller’s discretionary earnings, with real estate valued separately when it is included. The median asking price at the top of this page is calculated from the listings below.' },
      SOURCE_FAQ('auto repair'),
    ],
  },

  'convenience-stores': {
    title: 'Convenience Stores for Sale: {n} C-Stores & Mini Marts Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a convenience store',
        body: [
          'C-stores are priced on earnings from inside sales, plus any fuel income and real estate. Inside sales (grocery, beverages, prepared food) carry the margin; lottery and tobacco drive traffic but earn little per dollar.',
          'Stores with fuel come with a fuel supply agreement. Its brand, term, pricing and any obligations a buyer must take over can matter as much as the store’s own numbers.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Compare reported sales to supplier invoices, lottery commission statements and sales tax returns. Ask for a breakdown by category.',
          'List every license that has to transfer: tobacco, lottery, beer and wine, food, and fuel or environmental permits.',
          'If fuel is involved, get the supply contract and an environmental review of the tanks before you commit.',
        ],
      },
    ],
    faqs: [
      { q: 'How are convenience stores priced?', a: 'On inside-store earnings, plus fuel income and real estate when they are part of the sale. The figures at the top of this page are calculated from the listings below.' },
      { q: 'What should I verify on a store with gas?', a: 'The fuel supply agreement, tank age and testing records, and an environmental assessment. These can carry obligations that outlast the purchase.' },
      SOURCE_FAQ('convenience store'),
    ],
  },

  'hair-salons': {
    title: 'Hair Salons for Sale: {n} Salons & Salon Suites Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a hair salon',
        body: [
          'A salon is worth what its stylists bring in, so the first question is how the stylists are paid: commission employees, booth or chair renters, or suite tenants. Rental models earn steadier, smaller income; commission models earn more and depend on keeping the team.',
          'Clients often follow stylists, not the salon. Price follows how much of the revenue comes from stylists who are likely to stay after the sale.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Get revenue by stylist from the booking system, and service versus retail product sales.',
          'Ask which stylists have agreements, how long they have been there, and whether the seller is one of the top producers.',
          'Check the lease, the condition of chairs, stations and shampoo plumbing, and state cosmetology licensing for the business.',
        ],
      },
    ],
    faqs: [
      { q: 'How are hair salons priced?', a: 'As a multiple of seller’s discretionary earnings, discounted when revenue depends on the owner or on a few stylists. The figures at the top of this page are calculated from the listings below.' },
      { q: 'Do I need a cosmetology license to own a salon?', a: 'Rules vary by state. Many states license the establishment and the stylists separately and do not require the owner to be licensed. Check with your state board before you buy.' },
      SOURCE_FAQ('hair salon'),
    ],
  },

  'dental-practices': {
    title: 'Dental Practices for Sale: {n} Dental Practices Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a dental practice',
        body: [
          'Dental practices are usually priced as a share of annual collections, adjusted for profit, patient count, payer mix and the condition of the equipment. Fee-for-service and PPO-heavy practices are valued differently.',
          'Buyers include individual dentists and dental service organizations. A practice that keeps hygienists busy and has a steady flow of new patients is attractive to both.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Get collections, production and new-patient counts by month from the practice software, and reconcile them with tax returns.',
          'Ask how long the selling dentist will stay for transition, which staff stay, and what patient records and phone numbers transfer.',
          'Check the lease, operatory count and equipment age, and any state rules on who may own a dental practice.',
        ],
      },
    ],
    faqs: [
      { q: 'Who can buy a dental practice?', a: 'In most states the practice must be owned by a licensed dentist, though management companies and DSOs work within those rules. Check your state’s ownership rules before you make an offer.' },
      { q: 'How are dental practices valued?', a: 'Commonly as a percentage of collections, adjusted for profitability, patient base and equipment. Specialty and broker valuations vary; the figures at the top of this page come from the listings below.' },
      SOURCE_FAQ('dental practice'),
    ],
  },

  'med-spas': {
    title: 'Med Spas for Sale: {n} Med Spas & Aesthetics Clinics Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What sets the price of a med spa',
        body: [
          'Med spas sell on earnings, and the durable part is recurring treatments: injectables, memberships and treatment packages. Device-driven revenue (lasers, body contouring) depends on equipment that may be financed or near the end of its useful life.',
          'Who performs the treatments matters. A practice that depends on the owner as injector is worth less to an outside buyer than one with a stable clinical team.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Med spas operate under state medical practice rules: who must own or supervise the clinic, the medical director arrangement, and who may perform each treatment. Confirm the current structure is compliant and can continue under you.',
          'Get revenue by service and provider, membership counts and churn, and any equipment leases or financing.',
          'Check outstanding prepaid packages and gift cards. They are obligations a buyer may inherit.',
        ],
      },
    ],
    faqs: [
      { q: 'Do you need to be a doctor to own a med spa?', a: 'It depends on the state. Some require physician ownership or a management-company structure; others allow non-physician owners with a medical director. Confirm the rules where the clinic operates before you buy.' },
      { q: 'How are med spas priced?', a: 'As a multiple of earnings, weighted toward recurring treatment and membership revenue. The figures at the top of this page are calculated from the listings below.' },
      SOURCE_FAQ('med spa'),
    ],
  },
};
