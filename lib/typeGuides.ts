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
        heading: 'Unattended, attended, or service-led',
        body: [
          'Unattended stores run on coin, card or app payment with an owner or route person visiting daily to clean, collect and handle repairs. They are the listings that say “absentee” or “semi-absentee,” and they attract the most buyers, so they rarely trade at a discount.',
          'Attended stores keep an employee on site during open hours. Payroll is the biggest cost after rent and utilities, and it buys cleaner machines, fewer break-ins and the chance to sell services.',
          'Wash-and-fold, commercial accounts (gyms, salons, short-term rentals, clinics) and pickup and delivery turn a laundromat into a service business. They can lift revenue well beyond what the machines alone earn, but that revenue depends on staff and customer relationships, so ask how much of it the current owner personally manages.',
        ],
      },
      {
        heading: 'Equipment and payment systems',
        body: [
          'Commercial washers and dryers last many years but not forever. The age, brand and mix of machine sizes matter: large-capacity washers earn the most per turn, and a store with only small top-loaders will usually need investment.',
          'Card and app payment systems let an owner change prices, run promotions and see revenue by machine. They also make reported revenue easier to verify. Coin-only stores often carry a conversion cost in the buyer’s plan.',
          'Ask whether any equipment is still under a distributor loan or lease, because that debt either gets paid off at closing or transfers with the store.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Ask for twelve months of water and utility bills alongside the profit and loss statement. Water usage is the most reliable way to confirm reported revenue in a cash-heavy business.',
          'Get the lease, every option to renew, and the landlord’s willingness to assign it. Then get an equipment list with model years and any service records.',
          'Walk the store on a weekend and a weekday. Turns per machine per day, competition within a couple of miles and parking tell you more than the listing does.',
          'Check the neighborhood behind the numbers: the share of renters without in-unit laundry is what keeps a laundromat busy. New apartment buildings with in-unit machines, or a new store opening nearby, change the outlook.',
        ],
      },
      {
        heading: 'Buying the building too',
        body: [
          'Some laundromats come with the real estate. That removes lease risk and changes the financing: the building can be financed separately from the business, often over a longer term. It also means part of the price is real estate and should be checked against local property values, not only against the store’s earnings.',
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
        q: 'Is a laundromat a passive business?',
        a: 'An unattended store can be run in a few hours a day, but someone still has to clean, collect, handle repairs and deal with customers. Stores that earn more from wash-and-fold, delivery or commercial accounts take more management, not less.',
      },
      {
        q: 'Where do these laundromat listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
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
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
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
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
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
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
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
          'The lease is often the most valuable thing in the deal: location, remaining term, rent as a share of sales, and whether the landlord will assign it.',
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
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
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
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
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
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  plumbing: {
    title: 'Plumbing Companies for Sale: {n} Plumbing Businesses Listed by Brokers | DealLedger',
    h1: 'Plumbing Companies for Sale',
    sections: [
      {
        heading: 'What sets the price of a plumbing company',
        body: [
          'Service and repair work is steadier than new-construction work, and buyers pay for that. Emergency calls, drain cleaning, water heater replacement and maintenance agreements come back every year; new-build rough-ins rise and fall with housing starts.',
          'Licensed plumbers are hard to hire, so the crew is a large part of what a buyer is paying for. A company with licensed technicians who are staying, a dispatcher, and an owner who is not running every call is worth more than one built around the seller.',
          'Commercial and multifamily service contracts with property managers, restaurants or facilities can add recurring revenue, but check how concentrated it is. One large customer can be most of the value.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Revenue by year split into service, replacement and new construction, plus the count of any maintenance or service agreements and how often they renew.',
          'Whose master plumber license the company operates under, and how it transfers or who replaces it.',
          'Technician tenure and pay, the fleet and equipment list, and any open warranty or callback claims.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are plumbing companies valued?',
        a: 'Usually as a multiple of seller’s discretionary earnings for owner-operated companies, or EBITDA for larger ones with managers in place. Recurring service revenue and a team that runs without the owner push the multiple up; new-construction dependence pulls it down. The median figures at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Can I buy a plumbing company with an SBA loan?',
        a: 'Yes, plumbing companies are commonly financed with SBA 7(a) loans. Lenders will want a licensed qualifier in place, so plan for one if you are not licensed yourself.',
      },
      {
        q: 'Where do these plumbing listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  electrical: {
    title: 'Electrical Contractors for Sale: {n} Electrical Businesses Listed by Brokers | DealLedger',
    h1: 'Electrical Contractors for Sale',
    sections: [
      {
        heading: 'What sets the price of an electrical contractor',
        body: [
          'The mix of work matters most. Residential service, commercial service and maintenance, and new-construction or bid work each carry different risk. Service and maintenance revenue repeats; bid work depends on winning the next job and on margins holding through it.',
          'Backlog is part of the price. Signed contracts not yet billed show what the next year looks like, but check the margins on them and whether they need bonding the buyer can carry.',
          'Licensed electricians are the constraint. A company with journeymen and a master electrician who are staying, a project manager, and an owner who is not estimating every job is worth more.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Revenue by year and by type of work, the current backlog with expected margins, and the largest customers’ share of sales.',
          'Whose master electrician license the company operates under, how it transfers, and the company’s bonding capacity.',
          'Work-in-progress schedules for open jobs, retainage owed, and any warranty or callback claims.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are electrical contractors valued?',
        a: 'As a multiple of seller’s discretionary earnings or EBITDA, adjusted for the service and bid mix, backlog and owner dependence. The median figures at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Can I buy an electrical contractor with an SBA loan?',
        a: 'Yes. Lenders look for several years of verified earnings, a licensed qualifier, and for bid-heavy companies, work-in-progress schedules they can follow.',
      },
      {
        q: 'Where do these electrical contractor listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  construction: {
    title: 'Construction Companies for Sale: {n} Construction Businesses Listed by Brokers | DealLedger',
    h1: 'Construction Companies for Sale',
    sections: [
      {
        heading: 'What sets the price of a construction company',
        body: [
          'Buyers pay for earnings they expect to repeat. A general contractor or specialty trade with repeat commercial clients, maintenance work or a steady referral base is worth more than one whose last two years came from a few large jobs.',
          'Backlog and bonding capacity carry real value. Signed work shows the next year, and the bonding line decides which jobs the company can bid. Both depend partly on the owner’s personal finances and history, so ask how they transfer.',
          'Equipment can be a large part of the price. Get the list with ages and condition, and separate owned equipment from leased or financed equipment.',
          'Owner dependence is the biggest discount. If the seller holds the license, the relationships and the estimating, plan for how each one moves to you.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Revenue and gross margin by year and by customer, and work-in-progress schedules showing over- and under-billing on open jobs.',
          'Licenses, bonding capacity and surety relationship, and insurance and safety records, including the workers’ compensation experience modifier.',
          'Open warranty obligations, liens, disputes and retainage owed to or by the company.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are construction companies valued?',
        a: 'Smaller owner-operated companies trade on a multiple of seller’s discretionary earnings; larger ones on EBITDA, plus or minus equipment and working capital. Backlog quality, customer concentration and owner dependence move the multiple. The median figures at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Can I buy a construction company with an SBA loan?',
        a: 'Yes, though lenders look harder at construction than most industries: several years of earnings, work-in-progress schedules, bonding and a licensed qualifier.',
      },
      {
        q: 'Where do these construction listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  healthcare: {
    title: 'Healthcare Businesses for Sale: {n} Medical Practices & Healthcare Companies Listed by Brokers | DealLedger',
    h1: 'Healthcare Businesses for Sale',
    sections: [
      {
        heading: 'What sets the price of a healthcare business',
        body: [
          'Healthcare listings cover very different businesses: physician, dental and therapy practices, home health and home care agencies, pharmacies, medical spas, staffing firms and clinics. Each has its own licensing and payer rules, so compare a listing with others of the same kind.',
          'Payer mix moves value. Revenue from commercial insurance, Medicare, Medicaid and cash patients carries different reimbursement rates and different risk if rates or rules change.',
          'Provider dependence is the main discount. A practice where the selling clinician produces most of the revenue is worth less than one with associate providers who are staying, unless the seller agrees to a transition period.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Collections, not just billings, by year and by payer, and how many active patients or clients the business has.',
          'Licenses, certifications and payer contracts, and which of them transfer to a new owner, need a new application, or require a licensed professional as owner.',
          'Billing and coding compliance, any audits or recoupments, and the lease and equipment for the location.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are healthcare businesses valued?',
        a: 'Usually as a multiple of seller’s discretionary earnings or EBITDA, adjusted for payer mix, provider dependence and licensing. The median figures at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Do I need to be licensed to buy a medical practice?',
        a: 'It depends on the state and the type of practice. Some states require a licensed professional to own a physician or dental practice; other healthcare businesses, like home care agencies or staffing firms, can be owned by non-clinicians with licensed staff. Check before you make an offer.',
      },
      {
        q: 'Where do these healthcare listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  automotive: {
    title: 'Automotive Businesses for Sale: {n} Auto Repair Shops, Dealers & More Listed by Brokers | DealLedger',
    h1: 'Automotive Businesses for Sale',
    sections: [
      {
        heading: 'What sets the price of an automotive business',
        body: [
          'Automotive listings include repair and service shops, collision and body shops, tire and oil change franchises, used car dealers, detailing and parts businesses. Compare a listing with others of the same kind, because margins and risks are very different.',
          'For repair shops, buyers pay for technicians who are staying, a steady base of repeat customers and fleet accounts, and an owner who is not the only diagnostician. Car counts and average repair order tell you more than total revenue.',
          'Collision shops depend heavily on insurance direct-repair relationships, which may not transfer automatically. Dealers depend on inventory financing and, for franchised dealers, manufacturer approval of the buyer.',
          'Many automotive businesses come with the real estate, and environmental history matters when they do.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Revenue by year from the shop management or dealer system, car count and average repair order, and the share of revenue from fleet or insurance customers.',
          'Technician certifications and tenure, the equipment list with ages, and any franchise or insurance agreements that need approval to transfer.',
          'If the property is included, a Phase I environmental review covering lifts, waste oil, solvents and any underground tanks.',
        ],
      },
    ],
    faqs: [
      {
        q: 'How are automotive businesses valued?',
        a: 'Most are priced as a multiple of seller’s discretionary earnings plus equipment, with inventory and any real estate valued separately. The median figures at the top of this page are calculated from the listings shown below.',
      },
      {
        q: 'Can I buy an auto repair shop with an SBA loan?',
        a: 'Yes. Repair shops are commonly financed with SBA 7(a) loans, or 504 loans when the building is included. Lenders will usually want an environmental review on the property.',
      },
      {
        q: 'Where do these automotive listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  car_wash: {
    title: 'Car Washes for Sale: {n} Car Wash Businesses Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'Types of car washes for sale',
        body: [
          'Self-serve bays: customers wash their own cars with wands and foam brushes. Low labor and simple equipment, but revenue per car is small and the site needs regular cleaning and collection.',
          'In-bay automatics: the car parks and a machine moves around it. Often paired with self-serve bays or a gas station. One or two automatics limit how many cars an hour the site can handle.',
          'Express exterior tunnels: a conveyor pulls cars through in a few minutes, with free vacuums after. This is the format most new construction uses, because it handles high volume with a small crew and lends itself to monthly memberships.',
          'Full-service and flex-serve washes add interior cleaning and detailing. Revenue per car is higher; so are labor and management.',
        ],
      },
      {
        heading: 'What sets the price of a car wash',
        body: [
          'Most car washes come with the land, so the asking price usually combines a business valued on earnings with real estate valued on location. Ask the broker how the price splits between the two.',
          'Monthly unlimited memberships turn weather-dependent revenue into recurring revenue. The number of active members, how long they stay and the share of revenue they produce are the figures buyers weigh most.',
          'Equipment age matters because tunnel equipment, dryers, water reclaim systems and payment kiosks are expensive. A wash due for a rebuild is priced as one.',
          'Competition moves value more than in most businesses. A new express tunnel opening down the road can take members quickly, so buyers check what is permitted or under construction nearby.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Get revenue from the point-of-sale and membership system by month for at least two years, and compare it with bank deposits. Look at how much wet or cold months pull revenue down.',
          'Ask for water, sewer, electric and chemical bills. They track volume and confirm car counts, and they show what a reclaim system is saving.',
          'Get an equipment list with installation years and service history, and have an equipment technician look at the tunnel or automatics before closing.',
          'Order an environmental review. Car washes handle chemicals and wastewater, and many sit on former gas station sites.',
          'Check zoning, the traffic count on the road in front and how easy it is to turn in and out. Access is hard to fix after you buy.',
        ],
      },
    ],
    faqs: [
      { q: 'How are car washes for sale priced?', a: 'Usually as earnings plus real estate: a multiple of cash flow for the business, adjusted for format, membership base and equipment age, and the value of the land and building. The median asking price at the top of this page is calculated from the listings below.' },
      { q: 'Can I buy a car wash with an SBA loan?', a: 'Car washes are financed with both SBA 7(a) and 504 loans. The 504 program is often used when the purchase includes the real estate. Lenders look closely at environmental reports and equipment condition.' },
      { q: 'Is a car wash a passive investment?', a: 'Self-serve and express formats need less labor than most businesses, but equipment breaks, customers need help and memberships need to be managed. Plan on daily attention from an owner or manager.' },
      {
        q: 'Where do these car wash listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  gas_station: {
    title: 'Gas Stations for Sale: {n} Gas Stations & Convenience Stores Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'What you are actually buying',
        body: [
          'A gas station listing can mean very different things. Some include the land, the building, the tanks and the business. Others are the business only, with the site leased from a landlord or fuel distributor. Read the listing closely: the price, the risk and the financing are completely different.',
          'Most stations earn on two lines. Fuel sells in high volume at a thin margin per gallon. The convenience store inside sells at much higher margins. Many buyers value the inside sales, food service, car wash and lottery commissions more than the gallons.',
          'Listings offered for lease with no goodwill are operating rights on someone else’s site, not a business with a sales history. They are a different kind of deal from a business for sale.',
        ],
      },
      {
        heading: 'Fuel supply and branding',
        body: [
          'Branded stations buy fuel under a supply agreement with a distributor (jobber) or oil company. The agreement sets how fuel is priced, the minimum volume, how long it runs and whether it can be assigned to a new owner.',
          'Leaving a brand early can trigger repayment of incentives the original owner received for signing or rebranding. Ask for the supply agreement and any incentive schedule before you make an offer.',
          'Unbranded stations buy at the rack price and keep pricing control, but give up the brand’s traffic and card programs.',
        ],
      },
      {
        heading: 'Environmental review comes first',
        body: [
          'Underground storage tanks are regulated by the EPA and state agencies. Ask for tank installation dates and materials, leak detection and tank tightness test records, compliance inspections, and any history of releases or cleanups.',
          'A Phase I environmental site assessment is standard when the real estate is included, and lenders usually require it. If it finds a problem, a Phase II with soil and groundwater testing follows.',
          'Some states run funds that help pay for cleanup of past leaks. Find out whether the site is enrolled and in good standing.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Fuel gallons by month from distributor invoices, and inside sales from the point-of-sale system and sales tax filings. Compare both with bank deposits.',
          'Every license that has to transfer: tobacco, beer and wine, lottery, and food service. Each has its own process and timeline.',
          'The age of pumps, dispensers and payment equipment, and whether any upgrade is due under card network or state rules.',
          'Traffic, visibility and access from both directions, and any road work or new competing stations planned nearby.',
        ],
      },
    ],
    faqs: [
      { q: 'How are gas stations for sale priced?', a: 'Stations with real estate are priced on the combined value of the land, improvements and business earnings. Business-only stations are priced on earnings, mostly inside-store profit, plus inventory. The median asking price at the top of this page is calculated from the listings below.' },
      { q: 'Can I get an SBA loan for a gas station?', a: 'Gas stations are financed with SBA 7(a) and 504 loans. Expect lenders to require a Phase I environmental report, and sometimes more testing, before they approve the loan.' },
      { q: 'Is inventory included in the asking price?', a: 'Often not. Fuel and store inventory are commonly counted at closing and paid for on top of the asking price. The listing or the broker should say which.' },
      {
        q: 'Where do these gas station listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },

  liquor_store: {
    title: 'Liquor Stores for Sale: {n} Liquor Stores & Licenses Listed by Brokers | DealLedger',
    sections: [
      {
        heading: 'The license is the business',
        body: [
          'Liquor licensing is set state by state, and it decides what a liquor store is worth. In states that cap the number of retail licenses, the license itself can carry much of the price. In states that issue licenses freely, the value sits in the location and the store’s earnings.',
          'In control states, the state runs the wholesale or retail sale of spirits, which limits what a private store can sell and how it prices. Know which system the state uses before you look at the numbers.',
          'A license usually transfers only after the state and often the town approve the buyer, with background checks and sometimes a public hearing. Make the purchase contingent on that approval.',
        ],
      },
      {
        heading: 'What sets the price of a liquor store',
        body: [
          'Established stores are priced on owner’s earnings plus the license value, and inventory is usually counted and paid for separately at closing. Ask whether the asking price includes inventory.',
          'Margins on beer, wine and spirits are modest, so volume, product mix and shrink control drive profit. Stores that add lottery, tobacco, mixers and snacks earn more per customer.',
          'The lease and location matter as much as anywhere in retail: parking, visibility and nearby grocery or big-box competitors.',
        ],
      },
      {
        heading: 'What to check before you make an offer',
        body: [
          'Purchase records from distributors compared with reported sales. Distributor invoices are the best check on revenue in a cash-heavy store.',
          'Sales tax returns, point-of-sale reports and bank deposits for at least two years.',
          'The license type, any violations on record, and how long transfer takes in that state and town.',
          'A physical inventory count, with aged or slow-moving stock priced accordingly.',
        ],
      },
    ],
    faqs: [
      { q: 'Does the liquor license transfer with the store?', a: 'In most states it can, after the buyer is approved by the state licensing agency and sometimes the local authority. Some licenses are tied to the location. The purchase should be contingent on approval.' },
      { q: 'How are liquor stores priced?', a: 'On earnings plus the value of the license, with inventory usually added at cost at closing. The median asking price at the top of this page is calculated from the listings below.' },
      {
        q: 'Where do these liquor store listings come from?',
        a: 'Directly from business broker websites. DealLedger records when each listing first appears and when it comes down, so each listing’s age reflects only what we have actually observed, and every listing links straight to the broker.',
      },
    ],
  },
};
