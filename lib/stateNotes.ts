// lib/stateNotes.ts — one or two state-specific lines for the type-in-state pages that
// carry real search demand (claude/seo-keyword-targets.md, Tiers 3 and 5).
// Facts about rules and regulators only: no counts, prices or listing ages here; every
// number on the page comes from the live stats. Keep each line checkable against the
// named agency's own site.

export const STATE_NOTES: Record<string, Record<string, string>> = {
  laundromat: {
    NJ: 'In New Jersey, water and sewer rates are set by local utilities and vary widely from town to town, so ask for a full year of water and sewer bills: they move a laundromat’s margins as much as rent does.',
    NY: 'Laundries that wash, dry or fold clothes for customers in New York City need a license from the city’s Department of Consumer and Worker Protection; ask whether the store holds one and how much revenue comes from wash-and-fold and delivery.',
    TX: 'Outside city limits, many Texas stores get water from a municipal utility district (MUD) rather than a city utility, and MUD rates can differ sharply. Ask who supplies the water and for a year of bills.',
    PA: 'In Pennsylvania, water and sewer are often billed by separate local authorities. Get both sets of bills for a full year to check reported revenue and costs.',
    CT: 'Connecticut stores buy water from a mix of municipal and private utilities with different rates. Ask for a year of water and sewer bills alongside the profit and loss statement.',
  },
  gas_station: {
    TX: 'Underground storage tanks in Texas are regulated by the Texas Commission on Environmental Quality (TCEQ). Ask for the tank registration, compliance and leak-detection records.',
    OH: 'In Ohio, underground storage tanks are regulated by the Bureau of Underground Storage Tank Regulations (BUSTR) in the State Fire Marshal’s office. Ask for tank registration and inspection records.',
    NY: 'New York regulates gas station tanks under the Department of Environmental Conservation’s Petroleum Bulk Storage program. Ask for the facility’s registration and inspection history.',
    FL: 'Florida’s Department of Environmental Protection regulates storage tanks. Ask for tank registration, compliance inspections and any discharge or cleanup history.',
    NJ: 'New Jersey requires attendants to pump gas, so payroll is a larger share of costs than in most states. Tanks are regulated by the Department of Environmental Protection.',
    MD: 'Maryland’s Department of the Environment regulates underground storage tanks through its Oil Control Program. Ask for tank registration and inspection records.',
    CA: 'California regulates underground storage tanks through the State Water Resources Control Board and local agencies (CUPAs). Ask for permits, inspection reports and leak-detection records.',
    IN: 'The Indiana Department of Environmental Management (IDEM) regulates underground storage tanks. Ask for tank registration, inspections and any release history.',
    OK: 'In Oklahoma, fuel storage tanks are regulated by the Corporation Commission’s Petroleum Storage Tank Division. Ask for tank registration and compliance records.',
    MN: 'The Minnesota Pollution Control Agency (MPCA) regulates storage tanks. Ask for tank registration, inspections and any leak-site history.',
    GA: 'Georgia’s Environmental Protection Division regulates underground storage tanks. Ask for tank registration, inspections and any release history.',
    IL: 'In Illinois, underground storage tanks are regulated by the Office of the State Fire Marshal, with cleanups overseen by the Illinois EPA. Ask for registration and inspection records.',
    MI: 'Michigan’s Department of Environment, Great Lakes, and Energy (EGLE) regulates underground storage tanks. Ask for tank registration, inspections and any release history.',
    AZ: 'The Arizona Department of Environmental Quality (ADEQ) regulates underground storage tanks. Ask for tank registration, inspections and any release history.',
    VA: 'Virginia’s Department of Environmental Quality regulates storage tanks. Ask for tank registration, inspections and any release history.',
    NC: 'North Carolina’s Department of Environmental Quality regulates underground storage tanks. Ask for tank registration, inspections and any release history.',
    MA: 'In Massachusetts, underground storage tanks are regulated by MassDEP. Ask for tank registration, third-party inspection reports and any release history.',
  },
  liquor_store: {
    NJ: 'New Jersey limits the number of retail liquor licenses each town can issue based on population, so licenses are scarce and are bought and sold. A package store needs a plenary retail distribution license, and transfers need municipal and state approval.',
    NY: 'Liquor stores in New York are licensed by the State Liquor Authority, which must approve a new owner before the sale closes. New York liquor stores sell wine and spirits; beer is sold by groceries and delis.',
    TX: 'Package stores in Texas hold permits from the Texas Alcoholic Beverage Commission (TABC), which must approve a new owner. Texas liquor stores are closed on Sundays.',
    MD: 'In Maryland, liquor licenses are issued by the local liquor board of each county or Baltimore City, not the state, and each board sets its own transfer rules.',
    IL: 'An Illinois liquor store needs both a state license from the Illinois Liquor Control Commission and a local license from the city or county, and both must approve a new owner.',
    CA: 'California liquor stores need an off-sale general license (Type 21) from the Department of Alcoholic Beverage Control. New Type 21 licenses are limited by county population, so most buyers acquire an existing one with the store.',
    DC: 'In Washington, DC, liquor stores hold a Class A retailer license from the Alcoholic Beverage and Cannabis Administration, and some neighborhoods have moratoriums that limit new licenses.',
    GA: 'A Georgia liquor store needs a local license from the city or county and a state license from the Department of Revenue. Local rules, including distance limits from schools and churches, vary.',
    CT: 'Connecticut package store permits are issued by the Department of Consumer Protection’s Liquor Control Division, which must approve a new permittee.',
    MN: 'Liquor stores in Minnesota hold off-sale licenses issued by the city, and some cities run municipal liquor stores. Grocery and convenience stores cannot sell spirits or full-strength wine.',
    MI: 'Michigan is a control state for spirits: the state sets liquor prices and acts as the wholesaler. Stores selling spirits need a specially designated distributor (SDD) license, which is limited by population.',
    CO: 'Colorado grocery and convenience stores have sold full-strength beer since 2019 and wine since 2023, which changed the competition liquor stores face. Licenses are issued by the state and the local authority.',
    MA: 'Massachusetts towns issue a limited number of retail package store licenses based on population, and the Alcoholic Beverages Control Commission must approve a transfer.',
    AZ: 'Arizona off-sale liquor stores hold a Series 9 license. The number is limited by population, so existing licenses are bought and sold on the open market.',
  },
};

export const stateNote = (vertical: string, code: string): string | null => STATE_NOTES[vertical]?.[code] ?? null;
