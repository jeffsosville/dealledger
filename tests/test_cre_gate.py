"""CRE / lease gate, two-signal rule (job 3, 2026-11-12; principle 8 both ways).

looks_like_cre_or_lease() runs on every list-page card in from_card(). The old
single-keyword rule rejected operating businesses that print their own premises
lease: peakbusinessbrokersllc.com's Torrance staffing agency ("Monthly Lease
Rate: $4,200") and 6 laundromat-broker.com cards ("NNN"). Measured on the live
index pages 2026-11-12: 7 listing cards rejected, all 7 real businesses.

MUST_PASS  real business card texts (verbatim from the fetched index pages, or
           verbatim titles from data/snapshots/2026-11-11 plus the row's own
           asking price / cash flow, which is how a card shows them).
MUST_REJECT property / lease-only texts from real CRE sites in the snapshots
           (towerbusinessbrokers, nextwavecommercial, passive.investments,
           harwoodcommercialgroup, cannamls, accelvalue, reputedbrokerage,
           biz-match, riverbrokers, midtownmergers, corbettrestaurantgroup,
           naz-business) and CRE_LEASE_DOMAINS' malonecb pricing format.
Run: python3 -m pytest -q tests/test_cre_gate.py   (offline, no Supabase)
"""
import os
import sys

import pytest
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scrapers"))
import dealledger_scraper_v6 as v6  # noqa: E402

PEAK_INDEX = "https://peakbusinessbrokersllc.com/business-listings"

# peakbusinessbrokersllc.com card 33 (fetched 2026-11-12), text as from_card sees it.
TORRANCE_TEXT = (
    "Location: Torrance, California Asking Price: $180,000 Established Staffing Agency "
    "This business provides a full line of staffing services (temporary, flexible, "
    "evaluation hire, direct hire, and professional/contract staffing) and HR Business "
    "Solutions to individuals and businesses. This is a sales-focused, business to "
    "business opportunity in one of the top ten fastest growing industries in the US. "
    "Our model allows you to develop a professional business coupled with the ability "
    "to maintain a balanced lifestyle, and build a solid, scalable, equitable business. "
    "Our mission is to help as many people as possible find good jobs by helping as "
    "many clients as possible find good people. 3 Employess. Square Footage: 2,400 "
    "Monthly Lease Rate: $4,200"
)
TORRANCE_HTML = '<div class="elementor-container elementor-column-gap-default">\n<div class="elementor-column elementor-col-33 elementor-top-column elementor-element elementor-element-5e39190" data-element_type="column" data-id="5e39190">\n<div class="elementor-widget-wrap elementor-element-populated">\n<div class="elementor-element elementor-element-f24d02d elementor-widget elementor-widget-text-editor" data-element_type="widget" data-id="f24d02d" data-widget_type="text-editor.default">\n<div class="elementor-widget-container">\n<img src="/business_photos/141.jpg" style="border: 2px #000 solid;"/><br/><br/>\n<strong>Location:</strong> Torrance, California<br/><br/>\n<strong>Asking Price:</strong> $180,000<br/><br/>\n</div>\n</div>\n</div>\n</div>\n<div class="elementor-column elementor-col-66 elementor-top-column elementor-element elementor-element-4c68ff8" data-element_type="column" data-id="4c68ff8">\n<div class="elementor-widget-wrap elementor-element-populated">\n<div class="elementor-element elementor-element-3110d2e elementor-widget elementor-widget-text-editor" data-element_type="widget" data-id="3110d2e" data-widget_type="text-editor.default">\n<div class="elementor-widget-container">\n<strong>Established Staffing Agency</strong><br/><br/>This business provides a full line of staffing services (temporary, flexible, evaluation hire, direct hire, and professional/contract staffing) and HR Business Solutions to individuals and businesses. <br/>\nThis is a sales-focused, business to business opportunity in one of the top ten fastest growing industries in the US. Our model allows you to develop a professional business coupled with the ability to maintain a balanced lifestyle, and build a solid, scalable, equitable business. Our mission is to help as many people as possible find good jobs by helping as many clients as possible find good people.<br/>\n3 Employess.<br/>\nSquare Footage: 2,400 Monthly Lease Rate: $4,200\n             </div>\n</div>\n</div>\n</div>\n<!--\n       <div class="elementor-column elementor-col-33 elementor-top-column elementor-element elementor-element-2fdb97e" data-id="2fdb97e" data-element_type="column">\n         <div class="elementor-widget-wrap elementor-element-populated">\n           <div class="elementor-element elementor-element-c9df7b1 elementor-widget elementor-widget-text-editor" data-id="c9df7b1" data-element_type="widget" data-widget_type="text-editor.default">\n             <div class="elementor-widget-container">\n               &nbsp;\n             </div>\n           </div>\n         </div>\n       </div>\n-->\n</div>'

# laundromat-broker.com/listings.html cards (fetched 2026-11-12), verbatim. The
# first six died on "NNN" under the old rule; the last three were false rejects
# of the first draft of the new rule ("tenant", "strip mall").
LAUNDROMAT_CARDS = [
    'How does getting a return on your investment in year 1 sound? Stay and build with current concept (recommended) or create your own. This restaurant is poised to grow. A working owner can position this to $80,000. Even better, this restaurant is closed 1 day a week and mornings, convert that to working hours and the possibilities for $100,000 plus are realistic. $3400 Rent includes all NNN, cam, taxes, etc.. No surprises and plaza is extremely well maintained.Approx 2,000 sq ft. 10 Points for Success: 1) Restaurant is 6+ years old, 2) Extremely clean, 3) Equipment in excellent working condition, 4) Fully equipped with a build out estimated over $150,000, 5) Seasoned staff, 6) Low labor costs, 7) Simple but unique menu, 8) Spacious & well appointed interior, 9) 30+ indoor seats & 10+ outdoor seats, 10) Plenty of room for growth. Great location in the Southbury/Woodbury area in well maintained complex off main street.',
    "24 Hour laundromat. \u200b Neighborhood location surrounded by 2-3 family homes New Build- Laundromat was built 2 years ago from scratch- Everything is new. Over $350,000 invested in the store. Landlord is seller and doesn't have time to be an operator. Looking for a good buyer and tenant. They will do a new 10 year lease. Rent is $1500 for approx. 1000 sq. ft They have 2 part time attendants who come in 4-5 hours a day to clean and handle wash & fold. Store is unattended the other 18-20 hours. 10 washers & 12 dryers about 2 years old. VTM card system so no coins to collect. Building and location in excellent shape with ample parkin Great opportunity to build out wash & fold. Everything is here and NEW so you just need to come in and run business. No capital improvements or spending needed for many years. Cash offers only $235,000 Ad#:2391498",
    '\u200bCoin Laundry in stand alone building with plenty of parking off a main street. Store is profitable but in need of upgrades and new equipment as reflected by the price. Great entry price to get into the business and upgrade store with some new equipment and cosmetic updates. Store has been here over 30 years. 16 Washers & 19 Dryers about 25-35 years old. Approx. 10 out of service. New buyer should consider investing in a little less equipment based on size of store and add bathroom as well as additional folding tables and seats. Perfect retool opportunity. Great neighborhood location in are of 2-3 family homes. Landlord is seller and looking for a good tenant. Rent with NNN will be approx. $1600 all in for approx. 1000+ sq ft. Landlord will do a new 10 year lease and easy to work with, Great opportunity to : 1) increase and bring back self serve clients with an improved facility and new/used equipment, 2) develop wash & fold and 3) PUD (Pick Up & Delivery). Laundromats in this price range rarely come up. $35,000 List Price\xa0 \xa0Cash Offer Only\xa0 \xa0 \xa0No Financing',
    'Established Laundromat near Akron in stand alone building shared with one other tenant. The store has been here over 20 years but recently closed (about 4-5 months ago). Excellent location surrounded by apartments. 16 Dryers & 13 Washers approx. 15-20 years old. About 1/2 are out of service. VERY IMPORTANT: Store\xa0 needs cosmetic updates (paint, ceiling tiles as well as new water heater) and about 1/2 the machines work (estimated) so this is being priced as a retool. You can repair existing machines or buy and invest in new. Please note that existing machines have no keys for coin box so they will need to be replaced. Can be run unattended or attended. Lots of potential especially at this price. Has room/office for wash & fold if new buyer wants to add this service. Landlord will do a new 10 year lease. Great Rent starting at $1200 for 1,000 sq ft. Great opportunity to revitalize this laundromat and increase services. $35,000 FIRM No Offers less than this will be accepted.',
    'This store is in a GREAT location. It is surrounded by apartments and is in a well appointed stand alone building with only 1 other tenant and has PLENTY of parking lot. This is a totally absentee and unattended store with a card system in place. Lots of potential to start a Wash & Fold as well as a Pick Up Delivery (PUD) service as the store currently does not offer either. Great rent of $1400 and NNN is estimated to be about $300 month so you are about $1400 all in for 1400 sq ft and Landlord will offer a new 10 year lease. P&L assumes buyer cleans store daily but Seller has someone in place who can continue for approx. $800 month who is also handy with machine repair. 26 washers & 22 dryers approx. 20 years old in good working condition so you have time to come in and settle in and build sales/revenue further before investing in some new or used equipment. You can always buy, upgrade and finance new equipment but you cannot buy such a great location. $149,000\xa0 \xa0 Cash Offers only- No Financing.',
    'Very Large Laundromat in North Houston. This store is coin operated and is approx. 3500 sq ft with 49 washers and 48 dryers. Monthly rent is $5,400 and landlord will do a new 10 year lease. Strip mall has plenty of parking. The seller has owned approx. 7 years but has had a difficult time keeping up and got behind on machine repairs. Even though the machines are only about 20-25 years old, about 50% are out of service. The new buyer should have a mechanic come in to look at repairing them before replacement since they still have some years left of service left based on age. Also, great opportunity to retail and start bring in new equipment little by little. This is a complete package: The following is a list that is included: 2 top loader washers, 15 lb. 16 Huebsch front load washers, 20 lb. 18 Huebsch front load washers, 30 lb. 7 Huebsch front load washers, 40 lb. 6 Huebsch front load washers, 60 lb. 13 Huebsch stack dryers, 35 lb. double. 7 Huebsch stack dryers, 45 lb. double. 2 Huebsch 75 lb. single stack dryers/tumblers. 2 commercial water heaters, 100 gallons each. 3 coin change machines. 1 drink vending machine. 1 snack vending machine. Dry clean conveyor, 20 ft long. Security camera system. Cash register. Office computer and printer. Laundry carts. Folding tables. Chairs. Refrigerators. Arcade game machines. Kiddie ride. Coin pusher. Gumball machine. Toy dispenser. Soap cabinet. Cashier counter. 55-inch LED TV. Other furniture, fixtures, and business equipment. The owner and his wife work the store. It can be run unattended or attended and is operating about a breakeven. The business is priced for a motivated sale and for a buyer/operator or investor who is ready to invest time and capital to improve operations and build the business back up. The infrastructure alone (plumbing, eating and gas lines) would cost substantially more than the asking price to install in a new space. Please note that this store also has lots of potential as it has not only been managed but no advertising and not even a Google Listing. List Price - $100k Cash Offer Only- No bank or SBA Financing',
    "Well Appointed Laundromat that is run mostly unattended with the exception of part-time attendant working 7-10 hours per week depending on wash & fold orders to process. Located in well appointed plaza with plenty of parking and excellent mix of stores with excellent vehicle and foot traffic. Coin Operated store. Seller purchased a little over a year ago but with their full-time jobs and husband nearing retirement, they are planning/considering moving. They haven't had the time to build the wash & fold business, which is the key to the store's success. THE OPPORTUNITY: They currently run the business absentee and unattended with a part-time worker coming in 7-10 hour a week. Drop off hours are only 9-12 Mon to Friday, and normally a call is needed for someone to be there. You cannot run a vibrant wash & fold business with scattered hours just 9-12 and by phone appointment and not on weekends. You need steady consistent hours throughout 7 days a week as people want/need to drop off and pick up from the staff when it's convenient to them, not you. They currently do about $675 in wash & fold monthly with a commercial account giving them and additional $775 monthly for a total of $1450 monthly with just 7-10 hours a week of attendant hours. This is the KEY. Wash & fold garners $1.50 lb. from drop off and they do a handful of PUD's (Pick Up & Delivery) for $2 lb. a month. Very few PUD and its about 5 pickups.\xa0 The pickups are handled by the worker. She is willing to take on more hours and is an excellent worker who cleans daily and has been there for over 2 years. Auto locks open at 5AM and close at 11PM with an excellent camera system that can be viewed from your phone. Lease has approx. 9 years left and the store is approx. 1500-1700 sq ft. Excellent gross rent =$1,160 with no CAM or NNN. 20 Washers A) 5 washers are approx. 3 years old\xa0 (one = 80lb, two = 60ln & 2 = 40lb). B) 15 washers are approx. 20 years sold. 20 dryers are approx. 15-20 years old. Note: Vending machines and some tools are not included in price and can be purchased separately. Arcade games are not included and cannot be purchased separately. Great opportunity to build on an existing wash & fold business that does not have the necessary hours to grow, List Price = $140,000 Firm \xa0 \xa0 Cash Sales Only\xa0 \xa0 \xa0 No Seller, SBA or Bank Loans",
    "This laundromat checks all the boxes: 1)\xa0 \xa0Open 24 Hours. 2)\xa0 Well appointed store in well maintained building. 3)\xa0 Surrounded by 2-3 family homes. Laundromat has been here over 25 years. 4)\xa0 Good parking. 5)\xa0 34 BRAND NEW machines installed 10/1/25 with card system. 6)\xa0 High capacity machines: Washers:\xa0 (6 = 60lb, 6 = 40lb & 4 = 20lb). Dryers:\xa0 \xa0 \xa0 (4 - 50lb & 14 = 35lb). 7)\xa0 1,533 sq ft with office, bathroom & utility room. 8)\xa0 15 years left on lease (10 years plus 5 year option). 9)\xa0 POS system on machines for card transactions and POS for Wash/Fold & PUD (Pick Up & Delivery). 10) 20 year experienced employee attends store Mon-Fri from 11-6 & Sat 9-2. 11)\xa0 PUD Driver does picks up ups Tues, Wed & Thurs. 12) Uber & Door Dash integrated through POS pick up & deliver other times as needed. 13) Delivery van included in price with approx. 105,000 miles and well maintained. 14) State of the art website. 15) Cameras system in place\xa0 you can view from your phone. 16) PUD is approx. 1/2 of Wash & Fold number as shown on P&L. 17) This store is run absentee with owner having minimal involvement (checks in 1-2 a week) thanks in part to long term employee. However, please see Broker note below. 18) Some commercial accounts in place. 19) $3145 rent includes all NNN. 20 Everything is done here. Nothing to do but come in and keep building on the store's momentum. This store is growing each and every month with sales that can be verified since POS was installed in October 2025. This laundromat has operated for almost 30 years. It was purchased it in August 2025 as a coin-operated laundromat and fully retooled it with 34 new commercial machines in October 1, 2025, and converted to a cashless card system on October 1, 2025. The seller would genuinely like to keep it, but his family is relocating out of state in the next few months, which is driving the sale. Financing note: because of the recent conversion, this store doesn't have 2-3 years of tax returns, which most SBA lenders require. Financials from August–October 2025 exist but reflect the old coin operated system; anything after reflects the new card system. These can be shared during due diligence. This is an opportunity to get into a laundromat with new equipment, recently remodeled with nothing to do except walk in and keep building on the upward momentum. List Price = $589,000 FIRM (no lower offers will be accepted). Cash offers only (Certified check at closing or home equity loan).\xa0 \xa0 \xa0 \xa0 No bank, seller or SBA financing.",
    'Excellent location in Jersey City surrounded by 3-4 family homes and apartments in a well maintained building. Laundromat is in excellent condition, well appointed and busy. Approx. 2000 sq ft with excellent rent of $2758 (which includes all NNN and =CAM). Lease has 17 years left and has escalations of approx. 3% per year. Card Operated (SD CCI) store so all revenue can be tracked and wash & fold is tracked with Cents POS. 1) 3 PT employees who work approx. 40 hours a week. 2)Seller works store 8:30 to 5 Daily or approx. 50 hours a week and 3) Store is unattended approx. 22 hours a week from 6Am to 8:30. NOTE: The P&L accurately reflects hours added to payroll to cover owner owner hours to get to a true net profit and assumes a totally absentee model. . The store has auto lock doors. They do approx. $4,150 in self serve & approx. $2,300 (1846 lbs. @ $1.25) weekly. Seller has owned for 3 years but has family obligations requiring him to move on. 33\xa0 washers about 15 years old ( 13=20lbs, 15=40lbs & 5 = 60lbs) 34 dryers about 15 years old. All 30lb double stack. PRICED TO SELL: This store was priced using a 2.5-3 multiplier off the net income estimate of $177,000. $450,000 List Price Cash Offers only\xa0 \xa0 \xa0 No Seller, bank or SBA financing.',
]

MUST_PASS_TITLES = [
    # snapshot 2026-11-11 titles + the row's own figures
    "Established Pizzeria in Major Thousand Oaks Shopping Center Asking Price: $165,000",
    "Long established practice in growing suburb of Philadelphia. There are four modern "
    "ops in small shopping center adjacent to two main highways. Excellent parking and "
    "exposure. Practice grossing in the mid $300,000 range on a part-time basis. "
    "Asking Price: $300,000",
    "The water front restaurant is located on the turn basin at Port Canaveral and looks "
    "out over the entire port. The main building contains approximately 8,772 SF, with an "
    "enclosed porch that has approximately 868 SF, 770 SF Tiki Bar, and open porch seating "
    "of approximately 861 SF for a total seating area of approximately 11,271 SF, on 1.45 "
    "Acres. It has a remaining 27 year ground lease with the Canaveral Port Authority that "
    "costs around $80,000 per year and includes all the parking.",
    "Dry Cleaning Business with Building & Land \u2013 Redevelopment Opportunity "
    "Asking Price: $450,000 Cash Flow: $75,000",
    "NW Liquor Store rent $12,950/Month Asking Price: $1,000,000",
    "Premium private office management company - Flex Space / Coworking Asking Price: $650,000",
    "Exterior Maintenance Operations Hub \u2013 (2 Properties) Office & Warehouse Space "
    "Included Asking Price: $550,000",
    "Profitable Caf\u00e9 with Prime 9000 sq. ft. Real Estate Included Asking Price: "
    "$750,000 Cash Flow: $60,091",
    # reputedbrokerage.com saved index card (w5/raw): legacy did not reject it,
    # a draft of the new rule did ("NOI", "CAP") until fuel/gallons counted
    "Sold Not Available High Income Truck Stop in Heart of Texas $7,200,000 / "
    "Inside:$120K-$125K,Outside:110K-115K Gallons Gas Stations Rajesh Bhatia 2 years ago "
    "Inside Sale: $120K-$125K @37-38% Gas Sale :110k-115K Gal @20-25 cents Multi Income: "
    "$35-$40K NOI: $1 Million CAP: 14% Land Size: 18 Acres Asking Price: $7.2 Million",
    # a restaurant quoting its rent in $/SF NNN (lease figure + business evidence)
    "Turnkey Bar & Grill \u2013 Gross Sales $1.2M, Rent $24/SF NNN, Lease Term 10 years "
    "Asking Price: $395,000",
]

# accountingfirmsold.com index cards (saved page, job 3 W1): 8 of 188 cards die
# under the legacy rule on "triple net" in the Facilities line. Shortened to the
# opening and the Facilities clause; both halves verbatim.
AFS_TRIPLE_NET = (
    "MI3075 SE Michigan $427,000 $469,000 Geographic Location: SE MICHIGAN Practice "
    "Details: Outstanding tax practice just north of major metro area begun in 1992. "
    "Annual Revenue: $427,000 Owners Cash Flow: $234,000 Facilities: "
)

MUST_REJECT = [
    # towerbusinessbrokers.com (snapshot row; legacy also rejects)
    "Commercial Real Estate For Lease in Florida Asking Price: $26,000",
    # naz-business.com saved index card
    "Gas Station For Lease in Kenosha County, WI 2160 Asking Price:",
    # malonecb.com (CRE_LEASE_DOMAINS) pricing format, per the code comment
    "Office Space 2,150 SF $18.50 Annual/SF",
    # nextwavecommercial.com
    "Premier Coastal Office Space Asking Price: $5,000,000",
    "Landmark North Park Multi-Tenant Retail Building For Sale Asking Price: $52,000,000",
    # cannamls.com
    "2,800 SF of Retail Space Available in Atlantic City, NJ Asking Price: $6,500",
    # harwoodcommercialgroup.com
    "170 S Madison Ave Greenwood, IN 46142 FOR SALE Property Highlights Available Square "
    "Feet: 2,800 square feet Price: $449,000 Building Type: Retail / Office Condition: "
    "Exterior and structural renovation",
    # passive.investments
    "Fully Leased Street-Level Retail in the Heart of Downtown Kingsport \u2014 100% "
    "Occupied, Six Tenants | $619,000",
    "Aggressively Priced Downtown Corner \u2014 Retail or Office Below, Urban Lofts Above "
    "| For Sale or Lease $384,000",
    "$300,000,000 Multifamily Portfolio",
    # accelvalue.com
    "Two 6-acre Buildable Country Parcels in Salinas, CA Asking Price: $587,000",
    # reputedbrokerage.com (decision 7: lands are CRE)
    "Land Approved for Gas Station In Palestine, TX Asking Price: $900,000",
    # biz-match.com
    "Alice Texas Investment Property Package Asking Price: $1,195,000",
    # riverbrokers.com
    "Shane's Rib Shack Net-Lease | Ellijay, GA - River Brokers Asking Price: $1,899,000",
    # midtownmergers.com
    "Lillington Shopping Center for Sale Asking Price: $2,650,000",
    # corbettrestaurantgroup.com
    "Commercial Condo in Cambridge For Sale Asking Price: $1,300,000",
    # lease-figure-only property card: cap rate / NNN and no business evidence
    "Freestanding retail building, 6,000 SF, Cap Rate 6.25%, NNN lease to national "
    "tenant. Price: $2,150,000",
]


@pytest.fixture(autouse=True)
def _two_signal(monkeypatch):
    monkeypatch.setattr(v6, "CRE_GATE_MODE", "two_signal")


def test_shipped_default_is_narrow():
    # two_signal is opt-in until the owner reviews the rows it newly drops;
    # the other tests pin two_signal via the autouse fixture.
    src = open(v6.__file__).read()
    assert 'CRE_GATE_MODE = "narrow"' in src


def test_torrance_staffing_agency_passes():
    assert v6.cre_gate_reason(TORRANCE_TEXT) is None


def test_torrance_card_becomes_a_listing():
    el = BeautifulSoup(TORRANCE_HTML, "html.parser").select_one("div.elementor-container")
    card = v6.ListingExtractor.from_card(el, PEAK_INDEX, "peakbusinessbrokersllc.com")
    assert card is not None
    assert card["asking_price"] == 180000
    # (title extraction picks the card's 2nd sentence on this site, like the
    # other 32 peak rows; a separate, pre-existing issue, not asserted here)


@pytest.mark.parametrize("text", LAUNDROMAT_CARDS, ids=lambda t: t[:40])
def test_laundromat_cards_pass(text):
    assert v6.cre_gate_reason(text) is None


@pytest.mark.parametrize("text", MUST_PASS_TITLES, ids=lambda t: t[:40])
def test_business_with_property_words_passes(text):
    assert v6.cre_gate_reason(text) is None


@pytest.mark.parametrize("text", MUST_REJECT, ids=lambda t: t[:40])
def test_property_text_rejected(text):
    assert v6.looks_like_cre_or_lease(text), text


def test_legacy_flip_restores_old_behaviour(monkeypatch):
    monkeypatch.setattr(v6, "CRE_GATE_MODE", "legacy")
    assert v6.looks_like_cre_or_lease(TORRANCE_TEXT)          # the old false reject
    for t in LAUNDROMAT_CARDS + MUST_PASS_TITLES + MUST_REJECT:
        assert v6.looks_like_cre_or_lease(t) == bool(v6._LEASE_CRE_RE.search(t))


def test_narrow_mode_fixes_lease_figures_and_adds_no_rejects(monkeypatch):
    monkeypatch.setattr(v6, "CRE_GATE_MODE", "narrow")
    assert v6.cre_gate_reason(TORRANCE_TEXT) is None
    for t in LAUNDROMAT_CARDS + MUST_PASS_TITLES:
        assert v6.cre_gate_reason(t) is None
    # narrow never rejects anything legacy accepted
    for t in MUST_REJECT:
        if v6.looks_like_cre_or_lease(t):
            assert v6._LEASE_CRE_RE.search(t), t
    # and still rejects the strong legacy phrases
    assert v6.looks_like_cre_or_lease("Commercial Real Estate For Lease in Florida")
    assert v6.looks_like_cre_or_lease("Office Space 2,150 SF $18.50 Annual/SF")


def test_lease_figure_needs_second_signal():
    assert v6.cre_gate_reason("Suite 4B, 1,850 SF, lease rate $22.00/SF NNN") is not None
    assert v6.cre_gate_reason("Suite 4B, 1,850 SF, lease rate $22.00/SF NNN. "
                              "Cash Flow: $140,000") is None


def test_strong_phrase_beats_business_evidence():
    # "for lease" is a lease offering even with figures on the card (legacy
    # behaviour kept; listing_status drops a For Lease label too, decision 5)
    assert v6.looks_like_cre_or_lease(
        "Liquor/grocery Store For Lease Racine County 2089 Asking Price : $150,000 "
        "Gross Revenue: $700,000")


def test_accountingfirmsold_triple_net_facilities_passes():
    text = AFS_TRIPLE_NET + "triple net lease, 1600 square feet"
    assert v6._LEASE_CRE_RE.search(text)               # legacy rejected it
    assert v6.cre_gate_reason(text) is None


# ── wrapper matches (peak's 34th selector match) ─────────────────────────────
def _peak_like_page(n):
    cards = "".join(
        f'<div class="row"><div class="row-a"><b>Location:</b> Town {i}, Ohio '
        f'<b>Asking Price:</b> ${100 + i},000</div><div class="row-b"><strong>'
        f'Listing number {i} plumbing company</strong> Net Income: $50,000</div></div>'
        for i in range(n))
    return f'<div class="row"><div>Below is a current list of Businesses for Sale.</div>{cards}</div>'


def test_wrapper_holding_cards_is_dropped():
    soup = BeautifulSoup(_peak_like_page(5), "html.parser")
    els = soup.select("div.row")
    assert len(els) == 6
    is_card = lambda e: v6.ListingExtractor.from_card(e, PEAK_INDEX, "x") is not None
    kept, dropped = v6.drop_wrapper_matches(els, is_card)
    assert dropped == 1 and els[0] not in kept and len(kept) == 5


def test_card_with_nested_same_selector_block_is_kept():
    # one card holding two nested matches (image block, title copy) is a card
    html = ('<div class="c"><div class="c">photo</div><div class="c">Plumbing company '
            'for sale</div> Asking Price: $250,000 Net Income: $80,000</div>')
    els = BeautifulSoup(html, "html.parser").select("div.c")
    kept, dropped = v6.drop_wrapper_matches(els, lambda e: True)
    assert dropped == 0


def test_chain_of_nested_matches_counts_once():
    # buysellgeo-style: div[class*=elementor] matches a chain of nested blocks
    # that all read as the same card; a parent of 3 such chains holds 1 card.
    html = ('<div class="e0"><div class="e1"><div class="e2"><div class="e3">'
            'FOR SALE ID:7069 Somerset County restaurant Asking Price: $300,000'
            '</div></div></div></div>')
    els = BeautifulSoup(html, "html.parser").select('div[class^="e"]')
    kept, dropped = v6.drop_wrapper_matches(els, lambda e: True)
    assert dropped == 0
