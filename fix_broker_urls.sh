#!/bin/bash
# Fix MOVED broker URLs in brokers_clean.csv (from the 2026-07-16 URL health sweep)
# Run from the repo root. Makes a backup first.
cd /Users/jeffsosville/desktop/dealledger-repo || exit 1
cp data/brokers_clean.csv data/brokers_clean.csv.pre_urlfix

fix() {
  # $1 = old url, $2 = new url  (escaped for sed's | delimiter)
  local old="${1//|/\\|}" new="${2//|/\\|}"
  perl -pi -e "s|\Q$1\E|$2|g" data/brokers_clean.csv
}

# --- REBRANDS / real moves (highest value) ---
fix "https://sofrankoadvisors.com/for-sale/"                              "https://www.sagbrokerage.com/for-sale/"
fix "https://businessbrokersofutah.com/businesses-for-sale/"              "https://businessbrokersofamerica.com/businesses-for-sale/"
fix "https://www.fbb.com/businesses-for-sale/"                            "https://therockbridge.group/businesses-for-sale/"
fix "https://www.benjaminrossgroup.com/businesses-for-sale"               "https://listings.benjaminrossgroup.com/"
fix "https://www.kingsleybrokers.com/current-listings"                    "https://listings.kingsleybrokers.com/"
fix "https://unbroker.com/businesses-for-sale/"                           "https://flow.unbroker.com/"

# --- PATH CHANGES ---
fix "https://brookspointe.com/our-listings/"                              "https://brookspointe.com/businesses-for-sale/"
fix "https://greenbridgebrokers.com/listings/"                            "https://greenbridgebrokers.com/business-listings-for-sale/"
fix "https://mainstreetmain.com/our-listings"                             "https://mainstreetmain.com/listings"
fix "https://tangentbrokerage.com/search/"                                "https://tangentbrokerage.com/listings/"
fix "https://mavenoadvisors.com/businesses-for-sale/"                     "https://mavenoadvisors.com/search/"
fix "https://murraybizbuy.com/businesses-for-sale-2/"                     "https://murraybizbuy.com/businesses-for-sale/"
fix "https://omni-pg.com/practices-for-sale/"                             "https://omni-pg.com/practices-for-sale-2/"
fix "https://arizonarestaurantsales.com/restaurants-for-sale/"            "https://arizonarestaurantsales.com/restaurants-for-sale-in-arizona/"
fix "https://businessfinderscanada.com/listings-business"                 "https://businessfinderscanada.com/search-businesses-for-sale"
fix "https://www.californiabusinessbrokers.com/for-sale-2/"               "https://www.californiabusinessbrokers.com/businesses-for-sale-in-california/"
fix "https://www.jennessey.com/businesses-for-sale"                       "https://www.jennessey.com/business-listings"
fix "https://www.lbrokers.com/coin-laundry-stores-for-sale/new-stores-for-sale" "https://www.lbrokers.com/stores-for-sale"
fix "https://www.midwest-brokers.com/businesses-sale-listings/"           "https://www.midwest-brokers.com/businesses-for-sale/"
fix "https://www.promed-financial.com/search-our-listings/"               "https://www.promed-financial.com/search-our-listing/"
fix "https://www.psbroker.com/property-category/all-veterinary-practices-for-sale/" "https://psbroker.com/veterinary-practices-for-sale/"
fix "https://www.thevantgroup.com/businesses-for-sale/"                   "https://www.thevantgroup.com/business-listings/"
fix "https://waddellmergers.com/search-for-businesses-for-sale/"          "https://waddellmergers.com/buy-a-business/search-for-businesses-for-sale/"
fix "https://certifiedbb.com/listings/"                                   "https://certifiedbb.com/listings/?statuses=ACTIVE"
fix "https://evergreenbroker.com/cannabis-business-for-sale-listings-map/?&wmvc_view_type=list&order_by=field_6_NUMBER%20ASC#wdk_map_results" "https://evergreenbroker.com/listings?wmvc_view_type=list&order_by=field_6_NUMBER%20ASC"

# --- http -> https / www normalization / trailing slash ---
fix "http://bbfbrokers.com/all%20listings.html"                           "https://bbfbrokers.com/all%20listings.html"
fix "http://gomerritt.com/listings/"                                      "http://www.gomerritt.com/listings/"
fix "http://iwbusbrokers.com/listings/"                                   "https://iwbusbrokers.com/listings/"
fix "http://www.glasscitypartners.com/available-listings.html"            "https://www.glasscitypartners.com/available-listings.html"
fix "https://californiabizsales.com/businesses-for-sale/"                 "https://www.californiabizsales.com/businesses-for-sale/"
fix "https://www.aiellobrokers.com/pcategory/businesses-for-sale/"        "https://aiellobrokers.com/pcategory/businesses-for-sale/"
fix "https://www.aldrin.ca/businesses-for-sale/"                          "https://aldrin.ca/businesses-for-sale/"
fix "https://www.allenandyoung.com/businesses-for-sale/"                  "https://allenandyoung.com/businesses-for-sale/"
fix "https://www.aspectbrokers.biz/BusinessForSale/BusinessForSale"       "https://aspectbrokers.biz/BusinessForSale/BusinessForSale"
fix "https://www.biztsfr.com/listings"                                    "https://biztsfr.com/listings"
fix "https://www.mcreek.com/businesses-for-sale/"                         "https://mcreek.com/businesses-for-sale/"
fix "https://www.pgpadvisory.com/businesses-for-sale/"                    "https://pgpadvisory.com/businesses-for-sale/"
fix "https://www.proassetadvisors.com/listings"                           "https://proassetadvisors.com/listings/"
fix "https://www.squizzero.com/businesses-for-sale/"                      "https://squizzero.com/businesses-for-sale/"
fix "https://www.thedealfirm.com/businesses-for-sale/"                    "https://thedealfirm.com/businesses-for-sale/"
fix "https://www.truviewbusiness.com/businesses-for-sale/"                "https://truviewbusiness.com/businesses-for-sale/"
fix "https://www.leadingedgebrokers.com/businesses-for-sale/"             "https://leadingedgebrokers.com/businesses-for-sale/"
fix "https://www.bizbrokeragehub.com/listings"                            "https://bizbrokeragehub.com/listings/"
fix "https://www.crebbgroup.com/business-brokerage-listings"              "https://www.crebbgroup.com/business-brokerage-listings/"
fix "https://businesssellercenter.com/listings"                           "https://businesssellercenter.com/listings/"
fix "https://usapoolroutesales.com/listings"                              "https://usapoolroutesales.com/listings/"
fix "https://sbpoolroutes.com/route-listings"                             "https://sbpoolroutes.com/route-listings/"
fix "https://sellmyroutes.com/interested-in-buying-fedex-routes"          "https://sellmyroutes.com/interested-in-buying-fedex-routes/"
fix "https://www.sealeybb.com/routes-for-sale"                            "https://www.sealeybb.com/routes-for-sale/"
fix "https://www.tambaymergers.com"                                       "https://www.tambaymergers.com/"
fix "https://www.thebusinessbrokerexperts.com/buy-a-business"             "https://www.thebusinessbrokerexperts.com/buy-a-business/"
fix "https://www.fnbcusa.com/buying-a-business#"                          "https://www.fnbcusa.com/buying-a-business"
fix "https://theveldgroup.com/main-street-businesses-for-sale/#"          "https://theveldgroup.com/main-street-businesses-for-sale/"
fix "https://baltimorebusinessbrokers.com/business?"                      "https://baltimorebusinessbrokers.com/business"
fix "https://gabb.org/businesses-for-sale/"                               "https://gabb.org/businesses-for-sale"
fix "https://acquisitionsdirect.com/buy/"                                 "https://acquisitionsdirect.com/"
fix "https://aria.net/listings/"                                          "https://www.aria.net/listings"

echo "Done. Diff:"
diff <(sort data/brokers_clean.csv.pre_urlfix) <(sort data/brokers_clean.csv) | head -40
echo "---"
echo "Changed lines: $(diff data/brokers_clean.csv.pre_urlfix data/brokers_clean.csv | grep -c '^<')"
