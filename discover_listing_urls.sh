#!/bin/bash
# Auto-discover listing-page URLs for broker domains.
# Input:  a file with one domain per line (no scheme)
# Output: DOMAIN|WORKING_URL|HTTP_CODE|SIZE   (only 200s with meaningful size)
#
# Usage:  ./discover_listing_urls.sh /tmp/gap_domains.txt | tee /tmp/found_urls.txt

PATHS=(
  "businesses-for-sale" "businesses-for-sale/" "listings" "listings/"
  "business-listings" "business-listings/" "for-sale" "for-sale/"
  "opportunities" "buy-a-business" "current-listings" "our-listings"
  "search" "business-for-sale" "available-listings" "businesses"
  "practices-for-sale" "routes-for-sale" "restaurants-for-sale"
)

while read -r d; do
  [ -z "$d" ] && continue
  found=""
  # 1) try the wp-json probe first (cheapest + best outcome)
  wp=$(curl -s -m 5 "https://$d/wp-json/wp/v2/types" 2>/dev/null | python3 -c "
import sys,json
DEF={'post','page','attachment','nav_menu_item','wp_block','wp_template','wp_template_part','wp_global_styles','wp_navigation','wp_font_family','wp_font_face'}
KEY=('listing','propert','business','deal','route','practice','opportunit','compan')
BAD=('tombstone','done-deal','done_deal','job_listing','companynews','property_types','rps_listing')
try:
    j=json.load(sys.stdin)
    hits=[k for k in j if k not in DEF and any(x in k.lower() for x in KEY) and not any(b in k.lower() for b in BAD)]
    print(hits[0] if hits else '')
except: pass
" 2>/dev/null)
  if [ -n "$wp" ]; then
    n=$(curl -sI -m 6 "https://$d/wp-json/wp/v2/$wp?per_page=1" 2>/dev/null | grep -i "^x-wp-total:" | tr -d '\r' | awk '{print $2}')
    [ -n "$n" ] && [ "$n" != "0" ] && { echo "$d|WPREST:$wp|$n"; continue; }
  fi
  # 2) otherwise probe common listing paths
  for p in "${PATHS[@]}"; do
    read -r code size < <(curl -s -o /dev/null -m 5 -L -w "%{http_code} %{size_download}" "https://$d/$p" 2>/dev/null)
    if [ "$code" = "200" ] && [ "${size:-0}" -gt 12000 ]; then
      echo "$d|https://$d/$p|$code|$size"
      found=1
      break
    fi
  done
  [ -z "$found" ] && echo "$d|NOT_FOUND||"
done
