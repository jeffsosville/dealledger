"""Vested Business Brokers scraper — uses listing_search_ajax.php POST endpoint."""
import requests
from bs4 import BeautifulSoup
import time

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Referer': 'https://www.vestedbb.com/advancesearch/index.html',
}

def scrape(max_pages=120, verbose=True):
    listings = []
    
    for page in range(1, max_pages + 1):
        if verbose:
            print(f"  [vestedbb] page {page}/{max_pages} ({len(listings)} so far)")
        
        r = requests.post(
            'https://www.vestedbb.com/script/listing_search_ajax.php',
            data={
                'page': page,
                'default_search': 1,
                'business_type': '',
                'stateid': '',
                'countyid': '',
                'asking_price_min': '',
                'asking_price_max': '',
                'cash_flow_min': '',
                'cash_flow_max': '',
                'listing_key': '',
                'owner_financed': '',
                'down_pay': '',
                'sort_by': 'ListingDate:HL',
            },
            headers=HEADERS,
            timeout=20
        )
        
        if r.status_code != 200:
            print(f"  [vestedbb] HTTP {r.status_code} on page {page}, stopping")
            break
        
        soup = BeautifulSoup(r.text, 'html.parser')
        boxes = soup.select('.product-box')
        
        if not boxes:
            print(f"  [vestedbb] no listings on page {page}, stopping")
            break
        
        for box in boxes:
            try:
                # Title
                name = box.select_one('.name a')
                title = name.get_text(strip=True) if name else None
                if not title:
                    continue
                
                # URL
                source_url = name.get('href') if name else None
                if not source_url:
                    onclick = box.get('onclick', '')
                    if 'redirectURL' in onclick:
                        source_url = onclick.split("'")[1]
                
                # Price from .price-box li
                asking_price = None
                for li in box.select('.price-box li'):
                    txt = li.get_text(strip=True)
                    if 'Asking' in txt and '$' in txt:
                        price_str = txt.split('$')[1].replace(',','').strip()
                        try:
                            asking_price = int(float(price_str))
                        except:
                            pass
                        break
                
                # Location + state
                location = box.select_one('.location')
                loc_text = location.get_text(strip=True) if location else ''
                state = ''
                if ',' in loc_text:
                    state = loc_text.split(',')[-1].strip()[:2].upper()
                
                listings.append({
                    'title': title,
                    'source_url': source_url,
                    'asking_price': asking_price,
                    'location': loc_text,
                    'state': state,
                    'broker_name': 'vested business brokers',
                    'broker_url': 'https://www.vestedbb.com',
                })
            except Exception as e:
                continue
        
        time.sleep(0.5)
    
    print(f"  [vestedbb] total: {len(listings)} listings")
    return listings

if __name__ == '__main__':
    results = scrape(max_pages=3)
    print(f"\nSample:")
    for r in results[:3]:
        print(r)


# BaseScraper-compatible wrapper for run_all.py auto-discovery
try:
    from scrapers.base import BaseScraper
    class VestedBBScraper(BaseScraper):
        broker_id = 'vestedbb'
        broker_name = 'vested business brokers'
        def run(self):
            return scrape(max_pages=120, verbose=True)
except ImportError:
    pass
