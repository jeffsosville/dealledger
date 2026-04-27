class LarryBodnerScraper:
    """
    Executive Business Brokers (Larry Bodner)
    https://execbb.com

    POSTs search form and parses HTML table directly.
    No detail page visits needed — all data is in the results table.
    Loops through NJ, NY, CT, PA to get all listings.
    """

    BASE = "https://execbb.com"
    SEARCH_URL = f"{BASE}/buyer/sub/search.asp"
    RESULTS_URL = f"{BASE}/Buyer/sub/results.asp?searchtype=incsearch"

    STATES = ['NJ', 'NY', 'CT', 'PA', 'al', 'AZ', 'CA', 'co', 'fl',
              'ga', 'il', 'in', 'md', 'ma', 'mi', 'mn', 'mo', 'nc',
              'oh', 'or', 'pa', 'sc', 'tn', 'tx', 'va', 'wa', 'wi']

    def __init__(self):
        self.session = requests.Session(impersonate="chrome120")
        # Establish session cookies
        try:
            self.session.get(self.SEARCH_URL, timeout=15)
        except Exception as e:
            print(f"[Bodner] Session init warning: {e}")

    def _fetch_state(self, state: str) -> List[Dict]:
        """POST search form for a state and parse results table."""
        data = {
            'Category': 'all',
            'State': state,
            'County': '0',
            'AllListings': 'ON',
            'searchtype': 'IncSearch'
        }
        try:
            r = self.session.post(self.RESULTS_URL, data=data, timeout=30)
            r.raise_for_status()
        except Exception as e:
            print(f"[Bodner] Error fetching {state}: {e}")
            return []

        soup = BeautifulSoup(r.text, 'html.parser')
        listings = []

        # Find all listing links by ID pattern
        import re as _re
        listing_links = soup.find_all('a', href=_re.compile(r'listingdetail\.asp'))

        for link in listing_links:
            try:
                href = link.get('href', '')
                title = link.get_text(strip=True)
                if not title or not href:
                    continue

                # Full URL
                if href.startswith('http'):
                    url = href
                else:
                    url = f"{self.BASE}/Buyer/sub/{href.lstrip('/')}"

                # Extract listing ID from href
                id_match = _re.search(r'listingid=([A-Z0-9]+)', href, _re.I)
                listing_id = id_match.group(1) if id_match else None

                # Parent row has title, sibling row has price/cf/location
                title_row = link.find_parent('tr')
                if not title_row:
                    continue

                next_row = title_row.find_next_sibling('tr')
                cells = next_row.find_all('td') if next_row else []

                # Cells: [description, blank, revenue, price, location]
                price_text = None
                revenue_text = None
                location = None
                description = None

                if len(cells) >= 1:
                    description = cells[0].get_text(strip=True)
                if len(cells) >= 3:
                    revenue_text = cells[2].get_text(strip=True)
                    if revenue_text in ['', '\xa0', 'Undisclosed']:
                        revenue_text = None
                if len(cells) >= 4:
                    price_text = cells[3].get_text(strip=True)
                    if price_text in ['', '\xa0']:
                        price_text = None
                if len(cells) >= 5:
                    location = cells[4].get_text(strip=True)
                    if location in ['', '\xa0']:
                        location = None

                city, st = extract_city_state(location or f", {state}")
                if not st:
                    st = state

                listings.append({
                    'url': url,
                    'title': title,
                    'price_text': price_text,
                    'revenue_text': revenue_text,
                    'location': location,
                    'city': city,
                    'state': st,
                    'description': description
                })

            except Exception:
                continue

        return listings

    def scrape(self, broker_account: str, headless: bool = True, verbose: bool = True) -> List[Dict]:
        if verbose:
            print(f"\n{'='*60}")
            print("Executive Business Brokers (Larry Bodner)")
            print('='*60)

        all_items = []
        seen_urls = set()

        for state in self.STATES:
            try:
                state_items = self._fetch_state(state)
                new = [l for l in state_items if l['url'] not in seen_urls]
                for l in new:
                    seen_urls.add(l['url'])
                all_items.extend(new)
                if verbose and new:
                    print(f"[Bodner] {state}: {len(new)} new listings | Total: {len(all_items)}")
                time.sleep(0.5)
            except Exception as e:
                if verbose:
                    print(f"[Bodner] Error on {state}: {e}")

        listings = []
        for item in all_items:
            listings.append(format_listing(
                url=item['url'],
                broker_account=broker_account,
                title=item['title'],
                price=parse_money(item['price_text']),
                price_text=item['price_text'],
                location=item['location'],
                city=item['city'],
                state=item['state'],
                description=item['description'],
                revenue=parse_money(item['revenue_text'])
            ))

        if verbose:
            with_price = sum(1 for l in listings if l.get('price'))
            print(f"\n✓ {len(listings)} Larry Bodner listings ({with_price} with price)")

        return listings
