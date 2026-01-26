# Adding a Broker to DealLedger

This guide walks you through adding a new broker scraper.

**Time required:** 30 minutes to 2 hours (depending on site complexity)

**Skills needed:** Basic Python, HTML/CSS selectors

---

## Before You Start

1. **Check if the broker is already covered** — Look in `scrapers/brokers/`
2. **Check the wanted list** — See [WANTED_BROKERS.md](WANTED_BROKERS.md) for priority targets
3. **Verify the broker qualifies:**
   - Public listings page (no login required)
   - At least 5 active listings
   - Identifiable company name

---

## Step 1: Reconnaissance

Before writing code, understand the target site:

1. **Find the listings index page**
   - Usually `/listings`, `/businesses-for-sale`, `/search`
   - Note if there's pagination

2. **Find a single listing page**
   - Click into any listing
   - Note the URL structure (often contains an ID)

3. **Inspect the HTML**
   - Open browser dev tools (F12)
   - Find the CSS selectors for:
     - Title
     - Price
     - Location
     - Description
     - Financials (revenue, cash flow)
   - Note any unusual patterns (JavaScript rendering, iframes, etc.)

---

## Step 2: Create the Scraper File

```bash
# Copy the template
cp scrapers/brokers/_template.py scrapers/brokers/your_broker.py
```

Edit the file and update:

```python
class YourBrokerScraper(BaseScraper):
    broker_id = "your_broker"  # lowercase, no spaces
    broker_name = "Your Broker Name"  # Full company name
    base_url = "https://www.yourbroker.com"
```

---

## Step 3: Implement `get_listing_urls()`

This method returns all listing URLs to scrape.

**Simple case (single page):**

```python
def get_listing_urls(self) -> List[str]:
    urls = []
    soup = self.fetch_page(f"{self.base_url}/listings")
    
    for link in soup.select('a.listing-link'):
        href = link.get('href')
        if href:
            urls.append(urljoin(self.base_url, href))
    
    return list(set(urls))
```

**Paginated case:**

```python
def get_listing_urls(self) -> List[str]:
    urls = []
    page = 1
    
    while True:
        soup = self.fetch_page(f"{self.base_url}/listings?page={page}")
        if not soup:
            break
        
        links = soup.select('a.listing-link')
        if not links:
            break
        
        for link in links:
            href = link.get('href')
            if href:
                urls.append(urljoin(self.base_url, href))
        
        page += 1
    
    return list(set(urls))
```

---

## Step 4: Implement `parse_listing()`

This method extracts data from a single listing page.

```python
def parse_listing(self, url: str, soup: BeautifulSoup) -> Optional[Listing]:
    # Title (required)
    title_el = soup.select_one('h1.listing-title')
    if not title_el:
        return None
    title = title_el.get_text(strip=True)
    
    # Price
    price_el = soup.select_one('span.price')
    asking_price = parse_price(price_el.get_text()) if price_el else None
    
    # Location
    location_el = soup.select_one('span.location')
    city, state = None, None
    if location_el:
        # Parse "City, ST" format
        match = re.match(r'([^,]+),\s*([A-Z]{2})', location_el.get_text())
        if match:
            city, state = match.groups()
    
    # ... extract more fields ...
    
    return Listing(
        source_url=url,
        broker_id=self.broker_id,
        broker_name=self.broker_name,
        title=title,
        asking_price=asking_price,
        city=city,
        state=state,
        # ... more fields ...
    )
```

---

## Step 5: Test Your Scraper

```bash
# Run just your scraper
python -m scrapers.brokers.your_broker

# Check the output
# - Are URLs being found?
# - Are listings being parsed correctly?
# - Are fields populated?
```

**Common issues:**

| Problem | Solution |
|---------|----------|
| No URLs found | Check your selector in `get_listing_urls()` |
| All listings return None | Check your title selector in `parse_listing()` |
| Missing fields | Adjust selectors, check for JavaScript rendering |
| 403 errors | Site may block scrapers — try adjusting headers |

---

## Step 6: Add Tests

Create `tests/test_your_broker.py`:

```python
import pytest
from scrapers.brokers.your_broker import YourBrokerScraper

def test_broker_metadata():
    scraper = YourBrokerScraper()
    assert scraper.broker_id == "your_broker"
    assert scraper.broker_name == "Your Broker Name"
    assert scraper.base_url.startswith("https://")

def test_get_listing_urls():
    scraper = YourBrokerScraper()
    urls = scraper.get_listing_urls()
    
    assert len(urls) > 0
    assert all(url.startswith("http") for url in urls)

def test_parse_listing():
    scraper = YourBrokerScraper()
    urls = scraper.get_listing_urls()
    
    if urls:
        soup = scraper.fetch_page(urls[0])
        listing = scraper.parse_listing(urls[0], soup)
        
        assert listing is not None
        assert listing.title
        assert listing.source_url == urls[0]
```

Run tests:

```bash
pytest tests/test_your_broker.py -v
```

---

## Step 7: Submit Your PR

1. **Fork the repo** (if you haven't)
2. **Create a branch:** `git checkout -b add-broker-yourbroker`
3. **Commit your changes:**
   ```bash
   git add scrapers/brokers/your_broker.py tests/test_your_broker.py
   git commit -m "Add scraper for Your Broker Name"
   ```
4. **Push and create PR**

**In your PR description, include:**

- Broker name and website
- Number of listings currently available
- Any special notes about the site structure
- Screenshot of a sample parsed listing

---

## Tips for Tricky Sites

### JavaScript-rendered content

If content loads via JavaScript:

1. Check if there's an API the JS calls (look in Network tab)
2. Use the API directly if possible
3. If not, you may need Selenium/Playwright (open an issue first)

### Anti-scraping measures

- Increase `request_delay` (be polite)
- Rotate user agents if needed
- Respect robots.txt

### Inconsistent HTML

Some sites have inconsistent markup:

```python
# Try multiple selectors
price_el = (
    soup.select_one('span.price') or
    soup.select_one('div.asking-price') or
    soup.select_one('[data-field="price"]')
)
```

### Confidential listings

Some brokers hide details:

```python
if 'confidential' in title.lower():
    return Listing(
        # ... basic fields ...
        location_hidden=True,
        price_hidden=True,
    )
```

---

## Getting Help

- **Stuck on selectors?** Open a draft PR and ask
- **Site too complex?** Open an issue describing the challenge
- **Not sure if broker qualifies?** Ask in Discussions

---

## Recognition

Contributors who add working scrapers are credited in:

- CONTRIBUTORS.md
- Release notes
- The broker record itself (`contributed_by` field)

Thank you for helping build the ledger!
