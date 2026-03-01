#!/usr/bin/env python3
"""
DealLedger Scraper Agent v2 — True Agentic Loop
Uses Claude tool use API. Claude decides what to do next.
"""
import argparse, json, os, re, sys
from datetime import datetime, timezone
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup, Comment

try:
    import anthropic
except ImportError:
    print("Install: pip3 install anthropic --break-system-packages")
    sys.exit(1)

UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"

def tool_fetch_page(url):
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup.find_all(["script","style","noscript","iframe","svg"]):
            tag.decompose()
        for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
            c.extract()
        body = soup.find("body") or soup
        clean = str(body)
        if len(clean) > 12000:
            clean = clean[:8000] + "\n...[TRUNCATED]...\n" + clean[-4000:]
        return {"ok": True, "url": url, "html": clean, "raw_length": len(resp.text)}
    except Exception as e:
        return {"ok": False, "url": url, "error": str(e)}

def tool_run_scraper(url, container_selector, fields):
    try:
        resp = requests.get(url, headers={"User-Agent": UA}, timeout=15, allow_redirects=True)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        containers = soup.select(container_selector)
        if not containers:
            return {"ok": False, "error": f"Selector '{container_selector}' matched 0 elements", "count": 0}
        records = []
        for el in containers[:5]:
            rec = {}
            for field, sel in fields.items():
                if sel:
                    found = el.select_one(sel)
                    rec[field] = found.get_text(strip=True)[:200] if found else None
            records.append(rec)
        has_title = sum(1 for r in records if r.get("title"))
        has_price = sum(1 for r in records if r.get("price"))
        return {
            "ok": True, "count": len(containers), "sample": records,
            "quality": {
                "has_title_pct": round(has_title/len(records)*100) if records else 0,
                "has_price_pct": round(has_price/len(records)*100) if records else 0,
            }
        }
    except Exception as e:
        return {"ok": False, "error": str(e), "count": 0}

def tool_save_config(url, broker_name, container_selector, fields, pagination_selector=None, notes="", confidence="medium"):
    os.makedirs("data/scraper_configs", exist_ok=True)
    domain = urlparse(url).netloc.replace("www.","")
    safe = re.sub(r"[^a-zA-Z0-9.-]","_",domain)
    path = f"data/scraper_configs/{safe}.json"
    config = {
        "broker_name": broker_name, "source_url": url,
        "container_selector": container_selector, "fields": fields,
        "pagination_next_selector": pagination_selector,
        "confidence": confidence, "notes": notes,
        "_meta": {"generated_at": datetime.now(timezone.utc).isoformat(), "generator": "dealledger-scraper-agent-v2"}
    }
    with open(path,"w") as f:
        json.dump(config, f, indent=2)
    return {"ok": True, "path": path, "broker_name": broker_name}

def tool_find_listings_url(base_url):
    domain = urlparse(base_url).netloc
    scheme = urlparse(base_url).scheme
    candidates = [
        f"{scheme}://{domain}/businesses-for-sale",
        f"{scheme}://{domain}/listings",
        f"{scheme}://{domain}/business-listings",
        f"{scheme}://{domain}/buy-a-business",
        f"{scheme}://{domain}/available-businesses",
    ]
    working = []
    for url in candidates:
        try:
            r = requests.head(url, headers={"User-Agent": UA}, timeout=8, allow_redirects=True)
            if r.status_code == 200:
                working.append(url)
        except:
            pass
    return {"candidates": candidates, "working": working}

TOOLS = [
    {"name": "fetch_page", "description": "Fetch a URL and return cleaned HTML. Use this first.", "input_schema": {"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]}},
    {"name": "run_scraper", "description": "Test CSS selectors against a live page. Returns actual scraped records. Always run before saving.", "input_schema": {"type": "object", "properties": {"url": {"type": "string"}, "container_selector": {"type": "string"}, "fields": {"type": "object"}}, "required": ["url","container_selector","fields"]}},
    {"name": "save_config", "description": "Save validated scraper config. Only call after run_scraper confirms count>0.", "input_schema": {"type": "object", "properties": {"url": {"type": "string"}, "broker_name": {"type": "string"}, "container_selector": {"type": "string"}, "fields": {"type": "object"}, "pagination_selector": {"type": "string"}, "notes": {"type": "string"}, "confidence": {"type": "string", "enum": ["high","medium","low"]}}, "required": ["url","broker_name","container_selector","fields"]}},
    {"name": "find_listings_url", "description": "Find the listings page on a broker site by trying common URL patterns.", "input_schema": {"type": "object", "properties": {"base_url": {"type": "string"}}, "required": ["base_url"]}}
]

SYSTEM = """You are a web scraping agent for DealLedger, an open-source ledger of business-for-sale listings.
Given a broker URL, your job is to build a working scraper config.
1. Call fetch_page to see the HTML
2. Identify CSS selectors for listings
3. Call run_scraper to TEST selectors — see actual results
4. If count=0: adjust selectors and retry (up to 3 attempts), or call find_listings_url
5. Only call save_config when run_scraper confirms count>0 and quality>50%
Be persistent. Try multiple selector approaches. Summarize results when done."""

def dispatch(name, inputs):
    if name == "fetch_page": result = tool_fetch_page(inputs["url"])
    elif name == "run_scraper": result = tool_run_scraper(inputs["url"], inputs["container_selector"], inputs.get("fields",{}))
    elif name == "save_config": result = tool_save_config(inputs["url"], inputs["broker_name"], inputs["container_selector"], inputs.get("fields",{}), inputs.get("pagination_selector"), inputs.get("notes",""), inputs.get("confidence","medium"))
    elif name == "find_listings_url": result = tool_find_listings_url(inputs["base_url"])
    else: result = {"error": f"Unknown tool: {name}"}
    return json.dumps(result)

def run_agent(url, verbose=True):
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
    messages = [{"role": "user", "content": f"Build a scraper for: {url}"}]
    print(f"\n{'='*60}\n🤖 Agent: {url}\n{'='*60}")
    final = {"url": url, "success": False, "config_path": None}

    for i in range(10):
        resp = client.messages.create(model="claude-sonnet-4-5", max_tokens=4096, system=SYSTEM, tools=TOOLS, messages=messages)
        for block in resp.content:
            if hasattr(block,"text") and block.text:
                print(f"\n💭 {block.text}")
        if resp.stop_reason == "end_turn":
            print(f"\n✅ Done ({i+1} iterations)")
            break
        if resp.stop_reason != "tool_use":
            break
        tool_results = []
        for block in resp.content:
            if block.type != "tool_use": continue
            print(f"\n🔧 {block.name}: {json.dumps(block.input)[:200]}")
            result_str = dispatch(block.name, block.input)
            result_data = json.loads(result_str)
            if block.name == "fetch_page":
                print(f"   → {result_data.get('raw_length',0):,} bytes, ok={result_data.get('ok')}")
            elif block.name == "run_scraper":
                print(f"   → count={result_data.get('count',0)}, quality={result_data.get('quality',{})}")
            else:
                print(f"   → {result_str[:200]}")
            if block.name == "save_config" and result_data.get("ok"):
                final = {"url": url, "success": True, "config_path": result_data["path"], "broker_name": result_data["broker_name"]}
            tool_results.append({"type": "tool_result", "tool_use_id": block.id, "content": result_str})
        messages.append({"role": "assistant", "content": resp.content})
        messages.append({"role": "user", "content": tool_results})
    return final

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", nargs="?")
    parser.add_argument("--batch", help="File with URLs")
    args = parser.parse_args()
    if args.batch:
        urls = [l.strip() for l in open(args.batch) if l.strip().startswith("http")]
        results = [run_agent(u) for u in urls]
        success = sum(1 for r in results if r["success"])
        print(f"\nBATCH: {success}/{len(urls)} successful")
    elif args.url:
        run_agent(args.url)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
