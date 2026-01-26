"""
DealLedger: Run All Scrapers

Executes all broker scrapers and outputs combined results.

Usage:
    python -m scrapers.run_all
    python -m scrapers.run_all --output data/raw/2026-01-23.json
    python -m scrapers.run_all --brokers transworld,sunbelt
"""

import argparse
import importlib
import json
import logging
import pkgutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def discover_scrapers() -> List[str]:
    """Discover all broker scraper modules."""
    scrapers_path = Path(__file__).parent / "brokers"
    
    scrapers = []
    for _, name, _ in pkgutil.iter_modules([str(scrapers_path)]):
        # Skip template and private modules
        if name.startswith('_'):
            continue
        scrapers.append(name)
    
    return sorted(scrapers)


def load_scraper(name: str):
    """Load a scraper class by module name."""
    module = importlib.import_module(f"scrapers.brokers.{name}")
    
    # Find the scraper class (should be the only BaseScraper subclass)
    from scrapers.base import BaseScraper
    
    for attr_name in dir(module):
        attr = getattr(module, attr_name)
        if (isinstance(attr, type) and 
            issubclass(attr, BaseScraper) and 
            attr is not BaseScraper):
            return attr
    
    raise ValueError(f"No scraper class found in {name}")


def run_scraper(name: str) -> Dict[str, Any]:
    """Run a single scraper and return results with metadata."""
    logger.info(f"Running scraper: {name}")
    
    try:
        scraper_class = load_scraper(name)
        scraper = scraper_class()
        
        start_time = datetime.now(timezone.utc)
        listings = scraper.run()
        end_time = datetime.now(timezone.utc)
        
        return {
            "broker_id": scraper.broker_id,
            "broker_name": scraper.broker_name,
            "status": "success",
            "listings_count": len(listings),
            "listings": listings,
            "started_at": start_time.isoformat(),
            "completed_at": end_time.isoformat(),
            "duration_seconds": (end_time - start_time).total_seconds(),
        }
        
    except Exception as e:
        logger.error(f"Scraper {name} failed: {e}")
        return {
            "broker_id": name,
            "broker_name": name,
            "status": "error",
            "error": str(e),
            "listings_count": 0,
            "listings": [],
        }


def run_all(broker_names: List[str] = None, output_path: str = None) -> Dict[str, Any]:
    """
    Run all (or specified) scrapers.
    
    Args:
        broker_names: List of broker names to run (None = all)
        output_path: Path to write JSON output (None = stdout)
        
    Returns:
        Combined results dictionary
    """
    available = discover_scrapers()
    
    if broker_names:
        # Validate requested brokers exist
        invalid = set(broker_names) - set(available)
        if invalid:
            raise ValueError(f"Unknown brokers: {invalid}")
        to_run = broker_names
    else:
        to_run = available
    
    logger.info(f"Running {len(to_run)} scrapers: {to_run}")
    
    run_start = datetime.now(timezone.utc)
    
    results = {
        "run_id": run_start.strftime("%Y%m%d_%H%M%S"),
        "started_at": run_start.isoformat(),
        "brokers": [],
        "summary": {
            "total_brokers": len(to_run),
            "successful": 0,
            "failed": 0,
            "total_listings": 0,
        }
    }
    
    for name in to_run:
        broker_result = run_scraper(name)
        results["brokers"].append(broker_result)
        
        if broker_result["status"] == "success":
            results["summary"]["successful"] += 1
            results["summary"]["total_listings"] += broker_result["listings_count"]
        else:
            results["summary"]["failed"] += 1
    
    run_end = datetime.now(timezone.utc)
    results["completed_at"] = run_end.isoformat()
    results["duration_seconds"] = (run_end - run_start).total_seconds()
    
    # Output
    if output_path:
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Results written to {output_path}")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Run DealLedger scrapers")
    parser.add_argument(
        "--brokers", 
        type=str, 
        help="Comma-separated list of brokers to run (default: all)"
    )
    parser.add_argument(
        "--output", "-o",
        type=str,
        help="Output file path (default: stdout)"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available scrapers and exit"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    # List mode
    if args.list:
        scrapers = discover_scrapers()
        print(f"Available scrapers ({len(scrapers)}):")
        for s in scrapers:
            print(f"  - {s}")
        return
    
    # Parse broker list
    broker_names = None
    if args.brokers:
        broker_names = [b.strip() for b in args.brokers.split(",")]
    
    # Run
    results = run_all(broker_names, args.output)
    
    # Print summary
    print(f"\n{'='*50}")
    print(f"DealLedger Scrape Complete")
    print(f"{'='*50}")
    print(f"Brokers: {results['summary']['successful']} succeeded, {results['summary']['failed']} failed")
    print(f"Listings: {results['summary']['total_listings']} total")
    print(f"Duration: {results['duration_seconds']:.1f} seconds")
    
    if not args.output:
        # Print full results to stdout if no output file
        print(f"\n{'='*50}")
        print(json.dumps(results, indent=2, default=str))


if __name__ == "__main__":
    main()
