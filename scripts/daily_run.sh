#!/bin/bash
#
# DealLedger Daily Run
# ====================
# Runs the scraper and exports data to the repo.
#
# Usage:
#   ./scripts/daily_run.sh           # Scrape and export
#   ./scripts/daily_run.sh --push    # Scrape, export, and push to GitHub
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(dirname "$SCRIPT_DIR")"

cd "$REPO_DIR"

echo "========================================"
echo "DealLedger Daily Run - $(date)"
echo "========================================"

# Run the unified scraper (all brokers)
echo ""
echo "Step 1: Running scraper..."
python scrapers/unified_scraper.py --all --export data

# Export from Supabase to repo
echo ""
echo "Step 2: Exporting to repo..."
python scripts/export_daily.py --all --output data

# Show what changed
echo ""
echo "Step 3: Changes..."
git status --short data/

# Optionally commit and push
if [ "$1" == "--push" ]; then
    echo ""
    echo "Step 4: Committing and pushing..."
    
    DATE=$(date +%Y-%m-%d)
    LISTING_COUNT=$(cat data/latest.json | python -c "import sys,json; print(len(json.load(sys.stdin)))")
    
    git add data/
    git commit -m "Daily snapshot: $DATE - $LISTING_COUNT listings" || echo "Nothing to commit"
    git push
    
    echo ""
    echo "✓ Pushed to GitHub"
fi

echo ""
echo "========================================"
echo "Done!"
echo "========================================"
