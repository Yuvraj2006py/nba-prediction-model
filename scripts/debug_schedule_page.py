"""Debug script to inspect Basketball Reference schedule page structure."""

import sys
import re
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.data_collectors.basketball_reference_selenium import BasketballReferenceSeleniumCollector
from src.database.db_manager import DatabaseManager
from bs4 import BeautifulSoup

# Initialize collector
db_manager = DatabaseManager()
collector = BasketballReferenceSeleniumCollector(db_manager)

# Test URL
season = '2019-20'
season_start_year = int(season.split('-')[0])
schedule_url = f"{collector.base_url}/leagues/NBA_{season_start_year}_games.html"

print(f"Fetching: {schedule_url}")
soup = collector._fetch_page(schedule_url)

if not soup:
    print("ERROR: Could not fetch page")
    sys.exit(1)

print("\n" + "="*70)
print("PAGE ANALYSIS")
print("="*70)

# Check for tables
all_tables = soup.find_all('table')
print(f"\nTotal tables found: {len(all_tables)}")

for i, table in enumerate(all_tables[:5]):  # First 5 tables
    classes = table.get('class', [])
    print(f"\nTable {i+1}:")
    print(f"  Classes: {classes}")
    
    # Check for caption
    caption = table.find('caption')
    if caption:
        print(f"  Caption: {caption.get_text().strip()[:100]}")
    
    # Check for tbody
    tbody = table.find('tbody')
    if tbody:
        rows = tbody.find_all('tr')
        print(f"  Rows in tbody: {len(rows)}")
        
        # Show first row structure
        if rows:
            first_row = rows[0]
            cells = first_row.find_all(['td', 'th'])
            print(f"  First row cells: {len(cells)}")
            for j, cell in enumerate(cells[:5]):  # First 5 cells
                text = cell.get_text().strip()[:50]
                links = cell.find_all('a')
                print(f"    Cell {j}: '{text}' (links: {len(links)})")
                if links:
                    for link in links[:2]:
                        href = link.get('href', '')
                        print(f"      Link: {href[:80]}")

# Look for schedule-specific elements
print("\n" + "="*70)
print("LOOKING FOR SCHEDULE-SPECIFIC ELEMENTS")
print("="*70)

# Check for divs with schedule-related classes
schedule_divs = soup.find_all('div', class_=lambda x: x and ('schedule' in str(x).lower() or 'game' in str(x).lower()))
print(f"\nSchedule-related divs: {len(schedule_divs)}")

# Check for links to boxscores - filter out non-game links
boxscore_links = soup.find_all('a', href=re.compile(r'/boxscores/'))
# Filter to only actual game boxscore links (have date pattern)
game_boxscore_links = [link for link in boxscore_links if re.search(r'/boxscores/\d{8}', link.get('href', ''))]

print(f"\nTotal boxscore links found: {len(boxscore_links)}")
print(f"Game boxscore links (with dates): {len(game_boxscore_links)}")

if game_boxscore_links:
    print("\nSample game boxscore links (first 20):")
    for i, link in enumerate(game_boxscore_links[:20]):
        href = link.get('href', '')
        text = link.get_text().strip()[:50]
        print(f"\n  {i+1}. {href}")
        print(f"     Text: '{text}'")
        # Check if it matches our pattern
        match = re.search(r'/boxscores/(\d{8})0([A-Z]{3})\.html', href)
        if match:
            print(f"     [OK] Matches pattern: date={match.group(1)}, team={match.group(2)}")
        else:
            print(f"     [NO] Does NOT match pattern")
            # Try to see what pattern it does match
            alt_match = re.search(r'/boxscores/(.+)', href)
            if alt_match:
                pattern = alt_match.group(1)
                print(f"     Actual pattern: {pattern[:80]}")
                # Try to extract date and team manually
                date_match = re.search(r'(\d{8})', pattern)
                team_match = re.search(r'([A-Z]{3})', pattern)
                if date_match:
                    print(f"     Found date: {date_match.group(1)}")
                if team_match:
                    print(f"     Found team: {team_match.group(1)}")

# Save HTML for inspection
html_file = project_root / "data" / "schedule_page_debug.html"
with open(html_file, 'w', encoding='utf-8') as f:
    f.write(str(soup))
print(f"\nHTML saved to: {html_file}")
