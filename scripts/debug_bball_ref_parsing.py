"""Debug script for Basketball Reference table parsing."""

import sys
import logging
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s - %(message)s')

from src.data_collectors.basketball_reference_collector import BasketballReferenceCollector
from src.database.db_manager import DatabaseManager
from bs4 import BeautifulSoup

collector = BasketballReferenceCollector(DatabaseManager())

html = """
<table class="sortable stats_table">
    <thead>
        <tr>
            <th>Team</th>
            <th>MP</th>
            <th>FG</th>
            <th>FGA</th>
            <th>FG%</th>
            <th>3P</th>
            <th>3PA</th>
            <th>3P%</th>
            <th>FT</th>
            <th>FTA</th>
            <th>FT%</th>
            <th>ORB</th>
            <th>DRB</th>
            <th>TRB</th>
            <th>AST</th>
            <th>STL</th>
            <th>BLK</th>
            <th>TOV</th>
            <th>PF</th>
            <th>PTS</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Team Totals</td>
            <td>240</td>
            <td>42</td>
            <td>90</td>
            <td>.467</td>
            <td>12</td>
            <td>35</td>
            <td>.343</td>
            <td>18</td>
            <td>22</td>
            <td>.818</td>
            <td>12</td>
            <td>35</td>
            <td>47</td>
            <td>28</td>
            <td>8</td>
            <td>5</td>
            <td>14</td>
            <td>20</td>
            <td>114</td>
        </tr>
    </tbody>
</table>
"""

soup = BeautifulSoup(html, 'html.parser')
table = soup.find('table')

# Debug: Check headers
thead = table.find('thead')
header_row = thead.find('tr')
headers = []
for th in header_row.find_all(['th', 'td']):
    header_text = th.get_text().strip()
    if header_text:
        headers.append(header_text)

print("Headers found:", headers)

# Debug: Check row data
rows = table.find('tbody').find_all('tr')
for row in rows:
    row_text = row.get_text().strip().upper()
    if 'TOTALS' in row_text or 'TEAM' in row_text:
        cells = row.find_all(['td', 'th'])
        row_data = [cell.get_text().strip() for cell in cells]
        print("Row data:", row_data)
        print("Row data length:", len(row_data))
        print("Headers length:", len(headers))
        break

result = collector._extract_team_stats_from_table(table, '0022300123', '1610612747', True)

print("\nResult:", result)
if result:
    print("Points:", result.get('points'))
    print("FG:", result.get('field_goals_made'))
    print("FGA:", result.get('field_goals_attempted'))
else:
    print("Result is None!")

