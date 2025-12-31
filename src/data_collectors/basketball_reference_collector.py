"""Basketball Reference web scraper for NBA game statistics."""

import logging
import time
import re
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from bs4 import BeautifulSoup
import requests

from config.settings import get_settings
from src.database.db_manager import DatabaseManager
from src.database.models import Team

logger = logging.getLogger(__name__)


class BasketballReferenceCollector:
    """Collects NBA game statistics by scraping Basketball Reference."""

    # Mapping from NBA API team abbreviations to Basketball Reference abbreviations
    # Most are the same, but some differ
    TEAM_ABBREV_MAP = {
        'ATL': 'ATL',  # Atlanta Hawks
        'BOS': 'BOS',  # Boston Celtics
        'BKN': 'BRK',  # Brooklyn Nets (BRK on BR)
        'CHA': 'CHO',  # Charlotte Hornets (CHO on BR)
        'CHI': 'CHI',  # Chicago Bulls
        'CLE': 'CLE',  # Cleveland Cavaliers
        'DAL': 'DAL',  # Dallas Mavericks
        'DEN': 'DEN',  # Denver Nuggets
        'DET': 'DET',  # Detroit Pistons
        'GSW': 'GSW',  # Golden State Warriors
        'HOU': 'HOU',  # Houston Rockets
        'IND': 'IND',  # Indiana Pacers
        'LAC': 'LAC',  # LA Clippers
        'LAL': 'LAL',  # Los Angeles Lakers
        'MEM': 'MEM',  # Memphis Grizzlies
        'MIA': 'MIA',  # Miami Heat
        'MIL': 'MIL',  # Milwaukee Bucks
        'MIN': 'MIN',  # Minnesota Timberwolves
        'NOP': 'NOP',  # New Orleans Pelicans
        'NYK': 'NYK',  # New York Knicks
        'OKC': 'OKC',  # Oklahoma City Thunder
        'ORL': 'ORL',  # Orlando Magic
        'PHI': 'PHI',  # Philadelphia 76ers
        'PHX': 'PHO',  # Phoenix Suns (PHO on BR)
        'POR': 'POR',  # Portland Trail Blazers
        'SAC': 'SAC',  # Sacramento Kings
        'SAS': 'SAS',  # San Antonio Spurs
        'TOR': 'TOR',  # Toronto Raptors
        'UTA': 'UTA',  # Utah Jazz
        'WAS': 'WAS',  # Washington Wizards
    }

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.settings = get_settings()
        self.base_url = self.settings.BBALL_REF_BASE_URL
        self.db_manager = db_manager or DatabaseManager()
        self.scraping_delay = self.settings.SCRAPING_DELAY
        self.max_retries = self.settings.MAX_RETRIES
        self.retry_delay = self.settings.RETRY_DELAY
        
        # Create a session for connection pooling
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0'
        })
        
        logger.info("Basketball Reference Collector initialized")

    def _rate_limit(self):
        """Apply rate limiting delay."""
        time.sleep(self.scraping_delay)

    def _get_team_abbrev(self, team_id: str) -> Optional[str]:
        """Get Basketball Reference abbreviation for a team."""
        team = self.db_manager.get_team(team_id)
        if not team:
            logger.warning(f"Team {team_id} not found in database")
            return None
        
        nba_abbrev = team.team_abbreviation
        return self.TEAM_ABBREV_MAP.get(nba_abbrev, nba_abbrev)

    def _build_boxscore_url(self, game_date: date, home_team_abbrev: str) -> str:
        """
        Build Basketball Reference boxscore URL.
        
        Format: https://www.basketball-reference.com/boxscores/YYYYMMDD0TTM.html
        Where TTM is the 3-letter home team abbreviation.
        """
        date_str = game_date.strftime('%Y%m%d')
        # Basketball Reference uses '0' before the team abbreviation
        url = f"{self.base_url}/boxscores/{date_str}0{home_team_abbrev}.html"
        return url

    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a web page with retry logic."""
        for attempt in range(self.max_retries):
            try:
                self._rate_limit()
                # Add longer delay for 403 errors
                if attempt > 0:
                    time.sleep(self.scraping_delay * 2)  # Double delay on retry
                
                response = self.session.get(url, timeout=15, allow_redirects=True)
                
                # Handle 403 specifically
                if response.status_code == 403:
                    if attempt < self.max_retries - 1:
                        wait_time = self.scraping_delay * (2 ** (attempt + 1))  # Exponential backoff
                        wait_time = min(wait_time, 30.0)  # Cap at 30 seconds
                        logger.warning(f"403 Forbidden (attempt {attempt + 1}/{self.max_retries}). Waiting {wait_time:.1f}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"403 Forbidden after {self.max_retries} attempts. Basketball Reference may be blocking requests.")
                        return None
                
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                return soup
                
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (1.5 ** attempt)
                    wait_time = min(wait_time, 15.0)  # Cap at 15 seconds
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}. Retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Failed to fetch {url} after {self.max_retries} attempts: {e}")
                    return None
        
        return None

    def _parse_float(self, value: Any, default: float = 0.0) -> float:
        """Safely parse a float value."""
        if value is None:
            return default
        try:
            # Remove any non-numeric characters except decimal point
            if isinstance(value, str):
                value = value.replace(',', '').strip()
                if value == '' or value == '—':
                    return default
            return float(value)
        except (ValueError, TypeError):
            return default

    def _parse_int(self, value: Any, default: int = 0) -> int:
        """Safely parse an integer value."""
        if value is None:
            return default
        try:
            if isinstance(value, str):
                value = value.replace(',', '').strip()
                if value == '' or value == '—':
                    return default
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def _parse_minutes(self, value: str) -> str:
        """Parse minutes played (format: MM:SS)."""
        if not value or value == '—':
            return '0:00'
        # Basketball Reference format is usually MM:SS
        if ':' in value:
            return value
        # If it's just a number, assume it's minutes
        try:
            mins = int(float(value))
            return f"{mins}:00"
        except (ValueError, TypeError):
            return '0:00'

    def get_game_details(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Get game details including scores, winner, etc. from Basketball Reference.
        Similar to NBA API's get_game_details method.
        
        Args:
            game_id: NBA game ID
            
        Returns:
            Game details dictionary or None
        """
        logger.debug(f"Getting game details from Basketball Reference for game {game_id}")
        
        # Get game from database to get date and teams
        game = self.db_manager.get_game(game_id)
        if not game:
            logger.warning(f"Game {game_id} not found in database")
            return None
        
        # Get team abbreviations
        home_abbrev = self._get_team_abbrev(game.home_team_id)
        if not home_abbrev:
            logger.warning(f"Could not get team abbreviation for game {game_id}")
            return None
        
        # Build URL
        boxscore_url = self._build_boxscore_url(game.game_date, home_abbrev)
        
        # Fetch page
        soup = self._fetch_page(boxscore_url)
        if not soup:
            logger.warning(f"Could not fetch boxscore page for game {game_id}")
            return None
        
        # Extract scores from the page
        # Basketball Reference shows scores in the header, usually in a div with class "scorebox"
        home_score = None
        away_score = None
        
        # Try to find scores in the scorebox
        scorebox = soup.find('div', class_='scorebox')
        if scorebox:
            # Look for score elements
            scores = scorebox.find_all(['div', 'span'], class_=lambda x: x and 'score' in x.lower())
            if not scores:
                # Try finding scores by text pattern
                score_text = scorebox.get_text()
                # Look for patterns like "126" or "117-126"
                import re
                score_pattern = r'(\d+)\s*[-–]\s*(\d+)'
                match = re.search(score_pattern, score_text)
                if match:
                    away_score = int(match.group(1))
                    home_score = int(match.group(2))
        
        # Alternative: Extract from team stats tables (points column)
        if home_score is None or away_score is None:
            team_stats_list = self._parse_team_stats(soup, game_id, game.home_team_id, game.away_team_id)
            for team_stat in team_stats_list:
                if team_stat['is_home']:
                    home_score = team_stat['points']
                else:
                    away_score = team_stat['points']
        
        # Determine winner
        winner = None
        point_differential = None
        if home_score is not None and away_score is not None:
            point_differential = home_score - away_score
            winner = game.home_team_id if home_score > away_score else game.away_team_id
        
        game_details = {
            'game_id': game_id,
            'season': game.season,
            'season_type': game.season_type,
            'game_date': game.game_date,
            'home_team_id': game.home_team_id,
            'away_team_id': game.away_team_id,
            'home_score': home_score,
            'away_score': away_score,
            'game_status': 'finished',  # If we can access boxscore, game is finished
            'winner': winner,
            'point_differential': point_differential
        }
        
        return game_details

    def collect_game_stats(self, game_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Collect both team and player stats for a game from Basketball Reference.
        
        Args:
            game_id: NBA game ID
            
        Returns:
            Dictionary with 'team_stats' and 'player_stats' lists
        """
        logger.debug(f"Collecting stats from Basketball Reference for game {game_id}")
        
        # Get game from database
        game = self.db_manager.get_game(game_id)
        if not game:
            logger.warning(f"Game {game_id} not found in database")
            return {'team_stats': [], 'player_stats': []}
        
        # Get team abbreviations
        home_abbrev = self._get_team_abbrev(game.home_team_id)
        away_abbrev = self._get_team_abbrev(game.away_team_id)
        
        if not home_abbrev or not away_abbrev:
            logger.warning(f"Could not get team abbreviations for game {game_id}")
            return {'team_stats': [], 'player_stats': []}
        
        # Build URL
        boxscore_url = self._build_boxscore_url(game.game_date, home_abbrev)
        logger.debug(f"Fetching boxscore from: {boxscore_url}")
        
        # Fetch page
        soup = self._fetch_page(boxscore_url)
        if not soup:
            logger.warning(f"Could not fetch boxscore page for game {game_id}")
            return {'team_stats': [], 'player_stats': []}
        
        # Parse team stats
        team_stats_list = self._parse_team_stats(soup, game_id, game.home_team_id, game.away_team_id)
        
        # Parse player stats
        player_stats_list = self._parse_player_stats(soup, game_id, game.home_team_id, game.away_team_id)
        
        logger.debug(f"Collected {len(team_stats_list)} team stats and {len(player_stats_list)} player stats for game {game_id}")
        return {
            'team_stats': team_stats_list,
            'player_stats': player_stats_list
        }
    
    def collect_all_game_data(self, game_id: str) -> Dict[str, Any]:
        """
        Collect ALL game data (details + stats) from Basketball Reference.
        This is the equivalent of what NBA API collector does.
        
        Args:
            game_id: NBA game ID
            
        Returns:
            Dictionary with 'game_details', 'team_stats', and 'player_stats'
        """
        logger.debug(f"Collecting all game data from Basketball Reference for game {game_id}")
        
        # Get game details
        game_details = self.get_game_details(game_id)
        
        # Get stats
        stats = self.collect_game_stats(game_id)
        
        return {
            'game_details': game_details,
            'team_stats': stats['team_stats'],
            'player_stats': stats['player_stats']
        }

    def _parse_team_stats(
        self,
        soup: BeautifulSoup,
        game_id: str,
        home_team_id: str,
        away_team_id: str
    ) -> List[Dict[str, Any]]:
        """Parse team statistics from the boxscore page."""
        team_stats_list = []
        
        # Find the team stats table (usually has id like 'box-{team}-game-basic')
        # Basketball Reference uses team abbreviations in lowercase
        home_abbrev = self._get_team_abbrev(home_team_id)
        away_abbrev = self._get_team_abbrev(away_team_id)
        
        if not home_abbrev or not away_abbrev:
            return []
        
        # Try to find team stats tables
        # Basketball Reference boxscore has team stats in a specific format
        # Look for tables with class 'sortable stats_table'
        team_tables = soup.find_all('table', class_='sortable stats_table')
        
        # The first two tables are usually the team basic stats (home and away)
        # We need to identify which is which
        home_stats = None
        away_stats = None
        
        for table in team_tables[:2]:  # First two tables are team stats
            # Check the table caption or header to identify team
            caption = table.find('caption')
            if caption:
                caption_text = caption.get_text().upper()
                if home_abbrev.upper() in caption_text or 'HOME' in caption_text:
                    home_stats = self._extract_team_stats_from_table(table, game_id, home_team_id, True)
                elif away_abbrev.upper() in caption_text or 'AWAY' in caption_text:
                    away_stats = self._extract_team_stats_from_table(table, game_id, away_team_id, False)
        
        # If we didn't find by caption, try by position (first table is usually away, second is home)
        if not home_stats and len(team_tables) >= 2:
            away_stats = self._extract_team_stats_from_table(team_tables[0], game_id, away_team_id, False)
            home_stats = self._extract_team_stats_from_table(team_tables[1], game_id, home_team_id, True)
        elif not away_stats and len(team_tables) >= 1:
            away_stats = self._extract_team_stats_from_table(team_tables[0], game_id, away_team_id, False)
        
        if home_stats:
            team_stats_list.append(home_stats)
        if away_stats:
            team_stats_list.append(away_stats)
        
        return team_stats_list

    def _extract_team_stats_from_table(
        self,
        table: Any,
        game_id: str,
        team_id: str,
        is_home: bool
    ) -> Optional[Dict[str, Any]]:
        """Extract team statistics from a table row."""
        try:
            # Find the team totals row in tbody (not thead)
            tbody = table.find('tbody')
            if not tbody:
                return None
            
            rows = tbody.find_all('tr')
            totals_row = None
            
            for row in rows:
                # Look for the totals row (usually has 'Team Totals' or is the last data row)
                row_text = row.get_text().strip().upper()
                if 'TOTALS' in row_text or 'TEAM' in row_text:
                    totals_row = row
                    break
            
            # If no totals row found, use the last row in tbody
            if not totals_row and rows:
                totals_row = rows[-1]
            
            if not totals_row:
                return None
            
            # Extract cells from the data row (only td, not th)
            cells = totals_row.find_all('td')
            if len(cells) < 15:  # Need at least basic stats
                return None
            
            # Use header mapping for reliable extraction
            thead = table.find('thead')
            if not thead:
                return None
            
            # Get headers - handle both th and td in header row
            header_row = thead.find('tr')
            if not header_row:
                return None
            
            headers = []
            for th in header_row.find_all(['th', 'td']):
                header_text = th.get_text().strip()
                # Skip empty headers or colspan cells
                if header_text:
                    headers.append(header_text)
            
            # Create header mapping - use exact header name as key
            header_map = {}
            for i, header in enumerate(headers):
                header_clean = header.strip()
                header_upper = header_clean.upper()
                
                # Direct matches first
                header_map[header_clean] = i
                header_map[header_upper] = i
                
                # Map common variations
                variation_map = {
                    'FG': ['FGM'],
                    'FGA': ['FG ATT'],
                    'FG%': ['FG PCT', 'FG PERCENTAGE'],
                    '3P': ['3PM', '3PT'],
                    '3PA': ['3PT ATT'],
                    '3P%': ['3PT%', '3P PCT'],
                    'FT': ['FTM'],
                    'FTA': ['FT ATT'],
                    'FT%': ['FT PCT', 'FT PERCENTAGE'],
                    'ORB': ['OR', 'OFF REB'],
                    'DRB': ['DR', 'DEF REB'],
                    'TRB': ['REB', 'TOT REB'],
                    'AST': ['ASSISTS'],
                    'STL': ['STEALS'],
                    'BLK': ['BLOCKS'],
                    'TOV': ['TO', 'TURNOVERS'],
                    'PF': ['FOULS', 'PERSONAL FOULS'],
                    'PTS': ['POINTS']
                }
                
                for key, variations in variation_map.items():
                    if header_upper == key or header_upper in variations:
                        header_map[key] = i
                        header_map[key.upper()] = i
            
            # Extract row data
            row_data = [cell.get_text().strip() for cell in cells]
            
            # Debug: Log what we have
            logger.debug(f"Headers: {headers}")
            logger.debug(f"Row data: {row_data}")
            logger.debug(f"Header map: {header_map}")
            
            def get_stat(header_name: str, default: Any = 0) -> Any:
                """Get stat value by header name."""
                # Try to find header index directly
                try:
                    idx = headers.index(header_name)
                    if idx < len(row_data):
                        value = row_data[idx]
                        logger.debug(f"Found {header_name} at index {idx}: {value}")
                        return value
                except ValueError:
                    logger.debug(f"Header '{header_name}' not found in headers list")
                    pass
                
                # Try header map
                if header_name in header_map:
                    idx = header_map[header_name]
                    if idx < len(row_data):
                        value = row_data[idx]
                        logger.debug(f"Found {header_name} in map at index {idx}: {value}")
                        return value
                
                # Try uppercase
                if header_name.upper() in header_map:
                    idx = header_map[header_name.upper()]
                    if idx < len(row_data):
                        value = row_data[idx]
                        logger.debug(f"Found {header_name.upper()} in map at index {idx}: {value}")
                        return value
                
                logger.debug(f"Could not find {header_name}, returning default {default}")
                return default
            
            # Extract all stats
            fgm = self._parse_int(get_stat('FG', 0))
            fga = self._parse_int(get_stat('FGA', 0))
            fg_pct_str = get_stat('FG%', '0')
            fg_pct = self._parse_float(fg_pct_str, 0.0)
            # Convert percentage from decimal (0.467) to percentage (46.7)
            if fg_pct < 1.0 and fg_pct > 0:
                fg_pct *= 100.0
            
            three_pm = self._parse_int(get_stat('3P', 0))
            three_pa = self._parse_int(get_stat('3PA', 0))
            three_pct_str = get_stat('3P%', '0')
            three_pct = self._parse_float(three_pct_str, 0.0)
            if three_pct < 1.0 and three_pct > 0:
                three_pct *= 100.0
            
            ftm = self._parse_int(get_stat('FT', 0))
            fta = self._parse_int(get_stat('FTA', 0))
            ft_pct_str = get_stat('FT%', '0')
            ft_pct = self._parse_float(ft_pct_str, 0.0)
            if ft_pct < 1.0 and ft_pct > 0:
                ft_pct *= 100.0
            
            orb = self._parse_int(get_stat('ORB', 0))
            drb = self._parse_int(get_stat('DRB', 0))
            trb = self._parse_int(get_stat('TRB', 0))
            if trb == 0:
                trb = orb + drb
            
            assists = self._parse_int(get_stat('AST', 0))
            steals = self._parse_int(get_stat('STL', 0))
            blocks = self._parse_int(get_stat('BLK', 0))
            turnovers = self._parse_int(get_stat('TOV', 0))
            fouls = self._parse_int(get_stat('PF', 0))
            points = self._parse_int(get_stat('PTS', 0))
            
            # Calculate advanced metrics
            ts_pct = None
            efg_pct = None
            if fga > 0 or fta > 0:
                ts_denominator = 2 * (fga + 0.44 * fta)
                if ts_denominator > 0:
                    ts_pct = (points / ts_denominator) * 100.0
            
            if fga > 0:
                efg_pct = ((fgm + 0.5 * three_pm) / fga) * 100.0
            
            team_stats = {
                'game_id': game_id,
                'team_id': team_id,
                'is_home': is_home,
                'points': points,
                'field_goals_made': fgm,
                'field_goals_attempted': fga,
                'field_goal_percentage': fg_pct,
                'three_pointers_made': three_pm,
                'three_pointers_attempted': three_pa,
                'three_point_percentage': three_pct,
                'free_throws_made': ftm,
                'free_throws_attempted': fta,
                'free_throw_percentage': ft_pct,
                'rebounds_offensive': orb,
                'rebounds_defensive': drb,
                'rebounds_total': trb,
                'assists': assists,
                'steals': steals,
                'blocks': blocks,
                'turnovers': turnovers,
                'personal_fouls': fouls,
                'offensive_rating': None,
                'defensive_rating': None,
                'pace': None,
                'true_shooting_percentage': ts_pct,
                'effective_field_goal_percentage': efg_pct
            }
            
            return team_stats
            
        except Exception as e:
            logger.error(f"Error extracting team stats from table: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None

    def _parse_player_stats(
        self,
        soup: BeautifulSoup,
        game_id: str,
        home_team_id: str,
        away_team_id: str
    ) -> List[Dict[str, Any]]:
        """Parse player statistics from the boxscore page."""
        player_stats_list = []
        
        # Find player stats tables (usually have class 'sortable stats_table')
        # There are typically 4 tables: 2 for team basic stats, 2 for player basic stats
        tables = soup.find_all('table', class_='sortable stats_table')
        
        # Player tables usually come after team tables
        # We need to identify which table belongs to which team
        home_abbrev = self._get_team_abbrev(home_team_id)
        away_abbrev = self._get_team_abbrev(away_team_id)
        
        # Basketball Reference typically has:
        # Table 0: Away team basic stats
        # Table 1: Home team basic stats
        # Table 2: Away team player stats
        # Table 3: Home team player stats
        
        # Extract away team player stats (usually table index 2)
        if len(tables) > 2:
            away_players = self._extract_player_stats_from_table(tables[2], game_id, away_team_id)
            player_stats_list.extend(away_players)
        
        # Extract home team player stats (usually table index 3)
        if len(tables) > 3:
            home_players = self._extract_player_stats_from_table(tables[3], game_id, home_team_id)
            player_stats_list.extend(home_players)
        
        return player_stats_list

    def _extract_player_stats_from_table(
        self,
        table: Any,
        game_id: str,
        team_id: str
    ) -> List[Dict[str, Any]]:
        """Extract player statistics from a table."""
        player_stats_list = []
        
        try:
            # Get table headers
            thead = table.find('thead')
            if not thead:
                return []
            
            headers = [th.get_text().strip() for th in thead.find_all('th')]
            header_map = {header.upper(): i for i, header in enumerate(headers)}
            
            # Find data rows (skip header and totals row)
            tbody = table.find('tbody')
            if not tbody:
                return []
            
            rows = tbody.find_all('tr')
            
            for row in rows:
                # Skip totals row
                row_text = row.get_text().strip().upper()
                if 'TOTALS' in row_text or 'TEAM' in row_text:
                    continue
                
                cells = row.find_all(['td', 'th'])
                if len(cells) < 10:  # Need at least basic stats
                    continue
                
                # Extract player name (usually first column)
                player_name_cell = cells[0]
                player_name = player_name_cell.get_text().strip()
                
                # Skip if no name or if it's a header
                if not player_name or player_name.upper() in ['PLAYER', 'RESERVES']:
                    continue
                
                # Get player link to extract player ID
                player_link = player_name_cell.find('a')
                player_id = None
                if player_link and player_link.get('href'):
                    href = player_link.get('href')
                    # Extract player ID from URL like /players/j/jamesle01.html
                    match = re.search(r'/players/([a-z])/([a-z]+)(\d+)\.html', href)
                    if match:
                        player_id = f"{match.group(2)}{match.group(3)}"
                
                # If no player ID from link, create a hash from name
                if not player_id:
                    # Simple hash from name (not ideal, but works)
                    player_id = f"br_{hash(player_name) % 1000000}"
                
                # Extract stats using header mapping
                def get_stat(header_name: str, default: Any = 0) -> Any:
                    if header_name in header_map:
                        idx = header_map[header_name]
                        if idx < len(cells):
                            return cells[idx].get_text().strip()
                    return default
                
                minutes = self._parse_minutes(get_stat('MP', '0:00'))
                points = self._parse_int(get_stat('PTS', 0))
                rebounds = self._parse_int(get_stat('TRB', 0))
                assists = self._parse_int(get_stat('AST', 0))
                fgm = self._parse_int(get_stat('FG', 0))
                fga = self._parse_int(get_stat('FGA', 0))
                three_pm = self._parse_int(get_stat('3P', 0))
                three_pa = self._parse_int(get_stat('3PA', 0))
                ftm = self._parse_int(get_stat('FT', 0))
                fta = self._parse_int(get_stat('FTA', 0))
                
                # Plus/minus might be in a different table (advanced stats)
                plus_minus = None
                plus_minus_str = get_stat('+/-', None)
                if plus_minus_str:
                    plus_minus = self._parse_int(plus_minus_str, None)
                
                # Skip if player didn't play (no meaningful stats)
                if points == 0 and rebounds == 0 and assists == 0 and fga == 0:
                    continue
                
                # Determine injury status
                injury_status = 'healthy'
                if minutes == '0:00' or minutes == '00:00':
                    injury_status = 'out'
                elif minutes and ':' in minutes:
                    try:
                        mins, secs = minutes.split(':')
                        total_seconds = int(mins) * 60 + int(secs)
                        if total_seconds < 300:  # Less than 5 minutes
                            injury_status = 'questionable'
                    except (ValueError, IndexError):
                        pass
                
                player_stat = {
                    'game_id': game_id,
                    'player_id': player_id,
                    'team_id': team_id,
                    'player_name': player_name,
                    'minutes_played': minutes,
                    'points': points,
                    'rebounds': rebounds,
                    'assists': assists,
                    'field_goals_made': fgm,
                    'field_goals_attempted': fga,
                    'three_pointers_made': three_pm,
                    'three_pointers_attempted': three_pa,
                    'free_throws_made': ftm,
                    'free_throws_attempted': fta,
                    'plus_minus': plus_minus,
                    'injury_status': injury_status
                }
                
                player_stats_list.append(player_stat)
            
            return player_stats_list
            
        except Exception as e:
            logger.error(f"Error extracting player stats from table: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return []

