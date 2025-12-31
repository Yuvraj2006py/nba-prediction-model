"""Basketball Reference scraper using Selenium (bypasses 403 errors)."""

import logging
import time
import re
from typing import List, Dict, Any, Optional
from datetime import date
from bs4 import BeautifulSoup

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

from config.settings import get_settings
from src.database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


class BasketballReferenceSeleniumCollector:
    """Collects NBA game statistics using Selenium to bypass anti-scraping."""

    # Same team abbreviation mapping as the regular collector
    TEAM_ABBREV_MAP = {
        'ATL': 'ATL', 'BOS': 'BOS', 'BKN': 'BRK', 'CHA': 'CHO',
        'CHI': 'CHI', 'CLE': 'CLE', 'DAL': 'DAL', 'DEN': 'DEN',
        'DET': 'DET', 'GSW': 'GSW', 'HOU': 'HOU', 'IND': 'IND',
        'LAC': 'LAC', 'LAL': 'LAL', 'MEM': 'MEM', 'MIA': 'MIA',
        'MIL': 'MIL', 'MIN': 'MIN', 'NOP': 'NOP', 'NYK': 'NYK',
        'OKC': 'OKC', 'ORL': 'ORL', 'PHI': 'PHI', 'PHX': 'PHO',
        'POR': 'POR', 'SAC': 'SAC', 'SAS': 'SAS', 'TOR': 'TOR',
        'UTA': 'UTA', 'WAS': 'WAS',
    }

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        if not SELENIUM_AVAILABLE:
            raise ImportError("Selenium is not installed. Install with: pip install selenium")
        
        self.settings = get_settings()
        self.base_url = self.settings.BBALL_REF_BASE_URL
        self.db_manager = db_manager or DatabaseManager()
        self.scraping_delay = self.settings.SCRAPING_DELAY
        
        # Initialize Selenium driver
        self.driver = None
        self._init_driver()
        
        logger.info("Basketball Reference Selenium Collector initialized")

    def _init_driver(self):
        """Initialize Selenium WebDriver."""
        try:
            chrome_options = Options()
            chrome_options.add_argument('--headless')  # Run in background
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            
            # Set page load strategy to 'eager' (don't wait for all resources)
            chrome_options.page_load_strategy = 'eager'
            
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # Set longer timeouts
            self.driver.set_page_load_timeout(60)  # 60 seconds for page load
            self.driver.implicitly_wait(10)  # 10 seconds for element finding
            
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
        except Exception as e:
            logger.error(f"Failed to initialize Selenium driver: {e}")
            logger.error("Make sure ChromeDriver is installed and in PATH")
            raise

    def __del__(self):
        """Clean up driver on deletion."""
        if self.driver:
            try:
                self.driver.quit()
            except:
                pass

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
        """Build Basketball Reference boxscore URL."""
        date_str = game_date.strftime('%Y%m%d')
        url = f"{self.base_url}/boxscores/{date_str}0{home_team_abbrev}.html"
        return url

    def _fetch_page(self, url: str) -> Optional[BeautifulSoup]:
        """Fetch and parse a web page using Selenium."""
        try:
            self._rate_limit()
            
            # Try to load the page with timeout handling
            try:
                self.driver.get(url)
            except TimeoutException:
                # Page load timed out, but we might still have content
                logger.warning(f"Page load timeout for {url}, but continuing...")
            except WebDriverException as e:
                logger.error(f"WebDriver error loading {url}: {e}")
                return None
            
            # Wait for at least some content to load (more lenient)
            try:
                # Wait for body tag (page has started loading)
                WebDriverWait(self.driver, 15).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
            except TimeoutException:
                logger.warning(f"Timeout waiting for body tag: {url}")
                # Continue anyway, might have partial content
            
            # Try to wait for tables, but don't fail if they're not there
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "table"))
                )
            except TimeoutException:
                logger.debug(f"No tables found immediately for {url}, but continuing...")
            
            # Get page source and parse with BeautifulSoup
            page_source = self.driver.page_source
            
            # Check if we got actual content
            if not page_source or len(page_source) < 1000:
                logger.warning(f"Page source seems empty or too short for {url}")
                return None
            
            soup = BeautifulSoup(page_source, 'html.parser')
            
            # Verify we got some content
            if not soup.find('body'):
                logger.warning(f"No body tag found in page source for {url}")
                return None
            
            return soup
            
        except WebDriverException as e:
            logger.error(f"Selenium error fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None

    def collect_game_stats(self, game_id: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Collect both team and player stats for a game from Basketball Reference.
        
        This method uses the same parsing logic as the regular collector.
        We'll import and reuse those methods.
        """
        from src.data_collectors.basketball_reference_collector import (
            BasketballReferenceCollector
        )
        
        # Use the regular collector's parsing methods
        regular_collector = BasketballReferenceCollector(self.db_manager)
        
        logger.debug(f"Collecting stats from Basketball Reference (Selenium) for game {game_id}")
        
        # Get game from database
        game = self.db_manager.get_game(game_id)
        if not game:
            logger.warning(f"Game {game_id} not found in database")
            return {'team_stats': [], 'player_stats': []}
        
        # Get team abbreviations
        home_abbrev = self._get_team_abbrev(game.home_team_id)
        if not home_abbrev:
            logger.warning(f"Could not get team abbreviations for game {game_id}")
            return {'team_stats': [], 'player_stats': []}
        
        # Build URL
        boxscore_url = self._build_boxscore_url(game.game_date, home_abbrev)
        logger.debug(f"Fetching boxscore from: {boxscore_url}")
        
        # Fetch page using Selenium
        soup = self._fetch_page(boxscore_url)
        if not soup:
            logger.warning(f"Could not fetch boxscore page for game {game_id}")
            return {'team_stats': [], 'player_stats': []}
        
        # Use regular collector's parsing methods
        team_stats_list = regular_collector._parse_team_stats(
            soup, game_id, game.home_team_id, game.away_team_id
        )
        
        player_stats_list = regular_collector._parse_player_stats(
            soup, game_id, game.home_team_id, game.away_team_id
        )
        
        logger.debug(f"Collected {len(team_stats_list)} team stats and {len(player_stats_list)} player stats for game {game_id}")
        return {
            'team_stats': team_stats_list,
            'player_stats': player_stats_list
        }
    
    def get_game_details(self, game_id: str) -> Optional[Dict[str, Any]]:
        """
        Get game details using Selenium (same pattern as collect_game_stats).
        Uses Selenium to fetch the page, then reuses regular collector's parsing logic.
        """
        from src.data_collectors.basketball_reference_collector import (
            BasketballReferenceCollector
        )
        
        logger.debug(f"Getting game details from Basketball Reference (Selenium) for game {game_id}")
        
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
        logger.debug(f"Fetching boxscore from: {boxscore_url}")
        
        # Fetch page using Selenium (not requests!)
        soup = self._fetch_page(boxscore_url)
        if not soup:
            logger.warning(f"Could not fetch boxscore page for game {game_id}")
            return None
        
        # Use regular collector's parsing methods (same as collect_game_stats does)
        regular_collector = BasketballReferenceCollector(self.db_manager)
        
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
                score_pattern = r'(\d+)\s*[-–]\s*(\d+)'
                match = re.search(score_pattern, score_text)
                if match:
                    away_score = int(match.group(1))
                    home_score = int(match.group(2))
        
        # Alternative: Extract from team stats tables (points column)
        if home_score is None or away_score is None:
            team_stats_list = regular_collector._parse_team_stats(soup, game_id, game.home_team_id, game.away_team_id)
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
            'winner': winner,
            'point_differential': point_differential,
            'game_status': 'finished'
        }
        
        return game_details
    
    def collect_all_game_data(self, game_id: str) -> Dict[str, Any]:
        """
        Collect ALL game data (details + stats) from Basketball Reference.
        This is the equivalent of what NBA API collector does.
        
        Args:
            game_id: NBA game ID
            
        Returns:
            Dictionary with 'game_details', 'team_stats', and 'player_stats'
        """
        logger.debug(f"Collecting all game data from Basketball Reference (Selenium) for game {game_id}")
        
        # Get game details
        game_details = self.get_game_details(game_id)
        
        # Get stats
        stats = self.collect_game_stats(game_id)
        
        return {
            'game_details': game_details,
            'team_stats': stats['team_stats'],
            'player_stats': stats['player_stats']
        }
