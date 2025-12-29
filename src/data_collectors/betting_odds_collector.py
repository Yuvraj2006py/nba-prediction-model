"""Betting Odds Collector - Fetches betting odds from The Odds API."""

import time
import logging
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, date
from config.settings import get_settings
from src.database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)


class BettingOddsCollector:
    """Collects betting odds from The Odds API."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize betting odds collector.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.settings = get_settings()
        self.db_manager = db_manager or DatabaseManager()
        self.api_key = self.settings.BETTING_API_KEY
        self.base_url = self.settings.BETTING_API_BASE_URL
        self.rate_limit_delay = self.settings.RATE_LIMIT_DELAY
        self.max_retries = self.settings.MAX_RETRIES
        self.retry_delay = self.settings.RETRY_DELAY
        
        if not self.api_key or self.api_key == 'your_betting_api_key_here':
            logger.warning("Betting API key not configured. Set BETTING_API_KEY in .env file.")
        
        logger.info("Betting Odds Collector initialized")
    
    def _rate_limit(self):
        """Apply rate limiting delay."""
        time.sleep(self.rate_limit_delay)
    
    def _make_api_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Make API request with retry logic.
        
        Args:
            endpoint: API endpoint path
            params: Query parameters
            
        Returns:
            JSON response or None
        """
        if not self.api_key or self.api_key == 'your_betting_api_key_here':
            logger.error("Betting API key not configured")
            return None
        
        url = f"{self.base_url}/{endpoint}"
        headers = {
            'Accept': 'application/json'
        }
        
        if params is None:
            params = {}
        params['apiKey'] = self.api_key
        
        for attempt in range(self.max_retries):
            try:
                self._rate_limit()
                response = requests.get(url, headers=headers, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 401:
                    logger.error("Invalid API key for betting odds API")
                    return None
                elif response.status_code == 429:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Rate limited. Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    logger.warning(f"API request failed with status {response.status_code}: {response.text}")
                    if attempt < self.max_retries - 1:
                        wait_time = self.retry_delay * (2 ** attempt)
                        time.sleep(wait_time)
                    else:
                        return None
                        
            except requests.exceptions.RequestException as e:
                if attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.max_retries}): {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    logger.error(f"Request failed after {self.max_retries} attempts: {e}")
                    return None
        
        return None
    
    def get_sports(self) -> List[Dict[str, Any]]:
        """
        Get list of available sports.
        
        Returns:
            List of sport dictionaries
        """
        logger.debug("Fetching available sports")
        response = self._make_api_request('sports')
        
        if response:
            logger.info(f"Found {len(response)} sports")
            return response
        else:
            logger.warning("Failed to fetch sports")
            return []
    
    def get_nba_odds(
        self,
        sport: str = 'basketball_nba',
        regions: str = 'us',
        markets: str = 'h2h,spreads,totals',
        odds_format: str = 'american',
        date_format: str = 'iso'
    ) -> List[Dict[str, Any]]:
        """
        Get NBA betting odds.
        
        Args:
            sport: Sport key (default: 'basketball_nba')
            regions: Comma-separated list of regions (default: 'us')
            markets: Comma-separated list of markets (default: 'h2h,spreads,totals')
            odds_format: Odds format - 'american' or 'decimal' (default: 'american')
            date_format: Date format - 'iso' or 'unix' (default: 'iso')
            
        Returns:
            List of game odds dictionaries
        """
        logger.info(f"Fetching NBA odds for sport: {sport}")
        
        params = {
            'sport': sport,
            'regions': regions,
            'markets': markets,
            'oddsFormat': odds_format,
            'dateFormat': date_format
        }
        
        # Correct endpoint format: sports/{sport}/odds
        endpoint = f'sports/{sport}/odds'
        response = self._make_api_request(endpoint, params)
        
        if response:
            logger.info(f"Found {len(response)} games with odds")
            return response
        else:
            logger.warning("Failed to fetch NBA odds")
            return []
    
    def get_odds_for_date(
        self,
        target_date: Optional[date] = None,
        sport: str = 'basketball_nba',
        regions: str = 'us',
        markets: str = 'h2h,spreads,totals',
        odds_format: str = 'american'
    ) -> List[Dict[str, Any]]:
        """
        Get NBA odds for a specific date.
        
        Args:
            target_date: Date to get odds for (default: today)
            sport: Sport key (default: 'basketball_nba')
            regions: Comma-separated list of regions (default: 'us')
            markets: Comma-separated list of markets (default: 'h2h,spreads,totals')
            odds_format: Odds format - 'american' or 'decimal' (default: 'american')
            
        Returns:
            List of game odds dictionaries
        """
        if target_date is None:
            target_date = date.today()
        
        logger.info(f"Fetching NBA odds for date: {target_date}")
        
        # Format date as ISO string (YYYY-MM-DD)
        date_str = target_date.isoformat()
        
        params = {
            'sport': sport,
            'regions': regions,
            'markets': markets,
            'oddsFormat': odds_format,
            'dateFormat': 'iso'
        }
        
        # The Odds API endpoint for specific date: sports/{sport}/odds
        endpoint = f'sports/{sport}/odds'
        params['commenceTimeFrom'] = f"{date_str}T00:00:00Z"
        params['commenceTimeTo'] = f"{date_str}T23:59:59Z"
        
        response = self._make_api_request(endpoint, params)
        
        if response:
            logger.info(f"Found {len(response)} games with odds for {target_date}")
            return response
        else:
            logger.warning(f"Failed to fetch NBA odds for {target_date}")
            return []
    
    def parse_and_store_odds(self, odds_data: List[Dict[str, Any]]) -> int:
        """
        Parse odds data and store in database.
        
        Args:
            odds_data: List of odds dictionaries from API
            
        Returns:
            Number of betting lines stored
        """
        stored_count = 0
        
        for game_odds in odds_data:
            try:
                # Extract game information
                game_id = self._extract_game_id_from_odds(game_odds)
                if not game_id:
                    logger.warning(f"Could not extract game ID from odds data: {game_odds.get('id', 'unknown')}")
                    continue
                
                # Get or create game in database
                game = self.db_manager.get_game(game_id)
                if not game:
                    # Try to create game from odds data
                    game_data = self._create_game_from_odds(game_odds)
                    if game_data:
                        game = self.db_manager.insert_game(game_data)
                    else:
                        logger.warning(f"Could not create game from odds data for {game_id}")
                        continue
                
                # Extract and store betting lines
                bookmakers = game_odds.get('bookmakers', [])
                for bookmaker in bookmakers:
                    sportsbook = bookmaker.get('key', 'unknown')
                    markets = bookmaker.get('markets', [])
                    
                    for market in markets:
                        line_data = self._extract_betting_line(
                            game_id,
                            sportsbook,
                            market
                        )
                        if line_data:
                            try:
                                self.db_manager.insert_betting_line(line_data)
                                stored_count += 1
                            except Exception as e:
                                logger.warning(f"Error storing betting line: {e}")
                
            except Exception as e:
                logger.error(f"Error processing odds data: {e}")
                continue
        
        logger.info(f"Stored {stored_count} betting lines")
        return stored_count
    
    def _extract_game_id_from_odds(self, odds_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract NBA game ID from odds data.
        
        The Odds API doesn't provide NBA game IDs directly, so we need to
        match teams and dates. For now, we'll use a combination approach.
        
        Args:
            odds_data: Odds dictionary from API
            
        Returns:
            Game ID or None
        """
        # The Odds API provides 'id' which is their own identifier
        # We'll need to match by teams and date
        # For now, return None and let the caller handle game creation
        return None
    
    def _create_game_from_odds(self, odds_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Create game data from odds information.
        
        Args:
            odds_data: Odds dictionary from API
            
        Returns:
            Game data dictionary or None
        """
        try:
            # Extract teams
            home_team = odds_data.get('home_team', '')
            away_team = odds_data.get('away_team', '')
            
            if not home_team or not away_team:
                return None
            
            # Extract commence time
            commence_time = odds_data.get('commence_time', '')
            if commence_time:
                try:
                    if 'T' in commence_time:
                        game_date = datetime.fromisoformat(commence_time.replace('Z', '+00:00')).date()
                    else:
                        game_date = datetime.strptime(commence_time, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    game_date = date.today()
            else:
                game_date = date.today()
            
            # Map team names to team IDs (would need a mapping)
            # For now, return None as we need team IDs
            return None
            
        except Exception as e:
            logger.error(f"Error creating game from odds: {e}")
            return None
    
    def _extract_betting_line(
        self,
        game_id: str,
        sportsbook: str,
        market: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Extract betting line from market data.
        
        Args:
            game_id: Game ID
            sportsbook: Sportsbook name
            market: Market dictionary from API
            
        Returns:
            Betting line dictionary or None
        """
        try:
            market_key = market.get('key', '')
            outcomes = market.get('outcomes', [])
            
            line_data = {
                'game_id': game_id,
                'sportsbook': sportsbook,
                'point_spread_home': None,
                'point_spread_away': None,
                'moneyline_home': None,
                'moneyline_away': None,
                'over_under': None
            }
            
            if market_key == 'h2h':  # Moneyline
                for outcome in outcomes:
                    name = outcome.get('name', '')
                    price = outcome.get('price', None)
                    
                    # Determine if home or away (would need team mapping)
                    # For now, store both
                    if price is not None:
                        if line_data['moneyline_home'] is None:
                            line_data['moneyline_home'] = int(price)
                        else:
                            line_data['moneyline_away'] = int(price)
            
            elif market_key == 'spreads':  # Point spread
                for outcome in outcomes:
                    name = outcome.get('name', '')
                    point = outcome.get('point', None)
                    price = outcome.get('price', None)
                    
                    if point is not None:
                        point_float = float(point)
                        if line_data['point_spread_home'] is None:
                            line_data['point_spread_home'] = point_float
                        else:
                            line_data['point_spread_away'] = point_float
            
            elif market_key == 'totals':  # Over/Under
                for outcome in outcomes:
                    name = outcome.get('name', '')
                    point = outcome.get('point', None)
                    
                    if point is not None and 'over' in name.lower():
                        line_data['over_under'] = float(point)
            
            # Only return if we have at least one line
            if any([
                line_data['point_spread_home'] is not None,
                line_data['moneyline_home'] is not None,
                line_data['over_under'] is not None
            ]):
                return line_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting betting line: {e}")
            return None

