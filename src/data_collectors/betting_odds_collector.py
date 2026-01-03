"""Betting Odds Collector - Fetches betting odds from The Odds API."""

import time
import logging
import requests
from typing import List, Dict, Any, Optional
from datetime import datetime, date, timedelta
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
    
    def fetch_odds_for_existing_games(self, game_date: date) -> int:
        """
        Fetch odds for games that already exist in database but don't have odds.
        This tries to match existing games with odds from the API.
        
        Args:
            game_date: Date to fetch odds for
            
        Returns:
            Number of betting lines stored
        """
        # First, get all odds for the date
        odds_data = self.get_odds_for_date(game_date)
        if not odds_data:
            logger.info(f"No odds data available for {game_date}")
            return 0
        
        # Get all games for this date that don't have odds
        from src.database.models import Game, BettingLine, Team
        with self.db_manager.get_session() as session:
            games_without_odds = session.query(Game).filter(
                Game.game_date == game_date,
                Game.home_score.is_(None)  # Not finished
            ).outerjoin(
                BettingLine, Game.game_id == BettingLine.game_id
            ).filter(
                BettingLine.id.is_(None)  # No betting lines
            ).all()
        
        if not games_without_odds:
            logger.info(f"All games for {game_date} already have odds")
            return 0
        
        logger.info(f"Found {len(games_without_odds)} games without odds, trying to match with API data...")
        
        # Try to match games with odds data
        from src.backtesting.team_mapper import TeamMapper
        team_mapper = TeamMapper(self.db_manager)
        matched_count = 0
        
        with self.db_manager.get_session() as session:
            for game in games_without_odds:
                # Get team names
                home_team = session.query(Team).filter_by(team_id=game.home_team_id).first()
                away_team = session.query(Team).filter_by(team_id=game.away_team_id).first()
                
                if not home_team or not away_team:
                    continue
                
                # Try to find matching odds data
                for odds_item in odds_data:
                    api_home = odds_item.get('home_team', '')
                    api_away = odds_item.get('away_team', '')
                    
                    # Check if teams match (try various name formats)
                    home_match = (
                        api_home.lower() == home_team.team_name.lower() or
                        api_home.lower() == home_team.city.lower() or
                        api_home.lower() in home_team.team_name.lower() or
                        home_team.team_name.lower() in api_home.lower()
                    )
                    
                    away_match = (
                        api_away.lower() == away_team.team_name.lower() or
                        api_away.lower() == away_team.city.lower() or
                        api_away.lower() in away_team.team_name.lower() or
                        away_team.team_name.lower() in api_away.lower()
                    )
                    
                    if home_match and away_match:
                        # Found a match! Store odds for this game
                        logger.info(f"Matched odds for {away_team.team_name} @ {home_team.team_name}")
                        stored = self._store_odds_for_game(game.game_id, odds_item)
                        if stored > 0:
                            matched_count += stored
                        break
        
        logger.info(f"Matched and stored odds for {matched_count} additional betting lines")
        return matched_count
    
    def _store_odds_for_game(self, game_id: str, odds_data: Dict[str, Any]) -> int:
        """Store odds data for a specific game."""
        stored_count = 0
        
        try:
            # Get home and away team names from API response for matching
            api_home_team = odds_data.get('home_team', '')
            api_away_team = odds_data.get('away_team', '')
            
            bookmakers = odds_data.get('bookmakers', [])
            for bookmaker in bookmakers:
                sportsbook = bookmaker.get('key', 'unknown')
                markets = bookmaker.get('markets', [])
                
                for market in markets:
                    line_data = self._extract_betting_line(
                        game_id,
                        sportsbook,
                        market,
                        api_home_team=api_home_team,
                        api_away_team=api_away_team
                    )
                    if line_data:
                        try:
                            self.db_manager.insert_betting_line(line_data)
                            stored_count += 1
                        except Exception as e:
                            logger.debug(f"Betting line already exists or error: {e}")
            
            return stored_count
        except Exception as e:
            logger.error(f"Error storing odds for game {game_id}: {e}")
            return 0
    
    def parse_and_store_odds(self, odds_data: List[Dict[str, Any]], preferred_sportsbook: str = 'draftkings') -> int:
        """
        Parse odds data and store in database.
        Prioritizes odds from preferred sportsbook (default: DraftKings).
        
        Args:
            odds_data: List of odds dictionaries from API
            preferred_sportsbook: Preferred sportsbook to prioritize (default: 'draftkings')
            
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
                
                # Sort bookmakers to prioritize preferred sportsbook
                bookmakers_sorted = sorted(
                    bookmakers,
                    key=lambda b: b.get('key', '').lower() != preferred_sportsbook.lower()
                )
                
                # Get home and away team names from API response for matching
                api_home_team = game_odds.get('home_team', '')
                api_away_team = game_odds.get('away_team', '')
                
                for bookmaker in bookmakers_sorted:
                    sportsbook = bookmaker.get('key', 'unknown')
                    markets = bookmaker.get('markets', [])
                    
                    for market in markets:
                        line_data = self._extract_betting_line(
                            game_id,
                            sportsbook,
                            market,
                            api_home_team=api_home_team,
                            api_away_team=api_away_team
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
        Extract or generate NBA game ID from odds data.
        
        The Odds API doesn't provide NBA game IDs directly, so we generate
        one based on teams and date.
        
        Args:
            odds_data: Odds dictionary from API
            
        Returns:
            Game ID or None
        """
        try:
            home_team = odds_data.get('home_team', '')
            away_team = odds_data.get('away_team', '')
            commence_time = odds_data.get('commence_time', '')
            
            if not home_team or not away_team:
                return None
            
            # Parse date and convert to US timezone for game ID
            # Games that are early morning UTC (0-6 AM) are actually "tonight" in US time
            if commence_time:
                try:
                    if 'T' in commence_time:
                        # Parse UTC datetime
                        if commence_time.endswith('Z'):
                            game_datetime_utc = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                        else:
                            game_datetime_utc = datetime.fromisoformat(commence_time)
                        
                        # Convert to US Eastern Time (most NBA games are in ET)
                        from datetime import timezone, timedelta
                        et_tz = timezone(timedelta(hours=-5))
                        game_datetime_us = game_datetime_utc.astimezone(et_tz)
                        game_date = game_datetime_us.date()
                    else:
                        game_date = datetime.strptime(commence_time, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    game_date = date.today()
            else:
                game_date = date.today()
            
            # Map team names to IDs
            from src.backtesting.team_mapper import TeamMapper
            team_mapper = TeamMapper(self.db_manager)
            
            home_team_id = team_mapper.map_team_name_to_id(home_team)
            away_team_id = team_mapper.map_team_name_to_id(away_team)
            
            if not home_team_id or not away_team_id:
                logger.warning(f"Could not map teams: {home_team} -> {home_team_id}, {away_team} -> {away_team_id}")
                return None
            
            # Generate game ID: DATE + AWAY_TEAM_ID + HOME_TEAM_ID
            # Format: YYYYMMDD + last 3 digits of each team ID
            date_str = game_date.strftime('%Y%m%d')
            home_suffix = home_team_id[-3:]
            away_suffix = away_team_id[-3:]
            game_id = f"{date_str}{away_suffix}{home_suffix}"
            
            return game_id
            
        except Exception as e:
            logger.error(f"Error extracting game ID from odds: {e}")
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
            
            # Extract commence time and convert to US timezone
            commence_time = odds_data.get('commence_time', '')
            if commence_time:
                try:
                    if 'T' in commence_time:
                        # Parse UTC datetime
                        if commence_time.endswith('Z'):
                            dt_utc = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                        else:
                            dt_utc = datetime.fromisoformat(commence_time)
                        
                        # Convert to US Eastern Time (most NBA games are in ET)
                        # Use UTC-5 for EST or UTC-4 for EDT (simplified to UTC-5)
                        from datetime import timezone, timedelta
                        et_tz = timezone(timedelta(hours=-5))
                        dt_us = dt_utc.astimezone(et_tz)
                        game_date = dt_us.date()
                    else:
                        game_date = datetime.strptime(commence_time, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    game_date = date.today()
            else:
                game_date = date.today()
            
            # Map team names to team IDs
            from src.backtesting.team_mapper import TeamMapper
            team_mapper = TeamMapper(self.db_manager)
            
            home_team_id = team_mapper.map_team_name_to_id(home_team)
            away_team_id = team_mapper.map_team_name_to_id(away_team)
            
            if not home_team_id or not away_team_id:
                logger.warning(f"Could not map teams for game creation: {home_team}, {away_team}")
                return None
            
            # Generate game ID
            game_id = self._extract_game_id_from_odds(odds_data)
            if not game_id:
                return None
            
            # Determine season (approximate)
            if game_date.month >= 10:
                season = f"{game_date.year}-{str(game_date.year + 1)[-2:]}"
            else:
                season = f"{game_date.year - 1}-{str(game_date.year)[-2:]}"
            
            return {
                'game_id': game_id,
                'season': season,
                'season_type': 'Regular Season',  # Default, could be improved
                'game_date': game_date,
                'home_team_id': home_team_id,
                'away_team_id': away_team_id,
                'game_status': 'scheduled'
            }
            
        except Exception as e:
            logger.error(f"Error creating game from odds: {e}")
            return None
    
    def _extract_betting_line(
        self,
        game_id: str,
        sportsbook: str,
        market: Dict[str, Any],
        api_home_team: str = '',
        api_away_team: str = ''
    ) -> Optional[Dict[str, Any]]:
        """
        Extract betting line from market data.
        
        Args:
            game_id: Game ID
            sportsbook: Sportsbook name
            market: Market dictionary from API
            api_home_team: Home team name from API (for matching outcomes)
            api_away_team: Away team name from API (for matching outcomes)
            
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
                    name = outcome.get('name', '').strip()
                    price = outcome.get('price', None)
                    
                    if price is None:
                        continue
                    
                    # Match outcome name to home or away team
                    # Try various matching strategies
                    name_lower = name.lower()
                    home_lower = api_home_team.lower() if api_home_team else ''
                    away_lower = api_away_team.lower() if api_away_team else ''
                    
                    is_home = False
                    is_away = False
                    
                    if home_lower and away_lower:
                        # Check if outcome name matches home team
                        if (name_lower == home_lower or 
                            name_lower in home_lower or 
                            home_lower in name_lower or
                            any(word in name_lower for word in home_lower.split() if len(word) > 3)):
                            is_home = True
                        # Check if outcome name matches away team
                        elif (name_lower == away_lower or 
                              name_lower in away_lower or 
                              away_lower in name_lower or
                              any(word in name_lower for word in away_lower.split() if len(word) > 3)):
                            is_away = True
                    
                    # Store odds in correct field
                    if is_home:
                        line_data['moneyline_home'] = int(price)
                    elif is_away:
                        line_data['moneyline_away'] = int(price)
                    else:
                        # Fallback: if we can't match, store in order (first = home, second = away)
                        # This is not ideal but better than nothing
                        if line_data['moneyline_home'] is None:
                            line_data['moneyline_home'] = int(price)
                        else:
                            line_data['moneyline_away'] = int(price)
            
            elif market_key == 'spreads':  # Point spread
                for outcome in outcomes:
                    name = outcome.get('name', '').strip()
                    point = outcome.get('point', None)
                    price = outcome.get('price', None)
                    
                    if point is None:
                        continue
                    
                    point_float = float(point)
                    
                    # Match outcome name to home or away team (same logic as moneyline)
                    name_lower = name.lower()
                    home_lower = api_home_team.lower() if api_home_team else ''
                    away_lower = api_away_team.lower() if api_away_team else ''
                    
                    is_home = False
                    is_away = False
                    
                    if home_lower and away_lower:
                        if (name_lower == home_lower or 
                            name_lower in home_lower or 
                            home_lower in name_lower or
                            any(word in name_lower for word in home_lower.split() if len(word) > 3)):
                            is_home = True
                        elif (name_lower == away_lower or 
                              name_lower in away_lower or 
                              away_lower in name_lower or
                              any(word in name_lower for word in away_lower.split() if len(word) > 3)):
                            is_away = True
                    
                    # Store spread in correct field
                    if is_home:
                        line_data['point_spread_home'] = point_float
                    elif is_away:
                        line_data['point_spread_away'] = point_float
                    else:
                        # Fallback: store in order
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

