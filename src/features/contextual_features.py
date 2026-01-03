"""Contextual Features Calculator - Calculates game context features."""

import logging
from typing import Optional
from datetime import date, timedelta
from src.database.db_manager import DatabaseManager
from config.settings import get_settings

logger = logging.getLogger(__name__)


class ContextualFeatureCalculator:
    """Calculates contextual features like rest days, home/away, etc."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize contextual feature calculator.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.settings = get_settings()
        
        logger.info("ContextualFeatureCalculator initialized")
    
    def calculate_rest_days(
        self,
        team_id: str,
        game_date: date
    ) -> Optional[int]:
        """
        Calculate days of rest since last game.
        
        Args:
            team_id: Team identifier
            game_date: Date of the current game
            
        Returns:
            Number of rest days or None if no previous game found
        """
        # Get previous game for this team
        games = self.db_manager.get_games(
            team_id=team_id,
            end_date=game_date - timedelta(days=1),  # Before current game
            limit=1
        )
        
        if not games:
            return None
        
        previous_game = games[0]
        if not previous_game.game_date:
            return None
        
        rest_days = (game_date - previous_game.game_date).days
        return max(rest_days, 0)  # Ensure non-negative
    
    def is_back_to_back(
        self,
        team_id: str,
        game_date: date
    ) -> bool:
        """
        Check if team is playing back-to-back games.
        
        Args:
            team_id: Team identifier
            game_date: Date of the current game
            
        Returns:
            True if playing back-to-back, False otherwise
        """
        rest_days = self.calculate_rest_days(team_id, game_date)
        return rest_days == 0 if rest_days is not None else False
    
    def is_home_game(
        self,
        team_id: str,
        game_id: str
    ) -> bool:
        """
        Check if team is playing at home.
        
        Args:
            team_id: Team identifier
            game_id: Game identifier
            
        Returns:
            True if home game, False if away
        """
        game = self.db_manager.get_game(game_id)
        if not game:
            return False
        
        return game.home_team_id == team_id
    
    def get_conference_matchup(
        self,
        team1_id: str,
        team2_id: str
    ) -> Optional[bool]:
        """
        Check if teams are in the same conference.
        
        Args:
            team1_id: First team identifier
            team2_id: Second team identifier
            
        Returns:
            True if same conference, False if different, None if unknown
        """
        team1 = self.db_manager.get_team(team1_id)
        team2 = self.db_manager.get_team(team2_id)
        
        if not team1 or not team2:
            return None
        
        if not team1.conference or not team2.conference:
            return None
        
        return team1.conference == team2.conference
    
    def get_division_matchup(
        self,
        team1_id: str,
        team2_id: str
    ) -> Optional[bool]:
        """
        Check if teams are in the same division.
        
        Args:
            team1_id: First team identifier
            team2_id: Second team identifier
            
        Returns:
            True if same division, False if different, None if unknown
        """
        team1 = self.db_manager.get_team(team1_id)
        team2 = self.db_manager.get_team(team2_id)
        
        if not team1 or not team2:
            return None
        
        if not team1.division or not team2.division:
            return None
        
        return team1.division == team2.division
    
    def get_season_type(
        self,
        game_id: str
    ) -> Optional[str]:
        """
        Get season type (Regular Season, Playoffs, etc.).
        
        Args:
            game_id: Game identifier
            
        Returns:
            Season type string or None
        """
        game = self.db_manager.get_game(game_id)
        if not game:
            return None
        
        return game.season_type
    
    def get_days_until_next_game(
        self,
        team_id: str,
        game_date: date
    ) -> Optional[int]:
        """
        Calculate days until next game (fatigue indicator).
        
        Args:
            team_id: Team identifier
            game_date: Date of the current game
            
        Returns:
            Number of days until next game or None if no next game
        """
        # Get next game for this team
        games = self.db_manager.get_games(
            team_id=team_id,
            start_date=game_date + timedelta(days=1),  # After current game
            limit=1
        )
        
        if not games:
            return None
        
        next_game = games[0]
        if not next_game.game_date:
            return None
        
        days_until = (next_game.game_date - game_date).days
        return max(days_until, 0)  # Ensure non-negative
    
    def calculate_games_in_last_7_days(
        self,
        team_id: str,
        game_date: date
    ) -> int:
        """
        Calculate number of games played in the last 7 days.
        
        Args:
            team_id: Team identifier
            game_date: Date of the current game
            
        Returns:
            Number of games in last 7 days
        """
        start_date = game_date - timedelta(days=7)
        games = self.db_manager.get_games(
            team_id=team_id,
            start_date=start_date,
            end_date=game_date - timedelta(days=1)  # Exclude current game
        )
        return len(games)
    
    def calculate_home_win_pct(
        self,
        team_id: str,
        games_back: int = 20,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate win percentage at home.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent home games to consider
            end_date: Cutoff date
            
        Returns:
            Home win percentage or None
        """
        from src.features.team_features import TeamFeatureCalculator
        team_calc = TeamFeatureCalculator(self.db_manager)
        return team_calc.calculate_win_percentage(team_id, games_back, True, end_date)
    
    def calculate_away_win_pct(
        self,
        team_id: str,
        games_back: int = 20,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate win percentage on the road.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent away games to consider
            end_date: Cutoff date
            
        Returns:
            Away win percentage or None
        """
        # Get away games (where team is NOT home)
        games = self.db_manager.get_games(
            team_id=team_id,
            end_date=end_date,
            limit=games_back * 2
        )
        
        # Filter for away games only
        away_games = [g for g in games if g.away_team_id == team_id]
        away_games = sorted(away_games, key=lambda x: x.game_date, reverse=True)[:games_back]
        
        if len(away_games) < 3:
            return None
        
        wins = sum(1 for g in away_games if g.winner == team_id)
        win_pct = (wins / len(away_games)) * 100
        return round(win_pct, 2)

