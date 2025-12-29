"""Matchup Features Calculator - Calculates head-to-head and style matchup features."""

import logging
from typing import Optional, Dict, Tuple
from datetime import date
from src.database.db_manager import DatabaseManager
from src.features.team_features import TeamFeatureCalculator
from config.settings import get_settings

logger = logging.getLogger(__name__)


class MatchupFeatureCalculator:
    """Calculates head-to-head and style matchup features."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize matchup feature calculator.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.settings = get_settings()
        self.team_calculator = TeamFeatureCalculator(db_manager)
        
        logger.info("MatchupFeatureCalculator initialized")
    
    def get_head_to_head_record(
        self,
        team1_id: str,
        team2_id: str,
        games_back: int = 5,
        end_date: Optional[date] = None
    ) -> Dict[str, int]:
        """
        Get head-to-head record between two teams.
        
        Args:
            team1_id: First team identifier
            team2_id: Second team identifier
            games_back: Number of recent H2H games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Dictionary with 'team1_wins', 'team2_wins', 'total_games'
        """
        # Get games where both teams played
        games = self.db_manager.get_games(
            team_id=team1_id,
            end_date=end_date,
            limit=games_back * 10  # Get more to filter
        )
        
        # Filter to only games between these two teams
        h2h_games = [
            g for g in games
            if (g.home_team_id == team1_id and g.away_team_id == team2_id) or
               (g.home_team_id == team2_id and g.away_team_id == team1_id)
        ]
        
        # Sort by date descending and take games_back
        h2h_games = sorted(h2h_games, key=lambda x: x.game_date, reverse=True)[:games_back]
        
        team1_wins = 0
        team2_wins = 0
        
        for game in h2h_games:
            if game.winner == team1_id:
                team1_wins += 1
            elif game.winner == team2_id:
                team2_wins += 1
        
        return {
            'team1_wins': team1_wins,
            'team2_wins': team2_wins,
            'total_games': len(h2h_games)
        }
    
    def get_avg_point_differential_h2h(
        self,
        team1_id: str,
        team2_id: str,
        games_back: int = 5,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Get average point differential in head-to-head games (from team1's perspective).
        
        Args:
            team1_id: First team identifier
            team2_id: Second team identifier
            games_back: Number of recent H2H games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Average point differential or None if insufficient data
        """
        h2h_record = self.get_head_to_head_record(team1_id, team2_id, games_back, end_date)
        
        if h2h_record['total_games'] < 1:
            return None
        
        games = self.db_manager.get_games(
            team_id=team1_id,
            end_date=end_date,
            limit=games_back * 10
        )
        
        h2h_games = [
            g for g in games
            if (g.home_team_id == team1_id and g.away_team_id == team2_id) or
               (g.home_team_id == team2_id and g.away_team_id == team1_id)
        ]
        
        h2h_games = sorted(h2h_games, key=lambda x: x.game_date, reverse=True)[:games_back]
        
        total_differential = 0
        valid_games = 0
        
        for game in h2h_games:
            if not game.home_score or not game.away_score:
                continue
            
            if game.home_team_id == team1_id:
                diff = game.home_score - game.away_score
            else:
                diff = game.away_score - game.home_score
            
            total_differential += diff
            valid_games += 1
        
        if valid_games == 0:
            return None
        
        avg_differential = total_differential / valid_games
        return round(avg_differential, 2)
    
    def calculate_style_matchup(
        self,
        team1_id: str,
        team2_id: str,
        games_back: int = 10,
        end_date: Optional[date] = None
    ) -> Dict[str, Optional[float]]:
        """
        Calculate style matchup differences (pace, shooting, etc.).
        
        Args:
            team1_id: First team identifier
            team2_id: Second team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Dictionary with pace_diff, ts_diff, efg_diff
        """
        team1_pace = self.team_calculator.calculate_pace(team1_id, games_back, end_date)
        team2_pace = self.team_calculator.calculate_pace(team2_id, games_back, end_date)
        
        team1_ts = self.team_calculator.calculate_true_shooting(team1_id, games_back, end_date)
        team2_ts = self.team_calculator.calculate_true_shooting(team2_id, games_back, end_date)
        
        team1_efg = self.team_calculator.calculate_effective_fg_percentage(team1_id, games_back, end_date)
        team2_efg = self.team_calculator.calculate_effective_fg_percentage(team2_id, games_back, end_date)
        
        pace_diff = (team1_pace - team2_pace) if (team1_pace and team2_pace) else None
        ts_diff = (team1_ts - team2_ts) if (team1_ts and team2_ts) else None
        efg_diff = (team1_efg - team2_efg) if (team1_efg and team2_efg) else None
        
        return {
            'pace_differential': round(pace_diff, 2) if pace_diff is not None else None,
            'ts_differential': round(ts_diff, 2) if ts_diff is not None else None,
            'efg_differential': round(efg_diff, 2) if efg_diff is not None else None
        }
    
    def get_recent_form_comparison(
        self,
        team1_id: str,
        team2_id: str,
        games_back: int = 10,
        end_date: Optional[date] = None
    ) -> Dict[str, Optional[float]]:
        """
        Compare recent form (win percentage) between two teams.
        
        Args:
            team1_id: First team identifier
            team2_id: Second team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Dictionary with team1_win_pct, team2_win_pct, win_pct_diff
        """
        team1_win_pct = self.team_calculator.calculate_win_percentage(team1_id, games_back, False, end_date)
        team2_win_pct = self.team_calculator.calculate_win_percentage(team2_id, games_back, False, end_date)
        
        win_pct_diff = (team1_win_pct - team2_win_pct) if (team1_win_pct and team2_win_pct) else None
        
        return {
            'team1_win_pct': team1_win_pct,
            'team2_win_pct': team2_win_pct,
            'win_pct_differential': round(win_pct_diff, 2) if win_pct_diff is not None else None
        }
    
    def get_avg_score_h2h(
        self,
        team1_id: str,
        team2_id: str,
        games_back: int = 5,
        end_date: Optional[date] = None
    ) -> Dict[str, Optional[float]]:
        """
        Get average scores in head-to-head games.
        
        Args:
            team1_id: First team identifier
            team2_id: Second team identifier
            games_back: Number of recent H2H games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Dictionary with team1_avg_score, team2_avg_score
        """
        games = self.db_manager.get_games(
            team_id=team1_id,
            end_date=end_date,
            limit=games_back * 10
        )
        
        h2h_games = [
            g for g in games
            if (g.home_team_id == team1_id and g.away_team_id == team2_id) or
               (g.home_team_id == team2_id and g.away_team_id == team1_id)
        ]
        
        h2h_games = sorted(h2h_games, key=lambda x: x.game_date, reverse=True)[:games_back]
        
        team1_scores = []
        team2_scores = []
        
        for game in h2h_games:
            if not game.home_score or not game.away_score:
                continue
            
            if game.home_team_id == team1_id:
                team1_scores.append(game.home_score)
                team2_scores.append(game.away_score)
            else:
                team1_scores.append(game.away_score)
                team2_scores.append(game.home_score)
        
        team1_avg = round(sum(team1_scores) / len(team1_scores), 2) if team1_scores else None
        team2_avg = round(sum(team2_scores) / len(team2_scores), 2) if team2_scores else None
        
        return {
            'team1_avg_score': team1_avg,
            'team2_avg_score': team2_avg
        }

