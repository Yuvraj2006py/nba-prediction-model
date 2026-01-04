"""
Player Importance Calculator - Calculates player value to their team.

This module provides weighted importance scores for players based on their
statistical contributions. These scores are used to weight injury impacts.
"""

import logging
from typing import Dict, Optional, List, Tuple
from datetime import date, timedelta
import numpy as np
from src.database.db_manager import DatabaseManager
from src.database.models import PlayerStats, Game

logger = logging.getLogger(__name__)


class PlayerImportanceCalculator:
    """
    Calculates player importance scores based on historical performance.
    
    The importance score (0-1) reflects how valuable a player is to their team,
    considering points, assists, rebounds, plus_minus, and minutes played.
    """
    
    # Weights for importance calculation
    POINTS_WEIGHT = 0.40
    ASSISTS_WEIGHT = 0.25
    REBOUNDS_WEIGHT = 0.20
    PLUS_MINUS_WEIGHT = 0.15
    
    # Normalization constants (approximate max values for elite players)
    MAX_POINTS_PER_GAME = 35.0
    MAX_ASSISTS_PER_GAME = 12.0
    MAX_REBOUNDS_PER_GAME = 15.0
    MAX_PLUS_MINUS_PER_GAME = 15.0
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize player importance calculator.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.db_manager = db_manager or DatabaseManager()
        self._cache: Dict[str, float] = {}
        self._team_max_cache: Dict[str, float] = {}
        
        logger.info("PlayerImportanceCalculator initialized")
    
    def clear_cache(self):
        """Clear the importance score cache."""
        self._cache.clear()
        self._team_max_cache.clear()
    
    def _parse_minutes(self, minutes_str: str) -> float:
        """
        Parse minutes string to float.
        
        Args:
            minutes_str: Minutes in MM:SS format
            
        Returns:
            Total minutes as float
        """
        if not minutes_str or minutes_str in ['0:00', '00:00', 'DNP', 'DND']:
            return 0.0
        
        try:
            if ':' in str(minutes_str):
                parts = str(minutes_str).split(':')
                mins = int(parts[0])
                secs = int(parts[1]) if len(parts) > 1 else 0
                return mins + secs / 60.0
            else:
                return float(minutes_str)
        except (ValueError, IndexError):
            return 0.0
    
    def calculate_player_importance(
        self,
        player_id: str,
        team_id: str,
        games_back: int = 20,
        end_date: Optional[date] = None
    ) -> Dict[str, Optional[float]]:
        """
        Calculate player's importance to their team.
        
        Uses weighted formula:
        importance = (
            points * 0.40 + 
            assists * 0.25 + 
            rebounds * 0.20 + 
            plus_minus * 0.15
        ) / max_possible_score
        
        Args:
            player_id: Player identifier
            team_id: Team identifier
            games_back: Number of recent games to analyze
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Dictionary with importance metrics:
            - importance_score: 0-1 normalized score
            - avg_minutes: Average minutes played
            - avg_points: Average points per game
            - avg_assists: Average assists per game
            - avg_rebounds: Average rebounds per game
            - avg_plus_minus: Average plus/minus
            - games_played: Number of games analyzed
            - usage_rate: Minutes played / 48 (proxy for role)
        """
        # Check cache
        cache_key = f"{player_id}_{team_id}_{games_back}_{end_date}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # Build query for player stats
        with self.db_manager.get_session() as session:
            query = session.query(PlayerStats).filter(
                PlayerStats.player_id == player_id,
                PlayerStats.team_id == team_id
            )
            
            # Filter by end_date if provided
            if end_date:
                # Join with Game to filter by date
                query = query.join(
                    Game, 
                    PlayerStats.game_id == Game.game_id
                ).filter(
                    Game.game_date < end_date
                ).order_by(
                    Game.game_date.desc()
                )
            
            # Limit to games_back
            player_stats = query.limit(games_back).all()
        
        if not player_stats or len(player_stats) < 3:
            result = {
                'importance_score': None,
                'avg_minutes': None,
                'avg_points': None,
                'avg_assists': None,
                'avg_rebounds': None,
                'avg_plus_minus': None,
                'games_played': len(player_stats) if player_stats else 0,
                'usage_rate': None
            }
            return result
        
        # Calculate averages
        total_minutes = 0.0
        total_points = 0
        total_assists = 0
        total_rebounds = 0
        total_plus_minus = 0
        games_with_stats = 0
        
        for stat in player_stats:
            minutes = self._parse_minutes(stat.minutes_played)
            
            # Skip games where player didn't really play
            if minutes < 1.0:
                continue
            
            total_minutes += minutes
            total_points += stat.points or 0
            total_assists += stat.assists or 0
            total_rebounds += stat.rebounds or 0
            total_plus_minus += stat.plus_minus or 0
            games_with_stats += 1
        
        if games_with_stats == 0:
            result = {
                'importance_score': None,
                'avg_minutes': None,
                'avg_points': None,
                'avg_assists': None,
                'avg_rebounds': None,
                'avg_plus_minus': None,
                'games_played': 0,
                'usage_rate': None
            }
            return result
        
        # Calculate averages
        avg_minutes = total_minutes / games_with_stats
        avg_points = total_points / games_with_stats
        avg_assists = total_assists / games_with_stats
        avg_rebounds = total_rebounds / games_with_stats
        avg_plus_minus = total_plus_minus / games_with_stats
        
        # Calculate normalized importance score
        # Normalize each stat to 0-1 range, then apply weights
        norm_points = min(1.0, avg_points / self.MAX_POINTS_PER_GAME)
        norm_assists = min(1.0, avg_assists / self.MAX_ASSISTS_PER_GAME)
        norm_rebounds = min(1.0, avg_rebounds / self.MAX_REBOUNDS_PER_GAME)
        
        # Plus/minus can be negative, normalize to -1 to 1, then shift to 0-1
        norm_plus_minus = np.clip(
            avg_plus_minus / self.MAX_PLUS_MINUS_PER_GAME, 
            -1.0, 
            1.0
        )
        norm_plus_minus = (norm_plus_minus + 1.0) / 2.0  # Shift to 0-1
        
        # Calculate weighted importance
        importance_score = (
            norm_points * self.POINTS_WEIGHT +
            norm_assists * self.ASSISTS_WEIGHT +
            norm_rebounds * self.REBOUNDS_WEIGHT +
            norm_plus_minus * self.PLUS_MINUS_WEIGHT
        )
        
        # Apply minutes factor (players who play more are generally more important)
        minutes_factor = min(1.0, avg_minutes / 36.0)  # 36 min = full starter
        importance_score = importance_score * (0.5 + 0.5 * minutes_factor)
        
        # Ensure 0-1 range
        importance_score = max(0.0, min(1.0, importance_score))
        
        # Usage rate (minutes per game / 48)
        usage_rate = avg_minutes / 48.0
        
        result = {
            'importance_score': round(importance_score, 4),
            'avg_minutes': round(avg_minutes, 2),
            'avg_points': round(avg_points, 2),
            'avg_assists': round(avg_assists, 2),
            'avg_rebounds': round(avg_rebounds, 2),
            'avg_plus_minus': round(avg_plus_minus, 2),
            'games_played': games_with_stats,
            'usage_rate': round(usage_rate, 4)
        }
        
        # Cache result
        self._cache[cache_key] = result
        
        return result
    
    def get_importance_score(
        self,
        player_id: str,
        team_id: str,
        games_back: int = 20,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Get just the importance score for a player.
        
        Args:
            player_id: Player identifier
            team_id: Team identifier
            games_back: Number of recent games to analyze
            end_date: Cutoff date
            
        Returns:
            Importance score (0-1) or None if insufficient data
        """
        result = self.calculate_player_importance(
            player_id, team_id, games_back, end_date
        )
        return result.get('importance_score')
    
    def get_player_importance_by_name(
        self,
        player_name: str,
        team_id: str,
        games_back: int = 20,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Get player importance by name (for matching with injury reports).
        
        Args:
            player_name: Player full name
            team_id: Team identifier
            games_back: Number of recent games
            end_date: Cutoff date
            
        Returns:
            Importance score or None
        """
        with self.db_manager.get_session() as session:
            # Find player_id by name (case-insensitive partial match)
            player = session.query(PlayerStats).filter(
                PlayerStats.team_id == team_id
            ).filter(
                PlayerStats.player_name.ilike(f"%{player_name}%")
            ).first()
            
            if not player:
                return None
            
            return self.get_importance_score(
                player.player_id,
                team_id,
                games_back,
                end_date
            )
    
    def get_team_player_importances(
        self,
        team_id: str,
        games_back: int = 20,
        end_date: Optional[date] = None
    ) -> List[Dict]:
        """
        Get importance scores for all players on a team.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to analyze
            end_date: Cutoff date
            
        Returns:
            List of dicts with player_id, player_name, importance_score, sorted descending
        """
        # Get all unique players on this team
        with self.db_manager.get_session() as session:
            # Get games before end_date
            if end_date:
                game_ids = session.query(Game.game_id).filter(
                    Game.game_date < end_date,
                    (Game.home_team_id == team_id) | (Game.away_team_id == team_id)
                ).order_by(Game.game_date.desc()).limit(games_back).all()
                game_ids = [g[0] for g in game_ids]
            else:
                game_ids = []
            
            # Get unique players from those games
            if game_ids:
                players = session.query(
                    PlayerStats.player_id,
                    PlayerStats.player_name
                ).filter(
                    PlayerStats.team_id == team_id,
                    PlayerStats.game_id.in_(game_ids)
                ).distinct().all()
            else:
                # Get most recent players
                players = session.query(
                    PlayerStats.player_id,
                    PlayerStats.player_name
                ).filter(
                    PlayerStats.team_id == team_id
                ).distinct().all()
        
        # Calculate importance for each player
        player_importances = []
        for player_id, player_name in players:
            importance = self.calculate_player_importance(
                player_id, team_id, games_back, end_date
            )
            if importance.get('importance_score') is not None:
                player_importances.append({
                    'player_id': player_id,
                    'player_name': player_name,
                    'importance_score': importance['importance_score'],
                    'avg_minutes': importance['avg_minutes'],
                    'avg_points': importance['avg_points'],
                    'games_played': importance['games_played']
                })
        
        # Sort by importance descending
        player_importances.sort(
            key=lambda x: x['importance_score'] or 0, 
            reverse=True
        )
        
        return player_importances
    
    def get_top_players(
        self,
        team_id: str,
        top_n: int = 5,
        games_back: int = 20,
        end_date: Optional[date] = None
    ) -> List[Dict]:
        """
        Get the top N most important players on a team.
        
        Args:
            team_id: Team identifier
            top_n: Number of top players to return
            games_back: Number of recent games to analyze
            end_date: Cutoff date
            
        Returns:
            List of top N player dicts sorted by importance
        """
        all_players = self.get_team_player_importances(
            team_id, games_back, end_date
        )
        return all_players[:top_n]
    
    def get_team_total_importance(
        self,
        team_id: str,
        games_back: int = 20,
        end_date: Optional[date] = None
    ) -> float:
        """
        Get the total importance score for all players on a team.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to analyze
            end_date: Cutoff date
            
        Returns:
            Sum of all player importance scores
        """
        cache_key = f"team_total_{team_id}_{games_back}_{end_date}"
        if cache_key in self._team_max_cache:
            return self._team_max_cache[cache_key]
        
        all_players = self.get_team_player_importances(
            team_id, games_back, end_date
        )
        
        total = sum(
            p['importance_score'] for p in all_players 
            if p['importance_score'] is not None
        )
        
        self._team_max_cache[cache_key] = total
        return total

