"""Team Features Calculator - Calculates team performance metrics."""

import logging
import numpy as np
from typing import Optional, List, Dict
from datetime import date
from src.database.db_manager import DatabaseManager
from src.database.models import TeamStats, Game, PlayerStats
from config.settings import get_settings

logger = logging.getLogger(__name__)


class TeamFeatureCalculator:
    """Calculates team performance features from historical game data."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize team feature calculator.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.settings = get_settings()
        self.default_games_back = self.settings.DEFAULT_GAMES_BACK
        
        logger.info("TeamFeatureCalculator initialized")
    
    def calculate_offensive_rating(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate offensive rating (points per 100 possessions).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Offensive rating or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:  # Need at least 3 games for reliable metric
            return None
        
        total_points = 0
        total_possessions = 0
        
        for stat in stats_history:
            total_points += stat.points
            possessions = self._calculate_possessions(stat)
            total_possessions += possessions
        
        if total_possessions == 0:
            return None
        
        offensive_rating = (total_points / total_possessions) * 100
        return round(offensive_rating, 2)
    
    def calculate_defensive_rating(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate defensive rating (points allowed per 100 possessions).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Defensive rating or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        # Get opponent stats for each game
        total_points_allowed = 0
        total_opponent_possessions = 0
        
        for stat in stats_history:
            game = self.db_manager.get_game(stat.game_id)
            if not game:
                continue
            
            # Get opponent team ID
            opponent_id = game.away_team_id if stat.is_home else game.home_team_id
            
            # Get opponent stats
            opponent_stats = self.db_manager.get_team_stats(stat.game_id, opponent_id)
            if opponent_stats:
                total_points_allowed += opponent_stats.points
                opponent_possessions = self._calculate_possessions(opponent_stats)
                total_opponent_possessions += opponent_possessions
        
        if total_opponent_possessions == 0:
            return None
        
        defensive_rating = (total_points_allowed / total_opponent_possessions) * 100
        return round(defensive_rating, 2)
    
    def calculate_net_rating(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate net rating (offensive rating - defensive rating).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Net rating or None if insufficient data
        """
        off_rating = self.calculate_offensive_rating(team_id, games_back, end_date)
        def_rating = self.calculate_defensive_rating(team_id, games_back, end_date)
        
        if off_rating is None or def_rating is None:
            return None
        
        return round(off_rating - def_rating, 2)
    
    def calculate_pace(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate pace (possessions per game).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Average pace or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_possessions = 0
        
        for stat in stats_history:
            possessions = self._calculate_possessions(stat)
            total_possessions += possessions
        
        avg_pace = total_possessions / len(stats_history)
        return round(avg_pace, 2)
    
    def calculate_true_shooting(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate true shooting percentage.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            True shooting percentage or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_points = 0
        total_tsa = 0  # True shooting attempts
        
        for stat in stats_history:
            total_points += stat.points
            tsa = stat.field_goals_attempted + (0.44 * stat.free_throws_attempted)
            total_tsa += tsa
        
        if total_tsa == 0:
            return None
        
        ts_pct = (total_points / (2 * total_tsa)) * 100
        return round(ts_pct, 2)
    
    def calculate_effective_fg_percentage(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate effective field goal percentage.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Effective FG% or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_fgm = 0
        total_3pm = 0
        total_fga = 0
        
        for stat in stats_history:
            total_fgm += stat.field_goals_made
            total_3pm += stat.three_pointers_made
            total_fga += stat.field_goals_attempted
        
        if total_fga == 0:
            return None
        
        efg_pct = ((total_fgm + 0.5 * total_3pm) / total_fga) * 100
        return round(efg_pct, 2)
    
    def calculate_rebound_rate(
        self,
        team_id: str,
        games_back: int = None,
        offensive: bool = True,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate rebound rate (percentage of available rebounds).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            offensive: If True, calculate offensive rebound rate; else defensive
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Rebound rate percentage or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_team_rebounds = 0
        total_opponent_rebounds = 0
        
        for stat in stats_history:
            game = self.db_manager.get_game(stat.game_id)
            if not game:
                continue
            
            opponent_id = game.away_team_id if stat.is_home else game.home_team_id
            opponent_stats = self.db_manager.get_team_stats(stat.game_id, opponent_id)
            
            if offensive:
                total_team_rebounds += stat.rebounds_offensive
                if opponent_stats:
                    total_opponent_rebounds += opponent_stats.rebounds_defensive
            else:
                total_team_rebounds += stat.rebounds_defensive
                if opponent_stats:
                    total_opponent_rebounds += opponent_stats.rebounds_offensive
        
        total_available = total_team_rebounds + total_opponent_rebounds
        if total_available == 0:
            return None
        
        rebound_rate = (total_team_rebounds / total_available) * 100
        return round(rebound_rate, 2)
    
    def calculate_turnover_rate(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate turnover rate (turnovers per 100 possessions).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Turnover rate or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_turnovers = 0
        total_possessions = 0
        
        for stat in stats_history:
            total_turnovers += stat.turnovers
            possessions = self._calculate_possessions(stat)
            total_possessions += possessions
        
        if total_possessions == 0:
            return None
        
        turnover_rate = (total_turnovers / total_possessions) * 100
        return round(turnover_rate, 2)
    
    def calculate_win_percentage(
        self,
        team_id: str,
        games_back: int = None,
        home_only: bool = False,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate win percentage.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            home_only: If True, only count home games
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Win percentage (0-100) or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        games = self.db_manager.get_games(
            team_id=team_id,
            end_date=end_date,
            limit=games_back * 2  # Get more to filter
        )
        
        # Filter by home_only if needed
        if home_only:
            games = [g for g in games if g.home_team_id == team_id]
        
        # Sort by date descending and take games_back
        games = sorted(games, key=lambda x: x.game_date, reverse=True)[:games_back]
        
        if len(games) < 3:
            return None
        
        wins = 0
        for game in games:
            if game.winner == team_id:
                wins += 1
        
        win_pct = (wins / len(games)) * 100
        return round(win_pct, 2)
    
    def calculate_avg_point_differential(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate average point differential.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Average point differential or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        games = self.db_manager.get_games(
            team_id=team_id,
            end_date=end_date,
            limit=games_back * 2
        )
        
        games = sorted(games, key=lambda x: x.game_date, reverse=True)[:games_back]
        
        if len(games) < 3:
            return None
        
        total_differential = 0
        for game in games:
            if game.home_team_id == team_id:
                diff = game.home_score - game.away_score if game.home_score and game.away_score else 0
            else:
                diff = game.away_score - game.home_score if game.home_score and game.away_score else 0
            total_differential += diff
        
        avg_differential = total_differential / len(games)
        return round(avg_differential, 2)
    
    def calculate_avg_points_for(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate average points scored.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Average points scored or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_points = sum(stat.points for stat in stats_history)
        avg_points = total_points / len(stats_history)
        return round(avg_points, 2)
    
    def calculate_avg_points_against(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate average points allowed.
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Average points allowed or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_points_allowed = 0
        for stat in stats_history:
            game = self.db_manager.get_game(stat.game_id)
            if not game:
                continue
            
            opponent_id = game.away_team_id if stat.is_home else game.home_team_id
            opponent_stats = self.db_manager.get_team_stats(stat.game_id, opponent_id)
            if opponent_stats:
                total_points_allowed += opponent_stats.points
        
        if len(stats_history) == 0:
            return None
        
        avg_points_allowed = total_points_allowed / len(stats_history)
        return round(avg_points_allowed, 2)
    
    def calculate_current_streak(
        self,
        team_id: str,
        end_date: Optional[date] = None
    ) -> Dict[str, int]:
        """
        Calculate current win/loss streak.
        
        Args:
            team_id: Team identifier
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Dictionary with 'win_streak' and 'loss_streak' (one will be 0)
        """
        games = self.db_manager.get_games(
            team_id=team_id,
            end_date=end_date,
            limit=20  # Check last 20 games
        )
        
        games = sorted(games, key=lambda x: x.game_date, reverse=True)
        
        if not games:
            return {'win_streak': 0, 'loss_streak': 0}
        
        streak_type = None
        streak_count = 0
        
        for game in games:
            if not game.winner:
                break
            
            if game.winner == team_id:
                if streak_type == 'win' or streak_type is None:
                    streak_type = 'win'
                    streak_count += 1
                else:
                    break
            else:
                if streak_type == 'loss' or streak_type is None:
                    streak_type = 'loss'
                    streak_count += 1
                else:
                    break
        
        if streak_type == 'win':
            return {'win_streak': streak_count, 'loss_streak': 0}
        else:
            return {'win_streak': 0, 'loss_streak': streak_count}
    
    def calculate_injury_impact(
        self,
        team_id: str,
        end_date: Optional[date] = None,
        use_weighted_importance: bool = True
    ) -> Dict[str, Optional[float]]:
        """
        Calculate team injury impact with optional player importance weighting.
        
        Enhanced version that weights injuries by player importance scores.
        A star player being out has more impact than a bench player.
        
        Args:
            team_id: Team identifier
            end_date: Cutoff date (to avoid data leakage)
            use_weighted_importance: If True, weight injuries by player importance
            
        Returns:
            Dictionary with injury metrics:
            - players_out: Count of players marked as 'out'
            - players_questionable: Count of players marked as 'questionable'
            - injury_severity_score: Traditional count-based severity (0-1)
            - weighted_injury_score: Sum of (importance * severity) for injured players
            - weighted_severity_score: Normalized weighted severity (0-1)
            - key_player_out: Boolean if a top-5 player is out
            - key_players_out_count: Count of top-5 players who are out
            - total_importance_out: Sum of importance scores for players out
        """
        # Get most recent game before end_date
        games = self.db_manager.get_games(
            team_id=team_id,
            end_date=end_date,
            limit=1
        )
        
        empty_result = {
            'players_out': None,
            'players_questionable': None,
            'injury_severity_score': None,
            'weighted_injury_score': None,
            'weighted_severity_score': None,
            'key_player_out': None,
            'key_players_out_count': None,
            'total_importance_out': None
        }
        
        if not games:
            return empty_result
        
        most_recent_game = games[0]
        
        # Get player stats for this game
        with self.db_manager.get_session() as session:
            player_stats = session.query(PlayerStats).filter_by(
                game_id=most_recent_game.game_id,
                team_id=team_id
            ).all()
        
        if not player_stats:
            return empty_result
        
        # Get injury severity weights from settings
        settings = get_settings()
        weight_out = settings.INJURY_WEIGHT_OUT
        weight_questionable = settings.INJURY_WEIGHT_QUESTIONABLE
        weight_probable = settings.INJURY_WEIGHT_PROBABLE
        top_players_count = settings.TOP_PLAYERS_COUNT
        
        # Count injuries (basic)
        players_out = 0
        players_questionable = 0
        total_players = len(player_stats)
        
        # Weighted injury tracking
        weighted_injury_score = 0.0
        total_importance_out = 0.0
        key_player_out = False
        key_players_out_count = 0
        
        # Get player importance calculator if using weighted importance
        importance_calc = None
        top_player_ids = set()
        if use_weighted_importance:
            from src.features.player_importance import PlayerImportanceCalculator
            importance_calc = PlayerImportanceCalculator(self.db_manager)
            
            # Get top players for this team
            top_players = importance_calc.get_top_players(
                team_id, 
                top_n=top_players_count,
                end_date=end_date
            )
            top_player_ids = {p['player_id'] for p in top_players}
        
        # Process each player
        for player in player_stats:
            injury_status = player.injury_status or 'healthy'
            
            # Get severity weight for this status
            if injury_status == 'out':
                severity_weight = weight_out
                players_out += 1
            elif injury_status == 'questionable':
                severity_weight = weight_questionable
                players_questionable += 1
            elif injury_status == 'probable':
                severity_weight = weight_probable
            else:
                severity_weight = 0.0
            
            # Skip healthy players for weighted calculations
            if severity_weight == 0.0:
                continue
            
            # Get player importance
            player_importance = 1.0  # Default: equal weight
            if importance_calc:
                importance = importance_calc.get_importance_score(
                    player.player_id,
                    team_id,
                    games_back=20,
                    end_date=end_date
                )
                if importance is not None:
                    player_importance = importance
            
            # Accumulate weighted injury score
            weighted_injury_score += player_importance * severity_weight
            
            # Track key player injuries
            if injury_status == 'out':
                total_importance_out += player_importance
                if player.player_id in top_player_ids:
                    key_player_out = True
                    key_players_out_count += 1
        
        # Calculate severity scores
        if total_players == 0:
            severity = None
            weighted_severity = None
        else:
            # Traditional severity (count-based)
            severity = (players_out * weight_out + players_questionable * weight_questionable) / total_players
            
            # Weighted severity (importance-based)
            # Normalize by team's total importance
            if importance_calc:
                team_total_importance = importance_calc.get_team_total_importance(
                    team_id, end_date=end_date
                )
                if team_total_importance > 0:
                    weighted_severity = min(1.0, weighted_injury_score / team_total_importance)
                else:
                    weighted_severity = weighted_injury_score
            else:
                weighted_severity = weighted_injury_score
        
        return {
            'players_out': players_out,
            'players_questionable': players_questionable,
            'injury_severity_score': round(severity, 4) if severity is not None else None,
            'weighted_injury_score': round(weighted_injury_score, 4) if weighted_injury_score > 0 else 0.0,
            'weighted_severity_score': round(weighted_severity, 4) if weighted_severity is not None else None,
            'key_player_out': key_player_out,
            'key_players_out_count': key_players_out_count,
            'total_importance_out': round(total_importance_out, 4) if total_importance_out > 0 else 0.0
        }
    
    def calculate_historical_injury_impact(
        self,
        team_id: str,
        end_date: Optional[date] = None,
        games_back: int = 50
    ) -> Dict[str, Optional[float]]:
        """
        Analyze how team performs when key players are injured vs healthy.
        
        This provides historical context about how injuries affect team performance.
        Compares win rate and point differential when key players are out vs playing.
        
        Args:
            team_id: Team identifier
            end_date: Cutoff date (to avoid data leakage)
            games_back: Number of recent games to analyze
            
        Returns:
            Dictionary with historical injury impact:
            - avg_win_pct_with_key_players: Win % when all key players healthy
            - avg_win_pct_without_key_players: Win % when key player(s) out
            - win_pct_delta: Difference (negative = worse without key players)
            - avg_point_diff_with: Avg point diff when healthy
            - avg_point_diff_without: Avg point diff when injured
            - point_diff_delta: Difference in point differential
            - games_with_key_players: Count of games with healthy key players
            - games_without_key_players: Count of games with injured key players
        """
        from src.features.player_importance import PlayerImportanceCalculator
        
        empty_result = {
            'avg_win_pct_with_key_players': None,
            'avg_win_pct_without_key_players': None,
            'win_pct_delta': None,
            'avg_point_diff_with': None,
            'avg_point_diff_without': None,
            'point_diff_delta': None,
            'games_with_key_players': 0,
            'games_without_key_players': 0
        }
        
        # Get recent games
        games = self.db_manager.get_games(
            team_id=team_id,
            end_date=end_date,
            limit=games_back
        )
        
        if not games or len(games) < 10:
            return empty_result
        
        # Get player importance calculator
        importance_calc = PlayerImportanceCalculator(self.db_manager)
        
        # Get top players (key players)
        settings = get_settings()
        top_players_count = settings.TOP_PLAYERS_COUNT
        top_players = importance_calc.get_top_players(
            team_id,
            top_n=top_players_count,
            end_date=end_date
        )
        
        if not top_players:
            return empty_result
        
        top_player_ids = {p['player_id'] for p in top_players}
        
        # Analyze each game
        games_with_key_players = []
        games_without_key_players = []
        
        for game in games:
            # Get player stats for this game
            with self.db_manager.get_session() as session:
                player_stats = session.query(PlayerStats).filter_by(
                    game_id=game.game_id,
                    team_id=team_id
                ).all()
            
            # Check if any key player was out
            key_player_out = False
            for player in player_stats:
                if player.player_id in top_player_ids:
                    if player.injury_status == 'out':
                        key_player_out = True
                        break
                    # Also check minutes - if key player played <5 min, consider them out
                    if player.minutes_played:
                        try:
                            if ':' in str(player.minutes_played):
                                mins = int(str(player.minutes_played).split(':')[0])
                                if mins < 5:
                                    key_player_out = True
                                    break
                        except (ValueError, IndexError):
                            pass
            
            # Determine if team won and by how much
            if game.winner is None:
                continue  # Skip unfinished games
            
            team_won = (game.winner == team_id)
            
            # Calculate point differential from team's perspective
            if game.home_team_id == team_id:
                point_diff = (game.home_score or 0) - (game.away_score or 0)
            else:
                point_diff = (game.away_score or 0) - (game.home_score or 0)
            
            game_result = {
                'won': team_won,
                'point_diff': point_diff
            }
            
            if key_player_out:
                games_without_key_players.append(game_result)
            else:
                games_with_key_players.append(game_result)
        
        # Calculate metrics
        result = empty_result.copy()
        result['games_with_key_players'] = len(games_with_key_players)
        result['games_without_key_players'] = len(games_without_key_players)
        
        if games_with_key_players:
            wins_with = sum(1 for g in games_with_key_players if g['won'])
            result['avg_win_pct_with_key_players'] = round(
                wins_with / len(games_with_key_players), 4
            )
            result['avg_point_diff_with'] = round(
                sum(g['point_diff'] for g in games_with_key_players) / len(games_with_key_players), 
                2
            )
        
        if games_without_key_players:
            wins_without = sum(1 for g in games_without_key_players if g['won'])
            result['avg_win_pct_without_key_players'] = round(
                wins_without / len(games_without_key_players), 4
            )
            result['avg_point_diff_without'] = round(
                sum(g['point_diff'] for g in games_without_key_players) / len(games_without_key_players), 
                2
            )
        
        # Calculate deltas
        if result['avg_win_pct_with_key_players'] is not None and \
           result['avg_win_pct_without_key_players'] is not None:
            result['win_pct_delta'] = round(
                result['avg_win_pct_without_key_players'] - result['avg_win_pct_with_key_players'],
                4
            )
        
        if result['avg_point_diff_with'] is not None and \
           result['avg_point_diff_without'] is not None:
            result['point_diff_delta'] = round(
                result['avg_point_diff_without'] - result['avg_point_diff_with'],
                2
            )
        
        return result
    
    def calculate_assist_rate(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate assist rate (assists per 100 possessions).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Assist rate or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_assists = 0
        total_possessions = 0
        
        for stat in stats_history:
            total_assists += stat.assists
            possessions = self._calculate_possessions(stat)
            total_possessions += possessions
        
        if total_possessions == 0:
            return None
        
        assist_rate = (total_assists / total_possessions) * 100
        return round(assist_rate, 2)
    
    def calculate_steal_rate(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate steal rate (steals per 100 possessions).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Steal rate or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_steals = 0
        total_possessions = 0
        
        for stat in stats_history:
            total_steals += stat.steals
            possessions = self._calculate_possessions(stat)
            total_possessions += possessions
        
        if total_possessions == 0:
            return None
        
        steal_rate = (total_steals / total_possessions) * 100
        return round(steal_rate, 2)
    
    def calculate_block_rate(
        self,
        team_id: str,
        games_back: int = None,
        end_date: Optional[date] = None
    ) -> Optional[float]:
        """
        Calculate block rate (blocks per 100 possessions).
        
        Args:
            team_id: Team identifier
            games_back: Number of recent games to consider
            end_date: Cutoff date (to avoid data leakage)
            
        Returns:
            Block rate or None if insufficient data
        """
        if games_back is None:
            games_back = self.default_games_back
        
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        if len(stats_history) < 3:
            return None
        
        total_blocks = 0
        total_possessions = 0
        
        for stat in stats_history:
            total_blocks += stat.blocks
            possessions = self._calculate_possessions(stat)
            total_possessions += possessions
        
        if total_possessions == 0:
            return None
        
        block_rate = (total_blocks / total_possessions) * 100
        return round(block_rate, 2)
    
    def calculate_rolling_stats(
        self,
        team_id: str,
        games_back: int,
        end_date: Optional[date] = None,
        use_exponential_decay: bool = True
    ) -> Dict[str, Optional[float]]:
        """
        Calculate rolling statistics for last N games with exponential decay weighting.
        
        Uses exponential decay to weight recent games more heavily than older games.
        This better captures recent form while still incorporating historical context.
        
        Weight formula: w_i = e^(-λ * games_ago_i)
        Where:
          - games_ago_i = 0 for most recent game, 1 for previous, etc.
          - λ = decay rate from settings (default 0.1)
        
        Weighted average: Σ(stat_i * w_i) / Σ(w_i)
        
        For percentages (FG%, 3P%, FT%), we use weighted totals:
          - total_made_weighted = Σ(made_i * w_i)
          - total_attempted_weighted = Σ(attempted_i * w_i)
          - percentage = total_made_weighted / total_attempted_weighted
        This ensures games with more attempts get proportionally more weight.
        
        Args:
            team_id: Team identifier
            games_back: Number of games to look back (5, 10, or 20)
            end_date: Cutoff date (to avoid data leakage)
            use_exponential_decay: If True, use exponential decay; else use simple average
            
        Returns:
            Dictionary with rolling stats: points, points_allowed, fg_pct, three_pct,
            ft_pct, rebounds, assists, turnovers, steals, blocks, win_pct
        """
        # Get decay rate (0.0 = no decay = simple average)
        decay_rate = self.settings.ROLLING_STATS_DECAY_RATE if use_exponential_decay else 0.0
        
        def calculate_weighted_average(values: List, weights: List[float]) -> Optional[float]:
            """
            Calculate weighted average, handling None/NaN values.
            
            Args:
                values: List of statistic values
                weights: List of corresponding weights
                
            Returns:
                Weighted average or None if no valid values
            """
            if not values or all(v is None for v in values):
                return None
            
            # Filter out None values
            valid_pairs = [(v, w) for v, w in zip(values, weights) if v is not None]
            if not valid_pairs:
                return None
            
            weighted_sum = sum(float(v) * w for v, w in valid_pairs)
            weight_sum = sum(w for _, w in valid_pairs)
            
            if weight_sum == 0:
                return None
            
            return weighted_sum / weight_sum
        
        # Try TeamStats first (more detailed)
        stats_history = self.db_manager.get_team_stats_history(
            team_id, games_back, end_date
        )
        
        # If no TeamStats, fall back to Game records
        if len(stats_history) < 1:
            # Use Game records to calculate basic stats
            games = self.db_manager.get_games(
                team_id=team_id,
                end_date=end_date,
                limit=games_back
            )
            
            # Filter for finished games only
            finished_games = [
                g for g in games 
                if g.game_status == 'finished' 
                and g.home_score is not None 
                and g.away_score is not None
            ]
            finished_games = sorted(
                finished_games, 
                key=lambda x: x.game_date, 
                reverse=True
            )[:games_back]
            
            if len(finished_games) < 1:
                return {
                    'points': None, 'points_allowed': None, 'fg_pct': None,
                    'three_pct': None, 'ft_pct': None, 'rebounds': None,
                    'assists': None, 'turnovers': None, 'steals': None,
                    'blocks': None, 'win_pct': None
                }
            
            # Calculate weights using exponential decay
            # Most recent game (index 0) has games_ago=0, next has games_ago=1, etc.
            num_games = len(finished_games)
            weights = [np.exp(-decay_rate * i) for i in range(num_games)]
            
            # Extract stats
            points_list = []
            points_allowed_list = []
            wins_list = []
            
            for game in finished_games:
                if game.home_team_id == team_id:
                    points = game.home_score or 0
                    points_allowed = game.away_score or 0
                else:
                    points = game.away_score or 0
                    points_allowed = game.home_score or 0
                
                points_list.append(float(points))
                points_allowed_list.append(float(points_allowed))
                wins_list.append(1.0 if game.winner == team_id else 0.0)
            
            # Calculate weighted averages
            avg_points = calculate_weighted_average(points_list, weights)
            avg_points_allowed = calculate_weighted_average(points_allowed_list, weights)
            win_pct = calculate_weighted_average(wins_list, weights)
            
            return {
                'points': round(avg_points, 2) if avg_points is not None else None,
                'points_allowed': round(avg_points_allowed, 2) if avg_points_allowed is not None else None,
                'fg_pct': None,  # Not available from Game records
                'three_pct': None,
                'ft_pct': None,
                'rebounds': None,
                'assists': None,
                'turnovers': None,
                'steals': None,
                'blocks': None,
                'win_pct': round(win_pct, 4) if win_pct is not None else None
            }
        
        # Get games to determine wins and points_allowed
        with self.db_manager.get_session() as session:
            from src.database.models import Game
            game_ids = [s.game_id for s in stats_history]
            games = session.query(Game).filter(Game.game_id.in_(game_ids)).all()
            game_dict = {g.game_id: g for g in games}
        
        # CRITICAL: Sort stats_history by date (most recent first)
        # This ensures weights are applied correctly (index 0 = most recent)
        def get_game_date(stat):
            game = game_dict.get(stat.game_id)
            return game.game_date if game else date.min
        
        stats_history = sorted(stats_history, key=get_game_date, reverse=True)
        
        # Calculate weights using exponential decay
        num_games = len(stats_history)
        weights = [np.exp(-decay_rate * i) for i in range(num_games)]
        
        # Extract stats for weighted calculation
        points_list = [s.points for s in stats_history]
        points_allowed_list = []
        wins_list = []
        fg_made_list = [s.field_goals_made for s in stats_history]
        fg_attempted_list = [s.field_goals_attempted for s in stats_history]
        three_made_list = [s.three_pointers_made for s in stats_history]
        three_attempted_list = [s.three_pointers_attempted for s in stats_history]
        ft_made_list = [s.free_throws_made for s in stats_history]
        ft_attempted_list = [s.free_throws_attempted for s in stats_history]
        rebounds_list = [s.rebounds_total for s in stats_history]
        assists_list = [s.assists for s in stats_history]
        turnovers_list = [s.turnovers for s in stats_history]
        steals_list = [s.steals for s in stats_history]
        blocks_list = [s.blocks for s in stats_history]
        
        # Calculate points_allowed and wins from game records
        for s in stats_history:
            game = game_dict.get(s.game_id)
            if game:
                # Calculate points allowed (opponent's points)
                if s.is_home:
                    points_allowed = game.away_score if game.away_score is not None else 0
                else:
                    points_allowed = game.home_score if game.home_score is not None else 0
                points_allowed_list.append(float(points_allowed))
                
                # Determine if team won
                wins_list.append(1.0 if game.winner == team_id else 0.0)
            else:
                points_allowed_list.append(0.0)
                wins_list.append(0.0)
        
        # Calculate weighted averages for simple stats
        avg_points = calculate_weighted_average(points_list, weights)
        avg_points_allowed = calculate_weighted_average(points_allowed_list, weights)
        avg_rebounds = calculate_weighted_average(rebounds_list, weights)
        avg_assists = calculate_weighted_average(assists_list, weights)
        avg_turnovers = calculate_weighted_average(turnovers_list, weights)
        avg_steals = calculate_weighted_average(steals_list, weights)
        avg_blocks = calculate_weighted_average(blocks_list, weights)
        win_pct = calculate_weighted_average(wins_list, weights)
        
        # For percentages, calculate weighted totals first, then divide
        # This ensures proper weighting (games with more attempts get more weight)
        total_fg_made_weighted = sum(
            fg_made_list[i] * weights[i] 
            for i in range(num_games) 
            if fg_made_list[i] is not None
        )
        total_fg_attempted_weighted = sum(
            fg_attempted_list[i] * weights[i] 
            for i in range(num_games) 
            if fg_attempted_list[i] is not None and fg_attempted_list[i] > 0
        )
        
        total_three_made_weighted = sum(
            three_made_list[i] * weights[i] 
            for i in range(num_games) 
            if three_made_list[i] is not None
        )
        total_three_attempted_weighted = sum(
            three_attempted_list[i] * weights[i] 
            for i in range(num_games) 
            if three_attempted_list[i] is not None and three_attempted_list[i] > 0
        )
        
        total_ft_made_weighted = sum(
            ft_made_list[i] * weights[i] 
            for i in range(num_games) 
            if ft_made_list[i] is not None
        )
        total_ft_attempted_weighted = sum(
            ft_attempted_list[i] * weights[i] 
            for i in range(num_games) 
            if ft_attempted_list[i] is not None and ft_attempted_list[i] > 0
        )
        
        # Calculate percentages from weighted totals
        fg_pct = (
            total_fg_made_weighted / total_fg_attempted_weighted 
            if total_fg_attempted_weighted > 0 else None
        )
        three_pct = (
            total_three_made_weighted / total_three_attempted_weighted 
            if total_three_attempted_weighted > 0 else None
        )
        ft_pct = (
            total_ft_made_weighted / total_ft_attempted_weighted 
            if total_ft_attempted_weighted > 0 else None
        )
        
        return {
            'points': round(avg_points, 2) if avg_points is not None else None,
            'points_allowed': round(avg_points_allowed, 2) if avg_points_allowed is not None else None,
            'fg_pct': round(fg_pct, 4) if fg_pct is not None else None,
            'three_pct': round(three_pct, 4) if three_pct is not None else None,
            'ft_pct': round(ft_pct, 4) if ft_pct is not None else None,
            'rebounds': round(avg_rebounds, 2) if avg_rebounds is not None else None,
            'assists': round(avg_assists, 2) if avg_assists is not None else None,
            'turnovers': round(avg_turnovers, 2) if avg_turnovers is not None else None,
            'steals': round(avg_steals, 2) if avg_steals is not None else None,
            'blocks': round(avg_blocks, 2) if avg_blocks is not None else None,
            'win_pct': round(win_pct, 4) if win_pct is not None else None
        }
    
    def _calculate_possessions(self, stat: TeamStats) -> float:
        """
        Calculate possessions for a team in a game.
        
        Formula: FGA - ORB + TOV + (0.44 * FTA)
        
        Args:
            stat: TeamStats object
            
        Returns:
            Number of possessions
        """
        possessions = (
            stat.field_goals_attempted -
            stat.rebounds_offensive +
            stat.turnovers +
            (0.44 * stat.free_throws_attempted)
        )
        return max(possessions, 0)  # Ensure non-negative

