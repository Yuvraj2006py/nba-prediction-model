#!/usr/bin/env python
"""
Feature Transformation Script - Creates model-ready rolling features.

This script calculates rolling averages and advanced metrics for each team
for each game, ensuring no data leakage (only uses past game data).

Usage:
    python scripts/transform_features.py --season 2025-26
    python scripts/transform_features.py --season 2025-26 --full-refresh
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
import argparse
from datetime import date, datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

import pandas as pd
import numpy as np
from tqdm import tqdm
from sqlalchemy import and_

from src.database.db_manager import DatabaseManager
from src.database.models import Game, Team, TeamStats, TeamRollingFeatures, GameMatchupFeatures, PlayerStats
from src.features.team_features import TeamFeatureCalculator
from src.features.matchup_features import MatchupFeatureCalculator
from src.features.contextual_features import ContextualFeatureCalculator


class FeatureTransformer:
    """
    Transforms raw game data into model-ready rolling features.
    
    Key principles:
    - No data leakage: Only uses data from BEFORE the current game
    - Rolling windows: Last 5, 10, 20 games
    - Pace-adjusted metrics
    - Contextual features (rest days, back-to-back, etc.)
    """
    
    def __init__(self, season: str = '2025-26', db_manager: Optional[DatabaseManager] = None):
        self.season = season
        self.db_manager = db_manager or DatabaseManager()
        
        # Initialize feature calculators
        self.team_calc = TeamFeatureCalculator(db_manager)
        self.matchup_calc = MatchupFeatureCalculator(db_manager)
        self.contextual_calc = ContextualFeatureCalculator(db_manager)
        
        # Stats tracking
        self.stats = {
            'games_processed': 0,
            'features_created': 0,
            'features_updated': 0,
            'matchup_features_created': 0,
            'matchup_features_updated': 0,
            'errors': 0
        }
    
    def run(self, full_refresh: bool = False) -> Dict[str, Any]:
        """
        Run feature transformation for the season.
        
        Args:
            full_refresh: If True, recalculate all features
            
        Returns:
            Statistics dictionary
        """
        print("\n" + "=" * 70)
        print(f"FEATURE TRANSFORMATION - Season {self.season}")
        print("=" * 70)
        
        try:
            # Load all game data for the season
            games_df, team_stats_df = self._load_game_data()
            
            if games_df.empty:
                print("No games found for this season")
                return self.stats
            
            # Calculate rolling features for each team-game
            self._calculate_rolling_features(games_df, team_stats_df, full_refresh)
            
            # Calculate matchup features for each game
            self._calculate_matchup_features(games_df, full_refresh)
            
        except Exception as e:
            logger.error(f"Transform error: {e}")
            import traceback
            traceback.print_exc()
            self.stats['errors'] += 1
        
        self._print_summary()
        return self.stats
    
    def _load_game_data(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load all game and team stats data for the season."""
        print("\n[STEP 1] Loading game data...")
        
        with self.db_manager.get_session() as session:
            # Load games (both finished and upcoming)
            games = session.query(Game).filter(
                Game.season == self.season
            ).order_by(Game.game_date).all()
            
            games_data = [{
                'game_id': g.game_id,
                'game_date': g.game_date,
                'home_team_id': g.home_team_id,
                'away_team_id': g.away_team_id,
                'home_score': g.home_score,
                'away_score': g.away_score,
                'winner': g.winner,
                'point_differential': g.point_differential,
                'game_status': g.game_status
            } for g in games]
            
            games_df = pd.DataFrame(games_data)
            
            # Load team stats (only for finished games)
            team_stats = session.query(TeamStats).join(Game).filter(
                Game.season == self.season
            ).all()
            
            stats_data = [{
                'game_id': ts.game_id,
                'team_id': ts.team_id,
                'is_home': ts.is_home,
                'points': ts.points,
                'field_goal_percentage': ts.field_goal_percentage,
                'three_point_percentage': ts.three_point_percentage,
                'free_throw_percentage': ts.free_throw_percentage,
                'rebounds_total': ts.rebounds_total,
                'assists': ts.assists,
                'turnovers': ts.turnovers,
                'steals': ts.steals,
                'blocks': ts.blocks,
                'field_goals_made': ts.field_goals_made,
                'field_goals_attempted': ts.field_goals_attempted,
                'three_pointers_made': ts.three_pointers_made,
                'three_pointers_attempted': ts.three_pointers_attempted,
                'free_throws_made': ts.free_throws_made,
                'free_throws_attempted': ts.free_throws_attempted,
                'true_shooting_percentage': ts.true_shooting_percentage,
                'effective_field_goal_percentage': ts.effective_field_goal_percentage
            } for ts in team_stats]
            
            team_stats_df = pd.DataFrame(stats_data)
        
        finished_count = len(games_df[games_df['game_status'] == 'finished'])
        upcoming_count = len(games_df[games_df['game_status'] != 'finished'])
        print(f"  [OK] Loaded {finished_count} finished games, {upcoming_count} upcoming games")
        print(f"  [OK] Loaded {len(team_stats_df)} team stats records")
        
        return games_df, team_stats_df
    
    def _calculate_rolling_features(self, games_df: pd.DataFrame, team_stats_df: pd.DataFrame, full_refresh: bool):
        """Calculate rolling features for each team-game combination."""
        print("\n[STEP 2] Calculating rolling features...")
        
        if games_df.empty:
            print("  No games to process")
            return
        
        # Get all unique teams from games (not just from stats)
        all_teams = set(games_df['home_team_id'].unique()) | set(games_df['away_team_id'].unique())
        print(f"  Processing {len(all_teams)} teams...")
        
        # Merge games with team stats
        games_df['game_date'] = pd.to_datetime(games_df['game_date'])
        
        # Get existing features to skip if not full refresh
        existing_features = set()
        if not full_refresh:
            with self.db_manager.get_session() as session:
                existing = session.query(TeamRollingFeatures.game_id, TeamRollingFeatures.team_id).filter(
                    TeamRollingFeatures.season == self.season
                ).all()
                existing_features = {(e.game_id, e.team_id) for e in existing}
                print(f"  Found {len(existing_features)} existing feature records")
        
        all_features = []
        
        for team_id in tqdm(all_teams, desc="  Computing features"):
            team_features = self._calculate_team_features(
                team_id, games_df, team_stats_df, existing_features
            )
            all_features.extend(team_features)
        
        # Store features in database
        if all_features:
            self._store_features(all_features, full_refresh)
    
    def _calculate_team_features(
        self,
        team_id: str,
        games_df: pd.DataFrame,
        team_stats_df: pd.DataFrame,
        existing_features: set
    ) -> List[Dict[str, Any]]:
        """Calculate rolling features for a single team."""
        features_list = []
        
        # Get all games for this team (as home or away)
        team_games = games_df[
            (games_df['home_team_id'] == team_id) | (games_df['away_team_id'] == team_id)
        ].sort_values('game_date').copy()
        
        if team_games.empty:
            return []
        
        # Get team stats (only for finished games)
        if team_stats_df.empty or 'team_id' not in team_stats_df.columns:
            team_game_stats = pd.DataFrame()
        else:
            team_game_stats = team_stats_df[team_stats_df['team_id'] == team_id].copy()
        
        # Merge with game dates
        if not team_game_stats.empty:
            team_game_stats = team_game_stats.merge(
                games_df[['game_id', 'game_date', 'home_team_id', 'away_team_id', 'home_score', 'away_score', 'winner']],
                on='game_id',
                how='left'
            )
            team_game_stats = team_game_stats.sort_values('game_date')
            
            # Add opponent's score (points allowed)
            team_game_stats['points_allowed'] = team_game_stats.apply(
                lambda row: row['away_score'] if row['is_home'] else row['home_score'],
                axis=1
            )
            
            # Add win indicator
            team_game_stats['won'] = team_game_stats.apply(
                lambda row: 1 if row['winner'] == team_id else 0,
                axis=1
            )
        
        # Process all games (including upcoming ones)
        for idx, game_row in team_games.iterrows():
            game_id = game_row['game_id']
            
            # Skip if already exists
            if (game_id, team_id) in existing_features:
                continue
            
            game_date = game_row['game_date']
            is_home = (game_row['home_team_id'] == team_id)
            is_finished = (game_row['game_status'] == 'finished')
            
            # Get past games BEFORE this game (no leakage!)
            # Use finished games with stats for rolling averages
            if not team_game_stats.empty:
                past_games = team_game_stats[team_game_stats['game_date'] < game_date]
            else:
                # Fallback: Use Game records when TeamStats is not available
                # Filter for finished games before this game date
                past_games_df = team_games[
                    (team_games['game_date'] < game_date) &
                    (team_games['game_status'] == 'finished') &
                    (team_games['home_score'].notna()) &
                    (team_games['away_score'].notna())
                ].copy()
                
                if not past_games_df.empty:
                    # Build a DataFrame similar to team_game_stats structure
                    past_games_list = []
                    for _, row in past_games_df.iterrows():
                        is_home_game = (row['home_team_id'] == team_id)
                        points = row['home_score'] if is_home_game else row['away_score']
                        points_allowed = row['away_score'] if is_home_game else row['home_score']
                        won = 1 if row['winner'] == team_id else 0
                        
                        past_games_list.append({
                            'game_id': row['game_id'],
                            'game_date': row['game_date'],
                            'is_home': is_home_game,
                            'points': points,
                            'points_allowed': points_allowed,
                            'won': won,
                            # For Game records, we don't have detailed stats, so set them to None
                            'field_goal_percentage': None,
                            'three_point_percentage': None,
                            'free_throw_percentage': None,
                            'rebounds_total': None,
                            'assists': None,
                            'turnovers': None,
                            'steals': None,
                            'blocks': None,
                        })
                    
                    past_games = pd.DataFrame(past_games_list)
                else:
                    past_games = pd.DataFrame()
            
            if past_games.empty:
                # First game of season - use minimal features
                logger.debug(f"No past games for {team_id} before {game_date}, using minimal features")
                features = self._create_minimal_features(game_id, team_id, is_home, game_date)
            else:
                logger.debug(f"Computing rolling averages for {team_id} game {game_id} with {len(past_games)} past games")
                features = self._compute_rolling_averages(
                    game_id, team_id, is_home, game_date, past_games
                )
                # Debug: Check if features were calculated
                if features.get('l5_points') is None and len(past_games) > 0:
                    logger.warning(f"l5_points is None for {team_id} game {game_id} despite {len(past_games)} past games")
            
            # Add target variables (only for finished games)
            if is_finished and not team_game_stats.empty:
                game_stats = team_game_stats[team_game_stats['game_id'] == game_id]
                if not game_stats.empty:
                    stats_row = game_stats.iloc[0]
                    features['won_game'] = bool(stats_row['won'])
                    features['point_differential'] = int(stats_row['points'] - stats_row['points_allowed']) if pd.notna(stats_row['points']) else None
                else:
                    features['won_game'] = None
                    features['point_differential'] = None
            else:
                # Upcoming game - no target variables
                features['won_game'] = None
                features['point_differential'] = None
            
            features_list.append(features)
            self.stats['games_processed'] += 1
        
        return features_list
    
    def _create_minimal_features(self, game_id: str, team_id: str, is_home: bool, game_date) -> Dict[str, Any]:
        """Create minimal features for first game of season."""
        return {
            'game_id': game_id,
            'team_id': team_id,
            'is_home': is_home,
            'game_date': game_date.date() if hasattr(game_date, 'date') else game_date,
            'season': self.season,
            # All rolling averages are None for first game
            'l5_points': None, 'l5_points_allowed': None, 'l5_fg_pct': None,
            'l5_three_pct': None, 'l5_ft_pct': None, 'l5_rebounds': None,
            'l5_assists': None, 'l5_turnovers': None, 'l5_steals': None,
            'l5_blocks': None, 'l5_win_pct': None,
            'l10_points': None, 'l10_points_allowed': None, 'l10_fg_pct': None,
            'l10_three_pct': None, 'l10_ft_pct': None, 'l10_rebounds': None,
            'l10_assists': None, 'l10_turnovers': None, 'l10_steals': None,
            'l10_blocks': None, 'l10_win_pct': None,
            'l20_points': None, 'l20_points_allowed': None, 'l20_fg_pct': None,
            'l20_three_pct': None, 'l20_win_pct': None,
            'offensive_rating': None, 'defensive_rating': None, 'net_rating': None,
            'pace': None, 'efg_pct': None, 'ts_pct': None, 'tov_pct': None,
            'offensive_rebound_rate': None, 'defensive_rebound_rate': None,
            'assist_rate': None, 'steal_rate': None, 'block_rate': None,
            'avg_point_differential': None, 'avg_points_for': None, 'avg_points_against': None,
            'win_streak': None, 'loss_streak': None,
            'players_out': None, 'players_questionable': None, 'injury_severity_score': None,
            'days_rest': None, 'is_back_to_back': None, 'games_in_last_7_days': None,
            'home_win_pct': None, 'away_win_pct': None
        }
    
    def _compute_rolling_averages(
        self,
        game_id: str,
        team_id: str,
        is_home: bool,
        game_date,
        past_games: pd.DataFrame
    ) -> Dict[str, Any]:
        """
        Compute rolling averages from past games using exponential decay weighting.
        
        This method is used to pre-compute features for the TeamRollingFeatures table.
        It uses the same exponential decay logic as calculate_rolling_stats() to ensure
        consistency between pre-computed and on-the-fly calculations.
        
        Weight formula: w_i = e^(-λ * games_ago_i)
        Where games_ago_i = 0 for most recent game, 1 for previous, etc.
        """
        # Sort by date descending (most recent first)
        past_games = past_games.sort_values('game_date', ascending=False)
        
        # Get decay rate from settings
        from config.settings import get_settings
        settings = get_settings()
        decay_rate = settings.ROLLING_STATS_DECAY_RATE
        
        def safe_weighted_mean(series: pd.Series, weights: np.ndarray) -> Optional[float]:
            """
            Calculate weighted mean, handling None/NaN values.
            
            Args:
                series: Pandas Series with values
                weights: NumPy array with corresponding weights
                
            Returns:
                Weighted mean or None if no valid values
            """
            if series.empty or series.isna().all():
                return None
            
            # Filter out NaN values
            valid_mask = ~series.isna()
            if not valid_mask.any():
                return None
            
            valid_series = series[valid_mask]
            valid_weights = weights[valid_mask.values]
            
            if valid_weights.sum() == 0:
                return None
            
            return (valid_series * valid_weights).sum() / valid_weights.sum()
        
        def compute_weighted_stats(df: pd.DataFrame, window_name: str) -> Dict[str, Optional[float]]:
            """
            Compute weighted statistics for a rolling window.
            
            Args:
                df: DataFrame with past games (already sorted, most recent first)
                window_name: Prefix for feature names (e.g., 'l5', 'l10', 'l20')
                
            Returns:
                Dictionary with weighted statistics
            """
            if df.empty:
                stats = {
                    f'{window_name}_points': None,
                    f'{window_name}_points_allowed': None,
                    f'{window_name}_fg_pct': None,
                    f'{window_name}_three_pct': None,
                    f'{window_name}_win_pct': None,
                }
                # Add extra stats for l5 and l10
                if window_name in ('l5', 'l10'):
                    stats.update({
                        f'{window_name}_ft_pct': None,
                        f'{window_name}_rebounds': None,
                        f'{window_name}_assists': None,
                        f'{window_name}_turnovers': None,
                        f'{window_name}_steals': None,
                        f'{window_name}_blocks': None,
                    })
                return stats
            
            # Calculate weights: most recent game (index 0) has weight 1.0
            num_games = len(df)
            weights = np.array([np.exp(-decay_rate * i) for i in range(num_games)])
            
            stats = {}
            
            # Simple weighted averages for count-based stats
            stats[f'{window_name}_points'] = safe_weighted_mean(df['points'], weights)
            stats[f'{window_name}_points_allowed'] = safe_weighted_mean(df['points_allowed'], weights)
            stats[f'{window_name}_win_pct'] = (
                safe_weighted_mean(df['won'], weights) * 100 
                if 'won' in df.columns and not df['won'].isna().all() else None
            )
            
            # For percentages, we can use the pre-calculated percentages with weighting
            # Or use weighted totals - using pre-calculated for simplicity here
            stats[f'{window_name}_fg_pct'] = safe_weighted_mean(df['field_goal_percentage'], weights)
            stats[f'{window_name}_three_pct'] = safe_weighted_mean(df['three_point_percentage'], weights)
            
            # Add extra stats for l5 and l10
            if window_name in ('l5', 'l10'):
                stats[f'{window_name}_ft_pct'] = safe_weighted_mean(df['free_throw_percentage'], weights)
                stats[f'{window_name}_rebounds'] = safe_weighted_mean(df['rebounds_total'], weights)
                stats[f'{window_name}_assists'] = safe_weighted_mean(df['assists'], weights)
                stats[f'{window_name}_turnovers'] = safe_weighted_mean(df['turnovers'], weights)
                stats[f'{window_name}_steals'] = safe_weighted_mean(df['steals'], weights)
                stats[f'{window_name}_blocks'] = safe_weighted_mean(df['blocks'], weights)
            
            return stats
        
        # Get last N games
        l5 = past_games.head(5)
        l10 = past_games.head(10)
        l20 = past_games.head(20)
        
        # Compute weighted statistics for each window
        l5_stats = compute_weighted_stats(l5, 'l5')
        l10_stats = compute_weighted_stats(l10, 'l10')
        l20_stats = compute_weighted_stats(l20, 'l20')
        
        # Calculate weights for advanced metrics (using l10)
        l10_weights = np.array([np.exp(-decay_rate * i) for i in range(len(l10))]) if not l10.empty else np.array([])
        
        # Build features dictionary
        features = {
            'game_id': game_id,
            'team_id': team_id,
            'is_home': is_home,
            'game_date': game_date.date() if hasattr(game_date, 'date') else game_date,
            'season': self.season,
            
            # Last 5 games (weighted)
            **l5_stats,
            
            # Last 10 games (weighted)
            **l10_stats,
            
            # Last 20 games (weighted)
            **l20_stats,
            
            # Advanced metrics (calculated using TeamFeatureCalculator)
            'offensive_rating': None,
            'defensive_rating': None,
            'net_rating': None,
            'pace': None,
            'efg_pct': safe_weighted_mean(l10['effective_field_goal_percentage'], l10_weights) if not l10.empty and 'effective_field_goal_percentage' in l10.columns else None,
            'ts_pct': safe_weighted_mean(l10['true_shooting_percentage'], l10_weights) if not l10.empty and 'true_shooting_percentage' in l10.columns else None,
            'tov_pct': None,
            'offensive_rebound_rate': None,
            'defensive_rebound_rate': None,
            'assist_rate': None,
            'steal_rate': None,
            'block_rate': None,
            'avg_point_differential': safe_weighted_mean(l10['points'] - l10['points_allowed'], l10_weights) if not l10.empty else None,
            'avg_points_for': safe_weighted_mean(l10['points'], l10_weights) if not l10.empty else None,
            'avg_points_against': safe_weighted_mean(l10['points_allowed'], l10_weights) if not l10.empty else None,
            'win_streak': None,
            'loss_streak': None,
            'players_out': None,
            'players_questionable': None,
            'injury_severity_score': None,
        }
        
        # Calculate advanced metrics using TeamFeatureCalculator
        try:
            game_date_obj = game_date.date() if hasattr(game_date, 'date') else game_date
            features['offensive_rating'] = self.team_calc.calculate_offensive_rating(team_id, 10, game_date_obj)
            features['defensive_rating'] = self.team_calc.calculate_defensive_rating(team_id, 10, game_date_obj)
            features['net_rating'] = self.team_calc.calculate_net_rating(team_id, 10, game_date_obj)
            features['pace'] = self.team_calc.calculate_pace(team_id, 10, game_date_obj)
            features['offensive_rebound_rate'] = self.team_calc.calculate_rebound_rate(team_id, 10, True, game_date_obj)
            features['defensive_rebound_rate'] = self.team_calc.calculate_rebound_rate(team_id, 10, False, game_date_obj)
            features['tov_pct'] = self.team_calc.calculate_turnover_rate(team_id, 10, game_date_obj)
            features['assist_rate'] = self.team_calc.calculate_assist_rate(team_id, 10, game_date_obj)
            features['steal_rate'] = self.team_calc.calculate_steal_rate(team_id, 10, game_date_obj)
            features['block_rate'] = self.team_calc.calculate_block_rate(team_id, 10, game_date_obj)
        except Exception as e:
            logger.debug(f"Error calculating advanced metrics for {team_id}: {e}")
        
        # Calculate streaks
        try:
            game_date_obj = game_date.date() if hasattr(game_date, 'date') else game_date
            streak = self.team_calc.calculate_current_streak(team_id, game_date_obj)
            features['win_streak'] = streak.get('win_streak', 0)
            features['loss_streak'] = streak.get('loss_streak', 0)
        except Exception as e:
            logger.debug(f"Error calculating streak for {team_id}: {e}")
        
        # Calculate injury features
        try:
            game_date_obj = game_date.date() if hasattr(game_date, 'date') else game_date
            injury = self.team_calc.calculate_injury_impact(team_id, game_date_obj)
            features['players_out'] = injury.get('players_out')
            features['players_questionable'] = injury.get('players_questionable')
            features['injury_severity_score'] = injury.get('injury_severity_score')
        except Exception as e:
            logger.debug(f"Error calculating injury features for {team_id}: {e}")
        
        # Calculate contextual features
        if len(past_games) > 0:
            last_game_date = past_games.iloc[0]['game_date']
            if hasattr(last_game_date, 'date'):
                last_game_date = last_game_date
            game_date_cmp = game_date if hasattr(game_date, 'days') else pd.Timestamp(game_date)
            
            days_rest = (game_date_cmp - last_game_date).days
            features['days_rest'] = int(days_rest)
            features['is_back_to_back'] = days_rest <= 1
            
            # Games in last 7 days
            week_ago = game_date_cmp - pd.Timedelta(days=7)
            games_last_week = past_games[past_games['game_date'] >= week_ago]
            features['games_in_last_7_days'] = len(games_last_week)
        else:
            features['days_rest'] = None
            features['is_back_to_back'] = None
            features['games_in_last_7_days'] = None
        
        # Home/Away win percentages (using weighted mean)
        home_games = past_games[past_games['is_home'] == True]
        away_games = past_games[past_games['is_home'] == False]
        home_weights = np.array([np.exp(-decay_rate * i) for i in range(len(home_games))]) if not home_games.empty else np.array([])
        away_weights = np.array([np.exp(-decay_rate * i) for i in range(len(away_games))]) if not away_games.empty else np.array([])
        features['home_win_pct'] = safe_weighted_mean(home_games['won'], home_weights) * 100 if not home_games.empty else None
        features['away_win_pct'] = safe_weighted_mean(away_games['won'], away_weights) * 100 if not away_games.empty else None
        
        return features
    
    def _store_features(self, features_list: List[Dict[str, Any]], full_refresh: bool):
        """Store features in database."""
        print(f"\n[STEP 3] Storing {len(features_list)} feature records...")
        
        with self.db_manager.get_session() as session:
            batch_count = 0
            
            for features in tqdm(features_list, desc="  Saving features"):
                try:
                    # Check if exists
                    existing = session.query(TeamRollingFeatures).filter_by(
                        game_id=features['game_id'],
                        team_id=features['team_id']
                    ).first()
                    
                    if existing:
                        if full_refresh:
                            for key, value in features.items():
                                if key not in ('game_id', 'team_id'):
                                    setattr(existing, key, value)
                            self.stats['features_updated'] += 1
                    else:
                        new_feature = TeamRollingFeatures(**features)
                        session.add(new_feature)
                        self.stats['features_created'] += 1
                    
                    batch_count += 1
                    if batch_count >= 100:
                        session.commit()
                        batch_count = 0
                        
                except Exception as e:
                    logger.debug(f"Error storing features: {e}")
                    self.stats['errors'] += 1
            
            session.commit()
        
        print(f"  [OK] Created {self.stats['features_created']}, Updated {self.stats['features_updated']}")
    
    def _calculate_matchup_features(self, games_df: pd.DataFrame, full_refresh: bool):
        """Calculate matchup features for each game."""
        print("\n[STEP 4] Calculating matchup features...")
        
        if games_df.empty:
            print("  No games to process")
            return
        
        # Get existing matchup features to skip if not full refresh
        existing_matchups = set()
        if not full_refresh:
            with self.db_manager.get_session() as session:
                existing = session.query(GameMatchupFeatures.game_id).filter(
                    GameMatchupFeatures.season == self.season
                ).all()
                existing_matchups = {e.game_id for e in existing}
                print(f"  Found {len(existing_matchups)} existing matchup feature records")
        
        matchup_features_list = []
        
        for idx, game_row in tqdm(games_df.iterrows(), total=len(games_df), desc="  Computing matchup features"):
            game_id = game_row['game_id']
            
            # Skip if already exists
            if game_id in existing_matchups:
                continue
            
            game_date = game_row['game_date']
            home_team_id = game_row['home_team_id']
            away_team_id = game_row['away_team_id']
            game_date_obj = game_date.date() if hasattr(game_date, 'date') else game_date
            
            try:
                matchup_features = {
                    'game_id': game_id,
                    'game_date': game_date_obj,
                    'season': self.season,
                    'home_team_id': home_team_id,
                    'away_team_id': away_team_id,
                }
                
                # Head-to-head features
                h2h = self.matchup_calc.get_head_to_head_record(home_team_id, away_team_id, 5, game_date_obj)
                matchup_features['h2h_home_wins'] = h2h.get('team1_wins', 0)
                matchup_features['h2h_away_wins'] = h2h.get('team2_wins', 0)
                matchup_features['h2h_total_games'] = h2h.get('total_games', 0)
                matchup_features['h2h_avg_point_differential'] = self.matchup_calc.get_avg_point_differential_h2h(
                    home_team_id, away_team_id, 5, game_date_obj
                )
                
                h2h_scores = self.matchup_calc.get_avg_score_h2h(home_team_id, away_team_id, 5, game_date_obj)
                matchup_features['h2h_home_avg_score'] = h2h_scores.get('team1_avg_score')
                matchup_features['h2h_away_avg_score'] = h2h_scores.get('team2_avg_score')
                
                # Style matchup
                style = self.matchup_calc.calculate_style_matchup(home_team_id, away_team_id, 10, game_date_obj)
                matchup_features['pace_differential'] = style.get('pace_differential')
                matchup_features['ts_differential'] = style.get('ts_differential')
                matchup_features['efg_differential'] = style.get('efg_differential')
                
                # Recent form comparison
                form = self.matchup_calc.get_recent_form_comparison(home_team_id, away_team_id, 10, game_date_obj)
                matchup_features['home_win_pct_recent'] = form.get('team1_win_pct')
                matchup_features['away_win_pct_recent'] = form.get('team2_win_pct')
                matchup_features['win_pct_differential'] = form.get('win_pct_differential')
                
                # Contextual features
                same_conf = self.contextual_calc.get_conference_matchup(home_team_id, away_team_id)
                matchup_features['same_conference'] = same_conf if same_conf is not None else None
                
                same_div = self.contextual_calc.get_division_matchup(home_team_id, away_team_id)
                matchup_features['same_division'] = same_div if same_div is not None else None
                
                season_type = self.contextual_calc.get_season_type(game_id)
                matchup_features['is_playoffs'] = (season_type == 'Playoffs')
                matchup_features['is_home_advantage'] = 1
                
                # Rest days
                home_rest = self.contextual_calc.calculate_rest_days(home_team_id, game_date_obj)
                away_rest = self.contextual_calc.calculate_rest_days(away_team_id, game_date_obj)
                matchup_features['home_rest_days'] = home_rest if home_rest is not None else None
                matchup_features['away_rest_days'] = away_rest if away_rest is not None else None
                matchup_features['rest_days_differential'] = (
                    (home_rest - away_rest) if (home_rest is not None and away_rest is not None) else None
                )
                
                # Back-to-back
                home_b2b = self.contextual_calc.is_back_to_back(home_team_id, game_date_obj)
                away_b2b = self.contextual_calc.is_back_to_back(away_team_id, game_date_obj)
                matchup_features['home_is_b2b'] = home_b2b if home_b2b is not None else None
                matchup_features['away_is_b2b'] = away_b2b if away_b2b is not None else None
                
                # Days until next game
                home_days_next = self.contextual_calc.get_days_until_next_game(home_team_id, game_date_obj)
                away_days_next = self.contextual_calc.get_days_until_next_game(away_team_id, game_date_obj)
                matchup_features['home_days_until_next'] = home_days_next if home_days_next is not None else None
                matchup_features['away_days_until_next'] = away_days_next if away_days_next is not None else None
                
                matchup_features_list.append(matchup_features)
                
            except Exception as e:
                logger.error(f"Error calculating matchup features for game {game_id}: {e}")
                self.stats['errors'] += 1
        
        # Store matchup features
        if matchup_features_list:
            self._store_matchup_features(matchup_features_list, full_refresh)
    
    def _store_matchup_features(self, features_list: List[Dict[str, Any]], full_refresh: bool):
        """Store matchup features in database."""
        print(f"\n[STEP 5] Storing {len(features_list)} matchup feature records...")
        
        with self.db_manager.get_session() as session:
            for features in tqdm(features_list, desc="  Saving matchup features"):
                try:
                    existing = session.query(GameMatchupFeatures).filter_by(
                        game_id=features['game_id']
                    ).first()
                    
                    if existing:
                        if full_refresh:
                            # Update existing
                            for key, value in features.items():
                                if key != 'game_id':
                                    setattr(existing, key, value)
                            self.stats['matchup_features_updated'] += 1
                    else:
                        # Create new
                        matchup_feat = GameMatchupFeatures(**features)
                        session.add(matchup_feat)
                        self.stats['matchup_features_created'] += 1
                    
                except Exception as e:
                    logger.error(f"Error storing matchup features for {features.get('game_id')}: {e}")
                    self.stats['errors'] += 1
            
            session.commit()
        
        print(f"  [OK] Created {self.stats['matchup_features_created']}, Updated {self.stats['matchup_features_updated']}")
    
    def _print_summary(self):
        """Print transformation summary."""
        print("\n" + "=" * 70)
        print("FEATURE TRANSFORMATION COMPLETE")
        print("=" * 70)
        print(f"  Season: {self.season}")
        print(f"  Games Processed: {self.stats['games_processed']}")
        print(f"  Team Features Created: {self.stats['features_created']}")
        print(f"  Team Features Updated: {self.stats['features_updated']}")
        print(f"  Matchup Features Created: {self.stats['matchup_features_created']}")
        print(f"  Matchup Features Updated: {self.stats['matchup_features_updated']}")
        print(f"  Errors: {self.stats['errors']}")
        print("=" * 70 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Transform raw game data into model-ready rolling features'
    )
    parser.add_argument(
        '--season',
        type=str,
        default='2025-26',
        help='NBA season (e.g., 2025-26)'
    )
    parser.add_argument(
        '--full-refresh',
        action='store_true',
        help='Recalculate all features even if they exist'
    )
    
    args = parser.parse_args()
    
    # Initialize database
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    # Run transformation
    transformer = FeatureTransformer(season=args.season, db_manager=db_manager)
    stats = transformer.run(full_refresh=args.full_refresh)
    
    return 0 if stats['errors'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())

