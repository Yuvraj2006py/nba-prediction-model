"""Feature Aggregator - Combines all feature calculators into feature vectors."""

import logging
import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
from datetime import date
from src.database.db_manager import DatabaseManager
from src.features.team_features import TeamFeatureCalculator
from src.features.matchup_features import MatchupFeatureCalculator
from src.features.contextual_features import ContextualFeatureCalculator
from src.features.betting_features import BettingFeatureCalculator
from config.settings import get_settings

logger = logging.getLogger(__name__)


class FeatureAggregator:
    """Aggregates all feature calculators into feature vectors for modeling."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize feature aggregator.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.settings = get_settings()
        self.cache_enabled = self.settings.FEATURE_CACHE_ENABLED
        self.default_games_back = self.settings.DEFAULT_GAMES_BACK
        
        # Initialize feature calculators
        self.team_calc = TeamFeatureCalculator(db_manager)
        self.matchup_calc = MatchupFeatureCalculator(db_manager)
        self.contextual_calc = ContextualFeatureCalculator(db_manager)
        self.betting_calc = BettingFeatureCalculator(db_manager)
        
        logger.info("FeatureAggregator initialized")
    
    def create_feature_vector(
        self,
        game_id: str,
        home_team_id: str,
        away_team_id: str,
        end_date: Optional[date] = None,
        games_back: int = None,
        use_cache: bool = True
    ) -> pd.DataFrame:
        """
        Create feature vector for a game by combining all feature calculators.
        
        Args:
            game_id: Game identifier
            home_team_id: Home team identifier
            away_team_id: Away team identifier
            end_date: Cutoff date (to avoid data leakage). If None, uses game date.
            games_back: Number of games to look back. If None, uses default.
            use_cache: If True, check cache first and save results
            
        Returns:
            DataFrame with single row containing all features
        """
        if games_back is None:
            games_back = self.default_games_back
        
        # Get game date if end_date not provided
        if end_date is None:
            game = self.db_manager.get_game(game_id)
            if game and game.game_date:
                end_date = game.game_date
            else:
                logger.warning(f"Could not determine game date for {game_id}, using today")
                end_date = date.today()
        
        # Check cache if enabled
        if use_cache and self.cache_enabled:
            cached_features = self.get_features_from_db(game_id)
            if cached_features is not None:
                logger.debug(f"Using cached features for game {game_id}")
                return cached_features
        
        logger.info(f"Creating feature vector for game {game_id}")
        
        # Calculate all features
        features = {}
        
        # Team features (home and away)
        features.update(self._calculate_team_features(home_team_id, True, end_date, games_back))
        features.update(self._calculate_team_features(away_team_id, False, end_date, games_back))
        
        # Matchup features
        features.update(self._calculate_matchup_features(home_team_id, away_team_id, end_date, games_back))
        
        # Contextual features
        features.update(self._calculate_contextual_features(game_id, home_team_id, away_team_id, end_date))
        
        # Betting features
        features.update(self._calculate_betting_features(game_id, home_team_id, away_team_id))
        
        # Create DataFrame
        feature_df = pd.DataFrame([features])
        
        # Save to database if caching enabled
        if use_cache and self.cache_enabled:
            self.save_features_to_db(game_id, features)
        
        return feature_df
    
    def _calculate_team_features(
        self,
        team_id: str,
        is_home: bool,
        end_date: date,
        games_back: int
    ) -> Dict[str, Any]:
        """Calculate team-specific features."""
        prefix = 'home_' if is_home else 'away_'
        
        features = {}
        
        # Offensive metrics
        off_rating = self.team_calc.calculate_offensive_rating(team_id, games_back, end_date)
        features[f'{prefix}offensive_rating'] = off_rating
        
        # Defensive metrics
        def_rating = self.team_calc.calculate_defensive_rating(team_id, games_back, end_date)
        features[f'{prefix}defensive_rating'] = def_rating
        
        # Net rating
        net_rating = self.team_calc.calculate_net_rating(team_id, games_back, end_date)
        features[f'{prefix}net_rating'] = net_rating
        
        # Pace
        pace = self.team_calc.calculate_pace(team_id, games_back, end_date)
        features[f'{prefix}pace'] = pace
        
        # Shooting metrics
        ts_pct = self.team_calc.calculate_true_shooting(team_id, games_back, end_date)
        features[f'{prefix}true_shooting_pct'] = ts_pct
        
        efg_pct = self.team_calc.calculate_effective_fg_percentage(team_id, games_back, end_date)
        features[f'{prefix}effective_fg_pct'] = efg_pct
        
        # Rebounding
        orb_rate = self.team_calc.calculate_rebound_rate(team_id, games_back, True, end_date)
        features[f'{prefix}offensive_rebound_rate'] = orb_rate
        
        drb_rate = self.team_calc.calculate_rebound_rate(team_id, games_back, False, end_date)
        features[f'{prefix}defensive_rebound_rate'] = drb_rate
        
        # Turnover rate
        tov_rate = self.team_calc.calculate_turnover_rate(team_id, games_back, end_date)
        features[f'{prefix}turnover_rate'] = tov_rate
        
        # Win percentage
        win_pct = self.team_calc.calculate_win_percentage(team_id, games_back, False, end_date)
        features[f'{prefix}win_pct'] = win_pct
        
        # Point differential
        avg_diff = self.team_calc.calculate_avg_point_differential(team_id, games_back, end_date)
        features[f'{prefix}avg_point_differential'] = avg_diff
        
        # Average points
        avg_points_for = self.team_calc.calculate_avg_points_for(team_id, games_back, end_date)
        features[f'{prefix}avg_points_for'] = avg_points_for
        
        avg_points_against = self.team_calc.calculate_avg_points_against(team_id, games_back, end_date)
        features[f'{prefix}avg_points_against'] = avg_points_against
        
        # Win/loss streaks
        streak = self.team_calc.calculate_current_streak(team_id, end_date)
        features[f'{prefix}win_streak'] = streak.get('win_streak', 0)
        features[f'{prefix}loss_streak'] = streak.get('loss_streak', 0)
        
        # Injury impact
        injury = self.team_calc.calculate_injury_impact(team_id, end_date)
        features[f'{prefix}players_out'] = injury.get('players_out')
        features[f'{prefix}players_questionable'] = injury.get('players_questionable')
        features[f'{prefix}injury_severity_score'] = injury.get('injury_severity_score')
        
        # Advanced stats
        assist_rate = self.team_calc.calculate_assist_rate(team_id, games_back, end_date)
        features[f'{prefix}assist_rate'] = assist_rate
        
        steal_rate = self.team_calc.calculate_steal_rate(team_id, games_back, end_date)
        features[f'{prefix}steal_rate'] = steal_rate
        
        block_rate = self.team_calc.calculate_block_rate(team_id, games_back, end_date)
        features[f'{prefix}block_rate'] = block_rate
        
        return features
    
    def _calculate_matchup_features(
        self,
        home_team_id: str,
        away_team_id: str,
        end_date: date,
        games_back: int
    ) -> Dict[str, Any]:
        """Calculate matchup-specific features."""
        features = {}
        
        # Head-to-head record
        h2h = self.matchup_calc.get_head_to_head_record(home_team_id, away_team_id, 5, end_date)
        features['h2h_home_wins'] = h2h['team1_wins']
        features['h2h_away_wins'] = h2h['team2_wins']
        features['h2h_total_games'] = h2h['total_games']
        
        # H2H point differential
        h2h_diff = self.matchup_calc.get_avg_point_differential_h2h(home_team_id, away_team_id, 5, end_date)
        features['h2h_avg_point_differential'] = h2h_diff
        
        # Style matchup
        style = self.matchup_calc.calculate_style_matchup(home_team_id, away_team_id, games_back, end_date)
        features['pace_differential'] = style.get('pace_differential')
        features['ts_differential'] = style.get('ts_differential')
        features['efg_differential'] = style.get('efg_differential')
        
        # Recent form comparison
        form = self.matchup_calc.get_recent_form_comparison(home_team_id, away_team_id, games_back, end_date)
        features['home_win_pct'] = form.get('team1_win_pct')
        features['away_win_pct'] = form.get('team2_win_pct')
        features['win_pct_differential'] = form.get('win_pct_differential')
        
        # H2H average scores
        h2h_scores = self.matchup_calc.get_avg_score_h2h(home_team_id, away_team_id, 5, end_date)
        features['h2h_home_avg_score'] = h2h_scores.get('team1_avg_score')
        features['h2h_away_avg_score'] = h2h_scores.get('team2_avg_score')
        
        return features
    
    def _calculate_contextual_features(
        self,
        game_id: str,
        home_team_id: str,
        away_team_id: str,
        end_date: date
    ) -> Dict[str, Any]:
        """Calculate contextual features."""
        features = {}
        
        # Rest days
        home_rest = self.contextual_calc.calculate_rest_days(home_team_id, end_date)
        away_rest = self.contextual_calc.calculate_rest_days(away_team_id, end_date)
        features['home_rest_days'] = home_rest if home_rest is not None else 0
        features['away_rest_days'] = away_rest if away_rest is not None else 0
        features['rest_days_differential'] = (home_rest - away_rest) if (home_rest and away_rest) else None
        
        # Back-to-back
        home_b2b = self.contextual_calc.is_back_to_back(home_team_id, end_date)
        away_b2b = self.contextual_calc.is_back_to_back(away_team_id, end_date)
        features['home_is_b2b'] = 1 if home_b2b else 0
        features['away_is_b2b'] = 1 if away_b2b else 0
        
        # Home advantage (always 1 for home team in this context)
        features['is_home_advantage'] = 1
        
        # Conference/division matchup
        same_conf = self.contextual_calc.get_conference_matchup(home_team_id, away_team_id)
        features['same_conference'] = 1 if same_conf else 0 if same_conf is not None else None
        
        same_div = self.contextual_calc.get_division_matchup(home_team_id, away_team_id)
        features['same_division'] = 1 if same_div else 0 if same_div is not None else None
        
        # Season type
        season_type = self.contextual_calc.get_season_type(game_id)
        features['is_playoffs'] = 1 if season_type == 'Playoffs' else 0
        
        # Days until next game (fatigue indicator)
        home_days_next = self.contextual_calc.get_days_until_next_game(home_team_id, end_date)
        away_days_next = self.contextual_calc.get_days_until_next_game(away_team_id, end_date)
        features['home_days_until_next'] = home_days_next if home_days_next is not None else None
        features['away_days_until_next'] = away_days_next if away_days_next is not None else None
        
        return features
    
    def _calculate_betting_features(
        self,
        game_id: str,
        home_team_id: str,
        away_team_id: str
    ) -> Dict[str, Any]:
        """Calculate betting-related features."""
        betting_features = self.betting_calc.get_all_betting_features(
            game_id, home_team_id, away_team_id
        )
        
        # Rename keys to match feature naming convention
        features = {
            'consensus_spread': betting_features.get('consensus_spread'),
            'consensus_total': betting_features.get('consensus_total'),
            'home_moneyline_prob': betting_features.get('home_moneyline_prob'),
            'away_moneyline_prob': betting_features.get('away_moneyline_prob'),
            'spread_implied_prob': betting_features.get('spread_implied_prob'),
            'over_implied_prob': betting_features.get('over_implied_prob'),
            'under_implied_prob': betting_features.get('under_implied_prob'),
            'spread_movement': betting_features.get('spread_movement'),
            'total_movement': betting_features.get('total_movement')
        }
        
        return features
    
    def save_features_to_db(
        self,
        game_id: str,
        features: Dict[str, Any]
    ) -> None:
        """
        Save features to database using batch insert for efficiency.
        
        Args:
            game_id: Game identifier
            features: Dictionary of feature names and values
        """
        try:
            # Get game once (cache it to avoid multiple queries)
            game = self.db_manager.get_game(game_id)
            if not game:
                logger.warning(f"Game {game_id} not found, cannot save features")
                return
            
            # Prepare all feature data in one batch
            feature_records = []
            
            for feature_name, feature_value in features.items():
                # Determine category
                if feature_name.startswith('home_') or feature_name.startswith('away_'):
                    category = 'team'
                    # Extract team_id from feature name
                    team_id = game.home_team_id if feature_name.startswith('home_') else game.away_team_id
                elif feature_name.startswith('h2h_') or 'differential' in feature_name:
                    category = 'matchup'
                    team_id = None
                elif feature_name.startswith('consensus_') or 'moneyline' in feature_name or 'implied' in feature_name or 'movement' in feature_name:
                    category = 'betting'
                    team_id = None
                else:
                    category = 'contextual'
                    team_id = None
                
                feature_records.append({
                    'game_id': game_id,
                    'feature_name': feature_name,
                    'feature_value': feature_value if feature_value is not None else None,
                    'feature_category': category,
                    'team_id': team_id
                })
            
            # Batch insert/update in a single session
            with self.db_manager.get_session() as session:
                from src.database.models import Feature
                
                for feature_data in feature_records:
                    # Check if feature exists
                    feature = session.query(Feature).filter_by(
                        game_id=feature_data['game_id'],
                        feature_name=feature_data['feature_name']
                    ).first()
                    
                    if feature:
                        # Update existing
                        feature.feature_value = feature_data['feature_value']
                        feature.feature_category = feature_data['feature_category']
                        feature.team_id = feature_data['team_id']
                    else:
                        # Create new
                        feature = Feature(**feature_data)
                        session.add(feature)
                
                # Single commit for all features
                session.commit()
            
            logger.debug(f"Saved {len(features)} features to database for game {game_id}")
            
        except Exception as e:
            logger.error(f"Error saving features to database: {e}")
            raise  # Re-raise to handle in calling code
    
    def get_features_from_db(
        self,
        game_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Retrieve cached features from database.
        
        Args:
            game_id: Game identifier
            
        Returns:
            DataFrame with features or None if not found
        """
        try:
            with self.db_manager.get_session() as session:
                from src.database.models import Feature
                features = session.query(Feature).filter_by(game_id=game_id).all()
                
                if not features:
                    return None
                
                # Convert to dictionary
                feature_dict = {f.feature_name: f.feature_value for f in features}
                
                # Create DataFrame
                feature_df = pd.DataFrame([feature_dict])
                
                return feature_df
                
        except Exception as e:
            logger.error(f"Error retrieving features from database: {e}")
            return None

