"""Data loader for model training - extracts features and labels from database."""

import logging
import pandas as pd
import numpy as np
from typing import Tuple, Optional, Dict, Any, List
from datetime import date
from src.database.db_manager import DatabaseManager
from src.database.models import Game, Feature, TeamRollingFeatures, GameMatchupFeatures
from config.settings import get_settings

logger = logging.getLogger(__name__)


class DataLoader:
    """Loads training data from database with proper preprocessing."""
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize data loader.
        
        Args:
            db_manager: Optional database manager. If None, creates new instance.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.settings = get_settings()
        
        logger.info("DataLoader initialized")
    
    def load_all_data(
        self,
        train_seasons: Optional[List[str]] = None,
        val_seasons: Optional[List[str]] = None,
        test_seasons: Optional[List[str]] = None,
        min_features: int = 40
    ) -> Dict[str, Any]:
        """
        Load all training data with temporal split.
        
        Args:
            train_seasons: List of seasons for training (default: ['2022-23'])
            val_seasons: List of seasons for validation (default: ['2023-24'])
            test_seasons: List of seasons for testing (default: ['2024-25'])
            min_features: Minimum number of features required per game
            
        Returns:
            Dictionary with:
            - X_train, y_train_class, y_train_reg, game_ids_train
            - X_val, y_val_class, y_val_reg, game_ids_val
            - X_test, y_test_class, y_test_reg, game_ids_test
            - feature_names
            - class_imbalance_info
        """
        # Default season splits
        if train_seasons is None:
            train_seasons = ['2022-23']
        if val_seasons is None:
            val_seasons = ['2023-24']
        if test_seasons is None:
            test_seasons = ['2024-25']
        
        logger.info("=" * 70)
        logger.info("Loading Training Data")
        logger.info("=" * 70)
        logger.info(f"Train seasons: {train_seasons}")
        logger.info(f"Val seasons: {val_seasons}")
        logger.info(f"Test seasons: {test_seasons}")
        
        # Load data for each split
        train_data = self._load_season_data(train_seasons, min_features, "Training")
        val_data = self._load_season_data(val_seasons, min_features, "Validation")
        test_data = self._load_season_data(test_seasons, min_features, "Test")
        
        # Check class imbalance
        class_imbalance_info = self._check_class_imbalance(
            train_data['y_class'],
            val_data['y_class'],
            test_data['y_class']
        )
        
        # Handle missing values
        train_data = self._handle_missing_values(train_data, "Training")
        val_data = self._handle_missing_values(val_data, "Validation")
        test_data = self._handle_missing_values(test_data, "Test")
        
        # Ensure all splits have same features
        all_features = set(train_data['X'].columns) | set(val_data['X'].columns) | set(test_data['X'].columns)
        for data in [train_data, val_data, test_data]:
            for feature in all_features:
                if feature not in data['X'].columns:
                    data['X'][feature] = np.nan
        
        # Reorder columns consistently
        feature_order = sorted(all_features)
        train_data['X'] = train_data['X'][feature_order]
        val_data['X'] = val_data['X'][feature_order]
        test_data['X'] = test_data['X'][feature_order]
        
        logger.info("=" * 70)
        logger.info("Data Loading Complete")
        logger.info("=" * 70)
        logger.info(f"Training: {len(train_data['X'])} games, {len(train_data['X'].columns)} features")
        logger.info(f"Validation: {len(val_data['X'])} games, {len(val_data['X'].columns)} features")
        logger.info(f"Test: {len(test_data['X'])} games, {len(test_data['X'].columns)} features")
        
        return {
            'X_train': train_data['X'],
            'y_train_class': train_data['y_class'],
            'y_train_reg': train_data['y_reg'],
            'game_ids_train': train_data['game_ids'],
            
            'X_val': val_data['X'],
            'y_val_class': val_data['y_class'],
            'y_val_reg': val_data['y_reg'],
            'game_ids_val': val_data['game_ids'],
            
            'X_test': test_data['X'],
            'y_test_class': test_data['y_class'],
            'y_test_reg': test_data['y_reg'],
            'game_ids_test': test_data['game_ids'],
            
            'feature_names': feature_order,
            'class_imbalance_info': class_imbalance_info,
            'feature_system': 'TeamRollingFeatures+GameMatchupFeatures',  # For metadata tracking
        }
    
    def _load_season_data(
        self,
        seasons: List[str],
        min_features: int,
        split_name: str
    ) -> Dict[str, Any]:
        """
        Load data for specific seasons.
        
        Uses ONLY the new feature system (TeamRollingFeatures + GameMatchupFeatures).
        No fallback to legacy Feature table to ensure consistency between training and inference.
        """
        logger.info(f"\nLoading {split_name} data for seasons: {seasons}")
        logger.info(f"Feature System: TeamRollingFeatures + GameMatchupFeatures (no fallback)")
        
        with self.db_manager.get_session() as session:
            # Get all finished games for these seasons
            games = session.query(Game).filter(
                Game.season.in_(seasons),
                Game.game_status == 'finished',
                Game.home_score.isnot(None),
                Game.away_score.isnot(None)
            ).order_by(Game.game_date).all()
        
        logger.info(f"Found {len(games)} finished games with scores")
        
        # Load features for each game
        feature_data = []
        game_ids = []
        y_class = []
        y_reg = []
        
        # Track statistics for logging
        games_with_features = 0
        games_missing_rolling = 0
        games_missing_matchup = 0
        
        with self.db_manager.get_session() as session:
            for game in games:
                # Get rolling features (new system ONLY - no fallback)
                home_features = session.query(TeamRollingFeatures).filter_by(
                    game_id=game.game_id,
                    team_id=game.home_team_id
                ).first()
                
                away_features = session.query(TeamRollingFeatures).filter_by(
                    game_id=game.game_id,
                    team_id=game.away_team_id
                ).first()
                
                if not home_features or not away_features:
                    games_missing_rolling += 1
                    logger.debug(f"Game {game.game_id}: Missing TeamRollingFeatures (skipping)")
                    continue
                
                # Extract team rolling features
                feature_dict = self._extract_rolling_features(home_features, away_features)
                
                # Get matchup features
                matchup_features = session.query(GameMatchupFeatures).filter_by(
                    game_id=game.game_id
                ).first()
                
                if matchup_features:
                    matchup_dict = self._extract_matchup_features(matchup_features)
                    feature_dict.update(matchup_dict)
                else:
                    games_missing_matchup += 1
                    logger.debug(f"Game {game.game_id}: Missing GameMatchupFeatures (using team features only)")
                
                # Check minimum features
                if len(feature_dict) < min_features:
                    logger.debug(f"Game {game.game_id} has only {len(feature_dict)} features, skipping")
                    continue
                
                # Create target variables
                if game.home_score is None or game.away_score is None:
                    continue
                
                point_diff = game.home_score - game.away_score
                
                # Skip ties (shouldn't happen in NBA, but handle gracefully)
                if point_diff == 0:
                    logger.debug(f"Game {game.game_id} is a tie, skipping")
                    continue
                
                # Classification: 1 if home wins, 0 if away wins
                home_wins = 1 if point_diff > 0 else 0
                
                feature_data.append(feature_dict)
                game_ids.append(game.game_id)
                y_class.append(home_wins)
                y_reg.append(point_diff)
                games_with_features += 1
        
        # Create DataFrame
        if feature_data:
            X = pd.DataFrame(feature_data)
        else:
            X = pd.DataFrame()
            logger.warning(f"No games loaded for {split_name}")
        
        # Log feature system usage
        logger.info(f"{split_name} Feature Statistics:")
        logger.info(f"  - Games with features: {games_with_features}")
        logger.info(f"  - Missing rolling features: {games_missing_rolling}")
        logger.info(f"  - Missing matchup features: {games_missing_matchup}")
        logger.info(f"  - Final feature count: {len(X.columns) if len(X) > 0 else 0}")
        
        return {
            'X': X,
            'y_class': pd.Series(y_class, name='home_wins', dtype=int),
            'y_reg': pd.Series(y_reg, name='point_differential', dtype=float),
            'game_ids': game_ids,
            'feature_system': 'TeamRollingFeatures+GameMatchupFeatures'
        }
    
    def _handle_missing_values(
        self,
        data: Dict[str, Any],
        split_name: str
    ) -> Dict[str, Any]:
        """Handle missing values in features."""
        X = data['X'].copy()
        
        # Handle empty DataFrame
        if len(X) == 0 or len(X.columns) == 0:
            logger.warning(f"{split_name}: Empty DataFrame, skipping missing value handling")
            data['X'] = X
            return data
        
        # Count missing values before
        missing_before = X.isnull().sum().sum()
        total_cells = len(X) * len(X.columns)
        missing_pct = (missing_before / total_cells * 100) if total_cells > 0 else 0
        
        if missing_before > 0:
            logger.info(f"{split_name}: {missing_before} missing values ({missing_pct:.2f}%)")
            
            # Strategy: Use median for numeric features, 0 for specific feature types
            for col in X.columns:
                if X[col].isnull().any():
                    # Check feature type
                    if 'injury' in col.lower() or 'players_out' in col.lower() or 'players_questionable' in col.lower():
                        # Injury features: default to 0 (no injuries)
                        X[col].fillna(0, inplace=True)
                    elif 'streak' in col.lower():
                        # Streak features: default to 0
                        X[col].fillna(0, inplace=True)
                    elif 'is_' in col.lower() or 'same_' in col.lower() or col.endswith('_b2b'):
                        # Binary features: default to 0
                        X[col].fillna(0, inplace=True)
                    elif 'prob' in col.lower() or 'probability' in col.lower():
                        # Probability features: default to 0.5 (neutral)
                        X[col].fillna(0.5, inplace=True)
                    else:
                        # Numeric features: use median
                        median_val = X[col].median()
                        if pd.isna(median_val):
                            # If all NaN, use 0
                            X[col].fillna(0, inplace=True)
                        else:
                            X[col].fillna(median_val, inplace=True)
            
            missing_after = X.isnull().sum().sum()
            logger.info(f"{split_name}: {missing_after} missing values after imputation")
        
        data['X'] = X
        return data
    
    def _check_class_imbalance(
        self,
        y_train: pd.Series,
        y_val: pd.Series,
        y_test: pd.Series
    ) -> Dict[str, Any]:
        """Check for class imbalance in target variable."""
        train_home_wins = int(y_train.sum()) if len(y_train) > 0 else 0
        train_total = len(y_train)
        train_home_rate = train_home_wins / train_total if train_total > 0 else 0.5
        
        val_home_wins = int(y_val.sum()) if len(y_val) > 0 else 0
        val_total = len(y_val)
        val_home_rate = val_home_wins / val_total if val_total > 0 else 0.5
        
        test_home_wins = int(y_test.sum()) if len(y_test) > 0 else 0
        test_total = len(y_test)
        test_home_rate = test_home_wins / test_total if test_total > 0 else 0.5
        
        overall_home_wins = train_home_wins + val_home_wins + test_home_wins
        overall_total = train_total + val_total + test_total
        overall_home_rate = overall_home_wins / overall_total if overall_total > 0 else 0.5
        
        
        # Determine if imbalance is significant (>55/45 split)
        is_imbalanced = overall_home_rate > 0.55 or overall_home_rate < 0.45
        
        # Calculate scale_pos_weight for XGBoost if imbalanced
        scale_pos_weight = None
        if is_imbalanced:
            # XGBoost scale_pos_weight = (negative_samples / positive_samples)
            # For binary classification: (class_0_count / class_1_count)
            home_wins = overall_home_wins
            away_wins = overall_total - overall_home_wins
            scale_pos_weight = away_wins / home_wins if home_wins > 0 else 1.0
        
        info = {
            'train_home_win_rate': train_home_rate,
            'val_home_win_rate': val_home_rate,
            'test_home_win_rate': test_home_rate,
            'overall_home_win_rate': overall_home_rate,
            'is_imbalanced': is_imbalanced,
            'scale_pos_weight': scale_pos_weight,
            'train_home_wins': train_home_wins,
            'train_away_wins': train_total - train_home_wins,
            'val_home_wins': val_home_wins,
            'val_away_wins': val_total - val_home_wins,
            'test_home_wins': test_home_wins,
            'test_away_wins': test_total - test_home_wins
        }
        
        logger.info("\n" + "=" * 70)
        logger.info("Class Imbalance Analysis")
        logger.info("=" * 70)
        logger.info(f"Train: {train_home_wins}/{train_total} home wins ({train_home_rate*100:.1f}%)")
        logger.info(f"Val: {val_home_wins}/{val_total} home wins ({val_home_rate*100:.1f}%)")
        logger.info(f"Test: {test_home_wins}/{test_total} home wins ({test_home_rate*100:.1f}%)")
        logger.info(f"Overall: {overall_home_wins}/{overall_total} home wins ({overall_home_rate*100:.1f}%)")
        
        if is_imbalanced:
            logger.warning(f"Class imbalance detected! Using scale_pos_weight={scale_pos_weight:.3f}")
        else:
            logger.info("No significant class imbalance detected")
        
        logger.info("=" * 70)
        
        return info
    
    def _extract_rolling_features(
        self,
        home_features: TeamRollingFeatures,
        away_features: TeamRollingFeatures
    ) -> Dict[str, Any]:
        """
        Extract rolling features from TeamRollingFeatures and combine home/away.
        
        Args:
            home_features: TeamRollingFeatures for home team
            away_features: TeamRollingFeatures for away team
            
        Returns:
            Dictionary with combined features (home_* and away_* prefixes)
        """
        feature_dict = {}
        
        # Columns to exclude (metadata, not features)
        exclude_cols = {
            'id', 'game_id', 'team_id', 'is_home', 'game_date', 'season',
            'created_at', 'updated_at', 'won_game', 'point_differential'
        }
        
        # Get all feature columns from the model
        feature_columns = [
            col.name for col in TeamRollingFeatures.__table__.columns
            if col.name not in exclude_cols
        ]
        
        # Name mapping: new name -> old name (for model compatibility)
        name_mapping = {
            'efg_pct': 'effective_fg_pct',
            'ts_pct': 'true_shooting_pct',
            'tov_pct': 'turnover_rate',
        }
        
        # Add home team features with 'home_' prefix
        for col in feature_columns:
            value = getattr(home_features, col, None)
            # Use old name if mapping exists, otherwise use new name
            feature_name = name_mapping.get(col, col)
            feature_dict[f'home_{feature_name}'] = value
        
        # Add away team features with 'away_' prefix
        for col in feature_columns:
            value = getattr(away_features, col, None)
            # Use old name if mapping exists, otherwise use new name
            feature_name = name_mapping.get(col, col)
            feature_dict[f'away_{feature_name}'] = value
        
        return feature_dict
    
    def _extract_matchup_features(self, matchup_features: GameMatchupFeatures) -> Dict[str, Any]:
        """
        Extract matchup features from GameMatchupFeatures.
        
        Args:
            matchup_features: GameMatchupFeatures object
            
        Returns:
            Dictionary with matchup features (using old naming convention for compatibility)
        """
        feature_dict = {}
        
        # Columns to exclude (metadata, not features)
        exclude_cols = {
            'id', 'game_id', 'game_date', 'season', 'home_team_id', 'away_team_id',
            'created_at', 'updated_at'
        }
        
        # Get all feature columns from the model
        feature_columns = [
            col.name for col in GameMatchupFeatures.__table__.columns
            if col.name not in exclude_cols
        ]
        
        # Name mapping for compatibility with old model
        name_mapping = {
            'home_win_pct_recent': 'home_win_pct',  # Map to old name
            'away_win_pct_recent': 'away_win_pct',  # Map to old name
        }
        
        # Add matchup features with name mapping
        for col in feature_columns:
            value = getattr(matchup_features, col, None)
            # Use mapped name if exists, otherwise use original
            feature_name = name_mapping.get(col, col)
            feature_dict[feature_name] = value
        
        return feature_dict
    
    def get_feature_statistics(self, X: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistics about features.
        
        Args:
            X: Feature DataFrame
            
        Returns:
            Dictionary with feature statistics
        """
        stats = {
            'total_features': len(X.columns),
            'total_samples': len(X),
            'missing_values': X.isnull().sum().to_dict(),
            'missing_percentage': (X.isnull().sum() / len(X) * 100).to_dict(),
            'feature_types': X.dtypes.to_dict(),
            'numeric_features': X.select_dtypes(include=[np.number]).columns.tolist(),
            'feature_ranges': {}
        }
        
        # Get ranges for numeric features
        for col in stats['numeric_features']:
            stats['feature_ranges'][col] = {
                'min': float(X[col].min()),
                'max': float(X[col].max()),
                'mean': float(X[col].mean()),
                'median': float(X[col].median()),
                'std': float(X[col].std())
            }
        
        return stats

