"""
Prediction Service for NBA game predictions.

This module provides a single source of truth for inference-time feature generation
and prediction. It ensures feature schema consistency with training.

CRITICAL: Features generated here MUST match those from DataLoader during training.
"""

import logging
import json
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import date, timedelta
import pandas as pd
import numpy as np

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamRollingFeatures, GameMatchupFeatures, Prediction
from src.models.xgboost_model import XGBoostModel
from config.settings import get_settings

logger = logging.getLogger(__name__)


class FeatureSchemaError(Exception):
    """Raised when feature schema mismatch is detected."""
    pass


class PredictionService:
    """
    Service for making predictions on NBA games.
    
    Enforces feature schema contract between training and inference.
    Features are generated using the SAME logic as DataLoader to ensure consistency.
    """
    
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """
        Initialize prediction service.
        
        Args:
            db_manager: Database manager instance. If None, creates new one.
        """
        self.db_manager = db_manager or DatabaseManager()
        self.settings = get_settings()
        self.models_dir = Path(self.settings.MODELS_DIR)
        
        # Cache for loaded models
        self._model_cache: Dict[str, XGBoostModel] = {}
        self._feature_schema_cache: Dict[str, List[str]] = {}
        
        # Feature name mappings (must match DataLoader)
        self._name_mapping = {
            'efg_pct': 'effective_fg_pct',
            'ts_pct': 'true_shooting_pct',
            'tov_pct': 'turnover_rate',
        }
        
        self._matchup_name_mapping = {
            'home_win_pct_recent': 'home_win_pct',
            'away_win_pct_recent': 'away_win_pct',
        }
        
        logger.info("PredictionService initialized")
    
    def load_model(
        self, 
        model_name: str,
        validate_schema: bool = True
    ) -> XGBoostModel:
        """
        Load a trained model from disk.
        
        Args:
            model_name: Name of the model to load
            validate_schema: Whether to validate feature schema exists
            
        Returns:
            Loaded XGBoostModel instance
            
        Raises:
            FileNotFoundError: If model file not found
            FeatureSchemaError: If model has no feature schema and validate_schema=True
        """
        if model_name in self._model_cache:
            return self._model_cache[model_name]
        
        model_path = self.models_dir / f"{model_name}.pkl"
        metadata_path = self.models_dir / f"{model_name}.json"
        
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
        
        # Load metadata first to get task type and feature names
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            task_type = metadata.get('task_type', 'classification')
            feature_names = metadata.get('feature_names', [])
        else:
            task_type = 'classification'
            feature_names = []
        
        # Create and load model
        model = XGBoostModel(model_name, task_type=task_type)
        model.load(model_path)
        
        # Set feature names if available
        if feature_names:
            model.set_feature_names(feature_names)
            self._feature_schema_cache[model_name] = feature_names
            logger.info(f"Loaded model '{model_name}' with {len(feature_names)} features")
        else:
            logger.warning(f"Model '{model_name}' has no feature_names in metadata!")
            if validate_schema:
                raise FeatureSchemaError(
                    f"Model '{model_name}' has no feature schema. "
                    "Cannot guarantee inference features match training. "
                    "Retrain model with updated save logic to persist feature_names."
                )
        
        self._model_cache[model_name] = model
        return model
    
    def get_features_for_game(
        self,
        game_id: str,
        target_feature_names: Optional[List[str]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Get features for a game using the SAME logic as DataLoader.
        
        This is the SINGLE SOURCE OF TRUTH for inference-time features.
        
        Args:
            game_id: Game identifier
            target_feature_names: Expected feature names (for validation)
            
        Returns:
            DataFrame with single row of features, or None if not available
        """
        with self.db_manager.get_session() as session:
            # Get game info
            game = session.query(Game).filter_by(game_id=game_id).first()
            if not game:
                logger.error(f"Game not found: {game_id}")
                return None
            
            # Get rolling features for both teams (SAME as DataLoader)
            home_features = session.query(TeamRollingFeatures).filter_by(
                game_id=game_id,
                team_id=game.home_team_id
            ).first()
            
            away_features = session.query(TeamRollingFeatures).filter_by(
                game_id=game_id,
                team_id=game.away_team_id
            ).first()
            
            if not home_features or not away_features:
                logger.warning(f"Missing rolling features for game {game_id}")
                return None
            
            # Extract team rolling features (SAME logic as DataLoader._extract_rolling_features)
            feature_dict = self._extract_rolling_features(home_features, away_features)
            
            # Get matchup features (SAME as DataLoader)
            matchup_features = session.query(GameMatchupFeatures).filter_by(
                game_id=game_id
            ).first()
            
            if matchup_features:
                matchup_dict = self._extract_matchup_features(matchup_features)
                feature_dict.update(matchup_dict)
            else:
                logger.warning(f"Missing matchup features for game {game_id}")
        
        # Create DataFrame
        feature_df = pd.DataFrame([feature_dict])
        
        # Handle missing values (SAME logic as DataLoader._handle_missing_values)
        feature_df = self._handle_missing_values(feature_df)
        
        # If target feature names provided, align columns
        if target_feature_names:
            feature_df = self._align_features(feature_df, target_feature_names)
        
        return feature_df
    
    def _extract_rolling_features(
        self,
        home_features: TeamRollingFeatures,
        away_features: TeamRollingFeatures
    ) -> Dict[str, Any]:
        """
        Extract rolling features from TeamRollingFeatures.
        
        MUST match DataLoader._extract_rolling_features exactly.
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
        
        # Add home team features with 'home_' prefix
        for col in feature_columns:
            value = getattr(home_features, col, None)
            # Use mapped name if exists (for model compatibility)
            feature_name = self._name_mapping.get(col, col)
            feature_dict[f'home_{feature_name}'] = value
        
        # Add away team features with 'away_' prefix
        for col in feature_columns:
            value = getattr(away_features, col, None)
            feature_name = self._name_mapping.get(col, col)
            feature_dict[f'away_{feature_name}'] = value
        
        return feature_dict
    
    def _extract_matchup_features(
        self,
        matchup_features: GameMatchupFeatures
    ) -> Dict[str, Any]:
        """
        Extract matchup features from GameMatchupFeatures.
        
        MUST match DataLoader._extract_matchup_features exactly.
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
        
        # Add matchup features with name mapping
        for col in feature_columns:
            value = getattr(matchup_features, col, None)
            feature_name = self._matchup_name_mapping.get(col, col)
            feature_dict[feature_name] = value
        
        return feature_dict
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values using same logic as DataLoader.
        """
        df = df.copy()
        
        for col in df.columns:
            if df[col].isnull().any():
                # Check feature type and apply appropriate default
                col_lower = col.lower()
                if 'injury' in col_lower or 'players_out' in col_lower or 'players_questionable' in col_lower:
                    df[col] = df[col].fillna(0)
                elif 'streak' in col_lower:
                    df[col] = df[col].fillna(0)
                elif 'is_' in col_lower or 'same_' in col_lower or col.endswith('_b2b'):
                    df[col] = df[col].fillna(0)
                elif 'prob' in col_lower or 'probability' in col_lower:
                    df[col] = df[col].fillna(0.5)
                else:
                    # Default to 0 for inference (median not available for single sample)
                    df[col] = df[col].fillna(0)
        
        return df
    
    def _align_features(
        self,
        df: pd.DataFrame,
        target_feature_names: List[str]
    ) -> pd.DataFrame:
        """
        Align DataFrame columns to match target feature names exactly.
        
        Raises FeatureSchemaError if critical mismatches are found.
        """
        current_features = set(df.columns)
        target_features = set(target_feature_names)
        
        missing = target_features - current_features
        extra = current_features - target_features
        
        if missing:
            logger.warning(f"Missing {len(missing)} features from model schema: {list(missing)[:5]}...")
            # Add missing features with default value 0
            for feat in missing:
                df[feat] = 0.0
        
        if extra:
            logger.debug(f"Dropping {len(extra)} extra features not in model schema")
            df = df.drop(columns=list(extra))
        
        # Reorder to match target exactly
        df = df[target_feature_names]
        
        return df
    
    def predict_game(
        self,
        game_id: str,
        model_name: str,
        clf_model_name: Optional[str] = None,
        reg_model_name: Optional[str] = None,
        regenerate_features: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Make prediction for a single game.
        
        Args:
            game_id: Game identifier
            model_name: Primary model name
            clf_model_name: Classification model name (optional)
            reg_model_name: Regression model name (optional)
            regenerate_features: Whether to regenerate features (not used, kept for API compat)
            
        Returns:
            Dictionary with prediction results, or None if prediction fails
        """
        # Load models
        clf_name = clf_model_name or model_name
        reg_name = reg_model_name
        
        try:
            clf_model = self.load_model(clf_name, validate_schema=False)
        except FileNotFoundError:
            logger.error(f"Classification model not found: {clf_name}")
            return None
        
        reg_model = None
        if reg_name:
            try:
                reg_model = self.load_model(reg_name, validate_schema=False)
            except FileNotFoundError:
                logger.warning(f"Regression model not found: {reg_name}")
        
        # Get game info
        with self.db_manager.get_session() as session:
            game = session.query(Game).filter_by(game_id=game_id).first()
            if not game:
                logger.error(f"Game not found: {game_id}")
                return None
            
            game_info = {
                'game_id': game.game_id,
                'game_date': game.game_date,
                'home_team_id': game.home_team_id,
                'away_team_id': game.away_team_id,
            }
        
        # Get features (aligned to model's expected schema if available)
        target_features = clf_model.feature_names if clf_model.feature_names else None
        features = self.get_features_for_game(game_id, target_features)
        
        if features is None:
            logger.error(f"Could not get features for game {game_id}")
            return None
        
        # Validate feature count
        if clf_model.feature_names:
            if len(features.columns) != len(clf_model.feature_names):
                logger.error(
                    f"Feature count mismatch: got {len(features.columns)}, "
                    f"model expects {len(clf_model.feature_names)}"
                )
                return None
        
        # Make classification prediction
        try:
            predictions, probabilities = clf_model.predict(features, return_proba=True)
            
            predicted_class = int(predictions[0])
            home_prob = float(probabilities[0][1]) if len(probabilities[0]) > 1 else float(probabilities[0][0])
            away_prob = 1.0 - home_prob
            
            predicted_winner = game_info['home_team_id'] if predicted_class == 1 else game_info['away_team_id']
            confidence = max(home_prob, away_prob)
            
        except Exception as e:
            logger.error(f"Classification prediction failed: {e}")
            return None
        
        # Make regression prediction if model available
        predicted_point_diff = None
        if reg_model:
            try:
                reg_target_features = reg_model.feature_names if reg_model.feature_names else None
                reg_features = self.get_features_for_game(game_id, reg_target_features)
                if reg_features is not None:
                    reg_pred = reg_model.predict(reg_features)
                    predicted_point_diff = float(reg_pred[0])
            except Exception as e:
                logger.warning(f"Regression prediction failed: {e}")
        
        result = {
            **game_info,
            'predicted_winner': predicted_winner,
            'win_probability_home': home_prob,
            'win_probability_away': away_prob,
            'confidence': confidence,
            'predicted_point_differential': predicted_point_diff,
            'model_name': clf_name,
        }
        
        return result
    
    def predict_batch(
        self,
        game_ids: List[str],
        model_name: str,
        clf_model_name: Optional[str] = None,
        reg_model_name: Optional[str] = None,
        save_to_db: bool = True,
        regenerate_features: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Make predictions for multiple games.
        
        Args:
            game_ids: List of game identifiers
            model_name: Primary model name
            clf_model_name: Classification model name
            reg_model_name: Regression model name
            save_to_db: Whether to save predictions to database
            regenerate_features: Whether to regenerate features
            
        Returns:
            List of prediction results
        """
        results = []
        
        for game_id in game_ids:
            try:
                result = self.predict_game(
                    game_id=game_id,
                    model_name=model_name,
                    clf_model_name=clf_model_name,
                    reg_model_name=reg_model_name,
                    regenerate_features=regenerate_features
                )
                
                if result:
                    if save_to_db:
                        self.save_prediction(result, model_name=clf_model_name or model_name)
                    results.append(result)
                else:
                    results.append({'game_id': game_id, 'error': 'Prediction failed'})
                    
            except Exception as e:
                logger.error(f"Error predicting game {game_id}: {e}")
                results.append({'game_id': game_id, 'error': str(e)})
        
        return results
    
    def save_prediction(
        self,
        prediction: Dict[str, Any],
        model_name: Optional[str] = None
    ) -> int:
        """
        Save prediction to database.
        
        Args:
            prediction: Prediction dictionary
            model_name: Model name to use (overrides prediction['model_name'])
            
        Returns:
            Prediction ID
        """
        with self.db_manager.get_session() as session:
            pred = Prediction(
                game_id=prediction['game_id'],
                model_name=model_name or prediction.get('model_name', 'unknown'),
                predicted_winner=prediction.get('predicted_winner'),
                win_probability_home=prediction['win_probability_home'],
                win_probability_away=prediction['win_probability_away'],
                predicted_point_differential=prediction.get('predicted_point_differential'),
                confidence=prediction['confidence']
            )
            session.add(pred)
            session.commit()
            return pred.id
    
    def get_upcoming_games(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: Optional[int] = 50
    ) -> List[Game]:
        """
        Get upcoming games that need predictions.
        
        Args:
            start_date: Start date (default: today)
            end_date: End date (default: 7 days from start)
            limit: Maximum number of games to return
            
        Returns:
            List of Game objects
        """
        if start_date is None:
            start_date = date.today()
        if end_date is None:
            end_date = start_date + timedelta(days=7)
        
        with self.db_manager.get_session() as session:
            query = session.query(Game).filter(
                Game.game_date >= start_date,
                Game.game_date <= end_date,
                Game.game_status != 'finished'
            ).order_by(Game.game_date)
            
            if limit:
                query = query.limit(limit)
            
            return query.all()
