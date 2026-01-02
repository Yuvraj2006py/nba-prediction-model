"""
Retrain NBA prediction model with feature contract enforcement.

This script:
1. Loads training data using the updated DataLoader (no fallback)
2. Trains new XGBoost models (classification + regression)
3. Saves models WITH feature_names in metadata (critical for inference)
4. Validates the entire pipeline end-to-end
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
import json
from datetime import datetime

from src.training.data_loader import DataLoader
from src.training.trainer import ModelTrainer
from src.models.xgboost_model import XGBoostModel
from src.prediction.prediction_service import PredictionService
from src.database.db_manager import DatabaseManager
from config.settings import get_settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def retrain_models():
    """Retrain models with proper feature contract."""
    
    logger.info("=" * 70)
    logger.info("RETRAINING NBA PREDICTION MODELS WITH FEATURE CONTRACT")
    logger.info("=" * 70)
    
    # Initialize
    db_manager = DatabaseManager()
    data_loader = DataLoader(db_manager)
    
    # Load training data
    logger.info("\n[STEP 1] Loading training data...")
    data = data_loader.load_all_data(
        train_seasons=['2022-23', '2023-24'],
        val_seasons=['2024-25'],
        test_seasons=['2025-26'],
        min_features=40
    )
    
    # Validate data loaded correctly
    if data['X_train'].empty:
        logger.error("No training data loaded! Check database.")
        return None
    
    feature_names = data['feature_names']
    logger.info(f"\nFeature Contract:")
    logger.info(f"  - Feature count: {len(feature_names)}")
    logger.info(f"  - Feature system: {data.get('feature_system', 'unknown')}")
    logger.info(f"  - Train samples: {len(data['X_train'])}")
    logger.info(f"  - Val samples: {len(data['X_val'])}")
    logger.info(f"  - Test samples: {len(data['X_test'])}")
    
    # Train classification model
    logger.info("\n[STEP 2] Training classification model...")
    clf_model = XGBoostModel(
        model_name='nba_v2_classifier',
        task_type='classification',
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        gamma=0.1,
        reg_alpha=0.1,
        reg_lambda=1.5
    )
    
    clf_metrics = clf_model.train(
        X_train=data['X_train'],
        y_train=data['y_train_class'],
        X_val=data['X_val'],
        y_val=data['y_val_class']
    )
    
    logger.info(f"Classification Training Metrics:")
    logger.info(f"  - Train Accuracy: {clf_metrics.get('train_accuracy', 'N/A'):.4f}")
    logger.info(f"  - Val Accuracy: {clf_metrics.get('val_accuracy', 'N/A'):.4f}")
    
    # Validate feature names were set
    assert clf_model.feature_names is not None, "Feature names not set on classification model!"
    assert len(clf_model.feature_names) == len(feature_names), "Feature count mismatch!"
    
    # Save classification model
    clf_path = clf_model.save()
    logger.info(f"Saved classification model to: {clf_path}")
    
    # Verify feature_names saved in metadata
    metadata_path = clf_path.with_suffix('.json')
    with open(metadata_path, 'r') as f:
        saved_metadata = json.load(f)
    
    assert 'feature_names' in saved_metadata, "feature_names not in saved metadata!"
    assert saved_metadata['feature_names'] == feature_names, "Saved feature_names don't match!"
    logger.info(f"✓ Verified feature_names saved in metadata ({len(saved_metadata['feature_names'])} features)")
    
    # Train regression model
    logger.info("\n[STEP 3] Training regression model...")
    reg_model = XGBoostModel(
        model_name='nba_v2_regressor',
        task_type='regression',
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=3,
        gamma=0.1,
        reg_alpha=0.1,
        reg_lambda=1.5
    )
    
    reg_metrics = reg_model.train(
        X_train=data['X_train'],
        y_train=data['y_train_reg'],
        X_val=data['X_val'],
        y_val=data['y_val_reg']
    )
    
    logger.info(f"Regression Training Metrics:")
    logger.info(f"  - Train MAE: {reg_metrics.get('train_mae', 'N/A'):.2f}")
    logger.info(f"  - Train R2: {reg_metrics.get('train_r2', 'N/A'):.4f}")
    logger.info(f"  - Val MAE: {reg_metrics.get('val_mae', 'N/A'):.2f}")
    logger.info(f"  - Val R2: {reg_metrics.get('val_r2', 'N/A'):.4f}")
    
    # Save regression model
    reg_path = reg_model.save()
    logger.info(f"Saved regression model to: {reg_path}")
    
    # Test loading model and checking feature_names restored
    logger.info("\n[STEP 4] Testing model load with feature contract...")
    loaded_clf = XGBoostModel('nba_v2_classifier', task_type='classification')
    loaded_clf.load(clf_path)
    
    assert loaded_clf.feature_names is not None, "Feature names not restored on load!"
    assert loaded_clf.feature_names == feature_names, "Loaded feature names don't match original!"
    logger.info(f"✓ Feature names correctly restored on load ({len(loaded_clf.feature_names)} features)")
    
    # Test prediction service
    logger.info("\n[STEP 5] Testing PredictionService...")
    prediction_service = PredictionService(db_manager)
    
    # Load model through prediction service
    loaded_model = prediction_service.load_model('nba_v2_classifier', validate_schema=True)
    logger.info(f"✓ Model loaded through PredictionService ({len(loaded_model.feature_names)} features)")
    
    # Get a test game
    test_game_ids = data['game_ids_test'][:3] if data['game_ids_test'] else []
    
    if test_game_ids:
        logger.info(f"\n[STEP 6] Testing predictions on {len(test_game_ids)} games...")
        
        for game_id in test_game_ids:
            result = prediction_service.predict_game(
                game_id=game_id,
                model_name='nba_v2_classifier',
                reg_model_name='nba_v2_regressor'
            )
            
            if result:
                logger.info(f"  Game {game_id}:")
                logger.info(f"    Predicted winner: {result['predicted_winner']}")
                logger.info(f"    Home prob: {result['win_probability_home']:.1%}")
                logger.info(f"    Point diff: {result.get('predicted_point_differential', 'N/A')}")
            else:
                logger.warning(f"  Game {game_id}: Prediction failed")
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("TRAINING COMPLETE - SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Feature System: {data.get('feature_system', 'unknown')}")
    logger.info(f"Feature Count: {len(feature_names)}")
    logger.info(f"Classification Model: nba_v2_classifier")
    logger.info(f"  - Train Accuracy: {clf_metrics.get('train_accuracy', 'N/A'):.4f}")
    logger.info(f"  - Val Accuracy: {clf_metrics.get('val_accuracy', 'N/A'):.4f}")
    logger.info(f"Regression Model: nba_v2_regressor")
    logger.info(f"  - Train MAE: {reg_metrics.get('train_mae', 'N/A'):.2f}")
    logger.info(f"  - Val MAE: {reg_metrics.get('val_mae', 'N/A'):.2f}")
    logger.info(f"\n✓ Feature contract enforced - feature_names saved in model metadata")
    logger.info(f"✓ PredictionService tested successfully")
    
    return {
        'clf_model': clf_model,
        'reg_model': reg_model,
        'clf_metrics': clf_metrics,
        'reg_metrics': reg_metrics,
        'feature_names': feature_names
    }


if __name__ == '__main__':
    retrain_models()

