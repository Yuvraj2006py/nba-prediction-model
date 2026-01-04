"""Retrain models with improved regularization to reduce overconfidence."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from datetime import datetime
from src.training.trainer import ModelTrainer
from src.models.xgboost_model import XGBoostModel
from src.training.data_loader import DataLoader

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_feature_importance(model: XGBoostModel) -> dict:
    """Get feature importance from trained model."""
    if not hasattr(model.model, 'feature_importances_'):
        return {}
    
    importances = model.model.feature_importances_
    feature_names = model.feature_names if model.feature_names else [f'f{i}' for i in range(len(importances))]
    
    # Create sorted dictionary
    importance_dict = dict(zip(feature_names, importances))
    return dict(sorted(importance_dict.items(), key=lambda x: x[1], reverse=True))


def compare_feature_importance(old_model_path: str, new_model: XGBoostModel) -> None:
    """Compare feature importance between old and new models."""
    try:
        # Load old model
        old_model = XGBoostModel('old_model', task_type='classification')
        old_model.load(old_model_path)
        
        old_importance = get_feature_importance(old_model)
        new_importance = get_feature_importance(new_model)
        
        print("\n" + "=" * 80)
        print("FEATURE IMPORTANCE COMPARISON")
        print("=" * 80)
        
        # Compare top 10 features
        print("\nTop 10 Features - OLD Model:")
        print("-" * 80)
        for i, (feat, imp) in enumerate(list(old_importance.items())[:10], 1):
            print(f"  {i:2d}. {feat:40s} {imp:.4f} ({imp*100:.1f}%)")
        
        print("\nTop 10 Features - NEW Model:")
        print("-" * 80)
        for i, (feat, imp) in enumerate(list(new_importance.items())[:10], 1):
            print(f"  {i:2d}. {feat:40s} {imp:.4f} ({imp*100:.1f}%)")
        
        # Check if streak features are less dominant
        streak_features_old = sum(imp for feat, imp in old_importance.items() if 'streak' in feat.lower())
        streak_features_new = sum(imp for feat, imp in new_importance.items() if 'streak' in feat.lower())
        
        print(f"\nStreak Features Total Importance:")
        print(f"  OLD Model: {streak_features_old:.4f} ({streak_features_old*100:.1f}%)")
        print(f"  NEW Model: {streak_features_new:.4f} ({streak_features_new*100:.1f}%)")
        
        if streak_features_new < streak_features_old:
            reduction = ((streak_features_old - streak_features_new) / streak_features_old) * 100
            print(f"  ✓ Reduced by {reduction:.1f}%")
        else:
            print(f"  ⚠️  Increased (may need more regularization)")
        
    except Exception as e:
        logger.warning(f"Could not compare feature importance: {e}")


def main():
    """Retrain models with improved regularization."""
    logger.info("=" * 80)
    logger.info("RETRAINING MODELS WITH IMPROVED REGULARIZATION")
    logger.info("=" * 80)
    logger.info("")
    logger.info("New Parameters:")
    logger.info("  - max_depth: 4 (was 6)")
    logger.info("  - learning_rate: 0.05 (was 0.1)")
    logger.info("  - min_child_weight: 3 (was 1)")
    logger.info("  - gamma: 0.1 (was 0)")
    logger.info("  - reg_alpha: 0.5 (was 0)")
    logger.info("  - reg_lambda: 2.0 (was 1.0)")
    logger.info("")
    logger.info("These changes will:")
    logger.info("  - Reduce overfitting to streak features")
    logger.info("  - Better balance feature importance")
    logger.info("  - Produce more realistic confidence scores")
    logger.info("=" * 80)
    
    # Initialize trainer and data loader
    trainer = ModelTrainer(random_state=42)
    data_loader = DataLoader()
    
    # Check available data first
    from src.database.db_manager import DatabaseManager
    from src.database.models import Game
    
    db_manager = DatabaseManager()
    with db_manager.get_session() as session:
        # Get available seasons
        seasons = session.query(Game.season).distinct().all()
        available_seasons = sorted(set([s[0] for s in seasons if s[0]]))
        
        # Count finished games per season
        season_counts = {}
        for season in available_seasons:
            count = session.query(Game).filter(
                Game.season == season,
                Game.home_score.isnot(None)
            ).count()
            season_counts[season] = count
        
        logger.info("\nAvailable data:")
        for season, count in season_counts.items():
            logger.info(f"  {season}: {count} finished games")
        
        # Determine season splits based on available data
        if '2025-26' in available_seasons and season_counts.get('2025-26', 0) >= 10:
            # Use 2025-26 data, split by date
            train_seasons = ['2025-26']
            val_seasons = ['2025-26']  # Will be split by date
            test_seasons = ['2025-26']  # Will be split by date
            logger.info("\nUsing 2025-26 season with temporal split")
        elif len(available_seasons) >= 2:
            # Use multiple seasons if available
            train_seasons = available_seasons[:-2] if len(available_seasons) > 2 else [available_seasons[0]]
            val_seasons = [available_seasons[-2]] if len(available_seasons) >= 2 else [available_seasons[0]]
            test_seasons = [available_seasons[-1]]
            logger.info(f"\nUsing multi-season split: train={train_seasons}, val={val_seasons}, test={test_seasons}")
        else:
            # Only one season available - use it for all
            train_seasons = available_seasons
            val_seasons = available_seasons
            test_seasons = available_seasons
            logger.info(f"\nUsing single season: {available_seasons[0]}")
    
    # Load data with lower min_features requirement for limited data
    logger.info("\nLoading training data...")
    data = data_loader.load_all_data(
        train_seasons=train_seasons,
        val_seasons=val_seasons,
        test_seasons=test_seasons,
        min_features=20  # Lower threshold for limited data
    )
    
    train_samples = len(data['X_train'])
    val_samples = len(data['X_val'])
    test_samples = len(data['X_test'])
    
    logger.info(f"Training samples: {train_samples}")
    logger.info(f"Validation samples: {val_samples}")
    logger.info(f"Test samples: {test_samples}")
    
    if train_samples == 0:
        logger.error("\nERROR: No training data available!")
        logger.error("Please ensure you have finished games with features in the database.")
        logger.error("You may need to:")
        logger.error("  1. Run the evening workflow to generate features for finished games")
        logger.error("  2. Collect historical data for previous seasons")
        return
    
    if train_samples < 10:
        logger.warning(f"\nWARNING: Only {train_samples} training samples available.")
        logger.warning("Model may not train well with so little data.")
        logger.warning("Consider collecting more historical data.")
        logger.warning("Proceeding with available data...")
    
    # If we have very little data, use all for training and skip validation/test
    if train_samples < 15:
        logger.warning("\nVery limited data - using all available samples for training")
        # Combine all data for training
        import pandas as pd
        X_all = pd.concat([data['X_train'], data['X_val'], data['X_test']], ignore_index=True)
        y_class_all = pd.concat([data['y_train_class'], data['y_val_class'], data['y_test_class']], ignore_index=True)
        y_reg_all = pd.concat([data['y_train_reg'], data['y_val_reg'], data['y_test_reg']], ignore_index=True)
        
        # Use 80% for training, 20% for validation
        from sklearn.model_selection import train_test_split
        X_train_final, X_val_final, y_class_train, y_class_val, y_reg_train, y_reg_val = train_test_split(
            X_all, y_class_all, y_reg_all, test_size=0.2, random_state=42, stratify=y_class_all
        )
        
        data['X_train'] = X_train_final
        data['y_train_class'] = y_class_train
        data['y_train_reg'] = y_reg_train
        data['X_val'] = X_val_final
        data['y_val_class'] = y_class_val
        data['y_val_reg'] = y_reg_val
        data['X_test'] = pd.DataFrame()  # Empty test set
        data['y_test_class'] = pd.Series(dtype=int)
        data['y_test_reg'] = pd.Series(dtype=float)
        
        train_samples = len(data['X_train'])
        val_samples = len(data['X_val'])
        test_samples = 0
        
        logger.info(f"After resplitting: {train_samples} train, {val_samples} val, {test_samples} test")
    
    logger.info(f"Features: {len(data['X_train'].columns) if train_samples > 0 else 0}")
    
    # Train classification model
    logger.info("\n" + "=" * 80)
    logger.info("TRAINING CLASSIFICATION MODEL")
    logger.info("=" * 80)
    
    clf_model = XGBoostModel(
        'nba_v3_classifier',
        task_type='classification',
        random_state=42
        # Uses new default parameters from xgboost_model.py
    )
    
    clf_results = trainer.train_model(
        clf_model,
        data['X_train'],
        data['y_train_class'],
        X_val=data['X_val'] if len(data['X_val']) > 0 else None,
        y_val=data['y_val_class'] if len(data['y_val_class']) > 0 else None,
        X_test=data['X_test'] if len(data['X_test']) > 0 else None,
        y_test=data['y_test_class'] if len(data['y_test_class']) > 0 else None,
        save_model=True
    )
    
    logger.info(f"\nClassification Results:")
    logger.info(f"  Train Accuracy: {clf_results.get('training_metrics', {}).get('train_accuracy', 'N/A')}")
    logger.info(f"  Val Accuracy: {clf_results.get('training_metrics', {}).get('val_accuracy', 'N/A')}")
    logger.info(f"  Test Accuracy: {clf_results.get('test_accuracy', 'N/A')}")
    
    # Train regression model
    logger.info("\n" + "=" * 80)
    logger.info("TRAINING REGRESSION MODEL")
    logger.info("=" * 80)
    
    reg_model = XGBoostModel(
        'nba_v3_regressor',
        task_type='regression',
        random_state=42
        # Uses new default parameters from xgboost_model.py
    )
    
    reg_results = trainer.train_model(
        reg_model,
        data['X_train'],
        data['y_train_reg'],
        X_val=data['X_val'] if len(data['X_val']) > 0 else None,
        y_val=data['y_val_reg'] if len(data['y_val_reg']) > 0 else None,
        X_test=data['X_test'] if len(data['X_test']) > 0 else None,
        y_test=data['y_test_reg'] if len(data['y_test_reg']) > 0 else None,
        save_model=True
    )
    
    logger.info(f"\nRegression Results:")
    logger.info(f"  Train RMSE: {reg_results.get('training_metrics', {}).get('train_rmse', 'N/A')}")
    logger.info(f"  Val RMSE: {reg_results.get('training_metrics', {}).get('val_rmse', 'N/A')}")
    logger.info(f"  Test RMSE: {reg_results.get('test_rmse', 'N/A')}")
    
    # Compare feature importance with old model
    old_model_path = Path('data/models/nba_v2_classifier.pkl')
    if old_model_path.exists():
        compare_feature_importance(str(old_model_path), clf_model)
    
    logger.info("\n" + "=" * 80)
    logger.info("RETRAINING COMPLETE")
    logger.info("=" * 80)
    logger.info(f"\nNew models saved:")
    logger.info(f"  - {clf_results.get('model_path', 'nba_v3_classifier.pkl')}")
    logger.info(f"  - {reg_results.get('model_path', 'nba_v3_regressor.pkl')}")
    logger.info(f"\nTo use new models, update prediction_service.py to use 'nba_v3_classifier'")
    logger.info("=" * 80)


if __name__ == "__main__":
    main()

