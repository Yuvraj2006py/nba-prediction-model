"""Production training script for NBA prediction models."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
import logging
from typing import List, Optional
from src.training.trainer import ModelTrainer
from src.models.xgboost_model import XGBoostModel
from config.settings import get_settings

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_seasons(seasons_str: str) -> List[str]:
    """Parse comma-separated seasons string into list."""
    return [s.strip() for s in seasons_str.split(',')]


def main():
    parser = argparse.ArgumentParser(
        description='Train NBA game prediction models',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Train classification model
  python scripts/train_model.py --task classification --seasons 2022-23,2023-24
  
  # Train with hyperparameter tuning
  python scripts/train_model.py --task classification --tune --n_iter 20
  
  # Train both classification and regression
  python scripts/train_model.py --task both
        """
    )
    
    # Task selection
    parser.add_argument(
        '--task',
        type=str,
        choices=['classification', 'regression', 'both'],
        default='both',
        help='Task type: classification, regression, or both (default: both)'
    )
    
    # Data selection
    parser.add_argument(
        '--train-seasons',
        type=str,
        default='2022-23',
        help='Comma-separated list of training seasons (default: 2022-23)'
    )
    parser.add_argument(
        '--val-seasons',
        type=str,
        default='2023-24',
        help='Comma-separated list of validation seasons (default: 2023-24)'
    )
    parser.add_argument(
        '--test-seasons',
        type=str,
        default='2024-25',
        help='Comma-separated list of test seasons (default: 2024-25)'
    )
    
    # Model configuration
    parser.add_argument(
        '--model-name',
        type=str,
        default=None,
        help='Model name prefix (default: auto-generated)'
    )
    parser.add_argument(
        '--n-estimators',
        type=int,
        default=100,
        help='Number of estimators for XGBoost (default: 100)'
    )
    parser.add_argument(
        '--max-depth',
        type=int,
        default=6,
        help='Max depth for XGBoost (default: 6)'
    )
    parser.add_argument(
        '--learning-rate',
        type=float,
        default=0.1,
        help='Learning rate for XGBoost (default: 0.1)'
    )
    
    # Hyperparameter tuning
    parser.add_argument(
        '--tune',
        action='store_true',
        help='Enable hyperparameter tuning (random search)'
    )
    parser.add_argument(
        '--n-iter',
        type=int,
        default=50,
        help='Number of random search iterations (default: 50)'
    )
    parser.add_argument(
        '--exclude-betting-features',
        action='store_true',
        help='Exclude betting-related features from training (already excluded by default)'
    )
    
    # Other options
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save trained models'
    )
    parser.add_argument(
        '--random-state',
        type=int,
        default=42,
        help='Random state for reproducibility (default: 42)'
    )
    
    args = parser.parse_args()
    
    # Parse seasons
    train_seasons = parse_seasons(args.train_seasons)
    val_seasons = parse_seasons(args.val_seasons)
    test_seasons = parse_seasons(args.test_seasons)
    
    logger.info("=" * 70)
    logger.info("NBA Model Training")
    logger.info("=" * 70)
    logger.info(f"Task: {args.task}")
    logger.info(f"Train seasons: {train_seasons}")
    logger.info(f"Val seasons: {val_seasons}")
    logger.info(f"Test seasons: {test_seasons}")
    logger.info(f"Hyperparameter tuning: {args.tune}")
    if args.tune:
        logger.info(f"Random search iterations: {args.n_iter}")
    logger.info("=" * 70)
    
    # Initialize trainer
    trainer = ModelTrainer(random_state=args.random_state)
    
    # Determine which tasks to run
    tasks = []
    if args.task == 'both':
        tasks = ['classification', 'regression']
    else:
        tasks = [args.task]
    
    # Train models
    for task_type in tasks:
        logger.info(f"\n{'=' * 70}")
        logger.info(f"Training {task_type} model")
        logger.info(f"{'=' * 70}")
        
        # Generate model name
        if args.model_name:
            model_name = f"{args.model_name}_{task_type}"
        else:
            model_name = f"nba_{task_type}"
        
        try:
            if args.tune:
                # Hyperparameter tuning
                logger.info("Starting hyperparameter tuning...")
                
                param_distributions = {
                    'max_depth': [3, 5, 7, 9],
                    'learning_rate': [0.01, 0.05, 0.1, 0.2],
                    'n_estimators': [50, 100, 200, 300],
                    'subsample': [0.7, 0.8, 0.9, 1.0],
                    'colsample_bytree': [0.7, 0.8, 0.9, 1.0],
                    'min_child_weight': [1, 3, 5],
                    'gamma': [0, 0.1, 0.2],
                    'reg_alpha': [0, 0.1, 0.5],
                    'reg_lambda': [1, 1.5, 2]
                }
                
                # Load data for tuning
                data = trainer.data_loader.load_all_data(
                    train_seasons=train_seasons,
                    val_seasons=val_seasons,
                    test_seasons=test_seasons
                )
                
                # Select appropriate target
                if task_type == "classification":
                    y_train = data['y_train_class']
                    y_val = data['y_val_class']
                else:
                    y_train = data['y_train_reg']
                    y_val = data['y_val_reg']
                
                # Perform tuning
                best_model, tuning_results = trainer.hyperparameter_tuning(
                    XGBoostModel,
                    model_name,
                    param_distributions,
                    data['X_train'],
                    y_train,
                    X_val=data['X_val'],
                    y_val=y_val,
                    n_iter=args.n_iter,
                    task_type=task_type,
                    random_state=args.random_state,
                    verbosity=0
                )
                
                logger.info(f"Best parameters: {tuning_results['best_params']}")
                logger.info(f"Best score: {tuning_results['best_score']:.4f}")
                
                # Evaluate on test set
                if 'X_test' in data:
                    if task_type == "classification":
                        y_test = data['y_test_class']
                    else:
                        y_test = data['y_test_reg']
                    
                    test_results = trainer.train_model(
                        best_model,
                        data['X_train'],
                        y_train,
                        X_val=data['X_val'],
                        y_val=y_val,
                        X_test=data['X_test'],
                        y_test=y_test,
                        save_model=not args.no_save
                    )
                    
                    logger.info(f"Test results: {test_results.get('test_accuracy' if task_type == 'classification' else 'test_rmse', 'N/A')}")
            else:
                # Standard training
                model = XGBoostModel(
                    model_name,
                    task_type,
                    n_estimators=args.n_estimators,
                    max_depth=args.max_depth,
                    learning_rate=args.learning_rate,
                    random_state=args.random_state,
                    verbosity=0
                )
                
                results = trainer.train_with_data_loader(
                    model,
                    train_seasons=train_seasons,
                    val_seasons=val_seasons,
                    test_seasons=test_seasons,
                    save_model=not args.no_save
                )
                
                logger.info(f"Training completed successfully")
                if task_type == "classification":
                    logger.info(f"  Train accuracy: {results['training_metrics'].get('train_accuracy', 'N/A'):.3f}")
                    logger.info(f"  Val accuracy: {results['training_metrics'].get('val_accuracy', 'N/A'):.3f}")
                    logger.info(f"  Test accuracy: {results.get('test_accuracy', 'N/A'):.3f}")
                else:
                    logger.info(f"  Train RMSE: {results['training_metrics'].get('train_rmse', 'N/A'):.3f}")
                    logger.info(f"  Val RMSE: {results['training_metrics'].get('val_rmse', 'N/A'):.3f}")
                    logger.info(f"  Test RMSE: {results.get('test_rmse', 'N/A'):.3f}")
        
        except Exception as e:
            logger.error(f"Error training {task_type} model: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return 1
    
    # Print comparison
    logger.info(f"\n{'=' * 70}")
    logger.info("Model Comparison")
    logger.info(f"{'=' * 70}")
    trainer.print_comparison()
    
    # Save training summary
    summary_path = trainer.save_training_summary()
    logger.info(f"\nTraining summary saved to: {summary_path}")
    
    logger.info(f"\n{'=' * 70}")
    logger.info("Training Complete!")
    logger.info(f"{'=' * 70}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

