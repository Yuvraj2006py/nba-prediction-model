"""Evaluate model performance on recent games."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
import logging
from datetime import date, timedelta
from typing import List, Optional
import pandas as pd
from src.database.db_manager import DatabaseManager
from src.database.models import Game, Prediction
from src.training.metrics import (
    calculate_classification_metrics,
    calculate_regression_metrics
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Evaluate model performance on recent games',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Evaluate on last 30 days
  python scripts/evaluate_model.py --model-name nba_classifier --days 30
  
  # Evaluate on specific date range
  python scripts/evaluate_model.py --model-name nba_classifier --start-date 2024-10-01 --end-date 2024-10-31
        """
    )
    
    parser.add_argument(
        '--model-name',
        type=str,
        required=True,
        help='Name of the model to evaluate'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=30,
        help='Number of days back to evaluate (default: 30)'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='Start date for evaluation (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date for evaluation (YYYY-MM-DD, default: today)'
    )
    parser.add_argument(
        '--min-confidence',
        type=float,
        default=0.0,
        help='Minimum confidence threshold for evaluation (default: 0.0)'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("Model Evaluation")
    logger.info("=" * 70)
    logger.info(f"Model: {args.model_name}")
    
    # Determine date range
    if args.start_date:
        start_date = date.fromisoformat(args.start_date)
    else:
        start_date = date.today() - timedelta(days=args.days)
    
    if args.end_date:
        end_date = date.fromisoformat(args.end_date)
    else:
        end_date = date.today()
    
    logger.info(f"Date range: {start_date} to {end_date}")
    logger.info(f"Min confidence: {args.min_confidence}")
    logger.info("=" * 70)
    
    # Get games with predictions and results
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Get finished games with predictions
        games = session.query(Game).join(Prediction).filter(
            Game.game_date >= start_date,
            Game.game_date <= end_date,
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Prediction.model_name == args.model_name
        ).all()
        
        logger.info(f"Found {len(games)} games with predictions and results")
        
        if len(games) == 0:
            logger.warning("No games found for evaluation")
            return 0
        
        # Collect predictions and actual results
        y_true_class = []
        y_pred_class = []
        y_proba_class = []
        y_true_reg = []
        y_pred_reg = []
        confidences = []
        
        for game in games:
            prediction = session.query(Prediction).filter_by(
                game_id=game.game_id,
                model_name=args.model_name
            ).first()
            
            if not prediction:
                continue
            
            # Skip if below confidence threshold
            if prediction.confidence < args.min_confidence:
                continue
            
            # Actual results
            if game.home_score is None or game.away_score is None:
                continue
            
            point_diff = game.home_score - game.away_score
            home_wins = 1 if point_diff > 0 else 0
            
            # Predictions
            pred_winner = prediction.predicted_winner
            if pred_winner == game.home_team_id:
                pred_class = 1
            elif pred_winner == game.away_team_id:
                pred_class = 0
            else:
                continue  # Skip if prediction doesn't match either team
            
            y_true_class.append(home_wins)
            y_pred_class.append(pred_class)
            y_proba_class.append([prediction.win_probability_away, prediction.win_probability_home])
            
            if prediction.predicted_point_differential is not None:
                y_true_reg.append(point_diff)
                y_pred_reg.append(prediction.predicted_point_differential)
            
            confidences.append(prediction.confidence)
        
        if len(y_true_class) == 0:
            logger.warning("No valid predictions found for evaluation")
            return 0
        
        logger.info(f"\nEvaluating {len(y_true_class)} predictions")
        
        # Classification metrics
        import numpy as np
        y_true_class = np.array(y_true_class)
        y_pred_class = np.array(y_pred_class)
        y_proba_class = np.array(y_proba_class)
        
        clf_metrics = calculate_classification_metrics(
            y_true_class, y_pred_class, y_proba_class, prefix="eval"
        )
        
        logger.info(f"\n{'=' * 70}")
        logger.info("Classification Metrics")
        logger.info(f"{'=' * 70}")
        logger.info(f"Accuracy: {clf_metrics.get('eval_accuracy', 0):.3f}")
        logger.info(f"Precision: {clf_metrics.get('eval_precision', 0):.3f}")
        logger.info(f"Recall: {clf_metrics.get('eval_recall', 0):.3f}")
        logger.info(f"F1 Score: {clf_metrics.get('eval_f1', 0):.3f}")
        if 'eval_roc_auc' in clf_metrics and clf_metrics['eval_roc_auc']:
            logger.info(f"ROC-AUC: {clf_metrics['eval_roc_auc']:.3f}")
        
        # Regression metrics (if available)
        if len(y_true_reg) > 0:
            y_true_reg = np.array(y_true_reg)
            y_pred_reg = np.array(y_pred_reg)
            
            reg_metrics = calculate_regression_metrics(
                y_true_reg, y_pred_reg, prefix="eval"
            )
            
            logger.info(f"\n{'=' * 70}")
            logger.info("Regression Metrics")
            logger.info(f"{'=' * 70}")
            logger.info(f"MAE: {reg_metrics.get('eval_mae', 0):.3f}")
            logger.info(f"RMSE: {reg_metrics.get('eval_rmse', 0):.3f}")
            logger.info(f"R²: {reg_metrics.get('eval_r2', 0):.3f}")
        
        # Confidence analysis
        logger.info(f"\n{'=' * 70}")
        logger.info("Confidence Analysis")
        logger.info(f"{'=' * 70}")
        confidences = np.array(confidences)
        logger.info(f"Mean confidence: {np.mean(confidences):.3f}")
        logger.info(f"Std confidence: {np.std(confidences):.3f}")
        logger.info(f"Min confidence: {np.min(confidences):.3f}")
        logger.info(f"Max confidence: {np.max(confidences):.3f}")
        
        # Accuracy by confidence bins
        bins = [0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
        logger.info(f"\nAccuracy by confidence bin:")
        for i in range(len(bins) - 1):
            mask = (confidences >= bins[i]) & (confidences < bins[i+1])
            if np.sum(mask) > 0:
                bin_accuracy = np.mean(y_true_class[mask] == y_pred_class[mask])
                logger.info(f"  {bins[i]:.1f}-{bins[i+1]:.1f}: {bin_accuracy:.3f} ({np.sum(mask)} games)")
        
        logger.info(f"\n{'=' * 70}")
        logger.info("Evaluation Complete")
        logger.info(f"{'=' * 70}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())



