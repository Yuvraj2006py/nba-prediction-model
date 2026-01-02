"""
Investigate why model accuracy is suspiciously high (99.9%).

Checks for:
1. Data leakage (temporal or target leakage)
2. Feature importance analysis
3. Test set performance
4. Feature correlations with target
5. Temporal constraint violations
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
import pandas as pd
import numpy as np
from datetime import date
import json

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamRollingFeatures
from src.models.xgboost_model import XGBoostModel
from src.training.data_loader import DataLoader
from src.prediction.prediction_service import PredictionService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_feature_importance():
    """Analyze feature importance to see what's driving predictions."""
    logger.info("=" * 70)
    logger.info("FEATURE IMPORTANCE ANALYSIS")
    logger.info("=" * 70)
    
    model_path = Path('data/models/nba_v2_classifier.pkl')
    if not model_path.exists():
        logger.error("Model not found!")
        return None
    
    model = XGBoostModel('nba_v2_classifier', task_type='classification')
    model.load(model_path)
    
    if not hasattr(model.model, 'feature_importances_'):
        logger.error("Model doesn't have feature_importances_")
        return None
    
    importances = model.model.feature_importances_
    feature_names = model.feature_names if model.feature_names else [f'f{i}' for i in range(len(importances))]
    
    # Create DataFrame
    importance_df = pd.DataFrame({
        'feature': feature_names,
        'importance': importances
    }).sort_values('importance', ascending=False)
    
    logger.info(f"\nTop 20 Most Important Features:")
    logger.info("-" * 70)
    for idx, row in importance_df.head(20).iterrows():
        logger.info(f"  {row['feature']:40s} {row['importance']:.6f}")
    
    # Check for suspicious features
    suspicious_keywords = ['win', 'won', 'differential', 'score', 'result', 'outcome']
    suspicious_features = []
    for feature in importance_df['feature'].head(30):
        if any(keyword in feature.lower() for keyword in suspicious_keywords):
            suspicious_features.append(feature)
    
    if suspicious_features:
        logger.warning(f"\n⚠️  Suspicious features in top 30:")
        for feat in suspicious_features:
            imp = importance_df[importance_df['feature'] == feat]['importance'].values[0]
            logger.warning(f"  {feat}: {imp:.6f}")
    
    return importance_df


def check_temporal_constraints():
    """Verify that features only use past data (no future leakage)."""
    logger.info("\n" + "=" * 70)
    logger.info("TEMPORAL CONSTRAINT VERIFICATION")
    logger.info("=" * 70)
    
    db = DatabaseManager()
    
    # Get a sample game
    with db.get_session() as session:
        game = session.query(Game).filter(
            Game.season == '2025-26',
            Game.game_status == 'finished'
        ).order_by(Game.game_date).first()
        
        if not game:
            logger.error("No test games found")
            return
        
        logger.info(f"Checking game: {game.game_id} on {game.game_date}")
        
        # Get rolling features for this game
        home_features = session.query(TeamRollingFeatures).filter_by(
            game_id=game.game_id,
            team_id=game.home_team_id
        ).first()
        
        if not home_features:
            logger.error("No rolling features found")
            return
        
        # Check: Are there any games AFTER this game that might have been included?
        later_games = session.query(Game).filter(
            Game.home_team_id == game.home_team_id,
            Game.game_date > game.game_date,
            Game.game_status == 'finished'
        ).count()
        
        logger.info(f"  Games after this game for home team: {later_games}")
        
        # Check rolling averages - they should only include games BEFORE this game
        # This is a sanity check - actual verification would need to check transform_features.py logic
        logger.info(f"  Home team l5_win_pct: {home_features.l5_win_pct}")
        logger.info(f"  Home team l10_win_pct: {home_features.l10_win_pct}")
        logger.info(f"  Home team l20_win_pct: {home_features.l20_win_pct}")
        
        logger.info("\n  ⚠️  Note: Full temporal verification requires checking transform_features.py")
        logger.info("  to ensure rolling averages only use games with game_date < current_game_date")


def evaluate_test_set():
    """Evaluate model on test set (2025-26) that wasn't used in training."""
    logger.info("\n" + "=" * 70)
    logger.info("TEST SET EVALUATION (2025-26)")
    logger.info("=" * 70)
    
    db = DatabaseManager()
    loader = DataLoader(db)
    
    # Load test data
    test_data = loader._load_season_data(['2025-26'], min_features=40, split_name="Test")
    
    if test_data['X'].empty:
        logger.error("No test data available")
        return None
    
    logger.info(f"Test set size: {len(test_data['X'])} games")
    
    # Load model
    model = XGBoostModel('nba_v2_classifier', task_type='classification')
    model.load('data/models/nba_v2_classifier.pkl')
    
    # Make predictions
    X_test = test_data['X']
    y_test = test_data['y_class']
    
    # Ensure feature alignment
    if model.feature_names:
        # Reorder to match model
        X_test = X_test[model.feature_names]
    
    predictions = model.model.predict(X_test.values)
    probabilities = model.model.predict_proba(X_test.values)
    
    # Calculate metrics
    from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
    
    accuracy = accuracy_score(y_test, predictions)
    precision = precision_score(y_test, predictions, zero_division=0)
    recall = recall_score(y_test, predictions, zero_division=0)
    f1 = f1_score(y_test, predictions, zero_division=0)
    
    cm = confusion_matrix(y_test, predictions)
    
    logger.info(f"\nTest Set Metrics:")
    logger.info(f"  Accuracy:  {accuracy:.4f} ({accuracy*100:.2f}%)")
    logger.info(f"  Precision: {precision:.4f}")
    logger.info(f"  Recall:    {recall:.4f}")
    logger.info(f"  F1 Score:  {f1:.4f}")
    
    logger.info(f"\nConfusion Matrix:")
    logger.info(f"  True Negatives (Away wins predicted correctly):  {cm[0,0]}")
    logger.info(f"  False Positives (Home predicted, Away won):     {cm[0,1]}")
    logger.info(f"  False Negatives (Away predicted, Home won):     {cm[1,0]}")
    logger.info(f"  True Positives (Home wins predicted correctly):  {cm[1,1]}")
    
    # Check probability distribution
    home_probs = probabilities[:, 1] if probabilities.shape[1] > 1 else probabilities[:, 0]
    logger.info(f"\nProbability Distribution:")
    logger.info(f"  Mean home win prob: {home_probs.mean():.4f}")
    logger.info(f"  Std home win prob:  {home_probs.std():.4f}")
    logger.info(f"  Min home win prob:  {home_probs.min():.4f}")
    logger.info(f"  Max home win prob:  {home_probs.max():.4f}")
    
    # Check for overconfidence
    very_confident = (home_probs > 0.9) | (home_probs < 0.1)
    logger.info(f"  Very confident predictions (>90% or <10%): {very_confident.sum()} ({very_confident.sum()/len(home_probs)*100:.1f}%)")
    
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'confusion_matrix': cm,
        'probabilities': home_probs
    }


def check_feature_correlations():
    """Check correlations between features and target variable."""
    logger.info("\n" + "=" * 70)
    logger.info("FEATURE-TARGET CORRELATION ANALYSIS")
    logger.info("=" * 70)
    
    db = DatabaseManager()
    loader = DataLoader(db)
    
    # Load training data
    data = loader.load_all_data(
        train_seasons=['2022-23', '2023-24'],
        val_seasons=['2024-25'],
        test_seasons=['2025-26'],
        min_features=40
    )
    
    if data['X_train'].empty:
        logger.error("No training data available")
        return None
    
    X_train = data['X_train']
    y_train = data['y_train_class']
    
    # Calculate correlations
    correlations = []
    for col in X_train.columns:
        corr = X_train[col].corr(y_train)
        if not np.isnan(corr):
            correlations.append({
                'feature': col,
                'correlation': abs(corr),
                'correlation_signed': corr
            })
    
    corr_df = pd.DataFrame(correlations).sort_values('correlation', ascending=False)
    
    logger.info(f"\nTop 20 Features by Absolute Correlation with Target:")
    logger.info("-" * 70)
    for idx, row in corr_df.head(20).iterrows():
        logger.info(f"  {row['feature']:40s} {row['correlation_signed']:+.4f}")
    
    # Check for suspiciously high correlations
    high_corr = corr_df[corr_df['correlation'] > 0.8]
    if not high_corr.empty:
        logger.warning(f"\n⚠️  Features with very high correlation (>0.8) with target:")
        for idx, row in high_corr.iterrows():
            logger.warning(f"  {row['feature']}: {row['correlation_signed']:.4f}")
    
    return corr_df


def check_data_leakage_indicators():
    """Check for common data leakage patterns."""
    logger.info("\n" + "=" * 70)
    logger.info("DATA LEAKAGE INDICATORS")
    logger.info("=" * 70)
    
    db = DatabaseManager()
    loader = DataLoader(db)
    
    # Load data
    data = loader.load_all_data(
        train_seasons=['2022-23', '2023-24'],
        val_seasons=['2024-25'],
        test_seasons=['2025-26'],
        min_features=40
    )
    
    X_train = data['X_train']
    y_train = data['y_train_class']
    
    # Check 1: Perfect or near-perfect features
    logger.info("\n1. Checking for perfect/near-perfect predictors:")
    perfect_features = []
    for col in X_train.columns:
        # Check if feature perfectly predicts target
        if X_train[col].nunique() == 2:  # Binary feature
            grouped = pd.DataFrame({'feature': X_train[col], 'target': y_train}).groupby('feature')['target'].mean()
            if len(grouped) == 2:
                if abs(grouped.iloc[0] - grouped.iloc[1]) > 0.95:
                    perfect_features.append(col)
                    logger.warning(f"  ⚠️  {col}: Very strong separation ({grouped.iloc[0]:.3f} vs {grouped.iloc[1]:.3f})")
    
    # Check 2: Features that might contain target information
    logger.info("\n2. Checking for suspicious feature names:")
    suspicious_patterns = ['won', 'win', 'result', 'outcome', 'score', 'differential', 'final']
    suspicious_features = []
    for col in X_train.columns:
        if any(pattern in col.lower() for pattern in suspicious_patterns):
            suspicious_features.append(col)
    
    if suspicious_features:
        logger.warning(f"  Found {len(suspicious_features)} features with suspicious names:")
        for feat in suspicious_features[:10]:
            logger.warning(f"    - {feat}")
    
    # Check 3: Check if any features are identical to target
    logger.info("\n3. Checking for features identical to target:")
    for col in X_train.columns:
        if X_train[col].dtype in [int, bool]:
            if X_train[col].equals(y_train):
                logger.error(f"  ❌ {col} is IDENTICAL to target variable!")
            elif (X_train[col] == y_train).sum() / len(y_train) > 0.95:
                logger.warning(f"  ⚠️  {col} matches target in {(X_train[col] == y_train).sum()/len(y_train)*100:.1f}% of cases")


def main():
    """Run all investigations."""
    logger.info("=" * 70)
    logger.info("INVESTIGATING HIGH MODEL ACCURACY (99.9%)")
    logger.info("=" * 70)
    
    results = {}
    
    # 1. Feature importance
    results['importance'] = check_feature_importance()
    
    # 2. Temporal constraints
    check_temporal_constraints()
    
    # 3. Test set evaluation
    results['test_metrics'] = evaluate_test_set()
    
    # 4. Feature correlations
    results['correlations'] = check_feature_correlations()
    
    # 5. Data leakage indicators
    check_data_leakage_indicators()
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("INVESTIGATION SUMMARY")
    logger.info("=" * 70)
    
    if results.get('test_metrics'):
        test_acc = results['test_metrics']['accuracy']
        logger.info(f"\nTest Set Accuracy: {test_acc:.4f} ({test_acc*100:.2f}%)")
        
        if test_acc < 0.60:
            logger.warning("⚠️  Test accuracy is much lower than training accuracy - indicates overfitting")
        elif test_acc > 0.95:
            logger.warning("⚠️  Test accuracy is suspiciously high - possible data leakage")
        else:
            logger.info("✓ Test accuracy seems reasonable")
    
    logger.info("\n" + "=" * 70)
    
    return results


if __name__ == '__main__':
    main()

