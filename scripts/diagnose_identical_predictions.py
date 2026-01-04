#!/usr/bin/env python
"""
Diagnose why all predictions show identical confidence (57.5%).
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from datetime import date
from src.database.db_manager import DatabaseManager
from src.prediction.prediction_service import PredictionService
from src.database.models import Game, TeamRollingFeatures
import pandas as pd
import numpy as np

def diagnose():
    """Diagnose the identical predictions issue."""
    print("=" * 70)
    print("DIAGNOSING IDENTICAL PREDICTIONS")
    print("=" * 70)
    
    db = DatabaseManager()
    pred_service = PredictionService(db)
    today = date.today()
    
    # Get today's games
    with db.get_session() as session:
        games = session.query(Game).filter(Game.game_date == today).all()
    
    print(f"\nFound {len(games)} games for {today}\n")
    
    # Collect features for all games
    all_feature_dfs = []
    game_ids = []
    
    for game in games:
        features = pred_service.get_features_for_game(game.game_id)
        if features is not None:
            all_feature_dfs.append(features.iloc[0])
            game_ids.append(game.game_id)
    
    if len(all_feature_dfs) == 0:
        print("[ERROR] No features generated for any games!")
        return
    
    # Create DataFrame with all games' features
    features_df = pd.DataFrame(all_feature_dfs, index=game_ids)
    
    print(f"Generated features for {len(features_df)} games")
    print(f"Total features: {len(features_df.columns)}\n")
    
    # Check for identical features
    print("=" * 70)
    print("FEATURE ANALYSIS")
    print("=" * 70)
    
    # Find features that are identical across all games
    identical_features = []
    for col in features_df.columns:
        unique_vals = features_df[col].nunique()
        if unique_vals == 1:
            identical_features.append(col)
    
    print(f"\n[IDENTICAL FEATURES] {len(identical_features)}/{len(features_df.columns)} features are identical across all games")
    if len(identical_features) > 0:
        print(f"  Sample (first 20): {identical_features[:20]}")
    
    # Find features that differ
    different_features = [col for col in features_df.columns if col not in identical_features]
    print(f"\n[DIFFERENT FEATURES] {len(different_features)} features differ across games")
    
    if len(different_features) > 0:
        print(f"\n  Sample different features:")
        for col in different_features[:10]:
            unique_vals = features_df[col].nunique()
            min_val = features_df[col].min()
            max_val = features_df[col].max()
            print(f"    {col}: {unique_vals} unique values, range [{min_val:.2f}, {max_val:.2f}]")
    
    # Check key rolling stats
    print("\n" + "=" * 70)
    print("KEY ROLLING STATS COMPARISON")
    print("=" * 70)
    
    key_stats = ['home_l5_points', 'home_l10_points', 'home_l5_win_pct', 
                 'away_l5_points', 'away_l10_points', 'away_l5_win_pct']
    
    for stat in key_stats:
        if stat in features_df.columns:
            unique_vals = features_df[stat].nunique()
            if unique_vals == 1:
                val = features_df[stat].iloc[0]
                print(f"  {stat}: IDENTICAL = {val}")
            else:
                min_val = features_df[stat].min()
                max_val = features_df[stat].max()
                mean_val = features_df[stat].mean()
                print(f"  {stat}: {unique_vals} unique values, range [{min_val:.2f}, {max_val:.2f}], mean={mean_val:.2f}")
    
    # Check if features are all zeros or None
    print("\n" + "=" * 70)
    print("MISSING/ZERO VALUES")
    print("=" * 70)
    
    null_counts = features_df.isnull().sum()
    zero_counts = (features_df == 0).sum()
    
    print(f"\nFeatures with None values:")
    null_cols = null_counts[null_counts > 0]
    if len(null_cols) > 0:
        print(f"  {len(null_cols)} features have None values")
        print(f"  Sample: {dict(null_cols.head(10))}")
    else:
        print("  No None values")
    
    print(f"\nFeatures with 0 values (all games):")
    zero_cols = zero_counts[zero_counts == len(features_df)]
    if len(zero_cols) > 0:
        print(f"  {len(zero_cols)} features are 0 for all games")
        print(f"  Sample: {list(zero_cols.head(10))}")
    
    # Make predictions and check probabilities
    print("\n" + "=" * 70)
    print("PREDICTION PROBABILITIES")
    print("=" * 70)
    
    predictions = []
    for game in games:
        result = pred_service.predict_game(
            game.game_id,
            model_name='nba_v2_classifier',
            reg_model_name='nba_v2_regressor'
        )
        if result:
            predictions.append({
                'game_id': game.game_id,
                'home_prob': result['win_probability_home'],
                'away_prob': result['win_probability_away'],
                'confidence': result['confidence']
            })
    
    if predictions:
        pred_df = pd.DataFrame(predictions)
        print(f"\nHome win probabilities:")
        print(f"  Unique values: {pred_df['home_prob'].nunique()}")
        print(f"  Range: [{pred_df['home_prob'].min():.4f}, {pred_df['home_prob'].max():.4f}]")
        print(f"  Mean: {pred_df['home_prob'].mean():.4f}")
        print(f"  Std: {pred_df['home_prob'].std():.4f}")
        
        print(f"\nConfidence scores:")
        print(f"  Unique values: {pred_df['confidence'].nunique()}")
        print(f"  Range: [{pred_df['confidence'].min():.4f}, {pred_df['confidence'].max():.4f}]")
        print(f"  Values: {sorted(pred_df['confidence'].unique())}")
    
    # Check database features directly
    print("\n" + "=" * 70)
    print("DATABASE FEATURES CHECK")
    print("=" * 70)
    
    with db.get_session() as session:
        for i, game in enumerate(games[:3], 1):
            print(f"\nGame {i}: {game.game_id}")
            home_feat = session.query(TeamRollingFeatures).filter_by(
                game_id=game.game_id,
                team_id=game.home_team_id
            ).first()
            
            if home_feat:
                print(f"  Home team ({game.home_team_id}):")
                print(f"    l5_points: {home_feat.l5_points}")
                print(f"    l10_points: {home_feat.l10_points}")
                print(f"    l5_win_pct: {home_feat.l5_win_pct}")
            else:
                print(f"  Home team: No TeamRollingFeatures found")
            
            away_feat = session.query(TeamRollingFeatures).filter_by(
                game_id=game.game_id,
                team_id=game.away_team_id
            ).first()
            
            if away_feat:
                print(f"  Away team ({game.away_team_id}):")
                print(f"    l5_points: {away_feat.l5_points}")
                print(f"    l10_points: {away_feat.l10_points}")
                print(f"    l5_win_pct: {away_feat.l5_win_pct}")
            else:
                print(f"  Away team: No TeamRollingFeatures found")

if __name__ == "__main__":
    diagnose()

