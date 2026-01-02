"""Investigate feature mismatch between model and TeamRollingFeatures."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import json
import pandas as pd
from src.database.db_manager import DatabaseManager
from src.database.models import TeamRollingFeatures, Game
from src.training.data_loader import DataLoader
from datetime import date

def main():
    print("=" * 70)
    print("FEATURE INVESTIGATION")
    print("=" * 70)
    
    # 1. Check what features TeamRollingFeatures has
    print("\n[1] TeamRollingFeatures columns:")
    exclude_cols = {
        'id', 'game_id', 'team_id', 'is_home', 'game_date', 'season',
        'created_at', 'updated_at', 'won_game', 'point_differential'
    }
    feature_columns = [
        col.name for col in TeamRollingFeatures.__table__.columns
        if col.name not in exclude_cols
    ]
    print(f"  Total feature columns per team: {len(feature_columns)}")
    print(f"  Expected total (home + away): {len(feature_columns) * 2}")
    print(f"  Feature columns: {feature_columns}")
    
    # 2. Check what the model expects
    print("\n[2] Model expectations:")
    model_path = Path("data/models/nba_test_fixed_classification.json")
    if model_path.exists():
        with open(model_path) as f:
            model_meta = json.load(f)
        print(f"  Model expects: {model_meta.get('n_features', 'unknown')} features")
        feature_names = model_meta.get('feature_names', [])
        if feature_names:
            print(f"  Model has {len(feature_names)} feature names stored")
            print(f"  First 20: {feature_names[:20]}")
            print(f"  Last 20: {feature_names[-20:]}")
        else:
            print("  No feature names stored in model metadata")
    else:
        print("  Model metadata not found")
    
    # 3. Check what DataLoader actually extracts
    print("\n[3] DataLoader extraction test:")
    db = DatabaseManager()
    with db.get_session() as session:
        # Get a finished game with rolling features
        game = session.query(Game).filter(
            Game.season == '2025-26',
            Game.game_status == 'finished'
        ).first()
        
        if game:
            print(f"  Testing with game: {game.game_id}")
            home_features = session.query(TeamRollingFeatures).filter_by(
                game_id=game.game_id,
                team_id=game.home_team_id
            ).first()
            
            away_features = session.query(TeamRollingFeatures).filter_by(
                game_id=game.game_id,
                team_id=game.away_team_id
            ).first()
            
            if home_features and away_features:
                loader = DataLoader()
                feature_dict = loader._extract_rolling_features(home_features, away_features)
                print(f"  Extracted features: {len(feature_dict)}")
                print(f"  Feature keys (first 20): {list(feature_dict.keys())[:20]}")
                print(f"  Feature keys (last 20): {list(feature_dict.keys())[-20:]}")
                
                # Create DataFrame to see what pandas does
                df = pd.DataFrame([feature_dict])
                print(f"  DataFrame columns: {len(df.columns)}")
                print(f"  DataFrame columns match dict keys: {len(df.columns) == len(feature_dict)}")
                
                # Check for None values that might be dropped
                none_count = sum(1 for v in feature_dict.values() if v is None)
                print(f"  None values in dict: {none_count}")
            else:
                print("  No rolling features found for this game")
        else:
            print("  No finished games found")
    
    # 4. Try loading actual training data
    print("\n[4] Actual training data check:")
    try:
        loader = DataLoader()
        data = loader.load_all_data(
            train_seasons=['2024-25'],
            val_seasons=[],
            test_seasons=[]
        )
        
        if 'X' in data and not data['X'].empty:
            print(f"  Training data features: {len(data['X'].columns)}")
            print(f"  First 20 columns: {list(data['X'].columns[:20])}")
            print(f"  Last 20 columns: {list(data['X'].columns[-20:])}")
            
            # Compare with what we extract
            print(f"\n  Comparison:")
            print(f"    TeamRollingFeatures (per team): {len(feature_columns)}")
            print(f"    Expected total (home + away): {len(feature_columns) * 2}")
            print(f"    Actual training data: {len(data['X'].columns)}")
            print(f"    Difference: {len(data['X'].columns) - len(feature_columns) * 2}")
            
            # Check if there are additional features beyond home/away
            expected_prefixes = ['home_', 'away_']
            all_home_away = all(
                col.startswith('home_') or col.startswith('away_')
                for col in data['X'].columns
            )
            print(f"    All columns are home_/away_ prefixed: {all_home_away}")
            
            if not all_home_away:
                other_cols = [
                    col for col in data['X'].columns
                    if not (col.startswith('home_') or col.startswith('away_'))
                ]
                print(f"    Non-home/away columns: {other_cols}")
        else:
            print("  No training data loaded")
    except Exception as e:
        print(f"  Error loading training data: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 70)

if __name__ == '__main__':
    main()


