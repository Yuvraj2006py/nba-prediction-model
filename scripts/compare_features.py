"""Compare generated features with model expectations."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamRollingFeatures, GameMatchupFeatures
from src.features.feature_aggregator import FeatureAggregator
from datetime import date

def main():
    print("=" * 70)
    print("FEATURE COMPARISON")
    print("=" * 70)
    
    db = DatabaseManager()
    feature_agg = FeatureAggregator(db)
    
    # Get a finished game
    with db.get_session() as session:
        game = session.query(Game).filter(
            Game.season == '2025-26',
            Game.game_status == 'finished'
        ).first()
        
        if not game:
            print("No finished games found")
            return
        
        print(f"\nTesting with game: {game.game_id}")
        
        # Method 1: Old FeatureAggregator (what model was trained with)
        print("\n[1] Old FeatureAggregator (63 features, no betting):")
        features_old = feature_agg.create_feature_vector(
            game_id=game.game_id,
            home_team_id=game.home_team_id,
            away_team_id=game.away_team_id,
            end_date=game.game_date,
            include_betting_features=False
        )
        old_features_set = set(features_old.columns)
        print(f"  Total: {len(old_features_set)} features")
        
        # Method 2: New system (TeamRollingFeatures + GameMatchupFeatures)
        print("\n[2] New system (TeamRollingFeatures + GameMatchupFeatures):")
        home_features = session.query(TeamRollingFeatures).filter_by(
            game_id=game.game_id,
            team_id=game.home_team_id
        ).first()
        
        away_features = session.query(TeamRollingFeatures).filter_by(
            game_id=game.game_id,
            team_id=game.away_team_id
        ).first()
        
        matchup_features = session.query(GameMatchupFeatures).filter_by(
            game_id=game.game_id
        ).first()
        
        if home_features and away_features and matchup_features:
            # Extract features
            from src.training.data_loader import DataLoader
            loader = DataLoader()
            
            team_dict = loader._extract_rolling_features(home_features, away_features)
            matchup_dict = loader._extract_matchup_features(matchup_features)
            
            new_features_dict = {**team_dict, **matchup_dict}
            new_features_set = set(new_features_dict.keys())
            print(f"  Total: {len(new_features_set)} features")
            print(f"    Team features: {len(team_dict)}")
            print(f"    Matchup features: {len(matchup_dict)}")
        else:
            print("  Missing features in database")
            return
        
        # Compare
        print("\n[3] Comparison:")
        print(f"  Old system: {len(old_features_set)} features")
        print(f"  New system: {len(new_features_set)} features")
        print(f"  Model expects: 133 features")
        
        # Find missing features
        missing_in_new = old_features_set - new_features_set
        extra_in_new = new_features_set - old_features_set
        
        if missing_in_new:
            print(f"\n  Missing in new system ({len(missing_in_new)}):")
            for feat in sorted(missing_in_new):
                print(f"    - {feat}")
        
        if extra_in_new:
            print(f"\n  Extra in new system ({len(extra_in_new)}):")
            for feat in sorted(extra_in_new):
                print(f"    + {feat}")
        
        # Check feature name differences
        print("\n[4] Feature name analysis:")
        print("  Checking for naming differences...")
        
        # Map old feature names to new
        name_mapping = {}
        for old_feat in old_features_set:
            # Check if it exists with different name
            found = False
            for new_feat in new_features_set:
                if old_feat.replace('home_', '').replace('away_', '') in new_feat or \
                   new_feat.replace('home_', '').replace('away_', '') in old_feat:
                    name_mapping[old_feat] = new_feat
                    found = True
                    break
            if not found:
                name_mapping[old_feat] = None
        
        unmapped = [k for k, v in name_mapping.items() if v is None]
        if unmapped:
            print(f"  Unmapped features ({len(unmapped)}):")
            for feat in sorted(unmapped):
                print(f"    {feat}")
        
        print("\n" + "=" * 70)

if __name__ == '__main__':
    main()



