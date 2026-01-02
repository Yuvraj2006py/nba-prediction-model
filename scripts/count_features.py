"""Count features from different systems."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from datetime import date
from src.database.db_manager import DatabaseManager
from src.features.feature_aggregator import FeatureAggregator
from src.database.models import Game

def main():
    print("=" * 70)
    print("FEATURE COUNT COMPARISON")
    print("=" * 70)
    
    db = DatabaseManager()
    feature_agg = FeatureAggregator(db)
    
    # Get a game to test with
    with db.get_session() as session:
        game = session.query(Game).filter(
            Game.season == '2025-26',
            Game.game_status == 'finished'
        ).first()
        
        if not game:
            print("No finished games found")
            return
        
        print(f"\nTesting with game: {game.game_id}")
        print(f"  Home: {game.home_team_id}, Away: {game.away_team_id}")
        
        # Generate features using FeatureAggregator (old system)
        print("\n[1] FeatureAggregator (old system, without betting features):")
        features_old = feature_agg.create_feature_vector(
            game_id=game.game_id,
            home_team_id=game.home_team_id,
            away_team_id=game.away_team_id,
            end_date=game.game_date,
            include_betting_features=False
        )
        
        print(f"  Total features: {len(features_old.columns)}")
        print(f"  Feature names:")
        for i, col in enumerate(features_old.columns):
            prefix = "  " if i < 20 or i >= len(features_old.columns) - 5 else ""
            if i < 20 or i >= len(features_old.columns) - 5:
                print(f"    {i+1:3d}. {col}")
        
        # Categorize features
        team_features = [c for c in features_old.columns if c.startswith('home_') or c.startswith('away_')]
        matchup_features = [c for c in features_old.columns if 'h2h' in c.lower() or 'differential' in c.lower() or 'matchup' in c.lower()]
        contextual_features = [c for c in features_old.columns if c in ['days_rest_home', 'days_rest_away', 'is_back_to_back_home', 'is_back_to_back_away', 'same_conference', 'same_division', 'is_playoffs']]
        other_features = [c for c in features_old.columns if c not in team_features and c not in matchup_features and c not in contextual_features]
        
        print(f"\n  Feature breakdown:")
        print(f"    Team features (home_/away_): {len(team_features)}")
        print(f"    Matchup features: {len(matchup_features)}")
        print(f"    Contextual features: {len(contextual_features)}")
        print(f"    Other features: {len(other_features)}")
        
        if other_features:
            print(f"    Other feature names: {other_features}")
        
        print(f"\n[2] TeamRollingFeatures (new system):")
        print(f"  Total features: 78 (39 per team × 2)")
        print(f"  Missing from new system: {len(features_old.columns) - 78}")
        
        print(f"\n[3] Missing features breakdown:")
        print(f"    Matchup features: ~{len(matchup_features)}")
        print(f"    Contextual features: ~{len(contextual_features)}")
        print(f"    Other: ~{len(other_features)}")
        print(f"    Total missing: {len(matchup_features) + len(contextual_features) + len(other_features)}")
        
        print("\n" + "=" * 70)

if __name__ == '__main__':
    main()


