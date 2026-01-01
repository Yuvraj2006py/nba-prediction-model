"""Check feature generation status across games."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Game, Feature
from collections import defaultdict

def check_feature_status():
    """Check and display feature generation status."""
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Get all finished games
        total_games = session.query(Game).filter(
            Game.game_status == 'finished'
        ).count()
        
        # Get games with features
        games_with_features = session.query(Game.game_id).join(Feature).distinct().count()
        
        # Get feature counts by category
        feature_counts = session.query(
            Feature.feature_category,
            Feature.game_id
        ).distinct().all()
        
        category_stats = defaultdict(set)
        for category, game_id in feature_counts:
            category_stats[category].add(game_id)
        
        # Get feature counts by season
        season_stats = session.query(
            Game.season,
            Game.game_id
        ).join(Feature).distinct().all()
        
        season_counts = defaultdict(set)
        for season, game_id in season_stats:
            season_counts[season].add(game_id)
        
        # Get total finished games by season
        total_by_season = session.query(
            Game.season,
            Game.game_id
        ).filter(Game.game_status == 'finished').distinct().all()
        
        total_season_counts = defaultdict(set)
        for season, game_id in total_by_season:
            total_season_counts[season].add(game_id)
        
        print("=" * 70)
        print("FEATURE GENERATION STATUS")
        print("=" * 70)
        print(f"Total finished games: {total_games}")
        print(f"Games with features: {games_with_features}")
        print(f"Games missing features: {total_games - games_with_features}")
        if total_games > 0:
            coverage = (games_with_features / total_games) * 100
            print(f"Coverage: {coverage:.1f}%")
        else:
            print("Coverage: N/A (no finished games)")
        
        print("\nFeatures by category:")
        for category in sorted(category_stats.keys()):
            count = len(category_stats[category])
            print(f"  {category}: {count} games")
        
        print("\nCoverage by season:")
        for season in sorted(total_season_counts.keys()):
            total_season = len(total_season_counts[season])
            with_features = len(season_counts.get(season, set()))
            if total_season > 0:
                pct = (with_features / total_season) * 100
                print(f"  {season}: {with_features}/{total_season} ({pct:.1f}%)")
            else:
                print(f"  {season}: 0/0 (N/A)")
        
        print("=" * 70)

if __name__ == "__main__":
    check_feature_status()

