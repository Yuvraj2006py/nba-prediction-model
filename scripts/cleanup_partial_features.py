"""Clean up games with partial features (some but not all features saved)."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Game, Feature
from collections import defaultdict

def find_partial_features():
    """Find games with partial features."""
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Get all finished games
        finished_games = session.query(Game).filter(
            Game.game_status == 'finished'
        ).all()
        
        # Expected number of features per game (approximate)
        # Team: ~20, Matchup: ~8, Contextual: ~10, Betting: ~9 = ~47 features
        expected_min_features = 40  # Minimum to consider "complete"
        
        partial_games = []
        complete_games = []
        missing_games = []
        
        for game in finished_games:
            features = session.query(Feature).filter_by(game_id=game.game_id).all()
            feature_count = len(features)
            
            if feature_count == 0:
                missing_games.append(game.game_id)
            elif feature_count < expected_min_features:
                # Check categories
                categories = set(f.feature_category for f in features)
                partial_games.append({
                    'game_id': game.game_id,
                    'count': feature_count,
                    'categories': categories,
                    'season': game.season
                })
            else:
                complete_games.append(game.game_id)
        
        return {
            'partial': partial_games,
            'complete': complete_games,
            'missing': missing_games
        }

def cleanup_partial_features(dry_run=True):
    """Remove partial features for games that need regeneration."""
    results = find_partial_features()
    
    print("=" * 70)
    print("PARTIAL FEATURES ANALYSIS")
    print("=" * 70)
    print(f"Complete games: {len(results['complete'])}")
    print(f"Games with partial features: {len(results['partial'])}")
    print(f"Games with no features: {len(results['missing'])}")
    
    if results['partial']:
        print("\nGames with partial features:")
        for game in results['partial'][:10]:  # Show first 10
            print(f"  {game['game_id']}: {game['count']} features, categories: {game['categories']}")
        if len(results['partial']) > 10:
            print(f"  ... and {len(results['partial']) - 10} more")
    
    if not dry_run and results['partial']:
        db_manager = DatabaseManager()
        print("\nCleaning up partial features...")
        
        with db_manager.get_session() as session:
            for game_info in results['partial']:
                deleted = session.query(Feature).filter_by(
                    game_id=game_info['game_id']
                ).delete()
                session.commit()
                print(f"Deleted {deleted} partial features for game {game_info['game_id']}")
        
        print("Cleanup complete!")
    elif dry_run:
        print("\n[DRY RUN] Use --execute to actually delete partial features")
    
    print("=" * 70)
    
    return results

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean up partial features')
    parser.add_argument('--execute', action='store_true',
                       help='Actually delete partial features (default: dry run)')
    
    args = parser.parse_args()
    
    cleanup_partial_features(dry_run=not args.execute)

