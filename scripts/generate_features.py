"""Generate features for all games in the database.

This script processes all games and creates feature vectors using the FeatureAggregator.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
import argparse
from datetime import date
from tqdm import tqdm

from src.database.db_manager import DatabaseManager
from src.database.models import Game
from src.features.feature_aggregator import FeatureAggregator

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/feature_generation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def generate_features_for_season(
    season: str,
    db_manager: DatabaseManager,
    replace_existing: bool = False,
    limit: int = None
) -> dict:
    """
    Generate features for all games in a season.
    
    Args:
        season: Season string (e.g., '2019-20')
        db_manager: Database manager
        replace_existing: If True, regenerate features even if they exist
        limit: Optional limit on number of games to process
        
    Returns:
        Dictionary with generation statistics
    """
    stats = {
        'games_processed': 0,
        'features_generated': 0,
        'games_skipped': 0,
        'errors': 0
    }
    
    logger.info("=" * 70)
    logger.info(f"Generating features for season: {season}")
    logger.info("=" * 70)
    
    # Initialize feature aggregator
    aggregator = FeatureAggregator(db_manager)
    
    # Get all finished games for the season
    with db_manager.get_session() as session:
        query = session.query(Game).filter(
            Game.season == season,
            Game.game_status == 'finished'
        ).order_by(Game.game_date)
        
        if limit:
            query = query.limit(limit)
        
        games = query.all()
    
    logger.info(f"Found {len(games)} finished games for season {season}")
    
    if not games:
        logger.warning(f"No finished games found for season {season}")
        return stats
    
    # Process games
    for game in tqdm(games, desc=f"Generating features {season}"):
        try:
            # Check if features already exist
            if not replace_existing:
                cached_features = aggregator.get_features_from_db(game.game_id)
                if cached_features is not None and not cached_features.empty:
                    stats['games_skipped'] += 1
                    continue
            
            # Generate features
            feature_df = aggregator.create_feature_vector(
                game_id=game.game_id,
                home_team_id=game.home_team_id,
                away_team_id=game.away_team_id,
                end_date=game.game_date,
                use_cache=False  # We'll save manually
            )
            
            if feature_df is not None and not feature_df.empty:
                # Save to database
                feature_dict = feature_df.iloc[0].to_dict()
                aggregator.save_features_to_db(game.game_id, feature_dict)
                stats['features_generated'] += 1
            else:
                logger.warning(f"Could not generate features for game {game.game_id}")
                stats['errors'] += 1
            
            stats['games_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error generating features for game {game.game_id}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            stats['errors'] += 1
            continue
    
    logger.info("\n" + "=" * 70)
    logger.info(f"Feature Generation Complete for {season}")
    logger.info("=" * 70)
    logger.info(f"Games processed: {stats['games_processed']}")
    logger.info(f"Features generated: {stats['features_generated']}")
    logger.info(f"Games skipped (already had features): {stats['games_skipped']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info("=" * 70)
    
    return stats


def generate_features_for_all_seasons(
    db_manager: DatabaseManager,
    replace_existing: bool = False,
    seasons: list = None
) -> dict:
    """
    Generate features for all seasons.
    
    Args:
        db_manager: Database manager
        replace_existing: If True, regenerate features even if they exist
        seasons: Optional list of seasons to process. If None, processes all.
        
    Returns:
        Dictionary with total statistics
    """
    logger.info("=" * 70)
    logger.info("Feature Generation for All Seasons")
    logger.info("=" * 70)
    
    # Get all unique seasons from database
    with db_manager.get_session() as session:
        if seasons:
            unique_seasons = seasons
        else:
            unique_seasons = session.query(Game.season).distinct().all()
            unique_seasons = [s[0] for s in unique_seasons if s[0]]
            unique_seasons.sort()
    
    logger.info(f"Seasons to process: {', '.join(unique_seasons)}")
    
    total_stats = {
        'games_processed': 0,
        'features_generated': 0,
        'games_skipped': 0,
        'errors': 0
    }
    
    # Process each season
    for season in unique_seasons:
        try:
            season_stats = generate_features_for_season(
                season, db_manager, replace_existing
            )
            
            # Aggregate stats
            for key in total_stats:
                total_stats[key] += season_stats[key]
        
        except Exception as e:
            logger.error(f"Error processing season {season}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            continue
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("All Seasons Feature Generation Complete")
    logger.info("=" * 70)
    logger.info(f"Total games processed: {total_stats['games_processed']}")
    logger.info(f"Total features generated: {total_stats['features_generated']}")
    logger.info(f"Total games skipped: {total_stats['games_skipped']}")
    logger.info(f"Total errors: {total_stats['errors']}")
    logger.info("=" * 70)
    
    return total_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Generate features for NBA games',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Generate features for all seasons
  python scripts/generate_features.py
  
  # Generate features for specific season
  python scripts/generate_features.py --season 2019-20
  
  # Replace existing features
  python scripts/generate_features.py --replace
  
  # Process only first 100 games (for testing)
  python scripts/generate_features.py --limit 100
  
  # Use historical database
  python scripts/generate_features.py --season 2019-20 --historical
        """
    )
    parser.add_argument('--season', type=str,
                       help='Season to process (e.g., 2019-20). If not specified, processes all seasons.')
    parser.add_argument('--replace', action='store_true',
                       help='Replace existing features (default: skip games with existing features)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of games to process (useful for testing)')
    parser.add_argument('--database', type=str,
                       help='Path to database file (default: uses settings)')
    parser.add_argument('--historical', action='store_true',
                       help='Use historical database (data/nba_predictions_historical.db)')
    
    args = parser.parse_args()
    
    try:
        # Initialize database manager
        if args.database:
            database_url = f"sqlite:///{args.database}"
            db_manager = DatabaseManager(database_url=database_url)
        elif args.historical:
            # Use historical database
            historical_db_path = Path(project_root) / "data" / "nba_predictions_historical.db"
            database_url = f"sqlite:///{historical_db_path}"
            db_manager = DatabaseManager(database_url=database_url)
            logger.info(f"Using historical database: {historical_db_path}")
        else:
            db_manager = DatabaseManager()
        
        db_manager.create_tables()
        
        if not db_manager.test_connection():
            logger.error("Database connection failed!")
            sys.exit(1)
        
        logger.info("[OK] Database connection successful")
        
        # Generate features
        if args.season:
            # Single season
            seasons = [args.season] if args.season else None
            generate_features_for_season(
                args.season,
                db_manager,
                replace_existing=args.replace,
                limit=args.limit
            )
        else:
            # All seasons
            seasons = [args.season] if args.season else None
            generate_features_for_all_seasons(
                db_manager,
                replace_existing=args.replace,
                seasons=seasons
            )
        
        sys.exit(0)
    
    except KeyboardInterrupt:
        logger.info("\n\nFeature generation interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
