"""Delete all data for a specific season from the database."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import argparse
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats, Feature, Prediction, BettingLine

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/delete_season.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def delete_season_data(season: str, db_manager: DatabaseManager, confirm: bool = False):
    """
    Delete all data for a specific season.
    
    Args:
        season: Season string (e.g., '2023-24')
        db_manager: Database manager
        confirm: If False, will ask for confirmation
    """
    logger.info("=" * 70)
    logger.info(f"Deleting all data for season: {season}")
    logger.info("=" * 70)
    
    if not confirm:
        response = input(f"\nWARNING: This will delete ALL data for season {season}.\n"
                        f"This includes games, team stats, player stats, features, predictions, and betting lines.\n"
                        f"This action CANNOT be undone!\n\n"
                        f"Type 'DELETE {season}' to confirm: ")
        
        if response != f'DELETE {season}':
            logger.info("Deletion cancelled.")
            return False
    
    stats = {
        'games': 0,
        'team_stats': 0,
        'player_stats': 0,
        'features': 0,
        'predictions': 0,
        'betting_lines': 0
    }
    
    try:
        with db_manager.get_session() as session:
            # Get all game IDs for this season first
            games = session.query(Game).filter(Game.season == season).all()
            game_ids = [g.game_id for g in games]
            stats['games'] = len(game_ids)
            
            logger.info(f"Found {stats['games']} games for season {season}")
            
            if not game_ids:
                logger.info("No games found for this season. Nothing to delete.")
                return True
            
            # Delete in order (respecting foreign key constraints)
            # 1. Delete betting lines
            logger.info("Deleting betting lines...")
            betting_lines = session.query(BettingLine).filter(BettingLine.game_id.in_(game_ids)).all()
            stats['betting_lines'] = len(betting_lines)
            for line in betting_lines:
                session.delete(line)
            
            # 2. Delete predictions
            logger.info("Deleting predictions...")
            predictions = session.query(Prediction).filter(Prediction.game_id.in_(game_ids)).all()
            stats['predictions'] = len(predictions)
            for pred in predictions:
                session.delete(pred)
            
            # 3. Delete features
            logger.info("Deleting features...")
            features = session.query(Feature).filter(Feature.game_id.in_(game_ids)).all()
            stats['features'] = len(features)
            for feature in features:
                session.delete(feature)
            
            # 4. Delete player stats
            logger.info("Deleting player stats...")
            player_stats = session.query(PlayerStats).filter(PlayerStats.game_id.in_(game_ids)).all()
            stats['player_stats'] = len(player_stats)
            for stat in player_stats:
                session.delete(stat)
            
            # 5. Delete team stats
            logger.info("Deleting team stats...")
            team_stats = session.query(TeamStats).filter(TeamStats.game_id.in_(game_ids)).all()
            stats['team_stats'] = len(team_stats)
            for stat in team_stats:
                session.delete(stat)
            
            # 6. Delete games (last, as other tables reference it)
            logger.info("Deleting games...")
            for game in games:
                session.delete(game)
            
            # Commit all deletions
            session.commit()
            
            logger.info("\n" + "=" * 70)
            logger.info("Deletion Complete")
            logger.info("=" * 70)
            logger.info(f"Games deleted: {stats['games']}")
            logger.info(f"Team stats deleted: {stats['team_stats']}")
            logger.info(f"Player stats deleted: {stats['player_stats']}")
            logger.info(f"Features deleted: {stats['features']}")
            logger.info(f"Predictions deleted: {stats['predictions']}")
            logger.info(f"Betting lines deleted: {stats['betting_lines']}")
            logger.info("=" * 70)
            
            return True
    
    except Exception as e:
        logger.error(f"Error deleting season data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Delete all data for a specific season',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Delete 2023-24 season data (will ask for confirmation)
  python scripts/delete_season_data.py --season 2023-24
  
  # Delete without confirmation prompt
  python scripts/delete_season_data.py --season 2023-24 --confirm
        """
    )
    parser.add_argument('--season', type=str, required=True,
                       help='Season to delete (e.g., 2023-24)')
    parser.add_argument('--confirm', action='store_true',
                       help='Skip confirmation prompt (use with caution!)')
    parser.add_argument('--database', type=str,
                       help='Path to database file (default: uses settings)')
    
    args = parser.parse_args()
    
    try:
        # Initialize database manager
        if args.database:
            database_url = f"sqlite:///{args.database}"
            db_manager = DatabaseManager(database_url=database_url)
        else:
            db_manager = DatabaseManager()
        
        if not db_manager.test_connection():
            logger.error("Database connection failed!")
            sys.exit(1)
        
        success = delete_season_data(args.season, db_manager, confirm=args.confirm)
        
        if success:
            sys.exit(0)
        else:
            sys.exit(1)
    
    except KeyboardInterrupt:
        logger.info("\n\nDeletion cancelled by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
