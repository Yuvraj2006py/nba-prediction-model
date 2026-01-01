"""Clear all data from the database (except teams)."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats, BettingLine, Feature, Prediction, Bet

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def clear_database():
    """Clear all data from database except teams."""
    logger.info("=" * 70)
    logger.info("Clearing Database")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Count records before deletion
        games_count = session.query(Game).count()
        team_stats_count = session.query(TeamStats).count()
        player_stats_count = session.query(PlayerStats).count()
        betting_lines_count = session.query(BettingLine).count()
        features_count = session.query(Feature).count()
        predictions_count = session.query(Prediction).count()
        bets_count = session.query(Bet).count()
        
        logger.info(f"\nRecords to delete:")
        logger.info(f"  Games: {games_count}")
        logger.info(f"  Team Stats: {team_stats_count}")
        logger.info(f"  Player Stats: {player_stats_count}")
        logger.info(f"  Betting Lines: {betting_lines_count}")
        logger.info(f"  Features: {features_count}")
        logger.info(f"  Predictions: {predictions_count}")
        logger.info(f"  Bets: {bets_count}")
        
        # Delete in order (respecting foreign keys)
        logger.info("\nDeleting records...")
        session.query(Bet).delete()
        session.query(Prediction).delete()
        session.query(Feature).delete()
        session.query(BettingLine).delete()
        session.query(PlayerStats).delete()
        session.query(TeamStats).delete()
        session.query(Game).delete()
        
        session.commit()
        
        logger.info("\n✓ Database cleared successfully!")
        logger.info("=" * 70)

if __name__ == "__main__":
    clear_database()


