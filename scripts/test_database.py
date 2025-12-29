"""Test database connection and basic operations."""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import date
from src.database.db_manager import DatabaseManager
from src.database.models import Team, Game

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_database():
    """Test database operations."""
    logger.info("Testing database connection...")
    
    db_manager = DatabaseManager()
    
    # Test connection
    if not db_manager.test_connection():
        logger.error("Database connection failed!")
        return False
    
    logger.info("✓ Database connection successful")
    
    # Test inserting a team
    try:
        test_team = {
            'team_id': '1610612737',
            'team_name': 'Atlanta Hawks',
            'team_abbreviation': 'ATL',
            'city': 'Atlanta',
            'conference': 'Eastern',
            'division': 'Southeast'
        }
        team = db_manager.insert_team(test_team)
        logger.info(f"✓ Inserted test team: {test_team['team_name']}")
        
        # Test retrieving team
        retrieved_team = db_manager.get_team('1610612737')
        if retrieved_team:
            logger.info(f"✓ Retrieved team: {retrieved_team.team_name}")
        
        # Test inserting a game
        test_game = {
            'game_id': '0022300123',
            'season': '2023-24',
            'season_type': 'Regular Season',
            'game_date': date(2023, 10, 24),
            'home_team_id': '1610612737',
            'away_team_id': '1610612738',
            'game_status': 'finished',
            'home_score': 110,
            'away_score': 108,
            'winner': '1610612737',
            'point_differential': 2
        }
        game = db_manager.insert_game(test_game)
        logger.info(f"✓ Inserted test game: {test_game['game_id']}")
        
        # Test querying games
        games = db_manager.get_games(season='2023-24', limit=5)
        logger.info(f"✓ Retrieved {len(games)} games")
        
        logger.info("\n✓ All database tests passed!")
        return True
        
    except Exception as e:
        logger.error(f"✗ Database test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    test_database()

