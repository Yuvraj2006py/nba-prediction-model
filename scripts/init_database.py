"""Initialize the database with tables."""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from src.database.db_manager import DatabaseManager
from config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_database():
    """Initialize database tables."""
    logger.info("Initializing database...")
    
    settings = get_settings()
    db_manager = DatabaseManager()
    
    # Test connection
    if not db_manager.test_connection():
        logger.error("Failed to connect to database. Make sure Docker container is running.")
        logger.info("Start the database with: docker-compose up -d")
        return False
    
    # Create tables
    try:
        db_manager.create_tables()
        logger.info("Database tables created successfully!")
        return True
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        return False


if __name__ == "__main__":
    init_database()

