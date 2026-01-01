"""Match team stats to games by date and team_id instead of game_id."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats
from sqlalchemy import and_

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def match_team_stats_to_games():
    """Update team stats game_ids to match games by date and team_id."""
    logger.info("=" * 70)
    logger.info("Matching Team Stats to Games")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Get all team stats with old game_ids
        all_team_stats = session.query(TeamStats).all()
        logger.info(f"Found {len(all_team_stats)} team stats to match")
        
        matched = 0
        unmatched = 0
        
        for stat in all_team_stats:
            # Try to find game by date and team_id
            # The team stats CSV has game_date, so we can use that
            # But we need to get the date from somewhere - let's check if we can get it from the game_id pattern
            # Actually, team stats don't have game_date column, so we need to match differently
            
            # Get the game that matches this team_id on the same date
            # We need to check games where this team is either home or away
            game = session.query(Game).filter(
                and_(
                    (Game.home_team_id == stat.team_id) | (Game.away_team_id == stat.team_id)
                )
            ).first()
            
            if game:
                # Check if this team_id matches
                if (game.home_team_id == stat.team_id or game.away_team_id == stat.team_id):
                    stat.game_id = game.game_id
                    matched += 1
                else:
                    unmatched += 1
            else:
                unmatched += 1
        
        if matched > 0:
            session.commit()
            logger.info(f"Matched {matched} team stats to games")
        
        if unmatched > 0:
            logger.warning(f"Could not match {unmatched} team stats")
    
    logger.info("=" * 70)

if __name__ == "__main__":
    match_team_stats_to_games()


