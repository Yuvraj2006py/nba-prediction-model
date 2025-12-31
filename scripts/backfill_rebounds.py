"""Backfill rebound data for existing games using NBA API."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from tqdm import tqdm

from src.data_collectors.nba_api_collector import NBAPICollector
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def backfill_rebounds(season: str = None, limit: int = None):
    """
    Backfill rebound data for games with zero rebounds.
    
    Args:
        season: Season to backfill (e.g., '2022-23'). If None, backfills all seasons.
        limit: Maximum number of games to process (for testing). If None, processes all.
    """
    logger.info("=" * 70)
    logger.info("Rebound Data Backfill")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    collector = NBAPICollector(db_manager)
    
    if not db_manager.test_connection():
        logger.error("Database connection failed!")
        return False
    
    # Find games with zero rebounds
    with db_manager.get_session() as session:
        query = session.query(TeamStats).filter(
            TeamStats.rebounds_total == 0,
            TeamStats.rebounds_offensive == 0,
            TeamStats.rebounds_defensive == 0,
            TeamStats.points > 0  # Only games where team scored
        )
        
        # Join with Game to filter by season if specified
        if season:
            query = query.join(Game).filter(Game.season == season)
        
        zero_rebound_stats = query.all()
    
    # Get unique game IDs
    game_ids = list(set([ts.game_id for ts in zero_rebound_stats]))
    
    if limit:
        game_ids = game_ids[:limit]
        logger.info(f"Processing first {limit} games (for testing)")
    
    logger.info(f"Found {len(game_ids)} games with zero rebounds to backfill")
    
    if not game_ids:
        logger.info("No games need rebound data backfill!")
        return True
    
    updated_count = 0
    error_count = 0
    
    for game_id in tqdm(game_ids, desc="Backfilling rebounds"):
        try:
            # Collect fresh team stats from API
            team_stats_list = collector.collect_team_stats(game_id)
            
            if not team_stats_list:
                logger.debug(f"No team stats returned for game {game_id}")
                continue
            
            # Update existing records
            with db_manager.get_session() as session:
                for new_stats in team_stats_list:
                    # Find existing record
                    existing = session.query(TeamStats).filter_by(
                        game_id=game_id,
                        team_id=new_stats['team_id']
                    ).first()
                    
                    if existing:
                        # Update rebound fields
                        existing.rebounds_offensive = new_stats['rebounds_offensive']
                        existing.rebounds_defensive = new_stats['rebounds_defensive']
                        existing.rebounds_total = new_stats['rebounds_total']
                        updated_count += 1
                    else:
                        logger.warning(f"No existing team stats record found for game {game_id}, team {new_stats['team_id']}")
                
                session.commit()
        
        except Exception as e:
            logger.error(f"Error backfilling rebounds for game {game_id}: {e}")
            error_count += 1
            continue
    
    logger.info("\n" + "=" * 70)
    logger.info("Backfill Complete")
    logger.info("=" * 70)
    logger.info(f"Games processed: {len(game_ids)}")
    logger.info(f"Team stats records updated: {updated_count}")
    logger.info(f"Errors: {error_count}")
    logger.info("=" * 70)
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Backfill rebound data for existing games')
    parser.add_argument('--season', type=str, help='Season to backfill (e.g., 2022-23). If not specified, backfills all seasons.')
    parser.add_argument('--limit', type=int, help='Maximum number of games to process (for testing)')
    
    args = parser.parse_args()
    
    try:
        success = backfill_rebounds(season=args.season, limit=args.limit)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n\nBackfill interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
