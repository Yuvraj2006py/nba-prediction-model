"""Collect Basketball Reference stats for a season or all seasons."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import argparse
from typing import List, Dict, Any
from tqdm import tqdm

from src.data_collectors.basketball_reference_collector import BasketballReferenceCollector
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bball_ref_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def collect_season_bball_ref(season: str, replace_existing: bool = False) -> Dict[str, int]:
    """
    Collect Basketball Reference stats for all games in a season.
    
    Args:
        season: Season string (e.g., '2022-23')
        replace_existing: If True, replace existing stats. If False, skip games with stats.
        
    Returns:
        Dictionary with collection statistics
    """
    stats = {
        'games_processed': 0,
        'games_with_stats': 0,
        'games_skipped': 0,
        'team_stats_collected': 0,
        'player_stats_collected': 0,
        'errors': 0
    }
    
    logger.info("=" * 70)
    logger.info(f"Collecting Basketball Reference stats for season: {season}")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    collector = BasketballReferenceCollector(db_manager)
    
    if not db_manager.test_connection():
        logger.error("Database connection failed!")
        return stats
    
    # Get all finished games for the season
    with db_manager.get_session() as session:
        games = session.query(Game).filter(
            Game.season == season,
            Game.game_status == 'finished'
        ).order_by(Game.game_date).all()
    
    logger.info(f"Found {len(games)} finished games for season {season}")
    
    if not games:
        logger.warning(f"No finished games found for season {season}")
        return stats
    
    # Determine which games need stats
    games_to_process = []
    for game in games:
        with db_manager.get_session() as session:
            team_stats_count = session.query(TeamStats).filter_by(game_id=game.game_id).count()
            player_stats_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
        
        needs_stats = (team_stats_count == 0 or player_stats_count == 0)
        
        if replace_existing or needs_stats:
            games_to_process.append(game)
        else:
            stats['games_skipped'] += 1
    
    logger.info(f"Processing {len(games_to_process)} games (skipping {stats['games_skipped']} with existing stats)")
    
    if replace_existing:
        logger.warning("REPLACE_EXISTING is True - will overwrite existing stats!")
    
    # Process games
    for i, game in enumerate(tqdm(games_to_process, desc=f"Collecting stats for {season}")):
        try:
            # If replacing, delete existing stats first
            if replace_existing:
                with db_manager.get_session() as session:
                    session.query(TeamStats).filter_by(game_id=game.game_id).delete()
                    session.query(PlayerStats).filter_by(game_id=game.game_id).delete()
                    session.commit()
            
            # Collect stats from Basketball Reference
            result = collector.collect_game_stats(game.game_id)
            
            if result['team_stats'] or result['player_stats']:
                # Store team stats
                for team_stat in result['team_stats']:
                    try:
                        db_manager.insert_team_stats(team_stat)
                        stats['team_stats_collected'] += 1
                    except Exception as e:
                        logger.warning(f"Could not store team stat for game {game.game_id}: {e}")
                
                # Store player stats
                for player_stat in result['player_stats']:
                    try:
                        db_manager.insert_player_stats(player_stat)
                        stats['player_stats_collected'] += 1
                    except Exception as e:
                        logger.warning(f"Could not store player stat for game {game.game_id}: {e}")
                
                if result['team_stats'] and result['player_stats']:
                    stats['games_with_stats'] += 1
                    logger.debug(f"✓ Collected stats for game {game.game_id} ({game.game_date})")
                else:
                    logger.warning(f"Partial stats for game {game.game_id}: {len(result['team_stats'])} team, {len(result['player_stats'])} player")
            else:
                logger.warning(f"No stats collected for game {game.game_id} ({game.game_date})")
            
            stats['games_processed'] += 1
            
            # Log progress every 50 games
            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i + 1}/{len(games_to_process)} games processed")
        
        except Exception as e:
            logger.error(f"Error processing game {game.game_id}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            stats['errors'] += 1
            continue
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info(f"Collection Complete for {season}")
    logger.info("=" * 70)
    logger.info(f"Games processed: {stats['games_processed']}")
    logger.info(f"Games with stats: {stats['games_with_stats']}")
    logger.info(f"Games skipped: {stats['games_skipped']}")
    logger.info(f"Team stats collected: {stats['team_stats_collected']}")
    logger.info(f"Player stats collected: {stats['player_stats_collected']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info("=" * 70)
    
    return stats


def collect_all_seasons_bball_ref(replace_existing: bool = False):
    """Collect Basketball Reference stats for all seasons."""
    seasons = ['2022-23', '2023-24', '2024-25']
    
    logger.info("=" * 70)
    logger.info("Basketball Reference Stats Collection - All Seasons")
    logger.info("=" * 70)
    logger.info(f"Seasons: {', '.join(seasons)}")
    logger.info(f"Replace existing: {replace_existing}")
    logger.info("=" * 70)
    
    total_stats = {
        'games_processed': 0,
        'games_with_stats': 0,
        'games_skipped': 0,
        'team_stats_collected': 0,
        'player_stats_collected': 0,
        'errors': 0
    }
    
    for season in seasons:
        try:
            season_stats = collect_season_bball_ref(season, replace_existing=replace_existing)
            
            # Aggregate stats
            for key in total_stats:
                total_stats[key] += season_stats[key]
        
        except Exception as e:
            logger.error(f"Error collecting season {season}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            continue
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("All Seasons Collection Complete")
    logger.info("=" * 70)
    logger.info(f"Total games processed: {total_stats['games_processed']}")
    logger.info(f"Total games with stats: {total_stats['games_with_stats']}")
    logger.info(f"Total games skipped: {total_stats['games_skipped']}")
    logger.info(f"Total team stats collected: {total_stats['team_stats_collected']}")
    logger.info(f"Total player stats collected: {total_stats['player_stats_collected']}")
    logger.info(f"Total errors: {total_stats['errors']}")
    logger.info("=" * 70)
    
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect Basketball Reference stats for seasons')
    parser.add_argument('--season', type=str, help='Season to collect (e.g., 2022-23). If not specified, collects all seasons.')
    parser.add_argument('--replace', action='store_true', help='Replace existing stats (default: skip games with existing stats)')
    
    args = parser.parse_args()
    
    try:
        if args.season:
            success = collect_season_bball_ref(args.season, replace_existing=args.replace)
            sys.exit(0 if success else 1)
        else:
            success = collect_all_seasons_bball_ref(replace_existing=args.replace)
            sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n\nCollection interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
