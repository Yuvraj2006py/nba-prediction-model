"""ULTRA-FAST parallel collection from Basketball Reference using multiple Selenium instances."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import argparse
from typing import List, Dict, Any
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

try:
    from src.data_collectors.basketball_reference_selenium import BasketballReferenceSeleniumCollector
    USE_SELENIUM = True
except ImportError:
    USE_SELENIUM = False
    
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats

logging.basicConfig(
    level=logging.WARNING,  # Reduce log noise for speed
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bball_ref_fast_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Thread-local storage for database connections
thread_local = threading.local()


def get_thread_db_manager():
    """Get thread-local database manager."""
    if not hasattr(thread_local, "db_manager"):
        thread_local.db_manager = DatabaseManager()
    return thread_local.db_manager


def get_thread_collector():
    """Get thread-local collector."""
    if not hasattr(thread_local, "collector"):
        db_manager = get_thread_db_manager()
        thread_local.collector = BasketballReferenceSeleniumCollector(db_manager)
        # Disable rate limiting
        thread_local.collector.scraping_delay = 0
    return thread_local.collector


def process_single_game(game_id: str, season: str) -> Dict[str, Any]:
    """
    Process a single game in parallel.
    
    Args:
        game_id: Game ID to process
        season: Season string
        
    Returns:
        Dictionary with results
    """
    result = {
        'game_id': game_id,
        'success': False,
        'team_stats': 0,
        'player_stats': 0,
        'error': None
    }
    
    try:
        db_manager = get_thread_db_manager()
        collector = get_thread_collector()
        
        # Collect all game data (no retries)
        all_data = collector.collect_all_game_data(game_id)
        
        if not all_data:
            result['error'] = 'No data returned'
            return result
        
        # Update game details
        if all_data['game_details']:
            game_details = all_data['game_details']
            with db_manager.get_session() as session:
                existing_game = session.query(Game).filter_by(game_id=game_id).first()
                if existing_game:
                    if game_details.get('home_score') is not None:
                        existing_game.home_score = game_details['home_score']
                    if game_details.get('away_score') is not None:
                        existing_game.away_score = game_details['away_score']
                    if game_details.get('winner'):
                        existing_game.winner = game_details['winner']
                    if game_details.get('point_differential') is not None:
                        existing_game.point_differential = game_details['point_differential']
                    existing_game.game_status = game_details.get('game_status', 'finished')
                session.commit()
        
        # Batch insert team stats
        if all_data['team_stats']:
            for team_stat in all_data['team_stats']:
                with db_manager.get_session() as session:
                    existing = session.query(TeamStats).filter_by(
                        game_id=game_id,
                        team_id=team_stat['team_id']
                    ).first()
                    
                    if existing:
                        for key, value in team_stat.items():
                            if key not in ['game_id', 'team_id', 'is_home']:
                                setattr(existing, key, value)
                    else:
                        db_manager.insert_team_stats(team_stat)
                    session.commit()
                    result['team_stats'] += 1
        
        # Batch insert player stats
        if all_data['player_stats']:
            for player_stat in all_data['player_stats']:
                with db_manager.get_session() as session:
                    existing = session.query(PlayerStats).filter_by(
                        game_id=game_id,
                        player_id=player_stat['player_id']
                    ).first()
                    
                    if existing:
                        for key, value in player_stat.items():
                            if key not in ['game_id', 'player_id']:
                                setattr(existing, key, value)
                    else:
                        db_manager.insert_player_stats(player_stat)
                    session.commit()
                    result['player_stats'] += 1
        
        result['success'] = True
        
    except Exception as e:
        result['error'] = str(e)
    
    return result


def collect_season_fast(season: str, workers: int = 8, replace_existing: bool = False) -> Dict[str, int]:
    """
    ULTRA-FAST parallel collection with multiple Selenium instances.
    
    Args:
        season: Season string (e.g., '2024-25')
        workers: Number of parallel workers (default: 8)
        replace_existing: If True, replace existing data
        
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
    
    logger.warning("=" * 70)
    logger.warning(f"ULTRA-FAST Collection for {season} with {workers} workers")
    logger.warning("=" * 70)
    
    if not USE_SELENIUM:
        logger.error("Selenium not available!")
        return stats
    
    db_manager = DatabaseManager()
    
    if not db_manager.test_connection():
        logger.error("Database connection failed!")
        return stats
    
    # Get all games for the season
    with db_manager.get_session() as session:
        games = session.query(Game).filter(
            Game.season == season
        ).order_by(Game.game_date).all()
    
    logger.warning(f"Found {len(games)} games for season {season}")
    
    if not games:
        logger.error(f"No games found for season {season}")
        return stats
    
    # Filter games that need processing
    games_to_process = []
    for game in games:
        needs_processing = replace_existing
        
        if not needs_processing:
            with db_manager.get_session() as session:
                team_stats_count = session.query(TeamStats).filter_by(game_id=game.game_id).count()
                player_stats_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
                needs_processing = (team_stats_count == 0 or player_stats_count == 0)
        
        if needs_processing:
            games_to_process.append(game.game_id)
        else:
            stats['games_skipped'] += 1
    
    logger.warning(f"Processing {len(games_to_process)} games (skipping {stats['games_skipped']})")
    
    if not games_to_process:
        logger.warning("No games to process!")
        return stats
    
    # Process games in parallel
    logger.warning(f"Starting parallel processing with {workers} workers...")
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Submit all tasks
        future_to_game = {
            executor.submit(process_single_game, game_id, season): game_id
            for game_id in games_to_process
        }
        
        # Process completed tasks with progress bar
        with tqdm(total=len(games_to_process), desc=f"Collecting {season}") as pbar:
            for future in as_completed(future_to_game):
                game_id = future_to_game[future]
                try:
                    result = future.result()
                    
                    if result['success']:
                        stats['games_processed'] += 1
                        if result['team_stats'] > 0 or result['player_stats'] > 0:
                            stats['games_with_stats'] += 1
                        stats['team_stats_collected'] += result['team_stats']
                        stats['player_stats_collected'] += result['player_stats']
                    else:
                        stats['errors'] += 1
                        if result['error']:
                            logger.debug(f"Game {game_id}: {result['error']}")
                
                except Exception as e:
                    logger.error(f"Exception processing game {game_id}: {e}")
                    stats['errors'] += 1
                
                pbar.update(1)
    
    # Final summary
    logger.warning("\n" + "=" * 70)
    logger.warning(f"Collection Complete for {season}")
    logger.warning("=" * 70)
    logger.warning(f"Games processed: {stats['games_processed']}")
    logger.warning(f"Games with stats: {stats['games_with_stats']}")
    logger.warning(f"Games skipped: {stats['games_skipped']}")
    logger.warning(f"Team stats collected: {stats['team_stats_collected']}")
    logger.warning(f"Player stats collected: {stats['player_stats_collected']}")
    logger.warning(f"Errors: {stats['errors']}")
    logger.warning("=" * 70)
    
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ULTRA-FAST parallel Basketball Reference collection')
    parser.add_argument('--season', type=str, required=True, help='Season to collect (e.g., 2024-25)')
    parser.add_argument('--workers', type=int, default=8, help='Number of parallel workers (default: 8)')
    parser.add_argument('--replace', action='store_true', help='Replace existing data')
    
    args = parser.parse_args()
    
    try:
        logger.warning(f"\n{'='*70}")
        logger.warning(f"ULTRA-FAST MODE - {args.workers} parallel workers")
        logger.warning(f"No retries - Maximum speed")
        logger.warning(f"{'='*70}\n")
        
        stats = collect_season_fast(
            args.season,
            workers=args.workers,
            replace_existing=args.replace
        )
        
        sys.exit(0 if stats['games_processed'] > 0 else 1)
    
    except KeyboardInterrupt:
        logger.warning("\n\nCollection interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
