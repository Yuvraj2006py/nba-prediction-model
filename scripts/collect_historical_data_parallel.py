import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import time
import threading
from typing import Dict, List, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

from src.data_collectors.nba_api_collector import NBAPICollector
from src.database.db_manager import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Thread-safe rate limiter
class ThreadSafeRateLimiter:
    """Thread-safe rate limiter for parallel API calls."""
    
    def __init__(self, delay: float):
        self.delay = delay
        self.last_call = {}
        self.lock = threading.Lock()
    
    def wait(self, thread_id: int):
        """Wait if necessary to respect rate limit."""
        with self.lock:
            if thread_id in self.last_call:
                elapsed = time.time() - self.last_call[thread_id]
                if elapsed < self.delay:
                    time.sleep(self.delay - elapsed)
            self.last_call[thread_id] = time.time()


def get_games_needing_stats(season: str, db_manager: DatabaseManager, collector: NBAPICollector) -> List[str]:
    """Get list of game IDs that need stats collection."""
    from nba_api.stats.static import teams as nba_teams_static
    
    all_games = {}
    nba_teams = nba_teams_static.get_teams()
    
    logger.info(f"Collecting all games for season {season}...")
    logger.info(f"Checking {len(nba_teams)} teams for games...")
    
    for i, team in enumerate(nba_teams):
        team_id = str(team['id'])
        try:
            team_games = collector.get_games_for_team_season(team_id, season)
            for game in team_games:
                game_id = game.get('game_id')
                if game_id:
                    all_games[game_id] = game
            
            # Log progress every 10 teams
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i + 1}/{len(nba_teams)} teams, found {len(all_games)} games")
        except Exception as e:
            logger.error(f"Error getting games for team {team_id}: {e}")
            continue
    
    # Filter to games needing stats
    games_needing_stats = []
    skipped_existing = 0
    
    for game_id in all_games.keys():
        existing_game = db_manager.get_game(game_id)
        if existing_game:
            with db_manager.get_session() as session:
                from src.database.models import TeamStats
                team_stats = session.query(TeamStats).filter_by(game_id=game_id).count()
                if team_stats == 0:
                    games_needing_stats.append(game_id)
                else:
                    skipped_existing += 1
        else:
            games_needing_stats.append(game_id)
    
    logger.info(f"Found {len(games_needing_stats)} games needing stats collection (skipped {skipped_existing} already complete)")
    return games_needing_stats


def process_game_parallel(
    collector: NBAPICollector,
    db_manager: DatabaseManager,
    game_id: str,
    thread_id: int,
    rate_limiter: ThreadSafeRateLimiter
) -> Dict[str, Any]:
    """Process a single game (for parallel execution)."""
    try:
        # Rate limit per thread (reduced delay for parallel processing)
        rate_limiter.wait(thread_id)
        
        # Get game details with timeout handling
        try:
            game_details = collector.get_game_details(game_id)
        except Exception as e:
            # If getting game details fails, skip stats collection but don't fail completely
            error_str = str(e).lower()
            if 'timeout' in error_str or 'timed out' in error_str:
                logger.debug(f"Game {game_id} details timed out, skipping")
                return {'status': 'timeout', 'game_id': game_id}
            raise
        if not game_details:
            return {'status': 'no_details', 'game_id': game_id}
        
        # Store game
        try:
            game_data = {
                'game_id': game_details['game_id'],
                'season': game_details.get('season', '2022-23'),
                'season_type': game_details.get('season_type', 'Regular Season'),
                'game_date': game_details.get('game_date'),
                'home_team_id': game_details['home_team_id'],
                'away_team_id': game_details['away_team_id'],
                'home_score': game_details.get('home_score'),
                'away_score': game_details.get('away_score'),
                'winner': game_details.get('winner'),
                'point_differential': game_details.get('point_differential'),
                'game_status': game_details.get('game_status', 'finished')
            }
            db_manager.insert_game(game_data)
        except Exception as e:
            logger.debug(f"Game {game_id} already exists or error: {e}")
        
        # Collect stats if game is finished
        if game_details.get('game_status') == 'finished':
            # Check if stats already exist
            with db_manager.get_session() as session:
                from src.database.models import TeamStats, PlayerStats
                existing_team_stats = session.query(TeamStats).filter_by(game_id=game_id).count()
                existing_player_stats = session.query(PlayerStats).filter_by(game_id=game_id).count()
            
            if existing_team_stats == 0 or existing_player_stats == 0:
                # Use combined method - ONE API call for both
                try:
                    stats = collector.collect_game_stats(game_id)
                except Exception as e:
                    # If stats collection fails (timeout), log and continue
                    error_str = str(e).lower()
                    if 'timeout' in error_str or 'timed out' in error_str:
                        logger.debug(f"Game {game_id} stats collection timed out, will retry later")
                        return {'status': 'timeout', 'game_id': game_id}
                    raise
                
                # Store team stats
                for team_stat in stats['team_stats']:
                    try:
                        db_manager.insert_team_stats(team_stat)
                    except Exception as e:
                        logger.debug(f"Team stat already exists for {game_id}: {e}")
                
                # Store player stats
                for player_stat in stats['player_stats']:
                    try:
                        db_manager.insert_player_stats(player_stat)
                    except Exception as e:
                        logger.debug(f"Player stat already exists for {game_id}: {e}")
        
        return {'status': 'success', 'game_id': game_id}
        
    except Exception as e:
        logger.error(f"Error processing game {game_id}: {e}")
        return {'status': 'error', 'game_id': game_id, 'error': str(e)}


def collect_season_data_parallel(
    collector: NBAPICollector,
    db_manager: DatabaseManager,
    season: str,
    max_workers: int = 10
) -> Dict[str, int]:
    """Collect data with parallel processing."""
    logger.info("=" * 70)
    logger.info(f"Collecting data for season: {season} (PARALLEL MODE)")
    logger.info("=" * 70)
    
    stats = {
        'games_found': 0,
        'games_stored': 0,
        'games_with_details': 0,
        'games_with_team_stats': 0,
        'games_with_player_stats': 0,
        'errors': 0
    }
    
    # Get games needing stats
    logger.info(f"\n[Step 1/2] Discovering games for season {season}...")
    games_needing_stats = get_games_needing_stats(season, db_manager, collector)
    stats['games_found'] = len(games_needing_stats)
    logger.info(f"Found {len(games_needing_stats)} games needing stats collection")
    
    if not games_needing_stats:
        logger.warning(f"No games found for season {season}")
        return stats
    
    # Process games in parallel
    logger.info(f"\n[Step 2/2] Processing {len(games_needing_stats)} games with {max_workers} workers...")
    
    rate_limiter = ThreadSafeRateLimiter(collector.rate_limit_delay)
    results = {'success': 0, 'errors': 0, 'no_details': 0, 'timeout': 0}
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_game_parallel,
                collector,
                db_manager,
                game_id,
                i % max_workers,  # Thread ID for rate limiting
                rate_limiter
            ): game_id
            for i, game_id in enumerate(games_needing_stats)
        }
        
        for future in tqdm(as_completed(futures), total=len(futures), desc=f"Processing {season}"):
            try:
                result = future.result()
                results[result['status']] = results.get(result['status'], 0) + 1
                
                if result['status'] == 'success':
                    stats['games_stored'] += 1
                    stats['games_with_details'] += 1
                    
                    # Check if stats were collected
                    with db_manager.get_session() as session:
                        from src.database.models import TeamStats, PlayerStats
                        team_stats_count = session.query(TeamStats).filter_by(game_id=result['game_id']).count()
                        player_stats_count = session.query(PlayerStats).filter_by(game_id=result['game_id']).count()
                        
                        if team_stats_count > 0:
                            stats['games_with_team_stats'] += 1
                        if player_stats_count > 0:
                            stats['games_with_player_stats'] += 1
                elif result['status'] == 'timeout':
                    # Timeout games are logged but not counted as errors (can retry later)
                    stats['errors'] += 1
                            
            except Exception as e:
                logger.error(f"Future error: {e}")
                results['errors'] += 1
                stats['errors'] += 1
    
    logger.info(f"\nSeason {season} Summary:")
    logger.info(f"  Games found: {stats['games_found']}")
    logger.info(f"  Games stored: {stats['games_stored']}")
    logger.info(f"  Games with details: {stats['games_with_details']}")
    logger.info(f"  Games with team stats: {stats['games_with_team_stats']}")
    logger.info(f"  Games with player stats: {stats['games_with_player_stats']}")
    logger.info(f"  Errors/Timeouts: {stats['errors']} (can retry later)")
    if results.get('timeout', 0) > 0:
        logger.info(f"  Timeouts: {results['timeout']} (will be skipped, can retry)")
    
    return stats


def collect_all_seasons_parallel(max_workers: int = 10):
    """Collect data for all 3 seasons using parallel processing."""
    logger.info("=" * 70)
    logger.info("NBA Historical Data Collection - PARALLEL MODE")
    logger.info("=" * 70)
    logger.info(f"Using {max_workers} parallel workers")
    logger.info("Seasons: 2022-23, 2023-24, 2024-25")
    logger.info("=" * 70)
    
    # Initialize
    db_manager = DatabaseManager()
    collector = NBAPICollector(db_manager)
    
    if not db_manager.test_connection():
        logger.error("Database connection failed!")
        return False
    
    logger.info("✓ Database connection successful")
    
    # Ensure teams are collected
    logger.info("\n[Pre-flight] Ensuring all teams are collected...")
    teams_list = db_manager.get_all_teams()
    if len(teams_list) < 30:
        logger.info("Collecting teams...")
        collector.collect_all_teams()
        teams_list = db_manager.get_all_teams()
    logger.info(f"✓ {len(teams_list)} teams in database")
    
    # Seasons to collect
    seasons = ['2022-23', '2023-24', '2024-25']
    
    total_stats = {
        'games_found': 0,
        'games_stored': 0,
        'games_with_details': 0,
        'games_with_team_stats': 0,
        'games_with_player_stats': 0,
        'errors': 0
    }
    
    # Collect each season
    for season in seasons:
        try:
            season_stats = collect_season_data_parallel(
                collector, db_manager, season, max_workers=max_workers
            )
            
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
    logger.info("Collection Complete - Final Summary")
    logger.info("=" * 70)
    logger.info(f"Total games found: {total_stats['games_found']}")
    logger.info(f"Total games stored: {total_stats['games_stored']}")
    logger.info(f"Games with details: {total_stats['games_with_details']}")
    logger.info(f"Games with team stats: {total_stats['games_with_team_stats']}")
    logger.info(f"Games with player stats: {total_stats['games_with_player_stats']}")
    logger.info(f"Total errors: {total_stats['errors']}")
    logger.info("=" * 70)
    
    # Verify final counts
    with db_manager.get_session() as session:
        from src.database.models import Game, TeamStats, PlayerStats
        final_games = session.query(Game).count()
        final_team_stats = session.query(TeamStats).count()
        final_player_stats = session.query(PlayerStats).count()
        
        logger.info(f"\nDatabase Final Counts:")
        logger.info(f"  Games: {final_games}")
        logger.info(f"  Team Stats: {final_team_stats}")
        logger.info(f"  Player Stats: {final_player_stats}")
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Collect NBA data in parallel')
    parser.add_argument('--workers', type=int, default=10, help='Number of parallel workers (default: 10)')
    args = parser.parse_args()
    
    try:
        success = collect_all_seasons_parallel(max_workers=args.workers)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n\nCollection interrupted by user. Progress has been saved.")
        logger.info("You can resume by running this script again - it will skip already collected games.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

