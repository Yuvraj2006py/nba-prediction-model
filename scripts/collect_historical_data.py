"""Collect historical NBA data for 3 seasons (2022-23, 2023-24, 2024-25)."""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import date, datetime
from typing import Dict, Any
try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm not available
    def tqdm(iterable, desc=""):
        return iterable
from src.data_collectors.nba_api_collector import NBAPICollector
from src.database.db_manager import DatabaseManager
from nba_api.stats.static import teams

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/data_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_all_unique_games_for_season(
    collector: NBAPICollector,
    season: str,
    db_manager: DatabaseManager
) -> Dict[str, Dict[str, Any]]:
    """
    Get all unique games for a season by checking all teams.
    
    Args:
        collector: NBA API collector
        season: Season string (e.g., '2024-25')
        db_manager: Database manager
        
    Returns:
        Dictionary of unique games {game_id: game_data}
    """
    logger.info(f"Collecting all games for season {season}...")
    
    all_games = {}
    nba_teams = teams.get_teams()
    skipped_existing = 0
    
    logger.info(f"Checking {len(nba_teams)} teams for games...")
    
    for i, team in enumerate(nba_teams):
        team_id = str(team['id'])
        team_name = team['full_name']
        
        try:
            # Get games for this team in this season
            team_games = collector.get_games_for_team_season(team_id, season)
            
            for game in team_games:
                game_id = game.get('game_id')
                if game_id:
                    # Check if game already exists in database
                    existing_game = db_manager.get_game(game_id)
                    if existing_game:
                        skipped_existing += 1
                        continue
                    
                    # Add to collection if not already seen
                    if game_id not in all_games:
                        all_games[game_id] = {
                            'game_id': game_id,
                            'game_date': game.get('game_date'),
                            'season': season,
                            'season_type': game.get('season_type', 'Regular Season'),
                            'team_id': team_id,  # One of the teams (we'll get both from game details)
                            'matchup': game.get('matchup', '')
                        }
            
            # Log progress every 10 teams
            if (i + 1) % 10 == 0:
                logger.info(f"Processed {i + 1}/{len(nba_teams)} teams, found {len(all_games)} new games, skipped {skipped_existing} existing")
                
        except Exception as e:
            logger.error(f"Error getting games for team {team_name} ({team_id}): {e}")
            continue
    
    logger.info(f"Found {len(all_games)} new unique games for season {season} (skipped {skipped_existing} already in database)")
    return all_games


def collect_season_data(
    collector: NBAPICollector,
    db_manager: DatabaseManager,
    season: str,
    collect_stats: bool = True
) -> Dict[str, int]:
    """
    Collect all data for a single season.
    
    Args:
        collector: NBA API collector
        db_manager: Database manager
        season: Season string (e.g., '2024-25')
        collect_stats: Whether to collect team/player stats
        
    Returns:
        Dictionary with collection statistics
    """
    stats = {
        'games_found': 0,
        'games_stored': 0,
        'games_with_details': 0,
        'games_with_team_stats': 0,
        'games_with_player_stats': 0,
        'errors': 0
    }
    
    logger.info("=" * 70)
    logger.info(f"Collecting data for season: {season}")
    logger.info("=" * 70)
    
    # Step 1: Get all unique games for the season
    logger.info("\n[Step 1/4] Discovering all games for the season...")
    all_games = get_all_unique_games_for_season(collector, season, db_manager)
    stats['games_found'] = len(all_games)
    
    if not all_games:
        logger.warning(f"No games found for season {season}")
        return stats
    
    # Step 2: Store games and get details
    logger.info(f"\n[Step 2/4] Storing games and collecting details...")
    game_ids = list(all_games.keys())
    
    # Check which games already have stats
    games_needing_stats = []
    for game_id in game_ids:
        existing_game = db_manager.get_game(game_id)
        if existing_game:
            # Check if stats exist
            with db_manager.get_session() as session:
                from src.database.models import TeamStats
                team_stats = session.query(TeamStats).filter_by(game_id=game_id).count()
                if team_stats == 0:
                    games_needing_stats.append(game_id)
        else:
            games_needing_stats.append(game_id)
    
    logger.info(f"Found {len(games_needing_stats)} games needing stats collection")
    
    for i, game_id in enumerate(tqdm(games_needing_stats, desc="Processing games")):
        try:
            # Get full game details
            game_details = collector.get_game_details(game_id)
            
            if game_details:
                # Store/update game
                game_data = {
                    'game_id': game_details['game_id'],
                    'season': game_details.get('season', season),
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
                stats['games_stored'] += 1
                stats['games_with_details'] += 1
                
                # Step 3: Collect team stats (if game is finished and stats don't exist)
                if collect_stats and game_details.get('game_status') == 'finished':
                    # Check if stats already exist
                    with db_manager.get_session() as session:
                        from src.database.models import TeamStats, PlayerStats
                        existing_team_stats = session.query(TeamStats).filter_by(game_id=game_id).count()
                        existing_player_stats = session.query(PlayerStats).filter_by(game_id=game_id).count()
                    
                    # Collect stats if missing (using combined method - ONE API call for both)
                    if existing_team_stats == 0 or existing_player_stats == 0:
                        try:
                            # Use combined method - ONE API call for both team and player stats
                            stats_result = collector.collect_game_stats(game_id)
                            
                            # Store team stats
                            for team_stat in stats_result['team_stats']:
                                db_manager.insert_team_stats(team_stat)
                            if stats_result['team_stats']:
                                stats['games_with_team_stats'] += 1
                            
                            # Store player stats
                            for player_stat in stats_result['player_stats']:
                                db_manager.insert_player_stats(player_stat)
                            if stats_result['player_stats']:
                                stats['games_with_player_stats'] += 1
                        except Exception as e:
                            logger.warning(f"Error collecting stats for {game_id}: {e}")
                    else:
                        stats['games_with_team_stats'] += 1
                        stats['games_with_player_stats'] += 1
                
            else:
                logger.debug(f"No details found for game {game_id} (may be scheduled)")
                # Still store basic game info if we have it
                if game_id in all_games:
                    game_info = all_games[game_id]
                    try:
                        db_manager.insert_game({
                            'game_id': game_id,
                            'season': season,
                            'season_type': game_info.get('season_type', 'Regular Season'),
                            'game_date': game_info.get('game_date'),
                            'home_team_id': '',  # Will be updated when details are available
                            'away_team_id': '',
                            'game_status': 'scheduled'
                        })
                        stats['games_stored'] += 1
                    except Exception as e:
                        logger.debug(f"Could not store basic game info: {e}")
            
            # Log progress every 50 games
            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i + 1}/{len(game_ids)} games processed")
                
        except Exception as e:
            logger.error(f"Error processing game {game_id}: {e}")
            stats['errors'] += 1
            continue
    
    return stats


def collect_all_seasons():
    """Collect data for all 3 seasons."""
    logger.info("=" * 70)
    logger.info("NBA Historical Data Collection - 3 Seasons")
    logger.info("=" * 70)
    logger.info("Seasons: 2022-23, 2023-24, 2024-25")
    logger.info("=" * 70)
    
    # Initialize
    db_manager = DatabaseManager()
    collector = NBAPICollector(db_manager)
    
    # Test database connection
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
            season_stats = collect_season_data(collector, db_manager, season, collect_stats=True)
            
            # Aggregate stats
            for key in total_stats:
                total_stats[key] += season_stats[key]
            
            logger.info(f"\nSeason {season} Summary:")
            logger.info(f"  Games found: {season_stats['games_found']}")
            logger.info(f"  Games stored: {season_stats['games_stored']}")
            logger.info(f"  Games with details: {season_stats['games_with_details']}")
            logger.info(f"  Games with team stats: {season_stats['games_with_team_stats']}")
            logger.info(f"  Games with player stats: {season_stats['games_with_player_stats']}")
            logger.info(f"  Errors: {season_stats['errors']}")
            
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
    try:
        success = collect_all_seasons()
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

