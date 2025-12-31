"""Backfill rebound data and other stats using Basketball Reference scraper."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from tqdm import tqdm

try:
    from src.data_collectors.basketball_reference_selenium import BasketballReferenceSeleniumCollector
    USE_SELENIUM = True
except ImportError:
    from src.data_collectors.basketball_reference_collector import BasketballReferenceCollector
    USE_SELENIUM = False
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bball_ref_backfill.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def backfill_with_bball_ref(season: str = None, limit: int = None, replace_all: bool = False):
    """
    Backfill team and player stats using Basketball Reference scraper.
    
    Args:
        season: Season to backfill (e.g., '2022-23'). If None, backfills all seasons.
        limit: Maximum number of games to process (for testing). If None, processes all.
        replace_all: If True, replaces all stats. If False, only fills missing stats.
    """
    logger.info("=" * 70)
    logger.info("Basketball Reference Stats Backfill")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    
    if USE_SELENIUM:
        logger.info("Using Selenium-based Basketball Reference collector")
        collector = BasketballReferenceSeleniumCollector(db_manager)
    else:
        logger.info("Using requests-based Basketball Reference collector")
        collector = BasketballReferenceCollector(db_manager)
    
    if not db_manager.test_connection():
        logger.error("Database connection failed!")
        return False
    
    # Find games that need stats
    with db_manager.get_session() as session:
        query = session.query(Game).filter(
            Game.game_status == 'finished'
        )
        
        if season:
            query = query.filter(Game.season == season)
        
        all_games = query.order_by(Game.game_date).all()
    
    # Filter games that need stats
    games_to_process = []
    for game in all_games:
        with db_manager.get_session() as session:
            team_stats_count = session.query(TeamStats).filter_by(game_id=game.game_id).count()
            player_stats_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
            
            # Check if rebounds are missing (even if other stats exist)
            needs_rebounds = False
            if team_stats_count > 0:
                team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
                for ts in team_stats:
                    if ts.rebounds_total == 0 and ts.rebounds_offensive == 0 and ts.rebounds_defensive == 0 and ts.points > 0:
                        needs_rebounds = True
                        break
            
            needs_stats = (team_stats_count == 0 or player_stats_count == 0)
            
            if replace_all or needs_stats or needs_rebounds:
                games_to_process.append(game)
    
    if limit:
        games_to_process = games_to_process[:limit]
        logger.info(f"Processing first {limit} games (for testing)")
    
    logger.info(f"Found {len(games_to_process)} games to process")
    logger.info(f"Replace all: {replace_all}")
    
    if not games_to_process:
        logger.info("No games need stats backfill!")
        return True
    
    updated_games = 0
    updated_team_stats = 0
    updated_player_stats = 0
    error_count = 0
    skipped_count = 0
    
    for i, game in enumerate(tqdm(games_to_process, desc="Backfilling stats")):
        try:
            # If replacing, delete existing stats first
            if replace_all:
                with db_manager.get_session() as session:
                    session.query(TeamStats).filter_by(game_id=game.game_id).delete()
                    session.query(PlayerStats).filter_by(game_id=game.game_id).delete()
                    session.commit()
            
            # Collect stats from Basketball Reference
            result = collector.collect_game_stats(game.game_id)
            
            if not result['team_stats'] and not result['player_stats']:
                logger.debug(f"No stats collected for game {game.game_id} ({game.game_date})")
                skipped_count += 1
                continue
            
            # Update or insert team stats
            if result['team_stats']:
                for team_stat in result['team_stats']:
                    try:
                        with db_manager.get_session() as session:
                            # Check if record exists
                            existing = session.query(TeamStats).filter_by(
                                game_id=game.game_id,
                                team_id=team_stat['team_id']
                            ).first()
                            
                            if existing:
                                # Update existing record
                                existing.points = team_stat['points']
                                existing.field_goals_made = team_stat['field_goals_made']
                                existing.field_goals_attempted = team_stat['field_goals_attempted']
                                existing.field_goal_percentage = team_stat['field_goal_percentage']
                                existing.three_pointers_made = team_stat['three_pointers_made']
                                existing.three_pointers_attempted = team_stat['three_pointers_attempted']
                                existing.three_point_percentage = team_stat['three_point_percentage']
                                existing.free_throws_made = team_stat['free_throws_made']
                                existing.free_throws_attempted = team_stat['free_throws_attempted']
                                existing.free_throw_percentage = team_stat['free_throw_percentage']
                                existing.rebounds_offensive = team_stat['rebounds_offensive']
                                existing.rebounds_defensive = team_stat['rebounds_defensive']
                                existing.rebounds_total = team_stat['rebounds_total']
                                existing.assists = team_stat['assists']
                                existing.steals = team_stat['steals']
                                existing.blocks = team_stat['blocks']
                                existing.turnovers = team_stat['turnovers']
                                existing.personal_fouls = team_stat['personal_fouls']
                                existing.true_shooting_percentage = team_stat.get('true_shooting_percentage')
                                existing.effective_field_goal_percentage = team_stat.get('effective_field_goal_percentage')
                            else:
                                # Insert new record
                                db_manager.insert_team_stats(team_stat)
                            
                            session.commit()
                            updated_team_stats += 1
                    except Exception as e:
                        logger.warning(f"Could not update team stat for game {game.game_id}: {e}")
            
            # Update or insert player stats
            if result['player_stats']:
                for player_stat in result['player_stats']:
                    try:
                        with db_manager.get_session() as session:
                            # Check if record exists
                            existing = session.query(PlayerStats).filter_by(
                                game_id=game.game_id,
                                player_id=player_stat['player_id']
                            ).first()
                            
                            if existing:
                                # Update existing record
                                existing.points = player_stat['points']
                                existing.rebounds = player_stat['rebounds']
                                existing.assists = player_stat['assists']
                                existing.field_goals_made = player_stat['field_goals_made']
                                existing.field_goals_attempted = player_stat['field_goals_attempted']
                                existing.three_pointers_made = player_stat['three_pointers_made']
                                existing.three_pointers_attempted = player_stat['three_pointers_attempted']
                                existing.free_throws_made = player_stat['free_throws_made']
                                existing.free_throws_attempted = player_stat['free_throws_attempted']
                                existing.plus_minus = player_stat.get('plus_minus')
                                existing.injury_status = player_stat.get('injury_status', 'healthy')
                            else:
                                # Insert new record
                                db_manager.insert_player_stats(player_stat)
                            
                            session.commit()
                            updated_player_stats += 1
                    except Exception as e:
                        logger.warning(f"Could not update player stat for game {game.game_id}: {e}")
            
            if result['team_stats'] or result['player_stats']:
                updated_games += 1
            
            # Log progress every 50 games
            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i + 1}/{len(games_to_process)} games processed")
        
        except Exception as e:
            logger.error(f"Error processing game {game.game_id}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            error_count += 1
            continue
    
    logger.info("\n" + "=" * 70)
    logger.info("Backfill Complete")
    logger.info("=" * 70)
    logger.info(f"Games processed: {len(games_to_process)}")
    logger.info(f"Games updated: {updated_games}")
    logger.info(f"Team stats records: {updated_team_stats}")
    logger.info(f"Player stats records: {updated_player_stats}")
    logger.info(f"Skipped (no data): {skipped_count}")
    logger.info(f"Errors: {error_count}")
    logger.info("=" * 70)
    
    return True


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Backfill stats using Basketball Reference scraper')
    parser.add_argument('--season', type=str, help='Season to backfill (e.g., 2022-23). If not specified, backfills all seasons.')
    parser.add_argument('--limit', type=int, help='Maximum number of games to process (for testing)')
    parser.add_argument('--replace', action='store_true', help='Replace all existing stats (default: only fill missing)')
    
    args = parser.parse_args()
    
    try:
        success = backfill_with_bball_ref(
            season=args.season,
            limit=args.limit,
            replace_all=args.replace
        )
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n\nBackfill interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
