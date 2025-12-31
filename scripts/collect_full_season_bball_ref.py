"""Collect complete season data from Basketball Reference (game details + stats)."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import argparse
from typing import List, Dict, Any
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
        logging.FileHandler('logs/bball_ref_full_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def collect_season_full_bball_ref(season: str, replace_existing: bool = False) -> Dict[str, int]:
    """
    Collect complete season data from Basketball Reference (game details + stats).
    This is equivalent to what NBA API collector does.
    
    Args:
        season: Season string (e.g., '2022-23')
        replace_existing: If True, replace existing data. If False, skip games with data.
        
    Returns:
        Dictionary with collection statistics
    """
    stats = {
        'games_processed': 0,
        'games_with_details': 0,
        'games_with_stats': 0,
        'games_skipped': 0,
        'team_stats_collected': 0,
        'player_stats_collected': 0,
        'errors': 0
    }
    
    logger.info("=" * 70)
    logger.info(f"Collecting FULL season data from Basketball Reference: {season}")
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
        return stats
    
    # Get all games for the season (including scheduled/unfinished)
    with db_manager.get_session() as session:
        games = session.query(Game).filter(
            Game.season == season
        ).order_by(Game.game_date).all()
    
    logger.info(f"Found {len(games)} games for season {season}")
    
    if not games:
        logger.warning(f"No games found for season {season}")
        return stats
    
    # Filter games that need processing
    games_to_process = []
    for game in games:
        # Check if we need to update game details
        needs_details = False
        if replace_existing:
            needs_details = True
        elif game.home_score is None or game.away_score is None:
            needs_details = True
        
        # Check if we need stats
        with db_manager.get_session() as session:
            team_stats_count = session.query(TeamStats).filter_by(game_id=game.game_id).count()
            player_stats_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
            
            needs_rebounds = False
            if team_stats_count > 0:
                team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
                for ts in team_stats:
                    if ts.rebounds_total == 0 and ts.rebounds_offensive == 0 and ts.rebounds_defensive == 0 and ts.points > 0:
                        needs_rebounds = True
                        break
            
            needs_stats = (team_stats_count == 0 or player_stats_count == 0 or needs_rebounds)
        
        if needs_details or needs_stats or replace_existing:
            games_to_process.append(game)
        else:
            stats['games_skipped'] += 1
    
    logger.info(f"Processing {len(games_to_process)} games (skipping {stats['games_skipped']} with complete data)")
    
    # Process games
    for i, game in enumerate(tqdm(games_to_process, desc=f"Collecting {season}")):
        try:
            # Collect all game data (details + stats)
            all_data = collector.collect_all_game_data(game.game_id)
            
            if not all_data:
                logger.warning(f"No data collected for game {game.game_id}")
                stats['errors'] += 1
                continue
            
            # Update game details
            if all_data['game_details']:
                game_details = all_data['game_details']
                try:
                    with db_manager.get_session() as session:
                        existing_game = session.query(Game).filter_by(game_id=game.game_id).first()
                        if existing_game:
                            # Update existing game
                            if game_details.get('home_score') is not None:
                                existing_game.home_score = game_details['home_score']
                            if game_details.get('away_score') is not None:
                                existing_game.away_score = game_details['away_score']
                            if game_details.get('winner'):
                                existing_game.winner = game_details['winner']
                            if game_details.get('point_differential') is not None:
                                existing_game.point_differential = game_details['point_differential']
                            existing_game.game_status = game_details.get('game_status', 'finished')
                        else:
                            # Insert new game
                            db_manager.insert_game(game_details)
                        session.commit()
                        stats['games_with_details'] += 1
                except Exception as e:
                    logger.warning(f"Could not update game details for {game.game_id}: {e}")
            
            # Update team stats
            if all_data['team_stats']:
                for team_stat in all_data['team_stats']:
                    try:
                        with db_manager.get_session() as session:
                            existing = session.query(TeamStats).filter_by(
                                game_id=game.game_id,
                                team_id=team_stat['team_id']
                            ).first()
                            
                            if existing:
                                # Update existing
                                for key, value in team_stat.items():
                                    if key not in ['game_id', 'team_id', 'is_home']:
                                        setattr(existing, key, value)
                            else:
                                # Insert new
                                db_manager.insert_team_stats(team_stat)
                            
                            session.commit()
                            stats['team_stats_collected'] += 1
                    except Exception as e:
                        logger.warning(f"Could not update team stat for game {game.game_id}: {e}")
            
            # Update player stats
            if all_data['player_stats']:
                for player_stat in all_data['player_stats']:
                    try:
                        with db_manager.get_session() as session:
                            existing = session.query(PlayerStats).filter_by(
                                game_id=game.game_id,
                                player_id=player_stat['player_id']
                            ).first()
                            
                            if existing:
                                # Update existing
                                for key, value in player_stat.items():
                                    if key not in ['game_id', 'player_id']:
                                        setattr(existing, key, value)
                            else:
                                # Insert new
                                db_manager.insert_player_stats(player_stat)
                            
                            session.commit()
                            stats['player_stats_collected'] += 1
                    except Exception as e:
                        logger.warning(f"Could not update player stat for game {game.game_id}: {e}")
            
            if all_data['team_stats'] or all_data['player_stats']:
                stats['games_with_stats'] += 1
            
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
    logger.info(f"Games with details: {stats['games_with_details']}")
    logger.info(f"Games with stats: {stats['games_with_stats']}")
    logger.info(f"Games skipped: {stats['games_skipped']}")
    logger.info(f"Team stats records: {stats['team_stats_collected']}")
    logger.info(f"Player stats records: {stats['player_stats_collected']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info("=" * 70)
    
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect full season data from Basketball Reference')
    parser.add_argument('--season', type=str, required=True, help='Season to collect (e.g., 2022-23)')
    parser.add_argument('--replace', action='store_true', help='Replace existing data (default: only fill missing)')
    
    args = parser.parse_args()
    
    try:
        success = collect_season_full_bball_ref(args.season, replace_existing=args.replace)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n\nCollection interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
