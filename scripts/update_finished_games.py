"""
Update finished games with final scores.
Checks scheduled games and updates scores when games are completed.
Uses NBA API to get final scores.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

import logging
from datetime import date, timedelta
from argparse import ArgumentParser
from src.database.db_manager import DatabaseManager
from src.database.models import Game
from src.data_collectors.nba_api_collector import NBAPICollector

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def update_game_scores(target_date: date, db: DatabaseManager, quiet: bool = False) -> dict:
    """
    Update scores for finished games on the target date.
    
    Args:
        target_date: Date to check for finished games
        db: Database manager
        quiet: Suppress detailed output
        
    Returns:
        Dictionary with update statistics
    """
    stats = {
        'checked': 0,
        'updated': 0,
        'already_finished': 0,
        'still_scheduled': 0,
        'errors': 0
    }
    
    # Get scheduled games for the date
    with db.get_session() as session:
        scheduled_games = session.query(Game).filter(
            Game.game_date == target_date,
            Game.game_status != 'finished'
        ).all()
        
        finished_games = session.query(Game).filter(
            Game.game_date == target_date,
            Game.game_status == 'finished'
        ).all()
        
        stats['already_finished'] = len(finished_games)
    
    if not scheduled_games:
        if not quiet:
            logger.info(f"No scheduled games to check for {target_date}")
            if stats['already_finished'] > 0:
                logger.info(f"  {stats['already_finished']} games already finished")
        return stats
    
    if not quiet:
        logger.info(f"Checking {len(scheduled_games)} scheduled games for {target_date}")
    
    # Initialize NBA API collector
    try:
        nba_collector = NBAPICollector()
    except Exception as e:
        logger.error(f"Failed to initialize NBA API collector: {e}")
        return stats
    
    # Check each scheduled game
    for game in scheduled_games:
        stats['checked'] += 1
        
        try:
            # Check if game_id is betting API format (starts with date) or NBA format (starts with 00)
            if game.game_id.startswith('2026') or len(game.game_id) > 10:
                # Betting API format - need to find NBA game ID
                nba_game_id = nba_collector.find_nba_game_id(
                    game.home_team_id,
                    game.away_team_id,
                    game.game_date
                )
                if not nba_game_id:
                    if not quiet:
                        logger.debug(f"Could not find NBA game ID for {game.game_id}")
                    stats['still_scheduled'] += 1
                    continue
                game_details = nba_collector.get_game_details(nba_game_id)
            else:
                # NBA format - use directly
                game_details = nba_collector.get_game_details(game.game_id)
            
            if not game_details:
                # Try alternative: check if game has scores in database already
                if not quiet:
                    logger.debug(f"No API response for game {game.game_id}")
                stats['still_scheduled'] += 1
                continue
            
            # Check if game is finished
            if game_details.get('game_status') == 'finished' or (
                game_details.get('home_score') is not None and 
                game_details.get('away_score') is not None
            ):
                home_score = game_details.get('home_score')
                away_score = game_details.get('away_score')
                
                if home_score is not None and away_score is not None:
                    # Update game in database
                    with db.get_session() as session:
                        game_to_update = session.query(Game).filter_by(
                            game_id=game.game_id
                        ).first()
                        
                        if game_to_update:
                            game_to_update.home_score = home_score
                            game_to_update.away_score = away_score
                            game_to_update.point_differential = home_score - away_score
                            game_to_update.winner = (
                                game_to_update.home_team_id if home_score > away_score 
                                else game_to_update.away_team_id
                            )
                            game_to_update.game_status = 'finished'
                            session.commit()
                            
                            stats['updated'] += 1
                            if not quiet:
                                away_team = db.get_team(game.away_team_id)
                                home_team = db.get_team(game.home_team_id)
                                away_name = away_team.team_name if away_team else game.away_team_id
                                home_name = home_team.team_name if home_team else game.home_team_id
                                logger.info(f"  Updated: {away_name} @ {home_name}: {away_score}-{home_score}")
                else:
                    stats['still_scheduled'] += 1
            else:
                stats['still_scheduled'] += 1
                
        except Exception as e:
            stats['errors'] += 1
            if not quiet:
                logger.warning(f"Error checking game {game.game_id}: {e}")
    
    return stats


def main():
    """Main function to update finished games."""
    parser = ArgumentParser(description='Update finished games with final scores')
    parser.add_argument('--date', type=str, help='Date to check (YYYY-MM-DD, default: today)')
    parser.add_argument('--days-back', type=int, default=1,
                       help='Number of days back to check (default: 1)')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress detailed output (for automation)')
    args = parser.parse_args()
    
    # Determine target dates
    if args.date:
        target_dates = [date.fromisoformat(args.date)]
    else:
        # Check today and days back
        target_dates = []
        for i in range(args.days_back + 1):
            target_dates.append(date.today() - timedelta(days=i))
    
    if not args.quiet:
        print("=" * 70)
        print("UPDATE FINISHED GAMES")
        print("=" * 70)
        print(f"Checking {len(target_dates)} date(s)")
        print("=" * 70)
    
    db = DatabaseManager()
    
    total_stats = {
        'checked': 0,
        'updated': 0,
        'already_finished': 0,
        'still_scheduled': 0,
        'errors': 0
    }
    
    for target_date in target_dates:
        if not args.quiet:
            print(f"\n[{target_date}]")
            print("-" * 70)
        
        stats = update_game_scores(target_date, db, quiet=args.quiet)
        
        for key in total_stats:
            total_stats[key] += stats[key]
    
    if not args.quiet:
        print("\n" + "=" * 70)
        print("SUMMARY")
        print("=" * 70)
        print(f"Games checked: {total_stats['checked']}")
        print(f"Games updated: {total_stats['updated']}")
        print(f"Already finished: {total_stats['already_finished']}")
        print(f"Still scheduled: {total_stats['still_scheduled']}")
        print(f"Errors: {total_stats['errors']}")
        print("=" * 70)
    else:
        # Minimal output for automation
        print(f"Updated {total_stats['updated']} games, {total_stats['still_scheduled']} still scheduled")
    
    return 0 if total_stats['errors'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())

