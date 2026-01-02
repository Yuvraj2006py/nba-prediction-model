"""Fix game scores in database by re-importing from CSV files.

This script re-imports game scores from CSV files to fix the issue where
duplicate 'PTS' columns caused both home_score and away_score to be set to
the same value.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from src.database.db_manager import DatabaseManager
from src.database.models import Game
from scripts.import_csv_data import parse_schedule_csv_with_auto_season
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/fix_game_scores.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fix_game_scores_from_csv(csv_dir: Path = None, dry_run: bool = False) -> dict:
    """
    Re-import game scores from CSV files to fix duplicate PTS column issue.
    
    Args:
        csv_dir: Directory containing CSV files (default: data/raw/csv)
        dry_run: If True, only report what would be changed without updating database
        
    Returns:
        Dictionary with statistics about the fix operation
    """
    if csv_dir is None:
        csv_dir = Path('data/raw/csv')
    
    stats = {
        'csv_files_processed': 0,
        'games_found_in_csv': 0,
        'games_updated': 0,
        'games_skipped': 0,
        'errors': 0,
        'games_with_identical_scores': 0,
        'games_fixed': 0
    }
    
    db_manager = DatabaseManager()
    
    # Find all CSV files
    csv_files = list(csv_dir.glob('NBA_*_games-*.csv'))
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    for csv_file in csv_files:
        try:
            logger.info(f"Processing {csv_file.name}...")
            stats['csv_files_processed'] += 1
            
            # Parse CSV file
            games_from_csv = parse_schedule_csv_with_auto_season(csv_file)
            stats['games_found_in_csv'] += len(games_from_csv)
            
            with db_manager.get_session() as session:
                for game_data in games_from_csv:
                    try:
                        game_id = game_data['game_id']
                        home_score = game_data.get('home_score')
                        away_score = game_data.get('away_score')
                        
                        # Skip if scores are missing
                        if home_score is None or away_score is None:
                            stats['games_skipped'] += 1
                            continue
                        
                        # Find game in database
                        game = session.query(Game).filter_by(game_id=game_id).first()
                        
                        if not game:
                            # Game doesn't exist in database, skip
                            stats['games_skipped'] += 1
                            continue
                        
                        # Check if scores are identical (the bug we're fixing)
                        if game.home_score == game.away_score and game.home_score is not None:
                            stats['games_with_identical_scores'] += 1
                            
                            # Update with correct scores from CSV
                            if not dry_run:
                                game.home_score = home_score
                                game.away_score = away_score
                                
                                # Recalculate winner and point_differential
                                if home_score > away_score:
                                    game.winner = game.home_team_id
                                elif away_score > home_score:
                                    game.winner = game.away_team_id
                                else:
                                    game.winner = None
                                
                                game.point_differential = home_score - away_score
                                stats['games_fixed'] += 1
                            else:
                                logger.info(f"Would fix game {game_id}: {game.home_score}-{game.away_score} -> {home_score}-{away_score}")
                                stats['games_fixed'] += 1
                        
                        # Also update if scores are different but might be wrong
                        elif game.home_score != home_score or game.away_score != away_score:
                            if not dry_run:
                                game.home_score = home_score
                                game.away_score = away_score
                                
                                # Recalculate winner and point_differential
                                if home_score > away_score:
                                    game.winner = game.home_team_id
                                elif away_score > home_score:
                                    game.winner = game.away_team_id
                                else:
                                    game.winner = None
                                
                                game.point_differential = home_score - away_score
                                stats['games_updated'] += 1
                        
                    except Exception as e:
                        logger.warning(f"Error processing game {game_data.get('game_id', 'unknown')}: {e}")
                        stats['errors'] += 1
                        continue
                
                if not dry_run:
                    session.commit()
                    logger.info(f"Committed changes for {csv_file.name}")
            
        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            stats['errors'] += 1
            continue
    
    return stats


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Fix game scores in database from CSV files')
    parser.add_argument('--dry-run', action='store_true', help='Only report what would be changed')
    parser.add_argument('--csv-dir', type=Path, default=None, help='Directory containing CSV files')
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("Fix Game Scores from CSV")
    logger.info("=" * 70)
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No changes will be made to database")
    
    stats = fix_game_scores_from_csv(csv_dir=args.csv_dir, dry_run=args.dry_run)
    
    logger.info("=" * 70)
    logger.info("Fix Statistics:")
    logger.info(f"  CSV files processed: {stats['csv_files_processed']}")
    logger.info(f"  Games found in CSV: {stats['games_found_in_csv']}")
    logger.info(f"  Games with identical scores (bug): {stats['games_with_identical_scores']}")
    logger.info(f"  Games fixed: {stats['games_fixed']}")
    logger.info(f"  Games updated: {stats['games_updated']}")
    logger.info(f"  Games skipped: {stats['games_skipped']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("=" * 70)



