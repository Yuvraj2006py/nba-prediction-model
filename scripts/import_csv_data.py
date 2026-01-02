"""Import CSV data files into the database.

This script imports:
1. Game data from Basketball Reference schedule CSVs
2. Team stats from formatted CSV files
3. Player stats from formatted CSV files
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
# Force SQLite usage
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
import csv
import argparse
from typing import List, Dict, Any, Optional
from datetime import date, datetime

try:
    from tqdm import tqdm
    HAS_TQDM = True
except ImportError:
    HAS_TQDM = False
    # Fallback: create a dummy tqdm that just returns the iterable
    def tqdm(iterable, desc=""):
        return iterable

from src.database.db_manager import DatabaseManager
from src.database.models import TeamStats, PlayerStats, Game
from sqlalchemy import and_
from scripts.collect_seasons_csv_bball_ref import (
    parse_schedule_csv,
    import_csv_games_to_db,
    initialize_teams,
    TEAM_DATA
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/csv_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def import_team_stats_csv(csv_path: Path, db_manager: DatabaseManager, replace_existing: bool = True) -> Dict[str, int]:
    """
    Import team stats from CSV file.
    
    Args:
        csv_path: Path to team stats CSV file
        db_manager: Database manager
        replace_existing: If True, replace existing stats; if False, skip duplicates
        
    Returns:
        Dictionary with import statistics
    """
    stats = {
        'rows_processed': 0,
        'stats_imported': 0,
        'stats_updated': 0,
        'stats_skipped': 0,
        'errors': 0
    }
    
    logger.info(f"Importing team stats from {csv_path.name}...")
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in tqdm(reader, desc="Importing team stats"):
                try:
                    stats['rows_processed'] += 1
                    
                    # Parse game_date
                    game_date_str = row.get('game_date', '').strip()
                    if not game_date_str:
                        continue
                    
                    try:
                        game_date = datetime.strptime(game_date_str, '%Y-%m-%d').date()
                    except ValueError:
                        logger.warning(f"Could not parse date: {game_date_str}")
                        continue
                    
                    # Get game_id from CSV (may be NBA API format like 0022200001)
                    csv_game_id = row.get('game_id', '').strip()
                    if not csv_game_id:
                        continue
                    
                    # Get team_id
                    team_id = row.get('team_id', '').strip()
                    if not team_id:
                        continue
                    
                    # Get game_date to match with actual games
                    game_date_str = row.get('game_date', '').strip()
                    if not game_date_str:
                        continue
                    
                    try:
                        game_date = datetime.strptime(game_date_str, '%Y-%m-%d').date()
                    except ValueError:
                        logger.warning(f"Could not parse date: {game_date_str}")
                        continue
                    
                    # Find the actual game in database by date and team_id
                    with db_manager.get_session() as session:
                        game = session.query(Game).filter(
                            and_(
                                Game.game_date == game_date,
                                ((Game.home_team_id == team_id) | (Game.away_team_id == team_id))
                            )
                        ).first()
                    
                    if not game:
                        logger.debug(f"Could not find game for team {team_id} on {game_date}")
                        continue
                    
                    game_id = game.game_id
                    
                    # Check if stats already exist
                    existing_stats = db_manager.get_team_stats(game_id, team_id)
                    
                    if existing_stats and not replace_existing:
                        stats['stats_skipped'] += 1
                        continue
                    
                    # Prepare team stats data
                    team_stats_data = {
                        'game_id': game_id,
                        'team_id': team_id,
                        'is_home': row.get('is_home', '').strip().lower() in ('true', '1', 'yes'),
                        'points': int(row.get('points', 0) or 0),
                        'field_goals_made': int(row.get('field_goals_made', 0) or 0),
                        'field_goals_attempted': int(row.get('field_goals_attempted', 0) or 0),
                        'field_goal_percentage': float(row.get('field_goal_percentage', 0) or 0),
                        'three_pointers_made': int(row.get('three_pointers_made', 0) or 0),
                        'three_pointers_attempted': int(row.get('three_pointers_attempted', 0) or 0),
                        'three_point_percentage': float(row.get('three_point_percentage', 0) or 0),
                        'free_throws_made': int(row.get('free_throws_made', 0) or 0),
                        'free_throws_attempted': int(row.get('free_throws_attempted', 0) or 0),
                        'free_throw_percentage': float(row.get('free_throw_percentage', 0) or 0),
                        'rebounds_offensive': int(row.get('rebounds_offensive', 0) or 0),
                        'rebounds_defensive': int(row.get('rebounds_defensive', 0) or 0),
                        'rebounds_total': int(row.get('rebounds_total', 0) or 0),
                        'assists': int(row.get('assists', 0) or 0),
                        'steals': int(row.get('steals', 0) or 0),
                        'blocks': int(row.get('blocks', 0) or 0),
                        'turnovers': int(row.get('turnovers', 0) or 0),
                        'personal_fouls': int(row.get('personal_fouls', 0) or 0),
                    }
                    
                    # Optional advanced metrics
                    if row.get('offensive_rating'):
                        try:
                            team_stats_data['offensive_rating'] = float(row['offensive_rating'])
                        except (ValueError, TypeError):
                            pass
                    
                    if row.get('defensive_rating'):
                        try:
                            team_stats_data['defensive_rating'] = float(row['defensive_rating'])
                        except (ValueError, TypeError):
                            pass
                    
                    if row.get('pace'):
                        try:
                            team_stats_data['pace'] = float(row['pace'])
                        except (ValueError, TypeError):
                            pass
                    
                    if row.get('true_shooting_percentage'):
                        try:
                            team_stats_data['true_shooting_percentage'] = float(row['true_shooting_percentage'])
                        except (ValueError, TypeError):
                            pass
                    
                    if row.get('effective_field_goal_percentage'):
                        try:
                            team_stats_data['effective_field_goal_percentage'] = float(row['effective_field_goal_percentage'])
                        except (ValueError, TypeError):
                            pass
                    
                    # Insert or update (insert_team_stats handles both)
                    had_existing = existing_stats is not None
                    db_manager.insert_team_stats(team_stats_data)
                    if had_existing:
                        stats['stats_updated'] += 1
                    else:
                        stats['stats_imported'] += 1
                
                except Exception as e:
                    logger.warning(f"Error importing team stats row: {e}")
                    stats['errors'] += 1
                    continue
        
        logger.info(f"Team stats import complete: {stats['stats_imported']} imported, {stats['stats_updated']} updated, {stats['stats_skipped']} skipped, {stats['errors']} errors")
        return stats
    
    except Exception as e:
        logger.error(f"Error reading team stats CSV: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return stats


def import_player_stats_csv(csv_path: Path, db_manager: DatabaseManager, replace_existing: bool = True) -> Dict[str, int]:
    """
    Import player stats from CSV file.
    
    Args:
        csv_path: Path to player stats CSV file
        db_manager: Database manager
        replace_existing: If True, replace existing stats; if False, skip duplicates
        
    Returns:
        Dictionary with import statistics
    """
    stats = {
        'rows_processed': 0,
        'stats_imported': 0,
        'stats_updated': 0,
        'stats_skipped': 0,
        'errors': 0
    }
    
    logger.info(f"Importing player stats from {csv_path.name}...")
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in tqdm(reader, desc="Importing player stats"):
                try:
                    stats['rows_processed'] += 1
                    
                    # Get game_date to match with actual games
                    game_date_str = row.get('game_date', '').strip()
                    if not game_date_str:
                        continue
                    
                    try:
                        game_date = datetime.strptime(game_date_str, '%Y-%m-%d').date()
                    except ValueError:
                        logger.warning(f"Could not parse date: {game_date_str}")
                        continue
                    
                    # Get required fields
                    csv_game_id = row.get('game_id', '').strip()
                    player_id = row.get('player_id', '').strip()
                    team_id = row.get('team_id', '').strip()
                    
                    if not player_id or not team_id:
                        continue
                    
                    # Find the actual game in database by date and team_id
                    with db_manager.get_session() as session:
                        game = session.query(Game).filter(
                            and_(
                                Game.game_date == game_date,
                                ((Game.home_team_id == team_id) | (Game.away_team_id == team_id))
                            )
                        ).first()
                    
                    if not game:
                        logger.debug(f"Could not find game for team {team_id} on {game_date}")
                        continue
                    
                    game_id = game.game_id
                    
                    # Check if stats already exist
                    existing_stats = db_manager.get_player_stats(game_id, player_id)
                    
                    if existing_stats and not replace_existing:
                        stats['stats_skipped'] += 1
                        continue
                    
                    # Prepare player stats data
                    player_stats_data = {
                        'game_id': game_id,
                        'player_id': player_id,
                        'team_id': team_id,
                        'player_name': row.get('player_name', '').strip(),
                        'minutes_played': row.get('minutes_played', '0:00').strip(),
                        'points': int(row.get('points', 0) or 0),
                        'rebounds': int(row.get('rebounds', 0) or 0),
                        'assists': int(row.get('assists', 0) or 0),
                        'field_goals_made': int(row.get('field_goals_made', 0) or 0),
                        'field_goals_attempted': int(row.get('field_goals_attempted', 0) or 0),
                        'three_pointers_made': int(row.get('three_pointers_made', 0) or 0),
                        'three_pointers_attempted': int(row.get('three_pointers_attempted', 0) or 0),
                        'free_throws_made': int(row.get('free_throws_made', 0) or 0),
                        'free_throws_attempted': int(row.get('free_throws_attempted', 0) or 0),
                    }
                    
                    # Optional plus_minus
                    if row.get('plus_minus'):
                        try:
                            player_stats_data['plus_minus'] = int(row['plus_minus'])
                        except (ValueError, TypeError):
                            pass
                    
                    # Optional injury_status
                    if row.get('injury_status'):
                        player_stats_data['injury_status'] = row['injury_status'].strip()
                    
                    # Insert or update (insert_player_stats handles both)
                    had_existing = existing_stats is not None
                    db_manager.insert_player_stats(player_stats_data)
                    if had_existing:
                        stats['stats_updated'] += 1
                    else:
                        stats['stats_imported'] += 1
                
                except Exception as e:
                    logger.warning(f"Error importing player stats row: {e}")
                    stats['errors'] += 1
                    continue
        
        logger.info(f"Player stats import complete: {stats['stats_imported']} imported, {stats['stats_updated']} updated, {stats['stats_skipped']} skipped, {stats['errors']} errors")
        return stats
    
    except Exception as e:
        logger.error(f"Error reading player stats CSV: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return stats


def determine_season_from_date(game_date: date) -> str:
    """
    Determine NBA season from game date.
    NBA seasons run Oct-Apr, so:
    - Oct-Dec: season is YYYY-YY+1 (e.g., Oct 2022 = 2022-23)
    - Jan-Jun: season is YYYY-1-YY (e.g., Jan 2023 = 2022-23)
    
    Args:
        game_date: Game date
        
    Returns:
        Season string (e.g., '2022-23')
    """
    year = game_date.year
    month = game_date.month
    
    if month >= 10:  # Oct, Nov, Dec
        # Season starts in this year
        season_start = year
        season_end = (year + 1) % 100
    else:  # Jan-Jun
        # Season started in previous year
        season_start = year - 1
        season_end = year % 100
    
    return f"{season_start}-{season_end:02d}"


def parse_schedule_csv_with_auto_season(csv_path: Path) -> List[Dict[str, Any]]:
    """
    Parse Basketball Reference schedule CSV file and determine season from dates.
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        List of game dictionaries with correct season
    """
    games = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            # Use csv.reader first to get raw rows, then parse headers manually
            # to handle duplicate column names (e.g., 'PTS' appears twice)
            reader = csv.reader(f)
            header_row = next(reader)
            
            # Find column indices
            col_indices = {}
            for idx, col_name in enumerate(header_row):
                if col_name not in col_indices:
                    col_indices[col_name] = []
                col_indices[col_name].append(idx)
            
            # Find score column indices - handle duplicate 'PTS' columns
            visitor_pts_idx = None
            home_pts_idx = None
            
            # Find visitor score column (first 'PTS' after 'Visitor/Neutral')
            if 'Visitor/Neutral' in col_indices:
                visitor_col_idx = col_indices['Visitor/Neutral'][0]
                # Look for 'PTS' column that comes after visitor team column
                for idx, col_name in enumerate(header_row):
                    if col_name == 'PTS' and idx > visitor_col_idx:
                        visitor_pts_idx = idx
                        break
            
            # Find home score column (second 'PTS' after 'Home/Neutral')
            if 'Home/Neutral' in col_indices:
                home_col_idx = col_indices['Home/Neutral'][0]
                # Look for 'PTS' column that comes after home team column
                for idx, col_name in enumerate(header_row):
                    if col_name == 'PTS' and idx > home_col_idx and idx != visitor_pts_idx:
                        home_pts_idx = idx
                        break
            
            # Fallback: if we can't find by position, use first and second 'PTS' columns
            if visitor_pts_idx is None or home_pts_idx is None:
                pts_indices = [idx for idx, col_name in enumerate(header_row) if col_name == 'PTS']
                if len(pts_indices) >= 2:
                    visitor_pts_idx = pts_indices[0]
                    home_pts_idx = pts_indices[1]
                elif len(pts_indices) == 1:
                    visitor_pts_idx = pts_indices[0]
                    # Try to find home score in other columns
                    for idx, col_name in enumerate(header_row):
                        if col_name in ['PTS.1', 'Home PTS', 'Home/Neutral PTS']:
                            home_pts_idx = idx
                            break
            
            for row_idx, row in enumerate(reader):
                try:
                    # Skip empty rows
                    if not any(row):
                        continue
                    
                    # Extract date
                    date_idx = col_indices.get('Date', [None])[0]
                    if date_idx is None or date_idx >= len(row):
                        continue
                    date_str = row[date_idx].strip() if date_idx < len(row) else ''
                    if not date_str:
                        continue
                    
                    # Parse date - try multiple formats
                    game_date = None
                    date_formats = ['%a, %b %d, %Y', '%b %d, %Y', '%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y']
                    for fmt in date_formats:
                        try:
                            game_date = datetime.strptime(date_str, fmt).date()
                            break
                        except ValueError:
                            continue
                    
                    if not game_date:
                        logger.debug(f"Could not parse date: '{date_str}' in row {row_idx}")
                        continue
                    
                    # Determine season from actual date
                    season = determine_season_from_date(game_date)
                    season_start_year = int(season.split('-')[0])
                    
                    # Extract visitor team
                    visitor_idx = col_indices.get('Visitor/Neutral', col_indices.get('Visitor', [None]))[0]
                    if visitor_idx is None or visitor_idx >= len(row):
                        continue
                    visitor_str = row[visitor_idx].strip() if visitor_idx < len(row) else ''
                    if not visitor_str:
                        continue
                    
                    # Extract home team
                    home_idx = col_indices.get('Home/Neutral', col_indices.get('Home', [None]))[0]
                    if home_idx is None or home_idx >= len(row):
                        continue
                    home_str = row[home_idx].strip() if home_idx < len(row) else ''
                    if not home_str:
                        continue
                    
                    # Find team abbreviations
                    away_abbrev = None
                    home_abbrev = None
                    
                    # Check if it's already an abbreviation (3 letters)
                    if len(visitor_str) == 3 and visitor_str.isupper():
                        away_abbrev = visitor_str
                    else:
                        # Try to find abbreviation in team name
                        for abbrev, (_, team_name, _, _, _) in TEAM_DATA.items():
                            if abbrev in visitor_str.upper() or team_name in visitor_str:
                                away_abbrev = abbrev
                                break
                    
                    if len(home_str) == 3 and home_str.isupper():
                        home_abbrev = home_str
                    else:
                        for abbrev, (_, team_name, _, _, _) in TEAM_DATA.items():
                            if abbrev in home_str.upper() or team_name in home_str:
                                home_abbrev = abbrev
                                break
                    
                    if not away_abbrev or not home_abbrev:
                        logger.debug(f"Could not identify teams: '{visitor_str}' vs '{home_str}'")
                        continue
                    
                    # Get team IDs
                    away_team_data = TEAM_DATA.get(away_abbrev)
                    home_team_data = TEAM_DATA.get(home_abbrev)
                    
                    if not away_team_data or not home_team_data:
                        logger.debug(f"Unknown teams: {away_abbrev} or {home_abbrev}")
                        continue
                    
                    away_team_id = away_team_data[0]
                    home_team_id = home_team_data[0]
                    
                    # Extract scores using positional indices
                    away_score = None
                    home_score = None
                    
                    # Extract visitor score from first PTS column
                    if visitor_pts_idx is not None and visitor_pts_idx < len(row):
                        score_str = row[visitor_pts_idx].strip()
                        if score_str and score_str.isdigit():
                            away_score = int(score_str)
                    
                    # Extract home score from second PTS column
                    if home_pts_idx is not None and home_pts_idx < len(row):
                        score_str = row[home_pts_idx].strip()
                        if score_str and score_str.isdigit():
                            home_score = int(score_str)
                    
                    # Fallback: if positional extraction failed, try column name lookup
                    if away_score is None or home_score is None:
                        # Convert row to dict for fallback (but be aware of duplicate keys)
                        row_dict = dict(zip(header_row, row))
                        
                        # Try different column name variations for visitor
                        if away_score is None:
                            for col_name in ['PTS', 'Visitor PTS', 'Visitor/Neutral PTS', 'Away PTS']:
                                if col_name in row_dict and row_dict[col_name].strip().isdigit():
                                    away_score = int(row_dict[col_name].strip())
                                    break
                        
                        # Try different column name variations for home
                        if home_score is None:
                            for col_name in ['PTS.1', 'Home PTS', 'Home/Neutral PTS']:
                                if col_name in row_dict and row_dict[col_name].strip().isdigit():
                                    home_score = int(row_dict[col_name].strip())
                                    break
                        
                        # Last resort: find any numeric values that look like scores
                        if away_score is None or home_score is None:
                            found_scores = []
                            for idx, value in enumerate(row):
                                if value and value.strip().isdigit():
                                    score_val = int(value.strip())
                                    if 50 <= score_val <= 200:  # Reasonable score range
                                        if score_val not in found_scores:  # Avoid duplicates
                                            found_scores.append(score_val)
                            
                            # Assign first unique score to away, second to home
                            if away_score is None and len(found_scores) > 0:
                                away_score = found_scores[0]
                            if home_score is None and len(found_scores) > 1:
                                home_score = found_scores[1]
                            elif home_score is None and len(found_scores) > 0 and found_scores[0] != away_score:
                                # If we only found one score but it's different from away_score
                                home_score = found_scores[0] if away_score != found_scores[0] else None
                    
                    # Determine season type from context (Regular Season by default)
                    season_type = 'Regular Season'
                    
                    # Generate game ID
                    game_id = f"{season_start_year}{game_date.strftime('%m%d')}{away_abbrev}{home_abbrev}"
                    
                    game_data = {
                        'game_id': game_id,
                        'season': season,
                        'season_type': season_type,
                        'game_date': game_date,
                        'home_team_id': home_team_id,
                        'away_team_id': away_team_id,
                        'home_score': home_score,
                        'away_score': away_score,
                        'game_status': 'finished' if (home_score is not None and away_score is not None) else 'scheduled'
                    }
                    
                    games.append(game_data)
                
                except Exception as e:
                    logger.debug(f"Error parsing row {row_idx}: {e}")
                    continue
        
        logger.info(f"Parsed {len(games)} games from {csv_path.name}")
        return games
    
    except Exception as e:
        logger.error(f"Error parsing CSV file {csv_path}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def import_all_csv_data(
    csv_dir: Path,
    db_manager: DatabaseManager,
    import_games: bool = True,
    import_team_stats: bool = True,
    import_player_stats: bool = True,
    replace_existing: bool = True
) -> Dict[str, Any]:
    """
    Import all CSV data from the specified directory.
    
    Args:
        csv_dir: Directory containing CSV files
        db_manager: Database manager
        import_games: Whether to import game CSVs
        import_team_stats: Whether to import team stats CSVs
        import_player_stats: Whether to import player stats CSVs
        replace_existing: If True, replace existing records
        
    Returns:
        Dictionary with import statistics
    """
    total_stats = {
        'games_imported': 0,
        'games_updated': 0,
        'team_stats_imported': 0,
        'team_stats_updated': 0,
        'player_stats_imported': 0,
        'player_stats_updated': 0,
        'errors': 0
    }
    
    logger.info("=" * 70)
    logger.info("CSV Data Import")
    logger.info("=" * 70)
    
    # Initialize teams
    initialize_teams(db_manager)
    
    # Import games from schedule CSVs
    if import_games:
        logger.info("\n[Step 1] Importing games from schedule CSVs...")
        game_files = sorted(csv_dir.glob("NBA_*_games-*.csv"))
        
        if not game_files:
            logger.warning("No game CSV files found")
        else:
            # Process all files and determine season from dates
            logger.info(f"Found {len(game_files)} game CSV files")
            all_games = []
            
            for csv_file in game_files:
                logger.info(f"  Parsing {csv_file.name}...")
                games = parse_schedule_csv_with_auto_season(csv_file)
                all_games.extend(games)
                logger.info(f"    Found {len(games)} games")
            
            # Group games by season
            games_by_season = {}
            for game in all_games:
                season = game['season']
                if season not in games_by_season:
                    games_by_season[season] = []
                games_by_season[season].append(game)
            
            # Import games by season
            for season in sorted(games_by_season.keys()):
                games = games_by_season[season]
                logger.info(f"\nImporting {len(games)} games for season {season}...")
                import_stats = import_csv_games_to_db(games, db_manager, replace_existing=replace_existing)
                total_stats['games_imported'] += import_stats['games_imported']
                total_stats['games_updated'] += import_stats['games_updated']
                total_stats['errors'] += import_stats['errors']
    
    # Import team stats
    if import_team_stats:
        logger.info("\n[Step 2] Importing team stats from CSV files...")
        stats_dir = csv_dir / "stats"
        
        if stats_dir.exists():
            team_stats_files = sorted(stats_dir.glob("team_stats_*.csv"))
            
            for csv_file in team_stats_files:
                logger.info(f"\nProcessing {csv_file.name}...")
                import_stats = import_team_stats_csv(csv_file, db_manager, replace_existing=replace_existing)
                total_stats['team_stats_imported'] += import_stats['stats_imported']
                total_stats['team_stats_updated'] += import_stats['stats_updated']
                total_stats['errors'] += import_stats['errors']
        else:
            logger.warning("Stats directory not found")
    
    # Import player stats
    if import_player_stats:
        logger.info("\n[Step 3] Importing player stats from CSV files...")
        stats_dir = csv_dir / "stats"
        
        if stats_dir.exists():
            player_stats_files = sorted(stats_dir.glob("player_stats_*.csv"))
            
            for csv_file in player_stats_files:
                logger.info(f"\nProcessing {csv_file.name}...")
                import_stats = import_player_stats_csv(csv_file, db_manager, replace_existing=replace_existing)
                total_stats['player_stats_imported'] += import_stats['stats_imported']
                total_stats['player_stats_updated'] += import_stats['stats_updated']
                total_stats['errors'] += import_stats['errors']
        else:
            logger.warning("Stats directory not found")
    
    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("Import Complete - Summary")
    logger.info("=" * 70)
    logger.info(f"Games imported: {total_stats['games_imported']}")
    logger.info(f"Games updated: {total_stats['games_updated']}")
    logger.info(f"Team stats imported: {total_stats['team_stats_imported']}")
    logger.info(f"Team stats updated: {total_stats['team_stats_updated']}")
    logger.info(f"Player stats imported: {total_stats['player_stats_imported']}")
    logger.info(f"Player stats updated: {total_stats['player_stats_updated']}")
    logger.info(f"Total errors: {total_stats['errors']}")
    logger.info("=" * 70)
    
    return total_stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import CSV data into database")
    parser.add_argument(
        '--csv-dir',
        type=str,
        default=None,
        help='Directory containing CSV files (default: data/raw/csv)'
    )
    parser.add_argument(
        '--no-games',
        action='store_true',
        help='Skip importing games'
    )
    parser.add_argument(
        '--no-team-stats',
        action='store_true',
        help='Skip importing team stats'
    )
    parser.add_argument(
        '--no-player-stats',
        action='store_true',
        help='Skip importing player stats'
    )
    parser.add_argument(
        '--no-replace',
        action='store_true',
        help='Do not replace existing records (skip duplicates)'
    )
    
    args = parser.parse_args()
    
    # Set up CSV directory
    if args.csv_dir:
        csv_dir = Path(args.csv_dir)
    else:
        csv_dir = Path(project_root) / "data" / "raw" / "csv"
    
    if not csv_dir.exists():
        logger.error(f"CSV directory not found: {csv_dir}")
        sys.exit(1)
    
    logger.info(f"CSV directory: {csv_dir}")
    
    # Initialize database
    db_manager = DatabaseManager()
    db_manager.create_tables()
    
    if not db_manager.test_connection():
        logger.error("Database connection failed!")
        sys.exit(1)
    
    logger.info("[OK] Database connection successful")
    
    # Import data
    try:
        import_all_csv_data(
            csv_dir,
            db_manager,
            import_games=not args.no_games,
            import_team_stats=not args.no_team_stats,
            import_player_stats=not args.no_player_stats,
            replace_existing=not args.no_replace
        )
    except KeyboardInterrupt:
        logger.info("\n\nImport interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

