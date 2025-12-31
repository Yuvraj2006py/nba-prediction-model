"""Collect Basketball Reference data for historical seasons (2019-20, 2020-21, 2021-22) using a separate database.
This script ONLY uses Basketball Reference - no NBA API."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import argparse
import re
from typing import List, Dict, Any, Optional
from datetime import date, datetime
from tqdm import tqdm

try:
    from src.data_collectors.basketball_reference_selenium import BasketballReferenceSeleniumCollector
    USE_SELENIUM = True
except ImportError:
    USE_SELENIUM = False

from src.data_collectors.basketball_reference_collector import BasketballReferenceCollector
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats, Team

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/bball_ref_historical_collection.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)
# Enable debug logging for this module to see parsing details
logger.setLevel(logging.DEBUG)
logger = logging.getLogger(__name__)

# Historical seasons to collect
HISTORICAL_SEASONS = ['2019-20', '2020-21', '2021-22']

# Separate database path for historical data
HISTORICAL_DB_PATH = Path(project_root) / "data" / "nba_predictions_historical.db"

# Team mapping: Basketball Reference abbreviation -> (team_id, team_name, city, conference, division)
TEAM_DATA = {
    'ATL': ('1610612737', 'Atlanta Hawks', 'Atlanta', 'Eastern', 'Southeast'),
    'BOS': ('1610612738', 'Boston Celtics', 'Boston', 'Eastern', 'Atlantic'),
    'BRK': ('1610612751', 'Brooklyn Nets', 'Brooklyn', 'Eastern', 'Atlantic'),
    'CHO': ('1610612766', 'Charlotte Hornets', 'Charlotte', 'Eastern', 'Southeast'),
    'CHI': ('1610612741', 'Chicago Bulls', 'Chicago', 'Eastern', 'Central'),
    'CLE': ('1610612739', 'Cleveland Cavaliers', 'Cleveland', 'Eastern', 'Central'),
    'DAL': ('1610612742', 'Dallas Mavericks', 'Dallas', 'Western', 'Southwest'),
    'DEN': ('1610612743', 'Denver Nuggets', 'Denver', 'Western', 'Northwest'),
    'DET': ('1610612765', 'Detroit Pistons', 'Detroit', 'Eastern', 'Central'),
    'GSW': ('1610612744', 'Golden State Warriors', 'Golden State', 'Western', 'Pacific'),
    'HOU': ('1610612745', 'Houston Rockets', 'Houston', 'Western', 'Southwest'),
    'IND': ('1610612754', 'Indiana Pacers', 'Indiana', 'Eastern', 'Central'),
    'LAC': ('1610612746', 'LA Clippers', 'LA', 'Western', 'Pacific'),
    'LAL': ('1610612747', 'Los Angeles Lakers', 'Los Angeles', 'Western', 'Pacific'),
    'MEM': ('1610612763', 'Memphis Grizzlies', 'Memphis', 'Western', 'Southwest'),
    'MIA': ('1610612748', 'Miami Heat', 'Miami', 'Eastern', 'Southeast'),
    'MIL': ('1610612749', 'Milwaukee Bucks', 'Milwaukee', 'Eastern', 'Central'),
    'MIN': ('1610612750', 'Minnesota Timberwolves', 'Minnesota', 'Western', 'Northwest'),
    'NOP': ('1610612740', 'New Orleans Pelicans', 'New Orleans', 'Western', 'Southwest'),
    'NYK': ('1610612752', 'New York Knicks', 'New York', 'Eastern', 'Atlantic'),
    'OKC': ('1610612760', 'Oklahoma City Thunder', 'Oklahoma City', 'Western', 'Northwest'),
    'ORL': ('1610612753', 'Orlando Magic', 'Orlando', 'Eastern', 'Southeast'),
    'PHI': ('1610612755', 'Philadelphia 76ers', 'Philadelphia', 'Eastern', 'Atlantic'),
    'PHO': ('1610612756', 'Phoenix Suns', 'Phoenix', 'Western', 'Pacific'),
    'POR': ('1610612757', 'Portland Trail Blazers', 'Portland', 'Western', 'Northwest'),
    'SAC': ('1610612758', 'Sacramento Kings', 'Sacramento', 'Western', 'Pacific'),
    'SAS': ('1610612759', 'San Antonio Spurs', 'San Antonio', 'Western', 'Southwest'),
    'TOR': ('1610612761', 'Toronto Raptors', 'Toronto', 'Eastern', 'Atlantic'),
    'UTA': ('1610612762', 'Utah Jazz', 'Utah', 'Western', 'Northwest'),
    'WAS': ('1610612764', 'Washington Wizards', 'Washington', 'Eastern', 'Southeast')
}

# Reverse mapping: team_id -> BR abbreviation
TEAM_ID_TO_ABBREV = {team_id: abbrev for abbrev, (team_id, _, _, _, _) in TEAM_DATA.items()}


def initialize_teams(db_manager: DatabaseManager):
    """Initialize teams in database from TEAM_DATA."""
    logger.info("Initializing teams in database...")
    
    teams_added = 0
    for abbrev, (team_id, team_name, city, conference, division) in TEAM_DATA.items():
        try:
            # Check if team exists
            existing_team = db_manager.get_team(team_id)
            if not existing_team:
                team_data = {
                    'team_id': team_id,
                    'team_name': team_name,
                    'team_abbreviation': abbrev,
                    'city': city,
                    'conference': conference,
                    'division': division
                }
                db_manager.insert_team(team_data)
                teams_added += 1
        except Exception as e:
            logger.warning(f"Error adding team {team_id}: {e}")
    
    logger.info(f"[OK] {teams_added} teams added (total: {len(TEAM_DATA)} teams in database)")


def get_games_from_bball_ref_schedule(season: str, collector: BasketballReferenceCollector, months: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """
    Scrape Basketball Reference schedule pages (monthly) to get all games for a season.
    Optimized to parse tables directly instead of fetching individual boxscore pages.
    
    Args:
        season: Season string (e.g., '2019-20')
        collector: Basketball Reference collector
        months: Optional list of months to scrape. If None, scrapes all months.
                Use this to test on one month first (e.g., ['october'])
        
    Returns:
        List of game dictionaries with date, home_team, away_team, scores
    """
    logger.info(f"Scraping Basketball Reference schedule for season {season}...")
    
    season_start_year = int(season.split('-')[0])
    season_end_year = season_start_year + 1
    
    # Monthly pages: October (season start) through June (season end)
    # Format: NBA_YYYY_games-{month}.html
    all_months = ['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june']
    
    # If months specified, use those; otherwise use all months
    months_to_scrape = months if months else all_months
    
    all_games = []
    
    for month in months_to_scrape:
        schedule_url = f"{collector.base_url}/leagues/NBA_{season_end_year}_games-{month}.html"
        logger.info(f"Fetching {month.capitalize()} schedule...")
        
        soup = collector._fetch_page(schedule_url)
        if not soup:
            logger.warning(f"Could not fetch {month} schedule page")
            continue
        
        # DEBUG: Log page title and structure
        title = soup.find('title')
        if title:
            logger.debug(f"Page title: {title.get_text()}")
        
        # Find schedule table - it has class 'stats_table'
        schedule_table = soup.find('table', class_='stats_table')
        if not schedule_table:
            logger.warning(f"No schedule table found for {month}")
            # Debug: log all tables found
            all_tables = soup.find_all('table')
            logger.debug(f"Found {len(all_tables)} tables total. Looking for stats_table class...")
            for i, table in enumerate(all_tables):
                classes = table.get('class', [])
                logger.debug(f"Table {i}: classes={classes}")
                # Also check if it has schedule-like content
                if table.find('a', href=re.compile(r'/teams/')):
                    logger.debug(f"  Table {i} has team links - might be schedule table")
            continue
        
        # Get season type from caption
        caption = schedule_table.find('caption')
        season_type = 'Regular Season'
        if caption:
            caption_text = caption.get_text().upper()
            if 'PLAYOFF' in caption_text:
                season_type = 'Playoffs'
            elif 'PRESEASON' in caption_text:
                season_type = 'Preseason'
        
        # Parse table rows
        tbody = schedule_table.find('tbody')
        if not tbody:
            logger.warning(f"No tbody found in {month} schedule table")
            continue
        
        rows = tbody.find_all('tr')
        month_games = 0
        skipped_rows = 0
        
        logger.debug(f"Found {len(rows)} rows in {month} table")
        
        for row_idx, row in enumerate(rows):
            # Skip header rows
            if row.find('th'):
                skipped_rows += 1
                if row_idx < 3:  # Log first few to see structure
                    logger.debug(f"Row {row_idx}: Skipped (header row with <th>)")
                continue
            
            cells = row.find_all(['td', 'th'])
            if len(cells) < 5:  # Need: Date, Visitor, Visitor_PTS, Home, Home_PTS
                skipped_rows += 1
                if row_idx < 3:
                    logger.debug(f"Row {row_idx}: Skipped (only {len(cells)} cells, need 5+)")
                continue
            
            # Debug: Log first few data rows to see structure
            if row_idx < 5:
                cell_texts = [cell.get_text().strip()[:30] for cell in cells[:6]]
                logger.debug(f"Row {row_idx} sample cells: {cell_texts}")
            
            try:
                # Column 0: Date (with link)
                date_cell = cells[0]
                # Try multiple patterns for date link
                date_link = None
                # Pattern 1: Link to boxscore index
                date_link = date_cell.find('a', href=re.compile(r'/boxscores/'))
                # Pattern 2: Any link in first cell
                if not date_link:
                    date_link = date_cell.find('a')
                # Pattern 3: Date might be plain text
                if not date_link:
                    date_text = date_cell.get_text().strip()
                else:
                    date_text = date_link.get_text().strip()
                
                if not date_text or len(date_text) < 5:
                    skipped_rows += 1
                    logger.debug(f"Row {row_idx}: Skipped (no date text found: '{date_text}')")
                    continue
                
                # Parse date: "Tue, Oct 22, 2019" or "Oct 22, 2019" or "2019-10-22"
                game_date = None
                date_formats = ['%a, %b %d, %Y', '%b %d, %Y', '%Y-%m-%d', '%m/%d/%Y']
                for fmt in date_formats:
                    try:
                        game_date = datetime.strptime(date_text, fmt).date()
                        break
                    except ValueError:
                        continue
                
                if not game_date:
                    logger.debug(f"Row {row_idx}: Could not parse date: '{date_text}'")
                    skipped_rows += 1
                    continue
                
                # Find team links - visitor is first, home is second
                team_links = row.find_all('a', href=re.compile(r'/teams/[A-Z]{3}/'))
                if len(team_links) < 2:
                    # Try finding team links in any cell
                    all_links = row.find_all('a', href=re.compile(r'/teams/'))
                    team_links = [link for link in all_links if re.search(r'/teams/([A-Z]{3})/', link.get('href', ''))]
                    
                    if len(team_links) < 2:
                        logger.debug(f"Row {row_idx}: Found only {len(team_links)} team links (need 2)")
                        skipped_rows += 1
                        continue
                
                away_team_link = team_links[0]
                home_team_link = team_links[1]
                
                # Extract team abbreviations
                away_href = away_team_link.get('href', '')
                home_href = home_team_link.get('href', '')
                
                away_match = re.search(r'/teams/([A-Z]{3})/', away_href)
                home_match = re.search(r'/teams/([A-Z]{3})/', home_href)
                
                if not away_match or not home_match:
                    logger.debug(f"Could not extract team abbrevs from: {away_href}, {home_href}")
                    skipped_rows += 1
                    continue
                
                away_abbrev = away_match.group(1)
                home_abbrev = home_match.group(1)
                
                # Get team IDs
                away_team_data = TEAM_DATA.get(away_abbrev)
                home_team_data = TEAM_DATA.get(home_abbrev)
                
                if not away_team_data or not home_team_data:
                    logger.debug(f"Unknown team: {away_abbrev} or {home_abbrev}")
                    skipped_rows += 1
                    continue
                
                away_team_id = away_team_data[0]
                home_team_id = home_team_data[0]
                
                # Extract scores - look for numeric cells in reasonable score range
                # Table structure: Date | Start | Visitor | Visitor_PTS | Home | Home_PTS | ...
                # Scores are typically in columns after team names
                away_score = None
                home_score = None
                
                # Try to find scores by looking for numeric cells
                # The schedule table typically has: Date, Start, Visitor, Visitor_PTS, Home, Home_PTS
                # So scores should be in cells after the team links
                for i, cell in enumerate(cells):
                    cell_text = cell.get_text().strip()
                    # Skip if it's a date, time, or team link
                    if i == 0 or ':' in cell_text or cell.find('a', href=re.compile(r'/teams/')):
                        continue
                    
                    if cell_text.isdigit():
                        score_val = int(cell_text)
                        if 50 <= score_val <= 200:  # Reasonable score range
                            if away_score is None:
                                away_score = score_val
                            elif home_score is None:
                                home_score = score_val
                                break
                
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
                
                all_games.append(game_data)
                month_games += 1
                
            except Exception as e:
                logger.debug(f"Error parsing row {row_idx} in {month}: {e}")
                import traceback
                logger.debug(traceback.format_exc())
                skipped_rows += 1
                continue
        
        logger.info(f"  Found {month_games} games in {month.capitalize()} (skipped {skipped_rows} rows)")
    
    logger.info(f"Total games found from all months: {len(all_games)}")
    return all_games


def collect_season_games_from_bball_ref(season: str, db_manager: DatabaseManager, test_month: Optional[str] = None) -> Dict[str, int]:
    """
    Collect games from Basketball Reference schedule for a season.
    
    Args:
        season: Season string (e.g., '2019-20')
        db_manager: Database manager
        
    Returns:
        Dictionary with collection statistics
    """
    stats = {
        'games_found': 0,
        'games_stored': 0,
        'errors': 0
    }
    
    logger.info("=" * 70)
    logger.info(f"Collecting games from Basketball Reference schedule for season: {season}")
    logger.info("=" * 70)
    
    # Initialize teams if needed
    initialize_teams(db_manager)
    
    # Create collector
    if USE_SELENIUM:
        collector = BasketballReferenceSeleniumCollector(db_manager)
    else:
        collector = BasketballReferenceCollector(db_manager)
    
    # Get all games from schedule (optionally test on one month)
    months_to_scrape = [test_month] if test_month else None
    all_games = get_games_from_bball_ref_schedule(season, collector, months=months_to_scrape)
    stats['games_found'] = len(all_games)
    
    if not all_games:
        logger.warning(f"No games found for season {season}")
        return stats
    
    # Store games
    logger.info(f"Storing {len(all_games)} games...")
    skipped_existing = 0
    
    for game_data in tqdm(all_games, desc=f"Storing games {season}"):
        try:
            # Check if game already exists
            existing_game = db_manager.get_game(game_data['game_id'])
            if existing_game:
                skipped_existing += 1
                continue
            
            # Insert game
            db_manager.insert_game(game_data)
            stats['games_stored'] += 1
        
        except Exception as e:
            logger.warning(f"Error storing game {game_data.get('game_id')}: {e}")
            stats['errors'] += 1
            continue
    
    logger.info(f"Stored {stats['games_stored']} games for season {season} (skipped {skipped_existing} already in database)")
    return stats


def collect_season_bball_ref(season: str, db_manager: DatabaseManager, replace_existing: bool = False) -> Dict[str, int]:
    """
    Collect Basketball Reference stats for all games in a season.
    
    Args:
        season: Season string (e.g., '2019-20')
        db_manager: Database manager
        replace_existing: If True, replace existing stats. If False, skip games with stats.
        
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
    logger.info(f"Collecting Basketball Reference stats for season: {season}")
    logger.info("=" * 70)
    
    if USE_SELENIUM:
        logger.info("Using Selenium-based Basketball Reference collector")
        collector = BasketballReferenceSeleniumCollector(db_manager)
    else:
        logger.info("Using requests-based Basketball Reference collector")
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
            
            needs_stats = (team_stats_count == 0 or player_stats_count == 0)
        
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


def collect_all_historical_seasons(collect_games: bool = True, replace_existing: bool = False):
    """
    Collect all historical seasons (2019-20, 2020-21, 2021-22).
    
    Args:
        collect_games: If True, collect games from Basketball Reference schedule first. If False, assume games exist.
        replace_existing: If True, replace existing data. If False, skip games with data.
    """
    logger.info("=" * 70)
    logger.info("Basketball Reference Historical Data Collection (NO NBA API)")
    logger.info("=" * 70)
    logger.info(f"Seasons: {', '.join(HISTORICAL_SEASONS)}")
    logger.info(f"Database: {HISTORICAL_DB_PATH}")
    logger.info(f"Collect games first: {collect_games}")
    logger.info(f"Replace existing: {replace_existing}")
    logger.info("=" * 70)
    
    # Initialize database manager with separate database
    database_url = f"sqlite:///{HISTORICAL_DB_PATH}"
    db_manager = DatabaseManager(database_url=database_url)
    
    # Create database tables if they don't exist
    logger.info("Initializing database...")
    db_manager.create_tables()
    
    if not db_manager.test_connection():
        logger.error("Database connection failed!")
        return False
    
    logger.info("✓ Database connection successful")
    
    total_stats = {
        'games_processed': 0,
        'games_with_details': 0,
        'games_with_stats': 0,
        'games_skipped': 0,
        'team_stats_collected': 0,
        'player_stats_collected': 0,
        'errors': 0
    }
    
    # Collect each season
    for season in HISTORICAL_SEASONS:
        try:
            logger.info(f"\n{'='*70}")
            logger.info(f"Processing Season: {season}")
            logger.info(f"{'='*70}\n")
            
            # Step 1: Collect games from Basketball Reference schedule (if needed)
            if collect_games:
                logger.info(f"[Step 1/2] Collecting games from Basketball Reference schedule for {season}...")
                game_stats = collect_season_games_from_bball_ref(season, db_manager)
                logger.info(f"✓ Collected {game_stats['games_stored']} games for {season}")
            
            # Step 2: Collect Basketball Reference stats
            logger.info(f"\n[Step 2/2] Collecting Basketball Reference stats for {season}...")
            season_stats = collect_season_bball_ref(season, db_manager, replace_existing=replace_existing)
            
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
    logger.info("All Historical Seasons Collection Complete")
    logger.info("=" * 70)
    logger.info(f"Total games processed: {total_stats['games_processed']}")
    logger.info(f"Total games with details: {total_stats['games_with_details']}")
    logger.info(f"Total games with stats: {total_stats['games_with_stats']}")
    logger.info(f"Total games skipped: {total_stats['games_skipped']}")
    logger.info(f"Total team stats collected: {total_stats['team_stats_collected']}")
    logger.info(f"Total player stats collected: {total_stats['player_stats_collected']}")
    logger.info(f"Total errors: {total_stats['errors']}")
    logger.info(f"Database location: {HISTORICAL_DB_PATH}")
    logger.info("=" * 70)
    
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Collect Basketball Reference data for historical seasons (2019-20, 2020-21, 2021-22) - NO NBA API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Collect games and stats for all historical seasons (from Basketball Reference only)
  python scripts/collect_historical_bball_ref.py
  
  # Skip game collection (assume games already exist in database)
  python scripts/collect_historical_bball_ref.py --no-collect-games
  
  # Replace existing data
  python scripts/collect_historical_bball_ref.py --replace
  
  # Collect specific season only
  python scripts/collect_historical_bball_ref.py --season 2019-20
  
  # Test on one month only (e.g., October)
  python scripts/collect_historical_bball_ref.py --season 2019-20 --test-month october
        """
    )
    parser.add_argument('--season', type=str, choices=HISTORICAL_SEASONS,
                       help='Season to collect (default: all historical seasons)')
    parser.add_argument('--no-collect-games', action='store_true',
                       help='Skip game collection from Basketball Reference schedule (assume games already exist)')
    parser.add_argument('--replace', action='store_true',
                       help='Replace existing data (default: only fill missing)')
    parser.add_argument('--test-month', type=str, choices=['october', 'november', 'december', 'january', 'february', 'march', 'april', 'may', 'june'],
                       help='Test on one month only (e.g., --test-month october)')
    
    args = parser.parse_args()
    
    try:
        if args.season:
            # Collect single season
            database_url = f"sqlite:///{HISTORICAL_DB_PATH}"
            db_manager = DatabaseManager(database_url=database_url)
            db_manager.create_tables()
            
            if not args.no_collect_games:
                collect_season_games_from_bball_ref(args.season, db_manager, test_month=args.test_month)
            
            collect_season_bball_ref(args.season, db_manager, replace_existing=args.replace)
        else:
            # Collect all seasons
            collect_all_historical_seasons(
                collect_games=not args.no_collect_games,
                replace_existing=args.replace
            )
        
        sys.exit(0)
    except KeyboardInterrupt:
        logger.info("\n\nCollection interrupted by user. Progress has been saved.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
