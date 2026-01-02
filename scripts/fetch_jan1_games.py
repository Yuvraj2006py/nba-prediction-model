"""Fetch games for January 1st, 2026 from the betting API."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

import logging
from datetime import date, datetime, timezone, timedelta
from src.database.db_manager import DatabaseManager
from src.backtesting.team_mapper import TeamMapper
from src.data_collectors.betting_odds_collector import BettingOddsCollector
from src.database.models import Game

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Fetch January 1st, 2026 games from API."""
    target_date = date(2026, 1, 1)
    
    print("=" * 70)
    print(f"FETCHING GAMES FOR {target_date}")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    team_mapper = TeamMapper(db_manager)
    odds_collector = BettingOddsCollector(db_manager)
    
    # Step 1: Fetch games from API for specific date
    print(f"\n[STEP 1] Fetching games from API for {target_date}...")
    all_odds = odds_collector.get_odds_for_date(target_date=target_date)
    print(f"[INFO] API returned {len(all_odds)} games for {target_date}")
    
    if not all_odds:
        print(f"\n[WARNING] No games found in API for {target_date}")
        print("This could mean:")
        print("  1. There were no games on this date")
        print("  2. The API doesn't have historical data for this date")
        print("  3. The date format or timezone conversion needs adjustment")
        return
    
    # Step 2: Filter for games that are Jan 1st in US Eastern Time
    # API returns games in UTC, so we need to convert
    jan1_games = []
    et_tz = timezone(timedelta(hours=-5))  # US Eastern Time
    
    print(f"\n[STEP 2] Filtering games for {target_date} (US Eastern Time)...")
    for game_odds in all_odds:
        commence_time = game_odds.get('commence_time', '')
        if commence_time:
            try:
                if 'T' in commence_time:
                    if commence_time.endswith('Z'):
                        dt_utc = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                    else:
                        dt_utc = datetime.fromisoformat(commence_time)
                    
                    # Convert to US Eastern Time
                    dt_us = dt_utc.astimezone(et_tz)
                    game_date_us = dt_us.date()
                    
                    if game_date_us == target_date:
                        jan1_games.append(game_odds)
                        home = game_odds.get('home_team', 'Unknown')
                        away = game_odds.get('away_team', 'Unknown')
                        print(f"  [FOUND] {away} @ {home} - US Time: {dt_us}")
            except Exception as e:
                logger.debug(f"Error parsing: {e}")
    
    if not jan1_games:
        print(f"\n[WARNING] No games found for {target_date} after timezone conversion")
        return
    
    print(f"\n[STEP 3] Creating/updating {len(jan1_games)} games in database...")
    
    # Step 3: Create or update games in database
    games_created = 0
    games_updated = 0
    
    with db_manager.get_session() as session:
        for game_odds in jan1_games:
            try:
                # Extract game info
                home_team_name = game_odds.get('home_team', '')
                away_team_name = game_odds.get('away_team', '')
                
                if not home_team_name or not away_team_name:
                    continue
                
                # Get team IDs
                home_team_id = team_mapper.get_team_id(home_team_name)
                away_team_id = team_mapper.get_team_id(away_team_name)
                
                if not home_team_id or not away_team_id:
                    logger.warning(f"Could not map teams: {away_team_name} @ {home_team_name}")
                    continue
                
                # Create game_id from date and team IDs
                date_str = target_date.strftime('%Y%m%d')
                game_id = f"{date_str}{home_team_id}{away_team_id}"
                
                # Check if game already exists
                existing_game = session.query(Game).filter_by(game_id=game_id).first()
                
                if existing_game:
                    # Update existing game
                    existing_game.game_date = target_date
                    existing_game.season = '2025-26'
                    existing_game.season_type = 'Regular Season'
                    games_updated += 1
                else:
                    # Create new game
                    new_game = Game(
                        game_id=game_id,
                        season='2025-26',
                        season_type='Regular Season',
                        game_date=target_date,
                        home_team_id=home_team_id,
                        away_team_id=away_team_id,
                        game_status='scheduled'
                    )
                    session.add(new_game)
                    games_created += 1
                
            except Exception as e:
                logger.error(f"Error processing game {game_odds.get('id', 'unknown')}: {e}")
        
        session.commit()
    
    print(f"\n[OK] Created {games_created} new games")
    print(f"[OK] Updated {games_updated} existing games")
    
    # Final check
    with db_manager.get_session() as session:
        final_count = session.query(Game).filter(Game.game_date == target_date).count()
        print(f"\n[OK] Total games in database for {target_date}: {final_count}")
    
    print("\n" + "=" * 70)
    print("FETCH COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    main()

