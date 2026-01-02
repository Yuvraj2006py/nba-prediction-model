"""
Fetch today's NBA games from the betting API.
Automatically determines today's date and fetches games for that date.
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
from datetime import date, datetime
from src.database.db_manager import DatabaseManager
from src.backtesting.team_mapper import TeamMapper
from src.data_collectors.betting_odds_collector import BettingOddsCollector
from src.database.models import Game

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Fetch today's games from API."""
    today = date.today()
    yesterday = date(today.year, today.month, today.day - 1) if today.day > 1 else None
    
    print("=" * 70)
    print(f"FETCHING TODAY'S GAMES - {today}")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    team_mapper = TeamMapper(db_manager)
    odds_collector = BettingOddsCollector(db_manager)
    
    # Step 1: Clean up yesterday's games
    print(f"\n[STEP 1] Cleaning up old games...")
    with db_manager.get_session() as session:
        if yesterday:
            yesterday_games = session.query(Game).filter(Game.game_date == yesterday).all()
            if yesterday_games:
                print(f"  Deleting {len(yesterday_games)} games from {yesterday}...")
                for game in yesterday_games:
                    session.delete(game)
                session.commit()
                print(f"  [OK] Deleted {len(yesterday_games)} games from yesterday")
    
    # Step 2: Fetch games from API
    print(f"\n[STEP 2] Fetching games from API for {today}...")
    all_odds = odds_collector.get_nba_odds()
    print(f"[INFO] API returned {len(all_odds)} total games")
    
    # Filter for games that are today in US time
    # API returns games in UTC, so we need to convert
    today_games = []
    tomorrow_utc = date(today.year, today.month, today.day + 1)
    
    for game_odds in all_odds:
        commence_time = game_odds.get('commence_time', '')
        if commence_time:
            try:
                if 'T' in commence_time:
                    if commence_time.endswith('Z'):
                        game_datetime_utc = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                    else:
                        game_datetime_utc = datetime.fromisoformat(commence_time)
                    
                    game_date_utc = game_datetime_utc.date()
                    hour_utc = game_datetime_utc.hour
                    
                    # Games on today's date in UTC, or tomorrow UTC before 6 AM (which is today US evening)
                    if game_date_utc == today:
                        today_games.append(game_odds)
                        home = game_odds.get('home_team', 'Unknown')
                        away = game_odds.get('away_team', 'Unknown')
                        print(f"  [FOUND] {away} @ {home} - UTC: {game_datetime_utc}")
                    elif game_date_utc == tomorrow_utc and hour_utc < 6:
                        # Early morning UTC games are today US evening
                        today_games.append(game_odds)
                        home = game_odds.get('home_team', 'Unknown')
                        away = game_odds.get('away_team', 'Unknown')
                        print(f"  [FOUND] {away} @ {home} - UTC: {game_datetime_utc} (today US)")
            except Exception as e:
                logger.debug(f"Error parsing: {e}")
    
    if today_games:
        print(f"\n[OK] Found {len(today_games)} games for {today} (US time)")
        stored_count = odds_collector.parse_and_store_odds(today_games)
        print(f"[OK] Stored {stored_count} betting lines")
        
        # Ensure all stored games have the correct date
        with db_manager.get_session() as session:
            # Update any games that might have been stored with wrong date
            games_to_fix = session.query(Game).filter(
                Game.game_date != today
            ).filter(
                Game.game_date <= tomorrow_utc
            ).all()
            
            if games_to_fix:
                print(f"  Updating {len(games_to_fix)} games to correct date...")
                for game in games_to_fix:
                    game.game_date = today
                session.commit()
                print(f"  [OK] Updated {len(games_to_fix)} games to {today}")
    else:
        print(f"\n[WARNING] No games found for {today} from API")
    
    # Step 3: Final verification
    print(f"\n[STEP 3] Final verification...")
    with db_manager.get_session() as session:
        today_games_db = session.query(Game).filter(Game.game_date == today).all()
        
        print(f"  Games in database for {today}: {len(today_games_db)}")
        
        if today_games_db:
            print(f"\n  Games found:")
            for i, game in enumerate(today_games_db, 1):
                away_team = db_manager.get_team(game.away_team_id)
                home_team = db_manager.get_team(game.home_team_id)
                away_name = away_team.team_name if away_team else game.away_team_id
                home_name = home_team.team_name if home_team else game.home_team_id
                print(f"    {i}. {away_name} @ {home_name} (ID: {game.game_id})")
    
    print("\n" + "=" * 70)
    print("FETCH COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    main()
