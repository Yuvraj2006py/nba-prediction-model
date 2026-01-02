"""
Fetch ONLY today's games (January 2, 2026) - exactly 10 games.
Cleans up any duplicates or incorrectly dated games.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from datetime import date, datetime
from src.database.db_manager import DatabaseManager
from src.backtesting.team_mapper import TeamMapper
from src.data_collectors.betting_odds_collector import BettingOddsCollector
from src.database.models import Game

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Fetch only today's games (Jan 2, 2026) - should be exactly 10."""
    target_date = date(2026, 1, 2)
    yesterday = date(2026, 1, 1)
    
    print("=" * 70)
    print(f"FETCHING ONLY TODAY'S GAMES - {target_date}")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    team_mapper = TeamMapper(db_manager)
    odds_collector = BettingOddsCollector(db_manager)
    
    # Step 1: Clean up - delete games from yesterday that might be duplicates
    print(f"\n[STEP 1] Cleaning up old/duplicate games...")
    with db_manager.get_session() as session:
        # Delete all Jan 1 games (yesterday)
        jan1_games = session.query(Game).filter(Game.game_date == yesterday).all()
        if jan1_games:
            print(f"  Deleting {len(jan1_games)} games from {yesterday}...")
            for game in jan1_games:
                session.delete(game)
            session.commit()
            print(f"  [OK] Deleted {len(jan1_games)} games from yesterday")
        
        # Also delete any Jan 2 games that might be duplicates
        # Keep only the most recent ones
        jan2_games = session.query(Game).filter(Game.game_date == target_date).all()
        if len(jan2_games) > 10:
            print(f"  Found {len(jan2_games)} games for {target_date}, expected 10")
            print(f"  Keeping only the 10 most recent games...")
            # Sort by game_id (newer IDs are typically larger)
            jan2_games.sort(key=lambda g: g.game_id, reverse=True)
            # Delete the extras
            for game in jan2_games[10:]:
                session.delete(game)
            session.commit()
            print(f"  [OK] Deleted {len(jan2_games) - 10} duplicate games")
    
    # Step 2: Fetch fresh games from API
    print(f"\n[STEP 2] Fetching games from API for {target_date}...")
    all_odds = odds_collector.get_nba_odds()
    print(f"[INFO] API returned {len(all_odds)} total games")
    
    # Filter for games that are Jan 2 in US time
    # API returns games in UTC, so Jan 3 00:00-06:00 UTC = Jan 2 US evening
    jan2_games = []
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
                    
                    # Games on Jan 3 UTC before 6 AM = Jan 2 US evening
                    if game_date_utc == date(2026, 1, 3) and hour_utc < 6:
                        jan2_games.append(game_odds)
                        home = game_odds.get('home_team', 'Unknown')
                        away = game_odds.get('away_team', 'Unknown')
                        print(f"  [FOUND] {away} @ {home} - UTC: {game_datetime_utc}")
            except Exception as e:
                logger.debug(f"Error parsing: {e}")
    
    if jan2_games:
        print(f"\n[OK] Found {len(jan2_games)} games for Jan 2 (US time)")
        
        # Store games - this will create/update them with correct date
        stored_count = odds_collector.parse_and_store_odds(jan2_games)
        print(f"[OK] Stored {stored_count} betting lines")
        
        # Ensure all stored games have the correct date (Jan 2)
        with db_manager.get_session() as session:
            # Update any games that might have been stored with wrong date
            games_to_fix = session.query(Game).filter(
                Game.game_date == date(2026, 1, 3)
            ).all()
            
            if games_to_fix:
                print(f"  Updating {len(games_to_fix)} games to correct date...")
                for game in games_to_fix:
                    game.game_date = target_date
                session.commit()
                print(f"  [OK] Updated {len(games_to_fix)} games to {target_date}")
    else:
        print(f"\n[WARNING] No games found for Jan 2 from API")
    
    # Step 3: Final verification
    print(f"\n[STEP 3] Final verification...")
    with db_manager.get_session() as session:
        jan2_games = session.query(Game).filter(Game.game_date == target_date).all()
        jan1_games = session.query(Game).filter(Game.game_date == yesterday).all()
        
        print(f"  Games on {yesterday}: {len(jan1_games)} (should be 0)")
        print(f"  Games on {target_date}: {len(jan2_games)} (should be 10)")
        
        if len(jan2_games) == 10:
            print(f"\n  SUCCESS: Exactly 10 games for {target_date}")
            print(f"\n  Games:")
            for i, game in enumerate(jan2_games, 1):
                away_team = db_manager.get_team(game.away_team_id)
                home_team = db_manager.get_team(game.home_team_id)
                away_name = away_team.team_name if away_team else game.away_team_id
                home_name = home_team.team_name if home_team else game.home_team_id
                print(f"    {i}. {away_name} @ {home_name} (ID: {game.game_id})")
        elif len(jan2_games) > 10:
            print(f"\n  WARNING: Found {len(jan2_games)} games, expected 10")
            print(f"  Keeping only the 10 most recent...")
            jan2_games.sort(key=lambda g: g.game_id, reverse=True)
            for game in jan2_games[10:]:
                session.delete(game)
            session.commit()
            print(f"  [OK] Cleaned up to exactly 10 games")
        else:
            print(f"\n  WARNING: Only found {len(jan2_games)} games, expected 10")
    
    print("\n" + "=" * 70)
    print("FETCH COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    main()

