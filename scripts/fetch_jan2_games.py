"""
Fetch games specifically for January 2, 2026.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from datetime import date, datetime, timedelta
from src.database.db_manager import DatabaseManager
from src.backtesting.team_mapper import TeamMapper
from src.data_collectors.betting_odds_collector import BettingOddsCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Fetch games for January 2, 2026."""
    target_date = date(2026, 1, 2)
    
    print("=" * 70)
    print(f"FETCHING GAMES FOR {target_date}")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    team_mapper = TeamMapper(db_manager)
    odds_collector = BettingOddsCollector(db_manager)
    
    games_created = 0
    
    # STEP 1: Fetch games from betting API for Jan 2
    print(f"\n[STEP 1] Fetching games from Betting API for {target_date}...")
    try:
        if odds_collector.api_key and odds_collector.api_key != 'your_betting_api_key_here':
            # Get odds for specific date
            odds_data = odds_collector.get_odds_for_date(target_date)
            print(f"[INFO] API returned {len(odds_data)} games for {target_date}")
            
            if odds_data:
                # Parse and store games
                stored_count = odds_collector.parse_and_store_odds(odds_data)
                print(f"[OK] Stored {stored_count} betting lines")
                
                # Verify games were created with correct date
                from src.database.models import Game
                with db_manager.get_session() as session:
                    games = session.query(Game).filter(
                        Game.game_date == target_date
                    ).all()
                    
                    print(f"\n[VERIFICATION] Games in database for {target_date}:")
                    for game in games:
                        away_team = db_manager.get_team(game.away_team_id)
                        home_team = db_manager.get_team(game.home_team_id)
                        away_name = away_team.team_name if away_team else game.away_team_id
                        home_name = home_team.team_name if home_team else game.home_team_id
                        print(f"  {game.game_id}: {game.game_date} - {away_name} @ {home_name}")
                        print(f"    Commence time from API: {game.game_datetime if hasattr(game, 'game_datetime') else 'N/A'}")
            else:
                print(f"[WARNING] No games found for {target_date}")
                
                # Try fetching all games and filtering
                print("\n[STEP 2] Trying to fetch all games and filter for Jan 2...")
                all_odds = odds_collector.get_nba_odds()
                print(f"[INFO] API returned {len(all_odds)} total games")
                
                # Filter for Jan 2 (accounting for timezone)
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
                                
                                # Check if it's Jan 2 in UTC
                                if game_date_utc == target_date:
                                    jan2_games.append(game_odds)
                                    home = game_odds.get('home_team', 'Unknown')
                                    away = game_odds.get('away_team', 'Unknown')
                                    print(f"  [FOUND] {away} @ {home} - UTC date: {game_date_utc}, time: {game_datetime_utc}")
                        except Exception as e:
                            logger.debug(f"Error parsing commence_time: {e}")
                
                if jan2_games:
                    print(f"\n[OK] Found {len(jan2_games)} games for Jan 2 after filtering")
                    stored_count = odds_collector.parse_and_store_odds(jan2_games)
                    print(f"[OK] Stored {stored_count} betting lines")
                else:
                    print(f"\n[WARNING] No games found for Jan 2 after filtering all games")
                    print("  Showing all available dates:")
                    dates_found = {}
                    for game_odds in all_odds:
                        commence_time = game_odds.get('commence_time', '')
                        if commence_time:
                            try:
                                if 'T' in commence_time:
                                    if commence_time.endswith('Z'):
                                        game_datetime = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                                    else:
                                        game_datetime = datetime.fromisoformat(commence_time)
                                    game_date = game_datetime.date()
                                    if game_date not in dates_found:
                                        dates_found[game_date] = []
                                    dates_found[game_date].append(game_odds)
                            except:
                                pass
                    
                    for game_date, games in sorted(dates_found.items()):
                        print(f"  {game_date}: {len(games)} games")
        else:
            print("[SKIP] Betting API key not configured")
    except Exception as e:
        logger.error(f"Error fetching games: {e}")
        import traceback
        traceback.print_exc()
    
    # Final verification
    print("\n" + "=" * 70)
    print("FINAL VERIFICATION")
    print("=" * 70)
    
    from src.database.models import Game
    with db_manager.get_session() as session:
        games = session.query(Game).filter(
            Game.game_date == target_date
        ).all()
        
        print(f"\nGames in database for {target_date}: {len(games)}")
        for game in games:
            away_team = db_manager.get_team(game.away_team_id)
            home_team = db_manager.get_team(game.home_team_id)
            away_name = away_team.team_name if away_team else game.away_team_id
            home_name = home_team.team_name if home_team else game.home_team_id
            print(f"  {game.game_id}: {away_name} @ {home_name}")
            print(f"    Date: {game.game_date}")
            print(f"    Status: {game.game_status}")


if __name__ == '__main__':
    main()

