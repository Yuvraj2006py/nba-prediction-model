"""Fetch ALL today's NBA games - tries multiple sources."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from datetime import date, timedelta, datetime, timezone
from src.database.db_manager import DatabaseManager
from src.backtesting.team_mapper import TeamMapper
# Import directly to avoid nba_api dependency
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / 'src' / 'data_collectors'))
from betting_odds_collector import BettingOddsCollector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Known games for today (can be updated manually or from schedule)
KNOWN_GAMES_TODAY = [
    ("Houston Rockets", "Brooklyn Nets"),
    ("Miami Heat", "Detroit Pistons"),
    ("Philadelphia 76ers", "Dallas Mavericks"),
    ("Boston Celtics", "Sacramento Kings"),
    ("Utah Jazz", "LA Clippers"),
]


def create_game_from_teams(
    db_manager: DatabaseManager,
    team_mapper: TeamMapper,
    away_team_name: str,
    home_team_name: str,
    game_date: date
) -> bool:
    """Create a game in database from team names."""
    away_team_id = team_mapper.map_team_name_to_id(away_team_name)
    home_team_id = team_mapper.map_team_name_to_id(home_team_name)
    
    if not away_team_id or not home_team_id:
        logger.warning(f"Could not map teams: {away_team_name} -> {away_team_id}, {home_team_name} -> {home_team_id}")
        return False
    
    # Generate game ID
    date_str = game_date.strftime('%Y%m%d')
    home_suffix = home_team_id[-3:]
    away_suffix = away_team_id[-3:]
    game_id = f"{date_str}{away_suffix}{home_suffix}"
    
    # Check if game exists
    existing_game = db_manager.get_game(game_id)
    if existing_game:
        logger.debug(f"Game {game_id} already exists")
        return True
    
    # Determine season
    if game_date.month >= 10:
        season = f"{game_date.year}-{str(game_date.year + 1)[-2:]}"
    else:
        season = f"{game_date.year - 1}-{str(game_date.year)[-2:]}"
    
    # Create game
    game_data = {
        'game_id': game_id,
        'season': season,
        'season_type': 'Regular Season',
        'game_date': game_date,
        'home_team_id': home_team_id,
        'away_team_id': away_team_id,
        'game_status': 'scheduled'
    }
    
    try:
        db_manager.insert_game(game_data)
        logger.info(f"Created game: {away_team_name} @ {home_team_name} (ID: {game_id})")
        return True
    except Exception as e:
        logger.error(f"Error creating game: {e}")
        return False


def main():
    """Fetch all today's games."""
    print("=" * 70)
    print("Fetching ALL Today's NBA Games")
    print("=" * 70)
    
    today = date.today()
    print(f"\nDate: {today}")
    
    # Initialize
    db_manager = DatabaseManager()
    team_mapper = TeamMapper(db_manager)
    odds_collector = BettingOddsCollector(db_manager)
    
    games_created = 0
    games_with_odds = 0
    
    # STEP 1: Fetch ALL games from betting API (no date filter), then filter locally
    print(f"\n[STEP 1] Fetching ALL games from Betting API (no date filter)...")
    try:
        if odds_collector.api_key and odds_collector.api_key != 'your_betting_api_key_here':
            # Get ALL upcoming games from API
            all_odds = odds_collector.get_nba_odds()
            print(f"[INFO] API returned {len(all_odds)} total games")
            
            # Filter for today's games locally (accounting for timezone)
            odds_data = []
            
            print(f"  Filtering games for today ({today})...")
            tomorrow_utc = today + timedelta(days=1)
            
            for game_odds in all_odds:
                commence_time = game_odds.get('commence_time', '')
                home_team = game_odds.get('home_team', 'Unknown')
                away_team = game_odds.get('away_team', 'Unknown')
                
                if commence_time:
                    try:
                        # Parse the commence time (API returns in UTC)
                        if 'T' in commence_time:
                            # Handle ISO format with timezone
                            if commence_time.endswith('Z'):
                                game_datetime_utc = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                            else:
                                game_datetime_utc = datetime.fromisoformat(commence_time)
                            
                            game_date_utc = game_datetime_utc.date()
                            hour_utc = game_datetime_utc.hour
                            
                            # Check if it's today in UTC
                            if game_date_utc == today:
                                odds_data.append(game_odds)
                                print(f"    [INCLUDED] {away_team} @ {home_team} - UTC date {game_date_utc} matches today")
                            # Also check if it's tomorrow in UTC but early morning (tonight in US)
                            elif game_date_utc == tomorrow_utc:
                                # If game is early morning UTC (0-6 AM), it's likely tonight in US
                                if hour_utc < 6:
                                    odds_data.append(game_odds)
                                    print(f"    [INCLUDED] {away_team} @ {home_team} - UTC date {game_date_utc} hour {hour_utc} (tonight US)")
                                else:
                                    print(f"    [SKIPPED] {away_team} @ {home_team} - UTC date {game_date_utc} hour {hour_utc} (too late)")
                            else:
                                # Only print if it's close (within 2 days)
                                if abs((game_date_utc - today).days) <= 2:
                                    print(f"    [SKIPPED] {away_team} @ {home_team} - UTC date {game_date_utc} (not today/tomorrow)")
                    except Exception as e:
                        print(f"    [ERROR] {away_team} @ {home_team} - Error parsing '{commence_time}': {e}")
                        continue
                else:
                    print(f"    [SKIPPED] {away_team} @ {home_team} - No commence_time")
            
            # Debug: Show dates of all games from API
            print(f"\n[DEBUG] Checking dates of all {len(all_odds)} games from API:")
            games_by_date = {}
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
                            
                            if game_date not in games_by_date:
                                games_by_date[game_date] = []
                            games_by_date[game_date].append(game_odds)
                    except:
                        pass
            
            for game_date, games in sorted(games_by_date.items()):
                print(f"  {game_date}: {len(games)} games")
                for game in games[:3]:  # Show first 3 per date
                    home = game.get('home_team', 'Unknown')
                    away = game.get('away_team', 'Unknown')
                    commence = game.get('commence_time', 'Unknown')
                    print(f"    - {away} @ {home} ({commence})")
            
            if odds_data:
                print(f"\n[OK] Found {len(odds_data)} games for today (filtered from {len(all_odds)} total)")
                stored_count = odds_collector.parse_and_store_odds(odds_data)
                games_with_odds = len(odds_data)
                print(f"[OK] Stored {stored_count} betting lines")
            else:
                print(f"\n[WARNING] No games found for today after filtering")
                print(f"  Today is: {today}")
                print(f"  Games found for dates: {list(games_by_date.keys())}")
        else:
            print("[SKIP] Betting API key not configured")
    except Exception as e:
        print(f"[WARNING] Error fetching from betting API: {e}")
        import traceback
        traceback.print_exc()
    
    # STEP 1.5: Try to fetch odds for existing games that don't have odds yet
    print(f"\n[STEP 1.5] Trying to match odds for games without odds...")
    try:
        if odds_collector.api_key and odds_collector.api_key != 'your_betting_api_key_here':
            # Try fetching with a wider date range to catch games that might be listed differently
            print(f"  Checking API response for {today}...")
            
            # Get raw API response to see what's available
            odds_data = odds_collector.get_odds_for_date(today)
            if odds_data:
                print(f"  API returned {len(odds_data)} games with odds:")
                for i, game_odds in enumerate(odds_data, 1):
                    home = game_odds.get('home_team', 'Unknown')
                    away = game_odds.get('away_team', 'Unknown')
                    print(f"    {i}. {away} @ {home}")
            
            # Try to match existing games
            additional_odds = odds_collector.fetch_odds_for_existing_games(today)
            if additional_odds > 0:
                print(f"[OK] Found and stored odds for {additional_odds} additional betting lines")
            else:
                print(f"[INFO] No additional odds found")
                print(f"  Reason: Betting API only returns games with active betting lines.")
                print(f"  The other {len(KNOWN_GAMES_TODAY) - len(odds_data) if odds_data else len(KNOWN_GAMES_TODAY)} games may not have odds posted yet.")
                print(f"  Try running this script again closer to game time.")
        else:
            print("[SKIP] Betting API key not configured")
    except Exception as e:
        print(f"[WARNING] Error fetching additional odds: {e}")
        import traceback
        traceback.print_exc()
    
    # STEP 2: Add known games (from schedule)
    print(f"\n[STEP 2] Adding known games from schedule...")
    for away_team, home_team in KNOWN_GAMES_TODAY:
        if create_game_from_teams(db_manager, team_mapper, away_team, home_team, today):
            games_created += 1
    
    # STEP 3: Verify all games
    print(f"\n[STEP 3] Verifying games in database...")
    from src.database.models import Game, Team
    with db_manager.get_session() as session:
        games = session.query(Game).filter(
            Game.game_date == today,
            Game.home_score.is_(None)  # Not finished
        ).all()
        
        print(f"\n[OK] Total games in database for today: {len(games)}")
        print(f"\nGames:")
        for i, game in enumerate(games, 1):
            away_team = session.query(Team).filter_by(team_id=game.away_team_id).first()
            home_team = session.query(Team).filter_by(team_id=game.home_team_id).first()
            away_name = away_team.team_name if away_team else game.away_team_id
            home_name = home_team.team_name if home_team else game.home_team_id
            
            # Check if has odds
            from src.database.models import BettingLine
            has_odds = session.query(BettingLine).filter_by(game_id=game.game_id).first() is not None
            odds_status = "[Has odds]" if has_odds else "[No odds]"
            
            print(f"  {i}. {away_name} @ {home_name} ({odds_status})")
    
    print(f"\n" + "=" * 70)
    print("Fetch Complete!")
    print("=" * 70)
    print(f"\nSummary:")
    print(f"  Games with odds: {games_with_odds}")
    print(f"  Total games: {len(games)}")
    print(f"\nNext step:")
    print(f"  python scripts/make_predictions.py --model-name nba_classifier --start-date {today} --end-date {today}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

