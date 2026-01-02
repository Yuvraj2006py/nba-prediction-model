"""
Fix games that have incorrect dates - check game IDs and move games to correct dates.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

from datetime import date
from src.database.db_manager import DatabaseManager
from src.database.models import Game

def extract_date_from_game_id(game_id: str) -> date:
    """Extract date from game ID (format: YYYYMMDD...)."""
    if len(game_id) >= 8 and game_id[:8].isdigit():
        try:
            year = int(game_id[:4])
            month = int(game_id[4:6])
            day = int(game_id[6:8])
            return date(year, month, day)
        except (ValueError, IndexError):
            pass
    return None

def main():
    """Fix games with incorrect dates based on game IDs."""
    today = date.today()
    yesterday = date(today.year, today.month, today.day - 1)
    
    print("=" * 70)
    print(f"FIXING INCORRECT GAME DATES")
    print("=" * 70)
    print(f"Today: {today}")
    print(f"Yesterday: {yesterday}")
    
    db = DatabaseManager()
    
    with db.get_session() as session:
        # Get all games dated today
        today_games = session.query(Game).filter(Game.game_date == today).all()
        
        print(f"\n[STEP 1] Checking {len(today_games)} games dated {today}...")
        
        fixed_to_yesterday = []
        for game in today_games:
            game_id_date = extract_date_from_game_id(game.game_id)
            
            if game_id_date:
                # If game ID date is yesterday, move game to yesterday
                if game_id_date == yesterday:
                    fixed_to_yesterday.append(game)
                    away_team = db.get_team(game.away_team_id)
                    home_team = db.get_team(game.home_team_id)
                    away_name = away_team.team_name if away_team else game.away_team_id
                    home_name = home_team.team_name if home_team else game.home_team_id
                    print(f"  Found: {away_name} @ {home_name}")
                    print(f"    Game ID date: {game_id_date}, Stored date: {game.game_date}")
                    print(f"    Moving to {yesterday}")
        
        if fixed_to_yesterday:
            print(f"\n[STEP 2] Moving {len(fixed_to_yesterday)} games to {yesterday}...")
            for game in fixed_to_yesterday:
                game.game_date = yesterday
                if game.game_status == 'scheduled':
                    game.game_status = 'finished'  # If it's from yesterday, it's finished
            session.commit()
            print(f"  [OK] Moved {len(fixed_to_yesterday)} games to {yesterday}")
        else:
            print(f"\n  No games found that need date correction")
        
        # Final counts
        today_count = session.query(Game).filter(Game.game_date == today).count()
        yesterday_count = session.query(Game).filter(Game.game_date == yesterday).count()
        
        print(f"\n[STEP 3] Final counts:")
        print(f"  Games on {yesterday}: {yesterday_count}")
        print(f"  Games on {today}: {today_count}")
    
    print("\n" + "=" * 70)
    print("FIX COMPLETE")
    print("=" * 70)

if __name__ == '__main__':
    main()

