"""
Fix game dates - move finished games to correct date (yesterday if they have scores).
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

from datetime import date, timedelta
from src.database.db_manager import DatabaseManager
from src.database.models import Game

def main():
    """Fix incorrectly dated games."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    
    print("=" * 70)
    print(f"FIXING GAME DATES")
    print("=" * 70)
    print(f"Today: {today}")
    print(f"Yesterday: {yesterday}")
    
    db = DatabaseManager()
    
    with db.get_session() as session:
        # Find games dated today that have scores (finished games)
        finished_today = session.query(Game).filter(
            Game.game_date == today
        ).filter(
            (Game.home_score.isnot(None)) | (Game.away_score.isnot(None))
        ).all()
        
        print(f"\n[STEP 1] Found {len(finished_today)} finished games dated {today}")
        
        if finished_today:
            print("  These games have scores and should be moved to yesterday:")
            for game in finished_today:
                away_team = db.get_team(game.away_team_id)
                home_team = db.get_team(game.home_team_id)
                away_name = away_team.team_name if away_team else game.away_team_id
                home_name = home_team.team_name if home_team else game.home_team_id
                print(f"    - {away_name} @ {home_name}")
                print(f"      Score: {game.away_score} - {game.home_score}, Status: {game.game_status}")
            
            # Move them to yesterday
            print(f"\n[STEP 2] Moving {len(finished_today)} games to {yesterday}...")
            for game in finished_today:
                game.game_date = yesterday
                if game.game_status == 'scheduled':
                    game.game_status = 'finished'
            session.commit()
            print(f"  [OK] Moved {len(finished_today)} games to {yesterday}")
        
        # Also check for games dated today that are scheduled but should be yesterday
        # (games that happened yesterday evening US time but have today's UTC date)
        scheduled_today = session.query(Game).filter(
            Game.game_date == today,
            Game.game_status == 'scheduled',
            Game.home_score.is_(None),
            Game.away_score.is_(None)
        ).all()
        
        print(f"\n[STEP 3] Checking {len(scheduled_today)} scheduled games dated {today}...")
        
        # For now, we'll keep scheduled games as-is since we can't determine
        # if they're actually today or yesterday without checking the actual game time
        # The user will need to verify these manually or we can check game times
        
        # Final count
        today_count = session.query(Game).filter(Game.game_date == today).count()
        yesterday_count = session.query(Game).filter(Game.game_date == yesterday).count()
        
        print(f"\n[STEP 4] Final counts:")
        print(f"  Games on {yesterday}: {yesterday_count}")
        print(f"  Games on {today}: {today_count}")
    
    print("\n" + "=" * 70)
    print("FIX COMPLETE")
    print("=" * 70)

if __name__ == '__main__':
    main()

