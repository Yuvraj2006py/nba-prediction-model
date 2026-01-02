"""
Fix games that happened yesterday but are dated today.
Specifically fixes Utah Jazz @ LA Clippers and 76ers @ Mavs.
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

def main():
    """Fix specific games that happened yesterday."""
    today = date(2026, 1, 2)
    yesterday = date(2026, 1, 1)
    
    print("=" * 70)
    print(f"FIXING YESTERDAY'S GAMES")
    print("=" * 70)
    
    db = DatabaseManager()
    
    # Specific games that happened yesterday
    yesterday_game_ids = [
        '20260102762746',  # Utah Jazz @ LA Clippers
        '20260102755742'   # Philadelphia 76ers @ Dallas Mavericks
    ]
    
    with db.get_session() as session:
        fixed_count = 0
        for game_id in yesterday_game_ids:
            game = session.query(Game).filter(Game.game_id == game_id).first()
            if game and game.game_date == today:
                away_team = db.get_team(game.away_team_id)
                home_team = db.get_team(game.home_team_id)
                away_name = away_team.team_name if away_team else game.away_team_id
                home_name = home_team.team_name if home_team else game.home_team_id
                
                print(f"\nFixing: {away_name} @ {home_name}")
                print(f"  Game ID: {game_id}")
                print(f"  Moving from {game.game_date} to {yesterday}")
                
                game.game_date = yesterday
                if game.game_status == 'scheduled':
                    game.game_status = 'finished'
                fixed_count += 1
        
        if fixed_count > 0:
            session.commit()
            print(f"\n[OK] Fixed {fixed_count} games")
        
        # Final counts
        today_count = session.query(Game).filter(Game.game_date == today).count()
        yesterday_count = session.query(Game).filter(Game.game_date == yesterday).count()
        
        print(f"\nFinal counts:")
        print(f"  Games on {yesterday}: {yesterday_count}")
        print(f"  Games on {today}: {today_count}")
    
    print("\n" + "=" * 70)
    print("FIX COMPLETE")
    print("=" * 70)

if __name__ == '__main__':
    main()

