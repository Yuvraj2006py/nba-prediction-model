"""Check what games exist for January 1st, 2026."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from datetime import date
from src.database.db_manager import DatabaseManager
from src.database.models import Game

def main():
    db = DatabaseManager()
    jan1 = date(2026, 1, 1)
    jan2 = date(2026, 1, 2)
    
    print('=' * 70)
    print('CHECKING GAMES IN DATABASE')
    print('=' * 70)
    
    with db.get_session() as session:
        # All games dated Jan 1st
        games_jan1 = session.query(Game).filter(Game.game_date == jan1).all()
        
        print(f'\nGames with game_date == {jan1}: {len(games_jan1)}')
        for game in games_jan1:
            print(f'  {game.game_id}: {game.away_team_id} @ {game.home_team_id}')
            print(f'    game_id starts with "20260101": {game.game_id.startswith("20260101")}')
            print(f'    Status: {game.game_status}')
            if game.home_score is not None:
                print(f'    Score: {game.away_score} - {game.home_score}')
        
        # Games dated Jan 2nd (might have been incorrectly dated)
        games_jan2 = session.query(Game).filter(Game.game_date == jan2).all()
        print(f'\nGames with game_date == {jan2}: {len(games_jan2)}')
        for game in games_jan2[:15]:  # Show first 15
            print(f'  {game.game_id}: {game.away_team_id} @ {game.home_team_id}')
            print(f'    game_id starts with "20260101": {game.game_id.startswith("20260101")}')
            print(f'    game_id starts with "20260102": {game.game_id.startswith("20260102")}')
            print(f'    Status: {game.game_status}')
            if game.home_score is not None:
                print(f'    Score: {game.away_score} - {game.home_score}')
    
    print('\n' + '=' * 70)

if __name__ == '__main__':
    main()

