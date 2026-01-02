"""Quick script to check if rolling features exist for games."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamRollingFeatures
from datetime import date

def main():
    db = DatabaseManager()
    
    with db.get_session() as session:
        games = session.query(Game).filter(Game.game_date == date(2026, 1, 1)).all()
        print(f'Found {len(games)} games for 2026-01-01\n')
        
        for game in games:
            home_features = session.query(TeamRollingFeatures).filter_by(
                game_id=game.game_id,
                team_id=game.home_team_id
            ).first()
            
            away_features = session.query(TeamRollingFeatures).filter_by(
                game_id=game.game_id,
                team_id=game.away_team_id
            ).first()
            
            status = "[OK] Has features" if (home_features and away_features) else "[MISSING] No features"
            print(f'  Game {game.game_id}: {status}')
            if home_features and away_features:
                print(f'    Home: {game.home_team_id}, Away: {game.away_team_id}')
        
        total_features = session.query(TeamRollingFeatures).filter(
            TeamRollingFeatures.game_id.in_([g.game_id for g in games])
        ).count()
        print(f'\nTotal rolling features: {total_features} (expected: {len(games) * 2})')

if __name__ == '__main__':
    main()

