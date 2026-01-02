"""Check actual feature list being generated."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamRollingFeatures, GameMatchupFeatures
from src.prediction.prediction_service import PredictionService

def main():
    db = DatabaseManager()
    service = PredictionService(db)
    
    # Get a game
    with db.get_session() as session:
        game = session.query(Game).filter(
            Game.season == '2025-26',
            Game.game_status != 'finished'  # Get upcoming game
        ).first()
        
        if not game:
            print("No upcoming games found")
            return
        
        print(f"Testing with game: {game.game_id}")
        
        # Get features
        features = service.get_features_for_game(game.game_id)
        
        if features is not None and not features.empty:
            print(f"\nTotal features: {len(features.columns)}")
            print(f"\nFeature list:")
            for i, col in enumerate(sorted(features.columns), 1):
                print(f"  {i:3d}. {col}")
        else:
            print("No features generated")

if __name__ == '__main__':
    main()


