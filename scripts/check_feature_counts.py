"""Check how many features games have."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Game, Feature

db_manager = DatabaseManager()

with db_manager.get_session() as session:
    # Get a sample game from each season
    for season in ['2022-23', '2023-24', '2024-25']:
        game = session.query(Game).filter(Game.season == season).first()
        if game:
            features = session.query(Feature).filter_by(game_id=game.game_id).all()
            print(f"{season}: Game {game.game_id} has {len(features)} features")
            if len(features) > 0:
                print(f"  Sample: {[f.feature_name for f in features[:5]]}")
        else:
            print(f"{season}: No games found")
    
    # Get average feature count
    from sqlalchemy import func
    avg_features = session.query(
        func.avg(func.count(Feature.id))
    ).group_by(Feature.game_id).scalar()
    
    print(f"\nAverage features per game: {avg_features:.1f}" if avg_features else "\nNo features found")



