"""Check what data is available in the database for training."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Game
from datetime import date

def main():
    db = DatabaseManager()
    
    with db.get_session() as session:
        # Get date range
        earliest = session.query(Game).order_by(Game.game_date.asc()).first()
        latest = session.query(Game).order_by(Game.game_date.desc()).first()
        
        print("=" * 70)
        print("DATABASE DATA SUMMARY")
        print("=" * 70)
        print(f"\nDate Range: {earliest.game_date} to {latest.game_date}")
        
        # Get seasons
        seasons = session.query(Game.season).distinct().all()
        season_list = sorted(set([s[0] for s in seasons if s[0]]))
        print(f"\nSeasons Available: {season_list}")
        
        # Count games per season
        print(f"\nGames per Season:")
        for season in season_list:
            count = session.query(Game).filter_by(season=season).count()
            finished = session.query(Game).filter(
                Game.season == season,
                Game.home_score.isnot(None)
            ).count()
            print(f"  {season}: {count} total, {finished} finished")
        
        # Check current season (2025-26)
        current_season = "2025-26"
        if current_season in season_list:
            current_games = session.query(Game).filter_by(season=current_season).all()
            finished_current = [g for g in current_games if g.home_score is not None]
            print(f"\n{current_season} Season:")
            print(f"  Total games: {len(current_games)}")
            print(f"  Finished games: {len(finished_current)}")
            print(f"  Available for training: {len(finished_current)}")
            
            if finished_current:
                dates = sorted([g.game_date for g in finished_current])
                print(f"  Date range: {dates[0]} to {dates[-1]}")
        else:
            print(f"\n{current_season} season not found in database")
        
        # Most recent games
        print(f"\nMost Recent 10 Games:")
        recent = session.query(Game).order_by(Game.game_date.desc()).limit(10).all()
        for g in recent:
            status = "Finished" if g.home_score else "Scheduled"
            print(f"  {g.game_date} - {g.season} - {status}")

if __name__ == "__main__":
    main()



