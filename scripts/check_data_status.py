"""Check current data collection status."""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats, Team
from sqlalchemy import func

# Check both databases
main_db = DatabaseManager()
historical_db_path = Path(project_root) / "data" / "nba_predictions_historical.db"
historical_db = DatabaseManager(database_url=f"sqlite:///{historical_db_path}")

print("=" * 70)
print("Data Collection Status - ALL DATABASES")
print("=" * 70)

# Check MAIN database
print("\n[MAIN DATABASE: nba_predictions.db]")
print("-" * 70)
with main_db.get_session() as session:
    teams_count = session.query(Team).count()
    print(f"Teams: {teams_count}/30")
    
    # Get all unique seasons
    seasons_data = session.query(
        Game.season, 
        func.count(Game.game_id).label('game_count')
    ).group_by(Game.season).order_by(Game.season.desc()).all()
    
    if seasons_data:
        print("\nGames by Season:")
        total_games = 0
        for season, game_count in seasons_data:
            if season:
                print(f"  {season}: {game_count} games")
                total_games += game_count
        print(f"\nTotal Games: {total_games}")
        
        # Stats
        team_stats_count = session.query(TeamStats).count()
        player_stats_count = session.query(PlayerStats).count()
        print(f"Team Stats: {team_stats_count} records")
        print(f"Player Stats: {player_stats_count} records")
    else:
        print("No games found in main database")

# Check HISTORICAL database
print("\n" + "=" * 70)
print("[HISTORICAL DATABASE: nba_predictions_historical.db]")
print("-" * 70)

if historical_db_path.exists():
    with historical_db.get_session() as session:
        teams_count = session.query(Team).count()
        print(f"Teams: {teams_count}/30")
        
        # Get all unique seasons
        seasons_data = session.query(
            Game.season, 
            func.count(Game.game_id).label('game_count')
        ).group_by(Game.season).order_by(Game.season.desc()).all()
        
        if seasons_data:
            print("\nGames by Season:")
            total_games = 0
            for season, game_count in seasons_data:
                if season:
                    print(f"  {season}: {game_count} games")
                    total_games += game_count
            print(f"\nTotal Games: {total_games}")
            
            # Stats
            team_stats_count = session.query(TeamStats).count()
            player_stats_count = session.query(PlayerStats).count()
            print(f"Team Stats: {team_stats_count} records")
            print(f"Player Stats: {player_stats_count} records")
        else:
            print("No games found in historical database")
else:
    print("Historical database file does not exist")

# Expected totals
print("\n" + "=" * 70)
print("Expected Totals (for 3 seasons):")
print("  Games: ~3,690 (1,230 per season)")
print("  Team Stats: ~7,380 (2 per game)")
print("  Player Stats: ~150,000+ (varies by game)")
print("=" * 70)
