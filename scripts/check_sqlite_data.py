"""Check data status in SQLite database."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.database.db_manager import DatabaseManager
from src.database.models import Team, Game, TeamStats, PlayerStats, BettingLine

db_manager = DatabaseManager()

print("=" * 70)
print("Database Status - SQLite")
print("=" * 70)

with db_manager.get_session() as session:
    teams_count = session.query(Team).count()
    games_count = session.query(Game).count()
    team_stats_count = session.query(TeamStats).count()
    player_stats_count = session.query(PlayerStats).count()
    betting_lines_count = session.query(BettingLine).count()
    
    print(f"\nTeams: {teams_count}")
    print(f"Games: {games_count}")
    print(f"Team Stats: {team_stats_count}")
    print(f"Player Stats: {player_stats_count}")
    print(f"Betting Lines: {betting_lines_count}")
    
    # Check by season
    if games_count > 0:
        print("\nGames by Season:")
        from sqlalchemy import func
        season_counts = session.query(Game.season, func.count(Game.game_id)).group_by(Game.season).all()
        for season, count in season_counts:
            print(f"  {season}: {count} games")
    
    # Check games with stats
    if games_count > 0:
        games_with_team_stats = session.query(func.count(func.distinct(TeamStats.game_id))).scalar()
        games_with_player_stats = session.query(func.count(func.distinct(PlayerStats.game_id))).scalar()
        print(f"\nGames with Team Stats: {games_with_team_stats} ({games_with_team_stats/games_count*100:.1f}%)")
        print(f"Games with Player Stats: {games_with_player_stats} ({games_with_player_stats/games_count*100:.1f}%)")

print("=" * 70)


