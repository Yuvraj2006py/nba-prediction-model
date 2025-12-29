"""Check current data collection status."""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats, Team
from datetime import date

db = DatabaseManager()

print("=" * 70)
print("Data Collection Status")
print("=" * 70)

# Check teams
with db.get_session() as session:
    teams_count = session.query(Team).count()
    print(f"\nTeams: {teams_count}/30")

# Check games by season
seasons = ['2022-23', '2023-24', '2024-25']
total_games = 0

print("\nGames by Season:")
print("-" * 70)
with db.get_session() as session:
    for season in seasons:
        games = session.query(Game).filter(Game.season == season).count()
        total_games += games
        print(f"  {season}: {games} games")
    
    all_games = session.query(Game).count()
    print(f"\nTotal Games: {all_games}")

# Check stats
with db.get_session() as session:
    team_stats_count = session.query(TeamStats).count()
    player_stats_count = session.query(PlayerStats).count()
    
    print(f"\nTeam Stats: {team_stats_count} records")
    print(f"Player Stats: {player_stats_count} records")
    
    # Calculate coverage
    games_with_team_stats = session.query(Game).join(TeamStats).distinct().count()
    games_with_player_stats = session.query(Game).join(PlayerStats).distinct().count()
    
    print(f"\nCoverage:")
    print(f"  Games with team stats: {games_with_team_stats}/{all_games} ({games_with_team_stats/all_games*100:.1f}%)" if all_games > 0 else "  Games with team stats: 0")
    print(f"  Games with player stats: {games_with_player_stats}/{all_games} ({games_with_player_stats/all_games*100:.1f}%)" if all_games > 0 else "  Games with player stats: 0")

# Expected totals
print("\n" + "=" * 70)
print("Expected Totals (for 3 seasons):")
print("  Games: ~3,690 (1,230 per season)")
print("  Team Stats: ~7,380 (2 per game)")
print("  Player Stats: ~150,000+ (varies by game)")
print("=" * 70)

