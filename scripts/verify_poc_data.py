"""Verify POC collected data."""

import sys
from pathlib import Path

# Fix Unicode encoding for Windows PowerShell
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        # Python < 3.7 doesn't have reconfigure
        pass

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_manager import DatabaseManager

db = DatabaseManager()

print("=== POC Data Verification ===\n")

# Check teams
teams = db.get_all_teams()
print(f"Teams in database: {len(teams)}")
if teams:
    print(f"Sample teams: {[t.team_name for t in teams[:3]]}")

# Check games
games = db.get_games(limit=100)
print(f"\nGames in database: {len(games)}")
if games:
    print("\nSample games:")
    for g in games[:5]:
        print(f"  {g.game_id}: {g.game_date} - {g.home_team_id} vs {g.away_team_id}")
        print(f"    Score: {g.home_score} - {g.away_score}")
        print(f"    Winner: {g.winner}")
        print(f"    Status: {g.game_status}")

# Check team stats
print("\n" + "="*60)
print("Team Stats Check:")
print("="*60)
with db.get_session() as session:
    from src.database.models import TeamStats
    team_stats_count = session.query(TeamStats).count()
    print(f"Team stats records: {team_stats_count}")
    
    if team_stats_count > 0:
        sample_team_stat = session.query(TeamStats).first()
        print(f"Sample team stat: Game {sample_team_stat.game_id}, Team {sample_team_stat.team_id}, Points: {sample_team_stat.points}")

# Check player stats
print("\n" + "="*60)
print("Player Stats Check:")
print("="*60)
with db.get_session() as session:
    from src.database.models import PlayerStats
    player_stats_count = session.query(PlayerStats).count()
    print(f"Player stats records: {player_stats_count}")
    
    if player_stats_count > 0:
        sample_player_stat = session.query(PlayerStats).first()
        print(f"Sample player stat: Game {sample_player_stat.game_id}, Player {sample_player_stat.player_name}, Points: {sample_player_stat.points}")

print("\n" + "="*60)
print("Summary:")
print("="*60)
print(f"✓ Teams: {len(teams)}")
print(f"✓ Games: {len(games)}")
print(f"{'✓' if team_stats_count > 0 else '✗'} Team Stats: {team_stats_count}")
print(f"{'✓' if player_stats_count > 0 else '✗'} Player Stats: {player_stats_count}")

if team_stats_count == 0 and player_stats_count == 0:
    print("\n⚠️  WARNING: No team or player stats collected yet!")
    print("   Games are stored, but stats need to be implemented for training.")

print("\nPOC data verification complete!")

