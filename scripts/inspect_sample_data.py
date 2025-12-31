"""Inspect sample data from database to verify quality."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats, Team
from sqlalchemy import func

db_manager = DatabaseManager()

print("=" * 70)
print("SAMPLE DATA INSPECTION")
print("=" * 70)

with db_manager.get_session() as session:
    # Sample a few games
    print("\n[Sample Games]")
    games = session.query(Game).order_by(Game.game_date.desc()).limit(5).all()
    for game in games:
        print(f"\nGame ID: {game.game_id}")
        print(f"  Season: {game.season}")
        print(f"  Date: {game.game_date}")
        print(f"  Home: {game.home_team_id} (Score: {game.home_score})")
        print(f"  Away: {game.away_team_id} (Score: {game.away_score})")
        print(f"  Winner: {game.winner}")
        print(f"  Status: {game.game_status}")
        
        # Get team stats for this game
        team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
        print(f"  Team Stats: {len(team_stats)} records")
        for ts in team_stats:
            print(f"    - Team {ts.team_id} (Home: {ts.is_home}): {ts.points} pts, {ts.field_goals_made}/{ts.field_goals_attempted} FG ({ts.field_goal_percentage:.1f}%)")
        
        # Get player stats count
        player_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
        print(f"  Player Stats: {player_count} records")
    
    # Check a game with winner mismatch
    print("\n\n[Games with Winner Mismatches]")
    all_games = session.query(Game).filter(
        Game.home_score.isnot(None),
        Game.away_score.isnot(None),
        Game.winner.isnot(None)
    ).all()
    
    mismatch_count = 0
    for game in all_games[:10]:  # Check first 10
        expected_winner = game.home_team_id if game.home_score > game.away_score else game.away_team_id
        if game.winner != expected_winner:
            mismatch_count += 1
            print(f"\nGame {game.game_id}:")
            print(f"  Home: {game.home_team_id} ({game.home_score}) vs Away: {game.away_team_id} ({game.away_score})")
            print(f"  Expected winner: {expected_winner}, Stored winner: {game.winner}")
    
    if mismatch_count == 0:
        print("  (No mismatches in first 10 games checked)")
    
    # Check team stats quality
    print("\n\n[Team Stats Quality Sample]")
    sample_team_stats = session.query(TeamStats).limit(5).all()
    for ts in sample_team_stats:
        print(f"\nGame {ts.game_id}, Team {ts.team_id}:")
        print(f"  Points: {ts.points}")
        # Display percentages correctly (handle both decimal and percentage formats)
        fg_pct = ts.field_goal_percentage * 100 if ts.field_goal_percentage < 1.0 else ts.field_goal_percentage
        three_pct = ts.three_point_percentage * 100 if ts.three_point_percentage < 1.0 else ts.three_point_percentage
        ft_pct = ts.free_throw_percentage * 100 if ts.free_throw_percentage < 1.0 else ts.free_throw_percentage
        
        print(f"  FG: {ts.field_goals_made}/{ts.field_goals_attempted} ({fg_pct:.1f}%)")
        print(f"  3P: {ts.three_pointers_made}/{ts.three_pointers_attempted} ({three_pct:.1f}%)")
        print(f"  FT: {ts.free_throws_made}/{ts.free_throws_attempted} ({ft_pct:.1f}%)")
        print(f"  Rebounds: {ts.rebounds_total} (O: {ts.rebounds_offensive}, D: {ts.rebounds_defensive})")
        print(f"  Assists: {ts.assists}, Steals: {ts.steals}, Blocks: {ts.blocks}")
        
        # Verify percentage calculation
        if ts.field_goals_attempted > 0:
            calc_fg_pct = (ts.field_goals_made / ts.field_goals_attempted) * 100
            # Check if stored as decimal (0-1) or percentage (0-100)
            stored_pct = ts.field_goal_percentage
            if stored_pct < 1.0:
                stored_pct = stored_pct * 100
            diff = abs(calc_fg_pct - stored_pct)
            if diff > 0.5:
                print(f"  WARNING: FG% mismatch: Calculated {calc_fg_pct:.1f}%, Stored {ts.field_goal_percentage:.1f}%")
    
    # Check player stats quality
    print("\n\n[Player Stats Quality Sample]")
    sample_player_stats = session.query(PlayerStats).limit(5).all()
    for ps in sample_player_stats:
        print(f"\nGame {ps.game_id}, Player: {ps.player_name} ({ps.player_id})")
        print(f"  Team: {ps.team_id}")
        print(f"  Minutes: {ps.minutes_played}")
        print(f"  Points: {ps.points}, Rebounds: {ps.rebounds}, Assists: {ps.assists}")
        print(f"  FG: {ps.field_goals_made}/{ps.field_goals_attempted}")
        print(f"  3P: {ps.three_pointers_made}/{ps.three_pointers_attempted}")
        print(f"  FT: {ps.free_throws_made}/{ps.free_throws_attempted}")
        print(f"  Plus/Minus: {ps.plus_minus}")
    
    # Check season distribution
    print("\n\n[Season Distribution]")
    season_dist = session.query(
        Game.season,
        func.count(Game.game_id).label('count'),
        func.min(Game.game_date).label('min_date'),
        func.max(Game.game_date).label('max_date')
    ).group_by(Game.season).all()
    
    for season, count, min_date, max_date in season_dist:
        print(f"\n{season}:")
        print(f"  Games: {count}")
        print(f"  Date range: {min_date} to {max_date}")
    
    # Check games missing stats
    print("\n\n[Games Missing Stats]")
    finished_games = session.query(Game).filter(Game.game_status == 'finished').all()
    games_without_team_stats = []
    games_without_player_stats = []
    
    for game in finished_games[:20]:  # Check first 20
        team_stats_count = session.query(TeamStats).filter_by(game_id=game.game_id).count()
        player_stats_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
        
        if team_stats_count == 0:
            games_without_team_stats.append(game.game_id)
        if player_stats_count == 0:
            games_without_player_stats.append(game.game_id)
    
    if games_without_team_stats:
        print(f"\nGames without team stats (sample): {games_without_team_stats[:5]}")
    else:
        print("  All sampled games have team stats")
    
    if games_without_player_stats:
        print(f"\nGames without player stats (sample): {games_without_player_stats[:5]}")
    else:
        print("  All sampled games have player stats")

print("\n" + "=" * 70)
