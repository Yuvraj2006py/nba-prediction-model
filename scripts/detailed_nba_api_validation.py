"""Detailed validation of NBA API collected data."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats, Team
from sqlalchemy import func, and_
from datetime import date
import json

def validate_nba_api_data():
    """Comprehensive validation of NBA API collected data."""
    print("=" * 70)
    print("NBA API Data Validation - Comprehensive Review")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Get all seasons
        seasons = session.query(Game.season).distinct().order_by(Game.season).all()
        seasons = [s[0] for s in seasons if s[0]]
        
        print(f"\nSeasons in database: {', '.join(seasons)}")
        
        all_issues = {
            'missing_rebounds': [],
            'zero_rebounds': [],
            'percentage_format': [],
            'missing_scores': [],
            'winner_mismatch': [],
            'missing_stats': [],
            'data_consistency': []
        }
        
        for season in seasons:
            print(f"\n{'='*70}")
            print(f"Season: {season}")
            print(f"{'='*70}")
            
            games = session.query(Game).filter(Game.season == season).all()
            finished_games = [g for g in games if g.game_status == 'finished']
            
            print(f"Total games: {len(games)}")
            print(f"Finished games: {len(finished_games)}")
            
            # Check coverage
            games_with_team_stats = 0
            games_with_player_stats = 0
            games_with_both = 0
            games_missing = []
            
            for game in finished_games:
                team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
                player_stats = session.query(PlayerStats).filter_by(game_id=game.game_id).all()
                
                if team_stats:
                    games_with_team_stats += 1
                if player_stats:
                    games_with_player_stats += 1
                if team_stats and player_stats:
                    games_with_both += 1
                if not team_stats or not player_stats:
                    games_missing.append(game.game_id)
            
            print(f"\nCoverage:")
            print(f"  Games with team stats: {games_with_team_stats}/{len(finished_games)} ({games_with_team_stats/len(finished_games)*100:.1f}%)" if finished_games else "  N/A")
            print(f"  Games with player stats: {games_with_player_stats}/{len(finished_games)} ({games_with_player_stats/len(finished_games)*100:.1f}%)" if finished_games else "  N/A")
            print(f"  Games with both: {games_with_both}/{len(finished_games)} ({games_with_both/len(finished_games)*100:.1f}%)" if finished_games else "  N/A")
            print(f"  Games missing stats: {len(games_missing)}")
            
            if games_missing:
                all_issues['missing_stats'].extend([(season, gid) for gid in games_missing[:10]])
            
            # Detailed data quality checks
            print(f"\nData Quality Checks:")
            
            # Check rebounds
            zero_rebound_games = []
            for game in finished_games[:100]:  # Sample first 100
                team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
                for ts in team_stats:
                    if ts.rebounds_total == 0 and ts.rebounds_offensive == 0 and ts.rebounds_defensive == 0:
                        if ts.points > 0:  # Only flag if team scored
                            zero_rebound_games.append((game.game_id, ts.team_id, ts.points))
            
            if zero_rebound_games:
                print(f"  [ISSUE] Zero rebounds: {len(zero_rebound_games)} team-game records (in sample)")
                all_issues['zero_rebounds'].extend(zero_rebound_games[:5])
            
            # Check percentage format
            percentage_issues = []
            for game in finished_games[:100]:  # Sample first 100
                team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
                for ts in team_stats:
                    # Check if percentage is stored as decimal (0-1) when it should be (0-100)
                    fg_pct = ts.field_goal_percentage
                    if ts.field_goals_attempted > 0:
                        calc_pct = (ts.field_goals_made / ts.field_goals_attempted) * 100.0
                        # If stored as decimal, it will be much smaller
                        if fg_pct > 0 and fg_pct < 1.0:
                            if abs(calc_pct - (fg_pct * 100)) > 1:
                                percentage_issues.append((game.game_id, ts.team_id, 'FG%', fg_pct, calc_pct))
                        elif fg_pct > 1.0 and fg_pct < 100.0:
                            # Stored as percentage, check if correct
                            if abs(calc_pct - fg_pct) > 1:
                                percentage_issues.append((game.game_id, ts.team_id, 'FG%', fg_pct, calc_pct))
            
            if percentage_issues:
                print(f"  [ISSUE] Percentage format issues: {len(percentage_issues)} records (in sample)")
                all_issues['percentage_format'].extend(percentage_issues[:5])
            else:
                print(f"  [OK] Percentage format looks correct")
            
            # Check scores and winners
            missing_scores = []
            winner_mismatches = []
            for game in finished_games[:100]:  # Sample first 100
                if game.game_status == 'finished':
                    if game.home_score is None or game.away_score is None:
                        missing_scores.append(game.game_id)
                    elif game.home_score is not None and game.away_score is not None and game.winner:
                        expected_winner = game.home_team_id if game.home_score > game.away_score else game.away_team_id
                        if game.home_score == game.away_score:
                            expected_winner = None
                        if expected_winner and game.winner != expected_winner:
                            winner_mismatches.append((game.game_id, expected_winner, game.winner, game.home_score, game.away_score))
            
            if missing_scores:
                print(f"  [ISSUE] Missing scores: {len(missing_scores)} games (in sample)")
                all_issues['missing_scores'].extend(missing_scores[:5])
            else:
                print(f"  [OK] All games have scores")
            
            if winner_mismatches:
                print(f"  [ISSUE] Winner mismatches: {len(winner_mismatches)} games (in sample)")
                all_issues['winner_mismatch'].extend(winner_mismatches[:5])
            else:
                print(f"  [OK] All winners match scores")
            
            # Sample detailed game inspection
            print(f"\nSample Game Details (first 3 games):")
            sample_games = session.query(Game).filter(
                Game.season == season,
                Game.game_status == 'finished'
            ).order_by(Game.game_date).limit(3).all()
            
            for game in sample_games:
                print(f"\n  Game {game.game_id} ({game.game_date}):")
                print(f"    {game.away_team_id} @ {game.home_team_id}")
                print(f"    Score: {game.away_score} - {game.home_score}")
                print(f"    Winner: {game.winner}")
                
                team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
                print(f"    Team Stats: {len(team_stats)} records")
                for ts in team_stats:
                    home_away = "Home" if ts.is_home else "Away"
                    # Display percentage correctly
                    fg_pct = ts.field_goal_percentage * 100 if ts.field_goal_percentage < 1.0 else ts.field_goal_percentage
                    three_pct = ts.three_point_percentage * 100 if ts.three_point_percentage < 1.0 else ts.three_point_percentage
                    ft_pct = ts.free_throw_percentage * 100 if ts.free_throw_percentage < 1.0 else ts.free_throw_percentage
                    
                    print(f"      {home_away} Team {ts.team_id}:")
                    print(f"        Points: {ts.points}")
                    print(f"        FG: {ts.field_goals_made}/{ts.field_goals_attempted} ({fg_pct:.1f}%)")
                    print(f"        3P: {ts.three_pointers_made}/{ts.three_pointers_attempted} ({three_pct:.1f}%)")
                    print(f"        FT: {ts.free_throws_made}/{ts.free_throws_attempted} ({ft_pct:.1f}%)")
                    print(f"        Rebounds: Total={ts.rebounds_total}, Off={ts.rebounds_offensive}, Def={ts.rebounds_defensive}")
                    print(f"        Assists: {ts.assists}, Steals: {ts.steals}, Blocks: {ts.blocks}")
                    print(f"        Turnovers: {ts.turnovers}, Fouls: {ts.personal_fouls}")
                
                player_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
                print(f"    Player Stats: {player_count} records")
                
                # Sample a few players
                players = session.query(PlayerStats).filter_by(game_id=game.game_id).limit(3).all()
                for ps in players:
                    print(f"      {ps.player_name}: {ps.points} pts, {ps.rebounds} reb, {ps.assists} ast")
        
        # Final summary
        print(f"\n{'='*70}")
        print("Overall Summary")
        print(f"{'='*70}")
        
        total_issues = sum(len(v) for v in all_issues.values())
        if total_issues == 0:
            print("\n[OK] No data quality issues found!")
        else:
            print(f"\n[WARNING] Found {total_issues} potential issues across all seasons")
            print("\nIssue Breakdown:")
            for issue_type, issues in all_issues.items():
                if issues:
                    print(f"  {issue_type}: {len(issues)} issues")
                    if issue_type == 'zero_rebounds':
                        print(f"    Sample: {issues[:3]}")
                    elif issue_type == 'percentage_format':
                        print(f"    Sample: {issues[:2]}")
                    elif issue_type == 'winner_mismatch':
                        print(f"    Sample: {issues[:2]}")
        
        print(f"\n{'='*70}")
        
        return all_issues


if __name__ == "__main__":
    result = validate_nba_api_data()
    
    # Exit with error code if there are critical issues
    critical_issues = len(result['missing_scores']) + len(result['winner_mismatch'])
    sys.exit(1 if critical_issues > 0 else 0)
