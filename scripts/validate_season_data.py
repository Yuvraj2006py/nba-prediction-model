"""Validate data quality for a specific season."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats, Team
from sqlalchemy import func, and_
from datetime import date

def validate_season(season: str):
    """Validate data quality for a season."""
    print("=" * 70)
    print(f"Data Validation for Season: {season}")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Get all games for the season
        games = session.query(Game).filter(Game.season == season).all()
        total_games = len(games)
        
        print(f"\nTotal games: {total_games}")
        
        # Check finished games
        finished_games = [g for g in games if g.game_status == 'finished']
        print(f"Finished games: {len(finished_games)}")
        
        # Check games with stats
        games_with_team_stats = []
        games_with_player_stats = []
        games_with_both = []
        games_missing_stats = []
        
        issues = {
            'missing_rebounds': [],
            'zero_rebounds': [],
            'invalid_percentages': [],
            'missing_scores': [],
            'winner_mismatch': []
        }
        
        for game in finished_games:
            team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
            player_stats = session.query(PlayerStats).filter_by(game_id=game.game_id).all()
            
            has_team_stats = len(team_stats) > 0
            has_player_stats = len(player_stats) > 0
            
            if has_team_stats:
                games_with_team_stats.append(game.game_id)
            if has_player_stats:
                games_with_player_stats.append(game.game_id)
            if has_team_stats and has_player_stats:
                games_with_both.append(game.game_id)
            if not has_team_stats or not has_player_stats:
                games_missing_stats.append(game.game_id)
            
            # Check data quality issues
            for ts in team_stats:
                # Check rebounds
                if ts.rebounds_total == 0 and ts.rebounds_offensive == 0 and ts.rebounds_defensive == 0:
                    if ts.points > 0:  # Only flag if team scored (game was played)
                        issues['zero_rebounds'].append((game.game_id, ts.team_id))
                
                # Check percentages (should be between 0 and 100, or 0 and 1)
                fg_pct = ts.field_goal_percentage
                if fg_pct < 0 or (fg_pct > 1 and fg_pct > 100):
                    issues['invalid_percentages'].append((game.game_id, ts.team_id, 'FG%', fg_pct))
                elif fg_pct > 0 and fg_pct < 1:
                    # Might be stored as decimal, check if it should be percentage
                    if ts.field_goals_attempted > 0:
                        calc_pct = (ts.field_goals_made / ts.field_goals_attempted) * 100
                        if abs(calc_pct - fg_pct) > 1:  # More than 1% difference
                            issues['invalid_percentages'].append((game.game_id, ts.team_id, 'FG%', fg_pct, calc_pct))
            
            # Check scores
            if game.home_score is None or game.away_score is None:
                if game.game_status == 'finished':
                    issues['missing_scores'].append(game.game_id)
            
            # Check winner
            if game.home_score is not None and game.away_score is not None and game.winner:
                expected_winner = game.home_team_id if game.home_score > game.away_score else game.away_team_id
                if game.home_score == game.away_score:
                    expected_winner = None  # Tie
                if expected_winner and game.winner != expected_winner:
                    issues['winner_mismatch'].append((game.game_id, expected_winner, game.winner))
        
        # Print summary
        print(f"\n{'='*70}")
        print("Coverage Summary")
        print(f"{'='*70}")
        print(f"Games with team stats: {len(games_with_team_stats)}/{len(finished_games)} ({len(games_with_team_stats)/len(finished_games)*100:.1f}%)" if finished_games else "N/A")
        print(f"Games with player stats: {len(games_with_player_stats)}/{len(finished_games)} ({len(games_with_player_stats)/len(finished_games)*100:.1f}%)" if finished_games else "N/A")
        print(f"Games with both: {len(games_with_both)}/{len(finished_games)} ({len(games_with_both)/len(finished_games)*100:.1f}%)" if finished_games else "N/A")
        print(f"Games missing stats: {len(games_missing_stats)}")
        
        # Print issues
        print(f"\n{'='*70}")
        print("Data Quality Issues")
        print(f"{'='*70}")
        
        if issues['missing_scores']:
            print(f"\n[WARNING] Missing scores: {len(issues['missing_scores'])} games")
            print(f"  Sample: {issues['missing_scores'][:5]}")
        
        if issues['zero_rebounds']:
            print(f"\n[WARNING] Zero rebounds (but scored points): {len(issues['zero_rebounds'])} team-game records")
            print(f"  Sample: {issues['zero_rebounds'][:5]}")
        
        if issues['invalid_percentages']:
            print(f"\n[WARNING] Invalid percentages: {len(issues['invalid_percentages'])} records")
            for issue in issues['invalid_percentages'][:5]:
                if len(issue) == 5:
                    print(f"  Game {issue[0]}, Team {issue[1]}, {issue[2]}: stored={issue[3]:.3f}, calculated={issue[4]:.1f}")
                else:
                    print(f"  Game {issue[0]}, Team {issue[1]}, {issue[2]}: {issue[3]:.3f}")
        
        if issues['winner_mismatch']:
            print(f"\n[WARNING] Winner mismatches: {len(issues['winner_mismatch'])} games")
            for issue in issues['winner_mismatch'][:5]:
                print(f"  Game {issue[0]}: expected={issue[1]}, stored={issue[2]}")
        
        if not any(issues.values()):
            print("\n[OK] No data quality issues found!")
        
        # Sample some games for manual inspection
        print(f"\n{'='*70}")
        print("Sample Games (for manual inspection)")
        print(f"{'='*70}")
        
        sample_games = session.query(Game).filter(
            Game.season == season,
            Game.game_status == 'finished'
        ).order_by(Game.game_date).limit(3).all()
        
        for game in sample_games:
            print(f"\nGame {game.game_id} ({game.game_date}):")
            print(f"  {game.away_team_id} @ {game.home_team_id}")
            print(f"  Score: {game.away_score} - {game.home_score}")
            print(f"  Winner: {game.winner}")
            
            team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
            print(f"  Team stats: {len(team_stats)} records")
            for ts in team_stats:
                home_away = "Home" if ts.is_home else "Away"
                fg_pct = ts.field_goal_percentage * 100 if ts.field_goal_percentage < 1.0 else ts.field_goal_percentage
                print(f"    {home_away} Team {ts.team_id}: {ts.points} pts, "
                      f"{ts.field_goals_made}/{ts.field_goals_attempted} FG ({fg_pct:.1f}%), "
                      f"{ts.rebounds_total} reb")
            
            player_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
            print(f"  Player stats: {player_count} records")
        
        print(f"\n{'='*70}")
        
        return {
            'total_games': total_games,
            'finished_games': len(finished_games),
            'games_with_team_stats': len(games_with_team_stats),
            'games_with_player_stats': len(games_with_player_stats),
            'games_missing_stats': len(games_missing_stats),
            'issues': issues
        }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate data quality for a season')
    parser.add_argument('--season', type=str, default='2022-23', help='Season to validate (default: 2022-23)')
    
    args = parser.parse_args()
    
    result = validate_season(args.season)
    
    # Exit with error code if there are issues
    total_issues = sum(len(v) for v in result['issues'].values())
    sys.exit(1 if total_issues > 0 else 0)
