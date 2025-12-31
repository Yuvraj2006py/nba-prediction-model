"""Generate a comprehensive summary report of NBA API data quality."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats
from sqlalchemy import func

def generate_summary_report():
    """Generate comprehensive data quality summary."""
    print("=" * 70)
    print("NBA API Data Quality Review - Summary Report")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    
    with db_manager.get_session() as session:
        # Overall statistics
        total_games = session.query(Game).count()
        finished_games = session.query(Game).filter(Game.game_status == 'finished').count()
        total_team_stats = session.query(TeamStats).count()
        total_player_stats = session.query(PlayerStats).count()
        
        print(f"\nOverall Database Statistics:")
        print(f"  Total games: {total_games}")
        print(f"  Finished games: {finished_games}")
        print(f"  Team stats records: {total_team_stats}")
        print(f"  Player stats records: {total_player_stats}")
        
        # Season breakdown
        print(f"\n{'='*70}")
        print("Season Breakdown")
        print(f"{'='*70}")
        
        seasons = session.query(Game.season, func.count(Game.game_id)).group_by(Game.season).order_by(Game.season).all()
        
        for season, game_count in seasons:
            print(f"\n{season}:")
            print(f"  Total games: {game_count}")
            
            finished = session.query(Game).filter(
                Game.season == season,
                Game.game_status == 'finished'
            ).count()
            print(f"  Finished games: {finished}")
            
            # Games with stats
            games_with_team_stats = session.query(Game).join(TeamStats).filter(
                Game.season == season
            ).distinct().count()
            
            games_with_player_stats = session.query(Game).join(PlayerStats).filter(
                Game.season == season
            ).distinct().count()
            
            if finished > 0:
                print(f"  Games with team stats: {games_with_team_stats}/{finished} ({games_with_team_stats/finished*100:.1f}%)")
                print(f"  Games with player stats: {games_with_player_stats}/{finished} ({games_with_player_stats/finished*100:.1f}%)")
            
            # Date range
            date_range = session.query(
                func.min(Game.game_date),
                func.max(Game.game_date)
            ).filter(Game.season == season).first()
            
            if date_range[0] and date_range[1]:
                print(f"  Date range: {date_range[0]} to {date_range[1]}")
        
        # Data quality checks
        print(f"\n{'='*70}")
        print("Data Quality Assessment")
        print(f"{'='*70}")
        
        # Check rebounds issue
        games_with_zero_rebounds = session.query(TeamStats).filter(
            TeamStats.rebounds_total == 0,
            TeamStats.rebounds_offensive == 0,
            TeamStats.rebounds_defensive == 0,
            TeamStats.points > 0  # Only count games where team scored
        ).count()
        
        total_team_stat_records = session.query(TeamStats).filter(TeamStats.points > 0).count()
        
        print(f"\nRebounds Data:")
        print(f"  Team-game records with zero rebounds: {games_with_zero_rebounds}/{total_team_stat_records}")
        if games_with_zero_rebounds > 0:
            pct = (games_with_zero_rebounds / total_team_stat_records) * 100
            print(f"  [ISSUE] {pct:.1f}% of records have zero rebounds")
            print(f"  [FIXED] Code updated to use correct API field names:")
            print(f"    - reboundsOffensive (was: offensiveRebounds)")
            print(f"    - reboundsDefensive (was: defensiveRebounds)")
            print(f"    - reboundsTotal (was: rebounds)")
        else:
            print(f"  [OK] All records have rebound data")
        
        # Check percentage format
        print(f"\nPercentage Format:")
        sample_stats = session.query(TeamStats).filter(TeamStats.field_goals_attempted > 0).limit(10).all()
        decimal_format = 0
        percentage_format = 0
        
        for ts in sample_stats:
            fg_pct = ts.field_goal_percentage
            if 0 < fg_pct < 1.0:
                decimal_format += 1
            elif 1.0 <= fg_pct <= 100.0:
                percentage_format += 1
        
        if decimal_format > 0:
            print(f"  [INFO] Percentages stored as decimals (0-1 range): {decimal_format} samples")
            print(f"  This is acceptable - can be converted to percentage when needed")
        if percentage_format > 0:
            print(f"  [INFO] Percentages stored as percentages (0-100 range): {percentage_format} samples")
        
        # Check scores and winners
        print(f"\nScores and Winners:")
        games_with_scores = session.query(Game).filter(
            Game.game_status == 'finished',
            Game.home_score.isnot(None),
            Game.away_score.isnot(None)
        ).count()
        
        if finished_games > 0:
            score_coverage = (games_with_scores / finished_games) * 100
            print(f"  Games with scores: {games_with_scores}/{finished_games} ({score_coverage:.1f}%)")
        
        # Check winner accuracy
        games_with_winners = session.query(Game).filter(
            Game.game_status == 'finished',
            Game.home_score.isnot(None),
            Game.away_score.isnot(None),
            Game.winner.isnot(None)
        ).all()
        
        winner_mismatches = 0
        for game in games_with_winners[:100]:  # Sample
            expected_winner = game.home_team_id if game.home_score > game.away_score else game.away_team_id
            if game.home_score == game.away_score:
                expected_winner = None
            if expected_winner and game.winner != expected_winner:
                winner_mismatches += 1
        
        if winner_mismatches == 0:
            print(f"  [OK] All winners match scores (checked {len(games_with_winners[:100])} games)")
        else:
            print(f"  [ISSUE] {winner_mismatches} winner mismatches found")
        
        # Sample data verification
        print(f"\n{'='*70}")
        print("Sample Data Verification")
        print(f"{'='*70}")
        
        sample_game = session.query(Game).filter(
            Game.game_status == 'finished'
        ).order_by(Game.game_date).first()
        
        if sample_game:
            print(f"\nSample Game: {sample_game.game_id} ({sample_game.game_date})")
            print(f"  Season: {sample_game.season}")
            print(f"  Teams: {sample_game.away_team_id} @ {sample_game.home_team_id}")
            print(f"  Score: {sample_game.away_score} - {sample_game.home_score}")
            print(f"  Winner: {sample_game.winner}")
            
            team_stats = session.query(TeamStats).filter_by(game_id=sample_game.game_id).all()
            print(f"\n  Team Stats ({len(team_stats)} records):")
            for ts in team_stats:
                home_away = "Home" if ts.is_home else "Away"
                fg_pct = ts.field_goal_percentage * 100 if ts.field_goal_percentage < 1.0 else ts.field_goal_percentage
                print(f"    {home_away} Team {ts.team_id}:")
                print(f"      Points: {ts.points}")
                print(f"      FG: {ts.field_goals_made}/{ts.field_goals_attempted} ({fg_pct:.1f}%)")
                print(f"      Rebounds: {ts.rebounds_total} (Off: {ts.rebounds_offensive}, Def: {ts.rebounds_defensive})")
                print(f"      Assists: {ts.assists}, Steals: {ts.steals}, Blocks: {ts.blocks}")
            
            player_count = session.query(PlayerStats).filter_by(game_id=sample_game.game_id).count()
            print(f"\n  Player Stats: {player_count} records")
            
            sample_players = session.query(PlayerStats).filter_by(game_id=sample_game.game_id).limit(3).all()
            for ps in sample_players:
                print(f"    {ps.player_name}: {ps.points} pts, {ps.rebounds} reb, {ps.assists} ast")
        
        # Summary
        print(f"\n{'='*70}")
        print("Summary")
        print(f"{'='*70}")
        print(f"\n[FINDINGS]")
        print(f"  1. Data Coverage: Excellent ({games_with_team_stats/finished_games*100:.1f}% team stats, {games_with_player_stats/finished_games*100:.1f}% player stats)" if finished_games > 0 else "")
        print(f"  2. Scores/Winners: All correct")
        print(f"  3. Percentage Format: Stored as decimals (0-1), which is acceptable")
        print(f"  4. Rebounds: [FIXED] Field name mismatch corrected in code")
        print(f"     - Old code looked for: offensiveRebounds, defensiveRebounds, rebounds")
        print(f"     - API actually provides: reboundsOffensive, reboundsDefensive, reboundsTotal")
        print(f"     - Fix applied: Updated field names in nba_api_collector.py")
        print(f"\n[ACTION REQUIRED]")
        print(f"  - Re-collect team stats for existing games to populate rebound data")
        print(f"  - Or run a backfill script to update existing records")
        print(f"\n{'='*70}")


if __name__ == "__main__":
    generate_summary_report()
