"""Comprehensive data quality check for NBA database."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import datetime, date
from typing import Dict, List, Any, Optional
from collections import defaultdict
from sqlalchemy import func, and_, or_
from sqlalchemy.orm import Session

from src.database.db_manager import DatabaseManager
from src.database.models import (
    Team, Game, TeamStats, PlayerStats, BettingLine
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataQualityChecker:
    """Comprehensive data quality checker for NBA database."""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
        self.issues = []
        self.warnings = []
        self.stats = {}
    
    def check_all(self) -> Dict[str, Any]:
        """Run all quality checks."""
        logger.info("=" * 70)
        logger.info("NBA Database Quality Check")
        logger.info("=" * 70)
        
        results = {
            'basic_counts': self.check_basic_counts(),
            'teams': self.check_teams(),
            'games': self.check_games(),
            'team_stats': self.check_team_stats(),
            'player_stats': self.check_player_stats(),
            'data_completeness': self.check_data_completeness(),
            'data_consistency': self.check_data_consistency(),
            'date_ranges': self.check_date_ranges(),
            'duplicates': self.check_duplicates(),
            'sample_records': self.sample_records(),
            'summary': {}
        }
        
        # Generate summary
        results['summary'] = self.generate_summary(results)
        
        return results
    
    def check_basic_counts(self) -> Dict[str, int]:
        """Get basic record counts."""
        logger.info("\n[1/9] Checking basic counts...")
        
        with self.db_manager.get_session() as session:
            counts = {
                'teams': session.query(Team).count(),
                'games': session.query(Game).count(),
                'team_stats': session.query(TeamStats).count(),
                'player_stats': session.query(PlayerStats).count(),
                'betting_lines': session.query(BettingLine).count(),
            }
        
        logger.info(f"  Teams: {counts['teams']}")
        logger.info(f"  Games: {counts['games']}")
        logger.info(f"  Team Stats: {counts['team_stats']}")
        logger.info(f"  Player Stats: {counts['player_stats']}")
        logger.info(f"  Betting Lines: {counts['betting_lines']}")
        
        # Check for minimum expected values
        if counts['teams'] < 30:
            self.warnings.append(f"Only {counts['teams']} teams found (expected 30)")
        
        return counts
    
    def check_teams(self) -> Dict[str, Any]:
        """Check team data quality."""
        logger.info("\n[2/9] Checking teams...")
        
        with self.db_manager.get_session() as session:
            teams = session.query(Team).all()
            
            issues = []
            missing_fields = defaultdict(int)
            
            for team in teams:
                if not team.team_id:
                    issues.append(f"Team missing ID: {team}")
                if not team.team_name:
                    missing_fields['team_name'] += 1
                if not team.team_abbreviation:
                    missing_fields['team_abbreviation'] += 1
            
            # Check for duplicates
            team_ids = [t.team_id for t in teams if t.team_id]
            duplicates = len(team_ids) - len(set(team_ids))
            
            result = {
                'total': len(teams),
                'missing_fields': dict(missing_fields),
                'duplicates': duplicates,
                'issues': issues
            }
        
        if issues:
            logger.warning(f"  Found {len(issues)} team issues")
        if duplicates > 0:
            logger.warning(f"  Found {duplicates} duplicate team IDs")
        else:
            logger.info("  ✓ Teams look good")
        
        return result
    
    def check_games(self) -> Dict[str, Any]:
        """Check game data quality."""
        logger.info("\n[3/9] Checking games...")
        
        with self.db_manager.get_session() as session:
            games = session.query(Game).all()
            
            issues = []
            missing_fields = defaultdict(int)
            invalid_scores = []
            season_counts = defaultdict(int)
            
            for game in games:
                # Check required fields
                if not game.game_id:
                    issues.append(f"Game missing ID")
                if not game.season:
                    missing_fields['season'] += 1
                if not game.game_date:
                    missing_fields['game_date'] += 1
                if not game.home_team_id:
                    missing_fields['home_team_id'] += 1
                if not game.away_team_id:
                    missing_fields['away_team_id'] += 1
                
                # Check score validity
                if game.home_score is not None and game.away_score is not None:
                    if game.home_score < 0 or game.away_score < 0:
                        invalid_scores.append(f"Game {game.game_id}: negative scores")
                    if game.home_score > 200 or game.away_score > 200:
                        invalid_scores.append(f"Game {game.game_id}: suspiciously high scores")
                
                # Count by season
                if game.season:
                    season_counts[game.season] += 1
            
            # Check winner consistency
            winner_issues = []
            for game in games:
                if game.home_score is not None and game.away_score is not None and game.winner:
                    expected_winner = game.home_team_id if game.home_score > game.away_score else game.away_team_id
                    if game.winner != expected_winner:
                        winner_issues.append(f"Game {game.game_id}: winner mismatch")
            
            result = {
                'total': len(games),
                'missing_fields': dict(missing_fields),
                'invalid_scores': len(invalid_scores),
                'winner_issues': len(winner_issues),
                'season_counts': dict(season_counts),
                'issues': issues[:10]  # Limit to first 10
            }
        
        logger.info(f"  Total games: {result['total']}")
        logger.info(f"  By season: {result['season_counts']}")
        if result['invalid_scores'] > 0:
            logger.warning(f"  ⚠ Found {result['invalid_scores']} games with invalid scores")
        if result['winner_issues'] > 0:
            logger.warning(f"  ⚠ Found {result['winner_issues']} games with winner mismatches")
        if not result['issues'] and result['invalid_scores'] == 0 and result['winner_issues'] == 0:
            logger.info("  ✓ Games look good")
        
        return result
    
    def check_team_stats(self) -> Dict[str, Any]:
        """Check team stats data quality."""
        logger.info("\n[4/9] Checking team stats...")
        
        with self.db_manager.get_session() as session:
            stats = session.query(TeamStats).all()
            
            issues = []
            missing_fields = defaultdict(int)
            invalid_values = []
            
            for stat in stats:
                if not stat.game_id:
                    issues.append("Team stat missing game_id")
                if not stat.team_id:
                    issues.append("Team stat missing team_id")
                
                # Check for required numeric fields
                if stat.points is None:
                    missing_fields['points'] += 1
                if stat.field_goals_made is None:
                    missing_fields['field_goals_made'] += 1
                
                # Check for invalid percentages
                if stat.field_goal_percentage is not None:
                    if stat.field_goal_percentage < 0 or stat.field_goal_percentage > 100:
                        invalid_values.append(f"Game {stat.game_id}: invalid FG% ({stat.field_goal_percentage})")
                
                # Check consistency: FG% should match FGM/FGA
                if (stat.field_goals_made is not None and stat.field_goals_attempted is not None 
                    and stat.field_goals_attempted > 0):
                    expected_pct = (stat.field_goals_made / stat.field_goals_attempted) * 100
                    if stat.field_goal_percentage is not None:
                        diff = abs(stat.field_goal_percentage - expected_pct)
                        if diff > 0.5:  # Allow small rounding differences
                            invalid_values.append(f"Game {stat.game_id}: FG% mismatch (calc: {expected_pct:.1f}, stored: {stat.field_goal_percentage})")
            
            # Check games with missing team stats
            games_with_stats = session.query(TeamStats.game_id).distinct().count()
            total_games = session.query(Game).filter(Game.game_status == 'finished').count()
            missing_stats = total_games * 2 - len(stats)  # Each game should have 2 team stats
            
            result = {
                'total': len(stats),
                'games_with_stats': games_with_stats,
                'expected_games': total_games,
                'missing_stats': max(0, missing_stats),
                'missing_fields': dict(missing_fields),
                'invalid_values': len(invalid_values),
                'issues': issues[:10]
            }
        
        logger.info(f"  Total team stats: {result['total']}")
        logger.info(f"  Games with stats: {result['games_with_stats']} / {result['expected_games']}")
        if result['missing_stats'] > 0:
            logger.warning(f"  ⚠ Missing team stats for ~{result['missing_stats']} team-game combinations")
        if result['invalid_values'] > 0:
            logger.warning(f"  ⚠ Found {result['invalid_values']} invalid values")
        if result['invalid_values'] == 0 and result['missing_stats'] == 0:
            logger.info("  ✓ Team stats look good")
        
        return result
    
    def check_player_stats(self) -> Dict[str, Any]:
        """Check player stats data quality."""
        logger.info("\n[5/9] Checking player stats...")
        
        with self.db_manager.get_session() as session:
            stats = session.query(PlayerStats).all()
            
            issues = []
            missing_fields = defaultdict(int)
            games_with_stats = session.query(PlayerStats.game_id).distinct().count()
            total_games = session.query(Game).filter(Game.game_status == 'finished').count()
            
            # Sample check for data quality
            sample_issues = 0
            for stat in stats[:100]:  # Sample first 100
                if not stat.game_id:
                    issues.append("Player stat missing game_id")
                if not stat.player_id:
                    issues.append("Player stat missing player_id")
                if stat.points is None:
                    missing_fields['points'] += 1
                if stat.minutes_played and ':' not in str(stat.minutes_played):
                    sample_issues += 1
            
            result = {
                'total': len(stats),
                'games_with_stats': games_with_stats,
                'expected_games': total_games,
                'missing_fields': dict(missing_fields),
                'issues': issues[:10]
            }
        
        logger.info(f"  Total player stats: {result['total']}")
        logger.info(f"  Games with stats: {result['games_with_stats']} / {result['expected_games']}")
        avg_players_per_game = result['total'] / max(result['games_with_stats'], 1)
        logger.info(f"  Avg players per game: {avg_players_per_game:.1f}")
        if result['total'] > 0:
            logger.info("  ✓ Player stats present")
        
        return result
    
    def check_data_completeness(self) -> Dict[str, Any]:
        """Check data completeness across tables."""
        logger.info("\n[6/9] Checking data completeness...")
        
        with self.db_manager.get_session() as session:
            # Games without team stats
            games_without_team_stats = session.query(Game).filter(
                Game.game_status == 'finished',
                ~Game.game_id.in_(
                    session.query(TeamStats.game_id).distinct()
                )
            ).count()
            
            # Games without player stats
            games_without_player_stats = session.query(Game).filter(
                Game.game_status == 'finished',
                ~Game.game_id.in_(
                    session.query(PlayerStats.game_id).distinct()
                )
            ).count()
            
            # Games with incomplete team stats (only 1 team)
            games_with_one_team_stat = session.query(TeamStats.game_id).group_by(
                TeamStats.game_id
            ).having(func.count(TeamStats.team_id) == 1).count()
            
            result = {
                'games_without_team_stats': games_without_team_stats,
                'games_without_player_stats': games_without_player_stats,
                'games_with_one_team_stat': games_with_one_team_stat
            }
        
        logger.info(f"  Games without team stats: {result['games_without_team_stats']}")
        logger.info(f"  Games without player stats: {result['games_without_player_stats']}")
        logger.info(f"  Games with only 1 team stat: {result['games_with_one_team_stat']}")
        
        if result['games_without_team_stats'] == 0 and result['games_without_player_stats'] == 0:
            logger.info("  ✓ Data completeness looks good")
        else:
            logger.warning("  ⚠ Some games missing stats")
        
        return result
    
    def check_data_consistency(self) -> Dict[str, Any]:
        """Check data consistency across related tables."""
        logger.info("\n[7/9] Checking data consistency...")
        
        with self.db_manager.get_session() as session:
            # Team stats for games that don't exist
            orphaned_team_stats = session.query(TeamStats).filter(
                ~TeamStats.game_id.in_(session.query(Game.game_id))
            ).count()
            
            # Player stats for games that don't exist
            orphaned_player_stats = session.query(PlayerStats).filter(
                ~PlayerStats.game_id.in_(session.query(Game.game_id))
            ).count()
            
            # Team stats with invalid team IDs
            valid_team_ids = [t.team_id for t in session.query(Team.team_id).all()]
            invalid_team_ids = session.query(TeamStats).filter(
                ~TeamStats.team_id.in_(valid_team_ids)
            ).count()
            
            result = {
                'orphaned_team_stats': orphaned_team_stats,
                'orphaned_player_stats': orphaned_player_stats,
                'invalid_team_ids': invalid_team_ids
            }
        
        logger.info(f"  Orphaned team stats: {result['orphaned_team_stats']}")
        logger.info(f"  Orphaned player stats: {result['orphaned_player_stats']}")
        logger.info(f"  Invalid team IDs: {result['invalid_team_ids']}")
        
        if all(v == 0 for v in result.values()):
            logger.info("  ✓ Data consistency looks good")
        else:
            logger.warning("  ⚠ Found consistency issues")
        
        return result
    
    def check_date_ranges(self) -> Dict[str, Any]:
        """Check date ranges and season coverage."""
        logger.info("\n[8/9] Checking date ranges...")
        
        with self.db_manager.get_session() as session:
            # Get date range
            min_date = session.query(func.min(Game.game_date)).scalar()
            max_date = session.query(func.max(Game.game_date)).scalar()
            
            # Count by season
            season_counts = session.query(
                Game.season,
                func.count(Game.game_id).label('count')
            ).group_by(Game.season).all()
            
            # Expected games per season (regular season ~1230 games)
            expected_games = {
                '2022-23': 1230,
                '2023-24': 1230,
                '2024-25': 1230
            }
            
            result = {
                'min_date': str(min_date) if min_date else None,
                'max_date': str(max_date) if max_date else None,
                'season_counts': {s: c for s, c in season_counts},
                'expected_games': expected_games
            }
        
        logger.info(f"  Date range: {result['min_date']} to {result['max_date']}")
        logger.info(f"  Season counts: {result['season_counts']}")
        
        # Check if we have expected coverage
        for season, expected in expected_games.items():
            actual = result['season_counts'].get(season, 0)
            if actual > 0:
                coverage = (actual / expected) * 100
                logger.info(f"  {season}: {actual} games ({coverage:.1f}% of expected)")
        
        return result
    
    def check_duplicates(self) -> Dict[str, Any]:
        """Check for duplicate records."""
        logger.info("\n[9/9] Checking for duplicates...")
        
        with self.db_manager.get_session() as session:
            # Duplicate games
            duplicate_games = session.query(
                Game.game_id,
                func.count(Game.game_id).label('count')
            ).group_by(Game.game_id).having(func.count(Game.game_id) > 1).count()
            
            # Duplicate team stats (same game_id + team_id)
            duplicate_team_stats = session.query(
                TeamStats.game_id,
                TeamStats.team_id,
                func.count().label('count')
            ).group_by(TeamStats.game_id, TeamStats.team_id).having(func.count() > 1).count()
            
            result = {
                'duplicate_games': duplicate_games,
                'duplicate_team_stats': duplicate_team_stats
            }
        
        logger.info(f"  Duplicate games: {result['duplicate_games']}")
        logger.info(f"  Duplicate team stats: {result['duplicate_team_stats']}")
        
        if all(v == 0 for v in result.values()):
            logger.info("  ✓ No duplicates found")
        else:
            logger.warning("  ⚠ Found duplicates")
        
        return result
    
    def sample_records(self) -> Dict[str, Any]:
        """Sample records to verify data quality."""
        logger.info("\nSampling records...")
        
        with self.db_manager.get_session() as session:
            # Sample game
            sample_game = session.query(Game).first()
            game_sample = None
            if sample_game:
                game_sample = {
                    'game_id': sample_game.game_id,
                    'season': sample_game.season,
                    'game_date': str(sample_game.game_date),
                    'home_team_id': sample_game.home_team_id,
                    'away_team_id': sample_game.away_team_id,
                    'home_score': sample_game.home_score,
                    'away_score': sample_game.away_score,
                    'game_status': sample_game.game_status
                }
            
            # Sample team stats
            sample_team_stat = session.query(TeamStats).first()
            team_stat_sample = None
            if sample_team_stat:
                team_stat_sample = {
                    'game_id': sample_team_stat.game_id,
                    'team_id': sample_team_stat.team_id,
                    'points': sample_team_stat.points,
                    'field_goals_made': sample_team_stat.field_goals_made,
                    'field_goal_percentage': sample_team_stat.field_goal_percentage
                }
            
            # Sample player stat
            sample_player_stat = session.query(PlayerStats).first()
            player_stat_sample = None
            if sample_player_stat:
                player_stat_sample = {
                    'game_id': sample_player_stat.game_id,
                    'player_id': sample_player_stat.player_id,
                    'player_name': sample_player_stat.player_name,
                    'points': sample_player_stat.points,
                    'rebounds': sample_player_stat.rebounds,
                    'assists': sample_player_stat.assists
                }
        
        result = {
            'game': game_sample,
            'team_stat': team_stat_sample,
            'player_stat': player_stat_sample
        }
        
        logger.info("  Sample records retrieved")
        return result
    
    def generate_summary(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Generate overall summary."""
        logger.info("\n" + "=" * 70)
        logger.info("QUALITY CHECK SUMMARY")
        logger.info("=" * 70)
        
        counts = results['basic_counts']
        games = results['games']
        team_stats = results['team_stats']
        player_stats = results['player_stats']
        completeness = results['data_completeness']
        consistency = results['data_consistency']
        
        summary = {
            'overall_status': 'GOOD',
            'total_games': counts['games'],
            'total_team_stats': counts['team_stats'],
            'total_player_stats': counts['player_stats'],
            'data_coverage': {},
            'issues_found': 0,
            'warnings': []
        }
        
        # Calculate coverage
        if games['total'] > 0:
            finished_games = games['total']  # Assuming most are finished
            expected_team_stats = finished_games * 2
            coverage_team_stats = (counts['team_stats'] / expected_team_stats * 100) if expected_team_stats > 0 else 0
            summary['data_coverage']['team_stats'] = f"{coverage_team_stats:.1f}%"
            
            if counts['player_stats'] > 0:
                avg_players = counts['player_stats'] / finished_games
                summary['data_coverage']['avg_players_per_game'] = f"{avg_players:.1f}"
        
        # Count issues
        if consistency['orphaned_team_stats'] > 0 or consistency['orphaned_player_stats'] > 0:
            summary['issues_found'] += 1
            summary['warnings'].append("Orphaned records found")
        
        if completeness['games_without_team_stats'] > 0:
            summary['issues_found'] += 1
            summary['warnings'].append(f"{completeness['games_without_team_stats']} games missing team stats")
        
        if games['invalid_scores'] > 0 or games['winner_issues'] > 0:
            summary['issues_found'] += 1
            summary['warnings'].append("Data quality issues in games")
        
        if summary['issues_found'] > 0:
            summary['overall_status'] = 'NEEDS_ATTENTION'
        
        # Print summary
        logger.info(f"\nOverall Status: {summary['overall_status']}")
        logger.info(f"Total Games: {summary['total_games']}")
        logger.info(f"Total Team Stats: {summary['total_team_stats']}")
        logger.info(f"Total Player Stats: {summary['total_player_stats']}")
        logger.info(f"Data Coverage: {summary['data_coverage']}")
        
        if summary['warnings']:
            logger.warning(f"\nWarnings ({len(summary['warnings'])}):")
            for warning in summary['warnings']:
                logger.warning(f"  - {warning}")
        else:
            logger.info("\n✓ No major issues found!")
        
        logger.info("=" * 70)
        
        return summary


def main():
    """Run data quality check."""
    db_manager = DatabaseManager()
    
    if not db_manager.test_connection():
        logger.error("Failed to connect to database!")
        return False
    
    checker = DataQualityChecker(db_manager)
    results = checker.check_all()
    
    # Save results to file
    import json
    output_file = Path("data/quality_check_report.json")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Convert non-serializable objects to strings
    def convert_to_serializable(obj):
        if isinstance(obj, (date, datetime)):
            return str(obj)
        elif isinstance(obj, defaultdict):
            return dict(obj)
        return obj
    
    serializable_results = json.loads(
        json.dumps(results, default=convert_to_serializable, indent=2)
    )
    
    with open(output_file, 'w') as f:
        json.dump(serializable_results, f, indent=2)
    
    logger.info(f"\nFull report saved to: {output_file}")
    
    return results['summary']['overall_status'] == 'GOOD'


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
