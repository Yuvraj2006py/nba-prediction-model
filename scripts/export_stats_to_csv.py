"""Export team and player stats from database to CSV files."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
import argparse
import csv
from typing import List, Optional
from datetime import date

from src.database.db_manager import DatabaseManager
from src.database.models import TeamStats, PlayerStats, Game, Team
from sqlalchemy.orm import joinedload

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/export_stats.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def export_team_stats_to_csv(
    db_manager: DatabaseManager,
    season: Optional[str] = None,
    output_dir: Optional[Path] = None
) -> Path:
    """
    Export team stats to CSV file.
    
    Args:
        db_manager: Database manager instance
        season: Optional season filter (e.g., '2022-23'). If None, exports all seasons.
        output_dir: Output directory for CSV file. If None, uses data/raw/csv/stats/
        
    Returns:
        Path to created CSV file
    """
    if output_dir is None:
        output_dir = Path(project_root) / "data" / "raw" / "csv" / "stats"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine filename
    if season:
        filename = f"team_stats_{season}.csv"
    else:
        filename = "team_stats_all.csv"
    
    csv_path = output_dir / filename
    
    logger.info(f"Exporting team stats to {csv_path}")
    
    # Query team stats from database with eager loading
    with db_manager.get_session() as session:
        # Join explicitly to avoid ambiguity (Game has both home_team_id and away_team_id)
        # Use joinedload to eager load relationships so we can access them after session closes
        query = session.query(TeamStats).options(
            joinedload(TeamStats.game),
            joinedload(TeamStats.team)
        ).join(Game, TeamStats.game_id == Game.game_id).join(Team, TeamStats.team_id == Team.team_id)
        
        if season:
            query = query.filter(Game.season == season)
        
        query = query.order_by(Game.game_date, Game.game_id, TeamStats.is_home)
        team_stats = query.all()
    
    logger.info(f"Found {len(team_stats)} team stat records")
    
    if not team_stats:
        logger.warning("No team stats found to export")
        # Create empty CSV with headers
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=_get_team_stats_headers())
            writer.writeheader()
        return csv_path
    
    # Write to CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = _get_team_stats_headers()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for stat in team_stats:
            game = stat.game
            team = stat.team
            
            row = {
                'game_id': stat.game_id,
                'game_date': game.game_date.strftime('%Y-%m-%d') if game.game_date else '',
                'season': game.season if game else '',
                'team_id': stat.team_id,
                'team_name': team.team_name if team else '',
                'team_abbreviation': team.team_abbreviation if team else '',
                'is_home': stat.is_home,
                'points': stat.points,
                'field_goals_made': stat.field_goals_made,
                'field_goals_attempted': stat.field_goals_attempted,
                'field_goal_percentage': stat.field_goal_percentage,
                'three_pointers_made': stat.three_pointers_made,
                'three_pointers_attempted': stat.three_pointers_attempted,
                'three_point_percentage': stat.three_point_percentage,
                'free_throws_made': stat.free_throws_made,
                'free_throws_attempted': stat.free_throws_attempted,
                'free_throw_percentage': stat.free_throw_percentage,
                'rebounds_offensive': stat.rebounds_offensive,
                'rebounds_defensive': stat.rebounds_defensive,
                'rebounds_total': stat.rebounds_total,
                'assists': stat.assists,
                'steals': stat.steals,
                'blocks': stat.blocks,
                'turnovers': stat.turnovers,
                'personal_fouls': stat.personal_fouls,
                'offensive_rating': stat.offensive_rating if stat.offensive_rating is not None else '',
                'defensive_rating': stat.defensive_rating if stat.defensive_rating is not None else '',
                'pace': stat.pace if stat.pace is not None else '',
                'true_shooting_percentage': stat.true_shooting_percentage if stat.true_shooting_percentage is not None else '',
                'effective_field_goal_percentage': stat.effective_field_goal_percentage if stat.effective_field_goal_percentage is not None else ''
            }
            writer.writerow(row)
    
    logger.info(f"[OK] Exported {len(team_stats)} team stats to {csv_path.name}")
    return csv_path


def export_player_stats_to_csv(
    db_manager: DatabaseManager,
    season: Optional[str] = None,
    output_dir: Optional[Path] = None
) -> Path:
    """
    Export player stats to CSV file.
    
    Args:
        db_manager: Database manager instance
        season: Optional season filter (e.g., '2022-23'). If None, exports all seasons.
        output_dir: Output directory for CSV file. If None, uses data/raw/csv/stats/
        
    Returns:
        Path to created CSV file
    """
    if output_dir is None:
        output_dir = Path(project_root) / "data" / "raw" / "csv" / "stats"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Determine filename
    if season:
        filename = f"player_stats_{season}.csv"
    else:
        filename = "player_stats_all.csv"
    
    csv_path = output_dir / filename
    
    logger.info(f"Exporting player stats to {csv_path}")
    
    # Query player stats from database with eager loading
    with db_manager.get_session() as session:
        # Join explicitly to avoid ambiguity (Game has both home_team_id and away_team_id)
        # Use joinedload to eager load relationships so we can access them after session closes
        query = session.query(PlayerStats).options(
            joinedload(PlayerStats.game),
            joinedload(PlayerStats.team)
        ).join(Game, PlayerStats.game_id == Game.game_id).join(Team, PlayerStats.team_id == Team.team_id)
        
        if season:
            query = query.filter(Game.season == season)
        
        query = query.order_by(Game.game_date, Game.game_id, PlayerStats.team_id, PlayerStats.player_name)
        player_stats = query.all()
    
    logger.info(f"Found {len(player_stats)} player stat records")
    
    if not player_stats:
        logger.warning("No player stats found to export")
        # Create empty CSV with headers
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=_get_player_stats_headers())
            writer.writeheader()
        return csv_path
    
    # Write to CSV
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        fieldnames = _get_player_stats_headers()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for stat in player_stats:
            game = stat.game
            team = stat.team
            
            row = {
                'game_id': stat.game_id,
                'game_date': game.game_date.strftime('%Y-%m-%d') if game.game_date else '',
                'season': game.season if game else '',
                'player_id': stat.player_id,
                'player_name': stat.player_name,
                'team_id': stat.team_id,
                'team_name': team.team_name if team else '',
                'team_abbreviation': team.team_abbreviation if team else '',
                'minutes_played': stat.minutes_played,
                'points': stat.points,
                'rebounds': stat.rebounds,
                'assists': stat.assists,
                'field_goals_made': stat.field_goals_made,
                'field_goals_attempted': stat.field_goals_attempted,
                'three_pointers_made': stat.three_pointers_made,
                'three_pointers_attempted': stat.three_pointers_attempted,
                'free_throws_made': stat.free_throws_made,
                'free_throws_attempted': stat.free_throws_attempted,
                'plus_minus': stat.plus_minus if stat.plus_minus is not None else '',
                'injury_status': stat.injury_status if stat.injury_status else ''
            }
            writer.writerow(row)
    
    logger.info(f"[OK] Exported {len(player_stats)} player stats to {csv_path.name}")
    return csv_path


def export_all_stats_to_csv(
    db_manager: DatabaseManager,
    seasons: Optional[List[str]] = None,
    output_dir: Optional[Path] = None
) -> dict:
    """
    Export both team and player stats to CSV files.
    
    Args:
        db_manager: Database manager instance
        seasons: Optional list of seasons to export. If None, exports all seasons.
        output_dir: Output directory for CSV files. If None, uses data/raw/csv/stats/
        
    Returns:
        Dictionary with export statistics and file paths
    """
    if output_dir is None:
        output_dir = Path(project_root) / "data" / "raw" / "csv" / "stats"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info("=" * 70)
    logger.info("Exporting Stats to CSV")
    logger.info("=" * 70)
    
    stats = {
        'team_stats_files': [],
        'player_stats_files': [],
        'total_team_stats': 0,
        'total_player_stats': 0
    }
    
    if seasons:
        # Export each season separately
        for season in seasons:
            logger.info(f"\nExporting season {season}...")
            team_csv = export_team_stats_to_csv(db_manager, season, output_dir)
            player_csv = export_player_stats_to_csv(db_manager, season, output_dir)
            
            stats['team_stats_files'].append(team_csv)
            stats['player_stats_files'].append(player_csv)
            
            # Count records
            with open(team_csv, 'r', encoding='utf-8') as f:
                stats['total_team_stats'] += sum(1 for line in f) - 1  # Subtract header
            
            with open(player_csv, 'r', encoding='utf-8') as f:
                stats['total_player_stats'] += sum(1 for line in f) - 1  # Subtract header
    else:
        # Export all seasons combined
        logger.info("\nExporting all seasons...")
        team_csv = export_team_stats_to_csv(db_manager, None, output_dir)
        player_csv = export_player_stats_to_csv(db_manager, None, output_dir)
        
        stats['team_stats_files'].append(team_csv)
        stats['player_stats_files'].append(player_csv)
        
        # Count records
        with open(team_csv, 'r', encoding='utf-8') as f:
            stats['total_team_stats'] = sum(1 for line in f) - 1  # Subtract header
        
        with open(player_csv, 'r', encoding='utf-8') as f:
            stats['total_player_stats'] = sum(1 for line in f) - 1  # Subtract header
    
    logger.info("\n" + "=" * 70)
    logger.info("Export Complete")
    logger.info("=" * 70)
    logger.info(f"Team stats files: {len(stats['team_stats_files'])}")
    logger.info(f"Player stats files: {len(stats['player_stats_files'])}")
    logger.info(f"Total team stats exported: {stats['total_team_stats']}")
    logger.info(f"Total player stats exported: {stats['total_player_stats']}")
    logger.info("=" * 70)
    
    return stats


def _get_team_stats_headers() -> List[str]:
    """Get CSV headers for team stats."""
    return [
        'game_id',
        'game_date',
        'season',
        'team_id',
        'team_name',
        'team_abbreviation',
        'is_home',
        'points',
        'field_goals_made',
        'field_goals_attempted',
        'field_goal_percentage',
        'three_pointers_made',
        'three_pointers_attempted',
        'three_point_percentage',
        'free_throws_made',
        'free_throws_attempted',
        'free_throw_percentage',
        'rebounds_offensive',
        'rebounds_defensive',
        'rebounds_total',
        'assists',
        'steals',
        'blocks',
        'turnovers',
        'personal_fouls',
        'offensive_rating',
        'defensive_rating',
        'pace',
        'true_shooting_percentage',
        'effective_field_goal_percentage'
    ]


def _get_player_stats_headers() -> List[str]:
    """Get CSV headers for player stats."""
    return [
        'game_id',
        'game_date',
        'season',
        'player_id',
        'player_name',
        'team_id',
        'team_name',
        'team_abbreviation',
        'minutes_played',
        'points',
        'rebounds',
        'assists',
        'field_goals_made',
        'field_goals_attempted',
        'three_pointers_made',
        'three_pointers_attempted',
        'free_throws_made',
        'free_throws_attempted',
        'plus_minus',
        'injury_status'
    ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Export team and player stats from database to CSV files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export all stats for all seasons
  python scripts/export_stats_to_csv.py
  
  # Export stats for specific season
  python scripts/export_stats_to_csv.py --season 2022-23
  
  # Export stats for multiple seasons
  python scripts/export_stats_to_csv.py --season 2022-23 --season 2023-24
  
  # Export to custom directory
  python scripts/export_stats_to_csv.py --output-dir data/exports
        """
    )
    parser.add_argument('--season', type=str, action='append',
                       help='Season to export (e.g., 2022-23). Can be specified multiple times. If not specified, exports all seasons.')
    parser.add_argument('--output-dir', type=str,
                       help='Output directory for CSV files (default: data/raw/csv/stats/)')
    parser.add_argument('--team-only', action='store_true',
                       help='Export only team stats')
    parser.add_argument('--player-only', action='store_true',
                       help='Export only player stats')
    
    args = parser.parse_args()
    
    try:
        # Initialize database
        db_manager = DatabaseManager()
        
        if not db_manager.test_connection():
            logger.error("Database connection failed!")
            sys.exit(1)
        
        logger.info("[OK] Database connection successful")
        
        # Set output directory
        output_dir = Path(args.output_dir) if args.output_dir else None
        
        # Export stats
        if args.team_only:
            if args.season:
                for season in args.season:
                    export_team_stats_to_csv(db_manager, season, output_dir)
            else:
                export_team_stats_to_csv(db_manager, None, output_dir)
        elif args.player_only:
            if args.season:
                for season in args.season:
                    export_player_stats_to_csv(db_manager, season, output_dir)
            else:
                export_player_stats_to_csv(db_manager, None, output_dir)
        else:
            # Export both
            seasons = args.season if args.season else None
            export_all_stats_to_csv(db_manager, seasons, output_dir)
        
        sys.exit(0)
    
    except KeyboardInterrupt:
        logger.info("\n\nExport interrupted by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
