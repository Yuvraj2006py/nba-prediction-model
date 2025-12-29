"""Test collection script on a small subset before full run."""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from src.data_collectors.nba_api_collector import NBAPICollector
from src.database.db_manager import DatabaseManager
from nba_api.stats.static import teams

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_small_collection():
    """Test collection on a small subset (1 team, 1 season)."""
    logger.info("=" * 70)
    logger.info("Testing Collection on Small Subset")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    collector = NBAPICollector(db_manager)
    
    # Test with Lakers 2024-25 season (first 5 games)
    team_id = '1610612747'  # Lakers
    season = '2024-25'
    
    logger.info(f"Testing with team {team_id} in season {season}")
    
    # Get games
    games = collector.get_games_for_team_season(team_id, season)
    logger.info(f"Found {len(games)} games")
    
    # Limit to first 5 for testing
    test_games = games[:5]
    logger.info(f"Testing with {len(test_games)} games")
    
    stats = {
        'games_processed': 0,
        'games_with_details': 0,
        'games_with_team_stats': 0,
        'games_with_player_stats': 0
    }
    
    for game in test_games:
        game_id = game.get('game_id')
        if not game_id:
            continue
        
        logger.info(f"\nProcessing game {game_id}...")
        
        # Get game details
        game_details = collector.get_game_details(game_id)
        if game_details:
            # Store game
            game_data = {
                'game_id': game_details['game_id'],
                'season': game_details.get('season', season),
                'season_type': game_details.get('season_type', 'Regular Season'),
                'game_date': game_details.get('game_date'),
                'home_team_id': game_details['home_team_id'],
                'away_team_id': game_details['away_team_id'],
                'home_score': game_details.get('home_score'),
                'away_score': game_details.get('away_score'),
                'winner': game_details.get('winner'),
                'point_differential': game_details.get('point_differential'),
                'game_status': game_details.get('game_status', 'finished')
            }
            db_manager.insert_game(game_data)
            stats['games_processed'] += 1
            stats['games_with_details'] += 1
            
            # Collect stats if game is finished
            if game_details.get('game_status') == 'finished':
                # Team stats
                team_stats = collector.collect_team_stats(game_id)
                if team_stats:
                    for ts in team_stats:
                        db_manager.insert_team_stats(ts)
                    stats['games_with_team_stats'] += 1
                    logger.info(f"  ✓ Collected {len(team_stats)} team stats")
                
                # Player stats
                player_stats = collector.collect_player_stats(game_id)
                if player_stats:
                    for ps in player_stats:
                        db_manager.insert_player_stats(ps)
                    stats['games_with_player_stats'] += 1
                    logger.info(f"  ✓ Collected {len(player_stats)} player stats")
    
    logger.info("\n" + "=" * 70)
    logger.info("Test Results:")
    logger.info(f"  Games processed: {stats['games_processed']}")
    logger.info(f"  Games with details: {stats['games_with_details']}")
    logger.info(f"  Games with team stats: {stats['games_with_team_stats']}")
    logger.info(f"  Games with player stats: {stats['games_with_player_stats']}")
    logger.info("=" * 70)
    
    # Verify in database
    with db_manager.get_session() as session:
        from src.database.models import Game, TeamStats, PlayerStats
        games_count = session.query(Game).count()
        team_stats_count = session.query(TeamStats).count()
        player_stats_count = session.query(PlayerStats).count()
        
        logger.info(f"\nDatabase Verification:")
        logger.info(f"  Total games: {games_count}")
        logger.info(f"  Total team stats: {team_stats_count}")
        logger.info(f"  Total player stats: {player_stats_count}")


if __name__ == "__main__":
    test_small_collection()

