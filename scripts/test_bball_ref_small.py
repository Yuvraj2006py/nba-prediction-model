"""Test Basketball Reference collector on a small sample of games."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from src.data_collectors.basketball_reference_collector import BasketballReferenceCollector
from src.database.db_manager import DatabaseManager
from src.database.models import Game, TeamStats, PlayerStats
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_small_sample():
    """Test Basketball Reference collector on a small sample of games."""
    logger.info("=" * 70)
    logger.info("Testing Basketball Reference Collector - Small Sample")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    collector = BasketballReferenceCollector(db_manager)
    
    if not db_manager.test_connection():
        logger.error("Database connection failed! Exiting.")
        return False
    
    logger.info("✓ Database connection successful")
    
    # Get a small sample of finished games from the database
    with db_manager.get_session() as session:
        # Get games that are finished and have a game_date
        games = session.query(Game).filter(
            Game.game_status == 'finished'
        ).order_by(Game.game_date.desc()).limit(10).all()
    
    if not games:
        logger.error("No finished games found in database. Please collect some games first.")
        return False
    
    logger.info(f"Found {len(games)} games to test")
    
    # Check which games already have stats
    games_needing_stats = []
    for game in games:
        with db_manager.get_session() as session:
            team_stats_count = session.query(TeamStats).filter_by(game_id=game.game_id).count()
            player_stats_count = session.query(PlayerStats).filter_by(game_id=game.game_id).count()
        
        if team_stats_count == 0 or player_stats_count == 0:
            games_needing_stats.append(game)
            logger.info(f"  Game {game.game_id} ({game.game_date}): needs stats")
        else:
            logger.info(f"  Game {game.game_id} ({game.game_date}): already has stats (skipping)")
    
    if not games_needing_stats:
        logger.info("All test games already have stats. Clearing stats for first game to test...")
        # Clear stats for first game to test
        test_game = games[0]
        with db_manager.get_session() as session:
            session.query(TeamStats).filter_by(game_id=test_game.game_id).delete()
            session.query(PlayerStats).filter_by(game_id=test_game.game_id).delete()
            session.commit()
        games_needing_stats = [test_game]
        logger.info(f"Cleared stats for game {test_game.game_id} to test")
    
    logger.info(f"\nTesting on {len(games_needing_stats)} games...")
    
    success_count = 0
    team_stats_collected = 0
    player_stats_collected = 0
    
    for i, game in enumerate(tqdm(games_needing_stats, desc="Testing games")):
        logger.info(f"\n[{i+1}/{len(games_needing_stats)}] Testing game {game.game_id} ({game.game_date})")
        
        try:
            # Collect stats
            result = collector.collect_game_stats(game.game_id)
            
            if result['team_stats'] or result['player_stats']:
                # Store team stats
                for team_stat in result['team_stats']:
                    try:
                        db_manager.insert_team_stats(team_stat)
                        team_stats_collected += 1
                    except Exception as e:
                        logger.warning(f"  Could not store team stat: {e}")
                
                # Store player stats
                for player_stat in result['player_stats']:
                    try:
                        db_manager.insert_player_stats(player_stat)
                        player_stats_collected += 1
                    except Exception as e:
                        logger.warning(f"  Could not store player stat: {e}")
                
                logger.info(f"  ✓ Collected {len(result['team_stats'])} team stats, {len(result['player_stats'])} player stats")
                success_count += 1
            else:
                logger.warning(f"  ✗ No stats collected for game {game.game_id}")
        
        except Exception as e:
            logger.error(f"  ✗ Error collecting stats for game {game.game_id}: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("Test Summary")
    logger.info("=" * 70)
    logger.info(f"Games tested: {len(games_needing_stats)}")
    logger.info(f"Successful collections: {success_count}")
    logger.info(f"Team stats collected: {team_stats_collected}")
    logger.info(f"Player stats collected: {player_stats_collected}")
    
    if success_count > 0:
        logger.info("\n✓ Test PASSED - Basketball Reference collector is working!")
        
        # Verify data quality
        logger.info("\nVerifying data quality...")
        with db_manager.get_session() as session:
            for game in games_needing_stats[:success_count]:  # Check first successful game
                team_stats = session.query(TeamStats).filter_by(game_id=game.game_id).all()
                player_stats = session.query(PlayerStats).filter_by(game_id=game.game_id).all()
                
                if team_stats:
                    ts = team_stats[0]
                    logger.info(f"\nSample team stat for game {game.game_id}:")
                    logger.info(f"  Team: {ts.team_id}, Points: {ts.points}, FG: {ts.field_goals_made}/{ts.field_goals_attempted}")
                
                if player_stats:
                    ps = player_stats[0]
                    logger.info(f"\nSample player stat for game {game.game_id}:")
                    logger.info(f"  Player: {ps.player_name}, Points: {ps.points}, Rebounds: {ps.rebounds}, Assists: {ps.assists}")
        
        return True
    else:
        logger.error("\n✗ Test FAILED - No stats were collected")
        return False


if __name__ == "__main__":
    success = test_small_sample()
    sys.exit(0 if success else 1)
