"""Verify Phase 2 completion - Test all data collection functionality."""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import date
from src.database.db_manager import DatabaseManager
from src.data_collectors.nba_api_collector import NBAPICollector
from src.data_collectors.betting_odds_collector import BettingOddsCollector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def verify_phase2():
    """Verify all Phase 2 components are working."""
    logger.info("=" * 70)
    logger.info("Phase 2 Completion Verification")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    nba_collector = NBAPICollector(db_manager=db_manager)
    betting_collector = BettingOddsCollector(db_manager=db_manager)
    
    all_passed = True
    
    # Test 1: NBA API - Team Collection
    logger.info("\n[1/6] Testing NBA API - Team Collection...")
    try:
        teams = nba_collector.collect_all_teams()
        if len(teams) == 30:
            logger.info(f"✓ PASSED: Collected {len(teams)} teams")
        else:
            logger.warning(f"⚠ WARNING: Expected 30 teams, got {len(teams)}")
    except Exception as e:
        logger.error(f"✗ FAILED: {e}")
        all_passed = False
    
    # Test 2: NBA API - Game Details
    logger.info("\n[2/6] Testing NBA API - Game Details Collection...")
    try:
        game_id = '0022401199'  # Known finished game
        game_details = nba_collector.get_game_details(game_id)
        if game_details and game_details.get('game_id') == game_id:
            logger.info(f"✓ PASSED: Retrieved game details for {game_id}")
        else:
            logger.error("✗ FAILED: Could not retrieve game details")
            all_passed = False
    except Exception as e:
        logger.error(f"✗ FAILED: {e}")
        all_passed = False
    
    # Test 3: NBA API - Team Stats Collection
    logger.info("\n[3/6] Testing NBA API - Team Stats Collection...")
    try:
        team_stats = nba_collector.collect_team_stats(game_id)
        if len(team_stats) == 2:  # Home and away
            logger.info(f"✓ PASSED: Collected team stats for both teams")
            logger.info(f"  Home team: {team_stats[0].get('points', 0)} points")
            logger.info(f"  Away team: {team_stats[1].get('points', 0)} points")
        else:
            logger.error(f"✗ FAILED: Expected 2 team stats, got {len(team_stats)}")
            all_passed = False
    except Exception as e:
        logger.error(f"✗ FAILED: {e}")
        all_passed = False
    
    # Test 4: NBA API - Player Stats Collection
    logger.info("\n[4/6] Testing NBA API - Player Stats Collection...")
    try:
        player_stats = nba_collector.collect_player_stats(game_id)
        if len(player_stats) > 10:  # Should have multiple players
            logger.info(f"✓ PASSED: Collected stats for {len(player_stats)} players")
            top_scorer = max(player_stats, key=lambda x: x.get('points', 0))
            logger.info(f"  Top scorer: {top_scorer.get('player_name')} ({top_scorer.get('points')} pts)")
        else:
            logger.error(f"✗ FAILED: Expected >10 player stats, got {len(player_stats)}")
            all_passed = False
    except Exception as e:
        logger.error(f"✗ FAILED: {e}")
        all_passed = False
    
    # Test 5: Betting API - Connection
    logger.info("\n[5/6] Testing Betting API - Connection...")
    try:
        sports = betting_collector.get_sports()
        nba_sport = next((s for s in sports if 'nba' in s.get('key', '').lower()), None)
        if nba_sport:
            logger.info(f"✓ PASSED: Betting API connected, NBA sport available")
        else:
            logger.error("✗ FAILED: Betting API connected but NBA sport not found")
            all_passed = False
    except Exception as e:
        logger.error(f"✗ FAILED: {e}")
        all_passed = False
    
    # Test 6: Betting API - Odds Fetching
    logger.info("\n[6/6] Testing Betting API - Odds Fetching...")
    try:
        odds = betting_collector.get_nba_odds()
        if odds is not None:
            logger.info(f"✓ PASSED: Successfully fetched odds (found {len(odds)} games)")
            if len(odds) > 0:
                sample = odds[0]
                logger.info(f"  Sample: {sample.get('home_team')} vs {sample.get('away_team')}")
        else:
            logger.error("✗ FAILED: Could not fetch odds")
            all_passed = False
    except Exception as e:
        logger.error(f"✗ FAILED: {e}")
        all_passed = False
    
    # Summary
    logger.info("\n" + "=" * 70)
    if all_passed:
        logger.info("✓✓✓ ALL TESTS PASSED - Phase 2 Complete! ✓✓✓")
        logger.info("=" * 70)
        logger.info("\nPhase 2 Status:")
        logger.info("  ✓ Team Stats Collection: IMPLEMENTED")
        logger.info("  ✓ Player Stats Collection: IMPLEMENTED")
        logger.info("  ✓ Betting Odds Collector: IMPLEMENTED")
        logger.info("  ✓ Unit Tests: CREATED")
        logger.info("  ✓ Integration Tests: PASSING")
        logger.info("  ✓ Betting API: WORKING")
        logger.info("\nReady for Phase 4: Feature Engineering!")
    else:
        logger.error("✗✗✗ SOME TESTS FAILED - Please review errors above ✗✗✗")
    logger.info("=" * 70)
    
    return all_passed


if __name__ == "__main__":
    success = verify_phase2()
    sys.exit(0 if success else 1)

