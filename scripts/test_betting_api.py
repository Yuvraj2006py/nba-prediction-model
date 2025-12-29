"""Test script to verify betting API key works."""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import logging
from datetime import date
from src.data_collectors.betting_odds_collector import BettingOddsCollector

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_betting_api():
    """Test betting API by fetching today's NBA odds."""
    logger.info("=" * 60)
    logger.info("Testing Betting Odds API")
    logger.info("=" * 60)
    
    collector = BettingOddsCollector()
    
    # Test 1: Get available sports
    logger.info("\nTest 1: Fetching available sports...")
    sports = collector.get_sports()
    if sports:
        logger.info(f"✓ Successfully fetched {len(sports)} sports")
        nba_sport = next((s for s in sports if 'nba' in s.get('key', '').lower()), None)
        if nba_sport:
            logger.info(f"✓ Found NBA sport: {nba_sport.get('key')} - {nba_sport.get('title')}")
        else:
            logger.warning("⚠ NBA sport not found in sports list")
    else:
        logger.error("✗ Failed to fetch sports")
        return False
    
    # Test 2: Get today's NBA odds
    logger.info("\nTest 2: Fetching today's NBA odds...")
    today = date.today()
    odds = collector.get_odds_for_date(today, sport='basketball_nba')
    
    if odds:
        logger.info(f"✓ Successfully fetched odds for {len(odds)} games")
        
        # Display sample odds
        if len(odds) > 0:
            sample = odds[0]
            logger.info(f"\nSample game odds:")
            logger.info(f"  Game ID: {sample.get('id', 'N/A')}")
            logger.info(f"  Home Team: {sample.get('home_team', 'N/A')}")
            logger.info(f"  Away Team: {sample.get('away_team', 'N/A')}")
            logger.info(f"  Commence Time: {sample.get('commence_time', 'N/A')}")
            logger.info(f"  Bookmakers: {len(sample.get('bookmakers', []))}")
            
            # Show first bookmaker's odds
            bookmakers = sample.get('bookmakers', [])
            if bookmakers:
                first_book = bookmakers[0]
                logger.info(f"\n  First Bookmaker: {first_book.get('key', 'N/A')}")
                markets = first_book.get('markets', [])
                for market in markets:
                    logger.info(f"    Market: {market.get('key', 'N/A')}")
                    outcomes = market.get('outcomes', [])
                    for outcome in outcomes:
                        logger.info(f"      {outcome.get('name', 'N/A')}: {outcome.get('price', 'N/A')}")
        else:
            logger.info("  No games scheduled for today")
    else:
        logger.warning("⚠ No odds found for today (may be no games scheduled)")
        # Try getting general NBA odds
        logger.info("\nTrying to fetch general NBA odds...")
        general_odds = collector.get_nba_odds()
        if general_odds:
            logger.info(f"✓ Found {len(general_odds)} games with odds")
        else:
            logger.error("✗ Failed to fetch NBA odds")
            return False
    
    logger.info("\n" + "=" * 60)
    logger.info("✓ Betting API test completed successfully!")
    logger.info("=" * 60)
    return True


if __name__ == "__main__":
    success = test_betting_api()
    sys.exit(0 if success else 1)

