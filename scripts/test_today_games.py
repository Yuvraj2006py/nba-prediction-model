"""Test predictions and betting on today's games."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
import logging
from datetime import date
from src.database.db_manager import DatabaseManager
from src.prediction.prediction_service import PredictionService
from src.backtesting.forward_tester import ForwardTester
from src.backtesting.strategies import (
    ConfidenceThresholdStrategy,
    ExpectedValueStrategy,
    KellyCriterionStrategy
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def print_betting_decisions(setup_result: dict, db_manager: DatabaseManager):
    """Print betting decisions in a clear format."""
    print("\n" + "=" * 70)
    print("BETTING DECISIONS")
    print("=" * 70)
    
    if setup_result['status'] == 'no_games':
        print("\n[INFO] No games found for today")
        return
    
    print(f"\nGames Found: {setup_result['games_found']}")
    print(f"Bets Placed: {setup_result['bets_placed']}")
    print(f"Current Bankroll: ${setup_result['bankroll']:,.2f}")
    
    if setup_result['bets_placed'] == 0:
        print("\n[INFO] No bets placed (strategy did not recommend any)")
        return
    
    from src.database.models import Game, Team
    
    print(f"\nBets:")
    with db_manager.get_session() as session:
        for i, bet_info in enumerate(setup_result['bets'], 1):
            bet = bet_info['bet_decision']
            game_id = bet_info['game_id']
            
            # Get game info
            game = session.query(Game).filter_by(game_id=game_id).first()
            if game:
                home_team = session.query(Team).filter_by(team_id=game.home_team_id).first()
                away_team = session.query(Team).filter_by(team_id=game.away_team_id).first()
                home_name = home_team.team_name if home_team else game.home_team_id
                away_name = away_team.team_name if away_team else game.away_team_id
                matchup = f"{away_name} @ {home_name}"
            else:
                matchup = f"Game {game_id}"
            
            # Get team name for bet
            bet_team_id = bet['bet_team']
            bet_team = session.query(Team).filter_by(team_id=bet_team_id).first()
            bet_team_name = bet_team.team_name if bet_team else bet_team_id
            
            print(f"\n  {i}. {matchup}")
            print(f"     Game ID: {game_id}")
            print(f"     Bet Type: {bet['bet_type']}")
            print(f"     Betting On: {bet_team_name}")
            print(f"     Amount: ${bet['bet_amount']:.2f}")
            print(f"     Odds: {bet['odds']:.2f} (decimal)")
            print(f"     Confidence: {bet['confidence']:.1%}")
            print(f"     Expected Value: {bet['expected_value']:.3f}")


def print_resolution_results(resolution_result: dict):
    """Print bet resolution results."""
    print("\n" + "=" * 70)
    print("BET RESOLUTION RESULTS")
    print("=" * 70)
    
    if resolution_result['status'] == 'no_finished_games':
        print("\n[INFO] No finished games found")
        return
    
    print(f"\nBets Resolved: {resolution_result['bets_resolved']}")
    print(f"Wins: {resolution_result['wins']}")
    print(f"Losses: {resolution_result['losses']}")
    print(f"Win Rate: {resolution_result['win_rate']:.1%}")
    print(f"Total Profit/Loss: ${resolution_result['total_profit']:,.2f}")
    
    if resolution_result['total_profit'] > 0:
        print(f"\n[SUCCESS] Profitable day! +${resolution_result['total_profit']:.2f}")
    elif resolution_result['total_profit'] < 0:
        print(f"\n[LOSS] Lost ${abs(resolution_result['total_profit']):.2f}")
    else:
        print(f"\n[BREAK EVEN] No profit or loss")


def main():
    parser = argparse.ArgumentParser(description="Test predictions on today's games")
    parser.add_argument(
        '--date',
        type=str,
        help='Date to test (YYYY-MM-DD, default: today)'
    )
    parser.add_argument(
        '--model',
        type=str,
        default='nba_classifier',
        help='Model name (default: nba_classifier)'
    )
    parser.add_argument(
        '--clf-model',
        type=str,
        help='Classification model name (overrides --model)'
    )
    parser.add_argument(
        '--reg-model',
        type=str,
        help='Regression model name'
    )
    parser.add_argument(
        '--strategy',
        type=str,
        choices=['confidence', 'ev', 'kelly'],
        default='confidence',
        help='Betting strategy (default: confidence)'
    )
    parser.add_argument(
        '--confidence-threshold',
        type=float,
        default=0.60,
        help='Confidence threshold for confidence strategy (default: 0.60)'
    )
    parser.add_argument(
        '--bet-amount',
        type=float,
        default=100.0,
        help='Fixed bet amount for confidence strategy (default: 100.0)'
    )
    parser.add_argument(
        '--resolve',
        action='store_true',
        help='Resolve bets for finished games'
    )
    parser.add_argument(
        '--summary',
        action='store_true',
        help='Show summary of test results'
    )
    
    args = parser.parse_args()
    
    # Parse date
    test_date = date.today()
    if args.date:
        test_date = date.fromisoformat(args.date)
    
    print("=" * 70)
    print("NBA GAME PREDICTION TEST - TODAY'S GAMES")
    print("=" * 70)
    print(f"\nDate: {test_date}")
    print(f"Model: {args.clf_model or args.model}")
    print(f"Strategy: {args.strategy}")
    
    # Initialize
    db_manager = DatabaseManager()
    forward_tester = ForwardTester(db_manager)
    
    # Check if we're resolving or setting up
    if args.resolve or args.summary:
        # Resolve bets
        print(f"\n[STEP] Resolving bets for {test_date}...")
        resolution_result = forward_tester.resolve_today_bets(test_date)
        print_resolution_results(resolution_result)
        
        if args.summary:
            summary = forward_tester.get_test_summary(test_date)
            print("\n" + "=" * 70)
            print("TEST SUMMARY")
            print("=" * 70)
            print(f"\nDate: {summary['date']}")
            print(f"Total Bets: {summary['total_bets']}")
            print(f"Pending: {summary['pending']}")
            print(f"Resolved: {summary['resolved']}")
            print(f"Wins: {summary['wins']}")
            print(f"Losses: {summary['losses']}")
            print(f"Win Rate: {summary['win_rate']:.1%}")
            print(f"Total Profit: ${summary['total_profit']:,.2f}")
            print(f"Current Bankroll: ${summary['current_bankroll']:,.2f}")
        
        return 0
    
    # Setup test
    print(f"\n[STEP 1] Setting up forward test...")
    
    # Choose strategy
    if args.strategy == 'confidence':
        strategy = ConfidenceThresholdStrategy(
            confidence_threshold=args.confidence_threshold,
            bet_amount=args.bet_amount
        )
    elif args.strategy == 'ev':
        strategy = ExpectedValueStrategy()
    elif args.strategy == 'kelly':
        strategy = KellyCriterionStrategy()
    else:
        print(f"[ERROR] Unknown strategy: {args.strategy}")
        return 1
    
    # Run setup
    setup_result = forward_tester.setup_today_test(
        test_date=test_date,
        model_name=args.model,
        strategy=strategy,
        clf_model_name=args.clf_model,
        reg_model_name=args.reg_model
    )
    
    print_betting_decisions(setup_result, db_manager)
    
    print("\n" + "=" * 70)
    print("SETUP COMPLETE")
    print("=" * 70)
    print(f"\nNext steps:")
    print(f"  1. Wait for games to finish")
    print(f"  2. Resolve bets: python scripts/test_today_games.py --resolve --date {test_date}")
    print(f"  3. View summary: python scripts/test_today_games.py --summary --date {test_date}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

