"""Test the betting workflow with historical data."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from datetime import date
from src.backtesting.betting_manager import BettingManager

def test_betting_workflow():
    """Test placing bets and resolving them."""
    betting_manager = BettingManager()
    jan2 = date(2026, 1, 2)
    
    print("=" * 70)
    print("TESTING BETTING WORKFLOW")
    print("=" * 70)
    
    # Step 1: Place bets for Jan 2
    print("\n[TEST 1] Placing bets for Jan 2...")
    result = betting_manager.place_bets_for_date(
        target_date=jan2,
        strategy_names=['kelly', 'ev', 'confidence'],
        model_name='nba_v2_classifier',
        include_finished=True  # Allow betting on finished games for testing
    )
    
    print(f"Status: {result['status']}")
    for strat, data in result.get('strategies', {}).items():
        print(f"  {strat}: {data['bets_placed']} bets, ${data['total_wagered']:.2f} wagered")
        if data['bets']:
            for bet in data['bets'][:3]:  # Show first 3
                print(f"    - {bet['team']}: ${bet['amount']:.2f} @ {bet['odds']:.3f}")
    
    # Step 2: Resolve bets for Jan 2
    print("\n[TEST 2] Resolving bets for Jan 2...")
    resolve_result = betting_manager.resolve_bets_for_date(jan2)
    
    print(f"Status: {resolve_result['status']}")
    for strat, data in resolve_result.get('strategies', {}).items():
        pnl_str = f"+${data['total_profit']:.2f}" if data['total_profit'] >= 0 else f"-${abs(data['total_profit']):.2f}"
        print(f"  {strat}: {data['resolved']} resolved ({data['wins']}W/{data['losses']}L), PNL: {pnl_str}")
    
    # Step 3: Check bankrolls
    print("\n[TEST 3] Current bankrolls...")
    for strat in ['kelly', 'ev', 'confidence']:
        bankroll = betting_manager.get_bankroll(strat)
        print(f"  {strat}: ${bankroll:,.2f}")
    
    # Step 4: Daily PNL
    print("\n[TEST 4] Daily PNL for Jan 2...")
    daily_pnl = betting_manager.get_daily_pnl(jan2)
    for strat, data in daily_pnl.items():
        if data['bets'] > 0:
            pnl_str = f"+${data['pnl']:.2f}" if data['pnl'] >= 0 else f"-${abs(data['pnl']):.2f}"
            print(f"  {strat}: {data['bets']} bets, {data['wins']}W/{data['losses']}L, PNL: {pnl_str}")
    
    # Step 5: Print full summary
    print("\n[TEST 5] Full daily summary...")
    betting_manager.print_daily_summary(jan2)
    
    print("\n" + "=" * 70)
    print("TEST COMPLETE")
    print("=" * 70)


if __name__ == '__main__':
    test_betting_workflow()

