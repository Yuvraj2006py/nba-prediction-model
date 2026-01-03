"""
Automated Daily NBA Prediction Workflow.

This script handles the complete daily prediction pipeline:
1. Fetches today's games from the betting API
2. Makes predictions and saves them to the database
3. Updates scores for finished games (yesterday and today)
4. Evaluates yesterday's predictions

Can be run manually or scheduled via Windows Task Scheduler / cron.
Uses dynamic dates - no hardcoding required.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)
warnings.filterwarnings('ignore', category=RuntimeWarning)

import logging
from datetime import date, datetime, timedelta
from argparse import ArgumentParser

# Setup logging
log_dir = project_root / 'logs'
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f'workflow_{date.today().strftime("%Y%m%d")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def fetch_todays_games(quiet: bool = False) -> dict:
    """Step 1: Fetch today's games from the betting API."""
    from src.database.db_manager import DatabaseManager
    from src.backtesting.team_mapper import TeamMapper
    from src.data_collectors.betting_odds_collector import BettingOddsCollector
    from src.database.models import Game
    
    today = date.today()
    stats = {'fetched': 0, 'stored': 0}
    
    if not quiet:
        logger.info(f"[STEP 1] Fetching games for {today}")
    
    try:
        db_manager = DatabaseManager()
        odds_collector = BettingOddsCollector(db_manager)
        
        # Get all NBA odds
        all_odds = odds_collector.get_nba_odds()
        
        if all_odds:
            # Filter for today's games (based on US time)
            today_games = []
            tomorrow_utc = date(today.year, today.month, today.day + 1) if today.day < 28 else today
            
            for game_odds in all_odds:
                commence_time = game_odds.get('commence_time', '')
                if commence_time:
                    try:
                        if 'T' in commence_time:
                            if commence_time.endswith('Z'):
                                game_datetime_utc = datetime.fromisoformat(commence_time.replace('Z', '+00:00'))
                            else:
                                game_datetime_utc = datetime.fromisoformat(commence_time)
                            
                            game_date_utc = game_datetime_utc.date()
                            hour_utc = game_datetime_utc.hour
                            
                            # Games on today's date in UTC, or tomorrow UTC before 6 AM
                            if game_date_utc == today or (game_date_utc == tomorrow_utc and hour_utc < 6):
                                today_games.append(game_odds)
                    except Exception:
                        pass
            
            stats['fetched'] = len(today_games)
            
            if today_games:
                stored = odds_collector.parse_and_store_odds(today_games)
                stats['stored'] = stored
                
                # Ensure games have correct date
                with db_manager.get_session() as session:
                    games_to_fix = session.query(Game).filter(
                        Game.game_date != today,
                        Game.game_date >= today - timedelta(days=1)
                    ).all()
                    
                    for game in games_to_fix:
                        # Only fix future games that should be today
                        if game.game_status == 'scheduled':
                            game.game_date = today
                    session.commit()
        
        if not quiet:
            logger.info(f"  Fetched {stats['fetched']} games, stored {stats['stored']} betting lines")
            
    except Exception as e:
        logger.error(f"  Error fetching games: {e}")
    
    return stats


def make_predictions(quiet: bool = False) -> dict:
    """Step 2: Make predictions for today's games and save to database."""
    from src.database.db_manager import DatabaseManager
    from src.prediction.prediction_service import PredictionService
    from src.database.models import Game
    
    today = date.today()
    today_str = today.strftime('%Y%m%d')
    stats = {'games': 0, 'predicted': 0, 'saved': 0}
    
    if not quiet:
        logger.info(f"[STEP 2] Making predictions for {today}")
    
    try:
        db = DatabaseManager()
        prediction_service = PredictionService(db)
        
        # Get today's games
        with db.get_session() as session:
            games = session.query(Game).filter(
                Game.game_date == today
            ).order_by(Game.game_id).all()
        
        # Filter by game_id prefix if possible
        verified_games = [g for g in games if g.game_id.startswith(today_str)]
        if not verified_games:
            verified_games = games
        
        stats['games'] = len(verified_games)
        
        for game in verified_games:
            try:
                result = prediction_service.predict_game(
                    game_id=game.game_id,
                    model_name='nba_v2_classifier',
                    reg_model_name='nba_v2_regressor'
                )
                
                if result:
                    stats['predicted'] += 1
                    
                    # Save prediction
                    try:
                        prediction_service.save_prediction(result, model_name='nba_v2_classifier')
                        stats['saved'] += 1
                    except Exception as e:
                        logger.debug(f"Could not save prediction for {game.game_id}: {e}")
                        
            except Exception as e:
                logger.debug(f"Error predicting {game.game_id}: {e}")
        
        if not quiet:
            logger.info(f"  Predicted {stats['predicted']}/{stats['games']} games, saved {stats['saved']}")
            
    except Exception as e:
        logger.error(f"  Error making predictions: {e}")
    
    return stats


def update_scores(quiet: bool = False) -> dict:
    """Step 3: Update scores for finished games."""
    from src.database.db_manager import DatabaseManager
    from src.database.models import Game
    from src.data_collectors.nba_api_collector import NBAPICollector
    
    today = date.today()
    yesterday = today - timedelta(days=1)
    stats = {'checked': 0, 'updated': 0}
    
    if not quiet:
        logger.info(f"[STEP 3] Updating scores for {yesterday} and {today}")
    
    try:
        db = DatabaseManager()
        nba_collector = NBAPICollector()
        
        # Check both yesterday and today
        for check_date in [yesterday, today]:
            with db.get_session() as session:
                scheduled_games = session.query(Game).filter(
                    Game.game_date == check_date,
                    Game.game_status != 'finished'
                ).all()
            
            for game in scheduled_games:
                stats['checked'] += 1
                
                try:
                    # Check if game_id is betting API format (starts with date) or NBA format (starts with 00)
                    if game.game_id.startswith('2026') or len(game.game_id) > 10:
                        # Betting API format - need to find NBA game ID
                        nba_game_id = nba_collector.find_nba_game_id(
                            game.home_team_id,
                            game.away_team_id,
                            game.game_date
                        )
                        if not nba_game_id:
                            if not quiet:
                                logger.warning(f"Could not find NBA game ID for {game.game_id}")
                            continue
                        game_details = nba_collector.get_game_details(nba_game_id)
                    else:
                        # NBA format - use directly
                        game_details = nba_collector.get_game_details(game.game_id)
                    
                    if game_details and game_details.get('home_score') is not None:
                        home_score = game_details['home_score']
                        away_score = game_details.get('away_score')
                        
                        if home_score is not None and away_score is not None:
                            with db.get_session() as session:
                                game_to_update = session.query(Game).filter_by(
                                    game_id=game.game_id
                                ).first()
                                
                                if game_to_update:
                                    game_to_update.home_score = home_score
                                    game_to_update.away_score = away_score
                                    game_to_update.point_differential = home_score - away_score
                                    game_to_update.winner = (
                                        game_to_update.home_team_id if home_score > away_score 
                                        else game_to_update.away_team_id
                                    )
                                    game_to_update.game_status = 'finished'
                                    session.commit()
                                    stats['updated'] += 1
                                    
                except Exception as e:
                    logger.debug(f"Error updating {game.game_id}: {e}")
        
        if not quiet:
            logger.info(f"  Checked {stats['checked']} games, updated {stats['updated']}")
            
    except Exception as e:
        logger.error(f"  Error updating scores: {e}")
    
    return stats


def evaluate_predictions(quiet: bool = False) -> dict:
    """Step 4: Evaluate yesterday's predictions."""
    from src.database.db_manager import DatabaseManager
    from src.database.models import Game, Prediction
    import numpy as np
    
    yesterday = date.today() - timedelta(days=1)
    stats = {'games': 0, 'correct': 0, 'accuracy': 0.0, 'correct_games': [], 'incorrect_games': []}
    
    if not quiet:
        logger.info(f"[STEP 4] Evaluating predictions for {yesterday}")
    
    try:
        db = DatabaseManager()
        
        with db.get_session() as session:
            # Get finished games with predictions
            games = session.query(Game).join(Prediction).filter(
                Game.game_date == yesterday,
                Game.home_score.isnot(None),
                Game.away_score.isnot(None),
                Prediction.model_name == 'nba_v2_classifier'
            ).all()
            
            stats['games'] = len(games)
            
            for game in games:
                prediction = session.query(Prediction).filter_by(
                    game_id=game.game_id,
                    model_name='nba_v2_classifier'
                ).first()
                
                if not prediction:
                    continue
                
                # Get team names
                home_team = db.get_team(game.home_team_id)
                away_team = db.get_team(game.away_team_id)
                actual_winner_team = db.get_team(game.winner) if game.winner else None
                predicted_winner_team = db.get_team(prediction.predicted_winner) if prediction.predicted_winner else None
                
                home_name = home_team.team_name if home_team else game.home_team_id
                away_name = away_team.team_name if away_team else game.away_team_id
                actual_winner_name = actual_winner_team.team_name if actual_winner_team else (game.winner or "Unknown")
                predicted_winner_name = predicted_winner_team.team_name if predicted_winner_team else (prediction.predicted_winner or "Unknown")
                
                actual_winner = game.winner
                predicted_winner = prediction.predicted_winner
                
                is_correct = actual_winner == predicted_winner
                
                # Format score as "Away @ Home: AwayScore-HomeScore"
                score_str = f"{away_name} {game.away_score} @ {home_name} {game.home_score}"
                
                game_result = {
                    'game_id': game.game_id,
                    'matchup': f"{away_name} @ {home_name}",
                    'score': score_str,
                    'actual_winner': actual_winner_name,
                    'predicted_winner': predicted_winner_name,
                    'confidence': prediction.confidence,
                    'home_prob': prediction.win_probability_home,
                    'away_prob': prediction.win_probability_away,
                    'predicted_margin': prediction.predicted_point_differential
                }
                
                if is_correct:
                    stats['correct'] += 1
                    stats['correct_games'].append(game_result)
                else:
                    stats['incorrect_games'].append(game_result)
            
            if stats['games'] > 0:
                stats['accuracy'] = stats['correct'] / stats['games']
        
        if not quiet:
            logger.info(f"  Evaluated {stats['games']} games")
            logger.info(f"  Correct: {stats['correct']}/{stats['games']} ({stats['accuracy']:.1%})")
            
            # Show correct predictions
            if stats['correct_games']:
                logger.info(f"\n  [CORRECT] PREDICTIONS ({len(stats['correct_games'])}):")
                for game in stats['correct_games']:
                    logger.info(f"    - {game['score']}")
                    logger.info(f"      Winner: {game['actual_winner']} | Confidence: {game['confidence']:.1%}")
            
            # Show incorrect predictions
            if stats['incorrect_games']:
                logger.info(f"\n  [INCORRECT] PREDICTIONS ({len(stats['incorrect_games'])}):")
                for game in stats['incorrect_games']:
                    logger.info(f"    - {game['score']}")
                    logger.info(f"      Actual: {game['actual_winner']} | Predicted: {game['predicted_winner']} | Confidence: {game['confidence']:.1%}")
            
    except Exception as e:
        logger.error(f"  Error evaluating predictions: {e}")
    
    return stats


def place_bets(quiet: bool = False, strategies: list = None) -> dict:
    """Step 2.5: Place bets using betting strategies."""
    from src.backtesting.betting_manager import BettingManager, STRATEGIES
    
    today = date.today()
    stats = {'strategies': {}, 'total_bets': 0, 'total_wagered': 0.0}
    
    if strategies is None:
        strategies = list(STRATEGIES.keys())
    
    if not quiet:
        logger.info(f"[STEP 2.5] Placing bets for {today} using {', '.join(strategies)}")
    
    try:
        betting_manager = BettingManager()
        
        result = betting_manager.place_bets_for_date(
            target_date=today,
            strategy_names=strategies,
            model_name='nba_v2_classifier'
        )
        
        if result['status'] == 'complete':
            for strategy_name, data in result['strategies'].items():
                stats['strategies'][strategy_name] = {
                    'bets': data['bets_placed'],
                    'wagered': data['total_wagered'],
                    'bankroll': data['bankroll_after']
                }
                stats['total_bets'] += data['bets_placed']
                stats['total_wagered'] += data['total_wagered']
                
                if not quiet and data['bets_placed'] > 0:
                    logger.info(f"\n  {strategy_name.upper()}:")
                    logger.info(f"    Bets placed: {data['bets_placed']}")
                    logger.info(f"    Total wagered: ${data['total_wagered']:.2f}")
                    logger.info(f"    Bankroll: ${data['bankroll_after']:,.2f}")
                    
                    # Show individual bets
                    for bet in data['bets']:
                        logger.info(f"      - {bet['team']}: ${bet['amount']:.2f} @ {bet['odds']:.3f} (conf: {bet['confidence']:.1%})")
        
        if not quiet:
            logger.info(f"\n  TOTAL: {stats['total_bets']} bets, ${stats['total_wagered']:.2f} wagered")
            
    except Exception as e:
        logger.error(f"  Error placing bets: {e}")
        import traceback
        logger.debug(traceback.format_exc())
    
    return stats


def resolve_bets(quiet: bool = False) -> dict:
    """Step 4.5: Resolve bets for finished games."""
    from src.backtesting.betting_manager import BettingManager
    
    yesterday = date.today() - timedelta(days=1)
    stats = {'strategies': {}, 'total_resolved': 0, 'total_profit': 0.0}
    
    if not quiet:
        logger.info(f"[STEP 4.5] Resolving bets for {yesterday}")
    
    try:
        betting_manager = BettingManager()
        
        result = betting_manager.resolve_bets_for_date(yesterday)
        
        if result['status'] == 'complete':
            for strategy_name, data in result['strategies'].items():
                stats['strategies'][strategy_name] = {
                    'resolved': data['resolved'],
                    'wins': data['wins'],
                    'losses': data['losses'],
                    'profit': data['total_profit']
                }
                stats['total_resolved'] += data['resolved']
                stats['total_profit'] += data['total_profit']
                
                if not quiet and data['resolved'] > 0:
                    pnl_str = f"+${data['total_profit']:.2f}" if data['total_profit'] >= 0 else f"-${abs(data['total_profit']):.2f}"
                    logger.info(f"\n  {strategy_name.upper()}:")
                    logger.info(f"    Resolved: {data['resolved']} ({data['wins']}W / {data['losses']}L)")
                    logger.info(f"    Win Rate: {data['win_rate']:.1%}")
                    logger.info(f"    Profit: {pnl_str}")
                    
                    # Show individual bet results
                    for bet in data['bets']:
                        outcome_str = "[WIN]" if bet['outcome'] == 'win' else "[LOSS]"
                        profit_str = f"+${bet['profit']:.2f}" if bet['profit'] >= 0 else f"-${abs(bet['profit']):.2f}"
                        logger.info(f"      {outcome_str} {bet['bet_team']}: {profit_str}")
        
        total_pnl_str = f"+${stats['total_profit']:.2f}" if stats['total_profit'] >= 0 else f"-${abs(stats['total_profit']):.2f}"
        if not quiet:
            logger.info(f"\n  TOTAL: {stats['total_resolved']} bets resolved, {total_pnl_str}")
        
        # Show daily summary
        betting_manager.print_daily_summary(yesterday, quiet)
            
    except Exception as e:
        logger.error(f"  Error resolving bets: {e}")
        import traceback
        logger.debug(traceback.format_exc())
    
    return stats


def show_pnl(period: str = 'daily', quiet: bool = False) -> dict:
    """Show PNL summary for a period."""
    from src.backtesting.betting_manager import BettingManager
    
    stats = {}
    
    if quiet:
        return stats
    
    try:
        betting_manager = BettingManager()
        
        today = date.today()
        
        if period == 'daily':
            yesterday = today - timedelta(days=1)
            betting_manager.print_daily_summary(yesterday)
        elif period == 'weekly':
            # Last 7 days
            start_date = today - timedelta(days=7)
            betting_manager.print_period_summary(start_date, today)
        elif period == 'monthly':
            # Last 30 days
            start_date = today - timedelta(days=30)
            betting_manager.print_period_summary(start_date, today)
        elif period == 'all':
            # All time (from beginning of current season)
            start_date = date(2025, 10, 1)  # Season start
            betting_manager.print_period_summary(start_date, today)
        
    except Exception as e:
        logger.error(f"Error getting PNL: {e}")
    
    return stats


def run_morning_workflow(quiet: bool = False, enable_betting: bool = True, strategies: list = None):
    """Run morning workflow: fetch games, make predictions, and optionally place bets."""
    if not quiet:
        logger.info("=" * 70)
        logger.info(f"MORNING WORKFLOW - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
    
    # Step 1: Fetch games
    fetch_stats = fetch_todays_games(quiet)
    
    # Step 2: Make predictions
    pred_stats = make_predictions(quiet)
    
    # Step 2.5: Place bets (if enabled)
    bet_stats = {}
    if enable_betting:
        bet_stats = place_bets(quiet, strategies)
    
    if not quiet:
        logger.info("=" * 70)
        logger.info("MORNING WORKFLOW COMPLETE")
        logger.info(f"  Games fetched: {fetch_stats['fetched']}")
        logger.info(f"  Predictions saved: {pred_stats['saved']}")
        if enable_betting:
            logger.info(f"  Bets placed: {bet_stats.get('total_bets', 0)}")
            logger.info(f"  Total wagered: ${bet_stats.get('total_wagered', 0):.2f}")
        logger.info("=" * 70)
    
    return {'fetch': fetch_stats, 'predictions': pred_stats, 'bets': bet_stats}


def run_evening_workflow(quiet: bool = False, enable_betting: bool = True):
    """Run evening workflow: update scores, evaluate, and resolve bets."""
    if not quiet:
        logger.info("=" * 70)
        logger.info(f"EVENING WORKFLOW - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
    
    # Step 3: Update scores
    score_stats = update_scores(quiet)
    
    # Step 4: Evaluate predictions
    eval_stats = evaluate_predictions(quiet)
    
    # Step 4.5: Resolve bets (if enabled)
    bet_stats = {}
    if enable_betting:
        bet_stats = resolve_bets(quiet)
    
    if not quiet:
        logger.info("=" * 70)
        logger.info("EVENING WORKFLOW COMPLETE")
        logger.info(f"  Games updated: {score_stats['updated']}")
        logger.info(f"  Accuracy: {eval_stats['correct']}/{eval_stats['games']} ({eval_stats['accuracy']:.1%})")
        if enable_betting and bet_stats:
            pnl = bet_stats.get('total_profit', 0)
            pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
            logger.info(f"  Bets resolved: {bet_stats.get('total_resolved', 0)}")
            logger.info(f"  Daily PNL: {pnl_str}")
        logger.info("=" * 70)
    
    return {'scores': score_stats, 'evaluation': eval_stats, 'bets': bet_stats}


def run_full_workflow(quiet: bool = False, enable_betting: bool = True, strategies: list = None):
    """Run the complete workflow (all steps including betting)."""
    if not quiet:
        logger.info("=" * 70)
        logger.info(f"FULL DAILY WORKFLOW - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
    
    results = {}
    
    # All steps
    results['fetch'] = fetch_todays_games(quiet)
    results['predictions'] = make_predictions(quiet)
    
    # Place bets for today
    if enable_betting:
        results['bets_placed'] = place_bets(quiet, strategies)
    
    results['scores'] = update_scores(quiet)
    results['evaluation'] = evaluate_predictions(quiet)
    
    # Resolve bets from yesterday
    if enable_betting:
        results['bets_resolved'] = resolve_bets(quiet)
    
    if not quiet:
        logger.info("=" * 70)
        logger.info("FULL WORKFLOW COMPLETE")
        logger.info(f"  Games fetched: {results['fetch']['fetched']}")
        logger.info(f"  Predictions saved: {results['predictions']['saved']}")
        if enable_betting:
            logger.info(f"  Bets placed: {results.get('bets_placed', {}).get('total_bets', 0)}")
        logger.info(f"  Scores updated: {results['scores']['updated']}")
        logger.info(f"  Yesterday's accuracy: {results['evaluation']['accuracy']:.1%}")
        if enable_betting:
            pnl = results.get('bets_resolved', {}).get('total_profit', 0)
            pnl_str = f"+${pnl:.2f}" if pnl >= 0 else f"-${abs(pnl):.2f}"
            logger.info(f"  Yesterday's PNL: {pnl_str}")
        logger.info("=" * 70)
    
    return results


def main():
    """Main entry point."""
    parser = ArgumentParser(
        description='Automated NBA Prediction Workflow with Betting',
        epilog="""
Examples:
  python daily_workflow.py                  # Run full workflow with betting
  python daily_workflow.py --morning        # Morning: fetch + predict + place bets
  python daily_workflow.py --evening        # Evening: update scores + evaluate + resolve bets
  python daily_workflow.py --no-betting     # Run without betting
  python daily_workflow.py --strategy kelly # Use only Kelly strategy
  python daily_workflow.py --pnl weekly     # Show weekly PNL summary
        """
    )
    parser.add_argument('--morning', action='store_true',
                       help='Run morning workflow only (fetch + predict + bet)')
    parser.add_argument('--evening', action='store_true',
                       help='Run evening workflow only (update + evaluate + resolve)')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress detailed output (for automation)')
    parser.add_argument('--step', type=int, choices=[1, 2, 3, 4, 5, 6],
                       help='Run specific step (1=fetch, 2=predict, 3=update, 4=evaluate, 5=place bets, 6=resolve bets)')
    parser.add_argument('--no-betting', action='store_true',
                       help='Disable betting (predictions only)')
    parser.add_argument('--strategy', type=str, choices=['kelly', 'ev', 'confidence', 'all'],
                       default='all', help='Betting strategy to use (default: all)')
    parser.add_argument('--pnl', type=str, choices=['daily', 'weekly', 'monthly', 'all'],
                       help='Show PNL summary for period')
    args = parser.parse_args()
    
    # Parse strategies
    strategies = None if args.strategy == 'all' else [args.strategy]
    enable_betting = not args.no_betting
    
    try:
        # Handle PNL summary request
        if args.pnl:
            show_pnl(args.pnl, args.quiet)
            return 0
        
        if args.step:
            # Run specific step
            if args.step == 1:
                fetch_todays_games(args.quiet)
            elif args.step == 2:
                make_predictions(args.quiet)
            elif args.step == 3:
                update_scores(args.quiet)
            elif args.step == 4:
                evaluate_predictions(args.quiet)
            elif args.step == 5:
                place_bets(args.quiet, strategies)
            elif args.step == 6:
                resolve_bets(args.quiet)
        elif args.morning:
            run_morning_workflow(args.quiet, enable_betting, strategies)
        elif args.evening:
            run_evening_workflow(args.quiet, enable_betting)
        else:
            run_full_workflow(args.quiet, enable_betting, strategies)
        
        return 0
        
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        import traceback
        logger.debug(traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main())

