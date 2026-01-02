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
    stats = {'games': 0, 'correct': 0, 'accuracy': 0.0}
    
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
                
                if prediction:
                    actual_winner = game.winner
                    predicted_winner = prediction.predicted_winner
                    
                    if actual_winner == predicted_winner:
                        stats['correct'] += 1
            
            if stats['games'] > 0:
                stats['accuracy'] = stats['correct'] / stats['games']
        
        if not quiet:
            logger.info(f"  Evaluated {stats['games']} games")
            logger.info(f"  Correct: {stats['correct']}/{stats['games']} ({stats['accuracy']:.1%})")
            
    except Exception as e:
        logger.error(f"  Error evaluating predictions: {e}")
    
    return stats


def run_morning_workflow(quiet: bool = False):
    """Run morning workflow: fetch games and make predictions."""
    if not quiet:
        logger.info("=" * 70)
        logger.info(f"MORNING WORKFLOW - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
    
    # Step 1: Fetch games
    fetch_stats = fetch_todays_games(quiet)
    
    # Step 2: Make predictions
    pred_stats = make_predictions(quiet)
    
    if not quiet:
        logger.info("=" * 70)
        logger.info("MORNING WORKFLOW COMPLETE")
        logger.info(f"  Games fetched: {fetch_stats['fetched']}")
        logger.info(f"  Predictions saved: {pred_stats['saved']}")
        logger.info("=" * 70)
    
    return {'fetch': fetch_stats, 'predictions': pred_stats}


def run_evening_workflow(quiet: bool = False):
    """Run evening workflow: update scores and evaluate."""
    if not quiet:
        logger.info("=" * 70)
        logger.info(f"EVENING WORKFLOW - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
    
    # Step 3: Update scores
    score_stats = update_scores(quiet)
    
    # Step 4: Evaluate predictions
    eval_stats = evaluate_predictions(quiet)
    
    if not quiet:
        logger.info("=" * 70)
        logger.info("EVENING WORKFLOW COMPLETE")
        logger.info(f"  Games updated: {score_stats['updated']}")
        logger.info(f"  Accuracy: {eval_stats['correct']}/{eval_stats['games']} ({eval_stats['accuracy']:.1%})")
        logger.info("=" * 70)
    
    return {'scores': score_stats, 'evaluation': eval_stats}


def run_full_workflow(quiet: bool = False):
    """Run the complete workflow (all steps)."""
    if not quiet:
        logger.info("=" * 70)
        logger.info(f"FULL DAILY WORKFLOW - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 70)
    
    results = {}
    
    # All steps
    results['fetch'] = fetch_todays_games(quiet)
    results['predictions'] = make_predictions(quiet)
    results['scores'] = update_scores(quiet)
    results['evaluation'] = evaluate_predictions(quiet)
    
    if not quiet:
        logger.info("=" * 70)
        logger.info("FULL WORKFLOW COMPLETE")
        logger.info(f"  Games fetched: {results['fetch']['fetched']}")
        logger.info(f"  Predictions saved: {results['predictions']['saved']}")
        logger.info(f"  Scores updated: {results['scores']['updated']}")
        logger.info(f"  Yesterday's accuracy: {results['evaluation']['accuracy']:.1%}")
        logger.info("=" * 70)
    
    return results


def main():
    """Main entry point."""
    parser = ArgumentParser(
        description='Automated NBA Prediction Workflow',
        epilog="""
Examples:
  python daily_workflow.py                  # Run full workflow
  python daily_workflow.py --morning        # Morning: fetch + predict
  python daily_workflow.py --evening        # Evening: update scores + evaluate
  python daily_workflow.py --quiet          # Minimal output for automation
        """
    )
    parser.add_argument('--morning', action='store_true',
                       help='Run morning workflow only (fetch + predict)')
    parser.add_argument('--evening', action='store_true',
                       help='Run evening workflow only (update + evaluate)')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress detailed output (for automation)')
    parser.add_argument('--step', type=int, choices=[1, 2, 3, 4],
                       help='Run specific step only (1=fetch, 2=predict, 3=update, 4=evaluate)')
    args = parser.parse_args()
    
    try:
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
        elif args.morning:
            run_morning_workflow(args.quiet)
        elif args.evening:
            run_evening_workflow(args.quiet)
        else:
            run_full_workflow(args.quiet)
        
        return 0
        
    except Exception as e:
        logger.error(f"Workflow failed: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())

