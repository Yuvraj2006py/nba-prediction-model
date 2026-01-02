"""Batch prediction script for upcoming NBA games."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
import logging
from datetime import date, timedelta
from typing import List, Optional
from src.prediction.prediction_service import PredictionService
from src.database.db_manager import DatabaseManager
from src.database.models import Game

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description='Make predictions for upcoming NBA games',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Predict upcoming games (next 7 days)
  python scripts/make_predictions.py --model-name nba_classifier
  
  # Predict specific games
  python scripts/make_predictions.py --game-ids 20241001LALGSW --model-name nba_classifier
  
  # Predict games in date range
  python scripts/make_predictions.py --start-date 2024-10-01 --end-date 2024-10-07 --model-name nba_classifier
        """
    )
    
    # Model selection
    parser.add_argument(
        '--model-name',
        type=str,
        required=True,
        help='Name of the model to use for predictions'
    )
    parser.add_argument(
        '--clf-model',
        type=str,
        default=None,
        help='Classification model name (overrides --model-name for classification)'
    )
    parser.add_argument(
        '--reg-model',
        type=str,
        default=None,
        help='Regression model name (overrides --model-name for regression)'
    )
    
    # Game selection
    parser.add_argument(
        '--game-ids',
        type=str,
        default=None,
        help='Comma-separated list of game IDs to predict'
    )
    parser.add_argument(
        '--start-date',
        type=str,
        default=None,
        help='Start date for game search (YYYY-MM-DD, default: today)'
    )
    parser.add_argument(
        '--end-date',
        type=str,
        default=None,
        help='End date for game search (YYYY-MM-DD, default: 7 days from start)'
    )
    parser.add_argument(
        '--limit',
        type=int,
        default=None,
        help='Maximum number of games to predict'
    )
    
    # Options
    parser.add_argument(
        '--regenerate-features',
        action='store_true',
        help='Regenerate features even if they exist'
    )
    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save predictions to database'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be predicted without making predictions'
    )
    
    args = parser.parse_args()
    
    logger.info("=" * 70)
    logger.info("NBA Game Predictions")
    logger.info("=" * 70)
    
    # Initialize services
    db_manager = DatabaseManager()
    prediction_service = PredictionService(db_manager)
    
    # Determine game IDs
    game_ids = []
    
    if args.game_ids:
        # Use provided game IDs
        game_ids = [gid.strip() for gid in args.game_ids.split(',')]
        logger.info(f"Predicting {len(game_ids)} specified games")
    else:
        # Get upcoming games
        start_date = date.today()
        if args.start_date:
            start_date = date.fromisoformat(args.start_date)
        
        end_date = start_date + timedelta(days=7)
        if args.end_date:
            end_date = date.fromisoformat(args.end_date)
        
        logger.info(f"Finding games from {start_date} to {end_date}")
        games = prediction_service.get_upcoming_games(
            start_date=start_date,
            end_date=end_date,
            limit=args.limit
        )
        game_ids = [game.game_id for game in games]
        logger.info(f"Found {len(game_ids)} upcoming games")
    
    if not game_ids:
        logger.warning("No games to predict")
        return 0
    
    if args.dry_run:
        logger.info("DRY RUN - Would predict the following games:")
        with db_manager.get_session() as session:
            for game_id in game_ids:
                game = session.query(Game).filter_by(game_id=game_id).first()
                if game:
                    logger.info(f"  {game_id}: {game.away_team_id} @ {game.home_team_id} on {game.game_date}")
                else:
                    logger.info(f"  {game_id}: (game not found)")
        return 0
    
    # Make predictions
    logger.info(f"\nMaking predictions using model: {args.model_name}")
    if args.clf_model:
        logger.info(f"  Classification model: {args.clf_model}")
    if args.reg_model:
        logger.info(f"  Regression model: {args.reg_model}")
    
    results = prediction_service.predict_batch(
        game_ids=game_ids,
        model_name=args.model_name,
        clf_model_name=args.clf_model,
        reg_model_name=args.reg_model,
        save_to_db=not args.no_save,
        regenerate_features=args.regenerate_features
    )
    
    # Print results
    logger.info(f"\n{'=' * 70}")
    logger.info("Prediction Results")
    logger.info(f"{'=' * 70}")
    
    successful = 0
    errors = 0
    
    for result in results:
        if 'error' in result:
            logger.error(f"Game {result['game_id']}: {result['error']}")
            errors += 1
        else:
            successful += 1
            
            # Get team names
            away_team_id = result.get('away_team_id')
            home_team_id = result.get('home_team_id')
            predicted_winner_id = result.get('predicted_winner')
            
            away_team = db_manager.get_team(away_team_id)
            home_team = db_manager.get_team(home_team_id)
            predicted_winner_team = db_manager.get_team(predicted_winner_id) if predicted_winner_id else None
            
            away_name = away_team.team_name if away_team else away_team_id
            home_name = home_team.team_name if home_team else home_team_id
            winner_name = predicted_winner_team.team_name if predicted_winner_team else predicted_winner_id
            
            # Format output nicely
            logger.info(f"\n{'─' * 70}")
            logger.info(f"Game: {result['game_id']}")
            logger.info(f"Date: {result.get('game_date')}")
            logger.info(f"")
            logger.info(f"Matchup: {away_name} @ {home_name}")
            logger.info(f"")
            logger.info(f"PREDICTION:")
            logger.info(f"  Winner: {winner_name}")
            logger.info(f"  Confidence: {result.get('confidence', 0):.1%}")
            logger.info(f"  Home Win Probability: {result.get('win_probability_home', 0):.1%}")
            logger.info(f"  Away Win Probability: {result.get('win_probability_away', 0):.1%}")
            
            if result.get('predicted_point_differential') is not None:
                diff = result.get('predicted_point_differential')
                if diff > 0:
                    logger.info(f"  Predicted Margin: {home_name} wins by {diff:.1f} points")
                elif diff < 0:
                    logger.info(f"  Predicted Margin: {away_name} wins by {abs(diff):.1f} points")
                else:
                    logger.info(f"  Predicted Margin: Tie")
    
    logger.info(f"\n{'=' * 70}")
    logger.info(f"Summary: {successful} successful, {errors} errors")
    logger.info(f"{'=' * 70}")
    
    return 0 if errors == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

