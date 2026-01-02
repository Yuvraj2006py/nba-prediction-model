"""CLI interface for making predictions on NBA games."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import argparse
import logging
from datetime import date
from src.prediction.prediction_service import PredictionService
from src.database.db_manager import DatabaseManager
from src.database.models import Game

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def format_prediction(result: dict) -> str:
    """Format prediction result for display."""
    lines = []
    lines.append(f"Game: {result.get('game_id', 'N/A')}")
    lines.append(f"Date: {result.get('game_date', 'N/A')}")
    lines.append(f"Matchup: {result.get('away_team_id', 'N/A')} @ {result.get('home_team_id', 'N/A')}")
    lines.append(f"")
    lines.append(f"Prediction:")
    lines.append(f"  Winner: {result.get('predicted_winner', 'N/A')}")
    lines.append(f"  Home Win Probability: {result.get('win_probability_home', 0):.1%}")
    lines.append(f"  Away Win Probability: {result.get('win_probability_away', 0):.1%}")
    lines.append(f"  Confidence: {result.get('confidence', 0):.1%}")
    if result.get('predicted_point_differential') is not None:
        diff = result.get('predicted_point_differential')
        lines.append(f"  Predicted Point Differential: {diff:+.1f}")
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='CLI for NBA game predictions',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Predict a single game
  python scripts/predict_cli.py --game-id 20241001LALGSW --model nba_classifier
  
  # List upcoming games
  python scripts/predict_cli.py --list-games --days 7
  
  # Interactive mode
  python scripts/predict_cli.py --interactive --model nba_classifier
        """
    )
    
    parser.add_argument(
        '--model',
        type=str,
        default='nba_classifier',
        help='Model name to use (default: nba_classifier)'
    )
    parser.add_argument(
        '--clf-model',
        type=str,
        default=None,
        help='Classification model name'
    )
    parser.add_argument(
        '--reg-model',
        type=str,
        default=None,
        help='Regression model name'
    )
    
    # Actions
    parser.add_argument(
        '--game-id',
        type=str,
        help='Game ID to predict'
    )
    parser.add_argument(
        '--list-games',
        action='store_true',
        help='List upcoming games'
    )
    parser.add_argument(
        '--interactive',
        action='store_true',
        help='Interactive mode'
    )
    parser.add_argument(
        '--days',
        type=int,
        default=7,
        help='Days ahead to look for games (default: 7)'
    )
    
    parser.add_argument(
        '--save',
        action='store_true',
        help='Save prediction to database'
    )
    
    args = parser.parse_args()
    
    # Initialize services
    db_manager = DatabaseManager()
    prediction_service = PredictionService(db_manager)
    
    if args.list_games:
        # List upcoming games
        games = prediction_service.get_upcoming_games(limit=None)
        print(f"\nUpcoming Games (next {args.days} days):")
        print("=" * 70)
        for game in games:
            print(f"{game.game_id}: {game.away_team_id} @ {game.home_team_id} on {game.game_date}")
        return 0
    
    if args.interactive:
        # Interactive mode
        print("\nNBA Prediction CLI - Interactive Mode")
        print("=" * 70)
        
        while True:
            try:
                game_id = input("\nEnter game ID (or 'q' to quit, 'list' for games): ").strip()
                
                if game_id.lower() == 'q':
                    break
                
                if game_id.lower() == 'list':
                    games = prediction_service.get_upcoming_games(limit=10)
                    print(f"\nUpcoming Games:")
                    for game in games:
                        print(f"  {game.game_id}: {game.away_team_id} @ {game.home_team_id} on {game.game_date}")
                    continue
                
                if not game_id:
                    continue
                
                # Make prediction
                result = prediction_service.predict_game(
                    game_id,
                    args.model,
                    clf_model_name=args.clf_model,
                    reg_model_name=args.reg_model
                )
                
                print(f"\n{format_prediction(result)}")
                
                if args.save:
                    prediction_service.save_prediction(result)
                    print("\n[Saved to database]")
            
            except KeyboardInterrupt:
                print("\n\nExiting...")
                break
            except Exception as e:
                print(f"\nError: {e}")
        
        return 0
    
    if args.game_id:
        # Single game prediction
        try:
            result = prediction_service.predict_game(
                args.game_id,
                args.model,
                clf_model_name=args.clf_model,
                reg_model_name=args.reg_model
            )
            
            print(f"\n{format_prediction(result)}")
            
            if args.save:
                prediction_service.save_prediction(result)
                print("\n[Saved to database]")
            
            return 0
        except Exception as e:
            logger.error(f"Error making prediction: {e}")
            return 1
    
    # No action specified
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())



