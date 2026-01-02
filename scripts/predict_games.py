"""
Predict NBA games for a specific date or date range.
Usage:
    python scripts/predict_games.py                    # Today's games
    python scripts/predict_games.py --date 2026-01-05   # Specific date
    python scripts/predict_games.py --days 7           # Next 7 days
    python scripts/predict_games.py --date-range 2026-01-03 2026-01-07  # Date range
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import warnings
warnings.filterwarnings('ignore', category=FutureWarning)

from datetime import date, timedelta
from argparse import ArgumentParser
from src.database.db_manager import DatabaseManager
from src.prediction.prediction_service import PredictionService
from src.database.models import Game, Prediction

def get_team_name(team_id: str, db: DatabaseManager) -> str:
    """Get team name from team ID."""
    team = db.get_team(team_id)
    if team:
        return f"{team.team_name} ({team.team_abbreviation})"
    return team_id

def predict_games_for_date(target_date: date, db: DatabaseManager, prediction_service: PredictionService, 
                           save_to_db: bool = False, quiet: bool = False):
    """Predict all games for a specific date."""
    today_str = target_date.strftime('%Y%m%d')
    
    # Get games for target date
    with db.get_session() as session:
        games = session.query(Game).filter(
            Game.game_date == target_date
        ).order_by(Game.game_id).all()
    
    # First try to filter by game_id prefix (strict check)
    verified_games = [g for g in games if g.game_id.startswith(today_str)]
    
    # If no games found with strict check, use all games with correct date
    # (This handles cases where game_id might have wrong prefix but date is correct)
    if not verified_games:
        verified_games = games
        if verified_games and not quiet:
            print(f"  Note: Found {len(verified_games)} games by date (game_id prefix check skipped)")
    
    if not verified_games:
        if not quiet:
            print(f"  No games found for {target_date}")
        return []
    
    predictions = []
    saved_count = 0
    for game in verified_games:
        try:
            result = prediction_service.predict_game(
                game_id=game.game_id,
                model_name='nba_v2_classifier',
                reg_model_name='nba_v2_regressor'
            )
            
            if result:
                predictions.append((game, result))
                
                # Save prediction to database if requested
                if save_to_db:
                    try:
                        prediction_service.save_prediction(result, model_name='nba_v2_classifier')
                        saved_count += 1
                    except Exception as e:
                        if not quiet:
                            print(f"  Warning: Could not save prediction for {game.game_id}: {e}")
        except Exception as e:
            if not quiet:
                print(f"  Error predicting {game.game_id}: {e}")
    
    if save_to_db and not quiet:
        print(f"  Saved {saved_count} predictions to database")
    
    return predictions

def main():
    """Main prediction function."""
    parser = ArgumentParser(description='Predict NBA games for specific dates')
    parser.add_argument('--date', type=str, help='Date to predict (YYYY-MM-DD)')
    parser.add_argument('--days', type=int, help='Number of days ahead to predict (from today)')
    parser.add_argument('--date-range', nargs=2, metavar=('START', 'END'), 
                       help='Date range to predict (YYYY-MM-DD YYYY-MM-DD)')
    parser.add_argument('--save', action='store_true',
                       help='Save predictions to database for later evaluation')
    parser.add_argument('--quiet', action='store_true',
                       help='Suppress detailed output (for automation)')
    args = parser.parse_args()
    
    # Determine target dates
    target_dates = []
    
    if args.date_range:
        start_date = date.fromisoformat(args.date_range[0])
        end_date = date.fromisoformat(args.date_range[1])
        current = start_date
        while current <= end_date:
            target_dates.append(current)
            current += timedelta(days=1)
    elif args.date:
        target_dates.append(date.fromisoformat(args.date))
    elif args.days:
        for i in range(args.days):
            target_dates.append(date.today() + timedelta(days=i))
    else:
        # Default: today
        target_dates.append(date.today())
    
    print("=" * 70)
    print(f"NBA GAME PREDICTIONS")
    print("=" * 70)
    print(f"Predicting games for {len(target_dates)} date(s)")
    if len(target_dates) == 1:
        print(f"Date: {target_dates[0]}")
    else:
        print(f"Date range: {target_dates[0]} to {target_dates[-1]}")
    print("=" * 70)
    
    db = DatabaseManager()
    prediction_service = PredictionService(db)
    
    all_predictions = []
    
    for target_date in target_dates:
        if not args.quiet:
            print(f"\n[{target_date}]")
            print("-" * 70)
        
        predictions = predict_games_for_date(target_date, db, prediction_service, 
                                              save_to_db=args.save, quiet=args.quiet)
        
        if not predictions:
            if not args.quiet:
                print(f"  No games to predict for {target_date}")
            continue
        
        if not args.quiet:
            for i, (game, result) in enumerate(predictions, 1):
                away_name = get_team_name(game.away_team_id, db)
                home_name = get_team_name(game.home_team_id, db)
                winner_name = get_team_name(result['predicted_winner'], db)
                
                print(f'\n  [{i}] {away_name} @ {home_name}')
                print(f'      Game ID: {game.game_id}')
                print(f'      Winner: {winner_name}')
                print(f'      Home Win Prob: {result["win_probability_home"]:.1%}')
                print(f'      Away Win Prob: {result["win_probability_away"]:.1%}')
                print(f'      Confidence: {result["confidence"]:.1%}')
                
                if result.get('predicted_point_differential') is not None:
                    diff = result['predicted_point_differential']
                    if diff > 0:
                        print(f'      Margin: {home_name} by {diff:.1f} pts')
                    elif diff < 0:
                        print(f'      Margin: {away_name} by {abs(diff):.1f} pts')
                    else:
                        print(f'      Margin: Tie')
            
            print(f"\n  Total: {len(predictions)} predictions for {target_date}")
        
        all_predictions.extend(predictions)
    
    if not args.quiet:
        print("\n" + "=" * 70)
        print(f"SUMMARY")
        print("=" * 70)
        print(f"Total predictions: {len(all_predictions)}")
        print(f"Dates covered: {len(target_dates)}")
        if args.save:
            print(f"Predictions saved to database: Yes")
        print("=" * 70)
    else:
        # Minimal output for automation
        print(f"Predicted {len(all_predictions)} games for {len(target_dates)} date(s)")

if __name__ == '__main__':
    main()

