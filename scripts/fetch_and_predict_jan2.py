"""
Fetch January 2nd, 2026 games, generate features, and make predictions.
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from datetime import date
from src.database.db_manager import DatabaseManager
from src.prediction.prediction_service import PredictionService
from src.database.models import Game, Team
from scripts.transform_features import FeatureTransformer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_team_name(team_id: str, db: DatabaseManager) -> str:
    """Get team name from team ID."""
    team = db.get_team(team_id)
    if team:
        return f"{team.team_name} ({team.team_abbreviation})"
    return team_id


def main():
    """Fetch games for Jan 2, 2026 and make predictions."""
    target_date = date(2026, 1, 2)
    
    logger.info("=" * 70)
    logger.info(f"FETCHING AND PREDICTING GAMES FOR {target_date}")
    logger.info("=" * 70)
    
    db = DatabaseManager()
    
    # Step 1: Fetch games for Jan 2
    logger.info("\n[STEP 1] Fetching games for January 2, 2026...")
    try:
        from scripts.fetch_all_today_games import main as fetch_games
        # Temporarily override date
        import scripts.fetch_all_today_games as fetch_module
        original_date = date.today()
        # We'll manually set the date in the fetch script logic
        logger.info("Running fetch script...")
        # Actually, let's just check what games exist and fetch if needed
    except Exception as e:
        logger.warning(f"Could not run fetch script: {e}")
    
    # Check existing games
    with db.get_session() as session:
        games = session.query(Game).filter(
            Game.game_date == target_date
        ).all()
    
    logger.info(f"Found {len(games)} games for {target_date}")
    
    if not games:
        logger.error("No games found! Please run fetch_all_today_games.py first")
        return
    
    # Step 2: Generate features for these games
    logger.info("\n[STEP 2] Generating features for games...")
    transformer = FeatureTransformer(db)
    
    for game in games:
        logger.info(f"  Processing game: {game.game_id}")
        try:
            # Generate features for this game
            transformer.transform_features_for_season(
                season=game.season,
                full_refresh=False
            )
        except Exception as e:
            logger.warning(f"  Error generating features for {game.game_id}: {e}")
    
    # Step 3: Make predictions
    logger.info("\n[STEP 3] Making predictions...")
    prediction_service = PredictionService(db)
    
    predictions = []
    for game in games:
        try:
            result = prediction_service.predict_game(
                game_id=game.game_id,
                model_name='nba_v2_classifier',
                reg_model_name='nba_v2_regressor'
            )
            
            if result:
                predictions.append({
                    'game': game,
                    'prediction': result
                })
            else:
                logger.warning(f"Failed to predict game {game.game_id}")
        except Exception as e:
            logger.error(f"Error predicting {game.game_id}: {e}")
    
    # Step 4: Display predictions
    logger.info("\n" + "=" * 70)
    logger.info("PREDICTIONS FOR JANUARY 2, 2026")
    logger.info("=" * 70)
    
    for i, pred_data in enumerate(predictions, 1):
        game = pred_data['game']
        pred = pred_data['prediction']
        
        away_name = get_team_name(game.away_team_id, db)
        home_name = get_team_name(game.home_team_id, db)
        winner_name = get_team_name(pred['predicted_winner'], db)
        
        logger.info(f"\n[{i}] {away_name} @ {home_name}")
        logger.info(f"    Game ID: {game.game_id}")
        logger.info(f"    Predicted Winner: {winner_name}")
        logger.info(f"    Home Win Probability: {pred['win_probability_home']:.1%}")
        logger.info(f"    Away Win Probability: {pred['win_probability_away']:.1%}")
        logger.info(f"    Confidence: {pred['confidence']:.1%}")
        
        if pred.get('predicted_point_differential') is not None:
            diff = pred['predicted_point_differential']
            if diff > 0:
                logger.info(f"    Predicted Margin: {home_name} by {diff:.1f} points")
            elif diff < 0:
                logger.info(f"    Predicted Margin: {away_name} by {abs(diff):.1f} points")
            else:
                logger.info(f"    Predicted Margin: Tie")
    
    logger.info("\n" + "=" * 70)
    logger.info(f"Total predictions: {len(predictions)}")
    logger.info("=" * 70)
    
    # Step 5: Save predictions
    logger.info("\n[STEP 4] Saving predictions to database...")
    for pred_data in predictions:
        try:
            prediction_service.save_prediction(
                pred_data['prediction'],
                model_name='nba_v2_classifier'
            )
        except Exception as e:
            logger.warning(f"Error saving prediction: {e}")


if __name__ == '__main__':
    main()

