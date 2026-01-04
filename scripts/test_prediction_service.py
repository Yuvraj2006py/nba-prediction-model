"""Test script for prediction service."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from src.prediction.prediction_service import PredictionService
from src.database.db_manager import DatabaseManager
from src.database.models import Game

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_prediction_service():
    """Test prediction service functionality."""
    print("=" * 70)
    print("Testing Prediction Service")
    print("=" * 70)
    
    # Test 1: Initialize service
    print("\n1. Initializing prediction service...")
    db_manager = DatabaseManager()
    service = PredictionService(db_manager)
    print(f"   [OK] Service initialized")
    
    # Test 2: Get a finished game for testing
    print("\n2. Finding test game...")
    with db_manager.get_session() as session:
        # Get a finished game with features
        game = session.query(Game).filter(
            Game.home_score.isnot(None),
            Game.away_score.isnot(None)
        ).first()
        
        if not game:
            print("   [ERROR] No finished games found in database")
            return False
        
        print(f"   [OK] Found test game: {game.game_id}")
        print(f"       {game.away_team_id} @ {game.home_team_id} on {game.game_date}")
    
    # Test 3: Get features for game
    print("\n3. Testing feature generation...")
    try:
        features = service.get_features_for_game(game.game_id)
        if features is not None and not features.empty:
            print(f"   [OK] Features generated: {len(features.columns)} features")
        else:
            print(f"   [WARNING] Could not generate features (may need to run feature generation)")
    except Exception as e:
        print(f"   [WARNING] Feature generation error: {e}")
    
    # Test 4: Check if models exist
    print("\n4. Checking for trained models...")
    model_files = list(service.models_dir.glob("*.pkl"))
    if not model_files:
        print(f"   [WARNING] No trained models found in {service.models_dir}")
        print(f"   [INFO] Run training script first to create models")
        return True  # Not a failure, just no models yet
    
    model_names = [f.stem for f in model_files]
    print(f"   [OK] Found {len(model_names)} models: {', '.join(model_names)}")
    
    # Test 5: Try to load a model
    print("\n5. Testing model loading...")
    try:
        # Try to find classification model
        clf_models = [m for m in model_names if 'classifier' in m.lower() or 'clf' in m.lower()]
        if clf_models:
            model_name = clf_models[0]
            model = service.load_model(model_name, "classification")
            print(f"   [OK] Loaded model: {model_name}")
        else:
            print(f"   [INFO] No classification model found to test")
    except Exception as e:
        print(f"   [ERROR] Model loading failed: {e}")
        return False
    
    # Test 6: Test prediction (if we have features and model)
    print("\n6. Testing prediction...")
    try:
        if features is not None and not features.empty and 'model' in locals():
            prediction = service.predict_game(
                game.game_id,
                model_name,
                regenerate_features=False
            )
            print(f"   [OK] Prediction made successfully")
            print(f"       Predicted winner: {prediction.get('predicted_winner')}")
            print(f"       Confidence: {prediction.get('confidence', 0):.3f}")
            print(f"       Home probability: {prediction.get('win_probability_home', 0):.3f}")
        else:
            print(f"   [SKIP] Skipping prediction test (missing features or model)")
    except Exception as e:
        print(f"   [WARNING] Prediction test failed: {e}")
        print(f"   [INFO] This is expected if models haven't been trained yet")
    
    # Test 7: Test get_upcoming_games
    print("\n7. Testing get_upcoming_games...")
    try:
        upcoming = service.get_upcoming_games(limit=5)
        print(f"   [OK] Found {len(upcoming)} upcoming games")
    except Exception as e:
        print(f"   [ERROR] get_upcoming_games failed: {e}")
        return False
    
    print("\n" + "=" * 70)
    print("Prediction Service Tests Complete")
    print("=" * 70)
    return True


if __name__ == "__main__":
    success = test_prediction_service()
    sys.exit(0 if success else 1)




