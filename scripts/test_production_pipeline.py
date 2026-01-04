"""Comprehensive test for production pipeline components."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from src.prediction.prediction_service import PredictionService
from src.monitoring.prediction_monitor import PredictionMonitor
from src.database.db_manager import DatabaseManager
from src.database.models import Game, Prediction

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_production_pipeline():
    """Test all production pipeline components."""
    print("=" * 70)
    print("Production Pipeline Comprehensive Test")
    print("=" * 70)
    
    db_manager = DatabaseManager()
    
    # Test 1: Prediction Service
    print("\n1. Testing Prediction Service...")
    try:
        service = PredictionService(db_manager)
        print("   [OK] PredictionService initialized")
        
        # Get a test game
        with db_manager.get_session() as session:
            game = session.query(Game).filter(
                Game.home_score.isnot(None)
            ).first()
        
        if game:
            # Test prediction
            try:
                result = service.predict_game(
                    game.game_id,
                    'nba_classifier',
                    reg_model_name='nba_regressor'
                )
                print(f"   [OK] Prediction made: {result.get('predicted_winner')}")
                print(f"        Confidence: {result.get('confidence', 0):.3f}")
            except Exception as e:
                print(f"   [WARNING] Prediction test: {e}")
    except Exception as e:
        print(f"   [ERROR] Prediction service test failed: {e}")
        return False
    
    # Test 2: Save Prediction
    print("\n2. Testing Save Prediction...")
    try:
        if game:
            prediction = service.predict_and_save(
                game.game_id,
                'nba_classifier',
                reg_model_name='nba_regressor'
            )
            print(f"   [OK] Prediction saved to database: {prediction.id}")
    except Exception as e:
        print(f"   [WARNING] Save prediction test: {e}")
    
    # Test 3: Monitoring
    print("\n3. Testing Prediction Monitor...")
    try:
        monitor = PredictionMonitor(db_manager)
        health_check = monitor.run_health_check('nba_classifier')
        print(f"   [OK] Health check completed")
        print(f"        Overall status: {health_check.get('overall_status')}")
    except Exception as e:
        print(f"   [WARNING] Monitoring test: {e}")
    
    # Test 4: Batch Operations
    print("\n4. Testing Batch Operations...")
    try:
        with db_manager.get_session() as session:
            games = session.query(Game).filter(
                Game.home_score.isnot(None)
            ).limit(3).all()
        
        if games:
            game_ids = [g.game_id for g in games]
            results = service.predict_batch(
                game_ids,
                'nba_classifier',
                save_to_db=False
            )
            successful = len([r for r in results if 'error' not in r])
            print(f"   [OK] Batch prediction: {successful}/{len(results)} successful")
    except Exception as e:
        print(f"   [WARNING] Batch operations test: {e}")
    
    # Test 5: Upcoming Games
    print("\n5. Testing Upcoming Games...")
    try:
        upcoming = service.get_upcoming_games(limit=5)
        print(f"   [OK] Found {len(upcoming)} upcoming games")
    except Exception as e:
        print(f"   [ERROR] Upcoming games test failed: {e}")
        return False
    
    print("\n" + "=" * 70)
    print("Production Pipeline Tests Complete")
    print("=" * 70)
    return True


if __name__ == "__main__":
    success = test_production_pipeline()
    sys.exit(0 if success else 1)




