"""Test ModelTrainer with real database data."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from src.training.trainer import ModelTrainer
from src.models.xgboost_model import XGBoostModel

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_trainer_with_real_data():
    """Test ModelTrainer with real database data."""
    print("=" * 70)
    print("Testing ModelTrainer with Real Database Data")
    print("=" * 70)
    
    # Test 1: Initialize trainer
    print("\n1. Initializing trainer...")
    trainer = ModelTrainer(random_state=42)
    print(f"   [OK] Trainer initialized")
    
    # Test 2: Train classification model with real data
    print("\n2. Training classification model with real data...")
    clf_model = XGBoostModel(
        "nba_classifier",
        "classification",
        random_state=42,
        n_estimators=50,  # Small for testing
        verbosity=0
    )
    
    try:
        results_clf = trainer.train_with_data_loader(
            clf_model,
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25'],
            save_model=True
        )
        
        assert 'training_metrics' in results_clf, "Should have training metrics"
        assert 'test_metrics' in results_clf, "Should have test metrics"
        assert 'test_accuracy' in results_clf, "Should have test accuracy"
        
        print(f"   [OK] Classification model trained successfully")
        print(f"       Train samples: {results_clf['train_samples']}")
        print(f"       Val samples: {results_clf['val_samples']}")
        print(f"       Test samples: {results_clf['test_samples']}")
        print(f"       Train accuracy: {results_clf['training_metrics'].get('train_accuracy', 'N/A'):.3f}")
        print(f"       Val accuracy: {results_clf['training_metrics'].get('val_accuracy', 'N/A'):.3f}")
        print(f"       Test accuracy: {results_clf.get('test_accuracy', 'N/A'):.3f}")
        
    except Exception as e:
        print(f"   [ERROR] Classification training failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 3: Train regression model with real data
    print("\n3. Training regression model with real data...")
    reg_model = XGBoostModel(
        "nba_regressor",
        "regression",
        random_state=42,
        n_estimators=50,  # Small for testing
        verbosity=0
    )
    
    try:
        results_reg = trainer.train_with_data_loader(
            reg_model,
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25'],
            save_model=True
        )
        
        assert 'training_metrics' in results_reg, "Should have training metrics"
        assert 'test_metrics' in results_reg, "Should have test metrics"
        assert 'test_rmse' in results_reg, "Should have test RMSE"
        
        print(f"   [OK] Regression model trained successfully")
        print(f"       Train samples: {results_reg['train_samples']}")
        print(f"       Val samples: {results_reg['val_samples']}")
        print(f"       Test samples: {results_reg['test_samples']}")
        print(f"       Train RMSE: {results_reg['training_metrics'].get('train_rmse', 'N/A'):.3f}")
        print(f"       Val RMSE: {results_reg['training_metrics'].get('val_rmse', 'N/A'):.3f}")
        print(f"       Test RMSE: {results_reg.get('test_rmse', 'N/A'):.3f}")
        
    except Exception as e:
        print(f"   [ERROR] Regression training failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 4: Model comparison
    print("\n4. Comparing trained models...")
    try:
        comparison_clf = trainer.compare_trained_models(task_type="classification")
        comparison_reg = trainer.compare_trained_models(task_type="regression")
        
        assert not comparison_clf.empty, "Classification comparison should not be empty"
        assert not comparison_reg.empty, "Regression comparison should not be empty"
        
        print(f"   [OK] Model comparison works")
        print(f"       Classification models: {len(comparison_clf)}")
        print(f"       Regression models: {len(comparison_reg)}")
        
    except Exception as e:
        print(f"   [ERROR] Model comparison failed: {e}")
        return False
    
    # Test 5: Print comparison
    print("\n5. Printing model comparison...")
    try:
        print("\n   Classification Models:")
        trainer.print_comparison(task_type="classification")
        print("\n   Regression Models:")
        trainer.print_comparison(task_type="regression")
        print(f"   [OK] Print comparison works")
    except Exception as e:
        print(f"   [ERROR] Print comparison failed: {e}")
        return False
    
    # Test 6: Save training summary
    print("\n6. Saving training summary...")
    try:
        summary_path = trainer.save_training_summary()
        assert summary_path.exists(), "Summary file should exist"
        print(f"   [OK] Training summary saved: {summary_path}")
    except Exception as e:
        print(f"   [ERROR] Save summary failed: {e}")
        return False
    
    print("\n" + "=" * 70)
    print("All real data tests passed!")
    print("=" * 70)
    return True

if __name__ == "__main__":
    success = test_trainer_with_real_data()
    sys.exit(0 if success else 1)




