"""Test script for ModelTrainer implementation."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import numpy as np
import pandas as pd
from src.training.trainer import ModelTrainer
from src.training.data_loader import DataLoader
from src.models.xgboost_model import XGBoostModel

def test_trainer():
    """Test ModelTrainer functionality."""
    print("=" * 70)
    print("Testing ModelTrainer Implementation")
    print("=" * 70)
    
    # Test 1: Initialize trainer
    print("\n1. Initializing trainer...")
    trainer = ModelTrainer(random_state=42)
    print(f"   [OK] Trainer created: {trainer}")
    
    # Test 2: Create sample data
    print("\n2. Creating sample data...")
    X_train = pd.DataFrame(np.random.rand(100, 10), columns=[f'feature_{i}' for i in range(10)])
    y_train_clf = pd.Series(np.random.randint(0, 2, 100))
    y_train_reg = pd.Series(np.random.randn(100) * 10)
    X_val = pd.DataFrame(np.random.rand(20, 10), columns=X_train.columns)
    y_val_clf = pd.Series(np.random.randint(0, 2, 20))
    y_val_reg = pd.Series(np.random.randn(20) * 10)
    X_test = pd.DataFrame(np.random.rand(20, 10), columns=X_train.columns)
    y_test_clf = pd.Series(np.random.randint(0, 2, 20))
    y_test_reg = pd.Series(np.random.randn(20) * 10)
    print(f"   [OK] Created data: train={len(X_train)}, val={len(X_val)}, test={len(X_test)}")
    
    # Test 3: Train classification model
    print("\n3. Training classification model...")
    clf_model = XGBoostModel("test_classifier", "classification", random_state=42, n_estimators=50)
    results_clf = trainer.train_model(
        clf_model, X_train, y_train_clf, X_val, y_val_clf, X_test, y_test_clf, save_model=False
    )
    assert 'training_metrics' in results_clf, "Should have training metrics"
    assert 'test_metrics' in results_clf, "Should have test metrics"
    assert 'test_accuracy' in results_clf, "Should have test accuracy"
    print(f"   [OK] Classification model trained")
    print(f"       Train accuracy: {results_clf['training_metrics'].get('train_accuracy', 'N/A'):.3f}")
    print(f"       Test accuracy: {results_clf.get('test_accuracy', 'N/A'):.3f}")
    
    # Test 4: Train regression model
    print("\n4. Training regression model...")
    reg_model = XGBoostModel("test_regressor", "regression", random_state=42, n_estimators=50)
    results_reg = trainer.train_model(
        reg_model, X_train, y_train_reg, X_val, y_val_reg, X_test, y_test_reg, save_model=False
    )
    assert 'training_metrics' in results_reg, "Should have training metrics"
    assert 'test_metrics' in results_reg, "Should have test metrics"
    assert 'test_rmse' in results_reg, "Should have test RMSE"
    print(f"   [OK] Regression model trained")
    print(f"       Train RMSE: {results_reg['training_metrics'].get('train_rmse', 'N/A'):.3f}")
    print(f"       Test RMSE: {results_reg.get('test_rmse', 'N/A'):.3f}")
    
    # Test 5: Model comparison
    print("\n5. Testing model comparison...")
    comparison = trainer.compare_trained_models(task_type="classification")
    assert not comparison.empty, "Comparison should not be empty"
    assert len(comparison) == 1, "Should have 1 classification model"
    print(f"   [OK] Model comparison works: {len(comparison)} models")
    
    # Test 6: Hyperparameter tuning
    print("\n6. Testing hyperparameter tuning...")
    param_distributions = {
        'max_depth': [3, 5, 7],
        'learning_rate': [0.01, 0.1, 0.2],
        'n_estimators': [50, 100]
    }
    best_model, tuning_results = trainer.hyperparameter_tuning(
        XGBoostModel,
        "tuned_classifier",
        param_distributions,
        X_train, y_train_clf,
        X_val, y_val_clf,
        n_iter=5,  # Small number for testing
        task_type="classification",
        random_state=42,
        verbosity=0
    )
    assert best_model is not None, "Should have best model"
    assert 'best_params' in tuning_results, "Should have best parameters"
    assert best_model.is_trained, "Best model should be trained"
    print(f"   [OK] Hyperparameter tuning completed")
    print(f"       Best score: {tuning_results['best_score']:.4f}")
    print(f"       Best params: max_depth={tuning_results['best_params'].get('max_depth')}")
    
    # Test 7: Train with data loader (mock - using sample data)
    print("\n7. Testing train_with_data_loader interface...")
    # This would normally load from database, but we'll just verify the method exists
    assert hasattr(trainer, 'train_with_data_loader'), "Should have train_with_data_loader method"
    print(f"   [OK] train_with_data_loader method available")
    
    # Test 8: Save training summary
    print("\n8. Testing save training summary...")
    summary_path = trainer.save_training_summary()
    assert summary_path.exists(), "Summary file should exist"
    print(f"   [OK] Training summary saved to: {summary_path}")
    
    # Test 9: Print comparison
    print("\n9. Testing print comparison...")
    try:
        trainer.print_comparison(task_type="classification")
        print(f"   [OK] Print comparison works")
    except Exception as e:
        print(f"   [ERROR] Print comparison failed: {e}")
        return False
    
    # Test 10: Multiple models comparison
    print("\n10. Testing multiple models comparison...")
    clf_model2 = XGBoostModel("test_classifier_2", "classification", random_state=42, n_estimators=30)
    trainer.train_model(clf_model2, X_train, y_train_clf, X_val, y_val_clf, save_model=False)
    comparison = trainer.compare_trained_models(task_type="classification")
    assert len(comparison) == 3, f"Should have 3 models (2 trained + 1 tuned), got {len(comparison)}"
    print(f"   [OK] Multiple models comparison: {len(comparison)} models")
    
    print("\n" + "=" * 70)
    print("All ModelTrainer tests passed!")
    print("=" * 70)
    return True

if __name__ == "__main__":
    success = test_trainer()
    sys.exit(0 if success else 1)




