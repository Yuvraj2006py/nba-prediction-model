"""Test script for XGBoostModel implementation."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import numpy as np
import pandas as pd
from src.models.xgboost_model import XGBoostModel

def test_xgboost_model():
    """Test XGBoostModel functionality."""
    print("=" * 70)
    print("Testing XGBoostModel Implementation")
    print("=" * 70)
    
    # Test 1: Create classification model
    print("\n1. Creating classification model...")
    clf_model = XGBoostModel("test_classifier", "classification", random_state=42)
    print(f"   [OK] Created: {clf_model}")
    assert not clf_model.is_trained, "Model should not be trained initially"
    
    # Test 2: Create regression model
    print("\n2. Creating regression model...")
    reg_model = XGBoostModel("test_regressor", "regression", random_state=42)
    print(f"   [OK] Created: {reg_model}")
    
    # Test 3: Test classification training
    print("\n3. Testing classification training...")
    X_train_clf = pd.DataFrame(np.random.rand(100, 10), columns=[f'feature_{i}' for i in range(10)])
    y_train_clf = pd.Series(np.random.randint(0, 2, 100))
    X_val_clf = pd.DataFrame(np.random.rand(20, 10), columns=X_train_clf.columns)
    y_val_clf = pd.Series(np.random.randint(0, 2, 20))
    
    metrics_clf = clf_model.train(X_train_clf, y_train_clf, X_val_clf, y_val_clf, early_stopping_rounds=5)
    assert clf_model.is_trained, "Model should be trained"
    assert 'train_accuracy' in metrics_clf, "Should have train accuracy"
    assert 'val_accuracy' in metrics_clf, "Should have val accuracy"
    print(f"   [OK] Training completed. Train accuracy: {metrics_clf['train_accuracy']:.3f}")
    print(f"   [OK] Validation accuracy: {metrics_clf['val_accuracy']:.3f}")
    
    # Test 4: Test classification prediction
    print("\n4. Testing classification prediction...")
    X_test_clf = pd.DataFrame(np.random.rand(10, 10), columns=X_train_clf.columns)
    predictions_clf = clf_model.predict(X_test_clf)
    assert len(predictions_clf) == 10, "Should return 10 predictions"
    assert all(p in [0, 1] for p in predictions_clf), "Predictions should be binary"
    print(f"   [OK] Predictions shape: {predictions_clf.shape}")
    
    # Test 5: Test classification prediction with probabilities
    print("\n5. Testing classification prediction with probabilities...")
    pred_clf, proba_clf = clf_model.predict(X_test_clf, return_proba=True)
    assert proba_clf.shape == (10, 2), "Probabilities should be (n_samples, n_classes)"
    assert np.allclose(proba_clf.sum(axis=1), 1.0), "Probabilities should sum to 1"
    print(f"   [OK] Predictions and probabilities shape: {pred_clf.shape}, {proba_clf.shape}")
    
    # Test 6: Test regression training
    print("\n6. Testing regression training...")
    X_train_reg = pd.DataFrame(np.random.rand(100, 10), columns=[f'feature_{i}' for i in range(10)])
    y_train_reg = pd.Series(np.random.randn(100) * 10)  # Point differentials
    X_val_reg = pd.DataFrame(np.random.rand(20, 10), columns=X_train_reg.columns)
    y_val_reg = pd.Series(np.random.randn(20) * 10)
    
    metrics_reg = reg_model.train(X_train_reg, y_train_reg, X_val_reg, y_val_reg, early_stopping_rounds=5)
    assert reg_model.is_trained, "Model should be trained"
    assert 'train_rmse' in metrics_reg, "Should have train RMSE"
    assert 'val_rmse' in metrics_reg, "Should have val RMSE"
    print(f"   [OK] Training completed. Train RMSE: {metrics_reg['train_rmse']:.3f}")
    print(f"   [OK] Validation RMSE: {metrics_reg['val_rmse']:.3f}")
    
    # Test 7: Test regression prediction
    print("\n7. Testing regression prediction...")
    X_test_reg = pd.DataFrame(np.random.rand(10, 10), columns=X_train_reg.columns)
    predictions_reg = reg_model.predict(X_test_reg)
    assert len(predictions_reg) == 10, "Should return 10 predictions"
    print(f"   [OK] Predictions shape: {predictions_reg.shape}")
    
    # Test 8: Test save and load
    print("\n8. Testing save and load...")
    save_path = clf_model.save()
    assert save_path.exists(), "Model file should exist"
    print(f"   [OK] Model saved to: {save_path}")
    
    # Create new model and load
    new_model = XGBoostModel("test_classifier", "classification")
    new_model.load(save_path)
    assert new_model.is_trained, "Loaded model should be trained"
    print(f"   [OK] Model loaded successfully")
    
    # Test predictions match
    pred_original = clf_model.predict(X_test_clf)
    pred_loaded = new_model.predict(X_test_clf)
    assert np.allclose(pred_original, pred_loaded), "Loaded model predictions should match"
    print(f"   [OK] Loaded model predictions match original")
    
    # Test 9: Test feature validation
    print("\n9. Testing feature validation...")
    X_wrong = pd.DataFrame(np.random.rand(10, 5), columns=['a', 'b', 'c', 'd', 'e'])
    try:
        clf_model.predict(X_wrong)
        print("   [ERROR] Should have raised ValueError for wrong features")
        return False
    except ValueError:
        print("   [OK] Correctly validates feature names")
    
    # Test 10: Test metadata
    print("\n10. Testing metadata...")
    metadata = clf_model.get_metadata()
    assert 'n_samples' in metadata, "Metadata should include training info"
    assert metadata['is_trained'] == True, "Metadata should reflect training status"
    print(f"   [OK] Metadata keys: {list(metadata.keys())[:5]}...")
    
    # Test 11: Test scale_pos_weight
    print("\n11. Testing scale_pos_weight for class imbalance...")
    imbalanced_model = XGBoostModel("imbalanced_test", "classification", scale_pos_weight=0.8)
    assert imbalanced_model.scale_pos_weight == 0.8, "scale_pos_weight should be set"
    print(f"   [OK] scale_pos_weight set correctly: {imbalanced_model.scale_pos_weight}")
    
    print("\n" + "=" * 70)
    print("All XGBoostModel tests passed!")
    print("=" * 70)
    return True

if __name__ == "__main__":
    success = test_xgboost_model()
    sys.exit(0 if success else 1)




