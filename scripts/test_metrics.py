"""Test script for metrics module."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import numpy as np
import pandas as pd
from src.training.metrics import (
    calculate_classification_metrics,
    calculate_regression_metrics,
    compare_models,
    print_model_comparison,
    get_classification_report
)

def test_metrics():
    """Test metrics module functionality."""
    print("=" * 70)
    print("Testing Metrics Module")
    print("=" * 70)
    
    # Test 1: Classification metrics
    print("\n1. Testing classification metrics...")
    y_true_clf = np.array([0, 1, 1, 0, 1, 0, 0, 1, 1, 0])
    y_pred_clf = np.array([0, 1, 0, 0, 1, 1, 0, 1, 1, 0])
    y_proba_clf = np.array([[0.8, 0.2], [0.2, 0.8], [0.6, 0.4], [0.7, 0.3],
                            [0.3, 0.7], [0.4, 0.6], [0.9, 0.1], [0.1, 0.9],
                            [0.2, 0.8], [0.85, 0.15]])
    
    metrics_clf = calculate_classification_metrics(y_true_clf, y_pred_clf, y_proba_clf, prefix="test")
    
    assert 'test_accuracy' in metrics_clf, "Should have accuracy"
    assert 'test_precision' in metrics_clf, "Should have precision"
    assert 'test_recall' in metrics_clf, "Should have recall"
    assert 'test_f1' in metrics_clf, "Should have F1"
    assert 'test_roc_auc' in metrics_clf, "Should have ROC-AUC"
    assert 'test_log_loss' in metrics_clf, "Should have log loss"
    
    print(f"   [OK] Classification metrics calculated")
    print(f"       Accuracy: {metrics_clf['test_accuracy']:.3f}")
    print(f"       Precision: {metrics_clf['test_precision']:.3f}")
    print(f"       Recall: {metrics_clf['test_recall']:.3f}")
    print(f"       F1: {metrics_clf['test_f1']:.3f}")
    print(f"       ROC-AUC: {metrics_clf['test_roc_auc']:.3f}")
    
    # Test 2: Regression metrics
    print("\n2. Testing regression metrics...")
    y_true_reg = np.array([10.5, 5.2, -3.1, 8.7, -2.3, 12.1, 4.5, -1.2])
    y_pred_reg = np.array([10.2, 5.5, -2.8, 9.1, -2.0, 11.8, 4.8, -1.5])
    
    metrics_reg = calculate_regression_metrics(y_true_reg, y_pred_reg, prefix="test")
    
    assert 'test_mae' in metrics_reg, "Should have MAE"
    assert 'test_mse' in metrics_reg, "Should have MSE"
    assert 'test_rmse' in metrics_reg, "Should have RMSE"
    assert 'test_r2' in metrics_reg, "Should have R2"
    assert 'test_mean_error' in metrics_reg, "Should have mean error"
    
    print(f"   [OK] Regression metrics calculated")
    print(f"       MAE: {metrics_reg['test_mae']:.3f}")
    print(f"       RMSE: {metrics_reg['test_rmse']:.3f}")
    print(f"       R2: {metrics_reg['test_r2']:.3f}")
    
    # Test 3: Classification report
    print("\n3. Testing classification report...")
    report = get_classification_report(y_true_clf, y_pred_clf)
    assert len(report) > 0, "Report should not be empty"
    print(f"   [OK] Classification report generated")
    
    # Test 4: Model comparison
    print("\n4. Testing model comparison...")
    model_results = {
        'model_1': {
            'val_accuracy': 0.75,
            'val_precision': 0.72,
            'val_recall': 0.70,
            'val_f1': 0.71,
            'test_accuracy': 0.73
        },
        'model_2': {
            'val_accuracy': 0.80,
            'val_precision': 0.78,
            'val_recall': 0.75,
            'val_f1': 0.76,
            'test_accuracy': 0.78
        },
        'model_3': {
            'val_accuracy': 0.77,
            'val_precision': 0.74,
            'val_recall': 0.73,
            'val_f1': 0.73,
            'test_accuracy': 0.75
        }
    }
    
    comparison_df = compare_models(model_results, task_type="classification")
    assert not comparison_df.empty, "Comparison should not be empty"
    assert len(comparison_df) == 3, "Should have 3 models"
    assert comparison_df.iloc[0]['model_name'] == 'model_2', "Best model should be first"
    print(f"   [OK] Model comparison works: {len(comparison_df)} models")
    
    # Test 5: Print comparison
    print("\n5. Testing print comparison...")
    try:
        print_model_comparison(model_results, task_type="classification")
        print(f"   [OK] Print comparison works")
    except Exception as e:
        print(f"   [ERROR] Print comparison failed: {e}")
        return False
    
    # Test 6: Regression comparison
    print("\n6. Testing regression comparison...")
    reg_results = {
        'model_1': {
            'val_rmse': 12.5,
            'val_mae': 10.2,
            'val_r2': 0.65,
            'test_rmse': 13.1
        },
        'model_2': {
            'val_rmse': 11.2,
            'val_mae': 9.5,
            'val_r2': 0.72,
            'test_rmse': 11.8
        }
    }
    
    reg_comparison = compare_models(reg_results, task_type="regression")
    assert not reg_comparison.empty, "Regression comparison should not be empty"
    assert reg_comparison.iloc[0]['model_name'] == 'model_2', "Best model (lowest RMSE) should be first"
    print(f"   [OK] Regression comparison works")
    
    # Test 7: Edge cases
    print("\n7. Testing edge cases...")
    # Test with single class (should handle gracefully)
    y_single = np.array([1, 1, 1, 1, 1])
    y_pred_single = np.array([1, 1, 1, 1, 1])
    try:
        metrics_single = calculate_classification_metrics(y_single, y_pred_single, prefix="test")
        print(f"   [OK] Single class handled: accuracy={metrics_single.get('test_accuracy', 'N/A')}")
    except Exception as e:
        print(f"   [WARNING] Single class test: {e}")
    
    # Test with empty results
    empty_comparison = compare_models({}, task_type="classification")
    assert empty_comparison.empty, "Empty comparison should be empty"
    print(f"   [OK] Empty results handled")
    
    print("\n" + "=" * 70)
    print("All metrics tests passed!")
    print("=" * 70)
    return True

if __name__ == "__main__":
    success = test_metrics()
    sys.exit(0 if success else 1)




