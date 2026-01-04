"""Evaluation metrics for model training and comparison."""

import logging
from typing import Dict, Any, Optional, List, Tuple
import numpy as np
import pandas as pd
from sklearn.metrics import (
    # Classification
    accuracy_score, precision_score, recall_score, f1_score,
    roc_auc_score, log_loss, confusion_matrix, classification_report,
    # Regression
    mean_absolute_error, mean_squared_error, r2_score,
    mean_absolute_percentage_error
)

logger = logging.getLogger(__name__)


def calculate_classification_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    y_proba: Optional[np.ndarray] = None,
    prefix: str = ""
) -> Dict[str, float]:
    """
    Calculate comprehensive classification metrics.
    
    Args:
        y_true: True labels
        y_pred: Predicted labels
        y_proba: Predicted probabilities (optional, for ROC-AUC and log loss)
        prefix: Prefix for metric names (e.g., 'train', 'val', 'test')
        
    Returns:
        Dictionary of metrics
    """
    metrics = {}
    
    # Basic metrics
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, average='binary', zero_division=0)
    recall = recall_score(y_true, y_pred, average='binary', zero_division=0)
    f1 = f1_score(y_true, y_pred, average='binary', zero_division=0)
    
    metrics[f'{prefix}_accuracy'] = float(accuracy)
    metrics[f'{prefix}_precision'] = float(precision)
    metrics[f'{prefix}_recall'] = float(recall)
    metrics[f'{prefix}_f1'] = float(f1)
    
    # Advanced metrics (require probabilities)
    if y_proba is not None:
        try:
            # ROC-AUC requires probabilities for positive class
            if y_proba.ndim > 1 and y_proba.shape[1] == 2:
                roc_auc = roc_auc_score(y_true, y_proba[:, 1])
            else:
                roc_auc = roc_auc_score(y_true, y_proba)
            metrics[f'{prefix}_roc_auc'] = float(roc_auc)
        except ValueError:
            # ROC-AUC may fail if only one class present
            logger.warning(f"Could not calculate ROC-AUC for {prefix} set")
            metrics[f'{prefix}_roc_auc'] = None
        
        try:
            # Log loss
            if y_proba.ndim > 1 and y_proba.shape[1] == 2:
                logloss = log_loss(y_true, y_proba[:, 1])
            else:
                logloss = log_loss(y_true, y_proba)
            metrics[f'{prefix}_log_loss'] = float(logloss)
        except ValueError:
            logger.warning(f"Could not calculate log loss for {prefix} set")
            metrics[f'{prefix}_log_loss'] = None
    
    # Confusion matrix components
    cm = confusion_matrix(y_true, y_pred)
    if cm.shape == (2, 2):
        tn, fp, fn, tp = cm.ravel()
        metrics[f'{prefix}_true_positives'] = int(tp)
        metrics[f'{prefix}_true_negatives'] = int(tn)
        metrics[f'{prefix}_false_positives'] = int(fp)
        metrics[f'{prefix}_false_negatives'] = int(fn)
    
    return metrics


def calculate_regression_metrics(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    prefix: str = ""
) -> Dict[str, float]:
    """
    Calculate comprehensive regression metrics.
    
    Args:
        y_true: True target values
        y_pred: Predicted target values
        prefix: Prefix for metric names (e.g., 'train', 'val', 'test')
        
    Returns:
        Dictionary of metrics
    """
    metrics = {}
    
    # Basic metrics
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_true, y_pred)
    
    metrics[f'{prefix}_mae'] = float(mae)
    metrics[f'{prefix}_mse'] = float(mse)
    metrics[f'{prefix}_rmse'] = float(rmse)
    metrics[f'{prefix}_r2'] = float(r2)
    
    # Additional metrics
    try:
        mape = mean_absolute_percentage_error(y_true, y_pred)
        metrics[f'{prefix}_mape'] = float(mape)
    except (ValueError, ZeroDivisionError):
        # MAPE may fail if y_true contains zeros
        logger.warning(f"Could not calculate MAPE for {prefix} set")
        metrics[f'{prefix}_mape'] = None
    
    # Error statistics
    errors = y_pred - y_true
    metrics[f'{prefix}_mean_error'] = float(np.mean(errors))
    metrics[f'{prefix}_std_error'] = float(np.std(errors))
    metrics[f'{prefix}_max_error'] = float(np.max(np.abs(errors)))
    
    return metrics


def get_classification_report(
    y_true: np.ndarray,
    y_pred: np.ndarray,
    target_names: Optional[List[str]] = None
) -> str:
    """
    Get detailed classification report.
    
    Args:
        y_true: True labels
        y_pred: Predicted labels
        target_names: Optional names for classes
        
    Returns:
        Classification report string
    """
    if target_names is None:
        target_names = ['Away Win', 'Home Win']
    
    return classification_report(y_true, y_pred, target_names=target_names)


def compare_models(
    model_results: Dict[str, Dict[str, Any]],
    task_type: str = "classification",
    metric: Optional[str] = None
) -> pd.DataFrame:
    """
    Compare multiple models based on their evaluation metrics.
    
    Args:
        model_results: Dictionary mapping model names to their metrics dictionaries
        task_type: 'classification' or 'regression'
        metric: Specific metric to use for comparison (e.g., 'val_accuracy', 'val_rmse')
                If None, uses default metric for task type
        
    Returns:
        DataFrame with model comparison
    """
    if not model_results:
        return pd.DataFrame()
    
    # Determine comparison metric
    if metric is None:
        if task_type == "classification":
            # Try to find validation accuracy, fallback to test accuracy
            metric = 'val_accuracy'
            if metric not in list(model_results.values())[0]:
                metric = 'test_accuracy'
        else:
            # For regression, use RMSE (lower is better)
            metric = 'val_rmse'
            if metric not in list(model_results.values())[0]:
                metric = 'test_rmse'
    
    # Collect all metrics
    comparison_data = []
    for model_name, metrics in model_results.items():
        row = {'model_name': model_name}
        row.update(metrics)
        comparison_data.append(row)
    
    df = pd.DataFrame(comparison_data)
    
    # Sort by comparison metric (higher is better for accuracy, lower for RMSE)
    if metric in df.columns:
        ascending = 'rmse' in metric.lower() or 'mae' in metric.lower() or 'mape' in metric.lower()
        df = df.sort_values(metric, ascending=ascending)
    
    return df


def print_model_comparison(
    model_results: Dict[str, Dict[str, Any]],
    task_type: str = "classification",
    metric: Optional[str] = None
) -> None:
    """
    Print formatted model comparison.
    
    Args:
        model_results: Dictionary mapping model names to their metrics dictionaries
        task_type: 'classification' or 'regression'
        metric: Specific metric to use for comparison
    """
    df = compare_models(model_results, task_type, metric)
    
    if df.empty:
        logger.warning("No model results to compare")
        return
    
    print("\n" + "=" * 80)
    print("Model Comparison")
    print("=" * 80)
    
    # Select key metrics to display
    if task_type == "classification":
        key_metrics = ['val_accuracy', 'val_precision', 'val_recall', 'val_f1', 'val_roc_auc']
    else:
        key_metrics = ['val_rmse', 'val_mae', 'val_r2', 'val_mape']
    
    # Filter to available metrics
    display_metrics = ['model_name'] + [m for m in key_metrics if m in df.columns]
    display_df = df[display_metrics].copy()
    
    # Format numeric columns
    for col in display_df.columns:
        if col != 'model_name' and display_df[col].dtype in [np.float64, np.float32]:
            display_df[col] = display_df[col].apply(lambda x: f"{x:.4f}" if pd.notna(x) else "N/A")
    
    print(display_df.to_string(index=False))
    print("=" * 80)




