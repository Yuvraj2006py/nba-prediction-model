"""Model trainer for orchestrating training, hyperparameter tuning, and evaluation."""

import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
import numpy as np
import pandas as pd
from sklearn.model_selection import ParameterSampler

from src.training.data_loader import DataLoader
from src.training.metrics import (
    calculate_classification_metrics,
    calculate_regression_metrics,
    compare_models,
    print_model_comparison
)
from src.models.base_model import BaseModel
from config.settings import get_settings

logger = logging.getLogger(__name__)


class ModelTrainer:
    """
    Orchestrates model training, hyperparameter tuning, and evaluation.
    
    Handles:
    - Data loading and preprocessing
    - Model training (classification and regression)
    - Hyperparameter tuning (random search)
    - Model evaluation and comparison
    - Model persistence
    """
    
    def __init__(
        self,
        data_loader: Optional[DataLoader] = None,
        random_state: int = 42
    ):
        """
        Initialize model trainer.
        
        Args:
            data_loader: Optional DataLoader instance. If None, creates new one.
            random_state: Random seed for reproducibility
        """
        self.data_loader = data_loader or DataLoader()
        self.random_state = random_state
        self.settings = get_settings()
        self.trained_models: Dict[str, BaseModel] = {}
        self.training_results: Dict[str, Dict[str, Any]] = {}
        
        logger.info("ModelTrainer initialized")
    
    def train_model(
        self,
        model: BaseModel,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
        X_test: Optional[pd.DataFrame] = None,
        y_test: Optional[pd.Series] = None,
        save_model: bool = True,
        **train_kwargs
    ) -> Dict[str, Any]:
        """
        Train a single model and evaluate it.
        
        Args:
            model: Model instance to train
            X_train: Training features
            y_train: Training labels/targets
            X_val: Optional validation features
            y_val: Optional validation labels/targets
            X_test: Optional test features
            y_test: Optional test labels/targets
            save_model: Whether to save the trained model
            **train_kwargs: Additional training parameters
            
        Returns:
            Dictionary with training and evaluation results
        """
        logger.info(f"Training model: {model.model_name} ({model.task_type})")
        
        # Train the model
        train_metrics = model.train(
            X_train, y_train,
            X_val=X_val, y_val=y_val,
            **train_kwargs
        )
        
        # Evaluate on all available sets
        results = {
            'model_name': model.model_name,
            'task_type': model.task_type,
            'training_metrics': train_metrics,
            'train_samples': len(X_train),
            'val_samples': len(X_val) if X_val is not None else 0,
            'test_samples': len(X_test) if X_test is not None else 0,
        }
        
        # Evaluate on test set if available
        if X_test is not None and y_test is not None:
            test_pred = model.predict(X_test)
            
            if model.task_type == "classification":
                test_proba = model.predict(X_test, return_proba=True)[1]
                test_metrics = calculate_classification_metrics(
                    y_test.values, test_pred, test_proba, prefix="test"
                )
            else:
                test_metrics = calculate_regression_metrics(
                    y_test.values, test_pred, prefix="test"
                )
            
            results['test_metrics'] = test_metrics
            results.update(test_metrics)
        
        # Store model and results
        self.trained_models[model.model_name] = model
        self.training_results[model.model_name] = results
        
        # Save model if requested
        if save_model:
            model_path = model.save()
            results['model_path'] = str(model_path)
            logger.info(f"Model saved to {model_path}")
        
        return results
    
    def train_with_data_loader(
        self,
        model: BaseModel,
        train_seasons: Optional[List[str]] = None,
        val_seasons: Optional[List[str]] = None,
        test_seasons: Optional[List[str]] = None,
        save_model: bool = True,
        **train_kwargs
    ) -> Dict[str, Any]:
        """
        Train model using DataLoader to get data from database.
        
        Args:
            model: Model instance to train
            train_seasons: Seasons for training
            val_seasons: Seasons for validation
            test_seasons: Seasons for testing
            save_model: Whether to save the trained model
            **train_kwargs: Additional training parameters
            
        Returns:
            Dictionary with training and evaluation results
        """
        # Load data
        data = self.data_loader.load_all_data(
            train_seasons=train_seasons,
            val_seasons=val_seasons,
            test_seasons=test_seasons
        )
        
        # Select appropriate target based on task type
        if model.task_type == "classification":
            y_train = data['y_train_class']
            y_val = data['y_val_class'] if 'y_val_class' in data else None
            y_test = data['y_test_class'] if 'y_test_class' in data else None
        else:
            y_train = data['y_train_reg']
            y_val = data['y_val_reg'] if 'y_val_reg' in data else None
            y_test = data['y_test_reg'] if 'y_test_reg' in data else None
        
        # Handle class imbalance for classification
        if model.task_type == "classification" and 'class_imbalance_info' in data:
            imbalance_info = data['class_imbalance_info']
            if imbalance_info.get('is_imbalanced') and hasattr(model, 'scale_pos_weight'):
                if model.scale_pos_weight is None:
                    scale_pos_weight = imbalance_info.get('scale_pos_weight')
                    # Update model's scale_pos_weight if it's an XGBoost model
                    from src.models.xgboost_model import XGBoostModel
                    if isinstance(model, XGBoostModel):
                        model.scale_pos_weight = scale_pos_weight
                        # Update the underlying XGBoost model's parameter
                        if hasattr(model.model, 'set_params'):
                            model.model.set_params(scale_pos_weight=scale_pos_weight)
                        logger.info(f"Set scale_pos_weight={scale_pos_weight} for class imbalance")
        
        # Train model
        return self.train_model(
            model=model,
            X_train=data['X_train'],
            y_train=y_train,
            X_val=data.get('X_val'),
            y_val=y_val,
            X_test=data.get('X_test'),
            y_test=y_test,
            save_model=save_model,
            **train_kwargs
        )
    
    def hyperparameter_tuning(
        self,
        model_class: type,
        model_name_prefix: str,
        param_distributions: Dict[str, List[Any]],
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
        n_iter: int = 10,
        task_type: str = "classification",
        scoring_metric: Optional[str] = None,
        **base_params
    ) -> Tuple[BaseModel, Dict[str, Any]]:
        """
        Perform random search hyperparameter tuning.
        
        Args:
            model_class: Model class to instantiate
            model_name_prefix: Prefix for model names during tuning
            param_distributions: Dictionary of parameter distributions for random search
            X_train: Training features
            y_train: Training labels/targets
            X_val: Optional validation features
            y_val: Optional validation labels/targets
            n_iter: Number of random search iterations
            task_type: 'classification' or 'regression'
            scoring_metric: Metric to optimize (e.g., 'val_accuracy', 'val_rmse')
                          If None, uses default for task type
            **base_params: Base parameters to use for all models
            
        Returns:
            Tuple of (best_model, best_params)
        """
        logger.info(f"Starting hyperparameter tuning: {n_iter} iterations")
        
        # Determine scoring metric
        if scoring_metric is None:
            if task_type == "classification":
                scoring_metric = 'val_accuracy' if X_val is not None else 'train_accuracy'
            else:
                scoring_metric = 'val_rmse' if X_val is not None else 'train_rmse'
        
        # Determine if higher is better
        higher_is_better = 'accuracy' in scoring_metric or 'r2' in scoring_metric or 'f1' in scoring_metric
        
        best_score = float('-inf') if higher_is_better else float('inf')
        best_model = None
        best_params = None
        best_results = None
        
        # Generate random parameter combinations
        param_combinations = list(ParameterSampler(
            param_distributions,
            n_iter=n_iter,
            random_state=self.random_state
        ))
        
        logger.info(f"Testing {len(param_combinations)} parameter combinations...")
        
        for i, params in enumerate(param_combinations):
            # Combine base params with sampled params
            combined_params = {**base_params, **params}
            
            # Create model name
            model_name = f"{model_name_prefix}_tune_{i+1}"
            
            try:
                # Create and train model
                model = model_class(
                    model_name=model_name,
                    task_type=task_type,
                    **combined_params
                )
                
                # Train model
                train_results = model.train(
                    X_train, y_train,
                    X_val=X_val, y_val=y_val
                )
                
                # Get score
                if scoring_metric in train_results:
                    score = train_results[scoring_metric]
                else:
                    # Fallback to first available metric
                    available_metrics = [k for k in train_results.keys() if 'val_' in k or 'train_' in k]
                    if available_metrics:
                        score = train_results[available_metrics[0]]
                    else:
                        logger.warning(f"No suitable metric found for iteration {i+1}")
                        continue
                
                # Check if this is the best model
                is_better = (score > best_score) if higher_is_better else (score < best_score)
                if is_better:
                    best_score = score
                    best_model = model
                    best_params = combined_params
                    best_results = train_results
                    logger.info(f"Iteration {i+1}/{n_iter}: New best {scoring_metric}={score:.4f}")
                else:
                    logger.debug(f"Iteration {i+1}/{n_iter}: {scoring_metric}={score:.4f} (best: {best_score:.4f})")
                    
            except Exception as e:
                logger.warning(f"Iteration {i+1}/{n_iter} failed: {e}")
                continue
        
        if best_model is None:
            raise ValueError("No successful model training during hyperparameter tuning")
        
        # Rename best model
        best_model.model_name = f"{model_name_prefix}_best"
        
        # Store best model in trainer
        self.trained_models[best_model.model_name] = best_model
        
        # Store training results for best model
        best_model_results = {
            'model_name': best_model.model_name,
            'task_type': task_type,
            'training_metrics': best_results,
            'train_samples': len(X_train),
            'val_samples': len(X_val) if X_val is not None else 0,
        }
        self.training_results[best_model.model_name] = best_model_results
        
        logger.info(f"Hyperparameter tuning complete. Best {scoring_metric}: {best_score:.4f}")
        logger.info(f"Best parameters: {best_params}")
        
        return best_model, {
            'best_params': best_params,
            'best_score': best_score,
            'scoring_metric': scoring_metric,
            'training_results': best_results
        }
    
    def compare_trained_models(
        self,
        task_type: Optional[str] = None,
        metric: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Compare all trained models.
        
        Args:
            task_type: Filter by task type ('classification' or 'regression')
            metric: Specific metric to use for comparison
            
        Returns:
            DataFrame with model comparison
        """
        if not self.training_results:
            logger.warning("No trained models to compare")
            return pd.DataFrame()
        
        # Filter by task type if specified
        results_to_compare = self.training_results
        if task_type:
            results_to_compare = {
                name: results for name, results in results_to_compare.items()
                if results.get('task_type') == task_type
            }
        
        # Extract metrics for comparison
        metrics_dict = {}
        for model_name, results in results_to_compare.items():
            # Combine training and test metrics
            model_metrics = {}
            if 'training_metrics' in results:
                model_metrics.update(results['training_metrics'])
            if 'test_metrics' in results:
                model_metrics.update(results['test_metrics'])
            metrics_dict[model_name] = model_metrics
        
        # Determine task type from results if not specified
        if not task_type and metrics_dict:
            first_result = list(results_to_compare.values())[0]
            task_type = first_result.get('task_type', 'classification')
        
        return compare_models(metrics_dict, task_type, metric)
    
    def print_comparison(
        self,
        task_type: Optional[str] = None,
        metric: Optional[str] = None
    ) -> None:
        """
        Print formatted comparison of all trained models.
        
        Args:
            task_type: Filter by task type
            metric: Specific metric to use for comparison
        """
        if not self.training_results:
            logger.warning("No trained models to compare")
            return
        
        # Filter by task type if specified
        results_to_compare = self.training_results
        if task_type:
            results_to_compare = {
                name: results for name, results in results_to_compare.items()
                if results.get('task_type') == task_type
            }
        
        # Extract metrics for comparison
        metrics_dict = {}
        for model_name, results in results_to_compare.items():
            # Combine training and test metrics
            model_metrics = {}
            if 'training_metrics' in results:
                model_metrics.update(results['training_metrics'])
            if 'test_metrics' in results:
                model_metrics.update(results['test_metrics'])
            metrics_dict[model_name] = model_metrics
        
        # Determine task type from results if not specified
        if not task_type and metrics_dict:
            first_result = list(results_to_compare.values())[0]
            task_type = first_result.get('task_type', 'classification')
        
        print_model_comparison(metrics_dict, task_type, metric)
    
    def save_training_summary(
        self,
        filepath: Optional[Path] = None
    ) -> Path:
        """
        Save training summary to JSON file.
        
        Args:
            filepath: Optional filepath. If None, uses default location.
            
        Returns:
            Path where summary was saved
        """
        if filepath is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = Path(self.settings.MODELS_DIR) / f"training_summary_{timestamp}.json"
        else:
            filepath = Path(filepath)
        
        # Prepare summary data
        summary = {
            'timestamp': datetime.now().isoformat(),
            'models_trained': len(self.trained_models),
            'training_results': {}
        }
        
        for model_name, results in self.training_results.items():
            # Convert numpy types to native Python types for JSON serialization
            summary['training_results'][model_name] = self._convert_to_json_serializable(results)
        
        # Save to file
        filepath.parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(summary, f, indent=2, default=str)
        
        logger.info(f"Training summary saved to {filepath}")
        return filepath
    
    def _convert_to_json_serializable(self, obj: Any) -> Any:
        """Convert numpy types to JSON-serializable types."""
        if isinstance(obj, dict):
            return {k: self._convert_to_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            return [self._convert_to_json_serializable(item) for item in obj]
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Series):
            return obj.tolist()
        else:
            return obj

