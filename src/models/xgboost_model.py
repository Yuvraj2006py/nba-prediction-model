"""XGBoost model implementation for NBA game prediction."""

import logging
import json
from pathlib import Path
from typing import Any, Dict, Optional, Union, Tuple
import pandas as pd
import numpy as np
import joblib
from xgboost import XGBClassifier, XGBRegressor

from src.models.base_model import BaseModel
from config.settings import get_settings

logger = logging.getLogger(__name__)


class XGBoostModel(BaseModel):
    """
    XGBoost model for NBA game prediction.
    
    Supports both classification (win/loss) and regression (point differential) tasks.
    """
    
    def __init__(
        self,
        model_name: str,
        task_type: str = "classification",
        scale_pos_weight: Optional[float] = None,
        random_state: int = 42,
        **kwargs
    ):
        """
        Initialize XGBoost model.
        
        Args:
            model_name: Name identifier for this model
            task_type: 'classification' or 'regression'
            scale_pos_weight: Weight for positive class (classification only). 
                             If None, will be calculated from data if imbalanced.
            random_state: Random seed for reproducibility
            **kwargs: Additional XGBoost parameters
        """
        super().__init__(model_name, task_type)
        
        self.scale_pos_weight = scale_pos_weight
        self.random_state = random_state
        
        # Default XGBoost parameters
        # Updated with stronger regularization to prevent overfitting to streak features
        default_params = {
            'n_estimators': 100,
            'max_depth': 4,              # Reduced from 6 - simpler trees prevent overfitting
            'learning_rate': 0.05,       # Lower learning rate for more stable training
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'min_child_weight': 3,       # Increased from 1 - require more samples per leaf
            'gamma': 0.1,                # Minimum loss reduction for splits (prevents overfitting)
            'reg_alpha': 0.5,            # L1 regularization (was 0)
            'reg_lambda': 2.0,           # L2 regularization (was 1) - stronger penalty
            'random_state': random_state,
            'n_jobs': -1,
            'verbosity': 0
        }
        
        # Set eval_metric based on task type
        if task_type == "classification":
            default_params['eval_metric'] = 'logloss'
        else:
            default_params['eval_metric'] = 'rmse'
        
        # Update with any provided kwargs
        default_params.update(kwargs)
        self.params = default_params
        
        # Initialize model based on task type
        if task_type == "classification":
            if scale_pos_weight is not None:
                self.params['scale_pos_weight'] = scale_pos_weight
            self.model = XGBClassifier(**self.params)
        elif task_type == "regression":
            self.model = XGBRegressor(**self.params)
        else:
            raise ValueError(f"Invalid task_type: {task_type}. Must be 'classification' or 'regression'")
        
        # Set verbosity to reduce output during training
        if 'verbosity' not in kwargs:
            self.params['verbosity'] = 0
        
        logger.info(f"Initialized XGBoostModel '{model_name}' for {task_type} task")
    
    def train(
        self,
        X_train: Union[pd.DataFrame, np.ndarray],
        y_train: Union[pd.Series, np.ndarray],
        X_val: Optional[Union[pd.DataFrame, np.ndarray]] = None,
        y_val: Optional[Union[pd.Series, np.ndarray]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Train the XGBoost model.
        
        Args:
            X_train: Training features
            y_train: Training labels/targets
            X_val: Optional validation features
            y_val: Optional validation labels/targets
            **kwargs: Additional training parameters (e.g., early_stopping_rounds)
            
        Returns:
            Dictionary with training metrics and information
        """
        # Store feature names if DataFrame
        if isinstance(X_train, pd.DataFrame):
            self.set_feature_names(list(X_train.columns))
            # Convert to numpy for XGBoost
            X_train = X_train.values
            if X_val is not None and isinstance(X_val, pd.DataFrame):
                X_val = X_val.values
        
        # Convert y to numpy if needed
        if isinstance(y_train, pd.Series):
            y_train = y_train.values
        if y_val is not None and isinstance(y_val, pd.Series):
            y_val = y_val.values
        
        # Prepare training parameters
        fit_params = {}
        if X_val is not None and y_val is not None:
            fit_params['eval_set'] = [(X_val, y_val)]
            # Note: early_stopping_rounds is not supported in XGBoost 3.x fit() method
            # Remove it from kwargs if present (it was used in older versions)
            kwargs.pop('early_stopping_rounds', None)
        
        # Add any additional fit parameters
        fit_params.update(kwargs)
        
        logger.info(f"Training {self.task_type} model '{self.model_name}' on {len(X_train)} samples...")
        
        # Train the model
        self.model.fit(X_train, y_train, **fit_params)
        self.is_trained = True
        
        # Get training metrics
        train_pred = self.model.predict(X_train)
        train_metrics = self._calculate_metrics(y_train, train_pred, X_train, prefix="train")
        
        metrics = {
            'status': 'trained',
            'n_samples': len(X_train),
            'n_features': X_train.shape[1] if len(X_train.shape) > 1 else 1,
            **train_metrics
        }
        
        # Add validation metrics if available
        if X_val is not None and y_val is not None:
            val_pred = self.model.predict(X_val)
            val_metrics = self._calculate_metrics(y_val, val_pred, X_val, prefix="val")
            metrics.update(val_metrics)
        
        # Store training info in metadata
        self.update_metadata(
            n_samples=len(X_train),
            n_features=X_train.shape[1] if len(X_train.shape) > 1 else 1,
            training_completed=True
        )
        
        logger.info(f"Model '{self.model_name}' training completed. Train accuracy: {train_metrics.get('train_accuracy', 'N/A')}")
        
        return metrics
    
    def predict(
        self,
        X: Union[pd.DataFrame, np.ndarray],
        return_proba: bool = False
    ) -> Union[np.ndarray, Tuple[np.ndarray, np.ndarray]]:
        """
        Make predictions.
        
        Args:
            X: Features to predict on
            return_proba: If True, also return prediction probabilities (classification only)
            
        Returns:
            Predictions array, or tuple of (predictions, probabilities) if return_proba=True
        """
        self.validate_trained()
        
        # Validate features if feature names are set
        if self.feature_names is not None:
            self.validate_features(X)
        
        # Convert DataFrame to numpy
        if isinstance(X, pd.DataFrame):
            X = X.values
        
        # Make predictions
        predictions = self.model.predict(X)
        
        if return_proba:
            if self.task_type != "classification":
                raise ValueError("return_proba=True only supported for classification tasks")
            probabilities = self.model.predict_proba(X)
            return predictions, probabilities
        
        return predictions
    
    def save(self, filepath: Optional[Union[str, Path]] = None) -> Path:
        """
        Save the model to disk.
        
        Args:
            filepath: Optional custom filepath. If None, uses default location.
            
        Returns:
            Path where model was saved
        """
        self.validate_trained()
        
        # Determine filepath
        if filepath is None:
            filepath = self.get_default_save_path()
        else:
            filepath = Path(filepath)
        
        # Ensure directory exists
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Save model using joblib
        joblib.dump(self.model, filepath)
        logger.info(f"Saved model to {filepath}")
        
        # Save metadata
        metadata_path = self.get_metadata_path(filepath)
        metadata = self.get_metadata()
        metadata['model_type'] = 'xgboost'
        metadata['params'] = self.params
        metadata['scale_pos_weight'] = self.scale_pos_weight
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2, default=str)
        
        logger.info(f"Saved metadata to {metadata_path}")
        
        return filepath
    
    def load(self, filepath: Union[str, Path]) -> 'XGBoostModel':
        """
        Load the model from disk.
        
        Args:
            filepath: Path to saved model file
            
        Returns:
            Self for method chaining
        """
        filepath = Path(filepath)
        
        if not filepath.exists():
            raise FileNotFoundError(f"Model file not found: {filepath}")
        
        # Load model
        self.model = joblib.load(filepath)
        self.is_trained = True
        logger.info(f"Loaded model from {filepath}")
        
        # Load metadata if available
        metadata_path = self.get_metadata_path(filepath)
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            
            # Restore metadata (exclude special fields that are handled separately)
            self.metadata = {k: v for k, v in metadata.items() 
                           if k not in ['model_name', 'task_type', 'is_trained', 'feature_count', 
                                       'feature_names', 'model_type', 'params', 'scale_pos_weight']}
            
            # Restore parameters if available
            if 'params' in metadata:
                self.params = metadata['params']
            if 'scale_pos_weight' in metadata:
                self.scale_pos_weight = metadata['scale_pos_weight']
            
            # CRITICAL: Restore feature names for schema validation
            if 'feature_names' in metadata and metadata['feature_names']:
                self.set_feature_names(metadata['feature_names'])
                logger.info(f"Loaded {len(self.feature_names)} feature names from metadata")
            else:
                logger.warning(f"No feature_names in metadata - schema validation disabled")
            
            logger.info(f"Loaded metadata from {metadata_path}")
        
        return self
    
    def _calculate_metrics(
        self,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        X: Optional[np.ndarray] = None,
        prefix: str = ""
    ) -> Dict[str, Any]:
        """
        Calculate evaluation metrics.
        
        Args:
            y_true: True labels/targets
            y_pred: Predicted labels/targets
            X: Features (optional, for feature importance)
            prefix: Prefix for metric names (e.g., 'train', 'val')
            
        Returns:
            Dictionary of metrics
        """
        metrics = {}
        
        if self.task_type == "classification":
            from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
            
            accuracy = accuracy_score(y_true, y_pred)
            precision = precision_score(y_true, y_pred, average='binary', zero_division=0)
            recall = recall_score(y_true, y_pred, average='binary', zero_division=0)
            f1 = f1_score(y_true, y_pred, average='binary', zero_division=0)
            
            metrics[f'{prefix}_accuracy'] = float(accuracy)
            metrics[f'{prefix}_precision'] = float(precision)
            metrics[f'{prefix}_recall'] = float(recall)
            metrics[f'{prefix}_f1'] = float(f1)
            
        else:  # regression
            from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
            
            mae = mean_absolute_error(y_true, y_pred)
            mse = mean_squared_error(y_true, y_pred)
            rmse = np.sqrt(mse)
            r2 = r2_score(y_true, y_pred)
            
            metrics[f'{prefix}_mae'] = float(mae)
            metrics[f'{prefix}_mse'] = float(mse)
            metrics[f'{prefix}_rmse'] = float(rmse)
            metrics[f'{prefix}_r2'] = float(r2)
        
        # Add feature importance if available
        if X is not None and hasattr(self.model, 'feature_importances_'):
            importances = self.model.feature_importances_
            if self.feature_names is not None:
                # Create feature importance dict
                importance_dict = dict(zip(self.feature_names, importances))
                top_features = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)[:10]
                metrics[f'{prefix}_top_features'] = {name: float(imp) for name, imp in top_features}
        
        return metrics

