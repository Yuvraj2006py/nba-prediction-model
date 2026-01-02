"""Base model interface for all ML models in the NBA prediction system."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional, Union
import pandas as pd
import numpy as np

from config.settings import get_settings

logger = logging.getLogger(__name__)


class BaseModel(ABC):
    """
    Abstract base class for all machine learning models.
    
    This class defines the interface that all model implementations must follow,
    ensuring consistency across different model types (XGBoost, LightGBM, etc.).
    """
    
    def __init__(self, model_name: str, task_type: str = "classification"):
        """
        Initialize base model.
        
        Args:
            model_name: Name identifier for this model (e.g., 'xgboost_classifier')
            task_type: Type of task - 'classification' or 'regression'
        """
        self.model_name = model_name
        self.task_type = task_type
        self.settings = get_settings()
        self.model = None  # Will be set by subclasses
        self.is_trained = False
        self.feature_names: Optional[list] = None
        self.metadata: Dict[str, Any] = {}
        
        # Ensure models directory exists
        self.models_dir = Path(self.settings.MODELS_DIR)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        logger.debug(f"Initialized {self.__class__.__name__} with name '{model_name}' for {task_type}")
    
    @abstractmethod
    def train(
        self,
        X_train: Union[pd.DataFrame, np.ndarray],
        y_train: Union[pd.Series, np.ndarray],
        X_val: Optional[Union[pd.DataFrame, np.ndarray]] = None,
        y_val: Optional[Union[pd.Series, np.ndarray]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Train the model.
        
        Args:
            X_train: Training features
            y_train: Training labels/targets
            X_val: Optional validation features
            y_val: Optional validation labels/targets
            **kwargs: Additional training parameters
            
        Returns:
            Dictionary with training metrics and information
        """
        pass
    
    @abstractmethod
    def predict(
        self,
        X: Union[pd.DataFrame, np.ndarray],
        return_proba: bool = False
    ) -> Union[np.ndarray, tuple]:
        """
        Make predictions.
        
        Args:
            X: Features to predict on
            return_proba: If True, also return prediction probabilities (classification only)
            
        Returns:
            Predictions array, or tuple of (predictions, probabilities) if return_proba=True
        """
        pass
    
    @abstractmethod
    def save(self, filepath: Optional[Union[str, Path]] = None) -> Path:
        """
        Save the model to disk.
        
        Args:
            filepath: Optional custom filepath. If None, uses default location.
            
        Returns:
            Path where model was saved
        """
        pass
    
    @abstractmethod
    def load(self, filepath: Union[str, Path]) -> 'BaseModel':
        """
        Load the model from disk.
        
        Args:
            filepath: Path to saved model file
            
        Returns:
            Self for method chaining
        """
        pass
    
    def get_default_save_path(self) -> Path:
        """
        Get default filepath for saving this model.
        
        Returns:
            Path object for model file
        """
        filename = f"{self.model_name}.pkl"
        return self.models_dir / filename
    
    def get_metadata_path(self, model_path: Optional[Path] = None) -> Path:
        """
        Get path for model metadata JSON file.
        
        Args:
            model_path: Optional model file path. If None, uses default.
            
        Returns:
            Path object for metadata file
        """
        if model_path is None:
            model_path = self.get_default_save_path()
        return model_path.with_suffix('.json')
    
    def validate_trained(self) -> None:
        """
        Check if model is trained, raise error if not.
        
        Raises:
            ValueError: If model is not trained
        """
        if not self.is_trained:
            raise ValueError(f"Model '{self.model_name}' has not been trained yet. Call train() first.")
    
    def validate_features(self, X: Union[pd.DataFrame, np.ndarray]) -> None:
        """
        Validate that input features match expected feature names (if available).
        
        Args:
            X: Features to validate
            
        Raises:
            ValueError: If feature names don't match
        """
        if self.feature_names is None:
            return
        
        if isinstance(X, pd.DataFrame):
            if list(X.columns) != self.feature_names:
                raise ValueError(
                    f"Feature names don't match. Expected {len(self.feature_names)} features, "
                    f"got {len(X.columns)}. Expected: {self.feature_names[:5]}..., "
                    f"Got: {list(X.columns)[:5]}..."
                )
    
    def set_feature_names(self, feature_names: list) -> None:
        """
        Store feature names for validation.
        
        Args:
            feature_names: List of feature names
        """
        self.feature_names = feature_names
        logger.debug(f"Set {len(feature_names)} feature names for model '{self.model_name}'")
    
    def update_metadata(self, **kwargs) -> None:
        """
        Update model metadata.
        
        Args:
            **kwargs: Key-value pairs to add/update in metadata
        """
        self.metadata.update(kwargs)
        logger.debug(f"Updated metadata for model '{self.model_name}': {list(kwargs.keys())}")
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get current model metadata.
        
        Returns:
            Dictionary of metadata including feature_names for schema contract
        """
        return {
            'model_name': self.model_name,
            'task_type': self.task_type,
            'is_trained': self.is_trained,
            'feature_count': len(self.feature_names) if self.feature_names else None,
            'feature_names': self.feature_names,  # CRITICAL: Persist feature schema for inference
            **self.metadata
        }
    
    def __repr__(self) -> str:
        """String representation of model."""
        status = "trained" if self.is_trained else "untrained"
        return f"{self.__class__.__name__}(name='{self.model_name}', task='{self.task_type}', status='{status}')"



