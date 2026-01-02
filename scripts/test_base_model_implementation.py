"""Test that a concrete implementation of BaseModel works correctly."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import numpy as np
import pandas as pd
from src.models.base_model import BaseModel
from config.settings import get_settings

class DummyModel(BaseModel):
    """Dummy concrete implementation for testing."""
    
    def __init__(self, model_name: str = "dummy_test", task_type: str = "classification"):
        super().__init__(model_name, task_type)
        self.model = None
    
    def train(self, X_train, y_train, X_val=None, y_val=None, **kwargs):
        """Dummy train method."""
        self.model = "trained"
        self.is_trained = True
        if isinstance(X_train, pd.DataFrame):
            self.set_feature_names(list(X_train.columns))
        return {"status": "trained", "samples": len(X_train)}
    
    def predict(self, X, return_proba=False):
        """Dummy predict method."""
        self.validate_trained()
        n_samples = len(X) if hasattr(X, '__len__') else 1
        predictions = np.array([1] * n_samples)
        if return_proba:
            proba = np.array([[0.3, 0.7]] * n_samples)
            return predictions, proba
        return predictions
    
    def save(self, filepath=None):
        """Dummy save method."""
        self.validate_trained()
        path = filepath or self.get_default_save_path()
        # Just verify path is correct
        assert path.parent.exists(), f"Models directory should exist: {path.parent}"
        return path
    
    def load(self, filepath):
        """Dummy load method."""
        path = Path(filepath)
        assert path.exists() or path.parent.exists(), f"Path should exist: {path}"
        self.is_trained = True
        return self

def test_concrete_implementation():
    """Test that a concrete implementation works."""
    print("=" * 70)
    print("Testing Concrete BaseModel Implementation")
    print("=" * 70)
    
    # Test 1: Create instance
    print("\n1. Creating concrete model instance...")
    model = DummyModel("test_model", "classification")
    print(f"   [OK] Created: {model}")
    assert not model.is_trained, "Model should not be trained initially"
    
    # Test 2: Check settings integration
    print("\n2. Testing settings integration...")
    settings = get_settings()
    assert model.models_dir == Path(settings.MODELS_DIR), "Models directory should match settings"
    print(f"   [OK] Models directory: {model.models_dir}")
    
    # Test 3: Test training
    print("\n3. Testing training...")
    X_train = pd.DataFrame(np.random.rand(10, 5), columns=[f'feature_{i}' for i in range(5)])
    y_train = pd.Series([0, 1] * 5)
    result = model.train(X_train, y_train)
    assert model.is_trained, "Model should be trained after train()"
    assert model.feature_names == list(X_train.columns), "Feature names should be set"
    print(f"   [OK] Training result: {result}")
    
    # Test 4: Test prediction
    print("\n4. Testing prediction...")
    X_test = pd.DataFrame(np.random.rand(3, 5), columns=X_train.columns)
    predictions = model.predict(X_test)
    assert len(predictions) == 3, "Should return 3 predictions"
    print(f"   [OK] Predictions shape: {predictions.shape}")
    
    # Test 5: Test prediction with probabilities
    print("\n5. Testing prediction with probabilities...")
    pred, proba = model.predict(X_test, return_proba=True)
    assert len(pred) == 3, "Should return 3 predictions"
    assert proba.shape == (3, 2), "Should return probability matrix"
    print(f"   [OK] Predictions and probabilities shape: {pred.shape}, {proba.shape}")
    
    # Test 6: Test feature validation (manual call)
    print("\n6. Testing feature validation...")
    X_wrong = pd.DataFrame(np.random.rand(3, 3), columns=['a', 'b', 'c'])
    try:
        model.validate_features(X_wrong)
        print("   [ERROR] Should have raised ValueError for wrong features")
        return False
    except ValueError:
        print("   [OK] Correctly validates feature names when called")
    
    # Test that correct features pass validation
    X_correct = pd.DataFrame(np.random.rand(3, 5), columns=X_train.columns)
    try:
        model.validate_features(X_correct)
        print("   [OK] Correct features pass validation")
    except ValueError:
        print("   [ERROR] Correct features should pass validation")
        return False
    
    # Test 7: Test metadata
    print("\n7. Testing metadata...")
    model.update_metadata(version="1.0", accuracy=0.95)
    metadata = model.get_metadata()
    assert metadata['version'] == "1.0", "Metadata should be updated"
    assert metadata['is_trained'] == True, "Metadata should reflect training status"
    print(f"   [OK] Metadata: {list(metadata.keys())}")
    
    # Test 8: Test save path
    print("\n8. Testing save path...")
    save_path = model.get_default_save_path()
    assert save_path.parent == model.models_dir, "Save path should be in models directory"
    assert save_path.name == "test_model.pkl", "Save path should have correct filename"
    print(f"   [OK] Default save path: {save_path}")
    
    # Test 9: Test metadata path
    print("\n9. Testing metadata path...")
    metadata_path = model.get_metadata_path()
    assert metadata_path.suffix == '.json', "Metadata path should be JSON"
    assert metadata_path.stem == save_path.stem, "Metadata path should match model name"
    print(f"   [OK] Metadata path: {metadata_path}")
    
    # Test 10: Test save (without actually saving)
    print("\n10. Testing save method...")
    save_path = model.save()
    assert save_path.parent.exists(), "Models directory should exist"
    print(f"   [OK] Save path validated: {save_path}")
    
    print("\n" + "=" * 70)
    print("All implementation tests passed!")
    print("=" * 70)
    return True

if __name__ == "__main__":
    success = test_concrete_implementation()
    sys.exit(0 if success else 1)

