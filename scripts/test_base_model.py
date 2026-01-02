"""Test script to verify BaseModel interface works correctly."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.models.base_model import BaseModel
from abc import ABC

def test_base_model():
    """Test that BaseModel can be imported and has correct structure."""
    print("=" * 70)
    print("Testing BaseModel Interface")
    print("=" * 70)
    
    # Test 1: Import works
    print("\n1. Testing import...")
    assert BaseModel is not None, "BaseModel should be importable"
    print("   [OK] BaseModel imported successfully")
    
    # Test 2: Check it's an ABC
    print("\n2. Testing abstract base class...")
    assert issubclass(BaseModel, ABC), "BaseModel should inherit from ABC"
    print("   [OK] BaseModel is an abstract base class")
    
    # Test 3: Check abstract methods exist
    print("\n3. Testing abstract methods...")
    required_methods = ['train', 'predict', 'save', 'load']
    for method in required_methods:
        assert hasattr(BaseModel, method), f"BaseModel should have {method} method"
        assert getattr(BaseModel, method).__isabstractmethod__, f"{method} should be abstract"
    print(f"   [OK] All required abstract methods present: {required_methods}")
    
    # Test 4: Check concrete methods exist
    print("\n4. Testing concrete methods...")
    concrete_methods = ['get_default_save_path', 'get_metadata_path', 'validate_trained', 
                       'set_feature_names', 'update_metadata', 'get_metadata', '__repr__']
    for method in concrete_methods:
        assert hasattr(BaseModel, method), f"BaseModel should have {method} method"
    print(f"   [OK] All concrete helper methods present")
    
    # Test 5: Try to instantiate (should fail since it's abstract)
    print("\n5. Testing abstract instantiation...")
    try:
        model = BaseModel("test", "classification")
        print("   [ERROR] Should not be able to instantiate abstract BaseModel")
        return False
    except TypeError as e:
        if "abstract" in str(e).lower():
            print("   [OK] Correctly prevents instantiation of abstract class")
        else:
            print(f"   [ERROR] Unexpected error: {e}")
            return False
    
    print("\n" + "=" * 70)
    print("All tests passed! BaseModel interface is correctly implemented.")
    print("=" * 70)
    return True

if __name__ == "__main__":
    success = test_base_model()
    sys.exit(0 if success else 1)

