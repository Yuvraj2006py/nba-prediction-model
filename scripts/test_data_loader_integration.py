#!/usr/bin/env python
"""
Test the data loader with the new exponential decay features.

Usage:
    python scripts/test_data_loader_integration.py
"""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.training.data_loader import DataLoader
from src.database.db_manager import DatabaseManager
from config.settings import get_settings


def test_data_loader():
    """Test the data loader with new features."""
    print("=" * 60)
    print("DATA LOADER INTEGRATION TEST")
    print("=" * 60)
    
    settings = get_settings()
    print(f"Decay rate: {settings.ROLLING_STATS_DECAY_RATE}")
    
    try:
        loader = DataLoader()
        
        # Try to load data for 2025-26 season
        data = loader.load_all_data(
            train_seasons=['2024-25'],
            val_seasons=['2024-25'],
            test_seasons=['2025-26'],
            min_features=20
        )
        
        print("\n" + "-" * 60)
        print("DATA LOADING RESULTS")
        print("-" * 60)
        
        print(f"\nTraining:")
        print(f"  Games: {len(data['X_train'])}")
        print(f"  Features: {len(data['X_train'].columns) if len(data['X_train']) > 0 else 0}")
        
        print(f"\nValidation:")
        print(f"  Games: {len(data['X_val'])}")
        print(f"  Features: {len(data['X_val'].columns) if len(data['X_val']) > 0 else 0}")
        
        print(f"\nTest:")
        print(f"  Games: {len(data['X_test'])}")
        print(f"  Features: {len(data['X_test'].columns) if len(data['X_test']) > 0 else 0}")
        
        # Check for rolling features
        if len(data['X_test']) > 0:
            rolling_cols = [c for c in data['X_test'].columns if 'l5_' in c or 'l10_' in c or 'l20_' in c]
            print(f"\n  Rolling stat features: {len(rolling_cols)}")
            
            # Sample values
            if rolling_cols:
                print("\n  Sample rolling feature values:")
                for col in rolling_cols[:5]:
                    val = data['X_test'][col].iloc[0]
                    print(f"    {col}: {val}")
        
        print("\n" + "-" * 60)
        print("[PASS] Data loader integration test passed")
        print("-" * 60)
        return True
        
    except Exception as e:
        print(f"\n[FAIL] Data loader test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_data_loader()
    sys.exit(0 if success else 1)

