"""Test script to verify data loader works with real data."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import logging
from src.training.data_loader import DataLoader
from src.database.db_manager import DatabaseManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def test_data_loader():
    """Test data loader with real database."""
    logger.info("=" * 70)
    logger.info("Testing Data Loader")
    logger.info("=" * 70)
    
    db_manager = DatabaseManager()
    loader = DataLoader(db_manager=db_manager)
    
    # Load data
    try:
        data = loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        # Print summary
        print("\n" + "=" * 70)
        print("DATA LOADER TEST RESULTS")
        print("=" * 70)
        print(f"\nTraining Data:")
        print(f"  Games: {len(data['X_train'])}")
        print(f"  Features: {len(data['X_train'].columns) if len(data['X_train']) > 0 else 0}")
        print(f"  Home wins: {data['class_imbalance_info']['train_home_wins']}")
        print(f"  Away wins: {data['class_imbalance_info']['train_away_wins']}")
        
        print(f"\nValidation Data:")
        print(f"  Games: {len(data['X_val'])}")
        print(f"  Features: {len(data['X_val'].columns) if len(data['X_val']) > 0 else 0}")
        print(f"  Home wins: {data['class_imbalance_info']['val_home_wins']}")
        print(f"  Away wins: {data['class_imbalance_info']['val_away_wins']}")
        
        print(f"\nTest Data:")
        print(f"  Games: {len(data['X_test'])}")
        print(f"  Features: {len(data['X_test'].columns) if len(data['X_test']) > 0 else 0}")
        print(f"  Home wins: {data['class_imbalance_info']['test_home_wins']}")
        print(f"  Away wins: {data['class_imbalance_info']['test_away_wins']}")
        
        print(f"\nClass Imbalance:")
        print(f"  Overall home win rate: {data['class_imbalance_info']['overall_home_win_rate']*100:.1f}%")
        print(f"  Is imbalanced: {data['class_imbalance_info']['is_imbalanced']}")
        if data['class_imbalance_info']['scale_pos_weight']:
            print(f"  Scale pos weight: {data['class_imbalance_info']['scale_pos_weight']:.3f}")
        
        # Check for missing values
        if len(data['X_train']) > 0:
            missing_train = data['X_train'].isnull().sum().sum()
            print(f"\nMissing Values:")
            print(f"  Training: {missing_train}")
            print(f"  Validation: {data['X_val'].isnull().sum().sum()}")
            print(f"  Test: {data['X_test'].isnull().sum().sum()}")
        
        # Show sample features
        if len(data['X_train']) > 0:
            print(f"\nSample Features (first 10):")
            print(data['X_train'].columns[:10].tolist())
        
        print("\n" + "=" * 70)
        
        # Verify data integrity
        if len(data['X_train']) > 0:
            print("\n✓ Data loader working correctly!")
            print(f"✓ Loaded {len(data['X_train']) + len(data['X_val']) + len(data['X_test'])} total games")
            print(f"✓ {len(data['feature_names'])} features per game")
            return True
        else:
            print("\nWarning: No training data loaded. Check database.")
            return False
            
    except Exception as e:
        logger.error(f"Error testing data loader: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    success = test_data_loader()
    sys.exit(0 if success else 1)

