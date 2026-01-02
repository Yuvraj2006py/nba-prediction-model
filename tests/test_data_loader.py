"""Unit tests for DataLoader."""

import unittest
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

import pandas as pd
import numpy as np
from src.training.data_loader import DataLoader
from src.database.db_manager import DatabaseManager


class TestDataLoader(unittest.TestCase):
    """Test cases for DataLoader."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for the class."""
        cls.db_manager = DatabaseManager()
        cls.loader = DataLoader(db_manager=cls.db_manager)
    
    def test_init(self):
        """Test data loader initialization."""
        loader = DataLoader()
        self.assertIsNotNone(loader)
        self.assertIsNotNone(loader.db_manager)
    
    def test_load_all_data_structure(self):
        """Test that load_all_data returns correct structure."""
        data = self.loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        # Check all required keys exist
        required_keys = [
            'X_train', 'y_train_class', 'y_train_reg', 'game_ids_train',
            'X_val', 'y_val_class', 'y_val_reg', 'game_ids_val',
            'X_test', 'y_test_class', 'y_test_reg', 'game_ids_test',
            'feature_names', 'class_imbalance_info'
        ]
        
        for key in required_keys:
            self.assertIn(key, data, f"Missing key: {key}")
    
    def test_load_all_data_types(self):
        """Test that data types are correct."""
        data = self.loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        # Check types
        self.assertIsInstance(data['X_train'], pd.DataFrame)
        self.assertIsInstance(data['y_train_class'], pd.Series)
        self.assertIsInstance(data['y_train_reg'], pd.Series)
        self.assertIsInstance(data['game_ids_train'], list)
        
        self.assertIsInstance(data['X_val'], pd.DataFrame)
        self.assertIsInstance(data['y_val_class'], pd.Series)
        self.assertIsInstance(data['y_val_reg'], pd.Series)
        
        self.assertIsInstance(data['X_test'], pd.DataFrame)
        self.assertIsInstance(data['y_test_class'], pd.Series)
        self.assertIsInstance(data['y_test_reg'], pd.Series)
    
    def test_load_all_data_shapes(self):
        """Test that data shapes are consistent."""
        data = self.loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        # Check shapes match
        train_size = len(data['X_train'])
        self.assertEqual(len(data['y_train_class']), train_size)
        self.assertEqual(len(data['y_train_reg']), train_size)
        self.assertEqual(len(data['game_ids_train']), train_size)
        
        val_size = len(data['X_val'])
        self.assertEqual(len(data['y_val_class']), val_size)
        self.assertEqual(len(data['y_val_reg']), val_size)
        self.assertEqual(len(data['game_ids_val']), val_size)
        
        test_size = len(data['X_test'])
        self.assertEqual(len(data['y_test_class']), test_size)
        self.assertEqual(len(data['y_test_reg']), test_size)
        self.assertEqual(len(data['game_ids_test']), test_size)
        
        # Check feature counts match
        self.assertEqual(len(data['X_train'].columns), len(data['X_val'].columns))
        self.assertEqual(len(data['X_train'].columns), len(data['X_test'].columns))
        self.assertEqual(len(data['X_train'].columns), len(data['feature_names']))
    
    def test_target_variables(self):
        """Test that target variables are correct."""
        data = self.loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        # Only test if we have data
        if len(data['y_train_class']) > 0:
            # Classification target should be binary (0 or 1)
            y_class = data['y_train_class']
            self.assertTrue(y_class.isin([0, 1]).all(), "Classification target must be binary")
            
            # Regression target should be numeric
            y_reg = data['y_train_reg']
            self.assertTrue(pd.api.types.is_numeric_dtype(y_reg), "Regression target must be numeric")
            
            # Check reasonable range for point differential (typically -50 to +50)
            self.assertGreater(y_reg.max(), -100)
            self.assertLess(y_reg.max(), 100)
            self.assertGreater(y_reg.min(), -100)
            self.assertLess(y_reg.min(), 100)
        else:
            # If no data, just check types are correct
            self.assertIsInstance(data['y_train_class'], pd.Series)
            self.assertIsInstance(data['y_train_reg'], pd.Series)
    
    def test_missing_values_handled(self):
        """Test that missing values are handled."""
        data = self.loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        # Check no NaN values remain
        self.assertEqual(data['X_train'].isnull().sum().sum(), 0, "Training data should have no NaN values")
        self.assertEqual(data['X_val'].isnull().sum().sum(), 0, "Validation data should have no NaN values")
        self.assertEqual(data['X_test'].isnull().sum().sum(), 0, "Test data should have no NaN values")
    
    def test_class_imbalance_info(self):
        """Test that class imbalance info is correct."""
        data = self.loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        imbalance_info = data['class_imbalance_info']
        
        # Check required keys
        required_keys = [
            'train_home_win_rate', 'val_home_win_rate', 'test_home_win_rate',
            'overall_home_win_rate', 'is_imbalanced', 'scale_pos_weight'
        ]
        
        for key in required_keys:
            self.assertIn(key, imbalance_info, f"Missing key in imbalance info: {key}")
        
        # Check win rates are between 0 and 1
        self.assertGreaterEqual(imbalance_info['overall_home_win_rate'], 0)
        self.assertLessEqual(imbalance_info['overall_home_win_rate'], 1)
        
        # Check scale_pos_weight if imbalanced
        if imbalance_info['is_imbalanced']:
            self.assertIsNotNone(imbalance_info['scale_pos_weight'])
            self.assertGreater(imbalance_info['scale_pos_weight'], 0)
    
    def test_feature_statistics(self):
        """Test feature statistics function."""
        data = self.loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        stats = self.loader.get_feature_statistics(data['X_train'])
        
        # Check required keys
        required_keys = [
            'total_features', 'total_samples', 'missing_values',
            'missing_percentage', 'feature_types', 'numeric_features'
        ]
        
        for key in required_keys:
            self.assertIn(key, stats, f"Missing key in stats: {key}")
        
        # Check values (may be 0 if no data)
        self.assertGreaterEqual(stats['total_features'], 0)
        self.assertGreaterEqual(stats['total_samples'], 0)
        self.assertEqual(stats['total_samples'], len(data['X_train']))
    
    def test_min_features_filtering(self):
        """Test that games with insufficient features are filtered."""
        # Load with high min_features requirement
        data_strict = self.loader.load_all_data(
            train_seasons=['2022-23'],
            min_features=100  # Very high threshold
        )
        
        # Load with low min_features requirement
        data_lenient = self.loader.load_all_data(
            train_seasons=['2022-23'],
            min_features=10  # Low threshold
        )
        
        # Strict should have fewer or equal games
        self.assertLessEqual(len(data_strict['X_train']), len(data_lenient['X_train']))
    
    def test_season_splitting(self):
        """Test that seasons are split correctly."""
        data = self.loader.load_all_data(
            train_seasons=['2022-23'],
            val_seasons=['2023-24'],
            test_seasons=['2024-25']
        )
        
        # Check structure (may be empty if data quality issues exist)
        # The loader should still return proper structure even with no data
        self.assertIsInstance(data['X_train'], pd.DataFrame)
        self.assertIsInstance(data['X_val'], pd.DataFrame)
        self.assertIsInstance(data['X_test'], pd.DataFrame)
        
        # Check game IDs are unique across splits
        train_ids = set(data['game_ids_train'])
        val_ids = set(data['game_ids_val'])
        test_ids = set(data['game_ids_test'])
        
        self.assertEqual(len(train_ids & val_ids), 0, "Train and Val should not overlap")
        self.assertEqual(len(train_ids & test_ids), 0, "Train and Test should not overlap")
        self.assertEqual(len(val_ids & test_ids), 0, "Val and Test should not overlap")


if __name__ == '__main__':
    unittest.main()

