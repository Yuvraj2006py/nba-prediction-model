"""Check feature count from training data loader."""

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import os
os.environ['DATABASE_TYPE'] = 'sqlite'

from src.training.data_loader import DataLoader

loader = DataLoader()
data = loader.load_all_data(
    train_seasons=['2024-25'],
    val_seasons=['2025-26'],
    test_seasons=['2025-26']
)

print(f"Training features: {len(data['X'].columns) if not data['X'].empty else 0}")
print(f"Validation features: {len(data['val']['X'].columns) if not data['val']['X'].empty else 0}")
print(f"Test features: {len(data['test']['X'].columns) if not data['test']['X'].empty else 0}")

if not data['X'].empty:
    print(f"\nFirst 20 features: {list(data['X'].columns[:20])}")
    print(f"Last 20 features: {list(data['X'].columns[-20:])}")



