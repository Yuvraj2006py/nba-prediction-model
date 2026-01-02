"""Check feature count from data loader."""

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
    val_seasons=[],
    test_seasons=[]
)

print(f'Training features: {len(data["X"].columns)}')
print(f'\nFirst 20 features:')
for i, col in enumerate(list(data['X'].columns[:20])):
    print(f'  {i+1}. {col}')

print(f'\nLast 20 features:')
for i, col in enumerate(list(data['X'].columns[-20:])):
    print(f'  {len(data["X"].columns) - 20 + i + 1}. {col}')


