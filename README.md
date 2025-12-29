# NBA Game Prediction Model

An algorithmic trading model for predicting NBA game outcomes using machine learning.

## Project Overview

This project implements an end-to-end system for:
- Collecting NBA game data, team statistics, and betting odds
- Engineering predictive features from historical data
- Training machine learning models to predict game outcomes
- Backtesting betting strategies
- Deploying predictions for live games

## Project Structure

```
algo-model/
├── config/              # Configuration files
├── data/                # Data storage (raw, processed, models)
├── src/                 # Source code
│   ├── data_collectors/ # Data collection modules
│   ├── database/        # Database management
│   ├── features/        # Feature engineering
│   ├── models/          # ML model implementations
│   ├── training/        # Training and evaluation
│   ├── backtesting/     # Backtesting framework
│   ├── deployment/      # Deployment and automation
│   └── monitoring/      # Performance monitoring
├── notebooks/           # Jupyter notebooks for analysis
├── tests/               # Unit and integration tests
└── scripts/             # Utility scripts
```

## Setup Instructions

### 1. Prerequisites

- Python 3.8 or higher
- pip package manager

### 2. Installation

1. Clone or navigate to the project directory:
```bash
cd "Algo Model"
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
```

3. Activate the virtual environment:
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

4. Install dependencies:
```bash
pip install -r requirements.txt
```

### 3. Configuration

1. Copy the example environment file:
```bash
copy .env.example .env
```

2. Edit `.env` and add your API keys:
   - `NBA_API_KEY` - Optional, for NBA official API
   - `BETTING_API_KEY` - For betting odds data
   - Adjust other settings as needed

### 4. Initialize Database

The database will be created automatically when you first run the application. The schema is defined in `config/database.yaml`.

## Usage

### Data Collection

```python
from src.data_collectors.nba_api_collector import NBAPICollector

collector = NBAPICollector()
games = collector.get_game_data(season="2023-24")
```

### Feature Engineering

```python
from src.features.feature_aggregator import FeatureAggregator

aggregator = FeatureAggregator()
features = aggregator.create_feature_vector(game_id, home_team_id, away_team_id)
```

### Model Training

```python
from src.training.trainer import ModelTrainer
from src.models.xgboost_model import XGBoostModel

trainer = ModelTrainer()
model = XGBoostModel()
trainer.train_model(model, X_train, y_train, X_val, y_val)
```

### Backtesting

```python
from src.backtesting.backtester import Backtester

backtester = Backtester()
results = backtester.run_backtest(start_date="2022-10-01", end_date="2023-04-15")
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
black src/
```

### Linting

```bash
flake8 src/
```

## Project Status

- [x] Phase 1: Project Setup & Infrastructure
- [ ] Phase 2: Data Collection
- [ ] Phase 3: Database Implementation
- [ ] Phase 4: Feature Engineering
- [ ] Phase 5: Model Development
- [ ] Phase 6: Backtesting
- [ ] Phase 7: Deployment
- [ ] Phase 8: Monitoring

## License

This project is for educational and research purposes.

## Disclaimer

This model is for educational purposes only. Sports betting involves risk, and past performance does not guarantee future results. Always gamble responsibly and within your means.

