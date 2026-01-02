# NBA Prediction Automation

This folder contains scripts for automating the NBA prediction pipeline.

## Quick Start

### Option 1: Manual Setup (Recommended)

1. **Morning (before games start)**:
   ```bash
   python scripts/daily_workflow.py --morning
   ```
   This will:
   - Fetch today's games from the betting API
   - Make predictions and save to database

2. **Evening (after games finish)**:
   ```bash
   python scripts/daily_workflow.py --evening
   ```
   This will:
   - Update scores for finished games
   - Evaluate yesterday's predictions

3. **Full workflow (run once daily at 11 PM)**:
   ```bash
   python scripts/daily_workflow.py
   ```
   This runs all steps at once.

### Option 2: Windows Task Scheduler

#### Using PowerShell Script (Recommended)
```powershell
# Run as Administrator
.\automation\setup_task_scheduler.ps1
```

#### Manual Task Scheduler Setup
1. Open Task Scheduler (`taskschd.msc`)
2. Click "Create Basic Task..."
3. Create two tasks:

**Task 1: Morning Predictions**
- Name: `NBA Morning Predictions`
- Trigger: Daily at 9:00 AM
- Action: Start a program
- Program: `C:\Users\yuvi2\Downloads\nba-prediction-model\automation\run_morning_workflow.bat`

**Task 2: Evening Evaluation**
- Name: `NBA Evening Evaluation`
- Trigger: Daily at 11:00 PM
- Action: Start a program
- Program: `C:\Users\yuvi2\Downloads\nba-prediction-model\automation\run_evening_workflow.bat`

## Files

| File | Description |
|------|-------------|
| `run_morning_workflow.bat` | Batch file for morning workflow |
| `run_evening_workflow.bat` | Batch file for evening workflow |
| `run_full_workflow.bat` | Batch file for complete workflow |
| `setup_task_scheduler.ps1` | PowerShell script to create scheduled tasks |

## Logs

Workflow logs are saved to `logs/workflow_YYYYMMDD.log`

## Testing

To test the workflow without waiting for scheduled times:

```bash
# Test full workflow
python scripts/daily_workflow.py

# Test specific steps
python scripts/daily_workflow.py --step 1  # Fetch games
python scripts/daily_workflow.py --step 2  # Make predictions
python scripts/daily_workflow.py --step 3  # Update scores
python scripts/daily_workflow.py --step 4  # Evaluate predictions
```

## Switching Devices

The automation works on any device:
1. Clone the repo: `git clone https://github.com/Yuvraj2006py/nba-prediction-model.git`
2. Install dependencies: `pip install -r requirements.txt`
3. Set up Task Scheduler using the same batch files

All paths are relative - no configuration changes needed.

