@echo off
REM NBA Prediction Model - Full Daily Workflow
REM Run this once daily (recommended: 11 PM) for complete automation
REM 
REM This performs all steps:
REM 1. Fetches today's games
REM 2. Makes predictions and saves to database
REM 3. Updates scores for finished games
REM 4. Evaluates yesterday's predictions
REM
REM To set up in Windows Task Scheduler:
REM 1. Open Task Scheduler (taskschd.msc)
REM 2. Click "Create Basic Task..."
REM 3. Name: "NBA Daily Workflow"
REM 4. Trigger: Daily at 11:00 PM
REM 5. Action: Start a program
REM 6. Program: Browse to this .bat file
REM 7. Start in: The folder containing this file

cd /d "%~dp0.."
echo Running NBA Full Workflow at %date% %time%

REM Activate virtual environment if exists
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Run the full workflow
python scripts/daily_workflow.py --quiet

echo Full workflow completed at %time%

