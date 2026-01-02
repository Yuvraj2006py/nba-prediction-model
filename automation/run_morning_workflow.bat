@echo off
REM NBA Prediction Model - Morning Workflow
REM Run this at 9 AM daily to fetch games and make predictions
REM 
REM To set up in Windows Task Scheduler:
REM 1. Open Task Scheduler (taskschd.msc)
REM 2. Click "Create Basic Task..."
REM 3. Name: "NBA Morning Predictions"
REM 4. Trigger: Daily at 9:00 AM
REM 5. Action: Start a program
REM 6. Program: Browse to this .bat file
REM 7. Start in: The folder containing this file

cd /d "%~dp0.."
echo Running NBA Morning Workflow at %date% %time%

REM Activate virtual environment if exists
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Run the morning workflow
python scripts/daily_workflow.py --morning --quiet

echo Morning workflow completed at %time%

