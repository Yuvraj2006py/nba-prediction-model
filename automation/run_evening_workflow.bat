@echo off
REM NBA Prediction Model - Evening Workflow
REM Run this at 11 PM daily to update scores and evaluate predictions
REM 
REM To set up in Windows Task Scheduler:
REM 1. Open Task Scheduler (taskschd.msc)
REM 2. Click "Create Basic Task..."
REM 3. Name: "NBA Evening Evaluation"
REM 4. Trigger: Daily at 11:00 PM
REM 5. Action: Start a program
REM 6. Program: Browse to this .bat file
REM 7. Start in: The folder containing this file

cd /d "%~dp0.."
echo Running NBA Evening Workflow at %date% %time%

REM Activate virtual environment if exists
if exist "venv\Scripts\activate.bat" (
    call venv\Scripts\activate.bat
)

REM Run the evening workflow
python scripts/daily_workflow.py --evening --quiet

echo Evening workflow completed at %time%

