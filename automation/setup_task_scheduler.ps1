# PowerShell script to set up Windows Task Scheduler tasks
# Run this script as Administrator to create automated tasks

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonPath = "python"  # Or specify full path like "C:\Python313\python.exe"

Write-Host "Setting up NBA Prediction Automation Tasks" -ForegroundColor Green
Write-Host "Project Root: $ProjectRoot" -ForegroundColor Cyan

# Function to create a scheduled task
function Create-NBATask {
    param(
        [string]$TaskName,
        [string]$Description,
        [string]$Arguments,
        [string]$TriggerTime
    )
    
    $Action = New-ScheduledTaskAction -Execute $PythonPath `
        -Argument "scripts/daily_workflow.py $Arguments" `
        -WorkingDirectory $ProjectRoot
    
    $Trigger = New-ScheduledTaskTrigger -Daily -At $TriggerTime
    
    $Settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -RunOnlyIfNetworkAvailable
    
    # Check if task exists
    $ExistingTask = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if ($ExistingTask) {
        Write-Host "  Task '$TaskName' already exists. Updating..." -ForegroundColor Yellow
        Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
    }
    
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Description $Description `
        -Action $Action `
        -Trigger $Trigger `
        -Settings $Settings `
        -RunLevel Limited
    
    Write-Host "  Created task: $TaskName at $TriggerTime" -ForegroundColor Green
}

# Create Morning Task (9 AM)
Create-NBATask `
    -TaskName "NBA Morning Predictions" `
    -Description "Fetch NBA games and make predictions" `
    -Arguments "--morning --quiet" `
    -TriggerTime "09:00"

# Create Evening Task (11 PM)
Create-NBATask `
    -TaskName "NBA Evening Evaluation" `
    -Description "Update scores and evaluate predictions" `
    -Arguments "--evening --quiet" `
    -TriggerTime "23:00"

Write-Host ""
Write-Host "Task Scheduler setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "To view/modify tasks:" -ForegroundColor Cyan
Write-Host "  1. Open Task Scheduler (taskschd.msc)"
Write-Host "  2. Look for 'NBA Morning Predictions' and 'NBA Evening Evaluation'"
Write-Host ""
Write-Host "To run manually:" -ForegroundColor Cyan
Write-Host "  Morning: python scripts/daily_workflow.py --morning"
Write-Host "  Evening: python scripts/daily_workflow.py --evening"
Write-Host "  Full:    python scripts/daily_workflow.py"

