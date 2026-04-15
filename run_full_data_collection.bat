@echo off
echo ============================================================
echo  Muroto Offshore Current Data v2.0 - Full Data Collection
echo  Points: NW / W / Muroto-oki / E / NE
echo  Period:  2022-01-01 to today
echo ============================================================
echo.

cd /d "%~dp0muroto_offshore_current"
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: Cannot cd to muroto_offshore_current
    pause
    exit /b 1
)

echo [1/2] Starting data collection...
echo.
python main.py --all
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: Data collection failed.
    pause
    exit /b 1
)

echo.
echo [2/2] Updating dashboard JS file...
cd /d "%~dp0"
python update_offshore_dashboard_data.py
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo ERROR: Dashboard JS update failed.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo  DONE!
echo  Data: muroto_offshore_current\output\muroto_offshore_current_all.csv
echo  Open muroto_offshore_current_dashboard.html in browser
echo ============================================================
pause
