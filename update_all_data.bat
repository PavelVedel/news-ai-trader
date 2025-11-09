:x

@echo off
:: Set UTF-8
chcp 65001 >nul 

echo ========================================
echo    UPDATING ALL PROJECT DATA
echo ========================================
echo.

:: Check that we are in the project root folder
::if not exist "apps" (
::    echo [ERROR] Run the script from the project root folder!
::    echo Current folder: %CD%
::    echo Expected structure: apps\ingest, apps\market_data
::    pause
::    exit /b 1
::)
::
::echo [1/7] Starting news collection...
::echo ----------------------------------------
::python apps\ingest\rest_news_collector.py --mode update_all
::if %errorlevel% neq 0 (
::    echo [WARNING] Error collecting news (code: %errorlevel%)
::) else (
::    echo [OK] News collected successfully
::)
::echo.
::
::echo [2/7] Starting market data update...
::echo ----------------------------------------
::python apps\market_data\update_market_data.py
::if %errorlevel% neq 0 (
::    echo [WARNING] Error updating market data (code: %errorlevel%)
::) else (
::    echo [OK] Market data updated successfully
::)
::echo.
::
::echo [3/6] Starting fundamentals and company information update...
::echo ----------------------------------------
::python apps\market_data\update_infos_and_fundamentals.py
::if %errorlevel% neq 0 (
::    echo [WARNING] Error updating fundamentals and infos (code: %errorlevel%)
::) else (
::    echo [OK] Fundamentals and company information updated successfully
::)
::echo.

echo [4/6] Starting news analysis (Stage A)...
echo ----------------------------------------
python apps\ai\perform_stage_a_news_analyzation.py
if %errorlevel% neq 0 (
    echo [WARNING] Error analyzing news (code: %errorlevel%)
) else (
    echo [OK] News analysis completed successfully
)
echo.

echo [5/6] Starting entity alias formation (Stage B)...
echo ----------------------------------------
python apps\ai\perform_stage_b_entity_alias_formation.py --extract
if %errorlevel% neq 0 (
    echo [WARNING] Error forming entity aliases (code: %errorlevel%)
) else (
    echo [OK] Entity alias formation completed successfully
)
echo.

echo [6/6] Starting entity grounding to news (Stage C)...
echo ----------------------------------------
python apps\ingest\perform_stage_c_entity_grounding.py
if %errorlevel% neq 0 (
    echo [WARNING] Error grounding entities (code: %errorlevel%)
) else (
    echo [OK] Entity grounding completed successfully
)
echo.

echo ========================================
echo         UPDATE COMPLETED
echo ========================================
echo.
echo All scripts executed. Check logs for details.
echo.

goto x
pause
