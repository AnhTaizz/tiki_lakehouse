@echo off
color 0A
:menu
cls
echo ==============================================================
echo        TIKI LAKEHOUSE - CONTROL PANEL (GRADUATION DEMO)
echo ==============================================================
echo.
echo [1] Init SQLite Database (Load 180,000+ products)
echo [2] Start Mock API Service (For Airflow Batch pipeline)
echo [3] Start Real-time Streaming (Simulate transactions)
echo [4] Exit
echo.
set /p choice="Select option (1-4): "

if "%choice%"=="1" goto init_db
if "%choice%"=="2" goto mock_api
if "%choice%"=="3" goto streaming
if "%choice%"=="4" goto exit

echo.
echo Invalid option. Please try again.
pause
goto menu

:init_db
echo.
echo ==============================================================
echo Loading 180,000+ products into SQLite...
python src\simulators\init_sqlite.py
echo ==============================================================
pause
goto menu

:mock_api
echo.
echo ==============================================================
echo Starting Mock API Service at http://0.0.0.0:8000
start cmd /k "title TIKI MOCK API && python src\simulators\mock_tiki_service.py"
echo Opened background window.
echo ==============================================================
pause
goto menu

:streaming
echo.
echo ==============================================================
echo Opening 2 windows for Streaming pipeline...
start cmd /k "title TIKI SIMULATOR && set KAFKA_BROKER=localhost:9093 && python src\simulators\tiki_continuous_simulator.py"
start cmd /k "title SPARK STREAMING PROCESSOR && docker exec -it tiki_spark_crawler python /home/jovyan/work/src/jobs/tiki_stream_processor.py"
echo Done! Open Superset and enjoy Real-time analytics.
echo ==============================================================
pause
goto menu

:exit
echo.
echo Goodbye!
exit
