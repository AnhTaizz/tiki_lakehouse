@echo off
color 0A
:menu
cls
echo ==============================================================
echo        TIKI LAKEHOUSE - CONTROL PANEL (DEMO)
echo ==============================================================
echo.
echo [1] Crawl Data from Tiki (Generate JSON shards)
echo [2] Init SQLite Database (Load JSON into local DB)
echo [3] Start Mock API Service (For Airflow Batch pipeline)
echo [4] Start Real-time Streaming (Simulate transactions)
echo [5] Trigger MEGA LIVE Flash Sale (Chaos Mode)
echo [6] Trigger FATAL PRICE BUG (90%% Drop Anomaly)
echo [7] Trigger VIRAL TREND (Massive Sales Velocity)
echo [8] Trigger MASSIVE OUT-OF-STOCK (Empty Inventory)
echo [9] Exit
echo.
set /p choice="Select option (1-9): "

if "%choice%"=="1" goto crawl_data
if "%choice%"=="2" goto init_db
if "%choice%"=="3" goto mock_api
if "%choice%"=="4" goto streaming
if "%choice%"=="5" goto flash_sale
if "%choice%"=="6" goto price_bug
if "%choice%"=="7" goto viral_trend
if "%choice%"=="8" goto out_of_stock
if "%choice%"=="9" goto exit

echo.
echo Invalid option. Please try again.
pause
goto menu

:crawl_data
echo.
echo ==============================================================
echo Running Tiki Crawler... This will fetch product data and save as JSON.
start cmd /k "title SEED TIKI DATA && python scripts\seed_tiki_data.py"
echo Opened background window for Crawler. Wait for it to finish.
echo ==============================================================
pause
goto menu

:init_db
echo.
echo ==============================================================
echo Consolidating crawled JSON shards into SQLite Database...
python simulators\init_sqlite.py
echo ==============================================================
pause
goto menu

:mock_api
echo.
echo ==============================================================
echo Starting Mock API Service at http://0.0.0.0:8000
start cmd /k "title TIKI MOCK API && python simulators\mock_tiki_service.py"
echo Opened background window.
echo ==============================================================
pause
goto menu

:streaming
echo.
echo ==============================================================
echo Opening 2 windows for Streaming pipeline...
start cmd /k "title TIKI SIMULATOR && set KAFKA_BROKER=localhost:9093 && python simulators\tiki_continuous_simulator.py"
start cmd /k "title SPARK STREAMING PROCESSOR && docker exec -it tiki_spark_crawler python /home/jovyan/work/src/jobs/tiki_stream_processor.py"
echo Done! Open Superset and enjoy Real-time analytics.
echo ==============================================================
pause
goto menu

:flash_sale
echo.
echo ==============================================================
echo INJECTING CHAOS: MEGA LIVE FLASH SALE!
echo 5,000 products will have their prices slashed by 30-70%%
python simulators\trigger_flash_sale.py
echo ==============================================================
pause
goto menu

:price_bug
echo.
echo ==============================================================
echo RED ALERT: FATAL PRICE BUG SIMULATION!
echo 10 expensive products will instantly drop by 90-98%%
python simulators\trigger_price_bug.py
echo ==============================================================
pause
goto menu

:viral_trend
echo.
echo ==============================================================
echo VIRAL TIKTOK TREND SIMULATION!
echo 5 random products will instantly get 5000+ purchases
python simulators\trigger_viral_trend.py
echo ==============================================================
pause
goto menu

:out_of_stock
echo.
echo ==============================================================
echo MASSIVE OUT-OF-STOCK SIMULATION!
echo 100 hot-selling products will instantly be disabled
python simulators\trigger_out_of_stock.py
echo ==============================================================
pause
goto menu

:exit
echo.
echo Goodbye!
exit
