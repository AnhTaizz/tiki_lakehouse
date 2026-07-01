@echo off
color 0A
:menu
cls
echo ==============================================================
echo        TIKI LAKEHOUSE - CONTROL PANEL (GRADUATION DEMO)
echo ==============================================================
echo.
echo [1] Khoi tao co so du lieu loi (Init SQLite Database)
echo [2] Bat Mock API Service (Danh cho luong Airflow Batch)
echo [3] Bat Real-time Streaming (Mo phong giao dich tren toan san)
echo [4] Thoat
echo.
set /p choice="Chon chuc nang (1-4): "

if "%choice%"=="1" goto init_db
if "%choice%"=="2" goto mock_api
if "%choice%"=="3" goto streaming
if "%choice%"=="4" goto exit

echo.
echo Lua chon khong hop le. Vui long thu lai.
pause
goto menu

:init_db
echo.
echo ==============================================================
echo Dang nap 180,000+ san pham vao SQLite...
python src\simulators\init_sqlite.py
echo ==============================================================
pause
goto menu

:mock_api
echo.
echo ==============================================================
echo Dang bat Mock API Service tai http://0.0.0.0:8000
start cmd /k "title TIKI MOCK API && python src\simulators\mock_tiki_service.py"
echo Da mo cua so chay ngam.
echo ==============================================================
pause
goto menu

:streaming
echo.
echo ==============================================================
echo Dang mo 2 cua so phuc vu luong Streaming...
start cmd /k "title TIKI SIMULATOR && set KAFKA_BROKER=localhost:9093 && python src\simulators\tiki_continuous_simulator.py"
start cmd /k "title SPARK STREAMING PROCESSOR && docker exec -it tiki_spark_crawler python /home/jovyan/work/src/jobs/tiki_stream_processor.py"
echo Hoan thanh! Hay mo Superset va tan huong toc do Real-time.
echo ==============================================================
pause
goto menu

:exit
echo.
echo Tam biet!
exit
