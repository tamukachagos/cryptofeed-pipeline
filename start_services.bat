@echo off
cd /d %~dp0
if not exist logs mkdir logs
start "cryptofeed-collector" /MIN cmd /c "python collector\main.py >> logs\collector.log 2>&1"
timeout /t 2 /nobreak >nul
start "cryptofeed-processor" /MIN cmd /c "python processor\main.py >> logs\processor.log 2>&1"
echo Collector and Processor started.
