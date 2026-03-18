@echo off
taskkill /FI "WINDOWTITLE eq cryptofeed-collector" /F 2>nul
taskkill /FI "WINDOWTITLE eq cryptofeed-processor" /F 2>nul
echo Services stopped.
