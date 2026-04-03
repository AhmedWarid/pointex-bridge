@echo off
cd /d %~dp0
start "" venv\Scripts\pythonw.exe -m uvicorn app.main:app --host 0.0.0.0 --port 8470 --log-level info
