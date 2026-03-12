@echo off
echo === Installing Pointex Bridge ===
python -m venv venv
call venv\Scripts\activate
pip install -r requirements.txt
if not exist .env (
    copy .env.example .env
    echo Created .env from template — edit it with your settings.
) else (
    echo .env already exists, skipping.
)
echo.
echo === Installation complete ===
echo Edit .env with your settings, then run: run.bat
pause
