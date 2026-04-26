@echo off
cd /d "%~dp0\.."
call .venv\Scripts\activate.bat
python Run\send_warning.py
