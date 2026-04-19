@echo off
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
  echo No hay .venv — cree el entorno e instale: pip install -e .
  pause
  exit /b 1
)

call ".venv\Scripts\activate.bat"
set "PYTHONPATH=src"
python -m tasa_estadistica.cli fetch %*
