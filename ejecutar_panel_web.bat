@echo off
chcp 65001 >nul
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
  echo No hay .venv — cree el entorno e instale: pip install -e ".[web]"
  pause
  exit /b 1
)

call ".venv\Scripts\activate.bat"
set "TASA=tasa-arca"
where tasa-arca >nul 2>&1 || set "TASA=python -m tasa_estadistica.cli"

echo Instalando extra web si hace falta...
pip show fastapi >nul 2>&1 || pip install -e ".[web]" -q

echo.
echo Panel en http://127.0.0.1:8000/  (Ctrl+C para cerrar)
echo.
%TASA% serve --host 127.0.0.1 --port 8000
pause
