@echo off
chcp 65001 >nul
setlocal
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
  echo No hay entorno virtual en .venv
  echo Crear con: python -m venv .venv
  echo Luego: .venv\Scripts\pip install -e ".[dev]"
  pause
  exit /b 1
)

call ".venv\Scripts\activate.bat"
if errorlevel 1 (
  pause
  exit /b 1
)

set "TASA=tasa-arca"
where tasa-arca >nul 2>&1 || set "TASA=python -m tasa_estadistica.cli"

echo.
echo === 1/3 tasa-arca auth ===
%TASA% auth
if errorlevel 1 (
  echo Fallo auth. Revisar certificado y .env
  pause
  exit /b 1
)

echo.
echo === 2/3 tasa-arca fetch mayo 2025 ===
%TASA% fetch --desde 2025-05-01 --hasta 2025-05-31
if errorlevel 1 (
  echo Fallo fetch.
  pause
  exit /b 1
)

echo.
echo === 3/3 tasa-arca export-recupero mayo 2025 ===
%TASA% export-recupero --desde 2025-05-01 --hasta 2025-05-31
if errorlevel 1 (
  echo Fallo export-recupero.
  pause
  exit /b 1
)

echo.
echo Listo. Salida: out\recupero_tasa.xlsx y out\recupero_tasa.csv
pause
