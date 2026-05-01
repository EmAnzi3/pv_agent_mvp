@echo off
chcp 65001 >nul
setlocal

REM ============================================================
REM PV Agent MVP - Run pipeline completa
REM Esegue:
REM 1. app.main run-once
REM 2. app.data_quality --in-place
REM 3. app.dashboard_data_sync
REM 4. validazione finale
REM ============================================================

cd /d "%~dp0"

echo.
echo ============================================================
echo Avvio PV Agent Pipeline
echo Cartella progetto: %CD%
echo ============================================================
echo.

docker compose run --rm -v "%CD%\app:/app/app" -v "%CD%\reports:/app/reports" pv-agent python -m app.run_pipeline

if errorlevel 1 (
    echo.
    echo ============================================================
    echo ERRORE: pipeline non completata correttamente.
    echo Controlla il log sopra.
    echo ============================================================
    echo.
    pause
    exit /b 1
)

echo.
echo ============================================================
echo OK: pipeline completata correttamente.
echo Dashboard aggiornata in:
echo %CD%\reports\site\index.html
echo ============================================================
echo.

pause
exit /b 0
