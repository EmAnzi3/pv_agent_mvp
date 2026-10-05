@echo off
setlocal

REM ==========================================================
REM PV Agent - test isolato sorgente Piemonte
REM NON tocca database, docs/data.json o docs/index.html.
REM Produce solo reports\piemonte_probe\probe_latest.json
REM ==========================================================

cd /d "%~dp0"

set "PYTHON_EXE=.\.venv\Scripts\python.exe"
set "PYTHONPATH=%CD%"

echo.
echo ==========================================================
echo TEST ISOLATO PIEMONTE
echo ==========================================================
echo.

if not exist "%PYTHON_EXE%" (
    echo ERRORE: ambiente virtuale Python non trovato.
    echo Atteso: %PYTHON_EXE%
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\probe_piemonte.py"
set "RC=%ERRORLEVEL%"

echo.
echo ==========================================================
if "%RC%"=="0" (
    echo PIEMONTE: TEST OK
) else (
    echo PIEMONTE: TEST NON ANCORA OK - codice uscita %RC%
    echo Invia l'output completo e il file reports\piemonte_probe\probe_latest.json.
)
echo ==========================================================
echo.

pause
exit /b %RC%
