@echo off
setlocal enabledelayedexpansion

REM ==========================================================
REM PV Agent - aggiornamento manuale SENZA DOCKER
REM Percorso previsto:
REM C:\Users\anzillotti\OneDrive - CGT Edilizia S.p.a\Documenti\GitHub\pv_agent_mvp
REM ==========================================================

cd /d "%~dp0"

echo.
echo ==========================================================
echo [0/8] Controllo ambiente Python
echo ==========================================================

if not exist ".venv\Scripts\python.exe" (
    echo ERRORE: ambiente virtuale Python non trovato.
    echo Atteso: .venv\Scripts\python.exe
    echo.
    echo Prima esegui:
    echo py -m venv .venv
    echo .\.venv\Scripts\python.exe -m pip install --upgrade pip
    echo .\.venv\Scripts\pip.exe install -r requirements.txt
    pause
    exit /b 1
)

if not exist ".env" (
    echo DATABASE_URL=sqlite:///./data/pv_agent.sqlite> ".env"
    echo Creato .env per SQLite locale.
)

if not exist "data" mkdir "data"
if not exist "docs" mkdir "docs"
if not exist "reports" mkdir "reports"
if not exist "reports\change_reports" mkdir "reports\change_reports"
if not exist "tmp" mkdir "tmp"

echo.
echo ==========================================================
echo [1/8] Salvataggio snapshot precedente
echo ==========================================================

if exist "docs\data.json" (
    copy /Y "docs\data.json" "tmp\previous_data.json" >nul
    echo Vecchio docs\data.json copiato in tmp\previous_data.json
) else (
    echo {"records":[]} > "tmp\previous_data.json"
    echo Nessun docs\data.json precedente: creato snapshot vuoto.
)

echo.
echo ==========================================================
echo [2/8] Esecuzione pipeline locale Python
echo ==========================================================

".\.venv\Scripts\python.exe" -m app.run_pipeline
if errorlevel 1 (
    echo.
    echo ERRORE: la pipeline Python e' fallita.
    echo Controlla il log sopra. Nessun file in docs e' stato aggiornato.
    pause
    exit /b 1
)

if not exist ".\reports\site\data.json" (
    echo.
    echo ERRORE: reports\site\data.json non trovato.
    pause
    exit /b 1
)

if not exist ".\reports\site\index.html" (
    echo.
    echo ERRORE: reports\site\index.html non trovato.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3/8] Normalizzazione province
echo ==========================================================

if not exist ".\scripts\normalize_province_codes.py" (
    echo ERRORE: scripts\normalize_province_codes.py non trovato.
    echo Copia il file nella cartella scripts e rilancia.
    pause
    exit /b 1
)

".\.venv\Scripts\python.exe" ".\scripts\normalize_province_codes.py" --data ".\reports\site\data.json" --audit ".\reports\province_normalization_audit.csv"

if errorlevel 1 (
    echo.
    echo ERRORE: normalizzazione province fallita.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3B/8] Override manuali localizzazione
echo ==========================================================

if not exist ".\scripts\manual_location_overrides.py" (
    echo ERRORE: scripts\manual_location_overrides.py non trovato.
    echo Copia il file nella cartella scripts e rilancia.
    pause
    exit /b 1
)

".\.venv\Scripts\python.exe" ".\scripts\manual_location_overrides.py" --data ".\reports\site\data.json" --audit ".\reports\manual_location_overrides_audit.csv"

if errorlevel 1 (
    echo.
    echo ERRORE: override manuali localizzazione falliti.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [4/8] Sync HTML dopo normalizzazione province
echo ==========================================================

".\.venv\Scripts\python.exe" -m app.dashboard_data_sync --data ".\reports\site\data.json" --html ".\reports\site\index.html" --no-backup

if errorlevel 1 (
    echo.
    echo ERRORE: sync HTML dopo normalizzazione province fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [5/8] Generazione report cambiamenti
echo ==========================================================

".\.venv\Scripts\python.exe" ".\scripts\compare_json_report.py" ^
  --old ".\tmp\previous_data.json" ^
  --new ".\reports\site\data.json" ^
  --out-html ".\reports\change_reports\changes_latest.html" ^
  --out-csv ".\reports\change_reports\changes_latest.csv"

if errorlevel 1 (
    echo.
    echo ERRORE: generazione report cambiamenti fallita.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [6/8] Copia dashboard aggiornata in docs
echo ==========================================================

copy /Y ".\reports\site\data.json" ".\docs\data.json" >nul
copy /Y ".\reports\site\index.html" ".\docs\index.html" >nul

echo Dashboard aggiornata:
echo - docs\data.json
echo - docs\index.html

echo.
echo ==========================================================
echo [7/8] Apertura report locale
echo ==========================================================

start "" ".\reports\change_reports\changes_latest.html"

echo.
echo ==========================================================
echo [8/8] Riepilogo Git
echo ==========================================================

git status --short

echo.
echo Report cambiamenti:
echo reports\change_reports\changes_latest.html
echo reports\change_reports\changes_latest.csv
echo.
echo Audit normalizzazione province:
echo reports\province_normalization_audit.csv
echo.
echo Ora apri GitHub Desktop, verifica i file modificati, poi fai commit + push manuale.
echo.
pause
