@echo off
setlocal enabledelayedexpansion

REM ==========================================================
REM PV Agent - aggiornamento manuale SENZA DOCKER
REM Flusso: Python locale + SQLite + GitHub Desktop
REM ==========================================================

cd /d "%~dp0"

set "PYTHON_EXE=.\.venv\Scripts\python.exe"
set "DATA_JSON=.\reports\site\data.json"
set "SITE_HTML=.\reports\site\index.html"
set "DOCS_DATA=.\docs\data.json"
set "DOCS_HTML=.\docs\index.html"
set "PREVIOUS_DATA=.\tmp\previous_data.json"
set "CHANGE_HTML=.\reports\change_reports\changes_latest.html"
set "CHANGE_CSV=.\reports\change_reports\changes_latest.csv"

echo.
echo ==========================================================
echo [0/8] Controllo ambiente Python
echo ==========================================================

if not exist "%PYTHON_EXE%" (
    echo ERRORE: ambiente virtuale Python non trovato.
    echo Atteso: %PYTHON_EXE%
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

if exist "%DOCS_DATA%" (
    copy /Y "%DOCS_DATA%" "%PREVIOUS_DATA%" >nul
    echo Vecchio docs\data.json copiato in tmp\previous_data.json
) else (
    echo {"records":[]}> "%PREVIOUS_DATA%"
    echo Nessun docs\data.json precedente: creato snapshot vuoto.
)

echo.
echo ==========================================================
echo [2/8] Esecuzione pipeline locale Python
echo ==========================================================

"%PYTHON_EXE%" -m app.run_pipeline
if errorlevel 1 (
    echo.
    echo ERRORE: la pipeline Python e' fallita.
    echo Controlla il log sopra. Nessun file in docs e' stato aggiornato.
    pause
    exit /b 1
)

if not exist "%DATA_JSON%" (
    echo.
    echo ERRORE: data.json non trovato dopo la pipeline.
    echo Atteso: %DATA_JSON%
    pause
    exit /b 1
)

if not exist "%SITE_HTML%" (
    echo.
    echo ERRORE: index.html non trovato dopo la pipeline.
    echo Atteso: %SITE_HTML%
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3A/8] Normalizzazione province
echo ==========================================================

if not exist ".\scripts\normalize_province_codes.py" (
    echo ERRORE: scripts\normalize_province_codes.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\normalize_province_codes.py" --data "%DATA_JSON%" --audit ".\reports\province_normalization_audit.csv"
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
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\manual_location_overrides.py" --data "%DATA_JSON%" --audit ".\reports\manual_location_overrides_audit.csv"
if errorlevel 1 (
    echo.
    echo ERRORE: override manuali localizzazione falliti.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3C/8] Override manuali Calabria
echo ==========================================================

if not exist ".\scripts\manual_calabria_overrides.py" (
    echo ERRORE: scripts\manual_calabria_overrides.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\manual_calabria_overrides.py" --data "%DATA_JSON%" --audit ".\reports\manual_calabria_overrides_audit.csv"
if errorlevel 1 (
    echo.
    echo ERRORE: override Calabria fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3D/8] Override manuali Sardegna
echo ==========================================================

if not exist ".\scripts\manual_sardegna_overrides.py" (
    echo ERRORE: scripts\manual_sardegna_overrides.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\manual_sardegna_overrides.py" --data "%DATA_JSON%" --audit ".\reports\manual_sardegna_overrides_audit.csv"
if errorlevel 1 (
    echo.
    echo ERRORE: override Sardegna fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3E/8] Override link Toscana STAR
echo ==========================================================

if not exist ".\scripts\manual_toscana_url_overrides.py" (
    echo ERRORE: scripts\manual_toscana_url_overrides.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\manual_toscana_url_overrides.py" --data "%DATA_JSON%" --audit ".\reports\manual_toscana_url_overrides_audit.csv"
if errorlevel 1 (
    echo.
    echo ERRORE: override link Toscana fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3F/8] Override manuali Umbria
echo ==========================================================

if not exist ".\scripts\manual_umbria_overrides.py" (
    echo ERRORE: scripts\manual_umbria_overrides.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\manual_umbria_overrides.py" --data "%DATA_JSON%" --audit ".\reports\manual_umbria_overrides_audit.csv"
if errorlevel 1 (
    echo.
    echo ERRORE: override Umbria fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3G/8] Override link Lombardia SILVIA
echo ==========================================================
if not exist ".\scripts\manual_lombardia_url_overrides.py" (
    echo ERRORE: scripts\manual_lombardia_url_overrides.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\manual_lombardia_url_overrides.py" --data "%DATA_JSON%" --audit ".\reports\manual_lombardia_url_overrides_audit.csv"
if errorlevel 1 (
    echo.
    echo ERRORE: override link Lombardia fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3H/8] Esclusione progetti solo accumulo
echo ==========================================================
if not exist ".\scripts\exclude_storage_only_projects.py" (
    echo ERRORE: scripts\exclude_storage_only_projects.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\exclude_storage_only_projects.py" --data "%DATA_JSON%" --audit ".\reports\storage_only_exclusions_latest.csv" --apply
if errorlevel 1 (
    echo.
    echo ERRORE: esclusione progetti solo accumulo fallita.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3I/8] Override localizzazione MASE
echo ==========================================================
if not exist ".\scripts\manual_mase_location_overrides.py" (
    echo ERRORE: scripts\manual_mase_location_overrides.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\manual_mase_location_overrides.py" --data "%DATA_JSON%" --audit ".\reports\manual_mase_location_overrides_audit.csv"
if errorlevel 1 (
    echo.
    echo ERRORE: override localizzazione MASE fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3X/8] Pulizia falsi Taranto MASE
echo ==========================================================
if not exist ".\scripts\cleanup_mase_false_taranto.py" (
    echo ERRORE: scripts\cleanup_mase_false_taranto.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\cleanup_mase_false_taranto.py" --data "%DATA_JSON%" --audit ".\reports\mase_false_taranto_cleanup_audit.csv" --apply
if errorlevel 1 (
    echo.
    echo ERRORE: pulizia falsi Taranto MASE fallita.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3Y/8] Dedupe snapshot mensili Terna
echo ==========================================================
if not exist ".\scripts\dedupe_terna_monthly_snapshot.py" (
    echo ERRORE: scripts\dedupe_terna_monthly_snapshot.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\dedupe_terna_monthly_snapshot.py" --data "%DATA_JSON%" --audit ".\reports\terna_monthly_dedupe_audit.csv" --apply
if errorlevel 1 (
    echo.
    echo ERRORE: dedupe snapshot mensili Terna fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3Z/8] Correzione potenza Vigarano Mainarda
echo ==========================================================
if not exist ".\scripts\manual_vigarano_mainarda_power_override.py" (
    echo ERRORE: scripts\manual_vigarano_mainarda_power_override.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\manual_vigarano_mainarda_power_override.py" --data "%DATA_JSON%" --audit ".\reports\manual_vigarano_mainarda_power_override_audit.csv"
if errorlevel 1 (
    echo.
    echo ERRORE: correzione potenza Vigarano Mainarda fallita.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [3G/8] Normalizzazione label fonti
echo ==========================================================

if not exist ".\scripts\normalize_source_display_labels.py" (
    echo ERRORE: scripts\normalize_source_display_labels.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\normalize_source_display_labels.py" --data "%DATA_JSON%"
if errorlevel 1 (
    echo.
    echo ERRORE: normalizzazione label fonti fallita.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [4/8] Sync HTML dopo normalizzazioni e override
echo ==========================================================

"%PYTHON_EXE%" -m app.dashboard_data_sync
if errorlevel 1 (
    echo.
    echo ERRORE: sync dashboard_data_sync fallito.
    pause
    exit /b 1
)

echo.
echo ==========================================================
echo [5/8] Generazione report cambiamenti
echo ==========================================================

if not exist ".\scripts\compare_json_report.py" (
    echo ERRORE: scripts\compare_json_report.py non trovato.
    pause
    exit /b 1
)

"%PYTHON_EXE%" ".\scripts\compare_json_report.py" --old "%PREVIOUS_DATA%" --new "%DATA_JSON%" --out-html "%CHANGE_HTML%" --out-csv "%CHANGE_CSV%"
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

copy /Y "%DATA_JSON%" "%DOCS_DATA%" >nul
if errorlevel 1 (
    echo ERRORE: copia docs\data.json fallita.
    pause
    exit /b 1
)

copy /Y "%SITE_HTML%" "%DOCS_HTML%" >nul
if errorlevel 1 (
    echo ERRORE: copia docs\index.html fallita.
    pause
    exit /b 1
)

echo Dashboard aggiornata:
echo - docs\data.json
echo - docs\index.html

echo.
echo ==========================================================
echo [7/8] Apertura report locale
echo ==========================================================

if exist "%CHANGE_HTML%" (
    start "" "%CHANGE_HTML%"
) else (
    echo Report HTML non trovato: %CHANGE_HTML%
)

echo.
echo ==========================================================
echo [8/8] Riepilogo Git
echo ==========================================================

git status

echo.
echo Report cambiamenti:
echo %CHANGE_HTML%
echo %CHANGE_CSV%

if exist ".\reports\province_normalization_audit.csv" (
    echo.
    echo Audit normalizzazione province:
    echo reports\province_normalization_audit.csv
)

echo.
echo ==========================================================
echo Processo completato.
echo Se i controlli sono ok: commit + push da GitHub Desktop.
echo ==========================================================
echo.

pause
endlocal


