@echo off
setlocal
REM ==========================================================
REM PV Agent - esplorazione portali raggiungibili solo dall'Italia
REM Sicilia (SI-VVI) e Sistema Puglia: salva struttura pagine e chiamate API
REM in reports\doc_intel_explore\esplora_latest.json da inviare per scrivere gli adapter.
REM Non scarica documenti e non modifica dati.
REM ==========================================================
cd /d "%~dp0"
set "PYTHON_EXE=.\.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
    echo ERRORE: ambiente virtuale Python non trovato: %PYTHON_EXE%
    pause
    exit /b 1
)
if not exist "reports\doc_intel_explore" mkdir "reports\doc_intel_explore"
set "RENDER="
"%PYTHON_EXE%" -c "import playwright" 2>nul || set "RENDER=--no-render"
if defined RENDER echo Playwright non disponibile: esplorazione senza browser.
"%PYTHON_EXE%" -m doc_intel explore %RENDER% --out "reports\doc_intel_explore\esplora_latest.json" ^
  "https://si-vvi.regione.sicilia.it/viavas/index.php/it/component/fabrik/list/30?Itemid=332&procedura___oggetto_raw=175&limitstart30=0&resetfilters=1&fabrik_incsessionfilters=0" ^
  "https://si-vvi.regione.sicilia.it/viavas/index.php/it/component/fabrik/list/30?Itemid=332&procedura___oggetto_raw=274&limitstart30=0&resetfilters=1&fabrik_incsessionfilters=0" ^
  "https://www.sistema.puglia.it/portal/page/portal/SistemaPuglia/DettaglioInfo?id=63780" ^
  "https://www.sistema.puglia.it/portal/page/portal/SistemaPuglia/DettaglioInfo?id=63632"
if errorlevel 1 (
    echo ERRORE durante l'esplorazione.
    pause
    exit /b 1
)
echo.
echo Fatto. Invia questo file:
echo %CD%\reports\doc_intel_explore\esplora_latest.json
pause
endlocal
