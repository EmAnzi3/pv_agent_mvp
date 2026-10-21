@echo off
setlocal
REM ==========================================================
REM PV Agent - schede progetto per i portali raggiungibili solo dall'Italia
REM (Sicilia SI-VVI, Sistema Puglia, Basilicata). Da lanciare dal PC dell'ufficio/casa.
REM Ogni lancio elabora un blocco di progetti (default 15, dai piu' grandi) e salva le
REM schede nella memoria su GitHub: compaiono sul sito come quelle del cloud.
REM I documenti scaricati vengono cancellati dopo l'analisi.
REM Uso: analizza_fonti_locali.bat [numero progetti]
REM ==========================================================
cd /d "%~dp0"
set "PYTHON_EXE=.\.venv\Scripts\python.exe"
if not exist "%PYTHON_EXE%" (
    echo ERRORE: ambiente virtuale Python non trovato: %PYTHON_EXE%
    pause
    exit /b 1
)
where git >nul 2>nul
if errorlevel 1 (
    echo ERRORE: git non trovato nel PATH. Installa "Git for Windows" oppure lancia da GitHub Desktop ^(Repository ^> Open in Command Prompt^).
    pause
    exit /b 1
)
set "MAXP=%~1"
if "%MAXP%"=="" set "MAXP=15"
echo Aggiorno il repository (git pull)...
git pull --ff-only
"%PYTHON_EXE%" -m pip install -q pymupdf pypdf requests beautifulsoup4 >nul 2>nul
"%PYTHON_EXE%" -m doc_intel local-batch --data ".\docs\data.json" --max %MAXP%
if errorlevel 1 (
    echo.
    echo ATTENZIONE: la memoria non e' stata salvata su GitHub. Riprova piu' tardi.
    pause
    exit /b 1
)
echo.
echo Fatto. Le nuove schede saranno visibili sul sito entro pochi minuti.
pause
endlocal
