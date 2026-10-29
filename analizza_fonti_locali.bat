@echo off
setlocal
REM ==========================================================
REM PV Agent - schede progetto per i portali raggiungibili solo dall'Italia
REM (Sicilia SI-VVI, Sistema Puglia, Basilicata). Da lanciare dal PC dell'ufficio/casa.
REM Ogni lancio elabora un blocco di progetti (default 15, dai piu' grandi) e salva le
REM schede nella memoria su GitHub: compaiono sul sito come quelle del cloud.
REM I documenti scaricati vengono cancellati dopo l'analisi.
REM Uso: analizza_fonti_locali.bat [numero progetti] [ricalcola]
REM   ricalcola = rilegge i progetti che hanno gia' la scheda (dopo un miglioramento delle regole)
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
set "EXTRA="
set "WHAT=fv"
if /I "%~2"=="ricalcola" set "EXTRA=--recompute"
if /I "%~2"=="eolico" set "WHAT=eolico"
if /I "%~2"=="tutto" set "WHAT=tutto"
echo Aggiorno il repository (git pull)...
git pull --ff-only
"%PYTHON_EXE%" -m pip install -q pymupdf pypdf requests beautifulsoup4 >nul 2>nul
if /I not "%WHAT%"=="eolico" (
    echo.
    echo === Fotovoltaico ===
    "%PYTHON_EXE%" -m doc_intel local-batch --data ".\docs\data.json" --max %MAXP% %EXTRA%
    if errorlevel 1 goto :nosave
)
if /I not "%WHAT%"=="fv" (
    echo.
    echo === Eolico ===
    "%PYTHON_EXE%" -m doc_intel local-batch --wind --max %MAXP% %EXTRA%
    if errorlevel 1 goto :nosave
)
echo.
echo Fatto. Le nuove schede saranno visibili sul sito entro pochi minuti.
pause
endlocal
exit /b 0

:nosave
echo.
echo ATTENZIONE: la memoria non e' stata salvata su GitHub. Riprova piu' tardi.
pause
exit /b 1
