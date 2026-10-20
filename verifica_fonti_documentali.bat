@echo off
setlocal enabledelayedexpansion

REM ==========================================================
REM PV Agent - Verifica fonti documentali (rete italiana)
REM Per ogni fonte: prende un progetto reale gia' presente,
REM trova la documentazione, scarica, analizza, ELIMINA i file
REM e scrive un riepilogo. NON tocca database, docs\data.json
REM o docs\index.html.
REM
REM Uso:
REM   verifica_fonti_documentali.bat              tutte le fonti
REM   verifica_fonti_documentali.bat sicilia      solo una fonte
REM   verifica_fonti_documentali.bat sicilia Basilicata sistema_puglia_energia
REM ==========================================================

cd /d "%~dp0"

set "PYTHON_EXE=.\.venv\Scripts\python.exe"
set "PYTHONPATH=%CD%"
set "PYTHONIOENCODING=utf-8"
set "PYTHONUNBUFFERED=1"
set "OUT_DIR=.\reports\doc_intel_fonti"
set "STORE_DIR=.\.doc_intel_store"

echo.
echo ==========================================================
echo [1/4] Controllo ambiente
echo ==========================================================

if not exist "%PYTHON_EXE%" (
    echo ERRORE: ambiente virtuale Python non trovato: %PYTHON_EXE%
    echo Esegui prima la stessa preparazione di aggiorna_dashboard_senza_docker.bat
    pause
    exit /b 1
)

"%PYTHON_EXE%" -c "import fitz, pypdf, bs4, requests" 2>nul
if errorlevel 1 (
    echo Installo le librerie per la lettura dei PDF ^(solo la prima volta^)...
    "%PYTHON_EXE%" -m pip install --quiet pymupdf pypdf requests beautifulsoup4
    if errorlevel 1 (
        echo ERRORE: installazione librerie non riuscita.
        pause
        exit /b 1
    )
)

set "OCR_FLAG="
if exist "C:\Program Files\Tesseract-OCR\tesseract.exe" set "PATH=C:\Program Files\Tesseract-OCR;%PATH%"
where tesseract >nul 2>nul
if errorlevel 1 (
    echo ATTENZIONE: Tesseract OCR non trovato. Le pagine scansionate non verranno lette.
    echo Per installarlo: winget install UB-Mannheim.TesseractOCR  ^(poi rilancia^)
    set "OCR_FLAG=--no-ocr"
) else (
    echo OCR disponibile.
)

set "RENDER_FLAG="
"%PYTHON_EXE%" -c "import playwright" 2>nul
if not errorlevel 1 (
    "%PYTHON_EXE%" -m playwright install chromium >nul 2>nul
    set "RENDER_FLAG=--render"
    echo Browser per portali dinamici disponibile.
)

if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"

set "ONLY="
if not "%~1"=="" set "ONLY=--only %*"

echo.
echo ==========================================================
echo [2/4] Verifica fonti: campione reale per ogni fonte
echo       ^(fino a 25 documenti e 10 minuti per fonte^)
echo ==========================================================

"%PYTHON_EXE%" -m doc_intel sources --data docs\data.json --store "%STORE_DIR%" --out "%OUT_DIR%" %ONLY% %RENDER_FLAG% %OCR_FLAG%
set "RC=%ERRORLEVEL%"

echo.
echo ==========================================================
echo [3/4] Pulizia documenti scaricati
echo ==========================================================
if exist "%STORE_DIR%\blobs" rmdir /s /q "%STORE_DIR%\blobs"
if exist "%STORE_DIR%\text" rmdir /s /q "%STORE_DIR%\text"
echo Documenti originali eliminati. Resta solo la memoria delle analisi.

echo.
echo ==========================================================
echo [4/4] Risultato
echo ==========================================================
if exist "%OUT_DIR%\fonti_latest.md" type "%OUT_DIR%\fonti_latest.md"
echo.
if "%RC%"=="0" (
    echo VERIFICA COMPLETATA.
) else (
    echo VERIFICA INTERROTTA - codice %RC%
)
echo Invia a Claude il file: %OUT_DIR%\fonti_latest.json
echo.
start "" explorer "%OUT_DIR%"
pause
exit /b %RC%
