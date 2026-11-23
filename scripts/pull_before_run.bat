@echo off
REM Scarica l'ultima versione del codice prima di un run locale.
REM Chiamato da aggiorna_dashboard_senza_docker.bat e aggiorna_wind_radar.bat (cartella del repository),
REM con il nome del bat chiamante: se il pull lo modifica, esce con errore e chiede di rilanciarlo
REM (un bat cambiato mentre gira viene letto in modo sbagliato da Windows).
REM Se ci sono risultati di un run precedente non ancora pubblicati non scarica nulla e avvisa:
REM vanno prima pubblicati con GitHub Desktop (Pull origin, Commit, Push).
where git >nul 2>nul
if errorlevel 1 (
    echo ATTENZIONE: git non trovato nel PATH, aggiornamento automatico saltato.
    echo Fai "Pull origin" da GitHub Desktop prima di continuare.
    pause
    exit /b 0
)
git diff --quiet HEAD --
if errorlevel 1 (
    echo.
    echo ATTENZIONE: ci sono modifiche non ancora pubblicate:
    git status --short
    echo.
    echo Pubblicale prima con GitHub Desktop: Pull origin, poi Commit e Push.
    echo Chiudi questa finestra per fermarti, oppure premi un tasto per continuare senza aggiornare il codice.
    pause >nul
    exit /b 0
)
for /f %%H in ('git rev-parse HEAD') do set "PULL_OLD_HEAD=%%H"
echo Aggiorno il codice ^(git pull^)...
git pull --ff-only
if errorlevel 1 (
    echo ATTENZIONE: aggiornamento non riuscito ^(rete o conflitto^). Continuo con il codice attuale.
    echo Se il problema si ripete fai "Pull origin" da GitHub Desktop.
    exit /b 0
)
if not "%~1"=="" (
    git diff --quiet %PULL_OLD_HEAD% HEAD -- "%~1"
    if errorlevel 1 (
        echo.
        echo %~1 e' stato aggiornato: chiudi questa finestra e rilancialo.
        pause
        exit /b 1
    )
)
exit /b 0
