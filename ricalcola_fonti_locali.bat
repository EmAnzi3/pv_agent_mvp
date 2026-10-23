@echo off
REM ==========================================================
REM PV Agent - ricalcola le schede gia' create per Sicilia, Sistema Puglia e Basilicata
REM con le regole di estrazione piu' recenti (es. tempistica dei lavori).
REM Doppio clic: rielabora fino a 15 progetti per lancio. Le schede restano visibili nel frattempo.
REM ==========================================================
cd /d "%~dp0"
call analizza_fonti_locali.bat 15 ricalcola
