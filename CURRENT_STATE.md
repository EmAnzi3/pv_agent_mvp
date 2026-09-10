# CURRENT STATE — PV Agent MVP

## Obiettivo

Agente locale per monitorare la pipeline nazionale di progetti fotovoltaici, normalizzare le fonti, generare dataset pubblicabile e dashboard GitHub Pages.

## Workflow operativo

Esecuzione locale tramite aggiorna_dashboard_senza_docker.bat; raccolta fonti; normalizzazione province/comuni; enrichment; deduplica; audit; correzioni geografiche MASE protette; gate geografico fail-closed; generazione docs/data.json e docs/index.html.

Le correzioni geografiche MASE protette vengono applicate anche dentro `app.run_pipeline`, dopo data quality/enrichment e prima di `app.dashboard_data_sync`, così il gate finale verifica dati già corretti anche quando la pipeline viene eseguita standalone.

## File e cartelle critiche

- aggiorna_dashboard_senza_docker.bat
- app/
- scripts/
- data/pv_agent.sqlite
- docs/data.json
- docs/index.html
- reports/

## Cose da non rompere

- Non mischiare fonti raw, dati normalizzati e dati pubblicati.
- Non modificare manualmente output generati senza aggiornare la pipeline.
- Non esporre dettagli tecnici nella dashboard destinata agli utenti finali.
- Preservare compatibilità GitHub Pages.
- Non spostare il gate geografico protetto prima delle relative correzioni: deve verificare l'output già normalizzato.

## Stato corrente

- Stato: da aggiornare dopo il prossimo giro operativo.
- Ultima verifica manuale: da compilare.
- Ultima pubblicazione: da compilare.
- Ultimo commit stabile noto: da compilare.

## Problemi aperti

- Da compilare.

## Prossimo passo consigliato

1. Eseguire `.\scripts\check_before_publish.ps1`.
2. Controllare `git status` e `git diff --check`.
3. Aggiornare questa pagina se cambia il workflow.
4. Committare con messaggio piccolo e tematico.

