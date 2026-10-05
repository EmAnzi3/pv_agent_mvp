# CURRENT STATE — PV Agent MVP

## Obiettivo

Agente locale per monitorare la pipeline nazionale di progetti fotovoltaici, normalizzare le fonti, generare dataset pubblicabile e dashboard GitHub Pages.

## Workflow operativo

Esecuzione locale tramite aggiorna_dashboard_senza_docker.bat; raccolta fonti; normalizzazione province/comuni; enrichment; deduplica; audit; correzioni geografiche MASE protette; gate geografico fail-closed; generazione docs/data.json e docs/index.html.

Le correzioni geografiche MASE protette vengono applicate anche dentro `app.run_pipeline`, dopo data quality/enrichment e prima di `app.dashboard_data_sync`, così il gate finale verifica dati già corretti anche quando la pipeline viene eseguita standalone.

Per Piemonte SKVIA la ricerca deve simulare il click reale sul pulsante `Ricerca` e non il semplice submit/Invio della form. La query non deve essere limitata a `REGIONE PIEMONTE`, perché l'archivio restituisce anche pratiche di `SOGGETTO GESTORE RN2000`. Le pratiche concluse vengono escluse solo quando nel dettaglio è presente evidenza esplicita di esito negativo.

Il vecchio host `www.sistemapiemonte.it` ha mostrato un failure DNS durante il run del 05/10/2026, prima ancora del parsing. Fino alla stabilizzazione Piemonte va provato separatamente con `test_piemonte_solo.bat`, che non modifica database né output pubblicati e confronta anche la raggiungibilità del nuovo endpoint pubblico SCRIVA.

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

- Validare con il prossimo run live Piemonte il recupero dei record RN2000 individuati manualmente: `2024-20/VI`, `2025-140/VI`, `2025-144/VI`, `2026-118/VI`, `2025-87/VI`.

## Prossimo passo consigliato

1. Eseguire `test_piemonte_solo.bat` finché il probe Piemonte non chiude con baseline completa.
2. Se il legacy SKVIA resta irraggiungibile ma SCRIVA risponde, migrare/affiancare il collector alla sorgente SCRIVA prima di toccare il batch globale.
3. Controllare che eventuali pratiche concluse con esito esplicitamente negativo siano escluse.
4. Solo dopo la validazione isolata, reintegrare Piemonte nel flusso globale e rieseguire `.\scripts\check_before_publish.ps1`.

