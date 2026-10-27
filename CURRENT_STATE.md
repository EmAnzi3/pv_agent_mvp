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

## Toolbox documentale e schede progetto

- `doc_intel/` costruisce le schede dai documenti ufficiali; i documenti sono eliminati dopo l'estrazione, la memoria (manifest, fatti per SHA-256, schede, coda) è sul branch `doc-intel-state`. La pagina pubblica legge le schede da `schede/live_index.json` di quel branch.
- Cloud (`doc-intel-batch.yml`): nuovi progetti a ogni push di `docs/data.json`, arretrato ogni notte per potenza decrescente, ricontrollo settimanale la domenica. PC (`analizza_fonti_locali.bat`): Sicilia, Sistema Puglia, Basilicata (IP esteri rifiutati); Sicilia con limite di 1 progetto e 150 documenti al giorno.
- Batch locale: `[3H2/8]` esclude i progetti MASE archiviati; `[3ZQ/8]` mantiene le chiavi progetto del run precedente; `[3ZR/8]` riempie solo i campi vuoti dalle schede (cache `reports/schede_cache`, fuori da Git).
- Esecuzione: ruoli EPC/impresa con evidenza A/B nella scheda («Chi costruisce»); notizie di stampa compatibili (livello C) da `doc-intel-news.yml` in `news/matches.json` del branch di memoria.
- Vicini al cantiere: `doc_intel/readiness.py` (eseguito da `doc-intel-ferx.yml` dopo l'abbinamento FER X) scrive `readiness/index.json` sul ramo di memoria; regole e punteggi documentati in testa al modulo.
- Graduatorie FER X: `doc_intel/data/ferx_awarded.json` importato dai PDF GSE (`python -m doc_intel ferx-import`), abbinato ogni giorno e dopo ogni push (`ferx/matches.json`, solo procedure fotovoltaiche); graduatoria eolica abbinata al radar eolico in `ferx/wind_matches.json`, mostrata da `docs/wind/assets/ferx-wind-v08.js`.
- Censimento eolico: `doc_intel/wind_census.py` + `app/collectors/mase_wind.py`, workflow `wind-census.yml` (settimanale) e `wind-cards.yml` (schede notturne, stato coda `queue/wind_*.json`); dati `wind/census.json`, prontezza `readiness/wind_index.json`; pagina `docs/wind/assets/census-v09.js` (+ copia del pannello scheda `scheda-panel.*`, identica a `doc_intel/scheda_panel.*`, verificata in CI).
- Radar eolico: sezione `docs/wind/`, aggiornamento quotidiano `wind_agent_watch.yml`; il canonico eolico non viene modificato in automatico.

## Problemi aperti

- Dataset finale 05/10/2026 15:09: baseline Piemonte distinta e completa; residuo unico osservato: Isola Sant'Antonio ancora con provincia `DC` perché il valore errato era già persistito nel DB prima della correzione del parser. La migrazione pre-run ora ripara/azzera le province Piemonte non valide e il gate finale blocca il batch se la baseline o le province risultano errate.
- Probe isolato Piemonte del 05/10/2026: PASS. Collector live = 17 record; baseline RN2000 completa (`2024-20/VI`, `2025-140/VI`, `2025-144/VI`, `2026-118/VI`, `2025-87/VI`).
- Run globale del 05/10/2026: FAIL logico sul Piemonte. I 17 record del collector venivano collassati a valle perché condividevano il medesimo URL archivio; il report mostrava un record ibrido con localizzazione/potenza/proponente provenienti da pratiche diverse. Correzione in corso sulla PR #3: chiave stabile da autorità+codice pratica, strategia project_key basata su external_id e migrazione idempotente della vecchia chiave URL collassata.

## Prossimo passo consigliato

1. Fare Fetch origin sul branch `fix/piemonte-search-button-rn2000`.
2. Rieseguire una volta il batch globale: la migrazione correggerà il valore storico `DC` già presente nel DB.
3. Il gate finale verifica automaticamente i cinque record baseline, le province Piemonte e l'assenza di proponenti derivati da frammenti di potenza.
4. Se il batch chiude con `[run-pipeline] OK`, procedere al merge della PR #3.
