# Fase 5 — Copertura delle fonti regionali (un campione reale per fonte)

Aggiornato 20/10/2026. Metodo: `doc-intel-probe.yml` (HTTP + browser, chiamate API, un salto di link) su un record del canonico per fonte; dossier completo con `doc-intel-run.yml` sulle fonti con adapter.

| Fonte | Record canonici | Campione | Metodo | Stato | Evidenza |
|---|---|---|---|---|---|
| MASE | 306 | Troia Sun, Tortona 2, Aliai | indice paginato per procedura | **Supportata** | 112/112, 211/211, 398/398 |
| Emilia-Romagna | 33 | pratica 6902 | indice HTML con sezioni e totali dichiarati | **Supportata** | 140/140 in 9 sezioni, gate PASS ([run 39380150107](https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39380150107)) |
| Lazio | 320 | Box `VIA-060-2019` | cartella Box: elenco JSON incorporato, ricorsione sottocartelle | **Supportata** | 492/492 file (= totale Box), 81 cartelle, 4.080 pagine, gate PASS ([run 39380758823](https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39380758823)) |
| Veneto | 26 | Nextcloud `PROC_62-26` | condivisione Nextcloud via WebDAV | **Supportata** | 72 file, 1.058 pagine, gate PASS ([run 39275203067](https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39275203067)) |
| Toscana (PATTI) | 28 | avviso 110 | SPA con API pubblica `servizi.toscana.it/pub-api/suap/.../eventiPubblici/<id>` | Parziale: adapter API da scrivere | probe v2 |
| Lombardia (SILVIA) | 74 | scheda-sintesi | SPA (AngularJS + JSP) | Parziale: individuare servizio dati | probe v2 |
| Umbria | 7 | 94-2026-003 | allegati nell'elenco procedimenti (1 salto) | Parziale | 39 link a un salto |
| Toscana ATOS | 264 | scheda FER | rimanda a MASE | Riuso adapter MASE quando il link è presente | probe v2 |
| Calabria, Sardegna | 25, 5 | pagine avviso | documenti non collegati dalla pagina | Parziale: navigazione dedicata | probe v2 |
| Campania | 9 | prg_10148 | link `allegati/` vuoto | Parziale | probe v1 |
| Sicilia, Sistema Puglia, Basilicata | 288, 70, 15 | verificati a mano da rete italiana il 20/10 | Basilicata: lista «Allegati» PDF/ZIP (fino a 460 MB) su HTTP; Sistema Puglia: una determina PDF per record; Sicilia: lista Fabrik filtrata | **Raggiungibili solo da IP italiani** (timeout da GitHub/Azure): adapter pronti, serve runner self-hosted in Italia | probe v2 + screenshot utente |
| Piemonte | 13 | — | URL generico d'archivio nel canonico | **Bloccata a monte** (collector) | — |
| Terna Econnextion | 82 | — | dataset, non fascicolo | Non applicabile | — |

Copertura documentale supportata: MASE + Lazio + Emilia-Romagna + Veneto = **685 record su 1.565 (44%)**.
