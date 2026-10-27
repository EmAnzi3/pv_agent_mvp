# CHANGELOG — PV Agent MVP

Formato consigliato: voci brevi, orientate a cosa cambia per il progetto.

## Unreleased

- 2026-10-27 — Correzioni di potenza confermate dall'utente: Lazio VIA-064-2019 36,969 MWp (ridotta da 46,192), Emilia-Romagna 6658 San Giovanni in Persiceto 21,03 MWp (revisione del layout). Esclusione MASE: le procedure secondarie archiviate (modifica, variante, valutazione preliminare) non tolgono il progetto; scarico delle schede sul PC corretto (elenco del ramo di memoria troncato da GitHub).

- 2026-10-27 — Graduatorie FER X (GSE, Tabella A, 562 impianti ammessi): importate da PDF (`python -m doc_intel ferx-import`, il sito GSE rifiuta i download dal cloud) e abbinate ogni giorno ai progetti del radar solo per P. IVA del proponente o per stessa società nello stesso comune; etichetta «FER X», filtro «Solo con incentivo FER X assegnato» e sezione nella scheda.

- 2026-10-26 — Progetti molto grandi letti su più notti (massimo 120 documenti nuovi per run, rimessi in coda la notte dopo); schede: atti nei titoli senza «n.», tabelle delle fasi ripulite, potenza per blocco e pali della recinzione non più presi come valori dell'impianto; radar eolico con la copertura esecutiva v0.7.

- 2026-10-26 — Notizie di stampa (livello C, da verificare): ogni giorno `doc-intel-news.yml` legge pv-magazine.it, QualEnergia, Rinnovabili.it e SolareB2B, tiene gli articoli su cantieri/contratti fotovoltaici e li abbina a un progetto solo se comune e potenza (o proponente) compaiono nello stesso passaggio con parole di cantiere; esclusi riepiloghi VIA, graduatorie e offerte di lavoro. Etichetta «Stampa», sezione nella scheda, opzione nel filtro EPC.
- 2026-10-26 — Evidenza EPC/impresa esecutrice: ruoli esecutivi classificati A (atto di cantiere o dichiarazione formale) / B (cartiglio degli elaborati); esclusi proponente, gestori di rete e committenti; sezione «Chi costruisce» nella scheda, etichetta EPC e filtro dedicato in dashboard.
- 2026-10-26 — Esclusi dal radar i progetti MASE con tutte le procedure «Archiviata» (passo `[3H2/8]`, audit `reports/mase_archived_excluded.csv`); stato della procedura scritto sui record MASE.
- 2026-10-26 — Radar eolico migrato come sezione `docs/wind/` (dashboard, dati canonici v0.5, motore di monitoraggio, controlli); aggiornamento quotidiano spostato qui da project_radar_mvp.
- 2026-10-26 — Portale VIA Sicilia: accesso prudente dal PC (1 progetto e 150 documenti al giorno, 5 s di pausa, niente elaborati grafici).
- 2026-10-23 — Chiavi progetto stabili tra i run (passo `[3ZQ/8]`), copie delle schede fuori da Git (`reports/schede_cache`), collector Toscana sulla nuova API pubblica, pulizia falsi Taranto con provincia da elenco ISTAT.

- 2026-10-21 — Schede progetto in dashboard: pulsante «Scheda» accanto ad «Apri» per i progetti analizzati (sintesi, società e contatti, cronoprogramma, ubicazione per lotto, stato del progetto). Le schede si costruiscono su GitHub Actions: i nuovi progetti dopo il push di `docs/data.json`, l'arretrato ogni notte (35 progetti, potenza decrescente). Nuovo passo `[6B/8]` in `aggiorna_dashboard_senza_docker.bat` che scarica in `docs/schede` le schede pronte.
- doc-intel: scheda progetto v2 (sintesi dei dati essenziali, società con i rispettivi contatti, Gantt ricostruito dal cronoprogramma vettoriale o dalle tabelle con date, ubicazione con link a Google Maps); rianalisi selettiva dei documenti (`--reanalyse-match`).
- 2026-10-20 — `doc_intel`: i documenti originali vengono eliminati dopo l'estrazione; memoria persistente per progetto (manifest, fatti per SHA-256) con rilevamento di documenti nuovi, modificati o rimossi; adapter regionali Emilia-Romagna, Lazio (Box), Veneto (Nextcloud), regole Basilicata e Sistema Puglia.
- 2026-10-19 — `doc_intel`: run resumibili entro i limiti CI, controllo anagrafico P. IVA su VIES, correzioni di precisione dai primi run reali; dossier Tortona 2 chiuso per review umana.
- 2026-10-19 — Aggiunta toolbox documentale `doc_intel` (review-only): adapter MASE generico senza ID hardcoded, cache SHA-256 incrementale, OCR selettivo a 300 DPI, estrattori di società/P. IVA/ruoli, configurazione tecnica, coordinate con oggetto e CRS, cronoprogrammi con natura temporale, conflitti, confronto col canonico per URL esatto, gate bloccanti, dossier JSON/MD, gold set Troia Sun e Tortona 2, test offline e workflow CI. Nessuna modifica al dataset pubblicato.
- 2026-10-15 — Impostata a 19,8 MW la potenza del progetto Porto Viro FEV (FOOD ENERGY VALLEY), su indicazione dell'utente: correzione persistente associata all'URL esatto e applicata dal batch prima del ricalcolo dei riepiloghi e dei report.
- 2026-10-05 — Rafforzato il collector Piemonte SKVIA: ricerca tramite l'evento reale del pulsante `Ricerca`, rimosso il filtro esclusivo `REGIONE PIEMONTE` per acquisire anche `SOGGETTO GESTORE RN2000`, ed esclusione dei procedimenti conclusi quando il dettaglio contiene un esito esplicitamente negativo. Aggiunto regression check sui record mancanti rilevati manualmente.
- 2026-10-02 — Aggiunta correzione documentata della potenza per il nuovo progetto AG 31 di Castagnaro: 21,13886 MWp (21.138,86 kWp), mantenendo l'override AG 31 esistente.
- 2026-09-10 — Corretta e protetta la localizzazione di Sesto al Reghena: PN / Friuli-Venezia Giulia indipendentemente dall'URL MASE; la correzione viene applicata prima del gate bloccante sia nella pipeline standalone sia nel batch completo.
- 2026-06-02 — Aggiunti file minimi di manutenzione repository: `CURRENT_STATE.md`, `AGENTS.md`, `CHANGELOG.md` e `scripts/check_before_publish.ps1`.
