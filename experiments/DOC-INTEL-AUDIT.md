# Fase 1 — Audit PR #4 e base per la toolbox `doc_intel`

**Data:** 19/10/2026 · **PR:** #4 (Draft, head `0454ebe`, 51 commit, 34 file, `mergeable_state: clean`) · **Branch:** `experiment/crawl4ai-mase-record`

Ultimi 12 workflow del 19/10 (run 39225354xxx–39225355305): tutti `success`. Verde = acquisizione eseguita, non validazione semantica.

## 1. Vincoli dell'ambiente in cui è stato fatto l'audit

| Vincolo | Effetto | Rimedio |
|---|---|---|
| Push verso `EmAnzi3/pv_agent_mvp` rifiutato (app GitHub Claude non installata) | impossibile creare branch/PR e lanciare workflow | installare l'app GitHub o applicare la patch consegnata |
| `va.mite.gov.it` non raggiungibile dalla sandbox (proxy 403) | nessun download/rilettura PDF fuori da GitHub Actions | i run reali girano su Actions (`doc-intel-run.yml`) |
| Artifact e log Actions non scaricabili (storage Azure bloccato) | impossibile ispezionare i JSON dei batch/OCR già prodotti | ispezione da browser o nuovo run con dossier unico |
| PyPI non raggiungibile | test locali senza PyMuPDF | CI testa con e senza PyMuPDF |

## 2. Criticità del codice sperimentale

1. **12 script monouso con ID hardcoded** (`BAD_IDS`, lista di 35 ID OCR, `PROCEDURES={"19438":87,"12932":124}`, URL indice fissi). Non generalizzabili: ogni nuovo progetto richiederebbe nuovi script e nuovi workflow.
2. **Nessuna cache del testo.** I batch conservano al massimo 1.500 righe "keyword" per lotto (9.000 per l'OCR); il testo integrale viene scartato. Ogni nuova domanda semantica ha richiesto un nuovo download → origine dei 12 workflow.
3. **OCR a ~94 DPI** (`fitz.Matrix(1.3)`) con soglia 70 caratteri. Insufficiente per cartigli e tabelle di coordinate su tavole: "780 pagine OCR eseguite" non equivale a "780 pagine lette".
4. **Troia Sun: i "21 campi" sono fatti scritti a mano** (`FACTS` in `mase_semantic_compare.py`) e poi ricercati come stringhe. È un ottimo **gold set**, non una misura di recall dell'estrazione. Riutilizzato come tale in `doc_intel/gold/mase-6479.json`.
5. **Bug latente di matching canonico in `mase_semantic_compare.py`:** `URL in json.dumps(r)` è un match per sottostringa (stessa classe del falso duplicato già corretto su 8788): `.../6479` è sottostringa di `.../64790`. `doc_intel.canonical` usa solo l'URL esatto.
6. **Parser incoerenti:** 6479 su pypdf, 8788 su PyMuPDF → conteggi pagine/testo non confrontabili tra benchmark.
7. **Duplicazioni:** 4 implementazioni del download con cap, 3 della paginazione, 3 dizionari di keyword divergenti.
8. **Trigger:** tutti i workflow sono su `pull_request` con accesso live a MASE: ogni push ricrawla tutto.
9. **Punto di forza da conservare:** `evidence_toolbox.py` (contratti fail-closed). Le stesse regole sono ora applicate in `doc_intel` (P. IVA, scala di precisione geografica, durata ≠ data, ruoli esecutivi non inferibili); il file resta per retrocompatibilità dei test.

## 3. Cosa sostituisce `doc_intel`

| Esigenza mandato | Modulo | Stato |
|---|---|---|
| Discovery documentale generica (procedure multiple, paginazione, totali dichiarati, menu esclusi) | `adapters/mase.py` | implementato, testato offline con fixture HTML; **da validare live** |
| Manifest persistente, SHA-256, cache download, rilevamento revisioni, retry limitati, errori per file | `store.py`, `fetch.py` | implementato e testato |
| Firma formato, ZIP annidati, inventario membri scartati | `fetch.py` | implementato e testato |
| Testo per pagina, fallback parser, OCR selettivo a 300 DPI con budget e cap pixel | `parse.py` | implementato; OCR reale testato su poppler+tesseract |
| Aziende, P. IVA, PEC, telefoni, ruoli solo da etichetta, persone solo legate a un ruolo | `extract/entities.py` | implementato e testato |
| Potenze FV DC/AC, BESS MW/MWh, eolico separati; moduli, inverter, tracker, tensione RTN, superfici | `extract/technical.py` | implementato e testato |
| Coordinate UTM/latlon, CRS, oggetto della coordinata, poligoni da tabelle vertici | `extract/geo.py` | implementato e testato (conversione UTM→WGS84 pura Python, round-trip <1e-5°) |
| Durate per ambito, date con natura (evento / pianificata / modello PSC / scenario / condizionata) | `extract/schedule.py` | implementato e testato |
| Versioni e conflitti senza scelta arbitraria; hint non vincolante dagli atti | `reconcile.py` | implementato e testato |
| Confronto canonico per URL esatto, 5 classi | `canonical.py` | implementato e testato |
| Gate bloccanti (EPC, P. IVA, coordinate non pertinenti, date modello) | `gates.py` | implementato e testato |
| Dossier JSON versionato `pv-dossier/1.0` + Markdown | `pipeline.py`, `dossier.py` | implementato e testato |
| Precision/recall su gold set, fatti vietati, conflitti attesi | `metrics.py`, `gold/` | implementato; gold Troia (18 voci ancorate), Tortona provvisorio (6) |

Limiti dichiarati: estrazione deterministica (regex + contesto). Nessun LLM introdotto: va valutato **dopo** la prima misura di recall reale, solo sui casi residui (relazioni società-ruolo, tabelle complesse). Docling non introdotto: nessun beneficio misurabile dimostrato finora, dipendenza pesante.

## 4. Inventario fonti (per volume nel canonico `docs/data.json`, 1.565 record)

| Fonte | Record | URL nel canonico | Accesso documenti (analisi statica, non verificata live) | Priorità proposta |
|---|---|---|---|---|
| MASE (`mase` + `mase_provvedimenti`) | 131 + 175 | scheda/provvedimento per progetto | **adapter implementato** | 1 |
| Lazio | 320 | cartella `regionelazio.app.box.com/v/...` per pratica | documenti direttamente nella share Box; serve browser o API shared-link | 2 |
| Sicilia (SI-VVI) | 288 | lista Fabrik filtrata per oggetto | pagine Fabrik + allegati; probabile sessione | 3 |
| Lombardia (SILVIA) | 74 | SPA con route `#/scheda-sintesi?idProgetto=` | richiede browser (Playwright) | 4 |
| Puglia (Sistema Puglia) | 70 | `DettaglioInfo?id=` | HTML con link "Scarica" | 5 |
| Toscana ATOS | 264 | scheda impianto FER | anagrafica, documenti VIA non garantiti | da valutare |
| Emilia-Romagna | 33 | `viavasweb/ricerca/dettaglio/<id>` | dettaglio con documentazione | 6 |
| Toscana (PATTI) | 28 | avviso pubblico per id | allegati avviso | 7 |
| Veneto | 26 | share Nextcloud `sharing.regione.veneto.it/s/<token>` | cartella documenti diretta (WebDAV pubblico) | 8 (facile) |
| Calabria / Basilicata / Campania / Umbria / Sardegna | 25 / 15 / 9 / 7 / 5 | pagine per pratica | HTML, da verificare | 9 |
| **Piemonte** | 13 | **URL generico dell'archivio, identico per tutti i record** | identità pratica non ricostruibile dal canonico | **bloccata** finché il collector non salva l'URL/ID della singola pratica |
| Terna Econnextion | 82 | download center | dataset, non fascicolo documentale | non applicabile |

**Challenge sull'ordine del mandato** (Veneto, Piemonte, Toscana, Lombardia, Emilia-Romagna): pesa 174 record su 1.565 (11%, escluso Toscana ATOS) e include Piemonte, oggi bloccata a monte. MASE generalizzato + Lazio + Sicilia coprono 914 record (58%) del canonico. Proposta: MASE → Veneto (facile, utile come secondo pattern) → Lazio → Sicilia → Lombardia.

## 5. Soglie di accettazione proposte (derivate dai benchmark)

- **Acquisizione:** censiti = dichiarati per ogni procedura (raggiunto 112/112 e 211/211 → soglia 100%, ogni scarto blocca); download falliti = 0 dopo retry o elencati; pagine OCR in attesa = 0.
- **Fatti vietati** (EPC/BoP senza aggiudicazione documentata) = 0 → blocco.
- **Conflitti attesi** (54,9 vs 54,5 MWp; doppia P. IVA Margisolar) rilevati al 100% → blocco.
- **Recall ancorata** sul gold Troia: soglia da fissare **dopo** il primo run reale; proposta iniziale ≥0,80 per società/contatti/tecnico, nessuna soglia sul cronoprogramma finché il gold non è esteso.
- **Precision:** misurabile solo su campi annotati in modo esaustivo; da costruire su Troia per P. IVA/PEC.
