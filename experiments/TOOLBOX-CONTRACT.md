# Toolbox documentale riutilizzabile — stato e gate

**Destinazione:** `pv_agent_mvp` (solo branch sperimentale della PR #4). Non modificare automaticamente `docs/data.json` o il database.

## Componenti verificati su MASE Troia Sun

| Funzione | Implementazione | Stato |
|---|---|---|
| Lettura HTML e scoperta link | Crawl4AI | test live riuscito |
| Navigazione e paginazione | Playwright, `?pagina=N` | 12 pagine / 112 link verificati |
| Identità e deduplica allegati | URL + SHA256 originali | primo livello operativo |
| Download e apertura ZIP | requests + zipfile, riconoscimento firma | ZIP provvedimento aperto |
| PDF digitali | pypdf | parser principale |
| PDF complessi | PyMuPDF | recuperati 12 errori del primo parser |
| Scansioni | Tesseract italiano+inglese | 131 pagine OCR su due documenti |
| Evidenze e qualità | `experiments/evidence_toolbox.py` | contratti e test, non ancora integrato all'intera pipeline |
| Estrattore strutturato multi-documento | **da sviluppare** | non dichiarare completato |

Il crawler non equivale al parser documentale e il parser non equivale alla comprensione semantica. Ogni passaggio deve dichiarare cosa ha veramente elaborato.

## Contratto per ogni fatto

Valore, tipo, ruolo specifico, fonte URL, SHA del documento, pagina originale, estratto testuale, versione/data, stato (candidate/verified/conflicting/not_found), livello di precisione e spiegazione dell'incertezza.

### Georeferenziazione

Usare una scala di precisione: coordinate esplicite > particelle/area catastale > indirizzo > località > Comune > provincia > regione. Conservare il livello effettivamente provato. Mai sostituire automaticamente con centroide comunale o coordinate dedotte dalle distanze. Se gli elaborati riportano coordinate, estrarre anche sistema di riferimento (WGS84/UTM, fuso, EPSG), entità cui si riferiscono (cabina, accesso, baricentro, confine) e pagina/tavola: nessun pin di impianto da una coordinata di cabina.

### Cronoprogramma

Durata `24 mesi` non equivale a data certa di avvio lavori. Separare costruzione, dismissione, commissioning, prescrizioni e milestones autorizzative. Valori relativi mantengono un evento-condizione e non producono COD arbitrari.

### Aziende e contatti

Distinguere proponente, sviluppatore, consulente, progettista, DL, geologo, EPC, BoP, fornitori, installatori, O&M. Una P. IVA va collegata alla società esatta e verificata (check-digit + fonte); recapiti associati al soggetto/ruolo senza promuovere persone citate incidentalmente. Appalti solo con affermazione diretta project-specific.

## Quando è davvero chiuso un link

1. Tutti i documenti e revisioni dell'indice sono inventariati e la paginazione è verificata.
2. Per ogni file: download con hash o motivo di fallimento; apertura oppure impedimento concreto; pagine digitali/OCR e pagine senza contenuto leggibile conteggiate.
3. Per ogni campo richiesto: evidenza verificata o **non disponibile/non determinabile**, esplicitando i documenti effettivamente controllati.
4. Conflitti risolti o dichiarati, versioni storiche conservate, nessun dato inferito spacciato per certo.
5. Report di confronto col record esistente, con fatti nuovi corretti, duplicati, falsi positivi e mancanze.
6. Test di regressione e check della PR verdi; solo allora lo stato è `ready_for_review`. Non equivale a merge automatico.

## Troia Sun: quadro odierno

- Indice 112/112, ZIP + 112 documenti aperti con fallback.
- Due PDF con 131 pagine OCR recuperate (zero errori di esecuzione OCR).
- Provenienza e recupero: `MASE-6479-CONCLUSION.md` e `MASE-6479-DOSSIER.md`.
- **Non chiuso**: attribuzione automatica di tutti i ruoli/contatti, validazione di coordinate e loro oggetti, estrazione di cronoprogrammi e riconciliazione campo per campo con dati già pubblicati.
- Lo stato rimane `incomplete`; un `success` GitHub Actions non è sufficiente.

## Piano di consolidamento

Non effettuare una nuova scansione cieca dei 112 PDF a ogni run. Salvare manifest e cache SHA persistenti, processare solo file nuovi/variati, introdurre estrattori specializzati a seguito dei parser e una coda di review delle ambiguità. Il motore di estrazione deve restituire un JSON di dossier più le evidenze; i crawler restano adapter di acquisizione.
