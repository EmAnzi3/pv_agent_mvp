# Troia Sun MASE 6479 — Verifica semantica e delta PV Agent

**Stato della revisione:** PASS per confronto documentale di 21 campi espliciti; **non** significa che tutti i fatti commerciali reperibili siano stati esauriti. Nessun record canonico modificato.

**Run:** https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39217556506

## 1. Baseline effettiva di PV Agent

Verificata direttamente dal `docs/data.json` della PR:

- Fonte `mase_provvedimenti`, link esatto `https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6479`.
- Proponente `TROIA SUN S.R.L.`, regione Puglia, provincia FG, Comune Troia.
- Potenza canonica **54,5 MW**. Il titolo, e non un campo BESS autonomo, contiene la potenza d'accumulo di 10 MW.
- `project_type` è la procedura VIA PNIEC-PNRR, **non** un'indicazione normalizzata del tipo agrivoltaico.
- Nessun campo canonico dedicato a contatti, ruoli professionali, dettagli tecnici, coordinate validate, durata lavori/COD.

## 2. Confronto campo per campo (21 evidenze con ancora testuale nel documento)

Il workflow [semantic compare](https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39217556506) ha controllato documento, pagina e presenza letterale dell'evidenza nell'estrazione original-source.

| Campo | Valore | Fonte, pagina | Rispetto al canonico |
|---|---|---|---|
| Proponente | Troia Sun S.r.l. | [1126265](https://va.mite.gov.it/File/Documento/1126265), p.1 | Già presente |
| P.IVA proponente | **10148261216** | 1126265, p.1 | Nuovo |
| PEC proponente | **troiasunsrl@legalmail.it** | 1126265, p.1 | Nuovo |
| FV | **54,5 MWp** | 1126265, p.1 | Potenza già presente (tipo MWp da preservare) |
| BESS | **10 MW** | 1126265, p.1 | Già nel titolo, da strutturare in campo separato |
| Progettista | **Arch. Antonio Manco** | [1126247](https://va.mite.gov.it/File/Documento/1126247), p.1 | Nuovo |
| Geologo/tecnico | **Geol. Fabio Di Feo** | 1126247, p.1 | Nuovo |
| Contatto progettista | **a.manco@iprojectsrl.com** | [1126229](https://va.mite.gov.it/File/Documento/1126229), p.2 | Nuovo |
| Recapito nei cartigli | **3384117245** | 1126229, p.2 | Nuovo; verificare destinatario/contesto prima dell'uso |
| PEC geologo | **geofabiodifeo@pec.epap.it** | [1126165](https://va.mite.gov.it/File/Documento/1126165), p.2 | Nuovo |
| Società che firma elaborati | **IPROJECT S.r.l.** | [1126228](https://va.mite.gov.it/File/Documento/1126228), p.1 | Nuovo; ruolo EPC/BoP non provato |
| P.IVA IPROJECT | **11092870960** | 1126228, p.1 | Nuovo |
| PEC IPROJECT | **I-project@legalmail.it** | 1126228, p.1 | Nuovo |
| Comune | **Troia (FG)** | 1126265, p.1 | Già presente |
| Costruzione (durata) | **circa 24 mesi** | [1126259](https://va.mite.gov.it/File/Documento/1126259), p.188 | Nuovo; decorrente dalle autorizzazioni necessarie |
| Dismissione/ripristino | **circa 16 mesi** | [1438131](https://va.mite.gov.it/File/Documento/1438131), p.9 | Nuovo; NON durata costruzione |
| Inverter | **168** unità | [1126262](https://va.mite.gov.it/File/Documento/1126262), p.15 | Nuovo |
| Potenza inverter | **350 kW** per unità | 1126262, p.15 | Nuovo |
| Modulo FV | **0,710 kWp** | 1126259, p.19 | Nuovo |
| Riferimento coordinate in elaborato | **UTM-WGS84 fuso 33** | [1126263](https://va.mite.gov.it/File/Documento/1126263), p.4 | Nuovo, NON coordinate del baricentro |
| Tensione in opere di rete | **36 kV** | [1126180](https://va.mite.gov.it/File/Documento/1126180), p.12 | Nuovo |

**Conteggio automatico:** 21 corrispondenze documentali; 4 valori già presenti nel contenuto del record, 17 candidati incrementali. **Questo è un confronto letterale, non uno score di precision/recall su tutti i documenti.**

## 3. Valori non determinabili o ancora da riconciliare

| Campo | Stato corretto |
|---|---|
| Coordinate del centroide/perimetro dell'impianto | **Non validate**. Esistono elaborati con UTM WGS84 fuso 33, ma occorre associare ogni coppia numerica all'oggetto corretto (es. cabine). Non assumere che una coordinata di cabina rappresenti l'impianto. |
| EPC, Civil BoP, Electrical BoP, installatori, fornitori | **Non documentati come affidatari sulla base delle evidenze finora validate**. Non trasformare IPROJECT in appaltatore. |
| Inizio reale cantiere, fine e COD | **Non determinati**. Durata 24 mesi è condizionata al completamento delle autorizzazioni; non è una data assoluta. |
| Piena congruenza tra revisioni/versioni | **Non completamente risolta**; non usare elaborati storici per sovrascrivere il 54,5 MW del provvedimento 2026. |
| Completezza semantica di tavole/scansioni | **Non dimostrata**. 131 pagine OCR riuscite non implicano lettura sicura di tutti i disegni e le tabelle. |

## 4. Decisione di rilascio

- **PASS:** scoperta 112/112 URL; acquisizione ZIP e PDF; recupero 12 PDF con parser di riserva; OCR 131 pagine; 21 campi con evidenze e confronto con record esistente.
- **OPEN:** automatizzare relazioni azienda-ruolo-contatto, verifica geometrie/coordinate, risoluzione versioni, benchmark dell'accuratezza su una scheda gold set indipendente.
- **Nessuna autopromozione** dei 17 candidati nel canonico e nessun merge della PR.
- Il test è già sufficiente per considerare Crawl4AI **utile nell'acquisizione**, ma non ancora per considerare autonoma l'intera capacità di intelligence.

Riferimenti operativi: `MASE-6479-DOSSIER.md`, `MASE-6479-CONCLUSION.md`, `TOOLBOX-CONTRACT.md`. 
