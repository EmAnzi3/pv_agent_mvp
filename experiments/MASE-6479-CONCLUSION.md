# MASE 6479 — benchmark operativo / conclusione intermedia

Repository: EmAnzi3/pv_agent_mvp, PR #4, dataset canonico invariato.

## Ambito verificato
- Fonte provvedimento: https://va.mite.gov.it/it-IT/Comunicazione/DettaglioUltimiProvvedimenti/6479
- Scheda progetto: https://va.mite.gov.it/it-IT/Oggetti/Info/11109
- Indice documentale: https://va.mite.gov.it/it-IT/Oggetti/Documentazione/11109/16669
- ID VIP 12863, Troia Sun S.r.l., agrivoltaico 54,5 MWp e accumulo 10 MW.
- L'indice dichiara **112 documenti** e sono stati enumerati **112/112 URL distinti**, pagine 1–12. Pagine 13–14 non contengono righe e non appartengono all'indice utile.
- Cinque lotti GitHub Actions: https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39213237747
- 113 elementi in totale da processare (112 documenti indicizzati + ZIP provvedimento). **101 letti al primo passaggio; 12 fallimenti specifici**: 10 limiti pypdf e 2 PDF sopra il limite di 30 MB.
- Recupero con PyMuPDF: https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39213677698 — **12/12 recuperati**, zero download irrecuperabili.
- Su due PDF problematici di 66 e 67 pagine il testo è estratto solo in una pagina ciascuno. **Almeno 131 pagine necessitano di OCR/verifica visiva**. Il totale delle pagine non testuali di tutti gli altri documenti resta da quantificare.
- L'apertura dei file non equivale all'analisi integrale di disegni, scansioni o tabelle complesse.

## Esempi di evidenze con fonte
- La società Troia Sun S.r.l. è esplicitamente identificata nel PDF 1126265, p. 1.
- P. IVA **10148261216**, PDF 1126265 p. 1.
- PEC **TROIASUNSRL@LEGALMAIL.IT**, PDF 1126265 p. 1.
- Agrivoltaico **54,5 MWp**, accumulo separato **10 MW**, opere di connessione RTN: PDF 1126265 p. 1 e PDF 1438129 p. 1.
- Parere MASE allegato al ZIP: il testo descrive progetto a circa **5 km a est di Troia**, PDF m_amte_MASE_REGISTRO_UFFICIALE_E_0118979_05-06-2026.pdf, p. 5. Non è una coordinata esatta.
- Nello stesso parere (p. 9) sono citate durate di **24 mesi** e **16 mesi** riferite a diverse attività; NON sono date assolute di avvio lavori né autorizzano una timeline univoca senza verifica del contesto.
- Nel PDF 1438129, p. 7, è presente un obbligo di comunicazione di inizio lavori con 30 giorni di anticipo: NON prova che i lavori siano iniziati.

## Valutazione degli obiettivi
| Obiettivo | Stato |
|---|---|
| Discover MASE project and links | VERIFICATO |
| Enumerate all published index links | VERIFICATO 112/112 |
| Open ZIP and PDFs | VERIFICATO (con parser di riserva) |
| Recover explicitly named company and VAT/PEC | EVIDENZA PRESENTE |
| All company roles and verified contacts | NON ANCORA VALIDATO |
| Technical details beyond headline | PARZIALE |
| Coordinates and precise location | NON FORZATE, da verifica su elaborati |
| Reliable project schedule | NON ANCORA VALIDATO |
| Full coverage scanned/graphic pages | NON ANCORA RAGGIUNTA |
| Compare validated new facts against current record | NON ANCORA COMPLETATO |

## Decisione
Crawl4AI è utile per il **crawling HTML e la scoperta degli allegati**. Non è sufficiente, da solo, per soddisfare il requisito aziendale di dossier tecnico-commerciale affidabile: servono paginazione, unzip, parser PDF multi-engine, OCR, estrazione di entità e relazioni e gate probatorio.

Nessuna promozione nel dataset canonico. PR deve restare Draft finché non sono completati OCR, attribuzione verificata delle aziende/contatti, cronoprogramma e valutazione quantitativa contro la scheda di PV Agent.
