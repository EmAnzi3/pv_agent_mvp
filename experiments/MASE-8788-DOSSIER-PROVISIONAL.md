# Tortona 2 (MASE 8788) — dossier provvisorio, confronto con il canonico

**Stato: NON chiuso.** I gate che richiedono la rilettura dei documenti sorgente restano aperti: questa sessione non raggiunge `va.mite.gov.it` né gli artifact dei run. Il dossier definitivo sarà prodotto da `doc-intel-run.yml` (job `tortona-2`) con la toolbox `doc_intel` e confrontato con `doc_intel/gold/mase-8788.json`.

## 1. Record canonico (verificato su `docs/data.json` della PR)

Corrispondenza per URL esatto `https://va.mite.gov.it/it-IT/Oggetti/Info/8788`: **1 record**, chiave `url:910815a0c4750a6a`. (Il secondo record che contiene la stringa `8788` è Brindisi Sud HV, `url:7335001a607c8788`: falso positivo da sottostringa, già escluso.)

| Campo canonico | Valore |
|---|---|
| proponent | Margisolar S.r.l. |
| power_mw | 60.0 |
| region / province | Piemonte / AL |
| municipalities | **TORTONA** |
| project_type | Verifica di Ottemperanza (PNIEC-PNRR) |
| status | vuoto |

## 2. Confronto campo per campo

| Campo | Canonico | Fonte | Classe | Note |
|---|---|---|---|---|
| Proponente | Margisolar S.r.l. | titolo/scheda MASE; elaborato 706412 p.4 | **Confermato** | |
| Potenza | 60 MW | titolo "60 MW"; elaborati 60 MW DC / 50 MW AC (805858 p.1) | **Confermato** (= DC/titolo) | il canonico non dice DC/AC: proporre campi separati DC 60 / AC 50 dopo rilettura |
| Comuni | TORTONA | titolo della stessa scheda: Tortona, Pozzolo Formigaro, Frugarolo (AL) | **Nuovo verificato** | proposta: aggiungere Pozzolo Formigaro e Frugarolo. Fonte = titolo ufficiale già presente nel record canonico |
| Provincia / Regione | AL / Piemonte | titolo (AL) | **Confermato** | |
| Tecnologia | — | "agrivoltaico" (805858 p.1) | candidato | da confermare in run |
| P. IVA proponente | — | 03920651209 (805858 p.1) **e** 03920631201 (805857 p.1) | **Conflittuale** | entrambe superano il check digit (verificato): il checksum non discrimina; serve Registro Imprese/VIES |
| PEC proponente | — | margisolar@legalmail.it (805873 p.9) | candidato | |
| Società di ingegneria | — | INTEGRA s.r.l. (805857 p.1) | candidato, ruolo = firma elaborati | **non** EPC/BoP |
| SHELTER S.r.l. | — | redazione piano di monitoraggio (1439019 p.7) | candidato, ruolo = consulente | **non** impresa esecutrice |
| EPC / BoP / appaltatori | — | nessuna evidenza di aggiudicazione | **Non determinabile** | |
| Geometria impianto | — | vertici UTM WGS84 32N (805873 pp.60–61, 109) | **Non determinabile** (gate aperto) | oggetto, ordine vertici e CRS da ricostruire; Piemonte è interamente nel fuso 32 ma la toolbox converte solo se il fuso è dichiarato in pagina |
| Durata costruzione | — | 10–12 mesi (scenario squadre parallele), 12 mesi (attività), 18 mesi, 24 mesi | **Conflittuale** | varianti per ambito/revisione da ordinare con date documento |
| "Inizio lavori: 01/09/2026" | — | 1439043 p.133 | **Non determinabile** | se la pagina è il frontespizio dati-cantiere di un PSC, è `template_reference` e non prova l'avvio |
| Stato autorizzativo | vuoto | project_type = Verifica di Ottemperanza | candidato | indica decreto VIA emesso con prescrizioni in verifica; **non** equivale ad AU né ad avvio lavori |

## 3. Gate aperti (bloccano la chiusura)

1. Rilettura con testo integrale (cache) dei 189 contenuti distinti; misura pagine OCR a 300 DPI vs 94 DPI del run precedente.
2. P. IVA Margisolar: verifica anagrafica esterna (decisione di Fabio: abilitare una consultazione VIES/Registro Imprese).
3. Tabelle vertici 805873: estrazione poligono, controllo area vs superficie dichiarata, verifica visiva su tavola.
4. Cronoprogramma: ordinare le varianti per procedura e data documento (l'ordine 12932 → 19438 è plausibile dagli ID ma va verificato sulle date degli indici).
5. Run CI `doc-intel-run.yml` verde **e** `evaluate` senza blocchi (conflitto multi-P. IVA rilevato, nessun EPC estratto).
