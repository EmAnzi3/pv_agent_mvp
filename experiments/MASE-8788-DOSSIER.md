# Tortona 2 (MASE 8788) — dossier documentale

**Stato:** `ready_for_human_review` · approvazione automatica: **no** · nessuna modifica al canonico.
**Prodotto da:** toolbox `doc_intel`, [run 39266857390](https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39266857390), commit `bc4045b`. Versione strutturata: `MASE-8788-DOSSIER.json`.

## 1. Inventario e copertura

| Procedura | Dichiarati | Censiti | Pagine indice |
|---|---|---|---|
| [19438](https://va.mite.gov.it/it-IT/Oggetti/Documentazione/8788/19438) | 87 | 87 | 9 |
| [12932](https://va.mite.gov.it/it-IT/Oggetti/Documentazione/8788/12932) | 124 | 124 | 13 |

211 riferimenti, 0 download falliti, **189 contenuti distinti** (22 duplicati per SHA-256). **3.727 pagine distinte**: 2.921 digitali, 787 OCR, 19 tavole grafiche senza testo anche dopo OCR (elencate nel JSON per verifica visiva), 0 OCR falliti. Gate acquisizione: **PASS**.
Il dato "4.356 pagine" della PR #4 contava anche i 22 riferimenti duplicati.

Valutazione su gold set provvisorio: recall valore 1,00, recall ancorata 0,83; fatti vietati (EPC/BoP) 0; conflitto sulle P. IVA rilevato e poi risolto con VIES (19/10/2026).

## 2. Classificazione rispetto al record canonico `url:910815a0c4750a6a`

| Campo | Canonico | Fonte (doc, pagina) | Classe |
|---|---|---|---|
| Proponente | Margisolar S.r.l. | Committente MARGISOLAR S.R.L. (1438987 p.1, 377 occ.) | **Confermato** — varianti: MARGISOLAR ENERGY S.r.l. (1439026 p.2, 12 occ.), Luisolar Energy S.r.l. (735725 p.9, 1 occ.) |
| Potenza | 60 MW | DC 60,00865 MWp (1438987 p.8); AC 50 MW (1438987 p.1) | **Confermato** (= DC/titolo) |
| Comuni | TORTONA | Tortona, Pozzolo Formigaro, Frugarolo (titolo scheda MASE) | **Nuovo verificato** — proposta: aggiungere 2 Comuni |
| Provincia | AL | AL | **Confermato** |
| Regione | Piemonte | non estratta dalla scheda | **Non determinabile** (non contraddetto) |
| Sede proponente | — | Viale Antonio Silvani 2, Bologna (1438994 p.1; confermata VIES) | **Nuovo verificato** |
| P. IVA proponente | — | 03920651209 (149 occ., 1438987 p.1); VIES 19/10/2026: MARGISOLAR S.R.L., Viale A. Silvani 2, Bologna | **Nuovo verificato** — 03920631201 è LUISOLAR ENERGY S.R.L. (VIES, stessa sede): associazione in 1077136 p.15 errata |
| PEC proponente | — | margisolar@legalmail.it (805873 p.9) | **Nuovo verificato** |
| Tecnologia | — | agrivoltaico (805858 p.1), moduli bifacciali (1438987 p.6) | **Nuovo verificato** |
| Potenza moduli | — | 610 / 570 / 720 / 715 Wp in revisioni diverse | **Conflittuale** |
| Connessione RTN | — | 132 kV (1439019 p.15) vs 150 kV (1439019 p.13); 30 kV = MT interna | **Conflittuale** |
| Località | — | Cascina Ponzana, Cascina Ponzanina (1438995 p.2) | **Nuovo verificato** |
| Ingombro sito WGS84 | — | NE 44,84600 / 8,78820 · SO 44,82932 / 8,77739 (1439008 p.1, verifica ENAC) | **Nuovo verificato** (rettangolo, non perimetro) |
| Poligoni aree | — | vertici UTM 32N in 706408 e 805847, pp. 7–27 | **Conflittuale** (due revisioni, scarti di decine di metri) |
| Progettazione / sicurezza | — | INTEGRA S.r.l., Ing. M. Castagnello CSP (1438995 p.5); geologo Dott. V. Cortese (1439048 p.1) | **Nuovo verificato** — non EPC |
| EPC / BoP / appaltatore | — | nessuna aggiudicazione documentata | **Non determinabile** |
| Inizio lavori | — | 01/09/2026 – 30/08/2028 nei campi "presunti" del PSC datato 08/07/2026 (1438995 pp.3, 133) | **Non determinabile** — pianificazione, non evento |
| Durata costruzione | — | 12 mesi (1438993 p.15, 13 occ.), 18 mesi (1161323 p.15), 5 mesi (1439047 p.10) | **Conflittuale** |
| Durata dismissione | — | 12 mesi (1439001 p.16), 6–9 mesi (1439055 p.159) | **Conflittuale** |
| Vita utile | — | 30 anni (1438993 p.3) | **Nuovo verificato** |
| BESS | — | nessuna configurazione d'accumulo | **Non applicabile** |
| Parametri eolici | — | — | **Non applicabile** |
| Stato autorizzativo | Verifica di Ottemperanza | — | **Non determinabile** oltre il canonico: decreto VIA emesso, non AU né avvio |

## 3. Punti aperti per la review umana

1. Rapporto societario fra Margisolar S.r.l., Luisolar Energy S.r.l. (VIES: stessa sede legale) e "MARGISOLAR ENERGY S.r.l." (solo documentale): probabile gruppo con più SPV.
2. Ordinare le revisioni per data (moduli, durate, poligoni) e scegliere il valore corrente con motivazione.
3. Verifica visiva dei poligoni sulle tavole 706408/805847 prima di qualunque uso GIS.

## 4. Errori noti della toolbox emersi su questo caso (corretti o tracciati)

Associazione per vicinanza di 07176380017 (ARPA Piemonte) a Margisolar in 756119 p.5: escluso dal dossier; contatti dell'agronomo associati a Margisolar: bloccati dal gate "non sulla stessa riga".
