# Data quality audit - pv_agent_mvp

Generato: `2026-04-29T22:28:30`

## Sintesi

- Record iniziali: **1526**
- Record dopo deduplica: **1526**
- Record rimossi/fusi: **0**
- Gruppi realmente fusi: **0**
- Coppie accettate in deduplica: **0**
- Coppie respinte per conflitto o prove insufficienti: **197**
- Righe corrette automaticamente prima della deduplica: **0**
- Righe sospette da verificare: **5**

## Puntuali vs Terna

| Categoria | Prima | Dopo | MW prima | MW dopo |
|---|---:|---:|---:|---:|
| Progetti puntuali | 1444 | 1444 | 49158.86 | 49158.86 |
| Record Terna aggregati | 82 | 82 | 160805.70 | 160805.70 |

## Fonti principali dopo deduplica

| Fonte | Record |
|---|---:|
| puglia | 448 |
| lazio | 321 |
| sicilia | 314 |
| mase | 96 |
| terna_econnextion | 82 |
| mase_provvedimenti | 72 |
| sistema_puglia_energia | 70 |
| lombardia | 61 |
| emilia_romagna | 20 |
| toscana | 12 |
| veneto | 11 |
| campania | 10 |
| piemonte | 8 |
| sardegna | 1 |

## Motivi principali di rigetto

| Motivo | Count |
|---|---:|
| province_conflict | 105 |
| mw_conflict | 84 |
| title_similarity | 7 |
| same_specific_url+same_region | 1 |

## Righe sospette per tipologia

| Issue | Count |
|---|---:|
| title_mw_mismatch | 3 |
| title_province_mismatch | 2 |

## Correzioni automatiche pre-deduplica

| Correzione | Count |
|---|---:|
| Nessuna | 0 |

## Top projects

- Top projects prima: **20**
- Top projects dopo rigenerazione: **20**

## Prime fusioni accettate

Nessuna fusione accettata.

## Prime fusioni respinte

| Score | Motivo | Titolo A | Titolo B | MW A | MW B | Prov A | Prov B |
|---:|---|---|---|---:|---:|---|---|
| 0 | province_conflict:TA!=BA | Impianto FER agrivoltaico - Comune di Gravina in Puglia (BA) - 67.05 MW | Impianto FER agrivoltaico - Comune di Gravina in Puglia (BA) - 67.05 MW | 319.11 | 67.05 | TA | BA |
| 0 | mw_conflict:140.868!=110.94 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P”, DI POTENZA COMPLESSIVA PARI A 140.868 KWP (95.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 5”, DI POTENZA COMPLESSIVA PARI A 110.940 KWP (65.000 KW IN IMMISSIONE) | 140.868 | 110.94 |  |  |
| 0 | mw_conflict:140.868!=110.855 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P”, DI POTENZA COMPLESSIVA PARI A 140.868 KWP (95.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 7”, DI POTENZA COMPLESSIVA PARI A 110.855,10 KWP (100.000 KW IN IMMISSIONE) | 140.868 | 110.855 |  |  |
| 0 | mw_conflict:140.868!=92.64 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P”, DI POTENZA COMPLESSIVA PARI A 140.868 KWP (95.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 3”, DI POTENZA COMPLESSIVA PARI A 92.640,00 KWP (60.000 KW IN IMMISSIONE) | 140.868 | 92.64 |  |  |
| 0 | mw_conflict:140.868!=87.468 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P”, DI POTENZA COMPLESSIVA PARI A 140.868 KWP (95.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 4”, DI POTENZA COMPLESSIVA PARI A 87.468,00 KWP (60.000 KW IN IMMISSIONE) | 140.868 | 87.468 |  |  |
| 0 | mw_conflict:140.868!=30.732 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P”, DI POTENZA COMPLESSIVA PARI A 140.868 KWP (95.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 2”, DI POTENZA COMPLESSIVA PARI A 30.732 KWP (20.000 KW IN IMMISSIONE) | 140.868 | 30.732 |  |  |
| 0 | province_conflict:EN!=PA | Provvedimento Unico in materia Ambientale (PNIEC-PNRR): Progetto di un impianto agrivoltaico denominato "Bordonaro", della potenza di 130... | Provvedimento Unico in materia Ambientale (PNIEC-PNRR): Progetto di un impianto agrivoltaico denominato "Bordonaro", della potenza di 130... | 130.0 | 130.0 | EN | PA |
| 0 | mw_conflict:120.046!=75.16 | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 264 del 7 Novembre 2025 | 120.046 | 75.16 | FG |  |
| 0 | province_conflict:FG!=LE | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 293 del 28 Novembre 2025 | 120.046 | 66.0 | FG | LE |
| 0 | mw_conflict:120.046!=65.06 | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 276 del 12 Novembre 2025 | 120.046 | 65.06 | FG | FG |
| 0 | mw_conflict:120.046!=44.489 | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 292 del 28 Novembre 2025 | 120.046 | 44.489 | FG | FG |
| 0 | mw_conflict:120.046!=41.038 | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 273 del 12 Novembre 2025 | 120.046 | 41.038 | FG | FG |
| 0 | province_conflict:FG!=BR | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 263 del 6 Novembre 2025 | 120.046 | 40.0 | FG | BR |
| 0 | province_conflict:FG!=BR | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 294 del 28 Novembre 2025 | 120.046 | 27.1 | FG | BR |
| 0 | province_conflict:FG!=BA | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 268 del 7 Novembre 2025 | 120.046 | 8.5 | FG | BA |
| 0 | mw_conflict:120.046!=6.6 | Determinazione del Dirigente Sezione Transizione Energetica n. 282 del 21 Novembre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 274 del 12 Novembre 2025 | 120.046 | 6.6 | FG | FG |
| 79 | title_similarity:0.94+same_region+same_mw+title_token_overlap:1.00 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 5”, DI POTENZA COMPLESSIVA PARI A 110.940 KWP (65.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 7”, DI POTENZA COMPLESSIVA PARI A 110.855,10 KWP (100.000 KW IN IMMISSIONE) | 110.94 | 110.855 |  |  |
| 0 | mw_conflict:110.94!=92.64 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 5”, DI POTENZA COMPLESSIVA PARI A 110.940 KWP (65.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 3”, DI POTENZA COMPLESSIVA PARI A 92.640,00 KWP (60.000 KW IN IMMISSIONE) | 110.94 | 92.64 |  |  |
| 0 | mw_conflict:110.94!=87.468 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 5”, DI POTENZA COMPLESSIVA PARI A 110.940 KWP (65.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 4”, DI POTENZA COMPLESSIVA PARI A 87.468,00 KWP (60.000 KW IN IMMISSIONE) | 110.94 | 87.468 |  |  |
| 0 | mw_conflict:110.94!=30.732 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 5”, DI POTENZA COMPLESSIVA PARI A 110.940 KWP (65.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 2”, DI POTENZA COMPLESSIVA PARI A 30.732 KWP (20.000 KW IN IMMISSIONE) | 110.94 | 30.732 |  |  |
| 0 | mw_conflict:110.855!=92.64 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 7”, DI POTENZA COMPLESSIVA PARI A 110.855,10 KWP (100.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 3”, DI POTENZA COMPLESSIVA PARI A 92.640,00 KWP (60.000 KW IN IMMISSIONE) | 110.855 | 92.64 |  |  |
| 0 | mw_conflict:110.855!=87.468 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 7”, DI POTENZA COMPLESSIVA PARI A 110.855,10 KWP (100.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 4”, DI POTENZA COMPLESSIVA PARI A 87.468,00 KWP (60.000 KW IN IMMISSIONE) | 110.855 | 87.468 |  |  |
| 0 | mw_conflict:110.855!=30.732 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 7”, DI POTENZA COMPLESSIVA PARI A 110.855,10 KWP (100.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 2”, DI POTENZA COMPLESSIVA PARI A 30.732 KWP (20.000 KW IN IMMISSIONE) | 110.855 | 30.732 |  |  |
| 0 | mw_conflict:99.42!=68.58 | Determinazione del Dirigente Sezione Transizione Energetica n. 253 del 13 Ottobre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 248 del 9 Ottobre 2025 | 99.42 | 68.58 | FG | FG |
| 0 | province_conflict:FG!=LE | Determinazione del Dirigente Sezione Transizione Energetica n. 253 del 13 Ottobre 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 255 del 17 Ottobre 2025 | 99.42 | 48.733 | FG | LE |
| 0 | province_conflict:MN!=MI | Realizzazione di un impianto Agrivoltaico in Comune di Peschiera Borromeo, nei pressi del Depuratore di Via Roma, con sistema Irriguo int... | Realizzazione di un impianto Agrivoltaico in Comune di Peschiera Borromeo, nei pressi del Depuratore di Via Roma, con sistema Irriguo int... | 99.0 | 7.771 | MN | MI |
| 0 | mw_conflict:92.64!=87.468 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 3”, DI POTENZA COMPLESSIVA PARI A 92.640,00 KWP (60.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 4”, DI POTENZA COMPLESSIVA PARI A 87.468,00 KWP (60.000 KW IN IMMISSIONE) | 92.64 | 87.468 |  |  |
| 0 | mw_conflict:92.64!=30.732 | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 3”, DI POTENZA COMPLESSIVA PARI A 92.640,00 KWP (60.000 KW IN IMMISSIONE) | IMPIANTO AGRO-FOTOVOLTAICO A TERRA DENOMINATO “S&P 2”, DI POTENZA COMPLESSIVA PARI A 30.732 KWP (20.000 KW IN IMMISSIONE) | 92.64 | 30.732 |  |  |
| 0 | province_conflict:FG!=LE | Determinazione del Dirigente Sezione Transizione Energetica n. 113 del 19 Maggio 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 134 del 28 Maggio 2025 | 90.0 | 60.0 | FG | LE |
| 0 | province_conflict:FG!=BR | Determinazione del Dirigente Sezione Transizione Energetica n. 113 del 19 Maggio 2025 | Determinazione del Dirigente Sezione Transizione Energetica n. 123 del 26 Maggio 2025 | 90.0 | 41.45 | FG | BR |

## Prime righe sospette

| Problema | Titolo | Fonte | Prov titolo | Prov campo | MW titolo | MW campo |
|---|---|---|---|---|---:|---:|
| title_province_mismatch | Impianto FER agrivoltaico - Comune di Gravina in Puglia (BA) - 67.05 MW | puglia | BA | TA | 67.05 | 319.11 |
| title_mw_mismatch | Impianto FER agrivoltaico - Comune di Gravina in Puglia (BA) - 67.05 MW | puglia | BA | TA | 67.05 | 319.11 |
| title_mw_mismatch | ARPAE: PROGETTO DI REALIZZAZIONE DI UN IMPIANTO AGRIVOLTAICO INTEGRATO CON POTENZA DI PICCO PARI A 22,25 MWP, SISTEMA DI ACCUMULO E OPERE... | emilia_romagna | BO | BO | 22.25 | 83.2 |
| title_province_mismatch | Determinazione n°G09167 del 08/07/2021 Favorevole con prescrizioni Realizzazione Impianto FV da 15 MWp in loc. Giannettoli connesso nel c... | lazio | LT | RM | 15.0 | 15.0 |
| title_mw_mismatch | IMPIANTO SOLARE AGRIVOLTAICO AVANZATO DENOMINATO CARPI 1 E OPERE CONNESSE CON POTENZA DI PICCO DI 12.904,92 KWP | emilia_romagna |  | MO | 12.90492 | 15.0 |

## File generati

- `reports/data_deduped.json`
- `reports/dedupe_audit.md`
- `reports/dedupe_accepted.csv`
- `reports/dedupe_rejected.csv`
- `reports/dedupe_suspicious_rows.csv`
- `reports/dedupe_field_repairs.csv`
- `reports/dedupe_top_excluded.csv`
- `reports/dedupe_project_key_splits.csv`
