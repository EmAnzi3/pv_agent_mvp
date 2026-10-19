# Aliai (MASE 8824) — dossier documentale (Fase 4: progetto non usato per sviluppare la toolbox)

**Stato:** `ready_for_human_review` · approvazione automatica: **no** · [run 39270010732](https://github.com/EmAnzi3/pv_agent_mvp/actions/runs/39270010732) · nessuna modifica al canonico.

## Inventario
Procedure 19170 (68/68), 12972 (330/330) · 398 riferimenti, 385 contenuti distinti, 0 download falliti · 6002 pagine (5867 digitali, 91 OCR, 44 tavole grafiche senza testo) · gate acquisizione **PASS**. Nessun ID o URL hardcoded: stesso codice dei benchmark.

## Confronto con il canonico
| Campo | Canonico | Fonte | Classe |
|---|---|---|---|
| Proponente | Cherry Picking S.r.l. | "Committente Cherry Picking Srl" (1412579 p.1, 268 occ.) | **Confermato** |
| Potenza | 227 MW | DC 226,6669 MWp (1412573 p.7, 49 occ.); AC 180,18 MW (1412574 p.3) | **Confermato** (titolo/DC) |
| Comuni | RAMACCA | titolo MASE: Ramacca, Castel di Iudica (titolo: "Ludica"), Paternò (CT), Centuripe (EN) | **Nuovo verificato** (+3 Comuni, 2 province) |
| Provincia / Regione | CT / Sicilia | due province nel titolo | **Non determinabile** come valore unico |

## Società e ruoli
| Ruolo | Soggetto | Evidenza | Classe |
|---|---|---|---|
| Proponente / SPV | Cherry Picking S.r.l., Corso Buenos Aires 54, Milano; P. IVA 02016600898 non attiva su VIES | 936468 p.1 | Nuovo verificato (sede); P. IVA: membro di gruppo IVA |
| Gruppo IVA | P. IVA 11998240011 = **GRUPPO SONNEDIX** (VIES), stessa sede | 936468 p.1 "aderisce al gruppo IVA" | **Nuovo verificato** |
| **EPC Contractor** | **PANORAMA S.r.l.** | campo "EPC Contractor" nel cartiglio di 29 elaborati (es. 1412571 p.1) | **Candidato forte — conferma umana richiesta** (designazione nel cartiglio del proponente, non contratto) |
| Direttore dei lavori | Ing. Serafino Torregrossa | 1412584 p.6 | Nuovo verificato (ruolo da cartiglio) |
| Progettista | Investment Capital Solutions S.p.A., Via Nervesa 21 Milano (VIES), P. IVA 00485050892 | 1412571 p.2 | **Nuovo verificato** (VIES) — varianti S.r.l./"ICS S.r.l." nello stesso cartiglio |
| Geologo | GeoTechnology 4.0 S.r.l., Bari (VIES), Dott. Geol. G. P. Ottomanelli | 1412543 p.115 | Nuovo verificato |

## Tecnica, sito, tempi
- 5 campi (Aliai 1–5) entro ~7,5 km; tracker monoassiali (80 occ.), moduli bifacciali 600 Wp, ~392 ha (1412573 p.4).
- Connessione: 150 kV (165 occ.) e 380 kV (17 occ.) → **Conflittuale** finché non si legge lo schema di connessione; 30 kV = MT interna.
- Durata costruzione: 18 mesi (1227834 p.20, 17 occ.) vs 30 mesi (1412527 p.19) → **Conflittuale**. Nessuna data di inizio lavori trovata → **Non determinabile**.
- Coordinate: tabella WGS84 dei centri campo (1412574 p.3) **non estratta** (layout non riconosciuto, segnalato nelle diagnostiche); punti UTM 33 in 1412539 senza oggetto classificato.

## Punti aperti
1. Conferma dell'affidamento EPC a PANORAMA S.r.l. (P. IVA non presente nei cartigli).
2. Lettura della tabella centri campo (miglioramento parser) e dello schema di connessione 150/380 kV.
