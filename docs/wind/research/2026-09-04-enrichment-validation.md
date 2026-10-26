# Wind Radar enrichment v0.3 — validation

Data: 2026-09-04

## Dataset / overlay

- 17 progetti presenti e univoci.
- Totale wind invariato: 1.496,9 MW.
- Overlay enrichment presente per tutti i 17 progetti.
- Tutti i `source_id` usati da relazioni, timing e configurazioni risultano risolti.
- Commercial Window ammessi e osservati: `EARLY`, `OPEN`, `ACTIVE`, `LATE`.

## Scope coverage

- Core scope: Civil BoP, Electrical BoP, SSE/grid, Fondazioni WTG, Erection, Logistics/heavy transport, Dismantling per repowering.
- Scope A1/A2 coperti: 8 / 108 applicabili = 7,4%.
- MW con almeno uno scope esecutivo A1/A2: 230,9 MW.
- Il KPI MW non significa BoP completo: misura soltanto la presenza di almeno uno scope esecutivo confermato.

Controlli di coerenza:

- Carlentini: coperta solo `Fondazioni WTG` tramite Gruppo Mammana A2; non chiude Civil BoP generale.
- Serra Giannina: D’Agostino resta segnale B e non chiude Civil BoP.
- Tricarico: Vector Renewables è LTA / construction monitoring e non chiude scope esecutivi.
- Venusia: Idoka chiude Civil/site preparation; EGM è Engineering/DL e non aggiunge scope esecutivi.
- Nessun segnale B/C viene promosso automaticamente a scope coperto.

## Document intelligence

Full text già letto:

- Andretta-Bisaccia — Piano di cantierizzazione;
- Alia-Sclafani — cronoprogramma repowering;
- Serra Giannina — cronoprogramma BoP WTG-by-WTG.

Documenti prioritari individuati:

- Toritto — `Cronoprogramma Impianto Eolico Toritto` (MASE, stesura 01/03/2025);
- Lama Cupa — cronoprogramma attività, cronoprogramma SE RTN e pacchetto Piano Tecnico Opere;
- ALAS — fascicolo MASE opere civili / verifica di ottemperanza;
- Fenice — integrazioni 24/04/2026, Relazione Generale e Relazione Tecnica.

## UI / JavaScript

- Preview standalone generata con dataset e overlay incorporati.
- Tutti i blocchi JavaScript della preview passano `node --check`.
- La Contractor view v0.3 deriva i nominativi direttamente dal dataset arricchito e mantiene separate relazioni esecutive, tecniche e segnali B/C.
- KPI e chip scope coverage vengono ricalcolati sui progetti filtrati.

Nota: il runtime Chromium disponibile nel container non termina correttamente neppure su una pagina HTML minimale; per questo la verifica browser visuale automatica non è considerata attendibile in questo ambiente. La preview standalone resta il riferimento per la review visuale manuale.
