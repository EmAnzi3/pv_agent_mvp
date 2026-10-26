# Wind Radar v0.3 — glossary validation

Data: 2026-09-05

## Obiettivo

Rendere il Wind Project & Contractor Radar leggibile anche da utenti non tecnici, senza eliminare le sigle e i termini specialistici utili al lavoro operativo.

## Implementazione

- `docs/wind/data/glossary.json`: 37 termini realmente utilizzati nel Radar;
- `docs/wind/assets/glossary.js`: modale ricercabile e accessibile;
- `docs/wind/assets/glossary.css`: layout desktop/mobile;
- `docs/wind/index.html`: caricamento del glossario;
- pulsante **Glossario** accanto a **Legenda fonti A1–D** nella fascia Metodo.

Categorie: Impianto, Cantiere, Rete, Tempi, Mercato, Soggetti, Ruoli tecnici, Autorizzazioni, Metodo Radar.

Le definizioni privilegiano il significato operativo in linguaggio comune. Sono esplicitate alcune distinzioni anti-falso-positivo del Radar, per esempio:
- OEM non implica automaticamente erection/logistics;
- DL e LTA supervisionano/controllano ma non sono imprese esecutrici;
- l'appartenenza a un RTI non prova lo scope individuale;
- BESS resta separato dai MW eolici;
- A1/A2/B/C/D e E0–E8 sono spiegati come scale interne del Radar.

## Correzioni emerse dal test

1. La prima implementazione usava `open` per mostrare la modale, mentre il Radar usa `.modal.on`: corretto prima della chiusura.
2. La fascia Metodo aveva tre colonne e il nuovo pulsante introduceva una quarta voce: aggiunta griglia desktop a quattro colonne e mobile a due pulsanti con i testi descrittivi a tutta larghezza.
3. Corretto il conteggio singolare da `1 termini trovati` a `1 termine trovato`.

## Browser test

Chromium / Playwright sulla preview standalone v0.3 corrente.

Desktop 1440 × 1000:
- pulsante Glossario visibile;
- modale apre e chiude correttamente;
- 37 termini caricati;
- 9 categorie;
- ricerca `BoP` → 3 risultati;
- ricerca `WTG` → 2 risultati;
- ricerca `autorizzazione` → 2 risultati;
- `Esc` chiude la modale e restituisce il focus al pulsante Glossario;
- nessun overflow orizzontale.

Mobile 390 × 844:
- griglia Metodo coerente a due pulsanti;
- modale larga 374 px nel viewport da 390 px;
- ricerca `OEM` → `1 termine trovato`;
- nessun overflow orizzontale;
- apertura/chiusura corretta.

Il JavaScript finale passa anche `node --check`.

## Esito

PASS. Il glossario è parte della v0.3 e non modifica dati, contractor, scope coverage o KPI.
