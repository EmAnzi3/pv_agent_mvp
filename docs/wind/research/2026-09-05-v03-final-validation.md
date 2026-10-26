# Wind Radar v0.3 — final validation 2026-09-05

## Scope

Gate finale della Draft PR #2 dopo Priority 1 pass, public/source pass e Industry Press Pass.

## Data / regression validation

È stato ricostruito localmente il dataset embedded della preview corrente e sono stati rieseguiti gli stessi assert sostanziali di `scripts/check_wind_radar.py` e `scripts/check_wind_industry_press.py`.

Esito:
- 17 progetti;
- 1.496,9 MW wind;
- 8 / 108 scope esecutivi A1/A2 = 7,4%;
- 230,9 MW con almeno uno scope esecutivo A1/A2;
- Progeco/Andretta resta signal B e non chiude scope;
- Vestas/Tricarico installation resta B / erection hint e non chiude scope;
- Venusia resta coperta solo Civil, con 8 WTG × 5,6 MW e OEM Nordex rimosso dai gap;
- Serra Palino resta Civil + Electrical, OEM Nordex rimosso dai gap;
- Tarsia Ovest resta Civil + Electrical + SSE/grid; Michelangelo Mammana è unico e B / civil hint;
- Castelfranco/CER resta solo SSE/grid; Energy& resta signal B;
- Serra Giannina senza scope A1/A2;
- Carlentini solo Foundation;
- Hydro ALAS, ATS Fenice e Brulli/Lama Cupa non contaminano gli execution scope.

Industry discovery layer:
- 5 progetti con intelligence materiale;
- nessuna chiave `relations`, `scope_hint`, `execution_scope` o `covered_scope` nel registro Industry;
- Tricarico financial close UniCredit A2 presente;
- Castelfranco identity collision A1 con fonti Regione Campania per CUP 9207 vs CUP 9439;
- Alia site-services candidate resta C.

Nota tecnica: il runtime non risolve GitHub via `git`, quindi non è stato possibile lanciare i due file direttamente da un clone. Gli assert sono stati eseguiti localmente sul contenuto embedded corrente, equivalente ai gate dei due validator.

## Browser validation

Preview standalone corrente caricata con Chromium/Playwright tramite `page.set_content` per aggirare il blocco amministrativo su `file://`/`localhost`.

Desktop:
- 17 opportunity rows;
- KPI visualizzati: 1.496,9 MW, 230,9 MW, 8/108, 7,4%;
- Tricarico: sezione `Industry & market intelligence` presente con financial close 46,5 mln €;
- Castelfranco/CER: sezione Industry presente con collision guard Miscano;
- Alia-Sclafani: sezione Industry presente con candidate site-services;
- Venusia: 8 WTG, Nordex visibile, Contractor gap senza OEM;
- Contractor View: UniCredit e FM Service assenti; Energy& presente come nodo B; Michelangelo Mammana compare una sola volta;
- nessun errore console osservato.

Mobile 390 px:
- Tricarico e Venusia testati;
- `scrollWidth == clientWidth == 390`;
- nessun overflow orizzontale;
- nessun errore console;
- Industry section correttamente visibile su Tricarico.

## Esito

**PASS per la fase v0.3 di enrichment/research + rappresentazione UI.**

La PR resta Draft, non mergiata e non pubblicata. Il prossimo step è revisione utente / decisione sul passaggio successivo; nessun merge senza approvazione esplicita.