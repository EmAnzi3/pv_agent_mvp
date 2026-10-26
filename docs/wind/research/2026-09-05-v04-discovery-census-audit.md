# Wind Radar v0.4 — Discovery census & refresh audit

Data audit: 2026-09-05

## Obiettivo

Costruire un discovery layer nazionale onshore + offshore che ampli il Radar senza contaminare il dataset canonico v0.3. Il discovery deve distinguere una nuova opera da una nuova procedura della stessa opera, mantenere gli esiti negativi come collision guard e produrre eventi di variazione prima di qualsiasi promozione canonica.

## Stato del corpus discovery

Tre registri alimentano il discovery layer:
- `data/discovery-v04.json` — primo seed offshore;
- `data/discovery-census-v04.json` — prima tranche nazionale MASE;
- `data/discovery-census-v04b.json` — seconda tranche nazionale MASE.

Totale al 05/09/2026:
- **47 candidati distinti**;
- **38 correnti**;
- **4 scoping datati**;
- **5 rejected / guardie storiche**.

Tra i 38 correnti:
- **28 onshore**;
- **10 offshore**;
- **11.538,67 MW wind**;
- **661 MW BESS**, sempre separati dai MW eolici.

Scoping datati:
- 4 progetti offshore;
- 3.195 MW wind;
- ultimo avanzamento MASE individuato nel 2022–2023, senza procedura successiva trovata nel pass corrente.

Rejected / guardie:
- 5 progetti;
- 1.195,4 MW wind;
- non vengono trattati come opportunità attive.

Questi valori sono **discovery intelligence**, non si sommano ai KPI canonici v0.3.

## Limite della copertura MASE

Il portale MASE espone attualmente su alcune rotte di ricerca un messaggio di temporanea disabilitazione legato alla revisione dei requisiti di sicurezza informatica. Per questo motivo il corpus v0.4 va definito come **censimento corrente indicizzato e refreshable**, non come prova di completezza assoluta di ogni pratica storica MASE.

Il pass ha usato:
- pagine progetto MASE correnti;
- risultati indicizzati MASE;
- procedure VIA, PUA, Scoping e Verifica di Ottemperanza rilevanti;
- ricerca per regioni e aree a maggiore presenza eolica;
- controlli separati onshore/offshore.

Il motore è progettato per estendere il corpus quando le rotte MASE tornano pienamente interrogabili senza cambiare lo schema o perdere le identità già riconciliate.

## Activity class

Non si usa più `status != rejected` come sinonimo di progetto attivo.

Classi operative:
- `current` — procedura recente/in corso o avanzamento ufficiale recente;
- `stale_scoping` — scoping concluso nel 2022–2023 senza una procedura successiva individuata nel pass corrente;
- `rejected` — archiviazione/esito negativo, conservato solo come guardia.

Questa distinzione impedisce di gonfiare la pipeline commerciale con progetti tecnicamente non respinti ma privi di avanzamenti recenti.

## Identity reconciliation — casi di test

### NURAX
MASE espone più oggetti/procedure per la stessa opera offshore da 462 MW. Il Radar mantiene **una sola identità**, collegando le procedure successive alla genealogia dell'opera.

### Atis
Scoping e VIA corrente appartengono allo stesso progetto floating. Non vengono contati come due progetti.

### Poseidon
Scoping, PUA corrente e ulteriore oggetto 2026 sono ricondotti alla stessa opera da 1.008 MW.

### Kailia
La configurazione corrente da 900 MW non genera una seconda identità rispetto alla configurazione storica MASE da circa 1.170/1.176 MW. MW e WTG sono proprietà mutabili, non chiavi di identità.

### Le Chiancate
La procedura 14717 archiviata e la nuova istanza 14943 del giugno 2026 riguardano la stessa opera da 86,4 MW. Il discovery conserva **una sola identità corrente** con la procedura archiviata come genealogia.

### Nulvi-Sedini
Il progetto WPD da 57,6 MW è distinto dal canonico ERG Nulvi-Ploaghe e dal progetto FRI-EL Nulvi-Tergu. La sola presenza del Comune di Nulvi non è sufficiente per un match.

### SV9 Monte Camulera
La pagina MASE riporta una tipologia `Impianti eolici offshore`, ma l'opera è descritta esclusivamente nei Comuni liguri di Murialdo, Osiglia, Bormida e Mallare e senza area marina. Il discovery classifica il progetto come **onshore** e conserva l'etichetta MASE incoerente come data-quality guard.

### Rejected
Chieuti, Puglia 1, Brindisi Evolve, Mazara Wind e Naturgy/Thiesi restano nel corpus ma non possono entrare tra le opportunità correnti senza una nuova procedura distinta.

## Identity key

Regole implementate in `data/identity-rules-v04.json` e `scripts/wind_discovery_engine.py`:

1. `explicit_identity_group` quando la riconciliazione è già documentata;
2. `MYTERNA` come anchor stabile quando disponibile;
3. `mase_operation_anchor` quando disponibile;
4. fallback `site_type + nome normalizzato + area normalizzata`.

Gli ID procedura MASE **non sono identità progetto**: una stessa opera può attraversare più procedure.

Non fanno parte della identity key:
- MW;
- BESS;
- WTG count/taglia;
- developer/SPV;
- stato procedura;
- stage E0–E8.

Sono invece inclusi nel **change fingerprint**, perché devono generare una variazione osservabile.

## Change / refresh engine

`scripts/wind_discovery_engine.py` combina i tre registri e calcola:
- `identity_key`;
- `change_fingerprint`;
- classi current/stale/rejected;
- conteggi e MW onshore/offshore;
- eventi `baseline`, `discovered`, `changed`, `missing_from_refresh`.

Con `--write` può produrre/aggiornare:
- `data/discovery-index-v04.json`;
- `data/refresh-log-v04.json`.

La baseline corrente è già registrata in `data/refresh-log-v04.json`.

## Scope onshore / offshore

`data/scope-profiles-v04.json` separa i KPI di applicabilità.

### Onshore
Restano i 7 scope v0.3:
Civil BoP, Electrical BoP, SSE/grid, fondazioni WTG, erection, logistics/heavy transport, dismantling per repowering.

### Offshore
Profilo dedicato:
- fondazioni/substructure/mooring;
- WTG installation offshore;
- inter-array cables;
- offshore substation/electrical platform quando applicabile;
- export cable + landfall;
- onshore SSE/grid;
- marine logistics/port/heavy lift;
- opere civili onshore di connessione quando applicabili;
- dismantling/decommissioning per repowering.

Un progetto offshore non entra quindi nel denominatore dei vecchi 7 scope onshore.

## UI v0.4

Il filtro `Ambito` offre:
- Onshore + offshore;
- Onshore;
- Offshore.

Il dataset canonico resta separato dalla nuova sezione:
**Discovery · nuovi progetti da verificare**.

La sezione mostra current/stale/rejected e segue i filtri Ambito, Maturità quando esiste uno stage hint e ricerca testuale. Per evitare una pagina troppo lunga, mostra 12 candidati di default con espansione `Mostra tutti`.

Il discovery layer non alimenta:
- KPI canonici;
- Contractor View;
- scope coverage 8/108;
- 230,9 MW con scope esecutivo.

## Validazione

Regression guard:
- `scripts/check_wind_radar.py` — canonico v0.3;
- `scripts/check_wind_industry_press.py`;
- `scripts/check_wind_stages.py`;
- `scripts/check_wind_v04.py` — discovery, identity guards, scope profiles, UI;
- `scripts/wind_discovery_engine.py` — dry-run identity/change.

Workflow dedicato:
`.github/workflows/check_wind_v04.yml`.

Test browser locale della nuova sezione, su preview standalone con il corpus completo di 47 candidati:
- desktop 1440 px: PASS;
- mobile 390 px: PASS;
- 47 candidati caricati;
- current/stale/rejected 38/4/5;
- filtro Offshore: 10 current + 4 stale + 2 rejected;
- filtro Onshore: 28 current + 3 rejected;
- nessun overflow orizzontale.

## Gate di promozione

Al termine della v0.4 **nessun candidato viene promosso automaticamente nel canonico**.

La prima promozione richiede:
1. identity reconciliation completata;
2. stato corrente verificato;
3. configurazione MW/WTG canonica;
4. `site_type` esplicito;
5. stage E0–E8 sostenuto da fonte;
6. scope profile applicabile;
7. controllo collisioni con i 17 record esistenti.

La v0.4 costruisce il motore e la coda nazionale; la promozione massiva nel Radar diventa un pass successivo e controllabile.