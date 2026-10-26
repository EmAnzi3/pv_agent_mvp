# Wind Radar v0.6 — network expansion tranche B

Data audit: 2026-09-05

## Obiettivo

Portare il network da seed iniziale a una rete commerciale e istituzionale più larga e operativa, senza confondere capability aziendale, membership associativa o fonte istituzionale con un award di progetto.

## Company Network — tranche B

Aggiunti 19 player non presenti nel seed iniziale. Ogni nodo della tranche ha:
- priorità commerciale;
- cluster/capability;
- fonti da monitorare;
- data ultimo controllo;
- prossima azione commerciale;
- eventuale collegamento a progetto canonico solo quando esiste evidenza già disponibile.

### Player ad alta priorità aggiunti

- BayWa r.e. Italia — sviluppo eolico, procurement, costruzione civile/elettromeccanica e lavori HV.
- SAPE Costruzioni — Full BoP eolico turnkey esclusa fornitura/installazione WTG; civili, elettrico, elettromeccanico, AT/MT.
- IVPC Group / IVPC Service — sviluppo, costruzione, erection, O&M, AT/MT, blade repair.
- Renexia — onshore/offshore/floating developer.
- ReNEXT Solutions — EPCIM offshore floating.
- OX2 Italia — sviluppo, costruzione e gestione; Maia 27 MW come caso italiano.
- wpd Italia — sviluppo, costruzione tramite wpd construction e operation tramite wpd windmanager; collegato al canonico Nulvi-Sedini solo come company/network relation, non come scope execution.
- Hitachi Energy — trasformatori, collection/grid connection e soluzioni offshore.
- YCE — blade inspection/repair e Total Blade Maintenance.
- ENGIE Italia — developer/operator con programma eolico 2026-2027.
- Alerion — owner/developer con portafoglio e acquisizioni di progetti autorizzati.
- Goldwind Energy Italy — OEM; evidenza societaria di supply/installazione WTG in Italia.
- EDP Renewables Italia — developer/operator.

Altri nodi: InVento Italia, Tratos Cavi, LEITWIND, VSB Energia Verde Italia, European Energy Italia, Qair Italia.

## Institutional & Source Network — tranche B

Il registro istituzionale è stato esteso con un audit diretto di fonti regionali ufficiali.

### Gap chiusi a livello di source discovery

- Abruzzo — piattaforma Ambiente/VIA + servizio regionale AU FER DPC025.
- Liguria — banca dati dinamica procedimenti VIA, procedure in corso/concluse e cartografia; presente percorso specifico per impianti eolici.
- Marche — registro VIA regionale, mirror regionale VIA statale e pagina autorizzazioni energetiche/AU.
- Molise — canale regionale esplicitamente separato `Procedimenti Depositati - Eolico` e `Eolico - VIA nazionale`.

### Endpoint approfonditi

- Lazio — project list VIA/PAUR già utilizzata dal collector PV, da adattare eliminando l'esclusione eolico.
- Emilia-Romagna — VIAVASWeb riusabile con filtro wind.
- Lombardia — FERAU operativo dal 2026; competenza AU normalmente provinciale.
- Umbria — elenco VIA + servizio AU FER.
- Veneto — FER authorization/PAUR + BUR.
- Piemonte — piattaforma ambientale regionale con competenza AU FER distribuita su Province/Città Metropolitana.

## Regole probatorie confermate

1. Company capability != project award.
2. Association membership != project relation.
3. Official source endpoint != A1 evidence by itself.
4. Only a project-specific official act/document can establish A1 evidence.
5. B/C never closes an execution scope.
6. Developer/OEM/supplier network relations remain separate from canonical execution scopes.
7. BESS remains separate from wind MW.

## Stato dopo la tranche

- Company Network: almeno 48 player richiesti dal validator; actual merged registry > seed iniziale e tutti i 19 nuovi nodi hanno `last_checked`, `next_action` e `watch_urls`.
- Institutional Network: almeno 30 source nodes richiesti dal validator; 4 regional gaps source-audited/resolved.
- UI Company Watch merges `company-network-v06.json` + `company-network-v06b.json` and exposes last check / next action.
- Institutional Watch merges base + audit tranche and removes resolved gaps from the residual-gap count.

## Prossime priorità

1. Completare il census ANEV oltre i player già prioritizzati, mantenendo A/B/C e capability discipline.
2. Audit Friuli-Venezia Giulia, Trentino-Alto Adige e Valle d'Aosta.
3. Passare dai source endpoints ai collector wind-ready sulle regioni A.
4. Continuare contractor hunt A1/A2 sui progetti E4-E7 con maggiore urgenza commerciale.
