# Wind Radar — contractor + public entity pass 3

Data audit: 2026-09-05

## Regole applicate

- nessun contractor esecutivo viene attribuito per deduzione;
- A1/A2 chiudono un core scope solo quando progetto, azienda e ruolo esecutivo sono espliciti;
- B/C restano lead/segnali;
- gli atti territoriali vengono prima riconciliati contro identity/configurazione del progetto;
- proponente, progettista, DL, site management, inspection e concessionario non diventano automaticamente Civil/Electrical/SSE/Foundation/Erection/Logistics contractor.

## 1. Andretta-Bisaccia — Progeco Group / Progeco SE

Nuova evidenza wired nel file `contractor-leads-2026-09-05.json`.

Fonte diretta/aziendale: vacancy LinkedIn di Progeco Group per `Deputy - Site Manager`, località Bisaccia, riferita esplicitamente a progetti di supporto specialistico in ambito `Renewable Energy – Wind Repowering a Andretta e Bisaccia (AV)`.

La descrizione copre:

- gestione e supervisione opere civili e infrastrutturali;
- fondazioni;
- viabilità di cantiere;
- cavidotti interrati MT/AT;
- coordinamento di imprese esecutrici e subappaltatori;
- SAL, qualità e sicurezza.

Classificazione radar:

- **Progeco Group / Progeco SE — Site / construction management & works supervision — B**;
- nessun `scope_hint` esecutivo;
- **NON Civil BoP**;
- **NON Foundation contractor**;
- **NON Electrical BoP / SSE contractor**;
- **NON Logistics contractor**.

Motivo: la vacancy descrive in modo esplicito un ruolo di gestione/supervisione e distingue tale ruolo dalle imprese esecutrici/subappaltatori che la risorsa dovrà coordinare.

Fonte: https://it.linkedin.com/jobs/view/deputy-site-manager-at-progeco-group-4461062499

## 2. Tarsia Ovest — atti Provincia di Cosenza

### Concessione stradale n. 85 del 19/05/2025

- Comune: Tarsia;
- strada: **SP 176**;
- tratta: **km 0+000 – km 3+575**;
- oggetto: scavo e posa cavi elettrici in MT;
- soggetto indicato: **ENI PLENITUDE RENEWABLES ITALY S.p.A.**

Uso radar:

- evidenza territoriale utile per ricostruire il corridoio delle opere elettriche;
- non identifica la ditta esecutrice;
- non chiude Electrical BoP.

Fonte: Provincia di Cosenza, albo/provvedimenti dirigenti, pubblicazione 21/05/2025.

### Concessione stradale n. 111 del 09/07/2025

- Comune: Tarsia;
- strada: **SP 241**;
- tratta: **km 75+300 – km 75+500**;
- oggetto: scavo e posa cavi elettrici in MT;
- soggetto indicato: **ENI PLENITUDE RENEWABLES ITALY S.p.A.**

Uso radar:

- seconda tratta utile per route intelligence del cavidotto;
- non identifica la ditta esecutrice;
- non chiude Electrical BoP.

### Concessione stradale n. 139 del 05/08/2026

- Comune: Tarsia;
- strada: **SP 176**;
- posizione: **km 3+600 lato DX**;
- oggetto: realizzazione di accesso stradale ad uso commerciale/industriale;
- soggetto indicato: **ENI PLENITUDE RENEWABLES ITALY S.p.A.**

Classificazione prudenziale:

- **lead operativo ad alta utilità** per possibile accesso di cantiere/logistica;
- il titolo indicizzato non cita espressamente `Tarsia Ovest`;
- prima di attribuirlo definitivamente al parco serve il provvedimento integrale o altro collegamento project-specific;
- non chiude Logistics / heavy transport.

## 3. Identity guards aggiunti

### Andretta/Bisaccia — MERAL

Regione Campania, CUP 10177: distinto impianto eolico proposto da **MERAL S.p.A.**, **30 MW / 5 WTG**, nei Comuni di Andretta e Bisaccia, protocollo 29/06/2026.

Rischio: collisione molto alta con ricerche locali sul repowering Edison 88,5 MW.

Fonte: https://viavas.regione.campania.it/opencms/opencms/VIAVAS/VIA_files_new/Progetti/prg_10177_prot_2026.577306_del_29-06-2026.via

### Tricarico — Dolomiti Windfarm

MASE: distinto progetto **Dolomiti Windfarm S.r.l.**, **79,20 MW / 12 WTG × 6,6 MW**, Comuni di Tricarico, Vaglio Basilicata e Brindisi Montagna, MYTERNA **202200037**, procedura **10151**.

Il Decreto VIA **DM_2026-0000497 del 03/08/2026** ha esito **negativo**.

Non confondere con il record radar **Adest — Tricarico 42 MW / 7 WTG Vestas**.

Fonte: https://va.mite.gov.it/it-IT/Oggetti/Info/10094

### ALAS / Alas 2

Negli stessi Comuni di Ittiri e Villanova Monteleone e con lo stesso proponente RWE esistono due progetti distinti:

- **ALAS — 66 MW / 10 WTG**;
- **Alas 2 — 50,4 MW / 7 WTG**, procedura MASE **10816**.

Fonte Alas 2: https://va.mite.gov.it/it-IT/Oggetti/Info/10524

Ogni atto locale/corporate va quindi riconciliato almeno su nome progetto, MW/WTG, procedura o contenuto tecnico.

## 4. Negative / no-promotion checks

### Nulvi-Ploaghe

Il corpus MASE indicizzato espone la procedura 4230 e il progetto definitivo, ma le ricerche per `cronoprogramma`, `cantierizzazione` e `trasporto` non hanno restituito finora un elaborato chiaramente intitolato/indicizzato come cronoprogramma di costruzione corrente.

Non promuovere documenti progettuali 2018 a calendario operativo 2026.

### ALAS

Gli atti comunali di Ittiri restano utili per il quadro amministrativo e il rapporto con Regione Sardegna, ma non nominano un contractor esecutivo.

### Greci-Montaguto

ERG conferma direttamente avvio cantiere marzo 2026 e completamento nell'estate 2027. Le ricerche territoriali/corporate di questo pass non hanno aggiunto un Civil BoP, foundation contractor, logistics o erection contractor project-specific.

## 5. Impatto metriche

Nessun nuovo core scope A1/A2 è stato chiuso in questo pass.

Le metriche devono quindi restare:

- **230,9 MW** con almeno uno scope esecutivo A1/A2;
- **8 / 108 scope applicabili** coperti A1/A2;
- **7,4%** scope coverage.

Il checker `scripts/check_wind_radar.py` è stato esteso per accettare i lead Andretta-Bisaccia + Tricarico e contiene una regression guard che impone a Progeco di restare `signal/B` e di non chiudere scope.

## 6. Prossime ricerche ad alto rendimento

1. Tarsia: recuperare il testo integrale della Concessione 139/2026 e verificare se l'accesso SP176 km 3+600 è esplicitamente il cantiere Tarsia Ovest; cercare ditta esecutrice nelle concessioni/scavi.
2. Tarsia: recuperare delibera finale + allegato della convenzione Comune–Plenitude dicembre 2025.
3. Andretta-Bisaccia: sfruttare il presidio Progeco come chiave di ricerca per imprese esecutrici/subappaltatori su fondazioni, civili, cavidotti e SSE prima dell'apertura cantiere 02/11/2026.
4. Nulvi-Ploaghe: concentrarsi su SUAPEE 496419, Genio Civile Sassari, viabilità provinciale e ordinanze/trasporti invece di rileggere il solo corpus progettuale 2018.
5. ALAS: cercare atti di accesso/viabilità/cantiere distinguendo rigidamente ALAS da Alas 2.
6. Greci-Montaguto: Provincia di Avellino + Comuni + lato pugliese del cavidotto per occupazioni, attraversamenti e trasporti eccezionali.
7. Tricarico: mantenere il filtro identity Adest 42 MW contro Dolomiti 79,2 MW e cercare conferma diretta Vestas dell'eventuale installation scope.
