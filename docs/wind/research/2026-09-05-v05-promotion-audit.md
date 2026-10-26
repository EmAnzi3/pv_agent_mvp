# Wind Radar v0.5 — Canonical promotion audit

Data: 2026-09-05

## Obiettivo

Promuovere nel Radar canonico solo i candidati discovery che superano il gate definito in v0.4, evitando che la crescita del numero di progetti riduca la qualità del dataset principale.

## Esito gate sui 38 candidati current

- **34 eligible**
- **9.705,62 MW wind**
- **311 MW BESS**, separati dai MW wind
- **4 blocked**

Il gate non considera `current` sufficiente alla promozione.

## Criteri

Per ogni candidato sono richiesti:
1. identity reconciliation completata;
2. activity class corrente;
3. configurazione MW/WTG sufficientemente canonica;
4. `site_type` esplicito;
5. stage E0-E8 sostenuto da fonte;
6. scope profile applicabile;
7. collision check con il canonico esistente.

## Enrichment recuperati durante il promotion pass

Sono stati recuperati da fonti ufficiali/istituzionali i campi che mancavano nel discovery seed per:
- **Altamura** — proponente Alta Wind S.r.l.;
- **Poggio dell'Oro** — proponente Sorgenia Renewables S.r.l.;
- **Fiumicino 46,2 MW + BESS** — proponente SKI W A4 S.r.l.;
- **Fresagrandinaria-Dogliola-Lentella** — proponente Q-ENERGY RENEWABLES 2 S.r.l.;
- **Florinas Repowering** — proponente RWE Renewables Italia S.r.l.;
- **Atis** — 48 turbine da 18 MW, coerenti con 864 MW.

### Correzione stage Florinas

Il discovery v0.4 aveva `E4` come hint prudenziale basato sulla Verifica di Ottemperanza. Il promotion audit lo corregge a **E3 — Iter autorizzativo avanzato**: la VIA positiva e la successiva ottemperanza sono osservabili, ma non è stata ancora provata l'autorizzazione complessiva sufficiente per E4.

## Offshore senza stage_hint nel seed v0.4

Per OWF 1, Tramontana, Libeccio, Poseidon, Ulisse 1 e NURAX lo stage di promozione è **E2 — Iter autorizzativo**, perché le relative procedure MASE correnti/scoping/PUA sono direttamente osservate. Non viene attribuito uno stage superiore per deduzione.

## Quattro record bloccati

### Med Wind Grecale — 698,25 MW
Fonti ufficiali confermano il progetto e indicano **un massimo di 45 aerogeneratori** con potenza unitaria fino a 18,8 MW. Questo non equivale a una configurazione WTG corrente esatta da esporre come dato canonico. Resta discovery.

### Rospo Offshore — 1.005 MW + 350 MW BESS
Il progetto e il proponente sono A1 MASE. Il valore **67 × 15 MW** è riportato in modo consistente da stampa/ANSA, ma il gate v0.5 richiede ancora una fonte A1/A2 che esponga esplicitamente quella configurazione. Resta discovery.

### Sindia-Macomer — 43,4 MW
MASE conferma progetto, proponente e potenza. Le **7 WTG** risultano dalla stampa locale/regionale, ma non sono ancora agganciate a una fonte A1/A2 nella scheda discovery. Resta discovery.

### Le Chiancate — 86,4 MW
Identità, nuova procedura 14943 e Amaranth Energy sono verificati; manca ancora il numero esatto di WTG in una fonte A1/A2. Resta discovery.

## Tranche canonica

La prima tranche v0.5 comprende quindi **34 nuovi progetti canonici**. L'inserimento deve avvenire come chunk separato, mantenendo:
- origine discovery;
- identity key/genealogia procedurale;
- fonti A1/A2;
- BESS separato;
- scope gap differenziati onshore/offshore;
- nessuna relazione contractor inventata.

Dopo la promozione il Radar passa da 17 a **51 progetti canonici**, mentre i 4 current bloccati, i 4 stale_scoping e i 5 rejected restano nel discovery layer.

## Fase successiva

Una volta verificata la tranche canonica, il pass successivo della v0.5 è l'arricchimento commerciale dei 34 nuovi progetti:
- developer/SPV;
- OEM;
- contractor/EPC/BoP;
- tempi e milestone;
- accessi/logistica;
- connessione/SSE;
- finanziamento/FID;
- porti e marine logistics per offshore.

A1/A2 chiudono scope; B/C restano segnali.