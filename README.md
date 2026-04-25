# PV Agent MVP

MVP operativo per monitoraggio nazionale dei progetti fotovoltaici / agrivoltaici / BESS.

## Obiettivo

Questa base serve per:

- raccogliere dati da fonti nazionali e regionali;
- normalizzarli in un formato unico;
- deduplicare i progetti;
- memorizzare lo storico delle variazioni;
- produrre un report giornaliero con nuovi progetti e cambi di stato.

## Stato del progetto

Questa repository è una **ossatura eseguibile** pensata per partire subito.

Contiene:

- struttura progetto Python;
- database PostgreSQL via SQLAlchemy;
- collector base e due esempi (`MASE`, `Veneto`);
- pipeline di ingestione;
- deduplica iniziale;
- report CSV giornalieri;
- scheduler locale;
- Dockerfile e `docker-compose.yml` per NAS QNAP / server Docker.

### Limiti attuali

- i collector sono **starter implementation** e potrebbero richiedere aggiustamenti ai selettori HTML/API delle fonti reali;
- la copertura nazionale completa è impostata a livello di architettura, ma i connector effettivamente implementati qui sono solo esempi;
- alcuni siti regionali richiederanno browser automation con Playwright o parsing PDF dedicato;
- la dashboard web completa non è inclusa in questa prima ossatura.

## Architettura

```text
Fonti pubbliche -> Collectors -> Normalizzazione -> Deduplica -> PostgreSQL -> Report CSV
                                                        \-> Eventi storici
```

## Stack

- Python 3.11
- SQLAlchemy 2.x
- PostgreSQL 15
- SQLite per test locale
- Requests + BeautifulSoup
- APScheduler
- Docker / Docker Compose

## Struttura

```text
app/
  collectors/
  config.py
  db.py
  dedupe.py
  main.py
  models.py
  normalizers.py
  pipeline.py
  reporting.py
sql/
  init.sql
scripts/
  run_once.sh
.github/workflows/
  pv-agent-run.yml
```

## Variabili ambiente

Copia `.env.example` in `.env` e compila i valori.

## Avvio locale

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python -m app.main run-once
```

## Avvio con Docker / QNAP

1. Copia la cartella sul NAS o clonala da GitHub.
2. Compila il file `.env`.
3. Avvia:

```bash
docker compose up -d --build
```

4. Il servizio `pv-agent` resterà attivo e lancerà il job giornaliero all'orario definito da `DAILY_RUN_HOUR` e `DAILY_RUN_MINUTE`.

## Uso su QNAP

Su QNAP hai due strade:

### Opzione A - Container Station

- crei un progetto da `docker-compose.yml`;
- imposti il file `.env`;
- monti una cartella persistente per i report.

### Opzione B - CLI sul NAS

Se hai accesso SSH:

```bash
git clone <repo>
cd pv_agent_mvp
cp .env.example .env
nano .env
docker compose up -d --build
```

## Uso con GitHub

La repository include un workflow GitHub Actions di esempio.

Serve per:

- esecuzione schedulata opzionale;
- controllo manuale;
- eventuale esportazione report.

> Nota: per usare GitHub Actions con PostgreSQL esterno servono i secret del repository.

## Roadmap consigliata

### Sprint 1
- validare MASE e Veneto;
- controllare i campi estratti;
- testare il salvataggio storico.

### Sprint 2
- aggiungere Lombardia, Emilia-Romagna, Sicilia, Puglia;
- introdurre parsing PDF e Playwright per i portali dinamici.

### Sprint 3
- copertura nazionale completa;
- scoring commerciale;
- digest email;
- dashboard.

## Output atteso

Nella cartella `reports/` vengono generati file CSV con:

- nuovi progetti;
- cambi di stato;
- snapshot del run.

## Raccomandazione pratica

Questa base va bene per partire, ma il passaggio corretto è:

1. testarla su server/QNAP;
2. stabilizzare i collector;
3. farla rifinire da uno sviluppatore per tutte le fonti regionali;
4. collegarla a una dashboard e a digest automatici.
