# Wind Radar — coerenza tra maturità e calendario attività

Data: 2026-09-05  
Branch: `feat/wind-radar-enrichment-v0.3`

## Problema osservato

I riquadri `Pipeline per maturità` e `Timeline / pipeline di cantiere` erano alimentati da dati coerenti ma comunicavano due concetti diversi senza esplicitarlo:

- la maturità E0–E8 è uno **snapshot dello stato corrente**;
- la timeline è un **calendario di attività/milestone datate**, incluse attività future.

Inoltre:
- il calendario non mostrava un asse temporale leggibile né la data corrente del Radar;
- la legenda mostrava solo 8 tipi di fase mentre nei dati erano presenti 16 `timing.phase` diversi;
- il riquadro maturità copre tutti i progetti nel filtro, mentre il calendario mostra solo quelli con almeno una data disponibile.

Nel filtro iniziale questo significa **17 progetti nello snapshot e 12 nel calendario**; Fenice, Sava-Maruggio, Toritto, Volturino e Lama Cupa non hanno date di calendario sufficienti nel dataset corrente.

## Correzione UI

### 1. Nomi distinti
- `Pipeline per maturità` → **Stato corrente per maturità**;
- `Timeline / pipeline di cantiere` → **Calendario attività e milestone**.

### 2. Collegamento esplicito
Nel calendario ogni riga mostra:
- nome progetto;
- stage corrente completo, ad esempio `E6 · Procurement / affidamenti`;
- MW;
- attività datate.

Una nota dinamica chiarisce che una attività futura non fa avanzare lo stage finché l'avvio non è osservato.

### 3. Perimetro dichiarato
La nota mostra il rapporto tra i due riquadri:
- default: **12/17 progetti con date disponibili**;
- filtro E7: **9/9**;
- filtro E2: **0/3**.

I progetti senza date restano nello snapshot di maturità ma non vengono inventate date per farli comparire nel calendario.

### 4. Asse temporale
Aggiunto asse **2025–2029** e linea verticale:
`oggi · 05/09/2026`.

La stessa linea attraversa ogni riga progetto, separando visivamente attività passate/in corso da attività future.

### 5. Legenda operativa raggruppata
I 16 valori tecnici `timing.phase` sono ricondotti a 10 categorie leggibili, mantenendo il significato delle barre:

1. Autorizzazione / milestone;
2. Avvio sito;
3. Opere civili / costruzione;
4. Fondazioni;
5. Cavidotti / elettrico;
6. SSE / connessione;
7. Smantellamento;
8. Consegna WTG;
9. Montaggio WTG;
10. Collaudo / entrata in esercizio.

Le categorie sono solo rappresentazione UI del calendario; non modificano gli stage E0–E8 né i dati sorgente.

## Validazione browser

Chromium/Playwright:
- desktop 1440/1500 px: PASS;
- mobile 390 px: PASS;
- nessun overflow orizzontale;
- 12/12 righe con linea `oggi`;
- 12/12 righe con stage completo;
- legenda operativa completa per le categorie presenti;
- filtro E7: 9/9 datati;
- filtro E2: 0/3 datati e messaggio esplicativo corretto;
- nessun errore console.

## Regola di lettura finale

I due riquadri raccontano **la stessa pipeline da due prospettive diverse**:

- **Stato corrente per maturità** = dove si trova oggi ciascun progetto;
- **Calendario attività e milestone** = quando sono documentate o previste le attività del progetto.

Una previsione di costruzione futura può quindi comparire nel calendario mentre il progetto resta, per esempio, E6 fino a quando non è osservato l'avvio fisico del cantiere.
