# Wind Radar v0.6 — Company / Commercial Network

Data: 2026-09-05

## Perché serve

Il Radar non deve limitarsi a leggere progetto → contractor già noto. Per supportare davvero lo sviluppo commerciale deve anche costruire e mantenere una rete di aziende da conoscere, seguire e contattare prima che il singolo award emerga nei documenti di progetto.

Il nuovo layer `company-network-v06.json` separa quindi due concetti:

1. **project execution evidence**: relazione progetto ↔ azienda che può chiudere uno scope solo con prova A1/A2 specifica;
2. **commercial network intelligence**: azienda rilevante nel mercato, da monitorare e sviluppare commercialmente anche quando non è ancora collegata a un progetto canonico.

La seconda categoria non deve mai contaminare automaticamente la prima.

## Universo di scoperta

La pagina soci ANEV è il primo censimento strutturato: ANEV dichiara oltre 120 aziende attive nel settore eolico e il suo elenco include developer/operatori, OEM, EPC/BoP, infrastrutture elettriche, engineering/advisory e altri servizi.

Fonte:
- https://www.anev.org/soci/
- https://www.anev.org/chi-siamo/

L'universo ANEV viene usato come **discovery universe**, non come prova di ruolo su un progetto.

## Primo seed commerciale

Il primo seed v0.6 privilegia aziende con una delle seguenti caratteristiche:
- già presenti in uno o più progetti canonici;
- capacità Full BoP/EPC/electrical/grid documentata;
- ruolo OEM che anticipa procurement, delivery e commissioning;
- capacità heavy transport/heavy lift direttamente utile alla costruzione eolica;
- engineering/advisory ricorrente che può anticipare decisioni, cantieri e procurement;
- presenza italiana forte e pipeline rilevante.

### Fascia A — execution/network target

- PLC Group / PLC System / PLC Service Wind
- Tozzi Green
- GEKO
- D’Agostino Costruzioni Generali
- Idoka Costruzioni
- Mammana Michelangelo
- Delta Costruzioni
- Fagioli
- Mammoet
- Vestas
- Nordex
- RWE Renewables Italia
- ERG
- Edison Rinnovabili
- Eni Plenitude Renewables Italy
- ACCIONA Energía
- FRI-EL Green Power
- Fred. Olsen Renewables
- ESPE

### Fascia B — specialist / advisory / OEM watch

- Hydro Engineering
- ATS Engineering
- BAUTEL
- Siemens Gamesa
- ENERCON
- GE Vernova
- DNV
- Fichtner
- RINA
- WSP Italia
- OWC
- MPOWER

Il seed è intenzionalmente aperto: il successivo pass deve espandere e classificare l'intero universo ANEV e i player rilevanti non ANEV.

## Caso ESPE

ESPE è un ottimo esempio del perché serve un Network Radar distinto dal Contractor Radar.

Dalle fonti ufficiali 2026 emerge che ESPE:
- si presenta come operatore integrato lungo la value chain rinnovabile;
- svolge engineering elettrico, civile e di dettaglio anche per impianti wind;
- opera come EPC/System Integrator;
- opera in BESS e utility-scale renewable projects;
- produce e installa mini-eolico;
- ha un processo pubblico per la candidatura di nuovi fornitori/partner, con ambiti che includono **logistica, movimentazione e trasporto** oltre a servizi BESS, minieolico e specialistici.

Fonti:
- https://www.espe.it/wp-content/uploads/2026/05/ESPE-FY2025_investor-presentation.pdf
- https://www.espe.it/it/investor-relations/
- https://www.espe.it/it/fornitori/
- https://www.espe.it/it/soluzioni/mini-eolico/

Conclusione commerciale: **ESPE va inserita nella rete e contattata/monitorata**, ma allo stato delle evidenze pubbliche disponibili non va etichettata come contractor Full BoP utility-scale wind provato. È un target adiacente ad alto valore, soprattutto per system integration, BESS, elettrico e supply-chain partnership.

## Player execution particolarmente rilevanti

### PLC

La documentazione ufficiale PLC dichiara:
- Full BoP di impianti eolici;
- EPC/BoP civile ed elettrico;
- sottostazioni AT/MT e infrastrutture RTN;
- oltre 205 MW di Full BoP eolico eseguiti;
- O&M eolico tramite PLC Service Wind.

Fonti:
- https://www.plc-spa.it/linee-di-business/engineering-procurement-and-construction/
- https://www.plc-spa.it/plc-system/
- https://www.plc-spa.it/plc-service-wind/

### Tozzi Green

Tozzi Green documenta un modello integrato Development > EPC > O&M. La pagina procurement wind include esplicitamente fondazioni, viabilità, trasporto e montaggio WTG, cavi, sottostazioni e apparati elettrici. Comunicati recenti confermano costruzione e BoP civile/elettrico su parchi eolici italiani.

Fonti:
- https://www.tozzigreen.com/en/renewable-energies/procurement/
- https://www.tozzigreen.com/en/press-releases/

### Fagioli / Mammoet

Entrambe sono nodi da seguire per heavy transport/heavy lift e installation/logistics. Non serve attendere di trovarle già in un progetto canonico: possono anticipare mobilitazioni e necessità operative.

Fonti:
- https://www.fagioli.com/en/index.php
- https://www.mammoet.com/onshore-wind/

## Monitoring model

### Ogni 7 giorni — Fascia A

Controllare:
- news / press release / investor relations;
- project awards;
- nuove commesse EPC/BoP/grid;
- WTG orders e delivery;
- construction starts;
- mobilitazioni, cantieri e nuovi depot;
- supplier onboarding / procurement;
- acquisizioni societarie che cambiano le capability.

### Ogni 14 giorni — Fascia B

Controllare nuovi incarichi, engineering packages, studi, owner’s engineer, route/access engineering e nuovi ordini OEM.

### Ogni 30 giorni — Universo

Rieseguire il censimento ANEV e ampliare il registro con:
- nuovi soci/player;
- aziende rilevanti non associate;
- cambi nome/M&A;
- nuovi cluster di capability.

## Output v0.6 previsto

La UI deve evolvere dalla sola `Contractor view` a una vista distinta **Player & Network Watch**, con almeno:
- azienda;
- cluster/capability;
- priorità commerciale;
- relazione con progetti canonici;
- ultimo segnale;
- fonte;
- stato relazione commerciale (`target`, `known`, `active`, `supplier route`, ecc.);
- prossima azione suggerita;
- data ultimo controllo.

Questa vista non deve incrementare la scope coverage salvo presenza di un award A1/A2 specifico sul progetto.
