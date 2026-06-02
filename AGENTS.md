# AGENTS.md â€” PV Agent MVP

## Scopo

Questo file guida ChatGPT/Codex quando lavora su questo repository.

## Regole generali

- Prima di modificare codice o file, leggere `README.md`, `CURRENT_STATE.md` e `CHANGELOG.md`.
- Preferire modifiche piccole, verificabili e reversibili.
- Non cambiare struttura dati, colonne visibili, filtri o layout senza richiesta esplicita.
- Non introdurre dipendenze pesanti se non strettamente necessarie.
- Non esporre riferimenti tecnici a utenti finali: GitHub, JSON, script, pipeline, branch, commit.
- Se un file Ã¨ generato, modificare la sorgente/generatore, non solo l'output.
- Prima del commit eseguire `.\scripts\check_before_publish.ps1`.
- Dopo la modifica, aggiornare `CURRENT_STATE.md` se cambia il workflow.
- Aggiornare `CHANGELOG.md` con una voce sintetica.

## Vincoli specifici del repository

- Non mischiare fonti raw, dati normalizzati e dati pubblicati.
- Non modificare manualmente output generati senza aggiornare la pipeline.
- Non esporre dettagli tecnici nella dashboard destinata agli utenti finali.
- Preservare compatibilitÃ  GitHub Pages.

## Checklist prima di proporre commit

`powershell
git status
git diff --check
.\scripts\check_before_publish.ps1
git diff --stat
`

## Stile commit

Usare messaggi chiari:

`	ext
docs: add maintenance files
fix: normalize province fallback
feat: add branch shard loader
ui: remove horizontal scroll
test: add pre-publish validation
`

Evitare messaggi tipo:

`	ext
fix vari
aggiornamenti
sistemato tutto
`

