#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.wind_agents.adapters.mase import GENERIC_MASE_TITLE, MaseWindAgent
from app.wind_agents.adapters.mase_provvedimenti import MaseProvvedimentiWindAgent


# Current MASE project pages expose a generic H1 while the real project title is
# at the beginning of the page text. The wind layer must repair it.
description = (
    'Progetto di repowering dell\'impianto eolico ex "Serra Marrocco", ubicato nei Comuni di Nicosìa (EN) '
    'e Mistretta (ME) e costituito da n. 55 aerogeneratori per una potenza complessiva installata di 46,75 MW, '
    'consistente in un nuovo impianto eolico "Nicosìa" costituito da n. 13 aerogeneratori per una potenza '
    'complessiva pari a 78 MW, le cui opere di connessione ricadono anche a Castel di Lucio (ME). '
    '- Info - Valutazioni e Autorizzazioni Ambientali: VAS - VIA - AIA'
)
repaired = MaseWindAgent._project_title(GENERIC_MASE_TITLE, description)
assert repaired.startswith('Progetto di repowering'), repaired
assert 'VAS - VIA - AIA' not in repaired
assert MaseWindAgent._wind_power_mw(repaired, 46.75) == 78.0
assert MaseWindAgent._province(repaired, None) == 'EN'

# Unit turbine power must not win over explicit project total.
tricarico_like = (
    'Progetto di un impianto eolico costituito da 12 aerogeneratori, ciascuno con potenza pari a 6,6 MW '
    'per una potenza complessiva di 79,20 MW, da realizzarsi nel Comune di Tricarico (MT).'
)
assert MaseWindAgent._wind_power_mw(tricarico_like, 6.6) == 79.2

# Storage stays separate from wind MW.
astra_like = (
    'Parco eolico denominato Astra, della potenza complessiva di 39,6 MW, con sistema di accumulo integrato da 20 MW.'
)
assert MaseWindAgent._wind_power_mw(astra_like, None) == 39.6
assert MaseWindAgent._bess_power_mw(astra_like) == 20.0

# MASE latest-decision pages must recover structured project fields without
# swallowing navigation text into the proponent.
project_text = (
    'Progetto di integrale ricostruzione di un impianto eolico composto da 10 aerogeneratori di potenza unitaria '
    'pari 6,6 MW, per una potenza complessiva di 66 MW, nei Comuni di Celle di San Vito (FG) e Faeto (FG). '
    '- Info - Valutazioni e Autorizzazioni Ambientali: VAS - VIA - AIA '
    'Proponente: EDISON Rinnovabili S.p.A. Tipologia di opera: Impianti eolici onshore '
    'Territori ed aree marine Regioni: Puglia Province: Foggia Comuni: Celle di San Vito, Faeto '
    'Aree marine: Nessuna area marina Scegli la procedura VIA'
)
project_title = MaseProvvedimentiWindAgent._project_title(project_text, 'truncated')
fields = MaseProvvedimentiWindAgent._project_fields(project_text, project_title)
assert fields['proponent'].rstrip('.') == 'EDISON Rinnovabili S.p.A', fields
assert fields['region'] == 'Puglia', fields
assert fields['province'] == 'Foggia', fields
assert fields['municipalities'] == ['Celle di San Vito', 'Faeto'], fields
assert fields['power_mw'] == 66.0, fields

repowering = (
    'Progetto di repowering di un impianto eolico esistente da 46,75 MW, consistente in un nuovo impianto '
    'costituito da 13 aerogeneratori per una potenza complessiva pari a 78 MW.'
)
assert MaseProvvedimentiWindAgent._power_mw(repowering) == 78.0

print('v0.6 MASE normalization OK: real titles, current total MW, structured geography/proponent and separate BESS')
