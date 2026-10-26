(()=>{'use strict';
const $=s=>document.querySelector(s),all=s=>[...document.querySelectorAll(s)];
let meta=null,timer=null;
function stageCode(el){const cls=[...el.classList].find(x=>/^e[0-8]$/i.test(x));if(cls)return cls.toUpperCase();const m=String(el.textContent||'').match(/\bE[0-8]\b/i);return m?m[0].toUpperCase():null}
function stageMap(){return new Map((meta?.maturity_scale||[]).map(x=>[x.code,x]))}
function visibleCounts(){const counts=Object.fromEntries((meta?.maturity_scale||[]).map(x=>[x.code,0]));all('#opportunityRows [data-project-id]').forEach(row=>{const b=row.querySelector('.stage-badge'),code=b&&stageCode(b);if(code in counts)counts[code]++});return counts}
function canonicalizeBadges(){const map=stageMap();all('.stage-badge').forEach(b=>{const code=stageCode(b),s=map.get(code);if(!s)return;b.textContent=`${s.code} · ${s.label}`;b.title=s.description||s.label;b.setAttribute('aria-label',`${s.code}, ${s.label}. ${s.description||''}`.trim())})}
function renderLegend(){const root=$('#stageLegend');if(!root||!meta)return;const counts=visibleCounts();root.classList.add('stage-complete-legend');root.innerHTML=meta.maturity_scale.map(s=>`<span class="legend-chip stage-legend-chip ${counts[s.code]?'':'is-empty'}" title="${String(s.description||'').replace(/"/g,'&quot;')}"><i style="background:var(--${s.code.toLowerCase()})"></i><b>${s.code}</b><span>${s.label}</span><em>${counts[s.code]||0}</em></span>`).join('')}
function addPipelineNote(){const panel=$('.pipeline-panel');if(!panel||panel.querySelector('.stage-model-note'))return;const note=document.createElement('div');note.className='stage-model-note';note.innerHTML='<b>Scala unica E0–E8.</b> Ogni progetto è assegnato alla fase più avanzata direttamente osservata. Le fasi con 0 progetti restano visibili: uno zero non indica una fase saltata, ma che nessun progetto canonico si trova oggi in quella fase.';const scale=panel.querySelector('.scale-note');if(scale)scale.insertAdjacentElement('beforebegin',note);else panel.appendChild(note)}
function decoratePipelineRows(){const map=stageMap();all('#pipelineFallback .fallback-row').forEach(row=>{const label=row.querySelector('.fallback-label'),m=label?.textContent.match(/\bE[0-8]\b/);if(!m)return;const s=map.get(m[0]);if(!s)return;row.title=s.description||''})}
function refresh(){if(!meta)return;canonicalizeBadges();renderLegend();addPipelineNote();decoratePipelineRows()}
function schedule(){clearTimeout(timer);timer=setTimeout(refresh,60)}
async function init(){try{meta=await fetch('data/meta.json',{cache:'no-store'}).then(r=>r.json());refresh();new MutationObserver(schedule).observe(document.body,{childList:true,subtree:true})}catch(err){console.error('stage-consistency init failed',err)}}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
