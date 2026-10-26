(()=>{'use strict';
const PROVINCE_ALIASES={
  'Reggio di Calabria':'Reggio Calabria',
  'Monza e Brianza':'Monza e della Brianza',
  'Forli-Cesena':'Forlì-Cesena',
  'Forlì Cesena':'Forlì-Cesena',
  'Bolzano':'Bolzano/Bozen',
  'Bolzano-Bozen':'Bolzano/Bozen',
  "Valle d'Aosta":"Valle d'Aosta/Vallée d'Aoste",
  'Aosta':"Valle d'Aosta/Vallée d'Aoste",
  'Massa Carrara':'Massa-Carrara',
  'Pesaro Urbino':'Pesaro e Urbino',
  'Reggio Emilia':"Reggio nell'Emilia"
};
const GEO_URLS=[
  'https://cdn.jsdelivr.net/gh/openpolis/geojson-italy@master/geojson/limits_IT_provinces.geojson',
  'https://raw.githubusercontent.com/openpolis/geojson-italy/master/geojson/limits_IT_provinces.geojson'
];
const state={projects:[],meta:{},geo:null,chart:null,metric:'mw',selected:'',timer:null};
const $=id=>document.getElementById(id);
const fmt0=new Intl.NumberFormat('it-IT',{maximumFractionDigits:0});
const fmt1=new Intl.NumberFormat('it-IT',{maximumFractionDigits:1});
const normProvince=v=>{const s=String(v||'').trim().replace(/\s+/g,' ');return PROVINCE_ALIASES[s]||s};
const stageN=s=>+String(s||'E0').slice(1);
const asDate=v=>v?new Date(v+'T00:00:00'):null;
function strictRel(r){return(state.meta.execution_roles||[]).includes(r.role)&&r.status==='confirmed'&&['A1','A2'].includes(r.confidence)}
function hasExec(p){return(p.relations||[]).some(strictRel)}
function extent(p){const dates=[];(p.timing||[]).forEach(t=>['date','start','end'].forEach(k=>{if(t[k])dates.push(asDate(t[k]))}));return dates.length?[new Date(Math.min(...dates)),new Date(Math.max(...dates))]:null}
function selectedRange(){const el=$('window'),v=el?.value||'';if(!v)return null;const start=asDate(state.meta.as_of);if(!start)return null;let end=new Date(start);if(v==='6m')end.setMonth(end.getMonth()+6);else if(v==='12m')end.setMonth(end.getMonth()+12);else if(v==='2026q4')return[asDate('2026-10-01'),asDate('2026-12-31')];else if(v==='2027')return[asDate('2027-01-01'),asDate('2027-12-31')];else if(v==='2028')return[asDate('2028-01-01'),asDate('2028-12-31')];return[start,end]}
function matchesMainFilters(p){
  const q=($('q')?.value||'').trim().toLowerCase();
  const region=$('region')?.value||'',province=$('province')?.value||'',site=$('siteType')?.value||'',stage=$('stage')?.value||'',type=$('type')?.value||'',developer=$('developer')?.value||'',contractor=$('contractor')?.value||'';
  const minRaw=$('mwMin')?.value??'',maxRaw=$('mwMax')?.value??'',min=minRaw===''?null:+minRaw,max=maxRaw===''?null:+maxRaw,range=selectedRange();
  const siteType=p.site_type||'onshore';
  const hay=[p.name,p.developer,p.spv,p.region,p.province,siteType,...(p.municipalities||[]),...(p.relations||[]).flatMap(x=>[x.company,x.role,x.scope]),...(p.gaps||[])].join(' ').toLowerCase();
  if(q&&!hay.includes(q))return false;
  if(region&&p.region!==region||province&&normProvince(p.province)!==normProvince(province)||site&&siteType!==site||stage&&p.stage!==stage||type&&p.type!==type||developer&&p.developer!==developer)return false;
  if(contractor==='__open__'&&hasExec(p))return false;
  if(contractor==='__confirmed__'&&!hasExec(p))return false;
  if(contractor&&!contractor.startsWith('__')&&!(p.relations||[]).some(x=>x.company===contractor))return false;
  if(min!=null&&p.mw<min||max!=null&&p.mw>max)return false;
  if(range){const x=extent(p);if(!x||x[0]>range[1]||x[1]<range[0])return false}
  return true;
}
function aggregate(){
  const map=new Map(),filtered=state.projects.filter(matchesMainFilters);let missing=0;
  filtered.forEach(p=>{
    const province=normProvince(p.province);if(!province){missing++;return}
    if(!map.has(province))map.set(province,{name:province,mw:0,projects:0,e4mw:0,e7:0,priority:0,openExec:0,bess:0,items:[]});
    const row=map.get(province);row.mw+=+p.mw||0;row.projects++;if(stageN(p.stage)>=4)row.e4mw+=+p.mw||0;if(p.stage==='E7')row.e7++;if(String(p.priority||'').startsWith('A'))row.priority++;if(!hasExec(p))row.openExec++;row.bess+=+p.bess_mw||0;row.items.push(p);
  });
  map.forEach(row=>row.items.sort((a,b)=>(b.score||0)-(a.score||0)));
  return{rows:[...map.values()],filtered,missing};
}
function metricValue(row){if(state.metric==='projects')return row.projects;if(state.metric==='e4mw')return row.e4mw;return row.mw}
function metricLabel(){return state.metric==='projects'?'N. progetti':state.metric==='e4mw'?'MW E4+':'MW eolici'}
function metricFmt(v){return state.metric==='projects'?fmt0.format(v):fmt1.format(v)+' MW'}
function renderSelection(){const box=$('provinceMapSelection');if(!box)return;box.innerHTML=state.selected?`Provincia selezionata: <strong>${state.selected}</strong> <button type="button" class="province-map-clear" id="provinceMapClear">Mostra tutte</button>`:'Clicca una provincia per applicarla come filtro al Radar.';const clear=$('provinceMapClear');if(clear)clear.onclick=()=>{const province=$('province');state.selected='';if(province){province.value='';province.dispatchEvent(new Event('change',{bubbles:true}))}else{renderSelection();schedule()}}}
function fallback(rows){const host=$('provinceMap');if(!host)return;const ordered=[...rows].sort((a,b)=>metricValue(b)-metricValue(a));const max=Math.max(...ordered.map(metricValue),1);host.innerHTML=`<div class="province-map-fallback">${ordered.map(r=>`<div class="province-map-fallback-row"><b>${r.name}</b><div class="province-map-fallback-track"><div class="province-map-fallback-bar" style="width:${Math.max(1,metricValue(r)/max*100)}%"></div></div><span class="province-map-fallback-value">${metricFmt(metricValue(r))}</span></div>`).join('')||'<div class="empty-state">Nessun progetto nel filtro corrente.</div>'}</div>`}
function render(){
  const host=$('provinceMap'),status=$('provinceMapStatus');if(!host)return;
  const {rows,filtered,missing}=aggregate();renderSelection();
  if(status)status.textContent=`${filtered.length} progetti nel filtro · ${rows.length} province canoniche rappresentate${missing?` · ${missing} record senza provincia esclusi dalla mappa`:''}. BESS non sommato ai MW eolici.`;
  if(!window.echarts||!state.geo){fallback(rows);return}
  if(!state.chart)state.chart=echarts.init(host);
  const data=rows.map(r=>({name:r.name,value:metricValue(r),...r})),positive=data.map(d=>+d.value||0).filter(v=>v>0),max=Math.max(...positive,1);
  state.chart.setOption({
    animationDuration:350,
    tooltip:{trigger:'item',confine:true,renderMode:'html',extraCssText:'max-width:300px;white-space:normal;line-height:1.35;',formatter:p=>{const d=p.data;if(!d)return`<b>${p.name}</b><br>Nessun progetto nel filtro`;const top=d.items.slice(0,3).map(x=>`${x.name} · ${fmt1.format(x.mw)} MW · ${x.stage}`).join('<br>'),rows=[];if(state.metric!=='mw')rows.push(`MW eolici: ${fmt1.format(d.mw)} MW`);if(state.metric!=='projects')rows.push(`Progetti: ${fmt0.format(d.projects)}`);if(state.metric!=='e4mw')rows.push(`MW E4+: ${fmt1.format(d.e4mw)} MW`);rows.push(`E7 in costruzione: ${fmt0.format(d.e7)}`,`Priorità A/A+: ${fmt0.format(d.priority)}`,`Progetti senza contractor esecutivo A1/A2: ${fmt0.format(d.openExec)}`);if(d.bess)rows.push(`BESS separato: ${fmt1.format(d.bess)} MW`);return`<b>${p.name}</b><br>${metricLabel()}: <b>${metricFmt(d.value)}</b><br>${rows.join('<br>')}${top?`<br><br><b>Priorità nel filtro</b><br>${top}`:''}`}},
    visualMap:{min:0,max,orient:'horizontal',left:'center',bottom:7,itemWidth:8,itemHeight:150,text:[metricFmt(max),'0'],textGap:7,textStyle:{fontSize:9,color:'#66756f'},inRange:{color:['#f3f6f5','#d8eee5','#8dcfb7','#35a77e','#087f5b']}},
    series:[{name:metricLabel(),type:'map',map:'wind_italy_provinces',roam:true,layoutCenter:['50%','47%'],layoutSize:'91%',data,itemStyle:{borderColor:'#9aa4b2',borderWidth:.7,areaColor:'#f6f8fb'},emphasis:{label:{show:true,color:'#17231f',fontWeight:700},itemStyle:{areaColor:'#b8dfcf'}},select:{disabled:true}}]
  },true);
  state.chart.off('click');state.chart.on('click',params=>{if(!params?.name)return;const province=normProvince(params.name),select=$('province');state.selected=province;if(select){select.value=province;select.dispatchEvent(new Event('change',{bubbles:true}))}else{renderSelection();schedule()}});
}
function schedule(){clearTimeout(state.timer);state.timer=setTimeout(render,35)}
async function fetchJson(url){const r=await fetch(url,{cache:'no-store'});if(!r.ok)throw new Error(`${url}: ${r.status}`);return r.json()}
async function loadGeo(){let last=null;for(const url of GEO_URLS){try{const geo=await fetchJson(url);geo.features.forEach(f=>{const p=f.properties||{},raw=p.prov_name||p.provincia||p.name||p.NOME_PRO||p.DEN_UTS||p.prov_istat_code_name||'';f.properties.name=normProvince(raw)});state.geo=geo;if(window.echarts)echarts.registerMap('wind_italy_provinces',geo);return}catch(err){last=err}}throw last||new Error('GeoJSON province non disponibile')}
async function loadData(){const manifest=await fetchJson('data/projects.json'),all=await Promise.all([fetchJson('data/'+manifest.meta),...manifest.chunks.map(x=>fetchJson('data/'+x))]);state.meta=all[0];state.projects=all.slice(1).flat()}
function bind(){document.querySelectorAll('.province-map-metric').forEach(btn=>btn.addEventListener('click',()=>{state.metric=btn.dataset.metric;document.querySelectorAll('.province-map-metric').forEach(x=>x.classList.toggle('active',x===btn));render()}));['q','region','province','siteType','stage','type','developer','contractor','mwMin','mwMax','window'].forEach(id=>{const el=$(id);if(el)el.addEventListener(el.tagName==='SELECT'?'change':'input',schedule)});const province=$('province');if(province)province.addEventListener('change',()=>{state.selected=normProvince(province.value);renderSelection()});const reset=$('resetFilters');if(reset)reset.addEventListener('click',()=>{state.selected='';setTimeout(()=>{renderSelection();schedule()},0)});window.addEventListener('resize',()=>state.chart?.resize())}
async function init(){bind();const status=$('provinceMapStatus');try{await loadData();try{await loadGeo()}catch(err){console.warn('province map geo unavailable, using ranked fallback',err);if(status)status.textContent='GeoJSON province non disponibile: uso fallback ordinato per provincia.'}render()}catch(err){console.error('province map init failed',err);if(status)status.textContent='Mappa province non disponibile: errore nel caricamento del dataset.'}}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',init);else init();
})();
