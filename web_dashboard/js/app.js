/* DQ Monitor — standalone dashboard app.js */

// ── Global state ──────────────────────────────────────────────
const DATA = {};
const CHARTS = {};
let activeSource = 'all';
let sevFilter   = new Set(['CRITICAL','HIGH','MEDIUM']);
let tableFilter = new Set();

// ── Colour helpers ────────────────────────────────────────────
const SEV_COLORS = {CRITICAL:'#EF4444', HIGH:'#F59E0B', MEDIUM:'#D97706'};
const TABLE_COLORS = ['#3B82F6','#10B981','#8B5CF6','#F59E0B','#EF4444'];
const STATUS_LABELS = {CRITICAL:'CRITICAL', HIGH:'HIGH', MEDIUM:'MEDIUM', CLEAN:'CLEAN'};

function scoreColor(s){ return s>=90?'#22C55E':s>=70?'#F59E0B':'#EF4444'; }
function scoreClass(s){ return s>=90?'healthy':s>=70?'warning':'critical'; }
function badgeClass(s){ return s>=90?'badge-healthy':s>=70?'badge-warning':'badge-critical'; }
function sevBadge(s){
  const cls = {CRITICAL:'sev-critical',HIGH:'sev-high',MEDIUM:'sev-medium'}[s]||'';
  return `<span class="sev-badge ${cls}">${s}</span>`;
}
function chip(text){ return `<span class="metric-chip">${text}</span>`; }

// ── Data fetch ────────────────────────────────────────────────
async function loadAllData(){
  try {
    const files = ['summary','sources','dq_scores','charts','null_heatmap','anomalies','alert_log','history','alert_history'];
    for(const f of files){
      const r = await fetch(`data/${f}.json?t=${Date.now()}`);
      if(!r.ok) throw new Error(`HTTP ${r.status} loading ${f}.json`);
      DATA[f] = await r.json();
    }
  } catch(e){
    console.error('Data load failed:', e);
    document.getElementById('main').innerHTML =
      `<div style="padding:40px;text-align:center;color:#EF4444">
        <h2>Could not load data</h2>
        <p style="margin-top:8px;color:#6B7280;font-size:13px">
          Run <code>python export_dashboard_data.py</code> then refresh.<br>
          <small style="color:#9CA3AF">Error: ${e.message}</small>
        </p>
      </div>`;
    return;
  }
  try {
    renderAll();
  } catch(e){
    console.error('Render failed:', e);
  }
}

// ── Source selector ───────────────────────────────────────────
function renderSourceSelector(){
  const sources = DATA.sources || {};
  const names   = Object.keys(sources);
  if(names.length <= 1) return; // hide if only one source

  let el = document.getElementById('sourceSelector');
  if(!el){
    el = document.createElement('div');
    el.id = 'sourceSelector';
    el.style.cssText = 'padding:8px 24px;background:var(--bg-subtle);border-bottom:.5px solid var(--border);display:flex;align-items:center;gap:10px;font-size:12px;';
    const banner = document.getElementById('statusBanner');
    if(banner) banner.after(el);
  }

  const pills = ['all', ...names].map(s => {
    const active = s === activeSource ? 'style="background:var(--accent);color:#fff;border-color:var(--accent)"' : '';
    const label  = s === 'all' ? `All sources (${names.length})` :
                   `${s} <span style="opacity:.6">(${(sources[s]?.tables||[]).length} tables)</span>`;
    return `<span onclick="switchSource('${s}')" ${active}
      style="padding:4px 12px;border-radius:20px;border:.5px solid var(--border);
             background:var(--bg-surface);color:var(--text-secondary);
             cursor:pointer;transition:all .15s">${label}</span>`;
  }).join('');

  el.innerHTML = `<span style="color:var(--text-muted);font-weight:600;text-transform:uppercase;letter-spacing:.07em;font-size:10px">Source:</span> ${pills}`;
}

function switchSource(name){
  activeSource = name;
  renderSourceSelector();
  renderOverview();
  // Re-render current page charts
  const activePage = document.querySelector('.page.active');
  if(activePage){
    const id = activePage.id.replace('page-','');
    if(id==='trends')  renderTrends();
    if(id==='heatmap') renderHeatmap();
  }
}

function getActiveCharts(){
  // Returns charts data filtered by activeSource
  if(activeSource === 'all' || !DATA.sources) return DATA.charts;
  const src = DATA.sources[activeSource];
  if(!src) return DATA.charts;
  const filtered = {};
  (src.tables||[]).forEach(t => { if(DATA.charts[t]) filtered[t] = DATA.charts[t]; });
  return filtered;
}

function getActiveScores(){
  if(activeSource === 'all' || !DATA.sources) return DATA.dq_scores;
  const src = DATA.sources[activeSource];
  if(!src) return DATA.dq_scores;
  return DATA.dq_scores.filter(s => (src.tables||[]).includes(s.table));
}

function getActiveHeatmap(){
  if(activeSource === 'all' || !DATA.sources) return DATA.null_heatmap;
  const src = DATA.sources[activeSource];
  if(!src) return DATA.null_heatmap;
  const filtered = {};
  Object.entries(DATA.null_heatmap||{}).forEach(([col, dates]) => {
    const table = col.split('.')[0];
    if((src.tables||[]).includes(table)) filtered[col] = dates;
  });
  return filtered;
}

// ── Render all pages ──────────────────────────────────────────
// Track which pages have had their charts rendered
const RENDERED = {};

function renderAll(){
  renderTopbar();
  renderSourceSelector();
  renderHistory();
  initAiGreeting();
  renderOverview();
  renderTrends();
  renderHeatmap();
  renderAlerts();
  renderConnections();
}

// ── Topbar ────────────────────────────────────────────────────
function renderTopbar(){
  const s  = DATA.summary;
  const st = (s.overall_status||'CLEAN').toUpperCase();
  document.getElementById('lastRun').textContent = `Last run: ${s.last_run||'—'}`;
  document.getElementById('pageDate').textContent =
    new Date().toLocaleDateString('en-GB',{weekday:'long',day:'numeric',month:'long',year:'numeric'});

  const pill = document.getElementById('statusPill');
  const cls  = st==='CRITICAL'?'critical':st==='HIGH'?'high':'clean';
  pill.className = `status-pill ${cls}`;
  pill.innerHTML = `<div class="dot"></div>${st}`;

  const banner = document.getElementById('statusBanner');
  if(st !== 'CLEAN'){
    banner.style.display = 'block';
    banner.className = `status-banner ${cls}`;
    banner.textContent = st==='CRITICAL'
      ? `${s.total_anomalies} critical anomalies detected — review required before next pipeline run`
      : `${s.total_anomalies} anomaly(s) detected — check the Anomalies page`;
  } else {
    banner.style.display = 'block';
    banner.className = 'status-banner clean';
    banner.textContent = 'All checks passed — data quality is healthy';
  }
}

// ── Overview ──────────────────────────────────────────────────
function renderOverview(){
  const s = DATA.summary;
  renderHero();

  // Ribbon replaced by hero banner above

  // Score cards
  document.getElementById('scoresGrid').innerHTML = getActiveScores().map((sc,i) => {
    const cls = scoreClass(sc.score);
    const pct = sc.score;
    // Mini sparkline from row count data
    const chartData = DATA.charts[sc.table];
    const counts = chartData ? chartData.row_counts.slice(-10) : [];
    const max = Math.max(...counts)||1;
    const pts = counts.map((v,i)=>`${i*(100/(counts.length-1||1))},${36-(v/max*32)}`).join(' ');
    const sparkColor = scoreColor(sc.score);
    return `
    <div class="score-card">
      <div class="score-card-top">
        <span class="score-table-name">${sc.table}</span>
        <span class="score-badge ${badgeClass(sc.score)}">${sc.status}</span>
      </div>
      <div class="score-number ${cls}">${sc.score}</div>
      <div class="score-sub">/ 100${sc.issues.length?` — ${sc.issues.length} issue(s)`:''}</div>
      <div class="sparkline-wrap">
        <svg width="100%" height="36" viewBox="0 0 100 36" preserveAspectRatio="none">
          ${counts.length>1?`<polyline points="${pts}" fill="none" stroke="${sparkColor}" stroke-width="2" stroke-linejoin="round" vector-effect="non-scaling-stroke"/>`:''}
        </svg>
      </div>
      <div class="score-bar-bg">
        <div class="score-bar-fill ${cls}" style="width:${pct}%"></div>
      </div>
      ${sc.issues.length?`<div style="margin-top:8px;font-size:10px;color:#6B7280">${sc.issues.slice(0,2).join(' · ')}</div>`:''}
    </div>`;
  }).join('');

  // Row trend chart (overview mini)
  destroyChart('rowTrendChart');
  const ctx1 = document.getElementById('rowTrendChart').getContext('2d');
  const labels = Object.values(DATA.charts)[0]?.dates?.slice(-14)||[];
  const datasets = Object.entries(DATA.charts).map(([table,d],i)=>({
    label: table,
    data: d.row_counts.slice(-14),
    borderColor: TABLE_COLORS[i%TABLE_COLORS.length],
    backgroundColor: 'transparent',
    tension: .3, pointRadius: 2, borderWidth: 2,
  }));
  CHARTS['rowTrendChart'] = new Chart(ctx1,{
    type:'line',
    data:{labels,datasets},
    options:{ plugins:{legend:{labels:{font:{size:11},boxWidth:12}}},
              scales:{ x:{ticks:{font:{size:10}},grid:{color:'#F3F4F6'}},
                       y:{ticks:{font:{size:10}},grid:{color:'#F3F4F6'}} },
              responsive:true, maintainAspectRatio:true }
  });

  // Severity donut
  destroyChart('severityChart');
  const ctx2 = document.getElementById('severityChart').getContext('2d');
  const summ = DATA.summary;
  CHARTS['severityChart'] = new Chart(ctx2,{
    type:'doughnut',
    data:{
      labels:['Critical','High','Medium','Passed'],
      datasets:[{data:[summ.critical,summ.high,summ.medium,summ.gx_passed],
        backgroundColor:['#EF4444','#F59E0B','#D97706','#22C55E'],
        borderWidth:0, hoverOffset:4}]
    },
    options:{plugins:{legend:{position:'right',labels:{font:{size:11},boxWidth:12}}},
             responsive:true, maintainAspectRatio:true, cutout:'60%'}
  });

  // Anomaly preview table
  const rows = DATA.anomalies.slice(0,5);
  document.getElementById('anomalyTablePreview').innerHTML = buildAnomalyTable(rows);
}

// ── Row trends ────────────────────────────────────────────────
function renderTrends(){
  if(!document.getElementById('page-trends').classList.contains('active')) return;
  destroyChart('allTablesChart');
  const ctx = document.getElementById('allTablesChart').getContext('2d');
  const activeCharts = getActiveCharts();
  const allDates = Object.values(activeCharts)[0]?.dates||[];
  const datasets = Object.entries(activeCharts).map(([table,d],i)=>({
    label:table, data:d.row_counts,
    borderColor:TABLE_COLORS[i%TABLE_COLORS.length],
    backgroundColor:'transparent', tension:.3, pointRadius:3, borderWidth:2,
  }));
  CHARTS['allTablesChart'] = new Chart(ctx,{
    type:'line',
    data:{labels:allDates,datasets},
    options:{plugins:{legend:{labels:{font:{size:11},boxWidth:12}}},
             scales:{x:{ticks:{font:{size:10}},grid:{color:'#F3F4F6'}},
                     y:{ticks:{font:{size:10}},grid:{color:'#F3F4F6'}}},
             responsive:true,maintainAspectRatio:true}
  });

  // Per-table area charts
  const wrap = document.getElementById('perTableCharts');
  wrap.innerHTML = Object.entries(getActiveCharts()).map(([table,d],i)=>{
    const latest = d.row_counts.slice(-1)[0]||0;
    const prev   = d.row_counts.slice(-2,-1)[0]||latest;
    const delta  = latest - prev;
    const color  = TABLE_COLORS[i%TABLE_COLORS.length];
    return `
    <div class="chart-card" style="margin-bottom:14px">
      <div class="chart-title">${table} — full history</div>
      <div class="chart-sub">Latest: <strong>${latest.toLocaleString()}</strong> rows
        <span style="color:${delta<0?'#EF4444':'#22C55E'};margin-left:6px;font-weight:500">
          ${delta>=0?'+':''}${delta.toLocaleString()} vs previous
        </span>
      </div>
      <canvas id="chart_${table}" height="80"></canvas>
    </div>`;
  }).join('');

  Object.entries(activeCharts).forEach(([table,d],i)=>{
    const color = TABLE_COLORS[i%TABLE_COLORS.length];
    const ctx = document.getElementById(`chart_${table}`)?.getContext('2d');
    if(!ctx) return;
    destroyChart(`chart_${table}`);
    CHARTS[`chart_${table}`] = new Chart(ctx,{
      type:'line',
      data:{labels:d.dates, datasets:[{
        label:'Row count', data:d.row_counts,
        borderColor:color, backgroundColor:color+'18',
        fill:true, tension:.3, pointRadius:2, borderWidth:2,
      }]},
      options:{plugins:{legend:{display:false}},
               scales:{x:{ticks:{font:{size:9},maxTicksLimit:8},grid:{color:'#F9FAFB'}},
                       y:{ticks:{font:{size:9}},grid:{color:'#F9FAFB'}}},
               responsive:true,maintainAspectRatio:true}
    });
  });
}

// ── Heatmap ───────────────────────────────────────────────────
function renderHeatmap(){
  if(!document.getElementById('page-heatmap').classList.contains('active')) return;
  const hm = DATA.null_heatmap;
  const metrics = Object.keys(hm);
  if(!metrics.length){ document.getElementById('heatmapContainer').innerHTML='<p style="color:#6B7280;font-size:13px;padding:12px">No null % data available.</p>'; return; }

  const allDates = [...new Set(Object.values(hm).flatMap(d=>Object.keys(d)))].sort().slice(-30);

  function nullColor(v){
    if(v===undefined||v===null) return '#F9FAFB';
    if(v===0) return '#F0FDF4';
    if(v<5)   return '#DCFCE7';
    if(v<10)  return '#FEF9C3';
    if(v<20)  return '#FEF3C7';
    if(v<50)  return '#FED7AA';
    return '#FEE2E2';
  }

  const dateHeaders = allDates.map(d=>`<th>${d.slice(5)}</th>`).join('');
  const rows = metrics.map(m=>{
    const cells = allDates.map(d=>{
      const v = hm[m][d];
      const bg = nullColor(v);
      const tip = v!==undefined?`${v.toFixed(1)}%`:'—';
      return `<td style="background:${bg}" title="${m}: ${tip}"></td>`;
    }).join('');
    return `<tr><td class="heatmap-row-label">${m}</td>${cells}</tr>`;
  }).join('');

  document.getElementById('heatmapContainer').innerHTML = `
    <div class="heatmap-wrap">
      <table class="heatmap-table">
        <thead><tr><th>Column</th>${dateHeaders}</tr></thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
    <div style="display:flex;align-items:center;gap:6px;margin-top:10px;font-size:10px;color:#6B7280">
      <span>0%</span>
      <div style="display:flex;gap:2px">
        ${['#F0FDF4','#DCFCE7','#FEF9C3','#FEF3C7','#FED7AA','#FEE2E2'].map(c=>`<div style="width:18px;height:10px;background:${c};border-radius:2px"></div>`).join('')}
      </div>
      <span>100%</span>
    </div>`;

  // Worst nulls bar chart
  const avgNulls = metrics.map(m=>{
    const vals = Object.values(hm[m]).filter(v=>v!==null&&v!==undefined);
    return {col:m, avg:vals.length?vals.reduce((a,b)=>a+b,0)/vals.length:0};
  }).sort((a,b)=>b.avg-a.avg).slice(0,10);

  destroyChart('worstNullsChart');
  const ctx = document.getElementById('worstNullsChart').getContext('2d');
  CHARTS['worstNullsChart'] = new Chart(ctx,{
    type:'bar',
    data:{
      labels:avgNulls.map(x=>x.col),
      datasets:[{label:'Avg null %', data:avgNulls.map(x=>+x.avg.toFixed(2)),
        backgroundColor:avgNulls.map(x=>x.avg>20?'#EF4444':x.avg>10?'#F59E0B':'#22C55E'),
        borderWidth:0, borderRadius:4}]
    },
    options:{indexAxis:'y',plugins:{legend:{display:false}},
             scales:{x:{ticks:{font:{size:10}},grid:{color:'#F9FAFB'},max:100},
                     y:{ticks:{font:{size:10}},grid:{display:false}}},
             responsive:true,maintainAspectRatio:true}
  });
}

// ── Anomalies ─────────────────────────────────────────────────
function renderAnomalies(){
  const tables = [...new Set(DATA.anomalies.map(a=>a.table))];
  tableFilter = new Set(tables);  // reset filter with current tables

  document.getElementById('tableFilter').innerHTML = tables.map(t=>
    `<span class="fpill active" data-val="${t}" onclick="toggleFilter(this,'table')">${t}</span>`
  ).join('');

  updateAnomalyView();
}

function updateAnomalyView(){
  const filtered = DATA.anomalies.filter(a=>
    sevFilter.has(a.severity) && tableFilter.has(a.table)
  );
  const crit   = filtered.filter(a=>a.severity==='CRITICAL').length;
  const high   = filtered.filter(a=>a.severity==='HIGH').length;
  const medium = filtered.filter(a=>a.severity==='MEDIUM').length;

  document.getElementById('anomalyStats').innerHTML = `
    <div class="stat-box critical"><div class="stat-num">${crit}</div><div class="stat-label">Critical</div></div>
    <div class="stat-box high"><div class="stat-num">${high}</div><div class="stat-label">High</div></div>
    <div class="stat-box medium"><div class="stat-num">${medium}</div><div class="stat-label">Medium</div></div>
    <div class="stat-box"><div class="stat-num">${filtered.length}</div><div class="stat-label">Total shown</div></div>
  `;
  document.getElementById('fullAnomalyTable').innerHTML = buildAnomalyTable(filtered);
}

function toggleFilter(el, type){
  el.classList.toggle('active');
  const val = el.dataset.val;
  const set = type==='sev'?sevFilter:tableFilter;
  if(el.classList.contains('active')) set.add(val); else set.delete(val);
  updateAnomalyView();
}

function buildAnomalyTable(rows){
  if(!rows.length) return '<p style="color:var(--text-muted);font-size:13px;padding:12px">No anomalies match the current filters.</p>';
  const trs = rows.map(a=>{
    const explanation = a.explanation
      ? `<tr><td colspan="8" style="padding:6px 10px 10px;border-bottom:.5px solid var(--border)">
           <div style="display:flex;align-items:flex-start;gap:8px;background:var(--bg-subtle);border-radius:6px;padding:8px 12px;border-left:3px solid ${a.severity==='CRITICAL'?'var(--red)':a.severity==='HIGH'?'var(--amber)':'var(--green)'}">
             <span style="font-size:14px;flex-shrink:0">🤖</span>
             <span style="font-size:12px;color:var(--text-secondary);line-height:1.5">${a.explanation}</span>
           </div>
         </td></tr>`
      : '';
    return `
    <tr>
      <td>${a.detected_at||'—'}</td>
      <td><strong>${a.table}</strong></td>
      <td>${sevBadge(a.severity)}</td>
      <td>${chip(a.metric)}</td>
      <td>${a.detector}</td>
      <td class="val-bad">${a.today}</td>
      <td class="val-ok">~${a.expected}</td>
      <td class="val-bad">${a.score}</td>
    </tr>${explanation}`;
  }).join('');
  return `
    <table class="dq-table">
      <thead><tr>
        <th>Detected</th><th>Table</th><th>Severity</th>
        <th>Metric</th><th>Detector</th>
        <th>Today</th><th>Expected</th><th>Score</th>
      </tr></thead>
      <tbody>${trs}</tbody>
    </table>`;
}

// ── Alert log ─────────────────────────────────────────────────
function renderAlerts(){
  if(!document.getElementById('page-alerts').classList.contains('active')) return;
  const log = DATA.alert_log||[];
  const crit = log.filter(a=>a.severity==='CRITICAL').length;
  const channels = [...new Set(log.map(a=>a.channel))];

  document.getElementById('alertRibbon').innerHTML = `
    <div class="ribbon-card info">
      <div class="ribbon-label">Total alerts sent</div>
      <div class="ribbon-value">${log.length}</div>
    </div>
    <div class="ribbon-card ${crit>0?'danger':'success'}">
      <div class="ribbon-label">Critical alerts</div>
      <div class="ribbon-value">${crit}</div>
    </div>
    <div class="ribbon-card info">
      <div class="ribbon-label">Channels used</div>
      <div class="ribbon-value">${channels.length}</div>
      <div class="ribbon-delta">${channels.join(', ')}</div>
    </div>
  `;

  // Channel breakdown chart
  destroyChart('alertChannelChart');
  const chCounts = channels.map(c=>log.filter(a=>a.channel===c).length);
  const ctx = document.getElementById('alertChannelChart').getContext('2d');
  CHARTS['alertChannelChart'] = new Chart(ctx,{
    type:'bar',
    data:{labels:channels, datasets:[{data:chCounts,
      backgroundColor:['#3B82F6','#10B981','#8B5CF6'],borderWidth:0,borderRadius:4}]},
    options:{plugins:{legend:{display:false}},
             scales:{x:{ticks:{font:{size:11}},grid:{display:false}},
                     y:{ticks:{font:{size:10},stepSize:1},grid:{color:'#F9FAFB'}}},
             responsive:true,maintainAspectRatio:true,indexAxis:'x'}
  });

  // Alert table
  const trs = log.map(a=>`
    <tr>
      <td>${a.sent_at?.slice(0,19)||'—'}</td>
      <td><span style="background:#EFF6FF;color:#1D4ED8;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600">${a.channel}</span></td>
      <td>${sevBadge(a.severity)}</td>
      <td>${a.summary}</td>
    </tr>`).join('');

  document.getElementById('alertTable').innerHTML = log.length ? `
    <table class="dq-table">
      <thead><tr><th>Sent at</th><th>Channel</th><th>Severity</th><th>Summary</th></tr></thead>
      <tbody>${trs}</tbody>
    </table>` : '<p style="color:#6B7280;font-size:13px;padding:12px">No alerts sent yet. Run python run_alerting.py</p>';
}

// ── Connections ───────────────────────────────────────────────
function renderConnections(){
  const tables = Object.keys(DATA.charts);
  document.getElementById('connectionCards').innerHTML = `
    <div class="conn-card">
      <div class="conn-dot"></div>
      <div>
        <div class="conn-name">ecommerce_db</div>
        <div class="conn-detail">SQLite · ./data/ecommerce.db · ${tables.length} tables monitored: ${tables.join(', ')}</div>
      </div>
    </div>`;
}

// ── Page navigation ───────────────────────────────────────────
function showPage(name, el){
  document.querySelectorAll('.page').forEach(p=>p.classList.remove('active'));
  document.getElementById(`page-${name}`).classList.add('active');
  document.querySelectorAll('.nav-link').forEach(n=>n.classList.remove('active'));
  if(el) el.classList.add('active');
  // Re-render charts for the newly visible page
  if(Object.keys(DATA).length){
    if(name === 'trends')    renderTrends();
    if(name === 'anomalies') renderAnomalies();
    if(name === 'history')   renderHistory();
    if(name === 'ai')        initAiGreeting();
    if(name === 'heatmap')   renderHeatmap();
    if(name === 'alerts')    renderAlerts();
    if(name === 'overview')  renderOverview();
  }
}

// ── Chart cleanup ─────────────────────────────────────────────
function destroyChart(id){
  if(CHARTS[id]){ CHARTS[id].destroy(); delete CHARTS[id]; }
}


// ── Dark / Light mode toggle ──────────────────────────────────


function setTheme(theme){
  document.body.setAttribute('data-theme', theme);
  localStorage.setItem('dq-theme', theme);
  const btn = document.getElementById('themeBtn');
  if(btn) btn.textContent = theme === 'dark' ? '☀️' : '🌙';
  updateChartsTheme(theme === 'dark');
}

function updateChartsTheme(dark){
  const gridColor = dark ? '#2D3748' : '#F3F4F6';
  const textColor = dark ? '#94A3B8' : '#6B7280';
  Object.values(CHARTS).forEach(chart => {
    if(!chart || !chart.options) return;
    const scales = chart.options.scales || {};
    Object.values(scales).forEach(scale => {
      if(scale.grid)  scale.grid.color  = gridColor;
      if(scale.ticks) scale.ticks.color = textColor;
    });
    if(chart.options.plugins?.legend?.labels)
      chart.options.plugins.legend.labels.color = textColor;
    chart.update('none');
  });
}

function initTheme(){
  const saved = localStorage.getItem('dq-theme') || 'light';
  setTheme(saved);
}

// ── History page ──────────────────────────────────────────────
function renderHistory(){
  const history      = DATA.history       || [];
  const alertHistory = DATA.alert_history || [];

  const ribbon = document.getElementById('historyRibbon');
  if(ribbon){
    const totalRuns   = history.length;
    const totalTables = history.reduce((a,r)=>a+(r.table_count||0),0);
    const totalAlerts = alertHistory.length;
    const sources     = [...new Set(history.flatMap(r=>r.sources||[]))];
    ribbon.innerHTML = `
      <div class="ribbon-card info">
        <div class="ribbon-label">Pipeline runs</div>
        <div class="ribbon-value">${totalRuns}</div>
        <div class="ribbon-delta">last 50 stored</div>
      </div>
      <div class="ribbon-card info">
        <div class="ribbon-label">Tables profiled</div>
        <div class="ribbon-value">${totalTables}</div>
        <div class="ribbon-delta">across all runs</div>
      </div>
      <div class="ribbon-card ${totalAlerts>0?'danger':'success'}">
        <div class="ribbon-label">Alerts sent</div>
        <div class="ribbon-value">${totalAlerts}</div>
        <div class="ribbon-delta">total history</div>
      </div>
      <div class="ribbon-card info">
        <div class="ribbon-label">Sources tracked</div>
        <div class="ribbon-value">${sources.length}</div>
        <div class="ribbon-delta">${sources.join(', ')}</div>
      </div>`;
  }

  const runsEl = document.getElementById('historyRuns');
  if(runsEl){
    if(!history.length){
      runsEl.innerHTML = '<p style="color:var(--text-muted);font-size:13px;padding:12px">No pipeline run history yet. Run python main.py first.</p>';
    } else {
      const rows = history.map(r=>{
        const srcs   = (r.sources||[]).join(', ');
        const tables = (r.tables||[]).map(t=>`${t.source}.${t.table}`).slice(0,5).join(', ');
        const more   = (r.table_count||0) > 5 ? ` +${r.table_count-5} more` : '';
        return `<tr>
          <td>${r.run_at||'—'}</td>
          <td>${srcs}</td>
          <td><strong>${r.table_count||0}</strong></td>
          <td style="font-size:10px;color:var(--text-muted)">${tables}${more}</td>
        </tr>`;
      }).join('');
      runsEl.innerHTML = `
        <table class="dq-table">
          <thead><tr><th>Run at</th><th>Sources</th><th>Tables</th><th>Tables profiled</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
    }
  }

  const alertsEl = document.getElementById('historyAlerts');
  if(alertsEl){
    if(!alertHistory.length){
      alertsEl.innerHTML = '<p style="color:var(--text-muted);font-size:13px;padding:12px">No alerts in history yet. Run python run_alerting.py first.</p>';
    } else {
      const rows = alertHistory.map(a=>`
        <tr>
          <td>${(a.sent_at||'—').slice(0,19)}</td>
          <td><span style="background:var(--accent-light);color:var(--accent);padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600">${a.channel||'—'}</span></td>
          <td>${sevBadge(a.severity||'')}</td>
          <td style="font-size:11px;color:var(--text-secondary)">${a.summary||'—'}</td>
        </tr>`).join('');
      alertsEl.innerHTML = `
        <table class="dq-table">
          <thead><tr><th>Sent at</th><th>Channel</th><th>Severity</th><th>Summary</th></tr></thead>
          <tbody>${rows}</tbody>
        </table>`;
    }
  }
}

async function downloadReport(fmt){
  const API = 'http://127.0.0.1:5050/api/report';
  const urls = {
    pdf:   `${API}/pdf`,
    csv:   `${API}/csv`,
    excel: `${API}/excel`,
  };

  const format = fmt || 'pdf';
  const btn    = event?.target;
  const origText = btn?.textContent;
  if(btn){ btn.textContent = 'Generating...'; btn.style.opacity = '.6'; }

  try {
    const response = await fetch(urls[format]);
    if(!response.ok){
      const err = await response.json().catch(()=>({message:'Unknown error'}));
      throw new Error(err.message || `HTTP ${response.status}`);
    }
    // Get filename from Content-Disposition header
    const cd   = response.headers.get('Content-Disposition') || '';
    const match = cd.match(/filename=([^;]+)/);
    const fname = match ? match[1].replace(/"/g,'') : `dq_report.${format}`;

    // Trigger browser download
    const blob = await response.blob();
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href     = url;
    a.download = fname;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);

    if(btn){ btn.textContent = '✓ Downloaded'; btn.style.opacity='1'; }
    setTimeout(()=>{ if(btn){ btn.textContent=origText; btn.style.opacity='1'; } }, 2000);

  } catch(e) {
    if(btn){ btn.textContent = origText; btn.style.opacity = '1'; }
    if(e.message && e.message.includes('fetch')){
      alert('Could not reach the API server.\n\nMake sure this is running in a second CMD window:\n\n  python api_server.py\n\nThen try again.');
    } else {
      alert('Report generation failed: ' + e.message + '\n\nAlternatively run: python generate_report.py');
    }
  }
}


// ── Boot ──────────────────────────────────────────────────────
initTheme();
loadAllData();

// ── Connection Manager ─────────────────────────────────────────
const API = 'http://localhost:5050/api';
let currentDialect = 'sqlite';

function selectDialect(d){
  currentDialect = d;
  document.querySelectorAll('.db-type-card').forEach(c=>c.classList.remove('active'));
  document.getElementById('dtype-'+d).classList.add('active');
  document.querySelectorAll('.conn-form').forEach(f=>f.classList.remove('active'));
  document.getElementById('form-'+d).classList.add('active');
  hideFeedback();
}

function getFormData(){
  const d = currentDialect;
  if(d==='sqlite')     return {dialect:'sqlite',     name:v('sl-name'), path:v('sl-path')};
  if(d==='postgresql') return {dialect:'postgresql', name:v('pg-name'), database:v('pg-db'),  host:v('pg-host'), port:parseInt(v('pg-port'))||5432, user:v('pg-user'), password:v('pg-pass')};
  if(d==='mysql')      return {dialect:'mysql',      name:v('my-name'), database:v('my-db'),  host:v('my-host'), port:parseInt(v('my-port'))||3306, user:v('my-user'), password:v('my-pass')};
  if(d==='cloud')      return {dialect:'cloud',      name:v('cl-name'), cloud_type:v('cl-type'), connection_string:v('cl-dsn'), credentials_path:v('cl-creds')};
  return {};
}

function v(id){ const el=document.getElementById(id); return el?el.value.trim():''; }

function showFeedback(msg, type){
  const el = document.getElementById('connFeedback');
  el.style.display = 'block';
  el.className = 'conn-feedback ' + type;
  el.textContent = msg;
}
function hideFeedback(){
  document.getElementById('connFeedback').style.display='none';
  document.getElementById('tablesFound').style.display='none';
}

async function testConnection(){
  const data = getFormData();
  if(!data.name){ showFeedback('Please enter a connection name.','err'); return; }
  showFeedback('Testing connection...','loading');
  try {
    const r = await fetch(`${API}/connections/test`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    const res = await r.json();
    if(res.ok){
      showFeedback('✓ ' + res.message, 'ok');
      if(res.tables && res.tables.length){
        const tf = document.getElementById('tablesFound');
        tf.style.display='block';
        tf.innerHTML = 'Tables found: ' + res.tables.map(t=>`<code style="margin:0 3px">${t}</code>`).join('');
      }
    } else {
      showFeedback('✗ ' + res.message, 'err');
    }
  } catch(e){
    showFeedback('Could not reach API. Make sure you started the server with: python dashboard_server.py','err');
  }
}

async function saveConnection(){
  const data = getFormData();
  if(!data.name){ showFeedback('Please enter a connection name.','err'); return; }
  showFeedback('Saving...','loading');
  try {
    const r = await fetch(`${API}/connections`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});
    const res = await r.json();
    if(res.ok){
      showFeedback('✓ ' + res.message, 'ok');
      loadConnectionCards();
    } else {
      showFeedback('✗ ' + res.message, 'err');
    }
  } catch(e){
    showFeedback('Could not reach API. Make sure you started the server with: python dashboard_server.py','err');
  }
}

async function loadConnectionCards(){
  try {
    const r = await fetch(`${API}/connections`);
    const res = await r.json();
    const wrap = document.getElementById('connectionCards');
    if(!res.sources || !res.sources.length){
      wrap.innerHTML='<p style="color:#6B7280;font-size:13px;padding:8px 0">No connections saved yet.</p>';
      return;
    }
    wrap.innerHTML = res.sources.map(s=>{
      const loc = s.host ? `${s.host}:${s.port||''}` : s.path || s.connection_string || '';
      const icon = {sqlite:'&#128193;',postgresql:'&#128024;',mysql:'&#128031;',cloud:'&#9729;'}[s.dialect]||'&#128308;';
      return `
      <div class="saved-conn-card">
        <span style="font-size:20px">${icon}</span>
        <div style="flex:1">
          <div style="font-size:13px;font-weight:500;color:#111827">${s.name}</div>
          <div style="font-size:11px;color:#9CA3AF">${(s.dialect||'').toUpperCase()} · ${loc}</div>
        </div>
        <div class="saved-conn-actions">
          <button class="btn-sm primary" onclick="profileConn('${s.name}')">&#9654; Profile</button>
          <button class="btn-sm" onclick="listTables('${s.name}')">&#128203; Tables</button>
          <button class="btn-sm danger" onclick="removeConn('${s.name}')">&#128465; Remove</button>
        </div>
      </div>`;
    }).join('');
  } catch(e){
    document.getElementById('connectionCards').innerHTML =
      '<p style="color:#6B7280;font-size:13px">Start <code>python dashboard_server.py</code> to manage connections.</p>';
  }
}

async function listTables(name){
  showFeedback(`Fetching tables for ${name}...`,'loading');
  try {
    const r = await fetch(`${API}/connections/${name}/tables`);
    const res = await r.json();
    if(res.ok){
      showFeedback(`✓ Tables in ${name}: ${res.tables.join(', ')}`, 'ok');
    } else {
      showFeedback('✗ ' + res.message, 'err');
    }
  } catch(e){ showFeedback('API not reachable.','err'); }
}

async function profileConn(name){
  showFeedback(`Running profiler on ${name}... this may take a few seconds.`,'loading');
  try {
    const r = await fetch(`${API}/connections/${name}/profile`,{method:'POST'});
    const res = await r.json();
    showFeedback(res.ok ? '✓ ' + res.message : '✗ ' + res.message, res.ok?'ok':'err');
    if(res.ok) setTimeout(()=>loadAllData(), 1000);
  } catch(e){ showFeedback('API not reachable.','err'); }
}

async function removeConn(name){
  if(!confirm(`Remove connection "${name}"?`)) return;
  try {
    const r = await fetch(`${API}/connections/${name}`,{method:'DELETE'});
    const res = await r.json();
    showFeedback(res.ok ? '✓ ' + res.message : '✗ ' + res.message, res.ok?'ok':'err');
    if(res.ok) loadConnectionCards();
  } catch(e){ showFeedback('API not reachable.','err'); }
}

// Load connection cards when connections page is shown
const _origShowPage = showPage;
window.showPage = function(name, el){
  _origShowPage(name, el);
  if(name==='connections') loadConnectionCards();
};


// ── Dark mode ────────────────────────────────────────────────
function toggleTheme(){
  const isDark = document.body.getAttribute('data-theme') === 'dark';
  setTheme(isDark ? 'light' : 'dark');
}

// ── Hamburger menu ────────────────────────────────────────────
function toggleMobileMenu(){
  document.getElementById('mobileMenu').classList.toggle('open');
}
document.addEventListener('click', function(e){
  const m = document.getElementById('mobileMenu');
  const h = document.querySelector('.hamburger');
  if(m && h && !m.contains(e.target) && !h.contains(e.target)) m.classList.remove('open');
});

// ── Hero banner ───────────────────────────────────────────────
function renderHero(){
  const s = DATA.summary;
  if(!s) return;
  const cls = s.overall_status==='CLEAN'?'clean':s.overall_status==='HIGH'?'high':'critical';
  const sc  = s.avg_dq_score>=90?'healthy':s.avg_dq_score>=70?'warning':'critical';
  const el = (id) => document.getElementById(id);
  if(el('heroBanner')) el('heroBanner').className = 'hero-banner '+cls;
  if(el('heroScore'))  { el('heroScore').className='hero-score '+sc; el('heroScore').textContent=s.avg_dq_score; }
  if(el('heroStatus')) el('heroStatus').textContent = s.overall_status==='CLEAN'?'All systems clean':s.overall_status+' — action required';
  if(el('heroDetail')) el('heroDetail').textContent = s.total_anomalies+' anomaly(s) · '+s.total_snapshots+' snapshots · last run '+(s.last_run||'—');
  if(el('hAnomaly'))   el('hAnomaly').textContent = s.total_anomalies;
  if(el('hGxFail'))    el('hGxFail').textContent  = s.gx_failed;
  if(el('hSnaps'))     el('hSnaps').textContent   = s.total_snapshots;
}

// Apply saved theme immediately
(function(){
  const t = localStorage.getItem('dq-theme')||'light';
  document.body.setAttribute('data-theme', t);
  const btn = document.getElementById('themeBtn');
  if(btn) btn.textContent = t==='dark'?'☀️':'🌙';
})();

// ── AI Assistant ───────────────────────────────────────────────
const AI_API = 'http://127.0.0.1:5050/api/ask';

function toggleAIPanel(){
  const panel   = document.getElementById('aiPanel');
  const overlay = document.getElementById('aiOverlay');
  const isOpen  = panel.classList.contains('open');
  panel.classList.toggle('open', !isOpen);
  overlay.classList.toggle('open', !isOpen);
  if(!isOpen) document.getElementById('aiInput')?.focus();
}

function askSuggestion(el){
  const q = el.textContent;
  document.getElementById('aiInput').value = q;
  sendAIQuestion();
}

function _mdToHtml(text){
  return text
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    .replace(/\n•\s/g, '\n• ')
    .replace(/\n/g, '<br>');
}

async function sendAIQuestion(){
  const input  = document.getElementById('aiInput');
  const sendBtn= document.getElementById('aiSendBtn');
  const msgs   = document.getElementById('aiMessages');
  const question = input.value.trim();
  if(!question) return;

  // Show user message
  msgs.innerHTML += `
    <div class="ai-msg ai-msg-user">
      <div class="ai-bubble">${question}</div>
    </div>`;
  input.value = '';
  sendBtn.disabled = true;
  sendBtn.textContent = '...';

  // Show thinking indicator
  const thinkId = 'think-' + Date.now();
  msgs.innerHTML += `
    <div class="ai-msg ai-msg-bot" id="${thinkId}">
      <div class="ai-bubble">
        <div class="ai-thinking">
          <div class="ai-dot"></div>
          <div class="ai-dot"></div>
          <div class="ai-dot"></div>
        </div>
      </div>
    </div>`;
  msgs.scrollTop = msgs.scrollHeight;

  try {
    const response = await fetch(AI_API, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({question})
    });

    const data = await response.json();

    // Remove thinking indicator
    document.getElementById(thinkId)?.remove();

    if(data.ok){
      const modeLabel = data.mode === 'root_cause'
        ? '<span class="ai-mode-badge ai-mode-root_cause">Root Cause Analysis</span><br>'
        : '<span class="ai-mode-badge ai-mode-general">General Q&A</span><br>';
      const modelNote = `<div style="font-size:10px;color:var(--text-muted);margin-top:8px;text-align:right">via ${data.model||'AI'}</div>`;

      msgs.innerHTML += `
        <div class="ai-msg ai-msg-bot">
          <div class="ai-bubble">
            ${modeLabel}${_mdToHtml(data.answer)}${modelNote}
          </div>
        </div>`;
    } else {
      msgs.innerHTML += `
        <div class="ai-msg ai-msg-bot">
          <div class="ai-bubble" style="color:var(--red-text);background:var(--red-light)">
            Sorry — ${data.message || 'something went wrong'}.<br>
            <small>Make sure <code>python api_server.py</code> is running.</small>
          </div>
        </div>`;
    }
  } catch(e){
    document.getElementById(thinkId)?.remove();
    msgs.innerHTML += `
      <div class="ai-msg ai-msg-bot">
        <div class="ai-bubble" style="color:var(--red-text);background:var(--red-light)">
          Could not reach the API server.<br>
          <small>Run <code>python api_server.py</code> in a CMD window.</small>
        </div>
      </div>`;
  }

  sendBtn.disabled = false;
  sendBtn.textContent = 'Send';
  msgs.scrollTop = msgs.scrollHeight;
}
