import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

const BASE = window.location.origin;

// ── state ─────────────────────────────────────────────────────────────────────
let nodes        = [];  // { id, label, summary, type, status, x, y, z }
let links        = [];  // { source, target, origin, confidence, weight, _src, _tgt }
let allRelations = [];

// ── Three.js bootstrap ────────────────────────────────────────────────────────
const canvas  = document.getElementById('graph');
const tooltip = document.getElementById('tooltip');

const renderer = new THREE.WebGLRenderer({ canvas, antialias: true });
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
renderer.setClearColor(0x0f0e0c);

const scene  = new THREE.Scene();
const camera = new THREE.PerspectiveCamera(55, 1, 1, 5000);
camera.position.set(0, 0, 500);

const controls = new OrbitControls(camera, renderer.domElement);
controls.enableDamping = true;
controls.dampingFactor = 0.07;
controls.minDistance   = 50;
controls.maxDistance   = 2000;

scene.add(new THREE.AmbientLight(0xffffff, 0.55));
const sun = new THREE.DirectionalLight(0xffffff, 1.0);
sun.position.set(200, 300, 200);
scene.add(sun);

function resize() {
  const w = window.innerWidth, h = window.innerHeight;
  renderer.setSize(w, h);
  camera.aspect = w / h;
  camera.updateProjectionMatrix();
}
resize();
window.addEventListener('resize', resize);

// ── scene objects ─────────────────────────────────────────────────────────────
const NODE_GEO = new THREE.SphereGeometry(6, 16, 12);
const NODE_MAT = new THREE.MeshLambertMaterial();
let   nodeMesh = null;  // InstancedMesh — rebuilt on each data refresh
let   edgeLine = null;  // LineSegments  — rebuilt on each data refresh

const NODE_COLORS = {
  ready:      new THREE.Color(0x0F6E56),
  processing: new THREE.Color(0x7A6B10),
  error:      new THREE.Color(0x712B13),
  pending:    new THREE.Color(0x444441),
};
const EDGE_COLORS = {
  content_heuristic: new THREE.Color(0x085041),
  content_llm:       new THREE.Color(0x085041),
  semantic:          new THREE.Color(0x3C3489),
  asserted:          new THREE.Color(0x712B13),
};
const _mat = new THREE.Matrix4();

function buildScene() {
  if (nodeMesh) { scene.remove(nodeMesh); nodeMesh = null; }
  if (edgeLine) { scene.remove(edgeLine); edgeLine.geometry.dispose(); edgeLine.material.dispose(); edgeLine = null; }
  if (!nodes.length) return;

  // instanced spheres
  nodeMesh = new THREE.InstancedMesh(NODE_GEO, NODE_MAT, nodes.length);
  nodes.forEach((n, i) => {
    _mat.setPosition(n.x, n.y, n.z);
    nodeMesh.setMatrixAt(i, _mat);
    nodeMesh.setColorAt(i, NODE_COLORS[n.status] ?? NODE_COLORS.pending);
  });
  nodeMesh.instanceMatrix.needsUpdate = true;
  nodeMesh.instanceColor.needsUpdate  = true;
  scene.add(nodeMesh);

  // edge lines
  const validLinks = links.filter(l => l._src && l._tgt);
  const ePos = new Float32Array(validLinks.length * 6);
  const eCol = new Float32Array(validLinks.length * 6);
  validLinks.forEach((l, i) => {
    const b = i * 6;
    ePos[b]   = l._src.x; ePos[b+1] = l._src.y; ePos[b+2] = l._src.z;
    ePos[b+3] = l._tgt.x; ePos[b+4] = l._tgt.y; ePos[b+5] = l._tgt.z;
    const c = EDGE_COLORS[l.origin] ?? EDGE_COLORS.content_heuristic;
    eCol[b]   = c.r; eCol[b+1] = c.g; eCol[b+2] = c.b;
    eCol[b+3] = c.r; eCol[b+4] = c.g; eCol[b+5] = c.b;
  });
  const eGeo = new THREE.BufferGeometry();
  eGeo.setAttribute('position', new THREE.BufferAttribute(ePos, 3));
  eGeo.setAttribute('color',    new THREE.BufferAttribute(eCol, 3));
  edgeLine = new THREE.LineSegments(
    eGeo,
    new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, opacity: 0.55 }),
  );
  scene.add(edgeLine);
}

// ── render loop ───────────────────────────────────────────────────────────────
renderer.setAnimationLoop(() => {
  controls.update();
  renderer.render(scene, camera);
});

// ── picking & interaction ─────────────────────────────────────────────────────
const raycaster = new THREE.Raycaster();
const mouse2d   = new THREE.Vector2();
let   downPos   = null;

function toNDC(e) {
  mouse2d.x =  (e.clientX / window.innerWidth)  * 2 - 1;
  mouse2d.y = -(e.clientY / window.innerHeight) * 2 + 1;
}

canvas.addEventListener('pointermove', e => {
  if (!nodeMesh) return;
  toNDC(e);
  raycaster.setFromCamera(mouse2d, camera);
  const hits = raycaster.intersectObject(nodeMesh);
  if (hits.length) {
    const n = nodes[hits[0].instanceId];
    tooltip.textContent   = n.label;
    tooltip.style.left    = (e.clientX + 14) + 'px';
    tooltip.style.top     = (e.clientY -  8) + 'px';
    tooltip.style.opacity = '1';
  } else {
    tooltip.style.opacity = '0';
  }
});
canvas.addEventListener('pointerleave', () => { tooltip.style.opacity = '0'; });

canvas.addEventListener('pointerdown', e => { downPos = { x: e.clientX, y: e.clientY }; });

canvas.addEventListener('pointerup', e => {
  const isClick = downPos && Math.sqrt(
    (e.clientX - downPos.x) ** 2 + (e.clientY - downPos.y) ** 2,
  ) < 5;
  downPos = null;
  if (!isClick || !nodeMesh) return;
  toNDC(e);
  raycaster.setFromCamera(mouse2d, camera);
  const hits = raycaster.intersectObject(nodeMesh);
  if (hits.length) showDetail(nodes[hits[0].instanceId]);
});

// ── node detail panel ─────────────────────────────────────────────────────────
function showDetail(n) {
  const rels = links.filter(l =>
    (l._src && l._src.id === n.id) || (l._tgt && l._tgt.id === n.id),
  );
  document.getElementById('detail').innerHTML =
    '<strong>' + escHtml(n.type) + '</strong><br>' +
    '<span style="color:var(--hint);font-size:10px">' + n.id + '</span><br><br>' +
    escHtml(n.summary || n.label || '') + '<br><br>' +
    '<div class="pill-row">' +
    rels.map(r =>
      '<span class="badge b-' +
      (r.origin && r.origin.includes('content') ? 'content' : r.origin === 'asserted' ? 'asserted' : 'semantic') +
      '">' + (r.origin ?? '') + ' ' + (r.confidence != null ? r.confidence.toFixed(2) : '') + '</span>',
    ).join('') +
    '</div>';
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
const wsUrl = BASE.replace(/^http/, 'ws') + '/stream';
let ws;
function connectWs() {
  ws = new WebSocket(wsUrl);
  ws.onopen    = () => setWsStatus(true);
  ws.onclose   = () => { setWsStatus(false); setTimeout(connectWs, 3000); };
  ws.onmessage = e => {
    const ev = JSON.parse(e.data);
    pushEvent(ev);
    if (['complete', 'stage_change', 'error', 'worker_crash', 'retune_scheduled'].includes(ev.type)) {
      setTimeout(refresh, 600);
    }
  };
}
connectWs();

function setWsStatus(live) {
  const el = document.getElementById('ws-status');
  el.textContent = live ? 'live' : 'reconnecting…';
  el.className   = 'stat ' + (live ? 'ws-live' : 'ws-off');
}

function pushEvent(ev) {
  const el   = document.getElementById('events');
  const line = document.createElement('div');
  line.className    = 'ev-line';
  line.dataset.evTs = String(Date.now());
  const ts = new Date().toLocaleTimeString();
  let detail = ev.job_id ? ev.job_id.slice(0, 8) : '';
  if (ev.stage) detail += ' <span style="color:var(--amber)">→ ' + escHtml(ev.stage) + '</span>';
  if (ev.type === 'worker_crash') detail = escHtml(ev.name ?? 'unknown worker');
  if (ev.type === 'progress' && ev.payload?.chunks_done != null)
    detail += ' <span class="ev-detail">' + ev.payload.chunks_done + '/' + ev.payload.chunks_total + ' chunks</span>';
  line.innerHTML = '<span class="ev-ts">' + ts + '</span><span class="ev-type">' + escHtml(ev.type) + '</span> ' + detail;
  el.prepend(line);
  pruneEvents(el);
}

function pruneEvents(el) {
  const cutoff = Date.now() - 5 * 60 * 1000;
  Array.from(el.children).forEach(c => { if (+(c.dataset.evTs ?? 0) < cutoff) c.remove(); });
}

// ── data refresh ──────────────────────────────────────────────────────────────
async function refresh() {
  const [statusRes, entitiesRes, relationsRes, jobsRes] = await Promise.all([
    fetch(BASE + '/status'),
    fetch(BASE + '/entities?limit=200'),
    fetch(BASE + '/relations?limit=500&min_confidence=0.4'),
    fetch(BASE + '/jobs?limit=30'),
  ]);
  const status    = await statusRes.json();
  const entities  = await entitiesRes.json();
  const relations = await relationsRes.json();
  const jobs      = await jobsRes.json();

  document.getElementById('s-entities').textContent  = status.data?.entity_count   ?? '?';
  document.getElementById('s-chunks').textContent    = status.data?.chunk_count    ?? '?';
  document.getElementById('s-relations').textContent = status.data?.relation_count ?? '?';
  document.getElementById('s-queue').textContent     = status.data?.queue_depth    ?? '?';

  renderJobs(jobs.data?.jobs ?? []);

  // Merge incoming nodes, preserving existing 3-D positions so layout is stable
  const existPos = new Map(nodes.map(n => [n.id, n]));
  const sc = 300;
  nodes = (entities.data?.entities ?? []).map(e => {
    const ex  = existPos.get(e.id);
    const lbl = (e.summary || e.type || e.id).slice(0, 60);
    return ex
      ? Object.assign(ex, { type: e.type, status: e.status, label: lbl, summary: e.summary ?? '' })
      : {
          id: e.id, type: e.type, status: e.status, label: lbl, summary: e.summary ?? '',
          x: (Math.random() - 0.5) * sc,
          y: (Math.random() - 0.5) * sc,
          z: (Math.random() - 0.5) * sc,
        };
  });

  const nodeIds = new Set(nodes.map(n => n.id));
  const nodeIdx = new Map(nodes.map((n, i) => [n.id, i]));
  allRelations  = relations.data?.relations ?? [];
  links = allRelations
    .filter(r => nodeIds.has(r.source_id) && nodeIds.has(r.target_id))
    .map(r => ({
      source: r.source_id, target: r.target_id,
      origin: r.origin, weight: r.weight, confidence: r.confidence,
      _src: nodes[nodeIdx.get(r.source_id)],
      _tgt: nodes[nodeIdx.get(r.target_id)],
    }));

  buildScene();
  renderHistogram(allRelations);
}

// ── pipeline panel ────────────────────────────────────────────────────────────
const ACTIVE_STAGES = new Set(['fetching', 'chunking', 'embedding', 'sectioning', 'summarising', 'extracting']);

function stageClass(s) {
  if (s === 'done')   return 's-done';
  if (s === 'error')  return 's-error';
  if (s === 'queued') return 's-queued';
  return ACTIVE_STAGES.has(s) ? 's-active' : 's-queued';
}

function renderJobs(jobs) {
  const el     = document.getElementById('jobs-list');
  const cutoff = Date.now() - 5 * 60 * 1000;
  const visible = jobs.filter(j =>
    j.stage !== 'done' || !j.completed_at || +new Date(j.completed_at) > cutoff,
  );
  if (!visible.length) {
    el.innerHTML = '<div class="detail" style="color:var(--hint)">No recent jobs</div>';
    return;
  }
  el.innerHTML = visible.slice(0, 20).map(j => {
    const p    = j.progress ?? {};
    let   prog = '';
    if (p.chunks_total > 0) {
      prog = (p.chunks_done ?? 0) + '/' + p.chunks_total + 'c';
      if (p.sections_done   > 0) prog += ' ' + p.sections_done   + 's';
      if (p.relations_added > 0) prog += ' ' + p.relations_added + 'r';
    }
    return (
      '<div class="job-row">' +
      '<span class="job-kind">'  + escHtml(j.kind)  + '</span>' +
      '<span class="job-stage '  + stageClass(j.stage) + '">' + escHtml(j.stage) + '</span>' +
      (prog ? '<span class="job-progress">' + prog + '</span>' : '') +
      '<span class="job-age">'   + fmtAge(new Date(j.created_at)) + '</span>' +
      '</div>'
    );
  }).join('');
}

function fmtAge(d) {
  const s = Math.floor((Date.now() - d) / 1000);
  if (s < 60)   return s + 's';
  if (s < 3600) return Math.floor(s / 60) + 'm';
  return Math.floor(s / 3600) + 'h';
}

// ── confidence histogram ──────────────────────────────────────────────────────
function renderHistogram(rels) {
  const svg  = d3.select('#histogram');
  const rect = svg.node().getBoundingClientRect();
  const W = rect.width || 270, H = 80;
  svg.attr('viewBox', '0 0 ' + W + ' ' + H).selectAll('*').remove();
  if (!rels.length) return;
  const bins   = d3.bin().domain([0, 1]).thresholds(10)(rels.map(r => r.confidence));
  const xScale = d3.scaleLinear().domain([0, 1]).range([0, W]);
  const yScale = d3.scaleLinear().domain([0, d3.max(bins, b => b.length) || 1]).range([H - 4, 4]);
  svg.selectAll('rect').data(bins).join('rect')
    .attr('x',      b => xScale(b.x0) + 1)
    .attr('y',      b => yScale(b.length))
    .attr('width',  b => Math.max(0, xScale(b.x1) - xScale(b.x0) - 2))
    .attr('height', b => H - 4 - yScale(b.length))
    .attr('fill', '#534AB7')
    .attr('opacity', 0.7);
}

// ── util ──────────────────────────────────────────────────────────────────────
function escHtml(s) {
  return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

refresh();
setInterval(refresh, 30_000);
setInterval(() => pruneEvents(document.getElementById('events')), 60_000);
