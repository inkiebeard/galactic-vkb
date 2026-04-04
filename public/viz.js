import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

// ── State ─────────────────────────────────────────────────────────────────────
let nodes        = [];
let links        = [];
let allRelations = [];
let resolution   = 'chunk';   // 'entity' | 'chunk'
let _refreshPending = false;   // debounce WS-triggered refreshes

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

// ── Auto-orbit ────────────────────────────────────────────────────────────────
const orbit = {
  active:      true,   // whether auto-orbit is currently running
  resumeDelay: 3000,   // ms of inactivity before resuming
  speed:       0.000028, // radians per ms
  _timer:      null,
  _baseAngle:  null,   // angle captured when orbit last resumed
  _baseTime:   null,   // timestamp when orbit last resumed
  _center:     new THREE.Vector3(), // centroid of current graph
};

function orbitInterrupt() {
  if (orbit.active) {
    orbit.active = false;
    // Let OrbitControls take full control while user is interacting
    controls.autoRotate = false;
  }
  clearTimeout(orbit._timer);
  orbit._timer = setTimeout(() => {
    // Snapshot current azimuth so orbit resumes seamlessly from here
    const dx = camera.position.x - orbit._center.x;
    const dz = camera.position.z - orbit._center.z;
    orbit._baseAngle = Math.atan2(dx, dz);
    orbit._baseTime  = performance.now();
    orbit.active = true;
  }, orbit.resumeDelay);
}

function tickOrbit(t) {
  if (!orbit.active) return;
  if (orbit._baseAngle === null) {
    // First tick — initialise from current camera position
    const dx = camera.position.x - orbit._center.x;
    const dz = camera.position.z - orbit._center.z;
    orbit._baseAngle = Math.atan2(dx, dz);
    orbit._baseTime  = t;
  }
  const elapsed = t - orbit._baseTime;
  const angle   = orbit._baseAngle + elapsed * orbit.speed;
  const radius  = camera.position.distanceTo(orbit._center);
  // Keep current Y (elevation), orbit in XZ plane around center
  const cy = camera.position.y - orbit._center.y;
  const flatR = Math.sqrt(Math.max(0, radius * radius - cy * cy));
  camera.position.x = orbit._center.x + flatR * Math.sin(angle);
  camera.position.z = orbit._center.z + flatR * Math.cos(angle);
  camera.lookAt(orbit._center);
  controls.target.copy(orbit._center);
}

// Pause on any user pointer/touch/wheel/key activity
['pointerdown','pointermove','wheel','keydown','touchstart'].forEach(ev => {
  window.addEventListener(ev, orbitInterrupt, { passive: true });
});

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
window.addEventListener('resize', resize);

renderer.setAnimationLoop((t) => {
  tickOrbit(t);
  controls.update();
  tickStarscape(t);
  renderer.render(scene, camera);
});

// ── Starscape ─────────────────────────────────────────────────────────────────
// Two layers: a dense field of tiny static stars + a sparse layer of slow motes.
(function buildStarscape() {
  // --- star field: 2400 tiny points scattered in a large shell ---
  const starCount  = 2400;
  const starPos    = new Float32Array(starCount * 3);
  const starColors = new Float32Array(starCount * 3);
  const rng = (() => { let s = 0xbeef1234; return () => { s ^= s << 13; s ^= s >>> 17; s ^= s << 5; return (s >>> 0) / 0x100000000; }; })();
  for (let i = 0; i < starCount; i++) {
    // Uniform distribution on sphere shell between r=800 and r=1600
    const r     = 800 + rng() * 800;
    const theta = Math.acos(2 * rng() - 1);
    const phi   = rng() * Math.PI * 2;
    starPos[i*3]   = r * Math.sin(theta) * Math.cos(phi);
    starPos[i*3+1] = r * Math.sin(theta) * Math.sin(phi);
    starPos[i*3+2] = r * Math.cos(theta);
    // Warm-to-cool palette: mostly grey-white, occasional blue/amber tint
    const lum = 0.18 + rng() * 0.35;
    const tint = rng();
    starColors[i*3]   = lum + (tint < 0.15 ? 0.08 : tint > 0.85 ? -0.04 : 0);
    starColors[i*3+1] = lum + (tint < 0.15 ? 0.02 : 0);
    starColors[i*3+2] = lum + (tint < 0.15 ? -0.04 : tint > 0.85 ? 0.12 : 0);
  }
  const starGeo = new THREE.BufferGeometry();
  starGeo.setAttribute('position', new THREE.BufferAttribute(starPos, 3));
  starGeo.setAttribute('color',    new THREE.BufferAttribute(starColors, 3));
  const starMat = new THREE.PointsMaterial({ size: 0.9, vertexColors: true, transparent: true, opacity: 0.55, sizeAttenuation: true });
  const stars = new THREE.Points(starGeo, starMat);
  scene.add(stars);

  // --- mote layer: 120 larger, slower drifting points ---
  const moteCount = 120;
  const motePos   = new Float32Array(moteCount * 3);
  const motePhase = new Float32Array(moteCount * 3); // orbit params per mote
  for (let i = 0; i < moteCount; i++) {
    const r     = 300 + rng() * 700;
    const theta = Math.acos(2 * rng() - 1);
    const phi   = rng() * Math.PI * 2;
    motePos[i*3]   = r * Math.sin(theta) * Math.cos(phi);
    motePos[i*3+1] = r * Math.sin(theta) * Math.sin(phi);
    motePos[i*3+2] = r * Math.cos(theta);
    motePhase[i*3]   = rng() * Math.PI * 2; // phase offset
    motePhase[i*3+1] = (rng() - 0.5) * 0.00018; // orbit speed (rad/ms)
    motePhase[i*3+2] = r;                   // orbital radius
  }
  const moteGeo = new THREE.BufferGeometry();
  moteGeo.setAttribute('position', new THREE.BufferAttribute(motePos, 3));
  const moteMat = new THREE.PointsMaterial({ size: 2.4, color: 0x3a7a6a, transparent: true, opacity: 0.28, sizeAttenuation: true });
  const motes  = new THREE.Points(moteGeo, moteMat);
  scene.add(motes);

  // Slow whole-field drift: star field rotates almost imperceptibly
  window._starscape = { stars, motes, motePos, motePhase };
})();

function tickStarscape(t) {
  const sc = window._starscape;
  if (!sc) return;
  // Star field: extremely slow yaw (~2.4 deg/min)
  sc.stars.rotation.y = t * 0.000007;
  sc.stars.rotation.x = t * 0.000003;

  // Motes: each orbits its own axis with an individual phase & speed
  const pos   = sc.motePos;
  const phase = sc.motePhase;
  const buf   = sc.motes.geometry.attributes.position;
  for (let i = 0; i < pos.length / 3; i++) {
    const angle = phase[i*3] + t * phase[i*3+1];
    const r     = phase[i*3+2];
    // Orbit in the XZ plane with a fixed Y offset
    const baseY = pos[i*3+1];
    buf.array[i*3]   = r * Math.cos(angle);
    buf.array[i*3+1] = baseY + Math.sin(angle * 0.37 + phase[i*3]) * 18;
    buf.array[i*3+2] = r * Math.sin(angle);
  }
  buf.needsUpdate = true;
}

// ── Deterministic position from a string id ─────────────────────────────────
// Produces a stable float in [0, 1) from any string + integer seed.
function hashF(str, seed) {
  let h = seed ^ 0xdeadbeef;
  for (let i = 0; i < str.length; i++) {
    h = Math.imul(h ^ str.charCodeAt(i), 0x9e3779b9);
    h ^= h >>> 16;
  }
  return (h >>> 0) / 0x100000000;
}

// ── Node/Edge colors ──────────────────────────────────────────────────────────
const NODE_GEO  = new THREE.SphereGeometry(6, 16, 12);
const CHUNK_GEO = new THREE.SphereGeometry(3, 10, 8);
const NODE_MAT  = new THREE.MeshLambertMaterial();
let nodeMesh = null;
let edgeLine = null;

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

// ── Scene builder ─────────────────────────────────────────────────────────────
function buildScene() {
  if (nodeMesh) { scene.remove(nodeMesh); nodeMesh = null; }
  if (edgeLine) { scene.remove(edgeLine); edgeLine.geometry.dispose(); edgeLine.material.dispose(); edgeLine = null; }
  if (!nodes.length) return;

  // instanced spheres
  const geo = resolution === 'chunk' ? CHUNK_GEO : NODE_GEO;
  nodeMesh = new THREE.InstancedMesh(geo, NODE_MAT, nodes.length);
  nodes.forEach((n, i) => {
    _mat.setPosition(n.x, n.y, n.z);
    nodeMesh.setMatrixAt(i, _mat);
    nodeMesh.setColorAt(i, n._color ?? NODE_COLORS[n.status] ?? NODE_COLORS.pending);
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

  // Recompute orbit centroid from current node positions
  if (nodes.length) {
    let sx = 0, sy = 0, sz = 0;
    for (const n of nodes) { sx += n.x; sy += n.y; sz += n.z; }
    orbit._center.set(sx / nodes.length, sy / nodes.length, sz / nodes.length);
    // Reset orbit angle so it smoothly continues from the new center
    orbit._baseAngle = null;
  }
}

// ── Picking & interaction ─────────────────────────────────────────────────────
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

// ── showDetail ────────────────────────────────────────────────────────────────
function showDetail(n) {
  const { escHtml } = window.__vkb;
  const rels = links.filter(l => (l._src?.id === n.id) || (l._tgt?.id === n.id));
  const relBadges = rels.map(r =>
    '<span class="badge b-' +
    (r.origin?.includes('content') ? 'content' : r.origin === 'asserted' ? 'asserted' : 'semantic') +
    '">' + (r.origin ?? '') + ' ' + (r.confidence != null ? r.confidence.toFixed(2) : '') + '</span>'
  ).join('');
  if (resolution === 'chunk') {
    document.getElementById('detail').innerHTML =
      '<strong>chunk §' + escHtml(String((n.seq ?? 0) + 1)) + '</strong>' +
      '<span style="color:var(--hint);font-size:10px"> · entity ' + escHtml((n.entityId ?? '').slice(0, 8)) + '</span><br>' +
      '<span style="color:var(--hint);font-size:10px">' + n.id + '</span><br><br>' +
      escHtml(n.summary || n.label || '(no summary yet)') + '<br><br>' +
      '<div class="pill-row">' + relBadges + '</div>';
    return;
  }
  document.getElementById('detail').innerHTML =
    '<strong>' + escHtml(n.type) + '</strong><br>' +
    '<span style="color:var(--hint);font-size:10px">' + n.id + '</span><br><br>' +
    escHtml(n.summary || n.label || '') + '<br><br>' +
    '<div class="pill-row">' + relBadges + '</div>';
}

// ── WS status ─────────────────────────────────────────────────────────────────
function setWsStatus(live) {
  const el = document.getElementById('ws-status');
  if (!el) return;
  el.textContent = live ? 'live' : 'reconnecting…';
  el.className   = 'stat ' + (live ? 'ws-live' : 'ws-off');
}

// ── Events panel ──────────────────────────────────────────────────────────────
function pushEvent(ev) {
  const { escHtml } = window.__vkb;
  const el = document.getElementById('events');
  if (!el) return;
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
  if (!el) return;
  const cutoff = Date.now() - 5 * 60 * 1000;
  Array.from(el.children).forEach(c => { if (+(c.dataset.evTs ?? 0) < cutoff) c.remove(); });
}

// ── Loading overlay ─────────────────────────────────────────────────────────
const _projLoading = document.getElementById('proj-loading');
function setLoading(on) {
  _projLoading?.classList.toggle('visible', on);
}

// ── refreshChunks ────────────────────────────────────────────────────────────
async function refreshChunks() {
  setLoading(true);
  try {
    const [statusRes, projRes, relationsRes, jobsRes] = await Promise.all([
      fetch('/status'),
      fetch('/chunks/projection'),
      fetch('/relations?source_kind=chunk&limit=5000'),
      fetch('/jobs?limit=30'),
    ]);
    const status        = await statusRes.json();
    const projData      = await projRes.json();
    const relationsData = await relationsRes.json();
    const jobs          = await jobsRes.json();

    document.getElementById('s-entities').textContent  = status.data?.entity_count   ?? '?';
    document.getElementById('s-chunks').textContent    = status.data?.chunk_count    ?? '?';
    document.getElementById('s-relations').textContent = status.data?.relation_count ?? '?';
    document.getElementById('s-queue').textContent     = status.data?.queue_depth    ?? '?';
    renderJobs(jobs.data?.jobs ?? []);

    const points  = projData.data?.points ?? [];
    const existPos = new Map(nodes.map(n => [n.id, n]));

    nodes = points.map(p => {
      const ex    = existPos.get(p.id);
      const meta  = p.entity_meta ?? {};
      const entityLabel = meta.title || meta.filename || p.entity_ref || p.entity_type || p.entity_id.slice(0, 8);
      const lbl   = `${entityLabel} §${(p.seq ?? 0) + 1}`;
      const hue   = parseInt(p.entity_id.replace(/-/g, '').slice(0, 4), 16) / 65535;
      const color = new THREE.Color().setHSL(hue, 0.55, 0.45);
      // Projection coords are authoritative — always update x/y/z from server
      const pos = { x: p.x, y: p.y, z: p.z };
      if (ex) return Object.assign(ex, { entityId: p.entity_id, seq: p.seq, label: lbl, summary: p.summary ?? '', _color: color, ...pos });
      return { id: p.id, entityId: p.entity_id, seq: p.seq, label: lbl, summary: p.summary ?? '', _color: color, ...pos };
    });

    const nodeIds = new Set(nodes.map(n => n.id));
    const nodeIdx = new Map(nodes.map((n, i) => [n.id, i]));
    allRelations  = relationsData.data?.relations ?? [];
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
  } catch (err) {
    console.warn('viz chunk refresh error:', err);
  } finally {
    setLoading(false);
  }
}

// ── refresh ───────────────────────────────────────────────────────────────────
async function refresh() {
  if (resolution === 'chunk') { await refreshChunks(); return; }
  setLoading(true);
  try {
    const [statusRes, projRes, relationsRes, jobsRes] = await Promise.all([
      fetch('/status'),
      fetch('/entities/projection'),
      fetch('/relations?limit=500&min_confidence=0.4'),
      fetch('/jobs?limit=30'),
    ]);
    const status    = await statusRes.json();
    const projData  = await projRes.json();
    const relations = await relationsRes.json();
    const jobs      = await jobsRes.json();

    document.getElementById('s-entities').textContent  = status.data?.entity_count   ?? '?';
    document.getElementById('s-chunks').textContent    = status.data?.chunk_count    ?? '?';
    document.getElementById('s-relations').textContent = status.data?.relation_count ?? '?';
    document.getElementById('s-queue').textContent     = status.data?.queue_depth    ?? '?';

    renderJobs(jobs.data?.jobs ?? []);

    const points   = projData.data?.points ?? [];
    const existPos = new Map(nodes.map(n => [n.id, n]));
    nodes = points.map(p => {
      const ex  = existPos.get(p.id);
      const lbl = (p.summary || p.type || p.id).slice(0, 60);
      const pos = { x: p.x, y: p.y, z: p.z };
      return ex
        ? Object.assign(ex, { type: p.type, status: p.status, label: lbl, summary: p.summary ?? '', ...pos })
        : { id: p.id, type: p.type, status: p.status, label: lbl, summary: p.summary ?? '', ...pos };
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
  } catch (err) {
    console.warn('viz refresh error:', err);
  } finally {
    setLoading(false);
  }
}

// ── renderJobs ────────────────────────────────────────────────────────────────
const ACTIVE_STAGES = new Set(['fetching','chunking','embedding','sectioning','summarising','extracting']);
function stageClass(s) {
  if (s === 'done')   return 's-done';
  if (s === 'error')  return 's-error';
  if (s === 'queued') return 's-queued';
  return ACTIVE_STAGES.has(s) ? 's-active' : 's-queued';
}

function renderJobs(jobs) {
  const { escHtml, fmtAge } = window.__vkb;
  const el = document.getElementById('jobs-list');
  if (!el) return;
  const cutoff = Date.now() - 5 * 60 * 1000;
  const visible = jobs.filter(j => j.stage !== 'done' || !j.completed_at || +new Date(j.completed_at) > cutoff);
  if (!visible.length) { el.innerHTML = '<div class="detail" style="color:var(--hint)">No recent jobs</div>'; return; }
  el.innerHTML = visible.slice(0, 20).map(j => {
    const p = j.progress ?? {};
    let prog = '';
    if (p.chunks_total > 0) {
      prog = (p.chunks_done ?? 0) + '/' + p.chunks_total + 'c';
      if (p.sections_done   > 0) prog += ' ' + p.sections_done   + 's';
      if (p.relations_added > 0) prog += ' ' + p.relations_added + 'r';
    }
    return '<div class="job-row">' +
      '<span class="job-kind">'  + escHtml(j.kind)  + '</span>' +
      '<span class="job-stage '  + stageClass(j.stage) + '">' + escHtml(j.stage) + '</span>' +
      (prog ? '<span class="job-progress">' + prog + '</span>' : '') +
      '<span class="job-age">'   + fmtAge(new Date(j.created_at)) + '</span>' +
      '</div>';
  }).join('');
}

// ── renderHistogram ───────────────────────────────────────────────────────────
function renderHistogram(rels) {
  const svg  = d3.select('#histogram');
  const rect = svg.node()?.getBoundingClientRect();
  if (!rect) return;
  const W = rect.width || 270, H = 80;
  svg.attr('viewBox', '0 0 ' + W + ' ' + H).selectAll('*').remove();
  if (!rels.length) return;
  const bins   = d3.bin().domain([0,1]).thresholds(10)(rels.map(r => r.confidence));
  const xScale = d3.scaleLinear().domain([0,1]).range([0, W]);
  const yScale = d3.scaleLinear().domain([0, d3.max(bins, b => b.length) || 1]).range([H-4, 4]);
  svg.selectAll('rect').data(bins).join('rect')
    .attr('x',      b => xScale(b.x0) + 1)
    .attr('y',      b => yScale(b.length))
    .attr('width',  b => Math.max(0, xScale(b.x1) - xScale(b.x0) - 2))
    .attr('height', b => H - 4 - yScale(b.length))
    .attr('fill',   '#534AB7')
    .attr('opacity', 0.7);
}

// ── Bus subscriptions ─────────────────────────────────────────────────────────
// Debounce helper: coalesces rapid successive WS events into a single refresh.
function scheduleRefresh(delayMs = 800) {
  if (_refreshPending) return;
  _refreshPending = true;
  setTimeout(() => { _refreshPending = false; refresh(); }, delayMs);
}

function subscribeTobus() {
  const bus = window.__vkb.bus;
  bus.subscribe('ws_open',  () => { setWsStatus(true);  pushEvent({ type: 'ws_open' });  scheduleRefresh(200); });
  bus.subscribe('ws_close', () => { setWsStatus(false); pushEvent({ type: 'ws_close' }); });
  // Projection-relevant events: re-render the graph
  const REFRESH_TYPES = ['complete', 'stage_change', 'error', 'worker_crash', 'retune_scheduled'];
  REFRESH_TYPES.forEach(type => bus.subscribe(type, () => scheduleRefresh(800)));
  // Event log only (no full refresh needed)
  ['complete','stage_change','error','worker_crash','retune_scheduled','progress'].forEach(type => {
    bus.subscribe(type, ev => pushEvent(ev));
  });
}

// ── Resolution toggle ─────────────────────────────────────────────────────────
function setResolution(res) {
  resolution = res;
  nodes = []; links = [];  // reset positions so the new graph lays out fresh
  document.querySelectorAll('.res-btn').forEach(b => b.classList.toggle('active', b.dataset.res === res));
  document.getElementById('detail').textContent = 'Click a node to inspect';
  refresh();
}

// ── Public API ────────────────────────────────────────────────────────────────
window.__vkb_viz = { init };

function init() {
  document.querySelectorAll('.res-btn').forEach(btn => {
    btn.addEventListener('click', () => setResolution(btn.dataset.res));
  });
  resize();
  refresh();
  // Fallback poll: only fires when WS is disconnected (every 60 s).
  // When the socket is live, WS events drive all updates instead.
  setInterval(() => {
    const wsLive = document.getElementById('ws-status')?.classList.contains('ws-live');
    if (!wsLive) refresh();
  }, 60_000);
  setInterval(() => pruneEvents(document.getElementById('events')), 60_000);
  subscribeTobus();
}
