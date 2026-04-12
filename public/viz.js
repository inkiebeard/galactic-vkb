import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';

// ── State ─────────────────────────────────────────────────────────────────────
let nodes        = [];
let links        = [];
let allRelations = [];
let resolution   = 'chunk';   // 'entity' | 'chunk'

// ── Pagination / version tracking ────────────────────────────────────────────
const PROJ_PAGE_SIZE = 1000;                    // nodes per projection page
let projVersion  = { chunk: -1, entity: -1 }; // last known server version per resolution
let _loadId      = 0;                          // increment to cancel stale in-flight loads
let _refreshTimer = null;                      // debounce timer handle

// ── Selection state ──────────────────────────────────────────────────────────
let _selectedNode  = null;   // currently selected/highlighted node
let _selEdgeLine   = null;   // solid edge LineSegments for selected node's relations
let _selEdgeDots   = null;   // animated directional InstancedMesh dots
let _selEdgeLinks  = [];     // links connected to the selected node
let _selDotPhases  = [];     // per-edge dot phase offsets [0,1)

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
  tickSelectionAnim(t);
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
// ── Type-based colour for entity nodes ─────────────────────────────────────────────
// Each distinct entity type gets a consistent hue derived from the type string.
const _typeColorCache = new Map();
function typeColor(type) {
  if (_typeColorCache.has(type)) return _typeColorCache.get(type);
  const hue = hashF(type || 'unknown', 0x7c65);
  const col = new THREE.Color().setHSL(hue, 0.62, 0.42);
  _typeColorCache.set(type, col);
  return col;
}
// ── Node/Edge colors ──────────────────────────────────────────────────────────
const NODE_GEO  = new THREE.SphereGeometry(6, 16, 12);
const CHUNK_GEO = new THREE.SphereGeometry(3, 10, 8);
const NODE_MAT  = new THREE.MeshLambertMaterial();
const DOT_GEO   = new THREE.SphereGeometry(0.4, 6, 4);  // directional flow indicator (1/5 scale)
let nodeMesh = null;
let edgeLine = null;

const STATUS_COLORS = {
  ready:      new THREE.Color(0x0F6E56),
  processing: new THREE.Color(0x7A6B10),
  error:      new THREE.Color(0x712B13),
  pending:    new THREE.Color(0x444441),
};
const EDGE_COLORS = {
  content_heuristic: new THREE.Color(0x0e7a60),
  content_llm:       new THREE.Color(0x0e7a60),
  semantic:          new THREE.Color(0x7b6de0),
  asserted:          new THREE.Color(0xd95f3b),
  same_entity:       new THREE.Color(0x3a6a8a),
};
const _mat    = new THREE.Matrix4();
const _dotMat = new THREE.Matrix4();  // separate matrix for animated dot placement

// ── Scene builder ─────────────────────────────────────────────────────────────
function buildScene() {
  clearSelectionOverlay();
  if (nodeMesh) { scene.remove(nodeMesh); nodeMesh = null; }
  if (edgeLine) { scene.remove(edgeLine); edgeLine.geometry.dispose(); edgeLine.material.dispose(); edgeLine = null; }
  if (!nodes.length) return;

  // instanced spheres
  const geo = resolution === 'chunk' ? CHUNK_GEO : NODE_GEO;
  nodeMesh = new THREE.InstancedMesh(geo, NODE_MAT, nodes.length);
  nodes.forEach((n, i) => {
    _mat.setPosition(n.x, n.y, n.z);
    nodeMesh.setMatrixAt(i, _mat);
    nodeMesh.setColorAt(i, n._color ?? STATUS_COLORS[n.status] ?? STATUS_COLORS.pending);
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
    // Ghost opacity — selected node's relations are drawn solid in the overlay layer
    new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, opacity: 0.1 }),
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

  // Restore selection overlay on scene rebuild (e.g. after pagination or UMAP refresh)
  if (_selectedNode) buildSelectionOverlay(_selectedNode);
  updateNodeDim();
}

// ── Selection overlay ────────────────────────────────────────────────────────
// When the user clicks a node, its relations are highlighted with solid colored
// lines and animated dots that travel src→tgt to show directionality.

function clearSelectionOverlay() {
  if (_selEdgeLine) {
    scene.remove(_selEdgeLine);
    _selEdgeLine.geometry.dispose();
    _selEdgeLine.material.dispose();
    _selEdgeLine = null;
  }
  if (_selEdgeDots) {
    scene.remove(_selEdgeDots);
    _selEdgeDots.geometry.dispose();
    _selEdgeDots.material.dispose();
    _selEdgeDots = null;
  }
  _selEdgeLinks = [];
  _selDotPhases = [];
}

function buildSelectionOverlay(node) {
  clearSelectionOverlay();
  // Exclude synthetic same_entity links — they're grouping hints, not real relations
  _selEdgeLinks = links.filter(l =>
    (l._src?.id === node.id || l._tgt?.id === node.id) && l.origin !== 'same_entity',
  );
  if (!_selEdgeLinks.length) return;

  // Solid highlighted edge lines for this node's relations
  const ePos = new Float32Array(_selEdgeLinks.length * 6);
  const eCol = new Float32Array(_selEdgeLinks.length * 6);
  _selEdgeLinks.forEach((l, i) => {
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
  _selEdgeLine = new THREE.LineSegments(eGeo, new THREE.LineBasicMaterial({ vertexColors: true }));
  scene.add(_selEdgeLine);

  // One animated dot per relation that travels _src → _tgt to indicate direction
  _selDotPhases = _selEdgeLinks.map((_, i) => i / _selEdgeLinks.length); // stagger start phases
  _selEdgeDots  = new THREE.InstancedMesh(DOT_GEO, new THREE.MeshBasicMaterial(), _selEdgeLinks.length);
  _selEdgeLinks.forEach((l, i) => {
    const ph = _selDotPhases[i];
    _dotMat.setPosition(
      l._src.x + (l._tgt.x - l._src.x) * ph,
      l._src.y + (l._tgt.y - l._src.y) * ph,
      l._src.z + (l._tgt.z - l._src.z) * ph,
    );
    _selEdgeDots.setMatrixAt(i, _dotMat);
    _selEdgeDots.setColorAt(i, EDGE_COLORS[l.origin] ?? EDGE_COLORS.content_heuristic);
  });
  _selEdgeDots.instanceMatrix.needsUpdate = true;
  _selEdgeDots.instanceColor.needsUpdate  = true;
  scene.add(_selEdgeDots);
}

// Advance each dot along its edge; called every frame from the render loop.
function tickSelectionAnim(t) {
  if (!_selEdgeDots || !_selEdgeLinks.length) return;
  const speed = 0.00025; // ~4 s per full src→tgt traversal
  for (let i = 0; i < _selEdgeLinks.length; i++) {
    const phase = (_selDotPhases[i] + t * speed) % 1;
    const l = _selEdgeLinks[i];
    _dotMat.setPosition(
      l._src.x + (l._tgt.x - l._src.x) * phase,
      l._src.y + (l._tgt.y - l._src.y) * phase,
      l._src.z + (l._tgt.z - l._src.z) * phase,
    );
    _selEdgeDots.setMatrixAt(i, _dotMat);
  }
  _selEdgeDots.instanceMatrix.needsUpdate = true;
}

// ── Node dimming ─────────────────────────────────────────────────────────────
// BFS up to `hops` edges from `node`, following all links bidirectionally.
// Returns a Set of node IDs in the neighbourhood (including the start node).
function getNeighborhood(node, hops) {
  const visited = new Set([node.id]);
  let frontier  = new Set([node.id]);
  for (let h = 0; h < hops; h++) {
    if (!frontier.size) break;
    const next = new Set();
    for (const l of links) {
      const s = l._src?.id, t = l._tgt?.id;
      if (!s || !t) continue;
      if (frontier.has(s) && !visited.has(t)) { visited.add(t); next.add(t); }
      if (frontier.has(t) && !visited.has(s)) { visited.add(s); next.add(s); }
    }
    frontier = next;
  }
  return visited;
}

// Apply per-instance brightness to the node mesh.
// • No selection  → all nodes at 55% (slightly dimmed / "transparent")
// • Selection     → 3-hop neighbours at full brightness, rest at 10%
const _dimColor = new THREE.Color();
function updateNodeDim() {
  if (!nodeMesh) return;
  const nbhd = _selectedNode ? getNeighborhood(_selectedNode, 3) : null;
  nodes.forEach((n, i) => {
    _dimColor.copy(n._color ?? STATUS_COLORS[n.status] ?? STATUS_COLORS.pending);
    if (nbhd) {
      if (!nbhd.has(n.id)) _dimColor.multiplyScalar(0.10);
    } else {
      _dimColor.multiplyScalar(0.55);
    }
    nodeMesh.setColorAt(i, _dimColor);
  });
  nodeMesh.instanceColor.needsUpdate = true;
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

// ── openSidebar ───────────────────────────────────────────────────────────────
function openSidebar() {
  document.querySelector('.sidebar')?.classList.remove('collapsed');
}

// ── showDetail ────────────────────────────────────────────────────────────────
function showDetail(n) {
  const { escHtml } = window.__vkb;
  const rels = links.filter(l => (l._src?.id === n.id) || (l._tgt?.id === n.id));
  _selectedNode = n;
  buildSelectionOverlay(n);
  updateNodeDim();
  const relBadges = rels
    .filter(r => r.origin !== 'same_entity')  // omit synthetic intra-doc links from badge list
    .map(r => {
      const cls = r.origin?.includes('content') ? 'content'
        : r.origin === 'semantic' ? 'semantic'
        : r.origin === 'asserted' ? 'asserted'
        : 'content';
      return '<span class="badge b-' + cls + '">' + escHtml(r.origin ?? '') + ' ' +
        (r.confidence != null ? r.confidence.toFixed(2) : '') + '</span>';
    }).join('');

  const el = document.getElementById('detail');

  function tagPills(meta) {
    const tags = [].concat(meta.tag ?? [], meta.tags ?? []).filter(t => t && String(t).trim());
    return tags.length
      ? '<div class="tag-row">' + tags.map(t => '<span class="tag-pill">' + escHtml(String(t)) + '</span>').join('') + '</div>'
      : '';
  }

  if (resolution === 'chunk') {
    const meta = n.entityMeta ?? {};
    const entityLabel = meta.title || meta.filename || n.entityRef || n.entityType || (n.entityId ?? '').slice(0, 8);
    el.innerHTML =
      '<div class="det-header"><strong>chunk §' + escHtml(String((n.seq ?? 0) + 1)) + '</strong>' +
      '<span class="det-hint"> · ' + escHtml(entityLabel) + '</span></div>' +
      '<div class="det-id">' + escHtml(n.id) + '</div>' +
      (n.entityRef ? '<div class="det-ref" title="' + escHtml(n.entityRef) + '">' + escHtml(n.entityRef) + '</div>' : '') +
      tagPills(meta) +
      '<div class="det-divider"></div>' +
      '<div class="det-summary">' + escHtml(n.summary || '(no summary yet)') + '</div>' +
      (relBadges ? '<div class="pill-row">' + relBadges + '</div>' : '');
    openSidebar();
    return;
  }

  // Entity resolution
  const meta = n.meta ?? {};
  const metaKeys = Object.keys(meta).filter(k => k !== 'tag' && k !== 'tags');
  const metaHtml = metaKeys.map(k => {
    const v = meta[k];
    const vStr = Array.isArray(v) ? v.join(', ') : String(v ?? '');
    return '<div class="kv-row"><span class="kv-key">' + escHtml(k) + '</span><span class="kv-val">' + escHtml(vStr) + '</span></div>';
  }).join('');
  const typeHex = n._color ? '#' + n._color.getHexString() : 'var(--teal)';
  el.innerHTML =
    '<div class="det-type-row">' +
      '<span class="det-type-dot" style="background:' + typeHex + '"></span>' +
      '<strong>' + escHtml(n.type) + '</strong>' +
      '<span class="det-status-chip st-' + escHtml(n.status ?? 'pending') + '">' + escHtml(n.status ?? 'pending') + '</span>' +
    '</div>' +
    '<div class="det-id">' + escHtml(n.id) + '</div>' +
    (n.ref ? '<div class="det-ref" title="' + escHtml(n.ref) + '">' + escHtml(n.ref) + '</div>' : '') +
    tagPills(meta) +
    metaHtml +
    '<div class="det-divider"></div>' +
    '<div class="det-summary">' + escHtml(n.summary || '(no summary yet)') + '</div>' +
    (relBadges ? '<div class="pill-row">' + relBadges + '</div>' : '');
  openSidebar();
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
  // Don't block the UI with the spinner while the ingest view is active —
  // the graph is out of view so the projection update is invisible anyway.
  if (on && document.body.classList.contains('view-ingest')) return;
  _projLoading?.classList.toggle('visible', on);
}

// ── refreshChunks ────────────────────────────────────────────────────────────
// Loads the chunk projection incrementally, one PROJ_PAGE_SIZE page at a time.
// Each page is rendered immediately so nodes appear progressively. If a newer
// load starts (e.g. from a projection_version event), stale in-flight loads
// are cancelled via the _loadId token. The server version is compared before
// fetching all pages — if unchanged, only the status bar is updated.
async function refreshChunks() {
  const loadId = ++_loadId;
  setLoading(true);
  try {
    // Fetch status, jobs, and the first projection page simultaneously.
    const [statusRes, firstPageRes, jobsRes] = await Promise.all([
      fetch('/status'),
      fetch(`/chunks/projection?offset=0&limit=${PROJ_PAGE_SIZE}`),
      fetch('/jobs?limit=30'),
    ]);
    if (loadId !== _loadId) return; // superseded by a newer load

    const [status, firstPage, jobs] = await Promise.all([
      statusRes.json(), firstPageRes.json(), jobsRes.json(),
    ]);

    document.getElementById('s-entities').textContent  = status.data?.entity_count   ?? '?';
    document.getElementById('s-chunks').textContent    = status.data?.chunk_count    ?? '?';
    document.getElementById('s-relations').textContent = status.data?.relation_count ?? '?';
    document.getElementById('s-queue').textContent     = status.data?.queue_depth    ?? '?';
    renderJobs(jobs.data?.jobs ?? []);

    if (!firstPage.ok) return;
    const serverVersion = firstPage.data.version;
    const total         = firstPage.data.total;

    // Skip full projection reload if we already have this version and a complete node set.
    if (serverVersion === projVersion.chunk && nodes.length === total) return;
    projVersion.chunk = serverVersion;

    // Helper: convert a server point to a viz node, reusing existing position
    // objects where possible so references in any open detail panel stay valid.
    const existPos = new Map(nodes.map(n => [n.id, n]));
    function chunkPointToNode(p) {
      const ex    = existPos.get(p.id);
      const meta  = p.entity_meta ?? {};
      const entityLabel = meta.title || meta.filename || p.entity_ref || p.entity_type || p.entity_id.slice(0, 8);
      const lbl   = `${entityLabel} \u00a7${(p.seq ?? 0) + 1}`;
      const hue   = parseInt(p.entity_id.replace(/-/g, '').slice(0, 4), 16) / 65535;
      const color = new THREE.Color().setHSL(hue, 0.55, 0.45);
      const fields = {
        entityId: p.entity_id, entityType: p.entity_type, entityRef: p.entity_ref ?? null,
        entityMeta: meta, seq: p.seq, label: lbl, summary: p.summary ?? '', _color: color,
        x: p.x, y: p.y, z: p.z,
      };
      return ex ? Object.assign(ex, fields) : { id: p.id, ...fields };
    }

    // Render first page immediately so the user sees nodes appearing.
    nodes = firstPage.data.points.map(chunkPointToNode);
    links = [];
    buildScene();

    // Fetch remaining pages sequentially, rendering after each.
    if (firstPage.data.has_more) {
      let offset = PROJ_PAGE_SIZE;
      while (offset < total) {
        if (loadId !== _loadId) return;
        const res  = await fetch(`/chunks/projection?offset=${offset}&limit=${PROJ_PAGE_SIZE}`);
        const page = await res.json();
        if (!page.ok) break;
        for (const p of page.data.points) nodes.push(chunkPointToNode(p));
        buildScene();
        if (!page.data.has_more) break;
        offset += PROJ_PAGE_SIZE;
      }
    }

    if (loadId !== _loadId) return;

    // All nodes are loaded — now fetch relations and build the link graph.
    const relRes  = await fetch('/relations?source_kind=chunk&limit=20000');
    const relData = await relRes.json();
    if (loadId !== _loadId) return;

    const nodeIds = new Set(nodes.map(n => n.id));
    const nodeIdx = new Map(nodes.map((n, i) => [n.id, i]));
    allRelations  = relData.data?.relations ?? [];
    links = allRelations
      .filter(r => nodeIds.has(r.source_id) && nodeIds.has(r.target_id))
      .map(r => ({
        source: r.source_id, target: r.target_id,
        origin: r.origin, weight: r.weight, confidence: r.confidence,
        _src: nodes[nodeIdx.get(r.source_id)],
        _tgt: nodes[nodeIdx.get(r.target_id)],
      }));

    // ── Synthetic intra-entity "same document" links ──────────────────────
    const entityChunks = new Map();
    for (const n of nodes) {
      const eid = n.entityId;
      if (!eid) continue;
      if (!entityChunks.has(eid)) entityChunks.set(eid, []);
      entityChunks.get(eid).push(n);
    }
    for (const chunks of entityChunks.values()) {
      if (chunks.length < 2) continue;
      chunks.sort((a, b) => (a.seq ?? 0) - (b.seq ?? 0));
      const hub = chunks[0];
      for (let i = 1; i < chunks.length; i++) {
        links.push({
          source: hub.id, target: chunks[i].id,
          origin: 'same_entity', weight: 0.5, confidence: 1.0,
          _src: hub, _tgt: chunks[i],
        });
      }
    }

    buildScene();
    renderHistogram(allRelations);
  } catch (err) {
    console.warn('viz chunk refresh error:', err);
  } finally {
    if (loadId === _loadId) setLoading(false);
  }
}

// ── refresh (entity mode) ─────────────────────────────────────────────────────
// Same incremental paging strategy as refreshChunks. Since there are far fewer
// entities than chunks, relations are fetched in parallel with the first page.
async function refresh() {
  if (resolution === 'chunk') { await refreshChunks(); return; }
  const loadId = ++_loadId;
  setLoading(true);
  try {
    // Relations are bounded (~2000) so fetch in parallel with first page.
    const [statusRes, firstPageRes, relationsRes, jobsRes] = await Promise.all([
      fetch('/status'),
      fetch(`/entities/projection?offset=0&limit=${PROJ_PAGE_SIZE}`),
      fetch('/relations?source_kind=entity&limit=2000'),
      fetch('/jobs?limit=30'),
    ]);
    if (loadId !== _loadId) return;

    const [status, firstPage, relations, jobs] = await Promise.all([
      statusRes.json(), firstPageRes.json(), relationsRes.json(), jobsRes.json(),
    ]);

    document.getElementById('s-entities').textContent  = status.data?.entity_count   ?? '?';
    document.getElementById('s-chunks').textContent    = status.data?.chunk_count    ?? '?';
    document.getElementById('s-relations').textContent = status.data?.relation_count ?? '?';
    document.getElementById('s-queue').textContent     = status.data?.queue_depth    ?? '?';
    renderJobs(jobs.data?.jobs ?? []);

    if (!firstPage.ok) return;
    const serverVersion = firstPage.data.version;
    const total         = firstPage.data.total;

    if (serverVersion === projVersion.entity && nodes.length === total) return;
    projVersion.entity = serverVersion;

    const existPos = new Map(nodes.map(n => [n.id, n]));
    function entityPointToNode(p) {
      const ex   = existPos.get(p.id);
      const meta = p.meta ?? {};
      const lbl  = meta.title || meta.filename
        || (p.ref ? p.ref.split('/').pop()?.split('?')[0] : null)
        || (p.summary || p.type || p.id).slice(0, 60);
      const color = typeColor(p.type);
      const fields = {
        type: p.type, status: p.status, ref: p.ref ?? null,
        meta, label: lbl, summary: p.summary ?? '', _color: color,
        x: p.x, y: p.y, z: p.z,
      };
      return ex ? Object.assign(ex, fields) : { id: p.id, ...fields };
    }

    nodes = firstPage.data.points.map(entityPointToNode);
    links = [];
    buildScene();

    if (firstPage.data.has_more) {
      let offset = PROJ_PAGE_SIZE;
      while (offset < total) {
        if (loadId !== _loadId) return;
        const res  = await fetch(`/entities/projection?offset=${offset}&limit=${PROJ_PAGE_SIZE}`);
        const page = await res.json();
        if (!page.ok) break;
        for (const p of page.data.points) nodes.push(entityPointToNode(p));
        buildScene();
        if (!page.data.has_more) break;
        offset += PROJ_PAGE_SIZE;
      }
    }

    if (loadId !== _loadId) return;

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
    if (loadId === _loadId) setLoading(false);
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
// Debounce helper: always resets the timer so a short delay (e.g. projection_version
// at 100 ms) can preempt a longer pending delay (e.g. complete at 800 ms).
function scheduleRefresh(delayMs = 800) {
  clearTimeout(_refreshTimer);
  _refreshTimer = setTimeout(() => { _refreshTimer = null; refresh(); }, delayMs);
}

function subscribeTobus() {
  const bus = window.__vkb.bus;
  bus.subscribe('ws_open',  () => { setWsStatus(true);  pushEvent({ type: 'ws_open' });  refresh(); });
  bus.subscribe('ws_close', () => { setWsStatus(false); pushEvent({ type: 'ws_close' }); });
  // Server finished recomputing the projection — do a version-checked incremental load.
  // This fires 10-30 s after 'complete' once UMAP is done, and preempts any pending timer.
  bus.subscribe('projection_version', ev => {
    if (ev.resolution === resolution) scheduleRefresh(100);
  });
  // Non-projection events: update status bar / jobs list (refresh is version-aware, returns fast).
  const STATUS_TYPES = ['complete', 'stage_change', 'error', 'worker_crash', 'retune_scheduled'];
  STATUS_TYPES.forEach(type => bus.subscribe(type, () => scheduleRefresh(800)));
  // Event log only
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

// ── Viz lookahead search ──────────────────────────────────────────────────────
function nodeMatchesQuery(n, q) {
  const lq = q.toLowerCase();
  if (n.label?.toLowerCase().includes(lq)) return true;
  if (n.type?.toLowerCase().includes(lq))  return true;
  if (n.entityType?.toLowerCase().includes(lq)) return true;
  // Check tags in meta/entityMeta
  const meta = n.meta ?? n.entityMeta ?? {};
  const tags = [].concat(meta.tag ?? [], meta.tags ?? []).filter(Boolean);
  if (tags.some(t => String(t).toLowerCase().includes(lq))) return true;
  // Fuzzy summary keyword
  if (n.summary?.toLowerCase().includes(lq)) return true;
  return false;
}

function nodeSearchTags(n) {
  const meta = n.meta ?? n.entityMeta ?? {};
  return [].concat(meta.tag ?? [], meta.tags ?? []).filter(t => t && String(t).trim());
}

function initVizSearch() {
  const input  = document.getElementById('viz-search');
  const list   = document.getElementById('viz-search-list');
  if (!input || !list) return;

  let curIdx = -1;
  let lastQ  = '';

  function closeList() {
    list.classList.remove('open');
    input.setAttribute('aria-expanded', 'false');
    curIdx = -1;
  }

  function openList(items) {
    const { escHtml } = window.__vkb;
    list.innerHTML = '';
    if (!items.length) {
      list.innerHTML = '<div class="search-no-results">No matches</div>';
      list.classList.add('open');
      input.setAttribute('aria-expanded', 'true');
      return;
    }
    items.forEach((n, i) => {
      const div = document.createElement('div');
      div.className = 'search-opt';
      div.setAttribute('role', 'option');
      div.setAttribute('aria-selected', 'false');
      const typeLabel = escHtml(n.type ?? n.entityType ?? '');
      const tags = nodeSearchTags(n);
      const tagHtml = tags.length
        ? '<div class="search-opt-tags">' + tags.map(t => `<span class="search-opt-tag">${escHtml(String(t))}</span>`).join('') + '</div>'
        : '';
      div.innerHTML =
        `<div class="search-opt-title">${escHtml(n.label ?? n.id)}</div>` +
        `<div class="search-opt-sub">${typeLabel}${typeLabel && n.summary ? ' · ' : ''}${escHtml((n.summary ?? '').slice(0, 80))}</div>` +
        tagHtml;
      div.addEventListener('mousedown', e => {
        e.preventDefault();
        selectNode(n);
        input.value = '';
        closeList();
      });
      list.appendChild(div);
    });
    list.classList.add('open');
    input.setAttribute('aria-expanded', 'true');
    curIdx = -1;
  }

  function highlightItem(idx) {
    const opts = list.querySelectorAll('.search-opt');
    opts.forEach((el, i) => el.setAttribute('aria-selected', String(i === idx)));
    curIdx = idx;
  }

  function selectNode(n) {
    showDetail(n);
    openSidebar();
    // Fly camera to the node
    orbitInterrupt();
    const target = new THREE.Vector3(n.x, n.y, n.z);
    const dir = camera.position.clone().sub(target).normalize();
    const dist = 120;
    camera.position.copy(target.clone().add(dir.multiplyScalar(dist)));
    controls.target.copy(target);
    controls.update();
    orbit._center.copy(target);
    orbit._baseAngle = null;
  }

  input.addEventListener('input', () => {
    const q = input.value.trim();
    lastQ = q;
    if (!q) { closeList(); return; }
    const matches = nodes.filter(n => nodeMatchesQuery(n, q)).slice(0, 12);
    openList(matches);
  });

  input.addEventListener('keydown', e => {
    const opts = list.querySelectorAll('.search-opt');
    if (e.key === 'ArrowDown') {
      e.preventDefault();
      highlightItem(Math.min(curIdx + 1, opts.length - 1));
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      highlightItem(Math.max(curIdx - 1, 0));
    } else if (e.key === 'Enter') {
      e.preventDefault();
      if (curIdx >= 0 && opts[curIdx]) {
        opts[curIdx].dispatchEvent(new MouseEvent('mousedown'));
      }
    } else if (e.key === 'Escape') {
      closeList();
      input.blur();
    }
  });

  document.addEventListener('click', e => {
    if (!input.contains(e.target) && !list.contains(e.target)) closeList();
  });
}

// ── Public API ────────────────────────────────────────────────────────────────
window.__vkb_viz = { init };

function init() {
  document.querySelectorAll('.res-btn').forEach(btn => {
    btn.addEventListener('click', () => setResolution(btn.dataset.res));
  });
  initVizSearch();
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
