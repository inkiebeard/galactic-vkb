// galactic-vkb · app.js
// Orchestrator SPA: WebSocket, event bus, router, nav, alerts, utilities
import './ingest.js';
import './viz.js';

// ─── Event bus ────────────────────────────────────────────────────────────────
const subscribers = new Map();

function subscribe(type, fn) {
  if (!subscribers.has(type)) subscribers.set(type, new Set());
  subscribers.get(type).add(fn);
}

function publish(type, payload) {
  subscribers.get(type)?.forEach(fn => fn(payload));
  if (type !== '*') {
    subscribers.get('*')?.forEach(fn => fn({ type, ...payload }));
  }
}

// ─── Router ───────────────────────────────────────────────────────────────────
function navigate(view) {
  const isIngest = view === 'ingest';
  document.body.classList.toggle('view-ingest', isIngest);
  document.title = isIngest ? 'vkb · Ingest' : 'vkb · Graph';
  const targetHash = '#' + (isIngest ? 'ingest' : 'viz');
  if (location.hash !== targetHash) history.replaceState(null, '', targetHash);

  document.querySelectorAll('[data-nav]').forEach(el => {
    const active = el.dataset.nav === (isIngest ? 'ingest' : 'viz');
    el.classList.toggle('active', active);
    el.querySelector('.nav-item-sub')?.remove();
    if (active) {
      const sub = document.createElement('span');
      sub.className = 'nav-item-sub';
      sub.textContent = 'current';
      el.appendChild(sub);
    }
  });
}

// ─── System alert ─────────────────────────────────────────────────────────────
function showSysAlert(msg, isOk) {
  const el = document.getElementById('sys-alert');
  document.getElementById('sys-alert-msg').textContent = msg;
  document.getElementById('sys-alert-icon').textContent = isOk ? '✓' : '⚠';
  el.className = isOk ? 'visible ok' : 'visible';
}

function hideSysAlert() {
  document.getElementById('sys-alert').className = '';
}

// ─── Utilities ────────────────────────────────────────────────────────────────
function escHtml(s) {
  return String(s)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function fmtSize(bytes) {
  if (bytes < 1024) return bytes + ' B';
  if (bytes < 1048576) return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1048576).toFixed(1) + ' MB';
}

function fmtAge(d) {
  const s = Math.floor((Date.now() - d) / 1000);
  if (s < 60) return s + 's';
  if (s < 3600) return Math.floor(s / 60) + 'm';
  return Math.floor(s / 3600) + 'h';
}

// ─── Expose window.__vkb (synchronous — before DOMContentLoaded) ──────────────
window.__vkb = {
  bus: { subscribe, publish },
  navigate,
  showSysAlert,
  hideSysAlert,
  escHtml,
  fmtSize,
  fmtAge,
};

// ─── WebSocket ────────────────────────────────────────────────────────────────
let ws = null;

function setWsConnected(live) {
  // viz header
  const wsStatus = document.getElementById('ws-status');
  if (wsStatus) {
    wsStatus.textContent = live ? 'live' : 'reconnecting…';
    wsStatus.className = `stat ${live ? 'ws-live' : 'ws-off'}`;
  }
  // ingest header
  const wsDot = document.getElementById('ws-dot');
  if (wsDot) wsDot.classList.toggle('connected', live);
  const wsLabel = document.getElementById('ws-label');
  if (wsLabel) {
    wsLabel.textContent = live ? 'live' : 'reconnecting…';
    wsLabel.style.color = live ? 'var(--teal)' : 'var(--hint)';
  }
}

function connectWS() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/stream`);

  ws.addEventListener('open', () => {
    setWsConnected(true);
    window.__vkb.bus.publish('ws_open', {});
  });

  ws.addEventListener('close', () => {
    setWsConnected(false);
    window.__vkb.bus.publish('ws_close', {});
    setTimeout(connectWS, 3000);
  });

  ws.addEventListener('error', () => ws.close());

  ws.addEventListener('message', e => {
    let msg;
    try { msg = JSON.parse(e.data); } catch { return; }
    if (!msg || !msg.type) return;
    window.__vkb.bus.publish(msg.type, msg);
  });
}

// ─── Boot ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  // Wire nav
  const navAnchor = document.getElementById('nav-anchor');
  const navPopout = document.getElementById('nav-popout');
  navAnchor.addEventListener('click', e => {
    e.stopPropagation();
    const open = navPopout.classList.toggle('open');
    navAnchor.setAttribute('aria-expanded', String(open));
  });
  document.addEventListener('click', () => {
    navPopout.classList.remove('open');
    navAnchor.setAttribute('aria-expanded', 'false');
  });
  navPopout.addEventListener('click', e => e.stopPropagation());

  // Wire sys alert dismiss
  document.getElementById('sys-alert-dismiss').addEventListener('click', hideSysAlert);

  // Wire bus for db alerts
  window.__vkb.bus.subscribe('db_unavailable', () =>
    showSysAlert('Database unavailable — check that PostgreSQL (Docker) is running.', false)
  );
  window.__vkb.bus.subscribe('db_available', () => {
    showSysAlert('Database connection restored.', true);
    setTimeout(hideSysAlert, 4000);
  });

  // Initial navigation
  navigate(location.hash === '#ingest' ? 'ingest' : 'viz');

  // Hash router
  window.addEventListener('hashchange', () => {
    const navAnchor = document.getElementById('nav-anchor');
    const navPopout = document.getElementById('nav-popout');
    navigate(location.hash === '#ingest' ? 'ingest' : 'viz');
    navPopout.classList.remove('open');
    navAnchor.setAttribute('aria-expanded', 'false');
  });

  // Connect WebSocket
  connectWS();

  // Init context modules
  window.__vkb_ingest?.init();
  window.__vkb_viz?.init();
});
