// ingest.js — ES module, browser context
// Implements the ingest context for the vkb SPA.
// window.__vkb must be set by app.js before init() is called.

// ── State ────────────────────────────────────────────────────────────────────

const STAGES        = ['queued','fetching','chunking','embedding','sectioning','summarising','extracting','done'];
const STAGE_IDX     = Object.fromEntries(STAGES.map((s, i) => [s, i]));
const ACTIVE_STAGES = new Set(['queued','fetching','chunking','embedding','sectioning','summarising','extracting']);
const ARCHIVE_DELAY_MS = 30_000;
const ARCHIVE_LS_KEY   = 'vkb_archived_jobs';

const jobs         = new Map(); // Map<jobId, { label, stage, error, entityId, priorStage }>
const archiveTimers = new Map(); // Map<jobId, timeoutId>

let activeTab         = 'url';
let pendingFiles       = [];
let pendingFolderFiles = [];
let remItems           = [];

// ── Export ───────────────────────────────────────────────────────────────────

window.__vkb_ingest = { init };

// ── Archive persistence ───────────────────────────────────────────────────────

function loadArchive() {
  try { return JSON.parse(localStorage.getItem(ARCHIVE_LS_KEY) || '[]'); } catch { return []; }
}
function saveArchive(arr) {
  try { localStorage.setItem(ARCHIVE_LS_KEY, JSON.stringify(arr)); } catch {}
}
function archiveJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) return;
  archiveTimers.delete(jobId);
  const archived = loadArchive();
  if (!archived.find(a => a.id === jobId)) {
    archived.unshift({ id: jobId, label: job.label, stage: job.stage, error: job.error ?? null, archivedAt: Date.now() });
    if (archived.length > 200) archived.splice(200);
    saveArchive(archived);
  }
  jobs.delete(jobId);
  document.getElementById(`job-${jobId}`)?.remove();
  updateJobCountLabel();
  checkCompletion();
  renderArchive();
}
function scheduleArchive(jobId) {
  if (archiveTimers.has(jobId)) return;
  archiveTimers.set(jobId, setTimeout(() => archiveJob(jobId), ARCHIVE_DELAY_MS));
}

// ── Hydration ─────────────────────────────────────────────────────────────────

function jobLabelFromRecord(j) {
  const meta = j.meta ?? {};
  return meta.title || meta.filename || j.ref || j.entity_id?.slice(0, 8) || j.id.slice(0, 8);
}

async function hydrateLiveJobs() {
  try {
    const res = await fetch('/jobs?limit=200');
    const data = await res.json();
    if (!data.ok) return;

    const archived    = loadArchive();
    const archivedIds = new Set(archived.map(a => a.id));
    const toArchive   = [];

    for (const j of data.data.jobs) {
      if (archivedIds.has(j.id) || jobs.has(j.id)) continue;
      const isSettled = j.stage === 'done' || j.stage === 'error';
      const ageMs     = Date.now() - new Date(j.completed_at ?? j.created_at).getTime();

      if (!isSettled || ageMs <= 5 * 60 * 1000) {
        // Active or recently-settled: show in pipeline, auto-archive after delay
        const label  = jobLabelFromRecord(j);
        const errMsg = j.stage === 'error' ? (j.progress?.error_detail ?? null) : null;
        registerJob(j.id, j.entity_id, label, /* skipPoll */ true);
        if (j.stage !== 'queued') updateJobStage(j.id, j.stage, errMsg);
        if (isSettled) scheduleArchive(j.id);
      } else {
        // Old settled job: put straight into archive, skip pipeline
        toArchive.push(j);
      }
    }

    if (toArchive.length > 0) {
      for (const j of toArchive) {
        archived.unshift({
          id:         j.id,
          label:      jobLabelFromRecord(j),
          stage:      j.stage,
          error:      j.stage === 'error' ? (j.progress?.error_detail ?? null) : null,
          archivedAt: Date.now(),
        });
      }
      if (archived.length > 200) archived.splice(200);
      saveArchive(archived);
      renderArchive();
    }
  } catch { /* ignore */ }
}

async function ensureJobRegistered(jobId) {
  try {
    const res = await fetch('/jobs?limit=200');
    const data = await res.json();
    if (!data.ok) return;
    const j = data.data.jobs.find(r => r.id === jobId);
    if (!j) return;
    const archived = loadArchive();
    if (archived.find(a => a.id === j.id)) return;
    registerJob(j.id, j.entity_id, jobLabelFromRecord(j), true);
  } catch {}
}

// ── File helpers ──────────────────────────────────────────────────────────────

function normaliseName(filename) {
  return filename
    .replace(/\.[^.]+$/, '').replace(/[-_./ \\]+/g, ' ')
    .replace(/[^a-zA-Z0-9 ]/g, '').replace(/\s+/g, ' ').trim();
}
function isTextFile(name) {
  return /\.(txt|md|markdown|html?|csv|json|xml|rst|org|tex|js|ts|py|rb|go|rs|c|cpp|h|java|kt|swift|sql|yaml|yml|toml|sh|bash|zsh|ps1|r|jl|lua|pl|php)$/i.test(name);
}
function isEpub(name) { return /\.epub$/i.test(name); }

async function readFileText(file) {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = e => resolve(e.target.result);
    r.onerror = () => reject(new Error(`Failed to read ${file.name}`));
    r.readAsText(file, 'utf-8');
  });
}
async function readFileArrayBuffer(file) {
  return new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = e => resolve(e.target.result);
    r.onerror = () => reject(new Error(`Failed to read ${file.name}`));
    r.readAsArrayBuffer(file);
  });
}
function collapseWhitespace(text) {
  return text.replace(/\r\n/g, '\n').replace(/\r/g, '\n')
    .replace(/\t/g, ' ').replace(/ {2,}/g, ' ').replace(/\n{3,}/g, '\n\n').trim();
}
async function parseEpub(file) {
  const buf = await readFileArrayBuffer(file);
  const zip = await JSZip.loadAsync(buf);
  const containerXml = await zip.file('META-INF/container.xml')?.async('string');
  if (!containerXml) throw new Error('Missing META-INF/container.xml');
  const containerDoc = new DOMParser().parseFromString(containerXml, 'application/xml');
  const opfPath = containerDoc.querySelector('rootfile')?.getAttribute('full-path');
  if (!opfPath) throw new Error('Cannot locate OPF');
  const opfXml = await zip.file(opfPath)?.async('string');
  if (!opfXml) throw new Error(`Missing OPF at ${opfPath}`);
  const opfDoc = new DOMParser().parseFromString(opfXml, 'application/xml');
  const opfDir = opfPath.includes('/') ? opfPath.slice(0, opfPath.lastIndexOf('/') + 1) : '';
  const manifest = {};
  opfDoc.querySelectorAll('manifest item').forEach(item => { manifest[item.getAttribute('id')] = item.getAttribute('href'); });
  const spineHrefs = Array.from(opfDoc.querySelectorAll('spine itemref')).map(ref => manifest[ref.getAttribute('idref')]).filter(Boolean);
  const parts = [];
  for (const href of spineHrefs) {
    const zipPath = opfDir + href.replace(/#.*$/, '');
    const html = await zip.file(zipPath)?.async('string');
    if (!html) continue;
    const doc = new DOMParser().parseFromString(html, 'application/xhtml+xml');
    const body = doc.querySelector('body');
    if (body) parts.push(body.textContent || '');
  }
  if (!parts.length) throw new Error('No readable chapters found in EPUB');
  return collapseWhitespace(parts.join('\n\n'));
}

// ── File list rendering ───────────────────────────────────────────────────────

function renderFileList(listEl, files) {
  const { escHtml, fmtSize } = window.__vkb;
  listEl.innerHTML = '';
  files.forEach((f, i) => {
    const item = document.createElement('div');
    item.className = f.skipped ? 'file-item fi-skipped' : 'file-item';
    const label = escHtml(f.displayName || normaliseName(f.name));
    const hasPath = f.path && f.path !== f.name;
    const subPath = hasPath ? `<span class="fi-path">${escHtml(f.path)}</span>` : '';
    const rightSide = f.skipped
      ? `<span class="fi-skip-chip" title="${escHtml(f.skipReason || 'unsupported format')}">skipped</span>`
      : `<span class="fi-size">${fmtSize(f.size)}</span>`;
    item.innerHTML = `
      <span class="fi-info" title="${escHtml(f.name)}">
        <span class="fi-name">${label}</span>${subPath}
      </span>
      ${rightSide}
      <button class="fi-remove" data-idx="${i}" title="Remove">✕</button>
    `;
    listEl.appendChild(item);
  });
  listEl.querySelectorAll('.fi-remove').forEach(btn => {
    btn.addEventListener('click', () => {
      const idx = parseInt(btn.dataset.idx);
      if (listEl.id === 'files-list') pendingFiles.splice(idx, 1);
      else pendingFolderFiles.splice(idx, 1);
      renderFileList(listEl, listEl.id === 'files-list' ? pendingFiles : pendingFolderFiles);
      updateSubmitState();
    });
  });
}

async function handleFileSelection(input, targetArr, listEl) {
  const rawFiles = Array.from(input.files || []);
  for (const f of rawFiles) {
    const base = { name: f.name, displayName: normaliseName(f.name), size: f.size, path: f.webkitRelativePath || f.name };
    if (isEpub(f.name)) {
      try { targetArr.push({ ...base, text: await parseEpub(f) }); }
      catch { targetArr.push({ ...base, text: null, skipped: true, skipReason: 'epub parse error' }); }
    } else if (isTextFile(f.name)) {
      try { targetArr.push({ ...base, text: await readFileText(f) }); }
      catch { targetArr.push({ ...base, text: null, skipped: true, skipReason: 'read error' }); }
    } else {
      targetArr.push({ ...base, text: null, skipped: true, skipReason: 'unsupported format' });
    }
  }
  renderFileList(listEl, targetArr);
  updateSubmitState();
}

// ── Submit state ──────────────────────────────────────────────────────────────

function updateSubmitState() {
  const btn = document.getElementById('submit-btn');
  let ready = false;
  if (activeTab === 'url')    ready = !!document.getElementById('url-input').value.trim();
  if (activeTab === 'text')   ready = !!document.getElementById('text-content').value.trim();
  if (activeTab === 'files')  ready = pendingFiles.some(f => !f.skipped);
  if (activeTab === 'folder') ready = pendingFolderFiles.some(f => !f.skipped);
  btn.disabled = !ready;
}

// ── Pipeline UI ───────────────────────────────────────────────────────────────

function createJobCard(jobId, label) {
  const { escHtml } = window.__vkb;
  const el = document.createElement('div');
  el.className = 'job-card';
  el.id = `job-${jobId}`;
  el.innerHTML = `
    <div class="job-meta">
      <div class="job-label" title="${escHtml(label)}">${escHtml(label)}</div>
      <div class="job-status status-pending" id="jstatus-${jobId}">queued</div>
    </div>
    <div class="stage-rail" id="srail-${jobId}">
      ${STAGES.filter(s => s !== 'queued').map(s => `
        <div class="stage-node sn-pending" id="sn-${jobId}-${s}">
          <div class="stage-dot"></div>
          <div class="stage-label">${s}</div>
        </div>
      `).join('')}
    </div>
    <div class="job-progress-bar-wrap">
      <div class="job-progress-bar" id="jpbar-${jobId}" style="width:0%"></div>
    </div>
  `;
  return el;
}

function updateJobStage(jobId, stage, errorMsg) {
  const job = jobs.get(jobId);
  if (!job) return;
  job.stage = stage;
  if (errorMsg) job.error = errorMsg;
  const statusEl = document.getElementById(`jstatus-${jobId}`);
  const pbarEl   = document.getElementById(`jpbar-${jobId}`);
  if (!statusEl) return;
  const isDone  = stage === 'done';
  const isError = stage === 'error';
  const isActive = ACTIVE_STAGES.has(stage);
  statusEl.className   = `job-status ${isDone ? 'status-done' : isError ? 'status-error' : isActive ? 'status-active' : 'status-pending'}`;
  statusEl.textContent = stage;
  const stagesForBar = STAGES.filter(s => s !== 'queued');
  const curIdx = stagesForBar.indexOf(stage);
  const pct = isDone ? 100
    : isError ? (curIdx >= 0 ? Math.round(curIdx / stagesForBar.length * 100) : 0)
    : (curIdx >= 0 ? Math.round((curIdx + 0.5) / stagesForBar.length * 100) : 0);
  pbarEl.style.width = pct + '%';
  pbarEl.className = `job-progress-bar ${isDone ? '' : isError ? 'pb-error' : 'pb-active'}`;
  STAGES.filter(s => s !== 'queued').forEach(s => {
    const node = document.getElementById(`sn-${jobId}-${s}`);
    if (!node) return;
    const sIdx = STAGE_IDX[s];
    const cur  = isError
      ? (STAGE_IDX[stage] ?? STAGE_IDX[job.priorStage ?? 'queued'])
      : STAGE_IDX[stage];
    node.className = 'stage-node ' + (
      isDone                               ? 'sn-done'   :
      isError && s === 'error'             ? 'sn-error'  :
      isError && sIdx < cur                ? 'sn-done'   :
      !isError && sIdx < STAGE_IDX[stage]  ? 'sn-done'   :
      !isError && s === stage              ? 'sn-active'  : 'sn-pending'
    );
  });
  const existingErr = document.getElementById(`jerr-${jobId}`);
  if (isError && errorMsg) {
    if (!existingErr) {
      const errEl = document.createElement('div');
      errEl.className = 'job-error-msg';
      errEl.id = `jerr-${jobId}`;
      errEl.textContent = errorMsg;
      document.getElementById(`job-${jobId}`)?.appendChild(errEl);
    }
  } else if (existingErr) {
    existingErr.remove();
  }
  if (!isError) job.priorStage = stage;
  if (isDone || isError) scheduleArchive(jobId);
  checkCompletion();
}

function checkCompletion() {
  if (jobs.size === 0) { updateJobCountLabel(); return; }
  const all      = [...jobs.values()];
  const settled  = all.filter(j => j.stage === 'done' || j.stage === 'error');
  const errors   = all.filter(j => j.stage === 'error');
  updateJobCountLabel();
  if (settled.length < all.length) return;
  const resultSection = document.getElementById('result-section');
  const resultIcon    = document.getElementById('result-icon');
  const resultTitle   = document.getElementById('result-title');
  const resultSub     = document.getElementById('result-sub');
  resultSection.classList.add('visible');
  document.getElementById('pipeline-empty').style.display = 'none';
  if (errors.length === 0) {
    resultIcon.textContent = '✓'; resultIcon.style.color = 'var(--teal)';
    resultTitle.textContent = all.length === 1 ? 'Ingestion complete' : `${all.length} items ingested`;
    resultSub.textContent = 'All items processed and indexed. Explore the knowledge graph.';
  } else if (errors.length === all.length) {
    resultIcon.textContent = '✕'; resultIcon.style.color = 'var(--coral)';
    resultTitle.textContent = 'Ingestion failed';
    resultSub.textContent = `${errors.length} item${errors.length > 1 ? 's' : ''} encountered errors.`;
  } else {
    resultIcon.textContent = '⚠'; resultIcon.style.color = 'var(--amber)';
    resultTitle.textContent = `${all.length - errors.length} of ${all.length} items ingested`;
    resultSub.textContent = `${errors.length} item${errors.length > 1 ? 's' : ''} failed. Successful items are available in the graph.`;
  }
}

function updateJobCountLabel() {
  const all = [...jobs.values()];
  const lbl = document.getElementById('job-count-label');
  if (!lbl) return;
  if (all.length === 0) { lbl.textContent = ''; return; }
  const parts   = [];
  const running = all.filter(j => j.stage !== 'done' && j.stage !== 'error').length;
  const done    = all.filter(j => j.stage === 'done').length;
  const errored = all.filter(j => j.stage === 'error').length;
  if (running) parts.push(`${running} running`);
  if (done)    parts.push(`${done} done`);
  if (errored) parts.push(`${errored} error`);
  lbl.textContent = parts.join(' · ');
}

// ── Submit / register / poll ──────────────────────────────────────────────────

async function postIngest(payload) {
  const res = await fetch('/ingest', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const json = await res.json();
  if (!json.ok) throw new Error(json.error || 'Unknown server error');
  return json.data;
}

function registerJob(jobId, entityId, label, skipPoll = false) {
  const emptyEl = document.getElementById('pipeline-empty');
  if (emptyEl) emptyEl.style.display = 'none';
  const card = createJobCard(jobId, label);
  document.getElementById('pipeline-body')?.insertBefore(card, emptyEl);
  jobs.set(jobId, { stage: 'queued', label, entityId, error: null, priorStage: null });
  updateJobCountLabel();
  if (!skipPoll) pollJob(jobId);
}

async function pollJob(jobId) {
  const interval = setInterval(async () => {
    const job = jobs.get(jobId);
    if (!job || job.stage === 'done' || job.stage === 'error') { clearInterval(interval); return; }
    try {
      const res  = await fetch('/jobs?limit=200');
      const data = await res.json();
      if (!data.ok) return;
      const found = data.data.jobs.find(j => j.id === jobId);
      if (!found || found.stage === job.stage) return;
      const errMsg = found.stage === 'error' ? (found.progress?.error_detail ?? 'Pipeline error') : null;
      updateJobStage(jobId, found.stage, errMsg);
    } catch {}
  }, 2000);
}

async function submitURL() {
  const url   = document.getElementById('url-input').value.trim();
  const tag   = document.getElementById('url-tag').value.trim();
  const title = document.getElementById('url-title').value.trim();
  const meta  = {};
  if (tag)   meta.tag   = tag;
  if (title) meta.title = title;
  const d = await postIngest({ type: 'url', ref: url, meta });
  registerJob(d.job_id, d.entity_id, url);
  document.getElementById('url-input').value  = '';
  document.getElementById('url-tag').value    = '';
  document.getElementById('url-title').value  = '';
}

async function submitText() {
  const text  = document.getElementById('text-content').value.trim();
  const title = document.getElementById('text-title').value.trim() || 'Untitled note';
  const tag   = document.getElementById('text-tag').value.trim();
  const meta  = { title };
  if (tag) meta.tag = tag;
  const d = await postIngest({ type: 'note', text, meta });
  registerJob(d.job_id, d.entity_id, title);
  document.getElementById('text-content').value = '';
  document.getElementById('text-title').value   = '';
  document.getElementById('text-tag').value     = '';
}

async function submitFiles(fileArr, tag) {
  const filesList  = document.getElementById('files-list');
  const folderList = document.getElementById('folder-list');
  for (const f of fileArr) {
    if (f.skipped) continue;
    const meta = { filename: f.name, path: f.path };
    if (tag) meta.tag = tag;
    const d = await postIngest({ type: 'doc', text: f.text, meta });
    registerJob(d.job_id, d.entity_id, f.name);
  }
  if (activeTab === 'files') { pendingFiles.length = 0; renderFileList(filesList, pendingFiles); }
  else { pendingFolderFiles.length = 0; renderFileList(folderList, pendingFolderFiles); }
}

// ── Archive UI ────────────────────────────────────────────────────────────────

function renderArchive() {
  const archived = loadArchive();
  const badge    = document.getElementById('arc-badge');
  const listEl   = document.getElementById('archived-list');
  const emptyEl  = document.getElementById('archive-empty');
  if (badge) badge.textContent = String(archived.length);
  if (!listEl) return;
  listEl.innerHTML = '';
  if (archived.length === 0) {
    if (emptyEl) listEl.appendChild(emptyEl);
    return;
  }
  archived.forEach(a => {
    const isDone  = a.stage === 'done';
    const isError = a.stage === 'error';
    const card = document.createElement('div');
    card.className = 'job-card';
    card.innerHTML = `
      <div class="job-meta">
        <div class="job-label" title="${window.__vkb.escHtml(a.label)}">${window.__vkb.escHtml(a.label)}</div>
        <div class="job-status ${isDone ? 'status-done' : isError ? 'status-error' : 'status-pending'}">${a.stage}</div>
      </div>
      <div class="job-progress-bar-wrap">
        <div class="job-progress-bar ${isDone ? '' : 'pb-error'}" style="width:${isDone ? 100 : isError ? 40 : 0}%"></div>
      </div>
      ${a.error ? `<div class="job-error-msg">${window.__vkb.escHtml(a.error)}</div>` : ''}
    `;
    listEl.appendChild(card);
  });
}

// ── Remediation panel ─────────────────────────────────────────────────────────

function fmtRemLabel(item) {
  if (item.ref) return item.ref;
  const filename = item.meta?.filename || item.meta?.title || null;
  if (filename) return String(filename);
  return item.id.slice(0, 8) + '…';
}

function remedyLabel(r) {
  if (r === 'reingest') return 'needs repair';
  if (r === 'stuck')    return 'stuck';
  return 'no raw — delete only';
}

function updateRemBadge(count, ok) {
  const badge = document.getElementById('rem-badge');
  if (!badge) return;
  if (count === 0 && ok) {
    badge.textContent = 'all clear';
    badge.className = 'rem-badge b-ok';
  } else if (count > 0) {
    badge.textContent = `${count} issue${count !== 1 ? 's' : ''}`;
    badge.className = 'rem-badge';
  } else {
    badge.textContent = 'not scanned';
    badge.className = 'rem-badge b-none';
  }
}

function renderRemTable(items) {
  const content      = document.getElementById('rem-content');
  const lbl          = document.getElementById('rem-toolbar-label');
  const repairAllBtn = document.getElementById('rem-repair-all-btn');
  const deleteAllBtn = document.getElementById('rem-delete-all-btn');

  if (items.length === 0) {
    content.innerHTML = '<div class="rem-empty" style="color:var(--teal)">✓ No broken ingestions found.</div>';
    lbl.textContent = 'All clear.';
    repairAllBtn.disabled = true;
    deleteAllBtn.disabled = true;
    updateRemBadge(0, true);
    return;
  }

  const repairable = items.filter(i => i.remedy !== 'no_raw');
  lbl.textContent = `${items.length} issue${items.length !== 1 ? 's' : ''} found · ${repairable.length} repairable`;
  repairAllBtn.disabled = repairable.length === 0;
  deleteAllBtn.disabled = false;
  updateRemBadge(items.length, false);

  const tbl = document.createElement('table');
  tbl.className = 'rem-table';
  tbl.innerHTML = `
    <thead>
      <tr>
        <th>Source</th>
        <th>Type</th>
        <th>Status</th>
        <th>Chunks</th>
        <th>Last stage</th>
        <th>Diagnosis</th>
        <th>Actions</th>
      </tr>
    </thead>
    <tbody id="rem-tbody"></tbody>
  `;
  content.innerHTML = '';
  content.appendChild(tbl);

  const tbody = document.getElementById('rem-tbody');
  items.forEach(item => {
    const tr = document.createElement('tr');
    tr.id = `rem-row-${item.id}`;
    const lastStage = item.latest_job?.stage ?? '—';
    const errTip    = item.latest_job?.error ? ` title="${String(item.latest_job.error).replace(/"/g, '&quot;')}"` : '';
    tr.innerHTML = `
      <td><div class="rem-ref"${errTip}>${window.__vkb.escHtml(fmtRemLabel(item))}</div></td>
      <td>${window.__vkb.escHtml(item.type)}</td>
      <td>${window.__vkb.escHtml(item.status)}</td>
      <td>${item.chunk_count}</td>
      <td>${window.__vkb.escHtml(lastStage)}</td>
      <td><span class="rem-remedy r-${item.remedy}">${remedyLabel(item.remedy)}</span></td>
      <td>
        <div class="rem-action-cell">
          <button class="ra-repair" data-id="${item.id}" ${item.remedy === 'no_raw' ? 'disabled title="No raw content — cannot repair"' : ''}>Repair</button>
          <button class="ra-delete" data-id="${item.id}">Delete</button>
        </div>
      </td>
    `;
    tbody.appendChild(tr);
  });

  tbody.querySelectorAll('.ra-repair').forEach(btn => {
    btn.addEventListener('click', () => repairOne(btn.dataset.id));
  });
  tbody.querySelectorAll('.ra-delete').forEach(btn => {
    btn.addEventListener('click', () => deleteOne(btn.dataset.id));
  });
}

function updateAfterRemove() {
  const lbl          = document.getElementById('rem-toolbar-label');
  const repairAllBtn = document.getElementById('rem-repair-all-btn');
  const deleteAllBtn = document.getElementById('rem-delete-all-btn');
  if (remItems.length === 0) {
    document.getElementById('rem-content').innerHTML =
      '<div class="rem-empty" style="color:var(--teal)">✓ No broken ingestions found.</div>';
    lbl.textContent = 'All clear.';
    repairAllBtn.disabled = true;
    deleteAllBtn.disabled = true;
    updateRemBadge(0, true);
  } else {
    const repairable = remItems.filter(i => i.remedy !== 'no_raw');
    lbl.textContent = `${remItems.length} issue${remItems.length !== 1 ? 's' : ''} · ${repairable.length} repairable`;
    repairAllBtn.disabled = repairable.length === 0;
    updateRemBadge(remItems.length, false);
  }
}

async function scanBroken() {
  const btn = document.getElementById('rem-scan-btn');
  btn.disabled = true;
  btn.textContent = 'Scanning…';
  try {
    const res  = await fetch('/entities/broken');
    const data = await res.json();
    if (!data.ok) throw new Error(data.error);
    remItems = data.data.broken;
    renderRemTable(remItems);
  } catch (err) {
    document.getElementById('rem-content').innerHTML =
      `<div class="rem-empty" style="color:var(--coral)">Scan failed: ${window.__vkb.escHtml(err.message)}</div>`;
    updateRemBadge(0, false);
  } finally {
    btn.disabled = false;
    btn.textContent = 'Scan now';
  }
}

async function repairOne(entityId) {
  const row    = document.getElementById(`rem-row-${entityId}`);
  const repBtn = row?.querySelector('.ra-repair');
  const delBtn = row?.querySelector('.ra-delete');
  if (repBtn) { repBtn.disabled = true; repBtn.textContent = 'Queuing…'; }
  if (delBtn)   delBtn.disabled = true;
  try {
    const res = await fetch('/reingest', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ entity_id: entityId }),
    });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error);
    const job = data.data.jobs?.[0];
    if (job) {
      const item = remItems.find(i => i.id === entityId);
      registerJob(job.job_id, job.entity_id, item ? fmtRemLabel(item) : entityId.slice(0, 8));
      document.getElementById('pipeline-empty').style.display = 'none';
    }
    row?.remove();
    remItems = remItems.filter(i => i.id !== entityId);
    updateAfterRemove();
  } catch (err) {
    if (repBtn) { repBtn.disabled = false; repBtn.textContent = 'Repair'; }
    if (delBtn)   delBtn.disabled = false;
    alert('Repair failed: ' + err.message);
  }
}

async function deleteOne(entityId) {
  if (!confirm('Delete this entity and all its derived data?')) return;
  const row    = document.getElementById(`rem-row-${entityId}`);
  const repBtn = row?.querySelector('.ra-repair');
  const delBtn = row?.querySelector('.ra-delete');
  if (repBtn) repBtn.disabled = true;
  if (delBtn) { delBtn.disabled = true; delBtn.textContent = 'Deleting…'; }
  try {
    const res  = await fetch(`/entities/${entityId}`, { method: 'DELETE' });
    const data = await res.json();
    if (!data.ok) throw new Error(data.error);
    row?.remove();
    remItems = remItems.filter(i => i.id !== entityId);
    updateAfterRemove();
  } catch (err) {
    if (repBtn) repBtn.disabled = false;
    if (delBtn) { delBtn.disabled = false; delBtn.textContent = 'Delete'; }
    alert('Delete failed: ' + err.message);
  }
}

// ── init() ────────────────────────────────────────────────────────────────────

function init() {
  // Tabs
  document.querySelectorAll('.tab-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
      document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
      btn.classList.add('active');
      activeTab = btn.dataset.tab;
      document.getElementById(`tab-${activeTab}`).classList.add('active');
      updateSubmitState();
    });
  });
  document.getElementById('url-input').addEventListener('input', updateSubmitState);
  document.getElementById('text-content').addEventListener('input', updateSubmitState);

  // File inputs
  const filesInput  = document.getElementById('files-input');
  const filesList   = document.getElementById('files-list');
  const folderInput = document.getElementById('folder-input');
  const folderList  = document.getElementById('folder-list');
  filesInput.addEventListener('change',  () => handleFileSelection(filesInput,  pendingFiles,       filesList));
  folderInput.addEventListener('change', () => handleFileSelection(folderInput, pendingFolderFiles, folderList));

  // Drag & drop for #files-drop and #folder-drop
  ['files-drop', 'folder-drop'].forEach(id => {
    const zone = document.getElementById(id);
    zone.addEventListener('dragover',  e => { e.preventDefault(); zone.classList.add('drag-over'); });
    zone.addEventListener('dragleave', () => zone.classList.remove('drag-over'));
    zone.addEventListener('drop', e => {
      e.preventDefault(); zone.classList.remove('drag-over');
      const dt = e.dataTransfer;
      if (!dt?.files.length) return;
      const arr = id === 'files-drop' ? pendingFiles       : pendingFolderFiles;
      const lst = id === 'files-drop' ? filesList          : folderList;
      Promise.all(Array.from(dt.files).map(f => {
        const base = { name: f.name, displayName: normaliseName(f.name), size: f.size, path: f.name };
        if (isEpub(f.name)) {
          return parseEpub(f)
            .then(text => arr.push({ ...base, text }))
            .catch(() => arr.push({ ...base, text: null, skipped: true, skipReason: 'epub parse error' }));
        }
        if (!isTextFile(f.name)) {
          arr.push({ ...base, text: null, skipped: true, skipReason: 'unsupported format' });
          return;
        }
        return readFileText(f)
          .then(text => arr.push({ ...base, text }))
          .catch(() => arr.push({ ...base, text: null, skipped: true, skipReason: 'read error' }));
      })).then(() => { renderFileList(lst, arr); updateSubmitState(); });
    });
  });

  // Submit button
  document.getElementById('submit-btn').addEventListener('click', async () => {
    const btn = document.getElementById('submit-btn');
    btn.disabled = true; btn.textContent = 'Submitting…';
    document.getElementById('result-section').classList.remove('visible');
    document.getElementById('pipeline-empty').style.display = 'none';
    try {
      if      (activeTab === 'url')    await submitURL();
      else if (activeTab === 'text')   await submitText();
      else if (activeTab === 'files')  await submitFiles(pendingFiles,       document.getElementById('files-tag').value.trim());
      else if (activeTab === 'folder') await submitFiles(pendingFolderFiles, document.getElementById('folder-tag').value.trim());
    } catch (err) {
      console.error(err);
      alert('Submission error: ' + err.message);
    } finally {
      btn.disabled = false; btn.textContent = 'Ingest';
      updateSubmitState();
    }
  });

  // Start again
  document.getElementById('start-again-btn').addEventListener('click', () => {
    for (const [jobId, job] of jobs.entries()) {
      if (job.stage === 'done' || job.stage === 'error') {
        const tid = archiveTimers.get(jobId);
        if (tid) clearTimeout(tid);
        archiveJob(jobId);
      }
    }
    jobs.clear();
    document.getElementById('pipeline-body').querySelectorAll('.job-card').forEach(el => el.remove());
    document.getElementById('pipeline-empty').style.display = '';
    document.getElementById('result-section').classList.remove('visible');
    updateJobCountLabel();
    updateSubmitState();
    renderArchive();
  });

  // Archive panel
  document.getElementById('archived-toggle').addEventListener('click', () => {
    const toggle = document.getElementById('archived-toggle');
    const body   = document.getElementById('archived-body');
    const open   = body.classList.toggle('open');
    toggle.classList.toggle('open', open);
    toggle.setAttribute('aria-expanded', String(open));
    if (open) renderArchive();
  });
  document.getElementById('arc-clear-btn').addEventListener('click', () => { saveArchive([]); renderArchive(); });

  // Remediation panel
  document.getElementById('rem-card-header').addEventListener('click', () => {
    const body    = document.getElementById('rem-body');
    const chevron = document.getElementById('rem-chevron');
    const header  = document.getElementById('rem-card-header');
    const open    = body.classList.toggle('open');
    chevron.classList.toggle('open', open);
    header.classList.toggle('open', open);
    header.setAttribute('aria-expanded', String(open));
  });
  document.getElementById('rem-scan-btn').addEventListener('click', scanBroken);
  document.getElementById('rem-repair-all-btn').addEventListener('click', async () => {
    const repairable = remItems.filter(i => i.remedy !== 'no_raw');
    if (!repairable.length) return;
    const btn = document.getElementById('rem-repair-all-btn');
    btn.disabled = true; btn.textContent = `Queuing ${repairable.length}…`;
    for (const item of repairable) await repairOne(item.id).catch(() => {});
    btn.textContent = 'Repair all repairable';
  });
  document.getElementById('rem-delete-all-btn').addEventListener('click', async () => {
    if (!confirm(`Delete all ${remItems.length} broken entities and their data?`)) return;
    const btn = document.getElementById('rem-delete-all-btn');
    btn.disabled = true; btn.textContent = `Deleting ${remItems.length}…`;
    const ids = remItems.map(i => i.id);
    for (const id of ids) await deleteOne(id).catch(() => {});
    btn.textContent = 'Delete all';
  });

  // Bus subscriptions
  window.__vkb.bus.subscribe('ws_open', () => hydrateLiveJobs());
  window.__vkb.bus.subscribe('stage_change', async msg => {
    if (!msg.job_id) return;
    if (!jobs.has(msg.job_id)) await ensureJobRegistered(msg.job_id);
    if (jobs.has(msg.job_id)) updateJobStage(msg.job_id, msg.stage, null);
  });
  window.__vkb.bus.subscribe('error', async msg => {
    if (!msg.job_id) return;
    if (!jobs.has(msg.job_id)) await ensureJobRegistered(msg.job_id);
    const errMsg = typeof msg.payload === 'string'
      ? msg.payload
      : (msg.payload?.detail ?? msg.payload?.error ?? 'Pipeline error');
    if (jobs.has(msg.job_id)) updateJobStage(msg.job_id, 'error', errMsg);
  });
  window.__vkb.bus.subscribe('complete', async msg => {
    if (!msg.job_id) return;
    if (!jobs.has(msg.job_id)) await ensureJobRegistered(msg.job_id);
    if (jobs.has(msg.job_id)) updateJobStage(msg.job_id, 'done', null);
  });

  // Initial render
  renderArchive();
  updateSubmitState();
}
