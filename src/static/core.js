// Shared by every page (loaded from templates/_base.html before any
// page-specific script). Expects BASE_PATH and DELETE_TOKEN to be defined by
// the inline <script> in _base.html.
//
// Only function declarations live at the top level here — page scripts are
// classic scripts sharing the same global scope, so a top-level const/let in
// this file would collide with a same-named one in a page script.

// ── Theme toggle ──────────────────────────────────────────────
// Initial theme is already applied pre-paint by an inline <script> in
// <head> (avoids a flash of the wrong theme) — this just handles the
// button click and persists the explicit choice.
function toggleTheme() {
    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    if (isLight) {
        document.documentElement.removeAttribute('data-theme');
        localStorage.setItem('mediastat_theme', 'dark');
    } else {
        document.documentElement.setAttribute('data-theme', 'light');
        localStorage.setItem('mediastat_theme', 'light');
    }
}

// ── Ingress shim ─────────────────────────────────────────────
// When running behind HA ingress all absolute paths must be prefixed.
if (BASE_PATH) {
    // Patch fetch — skip URLs already prefixed (e.g. HTMX calling fetch after configRequest)
    const _fetch = window.fetch;
    window.fetch = (url, opts) => {
        if (typeof url === 'string' && url.startsWith('/') && !url.startsWith(BASE_PATH)) url = BASE_PATH + url;
        return _fetch(url, opts);
    };
    // Patch EventSource
    const _ES = window.EventSource;
    window.EventSource = function(url, cfg) {
        if (typeof url === 'string' && url.startsWith('/') && !url.startsWith(BASE_PATH)) url = BASE_PATH + url;
        return new _ES(url, cfg);
    };
    Object.assign(window.EventSource, { CONNECTING: 0, OPEN: 1, CLOSED: 2 });
    // Patch HTMX requests (sets path before fetch is called, so fetch guard above prevents double-prefix)
    document.addEventListener('htmx:configRequest', evt => {
        if (evt.detail.path.startsWith('/') && !evt.detail.path.startsWith(BASE_PATH))
            evt.detail.path = BASE_PATH + evt.detail.path;
    });
}

// ── Utility ───────────────────────────────────────────────────
function escHtml(s) {
    return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}

function fmtSize(bytes) {
    if (bytes == null) return '—';
    if (!bytes) return '0 B';
    const units = ['B','KB','MB','GB','TB'];
    let i = 0;
    while (bytes >= 1024 && i < units.length - 1) { bytes /= 1024; i++; }
    return `${bytes.toFixed(i ? 1 : 0)} ${units[i]}`;
}

// Returns `empty` (default null) for a missing/zero duration so callers can
// decide whether to omit the figure or show a placeholder.
function fmtDuration(secs, empty = null) {
    if (!secs) return empty;
    const h = Math.floor(secs / 3600);
    const m = Math.floor((secs % 3600) / 60);
    const s = Math.floor(secs % 60);
    if (h) return `${h}h ${m}m`;
    if (m) return `${m}m ${s}s`;
    return `${s}s`;
}

// ── Toast ────────────────────────────────────────────────────
// `html` is inserted as markup — escape anything user/file-derived with
// escHtml() first.
function showToast(html, type = 'info', duration = 4000) {
    let t = document.getElementById('_toast');
    if (!t) {
        t = document.createElement('div');
        t.id = '_toast';
        t.className = 'toast';
        document.body.appendChild(t);
    }
    t.className = 'toast ' + type;
    t.innerHTML = html;
    requestAnimationFrame(() => t.classList.add('show'));
    clearTimeout(t._timer);
    t._timer = setTimeout(() => t.classList.remove('show'), duration);
}

// Token-authenticated JSON POST. Resolves to the parsed body, or null after
// showing an error toast (server `detail` when there is one).
async function apiPost(url, body) {
    let resp;
    try {
        resp = await fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
            body: JSON.stringify(body || {}),
        });
    } catch (e) {
        showToast('Request failed: ' + escHtml(e.message), 'error');
        return null;
    }
    if (!resp.ok) {
        let detail = '';
        try { detail = (await resp.json()).detail; } catch (e) { /* non-JSON error body */ }
        showToast('Failed: ' + escHtml(detail || resp.status), 'error', 6000);
        return null;
    }
    return resp.json();
}

// ── Live encode job-count badge + progress fill on the Jobs nav link ───
(function () {
    const badge = document.getElementById('encode-jobs-count');
    const dot   = document.getElementById('encode-jobs-dot');
    const fill  = document.getElementById('encode-jobs-fill');
    if (!badge || !dot || !fill) return;
    const refresh = () => {
        fetch('/encode/active-count')
            .then(r => r.ok ? r.json() : { count: 0, avg_progress: null })
            .then(d => {
                if (d.count > 0) {
                    badge.textContent = d.count;
                    badge.style.display = '';
                    dot.style.display = 'inline-block';
                    fill.style.width = (d.avg_progress != null ? d.avg_progress : 0) + '%';
                } else {
                    badge.style.display = 'none';
                    dot.style.display = 'none';
                    fill.style.width = '0%';
                }
            })
            .catch(() => {});
    };
    refresh();
    setInterval(refresh, 5000);
})();
