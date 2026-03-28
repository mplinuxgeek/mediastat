
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

    const _encodingPaths = new Set();

    function _markEncodingRows() {
        document.querySelectorAll('.file-entry[data-path]').forEach(row => {
            row.classList.toggle('encoding', _encodingPaths.has(row.dataset.path));
        });
    }

    // ── Encode modal ─────────────────────────────────────────────
    const _ENCODE_PRESETS = {
        fast:     { qp: 22, preset: 'speed',    denoise: '', crop: false },
        balanced: { qp: 20, preset: 'balanced', denoise: '', crop: true  },
        quality:  { qp: 18, preset: 'quality',  denoise: '', crop: true  },
        archive:  { qp: 16, preset: 'archive',  denoise: '', crop: true  },
    };
    const _ENCODE_PRESET_LABELS = { fast: 'Fast', balanced: 'Balanced', quality: 'Quality', archive: 'Archive' };

    function openEncodeModal(btn) {
        const entry = btn.closest('.file-entry');
        const path  = entry.dataset.path;
        const name  = entry.querySelector('.file-stem').textContent.trim();
        document.getElementById('encode-modal-name').textContent = name;
        document.getElementById('encode-file-path').value = path;
        applyEncodePreset('quality');
        document.getElementById('encode-modal').style.display = 'flex';
    }
    function closeEncodeModal() {
        document.getElementById('encode-modal').style.display = 'none';
        // Restore original onclick if it was overridden by batch mode
        const btn = document.querySelector('#encode-modal .btn-primary');
        if (btn && btn._originalOnclick) { btn.onclick = btn._originalOnclick; btn._originalOnclick = null; }
    }
    function applyEncodePreset(name) {
        const p = _ENCODE_PRESETS[name];
        if (!p) return;
        document.getElementById('enc-qp').value        = p.qp;
        document.getElementById('enc-preset').value    = p.preset;
        document.getElementById('enc-denoise').value   = p.denoise;
        document.getElementById('enc-crop').checked    = p.crop;
        document.querySelectorAll('.encode-preset-btn').forEach(b =>
            b.classList.toggle('active', b.textContent === (_ENCODE_PRESET_LABELS[name] || name)));
    }
    async function startEncode() {
        const path = document.getElementById('encode-file-path').value;
        const config = {
            qp:      parseInt(document.getElementById('enc-qp').value, 10),
            preset:  document.getElementById('enc-preset').value,
            codec:   document.getElementById('enc-codec').value,
            gpu:     document.getElementById('enc-gpu').value,
            format:  document.getElementById('enc-format').value,
            denoise: document.getElementById('enc-denoise').value || null,
            crop:    document.getElementById('enc-crop').checked,
            lang:    document.getElementById('enc-lang').value.trim().toLowerCase() || 'eng',
            width:   document.getElementById('enc-width').value ? parseInt(document.getElementById('enc-width').value, 10) : null,
        };
        const btn = document.querySelector('#encode-modal .btn-primary');
        btn.textContent = 'Starting…';
        btn.disabled = true;
        try {
            const resp = await fetch('/encode?path=' + encodeURIComponent(path), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
                body: JSON.stringify(config),
            });
            if (!resp.ok) {
                const txt = await resp.text();
                showToast('Encode failed: ' + escHtml(txt), 'error');
                return;
            }
            _encodingPaths.add(path);
            _markEncodingRows();
            closeEncodeModal();
            showToast('Encode started · <a href="' + BASE_PATH + '/encode">View progress →</a>', 'success', 6000);
        } catch (e) {
            showToast('Error: ' + escHtml(e.message), 'error');
        } finally {
            btn.innerHTML = '⚙ Start Encode <span style="font-size:16px;line-height:1">›</span>';
            btn.disabled = false;
        }
    }

    // ── Batch encode ─────────────────────────────────────────────
    let _batchMode = false;

    function toggleBatchMode() {
        _batchMode = !_batchMode;
        document.body.classList.toggle('batch-mode', _batchMode);
        const btn = document.getElementById('batch-toggle-btn');
        btn.classList.toggle('batch-active', _batchMode);
        btn.textContent = _batchMode ? '✓ selecting' : '☐ select';
        if (!_batchMode) {
            document.querySelectorAll('.batch-cb:checked').forEach(cb => { cb.checked = false; });
            updateBatchFab();
        }
    }

    function updateBatchFab() {
        const count = document.querySelectorAll('.batch-cb:checked').length;
        document.getElementById('batch-fab-count').textContent = count;
        document.getElementById('batch-delete-fab-count').textContent = count;
        document.getElementById('batch-fab').classList.toggle('visible', count > 0);
        document.getElementById('batch-delete-fab').classList.toggle('visible', count > 0);
        document.getElementById('batch-move-fab').classList.toggle('visible', count > 0);
    }

    async function startBatchEncode() {
        const checked = [...document.querySelectorAll('.batch-cb:checked')];
        if (!checked.length) return;
        // Open encode modal pre-filled with count, then confirm
        const modal = document.getElementById('encode-modal');
        document.getElementById('encode-modal-name').textContent = `${checked.length} file${checked.length !== 1 ? 's' : ''}`;
        document.getElementById('encode-file-path').value = '';  // sentinel: batch mode
        applyEncodePreset('quality');
        modal.style.display = 'flex';
        // Override confirm button to do batch
        const btn = modal.querySelector('.btn-primary');
        btn._originalOnclick = btn.onclick;
        btn.onclick = async () => {
            const batchConfig = {
                qp:      parseInt(document.getElementById('enc-qp').value, 10),
                preset:  document.getElementById('enc-preset').value,
                codec:   document.getElementById('enc-codec').value,
            gpu:     document.getElementById('enc-gpu').value,
                format:  document.getElementById('enc-format').value,
                denoise: document.getElementById('enc-denoise').value || null,
                crop:    document.getElementById('enc-crop').checked,
                lang:    document.getElementById('enc-lang').value.trim().toLowerCase() || 'eng',
                width:   document.getElementById('enc-width').value ? parseInt(document.getElementById('enc-width').value, 10) : null,
            };
            btn.textContent = 'Queuing…';
            btn.disabled = true;
            const results = await Promise.all(checked.map(cb => {
                const path = cb.closest('.file-entry')?.dataset.path;
                if (!path) return Promise.resolve(false);
                return fetch('/encode?path=' + encodeURIComponent(path), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
                    body: JSON.stringify(batchConfig),
                }).then(r => r.ok).catch(() => false);
            }));
            const started = results.filter(Boolean).length;
            checked.forEach((cb, i) => {
                if (results[i]) {
                    const path = cb.closest('.file-entry')?.dataset.path;
                    if (path) _encodingPaths.add(path);
                }
            });
            _markEncodingRows();
            closeEncodeModal();
            btn.onclick = btn._originalOnclick;
            toggleBatchMode();
            showToast(`${started} encode job${started !== 1 ? 's' : ''} queued · <a href="${BASE_PATH}/encode">View →</a>`, 'success', 6000);
        };
    }

    // ── Bulk move to folder ───────────────────────────────────────
    let _batchMoveRows = [];

    function startBatchMove() {
        _batchMoveRows = [...document.querySelectorAll('.batch-cb:checked')]
            .map(cb => cb.closest('.file-entry')).filter(Boolean);
        if (!_batchMoveRows.length) return;
        const n = _batchMoveRows.length;
        document.getElementById('batch-move-count').textContent = `${n} file${n !== 1 ? 's' : ''}`;
        document.getElementById('batch-move-input').value = '';
        document.getElementById('batch-move-confirm').disabled = true;
        document.getElementById('batch-move-modal').style.display = 'flex';
        document.getElementById('batch-move-input').focus();
    }

    function closeBatchMoveModal() {
        document.getElementById('batch-move-modal').style.display = 'none';
        _batchMoveRows = [];
    }

    async function confirmBatchMove() {
        const folderName = document.getElementById('batch-move-input').value.trim();
        if (!folderName || !_batchMoveRows.length) return;
        const rows = [..._batchMoveRows];
        closeBatchMoveModal();
        const paths = rows.map(r => r.dataset.path);
        const resp = await fetch('/move-to-folder', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
            body: JSON.stringify({ paths, folder: folderName }),
        });
        const result = await resp.json();
        if (!resp.ok) { showToast('Move failed: ' + (result.detail || resp.status), 'error'); return; }
        const moved = result.moved?.length ?? 0;
        const failed = result.errors?.length ?? 0;
        toggleBatchMode();
        showToast(
            moved ? `${moved} file${moved !== 1 ? 's' : ''} moved to "${escHtml(folderName)}"${failed ? ` · ${failed} failed` : ''}` : 'Move failed',
            moved ? 'success' : 'error', 5000
        );
        setTimeout(() => location.reload(), 800);
    }

    // ── Bulk delete ──────────────────────────────────────────────
    let _batchDeleteRows = [];

    function startBatchDelete() {
        _batchDeleteRows = [...document.querySelectorAll('.batch-cb:checked')]
            .map(cb => cb.closest('.file-entry')).filter(Boolean);
        if (!_batchDeleteRows.length) return;
        document.getElementById('batch-delete-count').textContent =
            `${_batchDeleteRows.length} file${_batchDeleteRows.length !== 1 ? 's' : ''}`;
        document.getElementById('batch-delete-names').innerHTML =
            _batchDeleteRows.map(r => `<div>${escHtml(r.dataset.path.split('/').pop())}</div>`).join('');
        const totalBytes = _batchDeleteRows.reduce((s, r) => s + (parseInt(r.dataset.size) || 0), 0);
        const _units = ['B','KB','MB','GB','TB']; let _b = totalBytes, _i = 0;
        while (_b >= 1024 && _i < _units.length - 1) { _b /= 1024; _i++; }
        document.getElementById('batch-delete-size').textContent = `${_b.toFixed(_i ? 1 : 0)} ${_units[_i]}`;
        document.getElementById('batch-delete-input').value = '';
        document.getElementById('batch-delete-confirm').disabled = true;
        document.getElementById('batch-delete-modal').style.display = 'flex';
        document.getElementById('batch-delete-input').focus();
    }

    function closeBatchDeleteModal() {
        document.getElementById('batch-delete-modal').style.display = 'none';
        document.getElementById('batch-delete-input').value = '';
        _batchDeleteRows = [];
    }

    async function confirmBatchDelete() {
        if (!_batchDeleteRows.length) return;
        const rows = [..._batchDeleteRows];
        closeBatchDeleteModal();
        const results = await Promise.all(rows.map(row =>
            fetch('/file?path=' + encodeURIComponent(row.dataset.path), {
                method: 'DELETE',
                headers: { 'X-Delete-Token': DELETE_TOKEN },
            }).then(r => r.ok).catch(() => false)
        ));
        results.forEach((ok, i) => { if (ok) rows[i].remove(); });
        const deleted = results.filter(Boolean).length;
        const failed  = results.length - deleted;
        toggleBatchMode();
        showToast(
            deleted ? `${deleted} file${deleted !== 1 ? 's' : ''} deleted${failed ? ` · ${failed} failed` : ''}` : `Delete failed`,
            deleted ? 'success' : 'error', 5000
        );
    }

    // ── Folder encode ────────────────────────────────────────────
    let _folderEncodePath = null;

    function openFolderEncodeModal(event, btn) {
        event.stopPropagation();
        event.preventDefault();
        const entry = btn.closest('.dir-entry');
        const rawPath = entry ? decodeURIComponent(entry.dataset.dirPath || '') : '';
        if (!rawPath) return;
        _folderEncodePath = rawPath;
        const dirName = entry.querySelector('.dir-name')?.textContent || rawPath.split('/').pop() + '/';
        const modal = document.getElementById('encode-modal');
        document.getElementById('encode-modal-name').textContent = dirName + ' (recursive)';
        document.getElementById('encode-file-path').value = '';
        applyEncodePreset('quality');
        modal.style.display = 'flex';
        const confirmBtn = modal.querySelector('.btn-primary');
        confirmBtn._originalOnclick = confirmBtn.onclick;
        confirmBtn.onclick = async () => {
            const config = {
                qp:      parseInt(document.getElementById('enc-qp').value, 10),
                preset:  document.getElementById('enc-preset').value,
                codec:   document.getElementById('enc-codec').value,
            gpu:     document.getElementById('enc-gpu').value,
                format:  document.getElementById('enc-format').value,
                denoise: document.getElementById('enc-denoise').value || null,
                crop:    document.getElementById('enc-crop').checked,
                lang:    document.getElementById('enc-lang').value.trim().toLowerCase() || 'eng',
                width:   document.getElementById('enc-width').value ? parseInt(document.getElementById('enc-width').value, 10) : null,
            };
            confirmBtn.textContent = 'Queuing…';
            confirmBtn.disabled = true;
            try {
                const resp = await fetch('/encode/folder?path=' + encodeURIComponent(_folderEncodePath), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
                    body: JSON.stringify(config),
                });
                if (!resp.ok) throw new Error(await resp.text());
                const data = await resp.json();
                closeEncodeModal();
                showToast(`${data.queued} job${data.queued !== 1 ? 's' : ''} queued (${data.total} files found) · <a href="${BASE_PATH}/encode">View →</a>`, 'success', 7000);
            } catch (e) {
                showToast('Error: ' + escHtml(e.message), 'error');
                confirmBtn.textContent = '⚙ Start Encode ›';
                confirmBtn.disabled = false;
            }
        };
    }

    // ── Keyboard navigation ───────────────────────────────────────
    let _kbdSelected = null;

    function _kbdEntries() {
        return [...document.querySelectorAll('.files-table > .file-entry')]
            .filter(e => e.style.display !== 'none');
    }

    function _kbdSelect(entry) {
        if (_kbdSelected) _kbdSelected.classList.remove('kbd-selected');
        _kbdSelected = entry;
        if (entry) {
            entry.classList.add('kbd-selected');
            entry.scrollIntoView({ block: 'nearest' });
        }
    }

    document.addEventListener('keydown', e => {
        // Don't fire when typing in an input
        if (['INPUT','TEXTAREA','SELECT'].includes(e.target.tagName)) return;
        // Close modals on Escape
        if (e.key === 'Escape') {
            const modals = ['player-modal','search-settings-modal','rename-modal',
                'delete-modal','encode-modal','export-modal','dupes-modal',
                'health-modal'];
            for (const id of modals) {
                const el = document.getElementById(id);
                if (el && el.style.display !== 'none') { el.style.display = 'none'; return; }
            }
            if (_batchMode) { toggleBatchMode(); return; }
        }
        if (e.key === 'j' || e.key === 'ArrowDown') {
            e.preventDefault();
            const entries = _kbdEntries();
            if (!entries.length) return;
            const idx = _kbdSelected ? entries.indexOf(_kbdSelected) : -1;
            _kbdSelect(entries[Math.min(idx + 1, entries.length - 1)]);
        } else if (e.key === 'k' || e.key === 'ArrowUp') {
            e.preventDefault();
            const entries = _kbdEntries();
            if (!entries.length) return;
            const idx = _kbdSelected ? entries.indexOf(_kbdSelected) : entries.length;
            _kbdSelect(entries[Math.max(idx - 1, 0)]);
        } else if (e.key === 'Enter' && _kbdSelected) {
            e.preventDefault();
            _kbdSelected.querySelector('.play-btn')?.click();
        } else if (e.key === 'r' && _kbdSelected) {
            e.preventDefault();
            _kbdSelected.querySelector('.rename-btn')?.click();
        } else if (e.key === 'd' && _kbdSelected) {
            e.preventDefault();
            _kbdSelected.querySelector('.delete-btn')?.click();
        } else if (e.key === 'e' && _kbdSelected) {
            e.preventDefault();
            _kbdSelected.querySelector('.encode-btn')?.click();
        }
    });


    // ── IMDB ──────────────────────────────────────────────────────
    let _imdbCurrentRow = null;
    let _imdbSelectedResult = null;
    let _imdbSearchTimer = null;
    let _imdbSearchSource = 'imdb';   // 'imdb' | 'tmdb'
    let _tmdbConfigured = false;

    // Fetch TMDB status once and store
    fetch(BASE_PATH + '/tmdb/status').then(r => r.json()).then(d => {
        _tmdbConfigured = d.configured;
        // Show/hide notes in management modal when it opens
        const note = document.getElementById('imdb-tmdb-note');
        const missing = document.getElementById('imdb-tmdb-missing-note');
        if (note)    note.style.display    = _tmdbConfigured ? '' : 'none';
        if (missing) missing.style.display = _tmdbConfigured ? 'none' : '';
    }).catch(() => {});

    function _setSearchSource(src) {
        _imdbSearchSource = src;
        const toggle = document.getElementById('imdb-source-toggle');
        const label  = document.getElementById('imdb-source-label');
        if (toggle) {
            toggle.querySelectorAll('.btn').forEach(b => b.classList.remove('active'));
            const active = document.getElementById('src-btn-' + src);
            if (active) active.classList.add('active');
        }
        if (label) label.textContent = src === 'tmdb' ? 'TMDB' : 'IMDb';
        // Re-search with new source if there's already a query
        const q = document.getElementById('imdb-q')?.value.trim();
        if (q) imdbSearch();
    }

    // Strip edition/version/cut suffixes so "Deadpool 2 The Super Duper Cut (2018)"
    // and "Terminator 2 Judgment Day - Theatrical Cut (1991)" search correctly.
    const _EDITION_RE = /\s*(?:[-–—]\s*)?(?:the\s+)?(?:super\s+duper\s+cut|director'?s?\s+cut|theatrical\s+cut|extended\s+(?:cut|edition)|unrated\s+(?:cut|edition)|ultimate\s+(?:cut|edition)|special\s+edition|anniversary\s+edition|final\s+cut|collector'?s?\s+edition|redux\b|remastered\b)\s*$/i;
    function _cleanSearchTitle(title) { return title.replace(_EDITION_RE, '').trim(); }

    function parseMediaFilename(name) {
        const stem = name.replace(/\.[^.]+$/, '');
        let m = stem.match(/^(.+?)\s*\((\d{4})\)/);
        if (m) return { title: m[1].trim(), year: parseInt(m[2]) };
        m = stem.match(/^(.+?)\.((?:19|20)\d{2})(?:\.|$)/);
        if (m) return { title: m[1].replace(/\./g, ' ').trim(), year: parseInt(m[2]) };
        m = stem.match(/\b((?:19|20)\d{2})\b/);
        if (m) return { title: stem.slice(0, m.index).trim(), year: parseInt(m[1]) };
        return { title: stem, year: null };
    }

    // ── IMDb tag click handler ────────────────────────────────────
    function _imdbTagClick(tagEl) {
        const row = tagEl.closest('.file-entry');
        if (tagEl.classList.contains('matched')) {
            _openImdbPopup(tagEl, row);
        } else {
            _openImdbMatchModal(row);
        }
    }

    function _openImdbPopup(tagEl, row) {
        closeImdbPopup();
        const popup = document.getElementById('imdb-popup');
        const tconst      = row.dataset.imdbTconst || '';
        const title       = row.dataset.imdbTitle  || '';
        const origTitle   = row.dataset.imdbOriginalTitle || '';
        const releaseDate = row.dataset.imdbReleaseDate || '';
        const year        = releaseDate || row.dataset.imdbYear || '';
        const genres      = (row.dataset.imdbGenres || '').replace(/,/g, ', ');
        const runtime     = row.dataset.imdbRuntime ? `${row.dataset.imdbRuntime} min` : '';
        const cast        = row.dataset.imdbCast   || '';
        const source      = row.dataset.imdbSource || 'imdb';
        const meta        = [year, runtime, genres].filter(Boolean).join(' · ');
        const isTmdb      = source === 'tmdb';
        const externalLink = isTmdb
            ? ''
            : `<a class="btn" href="https://www.imdb.com/title/${escHtml(tconst)}/" target="_blank" rel="noopener">IMDb ↗</a>`;

        popup.innerHTML = `
            <div class="imdb-popup-title">${escHtml(title)}</div>
            ${origTitle && origTitle !== title ? `<div class="imdb-popup-original">${escHtml(origTitle)}</div>` : ''}
            ${meta ? `<div class="imdb-popup-meta">${escHtml(meta)}</div>` : ''}
            ${cast ? `<div class="imdb-popup-cast">★ <span>${escHtml(cast)}</span></div>` : ''}
            <div class="imdb-popup-actions">
                <span class="imdb-popup-tconst">${escHtml(isTmdb ? 'TMDB' : tconst)}</span>
                ${externalLink}
                <button class="btn" id="_imdb-popup-edit">Edit</button>
                <button class="btn btn-danger" id="_imdb-popup-remove">Remove</button>
            </div>`;

        popup.querySelector('#_imdb-popup-edit').addEventListener('click', e => {
            e.stopPropagation();
            closeImdbPopup();
            _openImdbMatchModal(row);
        });
        popup.querySelector('#_imdb-popup-remove').addEventListener('click', async e => {
            e.stopPropagation();
            closeImdbPopup();
            await fetch(BASE_PATH + '/imdb/match?' + new URLSearchParams({ path: row.dataset.path }), { method: 'DELETE' });
            _imdbClearBadge(row);
            showToast('IMDb match removed', 'success', 2000);
        });

        // Position below the tag, clamp to viewport
        const rect = tagEl.getBoundingClientRect();
        popup.style.display = 'block';
        const pw = popup.offsetWidth, ph = popup.offsetHeight;
        let top  = rect.bottom + 6;
        let left = rect.left;
        if (left + pw > window.innerWidth - 8)  left = window.innerWidth - pw - 8;
        if (top  + ph > window.innerHeight - 8) top  = rect.top - ph - 6;
        popup.style.top  = top  + 'px';
        popup.style.left = left + 'px';

        window._imdbPopupRow = row;
        setTimeout(() => document.addEventListener('click', _imdbPopupOutside, { once: true }), 0);
    }

    function _imdbPopupOutside(e) {
        if (!document.getElementById('imdb-popup').contains(e.target)) closeImdbPopup();
    }

    function closeImdbPopup() {
        document.getElementById('imdb-popup').style.display = 'none';
        document.removeEventListener('click', _imdbPopupOutside);
        window._imdbPopupRow = null;
    }

    async function openImdbModal(el) {
        // Legacy entry point — delegate to the unified handler
        const row = el?.closest?.('.file-entry') || _imdbCurrentRow;
        if (row) _openImdbMatchModal(row);
    }

    async function _openImdbMatchModal(row) {
        event?.stopPropagation?.();
        closeImdbPopup();
        _imdbCurrentRow = row;
        _imdbSelectedResult = null;
        const name = row.dataset.path.split('/').pop();
        const parsed = parseMediaFilename(name);
        document.getElementById('imdb-filename-hint').textContent = `File: ${name}`;
        document.getElementById('imdb-q').value = _cleanSearchTitle(parsed.title || '');
        document.getElementById('imdb-year').value = parsed.year || '';
        document.getElementById('imdb-result-list').innerHTML = '';
        document.getElementById('imdb-match-btn').disabled = true;
        const hasMatch = !!row.dataset.imdbTitle;
        document.getElementById('imdb-unmatch-btn').style.display = hasMatch ? '' : 'none';
        document.getElementById('imdb-setdate-btn').style.display =
            (hasMatch && (row.dataset.imdbYear || row.dataset.imdbReleaseDate)) ? '' : 'none';
        // Show source toggle if TMDB is configured
        const toggle = document.getElementById('imdb-source-toggle');
        const label  = document.getElementById('imdb-source-label');
        if (toggle) toggle.style.display = _tmdbConfigured ? 'inline-flex' : 'none';
        if (label)  label.style.display  = _tmdbConfigured ? 'none' : '';
        document.getElementById('imdb-search-modal').style.display = 'flex';
        if (parsed.title) await imdbSearch();
    }

    function closeImdbModal() {
        document.getElementById('imdb-search-modal').style.display = 'none';
        _imdbCurrentRow = null;
        _imdbSelectedResult = null;
    }

    function imdbSearchDebounced() {
        clearTimeout(_imdbSearchTimer);
        _imdbSearchTimer = setTimeout(imdbSearch, 400);
    }

    async function imdbSearch() {
        const q = document.getElementById('imdb-q').value.trim();
        const year = document.getElementById('imdb-year').value.trim();
        if (!q) return;
        const list = document.getElementById('imdb-result-list');
        list.innerHTML = '<div style="color:var(--muted);font-size:var(--fs-sm);padding:8px">Searching…</div>';
        _imdbSelectedResult = null;
        document.getElementById('imdb-match-btn').disabled = true;
        const isTmdb = _imdbSearchSource === 'tmdb';
        let url = BASE_PATH + (isTmdb ? '/tmdb/search' : '/imdb/search') + '?q=' + encodeURIComponent(q);
        if (year) url += '&year=' + encodeURIComponent(year);
        try {
            const resp = await fetch(url);
            if (!resp.ok) {
                const err = await resp.json().catch(() => ({}));
                list.innerHTML = `<div style="color:var(--red);font-size:var(--fs-sm);padding:8px">${escHtml(err.detail || 'Search failed')}</div>`;
                return;
            }
            const results = await resp.json();
            if (!results.length) {
                list.innerHTML = '<div style="color:var(--muted);font-size:var(--fs-sm);padding:8px">No results found</div>';
                return;
            }
            const normTitle = s => s.toLowerCase().replace(/&/g, ' and ').replace(/['\/\u2019]/g, '').replace(/[:",.?!\-\u2013\u2014]/g, ' ').replace(/\s+/g, ' ').trim();
            const exactTitle = normTitle(q);
            const exactYear  = year ? parseInt(year) : null;
            list.innerHTML = results.map((r, i) => {
                const isExact = (normTitle(r.primary_title) === exactTitle || (r.original_title && normTitle(r.original_title) === exactTitle)) && (!exactYear || r.start_year === exactYear);
                const mins = r.runtime_minutes ? `${r.runtime_minutes} min` : '';
                const genres = r.genres ? r.genres.replace(/,/g, ', ') : '';
                const meta = [mins, genres].filter(Boolean).join(' · ');
                // Display date: full YYYY-MM-DD for TMDB, year-only for IMDb
                const dateLabel = r.release_date ? r.release_date : (r.start_year || '—');
                const ratingStr = r.average_rating != null ? `★ ${r.average_rating.toFixed(1)}` : '';
                const sourceTag = r.source === 'tmdb'
                    ? `<span style="opacity:0.5;font-size:0.85em">${escHtml(r.media_type === 'tv' ? 'TMDB TV' : 'TMDB')} · ${escHtml(String(r.tconst))}</span>`
                    : `<span style="opacity:0.5"><a href="https://www.imdb.com/title/${escHtml(r.tconst)}/" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--imdb-gold)">${escHtml(r.tconst)} ↗</a></span>`;
                return `<div class="imdb-result" onclick="imdbSelectResult(this,${i})" data-idx="${i}">
                    <span class="imdb-result-year">${escHtml(String(dateLabel))}${ratingStr ? `<br><span class="imdb-result-rating">${escHtml(ratingStr)}</span>` : ''}</span>
                    <div class="imdb-result-info">
                        <div class="imdb-result-title">${escHtml(r.primary_title)}${isExact ? '<span class="imdb-exact-badge">✓ exact</span>' : ''}</div>
                        ${r.original_title && r.original_title !== r.primary_title
                            ? `<div class="imdb-result-meta" style="font-style:italic">${escHtml(r.original_title)}</div>` : ''}
                        ${meta ? `<div class="imdb-result-meta">${escHtml(meta)}</div>` : ''}
                        ${r.cast_names ? `<div class="imdb-result-meta" style="color:var(--text);opacity:0.75">★ ${escHtml(r.cast_names)}</div>` : ''}
                        <div class="imdb-result-meta">${sourceTag}</div>
                    </div>
                </div>`;
            }).join('');
            // Store results for later retrieval
            list._results = results;
            // Auto-select first exact match if any
            const exactIdx = results.findIndex(r =>
                normTitle(r.primary_title) === exactTitle && (!exactYear || r.start_year === exactYear));
            if (exactIdx >= 0) imdbSelectResult(list.querySelectorAll('.imdb-result')[exactIdx], exactIdx);
        } catch (e) {
            list.innerHTML = `<div style="color:var(--red);font-size:var(--fs-sm);padding:8px">Error: ${escHtml(e.message)}</div>`;
        }
    }

    function imdbSelectResult(el, idx) {
        document.querySelectorAll('.imdb-result.selected').forEach(r => r.classList.remove('selected'));
        el.classList.add('selected');
        const list = document.getElementById('imdb-result-list');
        _imdbSelectedResult = list._results?.[idx] ?? null;
        document.getElementById('imdb-match-btn').disabled = !_imdbSelectedResult;
    }

    async function imdbConfirmMatch() {
        if (!_imdbSelectedResult || !_imdbCurrentRow) return;
        const path = _imdbCurrentRow.dataset.path;
        const r = _imdbSelectedResult;
        const embedMeta = document.getElementById('imdb-set-metadata')?.checked ?? true;
        const setDates  = document.getElementById('imdb-set-dates')?.checked ?? true;
        await fetch(BASE_PATH + '/imdb/match', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                path, tconst: r.tconst, primary_title: r.primary_title,
                original_title: r.original_title, start_year: r.start_year,
                genres: r.genres, runtime_minutes: r.runtime_minutes,
                embed_meta: embedMeta,
                source: r.source || 'imdb',
                release_date: r.release_date || null,
                average_rating: r.average_rating ?? null,
            }),
        });
        if (setDates && (r.start_year || r.release_date)) {
            await fetch(BASE_PATH + '/imdb/set-release-date', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path }),
            });
        }
        // Add badge to row
        _imdbApplyBadge(_imdbCurrentRow, r);
        closeImdbModal();
        const dateStr = r.release_date || (r.start_year ? String(r.start_year) : '?');
        showToast(`Matched: ${r.primary_title} (${dateStr})`, 'success', 3000);
    }

    async function imdbUnmatch() {
        if (!_imdbCurrentRow) return;
        const path = _imdbCurrentRow.dataset.path;
        await fetch(BASE_PATH + '/imdb/match?' + new URLSearchParams({ path }), { method: 'DELETE' });
        _imdbClearBadge(_imdbCurrentRow);
        closeImdbModal();
        showToast('IMDB match removed', 'success', 2000);
    }

    async function imdbSetDate() {
        if (!_imdbCurrentRow) return;
        const path = _imdbCurrentRow.dataset.path;
        try {
            const resp = await fetch(BASE_PATH + '/imdb/set-release-date', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ path }),
            });
            const data = resp.ok ? await resp.json() : null;
            if (data?.ok) {
                showToast(`File date set to ${data.date}`, 'success', 3000);
            } else {
                showToast('Set date failed', 'error', 3000);
            }
        } catch (e) {
            showToast('Set date error: ' + e.message, 'error', 3000);
        }
    }

    function _imdbApplyBadge(row, info) {
        const tag = row.querySelector('.imdb-tag');
        const isTmdb = (info.source || 'imdb') === 'tmdb';
        if (tag) {
            tag.textContent = isTmdb ? 'TMDB✓' : 'IMDb✓';
            tag.classList.add('matched');
        }
        row.dataset.imdbTitle         = info.primary_title   || '';
        row.dataset.imdbYear          = info.start_year      || '';
        row.dataset.imdbTconst        = info.tconst          || '';
        row.dataset.imdbGenres        = info.genres          || '';
        row.dataset.imdbRuntime       = info.runtime_minutes || '';
        row.dataset.imdbCast          = info.cast_names      || '';
        row.dataset.imdbOriginalTitle = info.original_title  || '';
        row.dataset.imdbSource        = info.source          || 'imdb';
        row.dataset.imdbReleaseDate   = info.release_date    || '';
        const rating = info.average_rating ?? null;
        const ratingBadge = row.querySelector('.imdb-rating-badge');
        if (ratingBadge) {
            if (rating != null) {
                ratingBadge.textContent = '★ ' + rating.toFixed(1);
                ratingBadge.style.display = '';
                row.dataset.imdbRating = rating;
            } else {
                ratingBadge.style.display = 'none';
                delete row.dataset.imdbRating;
            }
        }
    }

    function _imdbClearBadge(row) {
        const tag = row.querySelector('.imdb-tag');
        if (tag) { tag.textContent = 'IMDb'; tag.classList.remove('matched'); }
        const ratingBadge = row.querySelector('.imdb-rating-badge');
        if (ratingBadge) { ratingBadge.style.display = 'none'; ratingBadge.textContent = ''; }
        delete row.dataset.imdbTitle;
        delete row.dataset.imdbYear;
        delete row.dataset.imdbTconst;
        delete row.dataset.imdbGenres;
        delete row.dataset.imdbRuntime;
        delete row.dataset.imdbCast;
        delete row.dataset.imdbOriginalTitle;
        delete row.dataset.imdbSource;
        delete row.dataset.imdbReleaseDate;
        delete row.dataset.imdbRating;
    }

    async function loadImdbBadges(dir) {
        try {
            const resp = await fetch(BASE_PATH + '/imdb/matches?dir=' + encodeURIComponent(dir));
            if (!resp.ok) return;
            const matches = await resp.json();
            for (const [path, info] of Object.entries(matches)) {
                const row = document.querySelector(`.file-entry[data-path="${CSS.escape(path)}"]`);
                if (row) _imdbApplyBadge(row, info);
            }
        } catch (_) { /* non-fatal */ }
    }

    // IMDB management modal
    function openImdbMgmtModal() {
        // Wire up the DB link with the correct BASE_PATH
        const dbLink = document.getElementById('imdb-mgmt-db-link');
        if (dbLink) dbLink.href = BASE_PATH + '/databases';
        // Show/hide TMDB bulk-enrich option
        const tmdbRow = document.getElementById('imdb-bulk-tmdb-row');
        if (tmdbRow) tmdbRow.style.display = _tmdbConfigured ? '' : 'none';
        document.getElementById('imdb-mgmt-modal').style.display = 'flex';
    }

    function closeImdbMgmtModal() {
        document.getElementById('imdb-mgmt-modal').style.display = 'none';
    }

    // ── IMDB auto-match ───────────────────────────────────────────
    let _autoMatchRunning = false;
    let _autoMatchResolve = null; // resolves with tconst string or null (skip)

    async function startAutoMatch() {
        if (_autoMatchRunning) return;

        // Pre-fetch already-matched paths from the DB so we don't rely solely on
        // DOM badges (which may not have loaded yet after a page refresh).
        const matchedPaths = new Set();
        const dirs = [...new Set([...document.querySelectorAll('.file-entry')].map(r => {
            const p = r.dataset.path || '';
            return p.substring(0, p.lastIndexOf('/'));
        }).filter(Boolean))];
        for (const dir of dirs) {
            try {
                const resp = await fetch(BASE_PATH + '/imdb/matches?dir=' + encodeURIComponent(dir));
                if (!resp.ok) continue;
                const matches = await resp.json();
                for (const [mPath, info] of Object.entries(matches)) {
                    matchedPaths.add(mPath);
                    // Also apply badge now if not already present
                    try {
                        const row = document.querySelector(`.file-entry[data-path="${CSS.escape(mPath)}"]`);
                        if (row && !row.dataset.imdbTitle) _imdbApplyBadge(row, info);
                    } catch (_) { /* badge apply failure must not abort matchedPaths population */ }
                }
            } catch (_) { /* network error for this dir, continue to next */ }
        }

        const forceRescan = document.getElementById('imdb-force-rescan').checked;
        const enrichTmdb  = _tmdbConfigured && (document.getElementById('imdb-bulk-use-tmdb')?.checked ?? false);
        const rows = [...document.querySelectorAll('.file-entry')]
            .filter(r => forceRescan || (!r.dataset.imdbTitle && !matchedPaths.has(r.dataset.path)));
        if (!rows.length) {
            showToast('All visible files already matched', 'success', 3000);
            return;
        }

        _autoMatchRunning = true;
        document.getElementById('imdb-auto-btn').disabled = true;
        document.getElementById('imdb-auto-close-btn').disabled = true;
        document.getElementById('imdb-auto-panel').style.display = '';
        document.getElementById('imdb-auto-prompt').style.display = 'none';

        let autoC = 0, manualC = 0, skippedC = 0, noneC = 0;
        const total = rows.length;
        const enrichQueue = [];

        const fillEl = document.getElementById('imdb-auto-fill');
        const pctEl  = document.getElementById('imdb-auto-pct');
        const msgEl  = document.getElementById('imdb-auto-msg');

        // ── Phase 1: parallel searches (32 concurrent) ───────────────
        const searchData = new Array(total).fill(null);
        let nextIdx = 0, doneCount = 0;

        async function _fetchOne() {
            while (nextIdx < total) {
                if (!_autoMatchRunning) return;
                const i = nextIdx++;
                const row = rows[i];
                const name = row.dataset.path.split('/').pop();
                const parsed = parseMediaFilename(name);
                parsed.searchTitle = _cleanSearchTitle(parsed.title);
                const rawDur = parseFloat(row.dataset.duration || 0);
                const fileRuntimeMin = rawDur > 0 ? Math.round(rawDur / 60) : null;
                let url = BASE_PATH + '/imdb/search?q=' + encodeURIComponent(parsed.searchTitle);
                if (parsed.year) url += '&year=' + encodeURIComponent(parsed.year);
                if (fileRuntimeMin) url += '&runtime=' + fileRuntimeMin;
                try {
                    const resp = await fetch(url);
                    searchData[i] = { parsed, fileRuntimeMin, results: resp.ok ? await resp.json() : [] };
                } catch (_) {
                    searchData[i] = { parsed, fileRuntimeMin, results: null };
                }
                doneCount++;
                fillEl.style.width = Math.round((doneCount / total) * 50) + '%';
                pctEl.textContent  = Math.round((doneCount / total) * 50) + '%';
                msgEl.textContent  = `Searching ${doneCount} / ${total}`;
            }
        }
        await Promise.all(Array.from({ length: Math.min(32, total) }, _fetchOne));

        // ── Phase 2: sort into auto vs prompt ────────────────────────
        const _norm = s => s.toLowerCase().replace(/&/g, ' and ').replace(/['\/\u2019]/g, '').replace(/[:",.?!\-\u2013\u2014]/g, ' ').replace(/\s+/g, ' ').trim();
        const skipManual = document.getElementById('imdb-skip-manual').checked;
        const autoItems   = [];  // [{row, result}]
        const promptItems = [];  // [{row, name, exact, results, fileRuntimeMin}]

        for (let i = 0; i < total; i++) {
            const { parsed, fileRuntimeMin, results } = searchData[i];
            const row = rows[i];
            if (!results)        { skippedC++; continue; }
            if (!results.length) { noneC++;    continue; }

            const _normQ = _norm(parsed.searchTitle || parsed.title);
            const exact = results.filter(r =>
                (_norm(r.primary_title) === _normQ || (r.original_title && _norm(r.original_title) === _normQ)) &&
                (!parsed.year || r.start_year === parsed.year)
            );
            // Prefer feature-length types over video/short when multiple exact matches
            const _PREF_TYPES = new Set(['movie', 'tvMovie', 'tvMiniSeries', 'tvSpecial']);
            const exactPref = exact.filter(r => _PREF_TYPES.has(r.title_type));
            const exactPool = exactPref.length > 0 ? exactPref : exact;
            let autoCandidate = null;
            if (exactPool.length === 1) {
                autoCandidate = exactPool[0];
            } else if (exactPool.length > 1 && fileRuntimeMin) {
                const withRuntime = exactPool.filter(r => r.runtime_minutes != null);
                if (withRuntime.length > 0) {
                    const minDiff = Math.min(...withRuntime.map(r => Math.abs(r.runtime_minutes - fileRuntimeMin)));
                    const closest = withRuntime.filter(r => Math.abs(r.runtime_minutes - fileRuntimeMin) === minDiff);
                    if (closest.length === 1 && minDiff <= 10) autoCandidate = closest[0];
                }
            }
            if (autoCandidate) {
                autoItems.push({ row, result: autoCandidate });
            } else if (skipManual) {
                skippedC++;
            } else {
                promptItems.push({
                    row, name: row.dataset.path.split('/').pop(),
                    parsed, fileRuntimeMin, results, exact,
                });
            }
        }

        // ── Phase 2b: parallel auto-matches (32 concurrent) ──────────
        const autoTotal   = autoItems.length;
        const promptTotal = promptItems.length;
        let nextAuto = 0, autoDone = 0;

        msgEl.textContent = `Matching ${autoTotal} files…`;

        async function _matchOne() {
            while (nextAuto < autoTotal) {
                if (!_autoMatchRunning) return;
                const i = nextAuto++;
                const { row, result } = autoItems[i];
                await _autoDoMatch(row, result);
                if (enrichTmdb && result.tconst?.startsWith('tt'))
                    enrichQueue.push({ path: row.dataset.path, tconst: result.tconst, row });
                autoDone++;
                autoC++;
                const pct = Math.round(50 + (autoDone / Math.max(autoTotal + promptTotal, 1)) * 50);
                fillEl.style.width = pct + '%';
                pctEl.textContent  = pct + '%';
                msgEl.textContent  = `Matching ${autoDone} / ${autoTotal + promptTotal}`;
                _autoUpdateStats(autoC, manualC, skippedC, noneC);
            }
        }
        if (autoTotal) await Promise.all(Array.from({ length: Math.min(32, autoTotal) }, _matchOne));

        // ── Phase 2c: sequential prompts ─────────────────────────────
        for (let i = 0; i < promptTotal; i++) {
            if (!_autoMatchRunning) break;
            const { row, name, parsed, fileRuntimeMin, results, exact } = promptItems[i];
            const pct = Math.round(50 + ((autoTotal + i) / Math.max(autoTotal + promptTotal, 1)) * 50);
            fillEl.style.width = pct + '%';
            pctEl.textContent  = pct + '%';
            msgEl.textContent  = `Reviewing ${i + 1} / ${promptTotal}`;
            _autoUpdateStats(autoC, manualC, skippedC, noneC);

            const reason = exact.length > 1
                ? `${exact.length} exact matches — runtime${fileRuntimeMin ? ' ('+fileRuntimeMin+' min)' : ''} not conclusive, select one or skip`
                : parsed.year ? 'No exact match — closest results shown'
                              : 'No year in filename — select the correct movie or skip';
            const tconst = await _autoPromptUser(name, reason, exact.length ? exact : results);
            if (tconst) {
                const picked = results.find(r => r.tconst === tconst);
                if (picked) {
                    await _autoDoMatch(row, picked);
                    if (enrichTmdb && picked.tconst?.startsWith('tt'))
                        enrichQueue.push({ path: row.dataset.path, tconst: picked.tconst, row });
                    manualC++;
                }
            } else {
                skippedC++;
            }
        }

        fillEl.style.width = '100%';
        pctEl.textContent = '100%';
        msgEl.textContent = 'Done';
        document.getElementById('imdb-auto-prompt').style.display = 'none';
        _autoUpdateStats(autoC, manualC, skippedC, noneC);
        document.getElementById('imdb-auto-btn').disabled = false;
        document.getElementById('imdb-auto-close-btn').disabled = false;
        _autoMatchRunning = false;
        showToast(`Auto-match: ${autoC} auto, ${manualC} manual, ${skippedC} skipped, ${noneC} no result`, 'success', 5000);

        if (enrichQueue.length) {
            const setDates = document.getElementById('imdb-set-dates')?.checked ?? true;
            _runTmdbEnrichments(enrichQueue, setDates);
        }
    }

    async function _runTmdbEnrichments(jobs, setDates) {
        for (const { path, tconst, row } of jobs) {
            try {
                const r = await fetch(BASE_PATH + '/tmdb/enrich', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ path, tconst }),
                });
                if (r.ok) {
                    const d = await r.json();
                    if (d.release_date) {
                        row.dataset.imdbReleaseDate = d.release_date;
                        if (setDates) {
                            await fetch(BASE_PATH + '/imdb/set-release-date', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ path }),
                            });
                        }
                    }
                }
            } catch (_) { /* non-fatal — best effort */ }
            // Small pause between requests to stay within TMDB rate limits
            await new Promise(res => setTimeout(res, 150));
        }
    }

    function _autoUpdateStats(a, m, s, n) {
        document.getElementById('iam-auto').textContent    = a;
        document.getElementById('iam-manual').textContent  = m;
        document.getElementById('iam-skipped').textContent = s;
        document.getElementById('iam-none').textContent    = n;
    }

    async function _autoDoMatch(row, result) {
        const embedMeta = document.getElementById('imdb-set-metadata')?.checked ?? true;
        const setDates  = document.getElementById('imdb-set-dates')?.checked ?? true;
        await fetch(BASE_PATH + '/imdb/match', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                path: row.dataset.path, tconst: result.tconst,
                primary_title: result.primary_title, original_title: result.original_title,
                start_year: result.start_year, genres: result.genres, runtime_minutes: result.runtime_minutes,
                embed_meta: embedMeta,
                source: 'imdb',
                set_dates: setDates && !!result.start_year,
                average_rating: result.average_rating ?? null,
            }),
        });
        _imdbApplyBadge(row, result);
    }

    let _autoSelectedTconst = null;

    function _autoPromptUser(filename, reason, results) {
        return new Promise(resolve => {
            _autoMatchResolve = resolve;
            _autoSelectedTconst = null;

            document.getElementById('imdb-auto-prompt-reason').textContent = reason;
            document.getElementById('imdb-auto-prompt-file').textContent = filename;
            document.getElementById('imdb-auto-match-btn').disabled = true;
            document.getElementById('imdb-auto-search-input').value = '';

            _autoRenderResults(results);
            document.getElementById('imdb-auto-prompt').style.display = '';
        });
    }

    function _autoRenderResults(results) {
        const listEl = document.getElementById('imdb-auto-results');
        listEl._results = results;
        listEl.innerHTML = results.slice(0, 15).map((r, i) => {
            const genres = r.genres ? r.genres.replace(/,/g, ', ') : '';
            const mins   = r.runtime_minutes ? `${r.runtime_minutes} min` : '';
            const meta   = [mins, genres].filter(Boolean).join(' · ');
            const rStr = r.average_rating != null ? `★ ${r.average_rating.toFixed(1)}` : '';
            return `<div class="imdb-result" onclick="_autoSelectResult(this,${i})">
                <span class="imdb-result-year">${r.start_year || '—'}${rStr ? `<br><span class="imdb-result-rating">${escHtml(rStr)}</span>` : ''}</span>
                <div class="imdb-result-info">
                    <div class="imdb-result-title">${escHtml(r.primary_title)}</div>
                    ${meta ? `<div class="imdb-result-meta">${escHtml(meta)}</div>` : ''}
                    ${r.cast_names ? `<div class="imdb-result-meta" style="color:var(--text);opacity:0.75">★ ${escHtml(r.cast_names)}</div>` : ''}
                    <div class="imdb-result-meta" style="opacity:0.5"><a href="https://www.imdb.com/title/${escHtml(r.tconst)}/" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:#f5c518">Open on IMDb ↗</a></div>
                </div>
            </div>`;
        }).join('') || '<div style="color:var(--muted);font-size:var(--fs-sm);padding:4px 0">No results</div>';
        _autoSelectedTconst = null;
        document.getElementById('imdb-auto-match-btn').disabled = true;
    }

    async function _autoManualSearch() {
        const q = document.getElementById('imdb-auto-search-input').value.trim();
        if (!q) return;
        const listEl = document.getElementById('imdb-auto-results');
        listEl.innerHTML = '<div style="color:var(--muted);font-size:var(--fs-sm);padding:4px 0">Searching…</div>';
        try {
            const resp = await fetch(BASE_PATH + '/imdb/search?' + new URLSearchParams({ q }));
            _autoRenderResults(resp.ok ? await resp.json() : []);
        } catch (e) {
            listEl.innerHTML = `<div style="color:var(--red);font-size:var(--fs-sm)">${escHtml(e.message)}</div>`;
        }
    }

    function _autoSelectResult(el, idx) {
        document.querySelectorAll('#imdb-auto-results .imdb-result.selected')
            .forEach(r => r.classList.remove('selected'));
        el.classList.add('selected');
        _autoSelectedTconst = document.getElementById('imdb-auto-results')._results[idx].tconst;
        document.getElementById('imdb-auto-match-btn').disabled = false;
    }

    function _imdbAutoSkip() {
        document.getElementById('imdb-auto-prompt').style.display = 'none';
        if (_autoMatchResolve) { _autoMatchResolve(null); _autoMatchResolve = null; }
    }

    function _imdbAutoConfirm() {
        document.getElementById('imdb-auto-prompt').style.display = 'none';
        if (_autoMatchResolve) { _autoMatchResolve(_autoSelectedTconst); _autoMatchResolve = null; }
    }

    // ── IMDB bulk rename ──────────────────────────────────────────
    async function startImdbBulkRename() {
        const rows = [...document.querySelectorAll('.file-entry')]
            .filter(r => r.dataset.imdbTitle && r.dataset.imdbYear);
        if (!rows.length) {
            showToast('No IMDB-matched files visible to rename', 'success', 3000);
            return;
        }

        document.getElementById('imdb-rename-btn').disabled = true;
        document.getElementById('imdb-auto-close-btn').disabled = true;
        document.getElementById('imdb-rename-panel').style.display = '';

        const fillEl = document.getElementById('imdb-rename-fill');
        const pctEl  = document.getElementById('imdb-rename-pct');
        const msgEl  = document.getElementById('imdb-rename-msg');
        let renamed = 0, alreadyOk = 0, failed = 0;
        const total = rows.length;

        for (let i = 0; i < rows.length; i++) {
            const row = rows[i];
            const path = row.dataset.path;
            const currentName = path.split('/').pop();
            const ext = currentName.includes('.') ? '.' + currentName.split('.').pop() : '';
            const newName = `${_sanitizeFilename(row.dataset.imdbTitle + ' (' + row.dataset.imdbYear + ')')}${ext}`;

            const pct = Math.round((i / total) * 100);
            fillEl.style.width = pct + '%';
            pctEl.textContent = pct + '%';
            msgEl.textContent = `${i + 1} / ${total}`;

            if (newName === currentName) {
                alreadyOk++;
                _updateRenameStats(renamed, alreadyOk, failed);
                continue;
            }

            try {
                const resp = await fetch(
                    BASE_PATH + '/rename?path=' + encodeURIComponent(path) +
                    '&new_name=' + encodeURIComponent(newName),
                    { method: 'POST', headers: { 'X-Delete-Token': DELETE_TOKEN } }
                );
                if (resp.ok) {
                    const data = await resp.json();
                    // Update row in place
                    const newPath = data.path;
                    const stem = newName.replace(/\.[^.]+$/, '');
                    row.dataset.path = newPath;
                    row.dataset.name = newName.toLowerCase();
                    const stemEl = row.querySelector('.file-stem');
                    if (stemEl) stemEl.textContent = stem;
                    renamed++;
                } else {
                    failed++;
                }
            } catch (_) {
                failed++;
            }
            _updateRenameStats(renamed, alreadyOk, failed);
        }

        fillEl.style.width = '100%';
        pctEl.textContent = '100%';
        msgEl.textContent = 'Done';
        document.getElementById('imdb-rename-btn').disabled = false;
        document.getElementById('imdb-auto-close-btn').disabled = false;
        showToast(
            `Bulk rename: ${renamed} renamed, ${alreadyOk} already correct${failed ? `, ${failed} failed` : ''}`,
            'success', 5000
        );
    }

    function _updateRenameStats(renamed, alreadyOk, failed) {
        document.getElementById('irn-renamed').textContent  = renamed;
        document.getElementById('irn-skipped').textContent  = alreadyOk;
        document.getElementById('irn-failed').textContent   = failed;
    }

    // ── IMDB bulk set release dates ───────────────────────────────
    async function startImdbSetDates() {
        const rows = [...document.querySelectorAll('.file-entry')]
            .filter(r => r.dataset.imdbTitle && r.dataset.imdbYear);
        if (!rows.length) {
            showToast('No IMDB-matched files visible', 'info', 3000);
            return;
        }
        document.getElementById('imdb-dates-btn').disabled = true;
        document.getElementById('imdb-auto-close-btn').disabled = true;
        const paths = rows.map(r => r.dataset.path);
        try {
            const resp = await fetch(BASE_PATH + '/imdb/set-release-dates', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ paths }),
            });
            const data = resp.ok ? await resp.json() : null;
            if (data) {
                showToast(`Set dates: ${data.updated} updated${data.errors ? `, ${data.errors} failed` : ''}`, 'success', 4000);
            } else {
                showToast('Set dates failed', 'error', 3000);
            }
        } catch (e) {
            showToast('Set dates error: ' + e.message, 'error', 3000);
        }
        document.getElementById('imdb-dates-btn').disabled = false;
        document.getElementById('imdb-auto-close-btn').disabled = false;
    }

    // ── Health check ──────────────────────────────────────────────
    let _healthSse = null;

    function openHealthModal() {
        document.getElementById('health-modal').style.display = 'flex';
    }
    function closeHealthModal() {
        document.getElementById('health-modal').style.display = 'none';
        if (_healthSse) { _healthSse.close(); _healthSse = null; }
    }
    function runHealthCheck() {
        if (_healthSse) { _healthSse.close(); _healthSse = null; }
        const btn  = document.getElementById('health-rescan-btn');
        const body = document.getElementById('health-body');
        const fill = document.getElementById('health-bar-fill');
        const pct  = document.getElementById('health-pct');
        body.innerHTML = '<span class="health-scanning">Scanning…</span>';
        fill.style.width = '0%';
        fill.classList.remove('done');
        pct.textContent = '0 / 0';
        btn.disabled = true;
        _healthSse = new EventSource('/health-check');
        _healthSse.onmessage = ev => {
            const msg = JSON.parse(ev.data);
            if (msg.type === 'start') {
                pct.textContent = `0 / ${msg.total}`;
            } else if (msg.type === 'progress') {
                const p = msg.total > 0 ? (msg.done / msg.total * 100) : 0;
                fill.style.width = p + '%';
                pct.textContent = `${msg.done} / ${msg.total}`;
            } else if (msg.type === 'done') {
                _healthSse.close(); _healthSse = null;
                btn.disabled = false;
                fill.style.width = '100%';
                fill.classList.add('done');
                pct.textContent = `${msg.scanned} / ${msg.scanned}`;
                if (!msg.issues.length) {
                    body.innerHTML = `<div class="health-ok">✓ All ${msg.scanned} file${msg.scanned !== 1 ? 's' : ''} look healthy.</div>`;
                } else {
                    body.innerHTML = msg.issues.map(iss => {
                        const dir = iss.path ? iss.path.replace(/\/[^/]+$/, '') : '';
                        return `<div class="health-issue-row">
                            <div style="flex:1;min-width:0">
                                <div class="health-issue-name" title="${escHtmlAttr(iss.path || iss.name)}">${escHtml(iss.name)}</div>
                                ${dir ? `<div class="file-dir">${escHtml(dir)}</div>` : ''}
                            </div>
                            <div class="health-issue-tags">${iss.issues.map(i => `<span class="health-issue-tag">${escHtml(i)}</span>`).join('')}</div>
                        </div>`;
                    }).join('');
                }
            }
        };
        _healthSse.onerror = () => {
            if (_healthSse) { _healthSse.close(); _healthSse = null; }
            btn.disabled = false;
            body.innerHTML = '<div class="health-scanning">Connection error. Try again.</div>';
        };
    }

    // ── Utility ───────────────────────────────────────────────────
    function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }
    function escHtmlAttr(s) { return String(s).replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
    function escJs(s) { return String(s).replace(/\\/g, '\\\\').replace(/'/g, "\\'"); }

    // ── Toast ────────────────────────────────────────────────────
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

    // ── File scan progress via SSE ───────────────────────────────
    // staleTable: existing .files-table to update in-place (may be null for fresh scans)
    function startFileScan(el, staleTable) {
        if (el._scanStarted) return;
        el._scanStarted = true;
        const path = el.dataset.path;
        const total = parseInt(el.dataset.total) || 0;
        const source = new EventSource('/dir-scan?path=' + encodeURIComponent(path));

        source.onmessage = (e) => {
            const data = JSON.parse(e.data);
            if (data.type === 'start' || data.type === 'progress') {
                const done = data.done || 0;
                const tot  = data.total || total;
                el.querySelector('.scan-done').textContent = done;
                el.querySelector('.scan-total').textContent = tot;
                el.querySelector('.scan-bar-fill').style.width = (tot > 0 ? done / tot * 100 : 0) + '%';

                // Incrementally update the stale table if we have per-file HTML
                if (staleTable && data.file_html) {
                    const tmp = document.createElement('div');
                    tmp.innerHTML = data.file_html;
                    const newRow = tmp.querySelector('.file-entry');
                    if (newRow) {
                        const existing = staleTable.querySelector(
                            `.file-entry[data-path="${CSS.escape(newRow.dataset.path)}"]`
                        );
                        if (existing) {
                            existing.replaceWith(newRow);
                        } else {
                            staleTable.appendChild(newRow);
                        }
                    }
                }
            } else if (data.type === 'done') {
                source.close();
                const block = el.closest('.dir-block');
                el.remove();
                if (staleTable) {
                    if (data.html) {
                        const tmp = document.createElement('div');
                        tmp.innerHTML = data.html;
                        const newTable = tmp.querySelector('.files-table');
                        if (newTable) staleTable.replaceWith(newTable);
                        else staleTable.remove();
                    } else {
                        staleTable.remove();
                    }
                } else if (data.html) {
                    // fresh scan — insert into block
                    const tmp = document.createElement('div');
                    tmp.innerHTML = data.html;
                    block && block.appendChild(tmp.firstElementChild);
                }
                applyFilters();
                rebuildDynamicFilters();
                loadImdbBadges(path);
            }
        };

        source.onerror = () => { source.close(); el.remove(); if (staleTable) staleTable.classList.remove('stale'); };
    }

    // ── Lazy dir sizes ────────────────────────────────────────────
    function loadDirSizes(container) {
        const entries = container.querySelectorAll('.dir-entry[data-size="-1"]');
        entries.forEach(el => {
            const p = el.dataset.dirPath;
            if (!p) return;
            fetch('/dir-size?path=' + p)
                .then(r => r.json())
                .then(d => {
                    el.dataset.size = d.size;
                    const span = el.querySelector('.dir-size-inline');
                    if (span) span.textContent = d.human;
                })
                .catch(() => {});
        });
    }

    // ── Dir-check: verify cached files are still up to date ──────
    function startDirCheck(el) {
        if (el._checkStarted) return;
        el._checkStarted = true;
        const path = el.dataset.path;
        fetch('/dir-check?path=' + encodeURIComponent(path))
            .then(r => r.json())
            .then(data => {
                if (!data.changed) { el.remove(); loadImdbBadges(path); return; }
                const block = el.closest('.dir-block');
                const table = block && block.querySelector('.files-table');
                const count = parseInt(el.dataset.count) || 0;
                const scanEl = document.createElement('div');
                scanEl.className = 'files-scanning inline';
                scanEl.dataset.path = path;
                scanEl.dataset.total = count;
                scanEl.innerHTML =
                    '<div class="scan-progress">' +
                    '<span class="scan-text">updating <span class="scan-done">0</span>' +
                    '/<span class="scan-total">' + count + '</span></span>' +
                    '<div class="scan-bar"><div class="scan-bar-fill"></div></div>' +
                    '</div>';
                // Insert progress bar above the table (or in place of the checking div)
                if (table) {
                    table.before(scanEl);
                } else {
                    el.replaceWith(scanEl);
                }
                el.remove();
                startFileScan(scanEl, table || null);
            })
            .catch(() => el.remove());
    }

    // ── URL-based filter persistence ─────────────────────────────
    let _sortDir = 'asc';
    const _sortDefaults = { name: 'asc', size: 'desc', date: 'desc' };

    function updateUrl() {
        const params = new URLSearchParams();
        const q = document.getElementById('search-input').value.trim();
        if (q) params.set('q', q);
        document.querySelectorAll('#global-sort-bar .filter-btn.active').forEach(btn => {
            params.append(btn.dataset.filterGroup, btn.dataset.filterVal);
        });
        const sort = document.querySelector('#global-sort-bar .sort-btn.active')?.dataset.sort;
        if (sort && sort !== 'name') params.set('sort', sort);
        const defaultDir = _sortDefaults[sort] ?? 'asc';
        if (_sortDir !== defaultDir) params.set('dir', _sortDir);
        const qs = params.toString();
        history.replaceState(null, '', qs ? '?' + qs : location.pathname);
    }

    function restoreFiltersToBlock() { /* no-op — filters now live only in global bar */ }

    function restoreFromUrl() {
        const params = new URLSearchParams(location.search);
        const q = params.get('q');
        if (q) document.getElementById('search-input').value = q;
        params.getAll('codec').forEach(val => {
            document.querySelector(`#global-sort-bar .filter-btn[data-filter-group="codec"][data-filter-val="${val}"]`)
                    ?.classList.add('active');
            _filterState.codec.add(val);
        });
        params.getAll('res').forEach(val => {
            document.querySelector(`#global-sort-bar .filter-btn[data-filter-group="res"][data-filter-val="${val}"]`)
                    ?.classList.add('active');
            _filterState.res.add(val);
        });
        // Dynamic filter groups — buttons are built later; store pending vals and apply after rebuild
        ['ext', 'audio', 'hdr'].forEach(group => {
            const vals = params.getAll(group);
            if (vals.length) _pendingRestoreFilters[group] = vals;
        });
        const sort = params.get('sort');
        if (sort) {
            document.querySelectorAll('#global-sort-bar .sort-btn').forEach(b => {
                b.classList.toggle('active', b.dataset.sort === sort);
            });
            _sortDir = params.get('dir') || (_sortDefaults[sort] ?? 'asc');
        }
        _refreshSortArrows();
    }

    restoreFromUrl();

    // Kick off scans/checks for placeholders on initial page load
    document.querySelectorAll('.files-scanning').forEach(startFileScan);
    document.querySelectorAll('.files-checking').forEach(startDirCheck);
    loadDirSizes(document);
    loadFolderHeader();

    // ── Folder header ─────────────────────────────────────────────
    async function loadFolderHeader() {
        const root = MEDIA_ROOT;
        const nameEl  = document.getElementById('folder-header-name');
        const usageEl = document.getElementById('folder-header-usage');
        nameEl.textContent = root.split('/').filter(Boolean).pop() || root;
        try {
            const resp = await fetch('/disk-usage?path=' + encodeURIComponent(root));
            if (!resp.ok) throw new Error('HTTP ' + resp.status);
            const d = await resp.json();
            const pct = d.used_pct;
            const fillClass = pct >= 90 ? 'fh-bar-fill crit' : pct >= 75 ? 'fh-bar-fill warn' : 'fh-bar-fill';
            usageEl.innerHTML = `
                <span class="fh-stat"><b>${escHtml(d.used_human)}</b> used (${pct}%)</span>
                <span class="fh-sep">·</span>
                <div class="fh-bar-wrap">
                    <div class="fh-bar"><div class="${fillClass}" style="width:${pct}%"></div></div>
                </div>
                <span class="fh-sep">·</span>
                <span class="fh-stat"><b>${escHtml(d.free_human)}</b> free (${d.free_pct}%)</span>
                <span class="fh-sep">of</span>
                <span class="fh-stat"><b>${escHtml(d.total_human)}</b></span>`;
        } catch (e) {
            usageEl.innerHTML = `<span class="fh-stat" style="color:var(--muted)">${escHtml(e.message)}</span>`;
        }
    }

    // Watch for placeholders and new dir-blocks injected by HTMX
    const _sortBar = document.getElementById('global-sort-bar');
    new MutationObserver(mutations => {
        let needsFilter = false, needsRebuild = false;
        for (const m of mutations) {
            if (_sortBar.contains(m.target)) continue;  // ignore sort-bar mutations (would cause infinite loop)
            for (const node of m.addedNodes) {
                if (node.nodeType !== 1) continue;
                if (node.classList.contains('files-scanning')) { startFileScan(node); continue; }
                if (node.classList.contains('files-checking')) { startDirCheck(node); continue; }
                node.querySelectorAll('.files-scanning').forEach(startFileScan);
                node.querySelectorAll('.files-checking').forEach(startDirCheck);
                if (node.classList.contains('dir-block')) { loadDirSizes(node); }
                node.querySelectorAll('.dir-block').forEach(b => { loadDirSizes(b); });
                needsFilter = true;
                needsRebuild = true;
            }
        }
        if (needsRebuild) rebuildDynamicFilters();
        if (needsFilter)  applyFilters();
    }).observe(document.body, { childList: true, subtree: true });

    // ── Search + Filters ─────────────────────────────────────────
    function resTag(width, height) {
        if (width >= 3000 || height >= 2000) return '4k';
        if (width >= 1700 || height >= 900)  return '1080p';
        if (width >= 900  || height >= 500)  return '720p';
        if (width > 0    || height > 0)      return 'sd';
        return 'unknown';
    }

    function _normalizeCodecVal(raw) {
        if (['hevc', 'h265'].includes(raw)) return 'h265';
        if (['avc',  'h264'].includes(raw)) return 'h264';
        return raw;
    }

    function codecMatches(codec, filterVal) {
        return _normalizeCodecVal(codec) === filterVal;
    }

    // Cached filter state — updated in toggleFilter/restoreFromUrl, avoids querySelectorAll per applyFilters call
    const _filterState = { codec: new Set(), res: new Set(), ext: new Set(), audio: new Set(), hdr: new Set(), imdb: new Set() };

    function _activeFilters(group) {
        // Used by _rebuildGroup to restore active state after dynamic rebuild
        return [...document.querySelectorAll(`#global-sort-bar .filter-btn.active[data-filter-group="${group}"]`)]
               .map(b => b.dataset.filterVal);
    }


    function _makeFilterBtn(group, val, label) {
        const btn = document.createElement('button');
        btn.className = 'filter-btn';
        btn.dataset.filterGroup = group;
        btn.dataset.filterVal   = val;
        btn.textContent         = label;
        btn.onclick = () => toggleFilter(btn);
        return btn;
    }

    const _pendingRestoreFilters = {};   // group → [val, …] — set by restoreFromUrl before buttons exist

    let _rebuildTimer = null;
    function rebuildDynamicFilters() {
        // Debounce — coalesce rapid calls during progressive scan
        clearTimeout(_rebuildTimer);
        _rebuildTimer = setTimeout(_doRebuild, 150);
    }

    function _doRebuild() {
        const entries = [...document.querySelectorAll('.files-table > .file-entry')];

        const codecs = new Set();
        const audios = new Set();
        const exts   = new Set();
        const hdrs   = new Set();
        entries.forEach(e => {
            const vc = (e.dataset.vcodec || '').toLowerCase().trim();
            if (vc && vc !== 'n/a') codecs.add(_normalizeCodecVal(vc));
            const ac = (e.dataset.audio || '').toLowerCase().trim();
            if (ac && ac !== 'n/a') audios.add(ac);
            const ex = (e.dataset.ext || '').toLowerCase().trim();
            if (ex) exts.add(ex);
            const hdr = (e.dataset.hdr || '').trim();
            if (hdr) hdrs.add(hdr);
        });

        _rebuildGroup('filter-codecs', 'codec', [...codecs].sort(), v => v);
        _rebuildGroup('filter-audio',  'audio', [...audios].sort(), v => v);
        _rebuildGroup('filter-ext',    'ext',   [...exts].sort(),   v => v.replace(/^\./, ''));
        // HDR types in logical order rather than alphabetical
        const _HDR_ORDER = ['DV', 'HDR', 'HLG', 'SDR'];
        _rebuildGroup('filter-hdr', 'hdr', [...hdrs].sort((a, b) => _HDR_ORDER.indexOf(a) - _HDR_ORDER.indexOf(b)), v => v);
    }

    function _rebuildGroup(containerId, group, vals, labelFn) {
        const container = document.getElementById(containerId);
        if (!container) return;
        const active = new Set([..._activeFilters(group), ...(_pendingRestoreFilters[group] || [])]);
        delete _pendingRestoreFilters[group];
        container.replaceChildren();
        if (_filterState[group]) {
            _filterState[group].clear();
            active.forEach(v => { if (vals.includes(v)) _filterState[group].add(v); });
        }
        vals.forEach(val => {
            const btn = _makeFilterBtn(group, val, labelFn(val));
            if (active.has(val)) btn.classList.add('active');
            container.appendChild(btn);
        });
    }

    function applyFilters() {
        const term = (document.getElementById('search-input').value || '').trim().toLowerCase();
        document.querySelectorAll('.files-table > .file-entry').forEach(e => {
            if (term && !(e.dataset.name || '').includes(term)) { e.style.display = 'none'; return; }
            if (_filterState.codec.size > 0 && ![..._filterState.codec].some(v => codecMatches(e.dataset.vcodec || '', v))) { e.style.display = 'none'; return; }
            if (_filterState.res.size > 0 && !_filterState.res.has(resTag(parseInt(e.dataset.width) || 0, parseInt(e.dataset.height) || 0))) { e.style.display = 'none'; return; }
            if (_filterState.ext.size > 0 && !_filterState.ext.has(e.dataset.ext || '')) { e.style.display = 'none'; return; }
            if (_filterState.audio.size > 0 && !_filterState.audio.has(e.dataset.audio || '')) { e.style.display = 'none'; return; }
            if (_filterState.hdr.size > 0 && !_filterState.hdr.has(e.dataset.hdr || '')) { e.style.display = 'none'; return; }
            if (_filterState.imdb.has('unmatched') && e.dataset.imdbTitle) { e.style.display = 'none'; return; }
            e.style.display = '';
        });
        document.querySelectorAll('.dirs-container > .dir-entry')
                .forEach(e => { e.style.display = (!term || (e.dataset.name || '').includes(term)) ? '' : 'none'; });
    }

    let _filterTimer = null;
    function applyGlobalSearch() {
        clearTimeout(_filterTimer);
        _filterTimer = setTimeout(() => { applyFilters(); updateUrl(); }, 80);
    }

    function toggleFilter(btn) {
        btn.classList.toggle('active');
        const group = btn.dataset.filterGroup;
        const val   = btn.dataset.filterVal;
        if (btn.classList.contains('active')) _filterState[group] && _filterState[group].add(val);
        else _filterState[group] && _filterState[group].delete(val);
        applyFilters();
        updateUrl();
    }

    function _refreshSortArrows() {
        document.querySelectorAll('#global-sort-bar .sort-btn').forEach(btn => {
            const key = btn.dataset.sort;
            if (btn.classList.contains('active')) {
                btn.textContent = key + (_sortDir === 'asc' ? ' ↑' : ' ↓');
            } else {
                btn.textContent = key;
            }
        });
    }

    function sortAll(btn) {
        const key = btn.dataset.sort;
        const wasActive = btn.classList.contains('active');
        _sortDir = wasActive ? (_sortDir === 'asc' ? 'desc' : 'asc') : (_sortDefaults[key] ?? 'asc');
        const reorder = (container, selector) => {
            const els = [...container.querySelectorAll(selector)];
            els.sort((a, b) => {
                let cmp = 0;
                if (key === 'name') cmp = a.dataset.name.localeCompare(b.dataset.name);
                if (key === 'size') cmp = parseInt(a.dataset.size) - parseInt(b.dataset.size);
                if (key === 'date') cmp = parseFloat(a.dataset.mtime) - parseFloat(b.dataset.mtime);
                return _sortDir === 'asc' ? cmp : -cmp;
            });
            const frag = document.createDocumentFragment();
            els.forEach(e => frag.appendChild(e));
            container.appendChild(frag);
        };
        document.querySelectorAll('.dir-block').forEach(block => {
            reorder(block.querySelector('.dirs-container'), ':scope > .dir-entry');
            const ft = block.querySelector('.files-table');
            if (ft) reorder(ft, ':scope > .file-entry');
        });
        document.querySelectorAll('#global-sort-bar .sort-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        _refreshSortArrows();
        updateUrl();
    }
    // ── File name: click copies ───────────────────────────────────
    function handleFileClick(stemEl) {
        openMediaInfo(stemEl.closest('.file-entry').dataset.path);
    }

    // ── Media info modal ──────────────────────────────────────────
    function openMediaInfo(path) {
        document.getElementById('mediainfo-title').textContent = path.split('/').pop();
        const body = document.getElementById('mediainfo-body');
        body.innerHTML = 'Loading…';
        document.getElementById('mediainfo-modal').style.display = 'flex';
        fetch('/file-info?path=' + encodeURIComponent(path))
            .then(r => { if (!r.ok) throw new Error('HTTP ' + r.status); return r.json(); })
            .then(d => { body.innerHTML = renderMediaInfo(d); })
            .catch(e => { body.innerHTML = '<span style="color:var(--red)">Error: ' + escHtml(e.message) + '</span>'; });
    }

    function closeMediaInfo() {
        document.getElementById('mediainfo-modal').style.display = 'none';
    }

    function renderMediaInfo(d) {
        const kv    = (k, v) => v != null && v !== '' && v !== 0
            ? `<div class="mi-row"><span class="mi-key">${k}</span><span class="mi-val">${escHtml(String(v))}</span></div>`
            : '';
        const kvRaw = (k, v) => v != null && v !== ''
            ? `<div class="mi-row"><span class="mi-key">${k}</span><span class="mi-val">${v}</span></div>`
            : '';
        const sec = (title, rows) => rows
            ? `<div class="mi-section"><div class="mi-section-title">${title}</div><div class="mi-grid">${rows}</div></div>`
            : '';
        const fmtBr  = br => br > 0 ? (br > 1e6 ? (br/1e6).toFixed(1)+' Mbps' : Math.round(br/1e3)+' kbps') : '';
        const fmtHz  = hz => hz > 0 ? (hz/1000).toFixed(1)+' kHz' : '';
        const fmtSz  = b  => { if(!b) return ''; const u=['B','KB','MB','GB']; let i=0; while(b>=1024&&i<3){b/=1024;i++;} return b.toFixed(i?1:0)+' '+u[i]; };
        const fmtDur = s  => { s=Math.round(s); const h=Math.floor(s/3600),m=Math.floor((s%3600)/60),ss=s%60; return h ? `${h}:${String(m).padStart(2,'0')}:${String(ss).padStart(2,'0')}` : `${m}:${String(ss).padStart(2,'0')}`; };
        const chName = (n, layout) => layout || (n === 1 ? 'mono' : n === 2 ? 'stereo' : n === 6 ? '5.1' : n === 8 ? '7.1' : n + 'ch');

        const fmtRows = [
            kv('Format',   d.format),
            kv('Size',     fmtSz(d.size)),
            kv('Duration', d.duration > 0 ? fmtDur(d.duration) : ''),
            kv('Bitrate',  fmtBr(d.bitrate)),
        ].filter(Boolean).join('');

        const videoSecs = (d.video || []).map((v, i) => {
            const hdrBadge = v.hdr ? '<span class="mi-badge hdr">HDR</span>' : '';
            const res = v.width && v.height ? `${v.width}×${v.height}` : '';
            const rows = [
                kvRaw('Codec',    escHtml(v.codec + (v.profile ? ' / '+v.profile : '')) + hdrBadge),
                kv('Resolution',  res),
                kv('Frame rate',  v.fps > 0 ? v.fps+' fps' : ''),
                kv('Pixel format',v.pix_fmt),
                kv('Bitrate',     fmtBr(v.bitrate)),
                kv('Color space', v.color_space),
                kv('Language',    v.lang),
            ].filter(Boolean).join('');
            return sec((d.video.length > 1 ? `Video Track ${i+1}` : 'Video'), rows);
        }).join('');

        const audioSecs = (d.audio || []).map((a, i) => {
            const rows = [
                kv('Codec',    a.codec),
                kv('Channels', chName(a.channels, a.channel_layout)),
                kv('Sample rate', fmtHz(a.sample_rate)),
                kv('Bitrate',  fmtBr(a.bitrate)),
                kv('Language', a.lang),
                kv('Title',    a.title),
            ].filter(Boolean).join('');
            return sec((d.audio.length > 1 ? `Audio Track ${i+1}` : 'Audio'), rows);
        }).join('');

        const subRows = (d.subtitle || []).map((s, i) =>
            `<div class="mi-row"><span class="mi-key">Track ${i+1}</span><span class="mi-val">${escHtml(s.codec)}${s.lang ? ' · '+escHtml(s.lang) : ''}${s.title ? ' · '+escHtml(s.title) : ''}</span></div>`
        ).join('');
        const subSec = subRows ? sec('Subtitles', subRows) : '';

        return sec('File', fmtRows) + videoSecs + audioSecs + subSec;
    }

    // ── Video player ─────────────────────────────────────────────
    const playerVideo = document.getElementById('player-video');
    let _playerPath      = null;
    let _playerVcodec    = null;
    let _playerHeight    = 0;
    let _playerToken     = null;
    let _transcodeActive = false;
    let _hlsInstance     = null;

    function _loadPlayerSrc(restoreTime = 0) {
        if (_hlsInstance) {
            _hlsInstance.stopLoad();
            _hlsInstance.detachMedia();
            _hlsInstance.destroy();
            _hlsInstance = null;
        }
        playerVideo.pause();
        playerVideo.removeAttribute('src');
        playerVideo.load();

        if (_transcodeActive) {
            if (typeof Hls === 'undefined') {
                document.getElementById('player-codec').textContent += ' [hls.js not loaded]';
                return;
            }
            const src = '/hls/playlist.m3u8'
                      + '?path='   + encodeURIComponent(_playerPath)
                      + '&vcodec=' + encodeURIComponent(_playerVcodec || '')
                      + '&height=' + _playerHeight
                      + '&token='  + _playerToken;

            if (Hls.isSupported()) {
                _hlsInstance = new Hls({ enableWorker: false, fragLoadingTimeOut: 60000, maxBufferLength: 30, maxMaxBufferLength: 30 });
                _hlsInstance.loadSource(src);
                _hlsInstance.attachMedia(playerVideo);
                _hlsInstance.on(Hls.Events.MANIFEST_PARSED, () => {
                    if (restoreTime > 0) playerVideo.currentTime = restoreTime;
                    playerVideo.play().catch(() => {});
                });
                _hlsInstance.on(Hls.Events.ERROR, (event, data) => {
                    console.error('[player] HLS error:', data.type, data.details);
                    if (data.fatal) {
                        document.getElementById('player-codec').textContent =
                            'Error: ' + data.type + ' / ' + data.details;
                    }
                });
            } else {
                // Safari native HLS
                playerVideo.src = src;
                if (restoreTime > 0)
                    playerVideo.addEventListener('loadedmetadata',
                        () => { playerVideo.currentTime = restoreTime; }, { once: true });
                playerVideo.play().catch(() => {});
            }
        } else {
            playerVideo.src = '/stream?path=' + encodeURIComponent(_playerPath);
            playerVideo.load();
            if (restoreTime > 0)
                playerVideo.addEventListener('canplay',
                    () => { playerVideo.currentTime = restoreTime; }, { once: true });
            playerVideo.play().catch(() => {});
        }
    }

    function openPlayer(el) {
        const entry      = el.closest('.file-entry');
        _playerPath      = entry.dataset.path;
        _playerVcodec    = entry.dataset.vcodec || '';
        _playerHeight    = parseInt(entry.dataset.height || '0', 10);
        _playerToken     = Math.random().toString(36).slice(2);
        _transcodeActive = entry.dataset.needsTranscode === 'true';
        const stem = _playerPath.split('/').pop().replace(/\.[^.]+$/, '');
        const codec = (entry.dataset.vcodec || '').toUpperCase();
        const w = entry.dataset.width, h = entry.dataset.height;
        const res = (w && h && w !== '0') ? `${w}×${h}` : '';

        document.getElementById('player-title').textContent = stem;
        document.getElementById('player-codec').textContent = [codec, res].filter(Boolean).join(' · ');
        document.getElementById('transcode-btn').classList.toggle('active', _transcodeActive);
        document.getElementById('player-modal').style.display = 'flex';
        _loadPlayerSrc(_playerResumePos(_playerPath));
    }

    function toggleTranscode() {
        const t = playerVideo.currentTime || 0;
        _transcodeActive = !_transcodeActive;
        document.getElementById('transcode-btn').classList.toggle('active', _transcodeActive);
        _loadPlayerSrc(t);
    }

    function _playerResumePos(path) {
        try {
            const raw = localStorage.getItem('ms_pos_' + btoa(path));
            if (!raw) return 0;
            const { t, saved } = JSON.parse(raw);
            // Discard positions older than 30 days
            if (Date.now() - saved > 30 * 86400 * 1000) { localStorage.removeItem('ms_pos_' + btoa(path)); return 0; }
            return t || 0;
        } catch { return 0; }
    }

    function closePlayer() {
        // Save playback position (skip if within 5 s of start or if essentially done)
        const t = playerVideo.currentTime;
        const dur = playerVideo.duration;
        if (_playerPath && t > 5 && (!dur || t < dur - 10)) {
            try { localStorage.setItem('ms_pos_' + btoa(_playerPath), JSON.stringify({ t, saved: Date.now() })); }
            catch {}
        } else if (_playerPath) {
            try { localStorage.removeItem('ms_pos_' + btoa(_playerPath)); } catch {}
        }
        playerVideo.pause();
        if (_hlsInstance) {
            _hlsInstance.stopLoad();
            _hlsInstance.detachMedia();
            _hlsInstance.destroy();
            _hlsInstance = null;
        }
        playerVideo.removeAttribute('src');
        playerVideo.load();
        _playerPath = null;
        _playerVcodec = null;
        _playerHeight = 0;
        _playerToken = null;
        _transcodeActive = false;
        document.getElementById('transcode-btn').classList.remove('active');
        document.getElementById('player-modal').style.display = 'none';
    }

    // ── Delete with confirmation ─────────────────────────────────
    let _deleteRow = null;

    function openDeleteModal(btn) {
        _deleteRow = btn.closest('.file-entry');
        const name = _deleteRow.dataset.path.split('/').pop();
        document.getElementById('modal-filename').textContent = name;
        document.getElementById('modal-input').value = '';
        document.getElementById('modal-confirm').disabled = true;
        document.getElementById('delete-modal').style.display = 'flex';
        document.getElementById('modal-input').focus();
    }

    function closeDeleteModal() {
        document.getElementById('delete-modal').style.display = 'none';
        _deleteRow = null;
    }

    document.getElementById('modal-input').addEventListener('input', e => {
        document.getElementById('modal-confirm').disabled =
            e.target.value.trim().toLowerCase() !== 'ok';
    });

    document.getElementById('modal-input').addEventListener('keydown', e => {
        if (e.key === 'Enter' && !document.getElementById('modal-confirm').disabled) confirmDelete();
    });

    document.addEventListener('keydown', e => {
        if (e.key === 'Escape') { closeDeleteModal(); closeRenameModal(); closePlayer(); closeMediaInfo(); closeSearchSettings(); closeSearchMenu(); }
    });

    // ── Search sites ─────────────────────────────────────────────
    let _searchDropdown = null;

    const _DEFAULT_SEARCH_SITES = [
        { name: 'NZBGeek', url: 'https://nzbgeek.info/geekseek.php?moviesgeekseek=1&c=&browseincludewords={query}', disabled: false },
        { name: 'IMDB',    url: 'https://www.imdb.com/find/?q={query}',                                            disabled: false },
        { name: 'TheTVDB', url: 'https://www.thetvdb.com/search?query={query}',                                    disabled: false },
    ];

    function _getSearchSites() {
        const raw = localStorage.getItem('ms_searches');
        if (raw === null) {
            // First run — seed defaults without overwriting a deliberate empty list
            localStorage.setItem('ms_searches', JSON.stringify(_DEFAULT_SEARCH_SITES));
            return _DEFAULT_SEARCH_SITES;
        }
        try { return JSON.parse(raw) || []; }
        catch { return []; }
    }
    function _saveSearchSites(arr) { localStorage.setItem('ms_searches', JSON.stringify(arr)); }

    function openSearchMenu(e, btn) {
        e.stopPropagation();
        closeSearchMenu();
        const sites = _getSearchSites();
        const fileEntry = btn.closest('.file-entry');
        const dirEntry = btn.closest('.dir-entry');
        let stem;
        if (fileEntry) stem = fileEntry.querySelector('.file-stem').textContent.trim();
        else if (dirEntry) stem = dirEntry.querySelector('.dir-name').textContent.trim().replace(/\/$/, '');
        if (!stem) return;
        if (sites.length === 0) { openSearchSettings(); return; }

        const menu = document.createElement('div');
        menu.className = 'search-dropdown';
        sites.filter(s => !s.disabled).forEach(s => {
            const q = encodeURIComponent(stem).replace(/%20/g, '+');
            const resolved = s.url.replace(/\{query\}/g, q);
            const a = document.createElement('a');
            if (/^https?:\/\//i.test(resolved)) a.href = resolved;
            a.target = '_blank';
            a.rel = 'noopener noreferrer';
            a.textContent = s.name;
            menu.appendChild(a);
        });

        const rect = btn.getBoundingClientRect();
        menu.style.top  = (rect.bottom + 4) + 'px';
        menu.style.left = rect.left + 'px';
        document.body.appendChild(menu);
        _searchDropdown = menu;
        setTimeout(() => document.addEventListener('click', closeSearchMenu, { once: true }), 10);
    }

    function closeSearchMenu() {
        if (_searchDropdown) { _searchDropdown.remove(); _searchDropdown = null; }
    }

    function openSearchSettings() {
        _renderSearchSites();
        document.getElementById('search-settings-modal').style.display = 'flex';
        document.getElementById('new-search-name').focus();
    }
    function closeSearchSettings() {
        document.getElementById('search-settings-modal').style.display = 'none';
    }

    function _renderSearchSites() {
        const sites = _getSearchSites();
        const list = document.getElementById('search-sites-list');
        if (sites.length === 0) {
            list.innerHTML = '<div style="color:var(--muted);font-size:13px;padding:4px 0">No sites configured yet.</div>';
            return;
        }
        list.replaceChildren(...sites.map((s, i) => {
            const row = document.createElement('div');
            row.className = 'search-site-row' + (s.disabled ? ' disabled' : '');

            // Enable/disable checkbox
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.checked = !s.disabled;
            cb.title = s.disabled ? 'Enable' : 'Disable';
            cb.onchange = () => toggleSearchSite(i, !cb.checked);

            const nameSpan = document.createElement('span');
            nameSpan.className = 'search-site-name';
            nameSpan.textContent = s.name;

            const urlSpan = document.createElement('span');
            urlSpan.className = 'search-site-url';
            urlSpan.title = s.url;
            urlSpan.textContent = s.url;

            const editBtn = document.createElement('button');
            editBtn.className = 'btn';
            editBtn.textContent = '✎';
            editBtn.title = 'Edit';
            editBtn.onclick = () => editSearchSite(i, row, s);

            const delBtn = document.createElement('button');
            delBtn.className = 'btn';
            delBtn.textContent = '✕';
            delBtn.title = 'Remove';
            delBtn.onclick = () => removeSearchSite(i);

            row.append(cb, nameSpan, urlSpan, editBtn, delBtn);
            return row;
        }));
    }

    function editSearchSite(i, row, s) {
        // Replace view cells with inline edit fields
        row.classList.add('editing');
        // Keep the checkbox (first child), replace the rest
        const cb = row.firstChild;
        const fields = document.createElement('div');
        fields.className = 'search-site-edit-fields';

        const nameIn = document.createElement('input');
        nameIn.className = 'modal-input';
        nameIn.type = 'text';
        nameIn.value = s.name;
        nameIn.style.width = '130px';

        const urlIn = document.createElement('input');
        urlIn.className = 'modal-input';
        urlIn.type = 'text';
        urlIn.value = s.url;
        urlIn.style.flex = '1';

        const saveBtn = document.createElement('button');
        saveBtn.className = 'btn';
        saveBtn.textContent = '✓';
        saveBtn.title = 'Save';
        saveBtn.onclick = () => saveSearchSite(i, nameIn.value.trim(), urlIn.value.trim());

        const cancelBtn = document.createElement('button');
        cancelBtn.className = 'btn';
        cancelBtn.textContent = '✕';
        cancelBtn.title = 'Cancel';
        cancelBtn.onclick = () => _renderSearchSites();

        fields.append(nameIn, urlIn, saveBtn, cancelBtn);
        row.replaceChildren(cb, fields);
        nameIn.focus();
        urlIn.addEventListener('keydown', e => { if (e.key === 'Enter') saveBtn.click(); });
    }

    function saveSearchSite(i, name, url) {
        if (!name || !url) return;
        const sites = _getSearchSites();
        sites[i] = { ...sites[i], name, url };
        _saveSearchSites(sites);
        _renderSearchSites();
    }

    function toggleSearchSite(i, disabled) {
        const sites = _getSearchSites();
        sites[i] = { ...sites[i], disabled };
        _saveSearchSites(sites);
        const row = document.getElementById('search-sites-list').children[i];
        if (row) row.className = 'search-site-row' + (disabled ? ' disabled' : '');
    }

    function addSearchSite() {
        const name = document.getElementById('new-search-name').value.trim();
        const url  = document.getElementById('new-search-url').value.trim();
        if (!name || !url) return;
        const sites = _getSearchSites();
        sites.push({ name, url, disabled: false });
        _saveSearchSites(sites);
        document.getElementById('new-search-name').value = '';
        document.getElementById('new-search-url').value  = '';
        _renderSearchSites();
        document.getElementById('new-search-name').focus();
    }

    function removeSearchSite(i) {
        const sites = _getSearchSites();
        sites.splice(i, 1);
        _saveSearchSites(sites);
        _renderSearchSites();
    }

    function exportSearchSites() {
        const sites = _getSearchSites();
        const rows = [['name', 'url', 'disabled']];
        sites.forEach(s => {
            // Wrap fields in quotes, escape inner quotes by doubling them
            const esc = v => '"' + String(v ?? '').replace(/"/g, '""') + '"';
            rows.push([esc(s.name), esc(s.url), esc(s.disabled ? '1' : '0')]);
        });
        const csv = rows.map(r => r.join(',')).join('\r\n');
        const a = document.createElement('a');
        a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
        a.download = 'search-sites.csv';
        a.click();
        URL.revokeObjectURL(a.href);
    }

    function importSearchSites(input) {
        const file = input.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = e => {
            const lines = e.target.result.replace(/\r\n?/g, '\n').trim().split('\n');
            // Simple CSV parse: respects quoted fields (handles embedded commas/quotes)
            const parseRow = line => {
                const out = [];
                let cur = '', inQ = false;
                for (let i = 0; i < line.length; i++) {
                    const ch = line[i];
                    if (inQ) {
                        if (ch === '"' && line[i + 1] === '"') { cur += '"'; i++; }
                        else if (ch === '"') inQ = false;
                        else cur += ch;
                    } else {
                        if (ch === '"') inQ = true;
                        else if (ch === ',') { out.push(cur); cur = ''; }
                        else cur += ch;
                    }
                }
                out.push(cur);
                return out;
            };
            const header = parseRow(lines[0]).map(h => h.toLowerCase());
            const ni = header.indexOf('name'), ui = header.indexOf('url'), di = header.indexOf('disabled');
            if (ni === -1 || ui === -1) { showToast('CSV must have name and url columns'); return; }
            const imported = [];
            for (let i = 1; i < lines.length; i++) {
                if (!lines[i].trim()) continue;
                const cols = parseRow(lines[i]);
                const name = cols[ni]?.trim(), url = cols[ui]?.trim();
                if (!name || !url) continue;
                imported.push({ name, url, disabled: di !== -1 && cols[di]?.trim() === '1' });
            }
            if (!imported.length) { showToast('No valid rows found in CSV'); return; }
            _saveSearchSites(imported);
            _renderSearchSites();
            showToast(`Imported ${imported.length} site${imported.length !== 1 ? 's' : ''}`);
        };
        reader.readAsText(file);
        input.value = '';  // reset so the same file can be re-imported
    }

    document.getElementById('new-search-url').addEventListener('keydown', e => {
        if (e.key === 'Enter') addSearchSite();
    });

    async function confirmDelete() {
        if (!_deleteRow) return;
        const path = _deleteRow.dataset.path;
        const row  = _deleteRow;
        closeDeleteModal();
        const res = await fetch('/file?path=' + encodeURIComponent(path), {
            method: 'DELETE',
            headers: { 'X-Delete-Token': DELETE_TOKEN },
        });
        if (res.ok) {
            row.remove();
        } else {
            alert('Delete failed (' + res.status + ')');
        }
    }

    // ── Rename ───────────────────────────────────────────────────
    let _renameRow = null;

    // Strip / replace characters that are illegal on Windows (NTFS/FAT) filesystems,
    // which is the common case for media drives and NAS shares.
    function _sanitizeFilename(stem) {
        return stem
            .replace(/:/g, ' -')           // "Title: Sub" → "Title - Sub"
            .replace(/[?*"<>|\\\/]/g, '')  // remove remaining illegal chars
            .replace(/\s{2,}/g, ' ')       // collapse double spaces left by removals
            .replace(/[. ]+$/, '')         // trailing dots/spaces (Windows reserved)
            .trim();
    }

    function _normaliseStem(stem) {
        return _sanitizeFilename(stem
            .replace(/\./g, ' ')          // dots → spaces
            .replace(/_/g, ' ')            // underscores → spaces
            .replace(/\s{2,}/g, ' ')       // collapse runs of spaces
            .trim());
    }

    function _buildDiff(oldName, newName) {
        if (oldName === newName) return '';
        const div = document.createElement('div');
        div.className = 'rename-diff';
        const del = document.createElement('del');
        del.textContent = oldName;
        const ins = document.createElement('ins');
        ins.textContent = newName;
        div.append(del, ' → ', ins);
        return div;
    }

    function openRenameModal(btn) {
        _renameRow = btn.closest('.file-entry');
        const currentName = _renameRow.dataset.path.split('/').pop();
        const ext = currentName.includes('.') ? '.' + currentName.split('.').pop() : '';
        const input = document.getElementById('rename-input');

        // If file has an IMDB match, suggest "primaryTitle (startYear).ext"
        const imdbTitle = _renameRow.dataset.imdbTitle;
        const imdbYear  = _renameRow.dataset.imdbYear;
        if (imdbTitle && imdbYear) {
            input.value = `${_sanitizeFilename(imdbTitle + ' (' + imdbYear + ')')}${ext}`;
        } else {
            input.value = currentName;
        }

        _updateRenameDiff();
        document.getElementById('rename-modal').style.display = 'flex';
        input.focus();
        input.select();
    }

    function closeRenameModal() {
        document.getElementById('rename-modal').style.display = 'none';
        _renameRow = null;
    }

    function normaliseRename() {
        if (!_renameRow) return;
        const currentName = _renameRow.dataset.path.split('/').pop();
        const lastDot = currentName.lastIndexOf('.');
        const ext  = lastDot >= 0 ? currentName.slice(lastDot) : '';
        const stem = lastDot >= 0 ? currentName.slice(0, lastDot) : currentName;
        document.getElementById('rename-input').value = _normaliseStem(stem) + ext;
        _updateRenameDiff();
    }

    function _updateRenameDiff() {
        if (!_renameRow) return;
        const currentName = _renameRow.dataset.path.split('/').pop();
        const newName = document.getElementById('rename-input').value.trim();
        const diffEl = document.getElementById('rename-diff');
        diffEl.replaceChildren();
        if (newName && newName !== currentName) {
            diffEl.append(_buildDiff(currentName, newName));
        }
    }

    document.getElementById('rename-input').addEventListener('input', _updateRenameDiff);
    document.getElementById('rename-input').addEventListener('keydown', e => {
        if (e.key === 'Enter') confirmRename();
    });

    // ── Export ──────────────────────────────────────────────────
    const _EXPORT_PRESETS = {
        'titles':       { fmt: 'text', fields: ['title'],                            scope: 'visible' },
        'titles-paths': { fmt: 'text', fields: ['path', 'title'],                    scope: 'visible' },
        'csv-all':      { fmt: 'csv',  fields: ['title','filename','path','size','vcodec','res','audio','duration'], scope: 'visible' },
        'json':         { fmt: 'json', fields: ['title','filename','path','size','vcodec','res','audio','duration'], scope: 'visible' },
    };

    function _exportGetConfig() {
        return {
            fmt:    document.querySelector('input[name="export-fmt"]:checked').value,
            scope:  document.querySelector('input[name="export-scope"]:checked').value,
            fields: ['title','filename','path','size','vcodec','res','audio','duration']
                        .filter(f => document.getElementById('ef-' + f)?.checked),
        };
    }

    function _exportRows(scope) {
        const all = [...document.querySelectorAll('.files-table > .file-entry')];
        const entries = scope === 'all' ? all : all.filter(el => el.style.display !== 'none');
        return entries.map(el => {
            const fname = el.dataset.path.split('/').pop() || '';
            const dot   = fname.lastIndexOf('.');
            const stem  = el.querySelector('.file-stem')?.textContent || (dot >= 0 ? fname.slice(0, dot) : fname);
            const w = parseInt(el.dataset.width) || 0;
            const h = parseInt(el.dataset.height) || 0;
            return {
                title:    stem,
                filename: fname,
                path:     el.dataset.path,
                size:     el.querySelector('.tag.size')?.textContent || '',
                vcodec:   el.dataset.vcodec || '',
                res:      (w && h) ? w + '×' + h : '',
                audio:    el.dataset.audio || '',
                duration: el.dataset.duration ? el.dataset.duration + 'm' : '',
            };
        });
    }

    function _csvEscape(v) {
        if (v.includes(',') || v.includes('"') || v.includes('\n')) return '"' + v.replace(/"/g, '""') + '"';
        return v;
    }

    function _buildExport(cfg) {
        const rows  = _exportRows(cfg.scope);
        const fields = cfg.fields.length ? cfg.fields : ['title'];

        if (cfg.fmt === 'json') {
            const out = rows.map(r => {
                const obj = {};
                fields.forEach(f => { obj[f] = r[f]; });
                return obj;
            });
            return JSON.stringify(out, null, 2);
        }
        if (cfg.fmt === 'csv') {
            const header = fields.join(',');
            const lines  = rows.map(r => fields.map(f => _csvEscape(r[f] || '')).join(','));
            return [header, ...lines].join('\n');
        }
        // plain text
        return rows.map(r => fields.map(f => r[f] || '').filter(Boolean).join('\t')).join('\n');
    }

    function refreshExportPreview() {
        const cfg  = _exportGetConfig();
        const text = _buildExport(cfg);
        const rows = _exportRows(cfg.scope);
        document.getElementById('export-count').textContent = rows.length + ' files';
        const lines = text.split('\n');
        const preview = lines.slice(0, 30).join('\n') + (lines.length > 30 ? '\n…' : '');
        document.getElementById('export-preview').textContent = preview;
    }

    function applyExportPreset(name) {
        const p = _EXPORT_PRESETS[name];
        if (!p) return;
        document.querySelector(`input[name="export-fmt"][value="${p.fmt}"]`).checked = true;
        document.querySelector(`input[name="export-scope"][value="${p.scope}"]`).checked = true;
        ['title','filename','path','size','vcodec','res','audio','duration'].forEach(f => {
            const el = document.getElementById('ef-' + f);
            if (el) el.checked = p.fields.includes(f);
        });
        refreshExportPreview();
    }

    function _exportFilename(fmt) {
        const ext = { text: 'txt', csv: 'csv', json: 'json' }[fmt] || 'txt';
        return 'mediastat-export.' + ext;
    }

    function copyExport() {
        const text = _buildExport(_exportGetConfig());
        navigator.clipboard.writeText(text).then(() => {
            const btn = document.querySelector('#export-modal .btn');
            const orig = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(() => btn.textContent = orig, 1500);
        });
    }

    function downloadExport() {
        const cfg  = _exportGetConfig();
        const text = _buildExport(cfg);
        const mime = cfg.fmt === 'json' ? 'application/json' : cfg.fmt === 'csv' ? 'text/csv' : 'text/plain';
        const blob = new Blob([text], { type: mime });
        const a    = document.createElement('a');
        a.href     = URL.createObjectURL(blob);
        a.download = _exportFilename(cfg.fmt);
        a.click();
        URL.revokeObjectURL(a.href);
    }

    function openExportModal() {
        document.getElementById('export-modal').style.display = 'flex';
        refreshExportPreview();
    }

    function closeExportModal() {
        document.getElementById('export-modal').style.display = 'none';
    }

    document.addEventListener('keydown', e => {
        if (e.key === 'Escape' && document.getElementById('export-modal').style.display !== 'none') {
            closeExportModal();
        }
    });

    // ── Duplicate detection ──────────────────────────────────────
    const _NOISE_RE = /\b(19|20)\d{2}\b|\b(2160|1080|720|480)[pi]\b|\b4k\b|\b(bluray|bdrip|webrip|web-dl|hdtv|dvdrip|x264|x265|hevc|avc|h264|h265|aac|dts|ac3|remux|extended|remastered|unrated|theatrical)\b/gi;

    function _normForDupe(rawName) {
        // rawName is already lowercase data-name (stem without extension)
        return rawName
            .replace(/[._\-]/g, ' ')   // separators → spaces
            .replace(_NOISE_RE, ' ')    // strip year/res/codec noise
            .replace(/\s+/g, ' ')
            .trim();
    }

    function _bigrams(s) {
        const out = [];
        for (let i = 0; i < s.length - 1; i++) out.push(s.slice(i, i + 2));
        return out;
    }

    function _diceSim(a, b) {
        if (a === b) return 1;
        if (a.length < 2 || b.length < 2) return 0;
        const ba = _bigrams(a), bb = _bigrams(b);
        const setB = {};
        bb.forEach(g => { setB[g] = (setB[g] || 0) + 1; });
        let common = 0;
        ba.forEach(g => { if (setB[g] > 0) { common++; setB[g]--; } });
        return (2 * common) / (ba.length + bb.length);
    }

    function _collectFiles() {
        return [...document.querySelectorAll('.files-table > .file-entry')].map(el => ({
            el,
            path:  el.dataset.path || '',
            name:  el.dataset.name || '',   // already lowercase stem
            norm:  _normForDupe(el.dataset.name || ''),
            size:  el.querySelector('.tag.size')?.textContent || '',
            codec: el.querySelector('[class*="codec-"]')?.textContent || '',
            res:   el.querySelectorAll('.tag')[3]?.textContent || '',
        }));
    }

    function _findDupeGroups(threshold) {
        const files = _collectFiles();
        const used = new Uint8Array(files.length);
        const groups = [];
        for (let i = 0; i < files.length; i++) {
            if (used[i]) continue;
            const group = [{ file: files[i], score: 1 }];
            for (let j = i + 1; j < files.length; j++) {
                if (used[j]) continue;
                if (!files[i].norm || !files[j].norm) continue;
                const score = _diceSim(files[i].norm, files[j].norm);
                if (score >= threshold) {
                    group.push({ file: files[j], score });
                    used[j] = 1;
                }
            }
            if (group.length > 1) {
                used[i] = 1;
                groups.push(group);
            }
        }
        return groups;
    }

    function _renderDupes(groups) {
        const body = document.getElementById('dupes-body');
        body.replaceChildren();
        if (!groups.length) {
            const p = document.createElement('div');
            p.className = 'dupes-empty';
            p.textContent = 'No duplicates found at this threshold.';
            body.appendChild(p);
            return;
        }
        groups.forEach(group => {
            const div = document.createElement('div');
            div.className = 'dupe-group';

            const hdr = document.createElement('div');
            hdr.className = 'dupe-group-header';
            hdr.textContent = group.length + ' likely duplicates';
            div.appendChild(hdr);

            group.forEach(({ file, score }) => {
                const row = document.createElement('div');
                row.className = 'dupe-row';

                const nameCol = document.createElement('div');
                nameCol.className = 'dupe-name-col';

                const nameEl = document.createElement('span');
                nameEl.className = 'dupe-name';
                nameEl.title = file.path;
                const fname = file.path.split('/').pop() || file.name;
                const dotIdx = fname.lastIndexOf('.');
                nameEl.textContent = dotIdx >= 0 ? fname.slice(0, dotIdx) : fname;
                nameCol.appendChild(nameEl);

                const dirEl = document.createElement('span');
                dirEl.className = 'dupe-dir';
                dirEl.textContent = file.path.split('/').slice(0, -1).join('/');
                nameCol.appendChild(dirEl);

                row.appendChild(nameCol);

                const tags = document.createElement('span');
                tags.className = 'dupe-tags';
                if (file.size)  { const t = document.createElement('span'); t.className = 'tag size';  t.textContent = file.size;  tags.appendChild(t); }
                if (file.codec) { const t = document.createElement('span'); t.className = 'tag';       t.textContent = file.codec; tags.appendChild(t); }
                if (file.res)   { const t = document.createElement('span'); t.className = 'tag';       t.textContent = file.res;   tags.appendChild(t); }
                row.appendChild(tags);

                if (score < 1) {
                    const sc = document.createElement('span');
                    sc.className = 'dupe-score';
                    sc.textContent = Math.round(score * 100) + '%';
                    row.appendChild(sc);
                }

                const del = document.createElement('button');
                del.className = 'delete-btn';
                del.title = 'Delete this file';
                del.textContent = '✕';
                del.onclick = () => {
                    // reuse existing delete modal flow by finding the row in the main table
                    const mainRow = document.querySelector(`.file-entry[data-path="${CSS.escape(file.path)}"]`);
                    if (mainRow) {
                        closeDupesModal();
                        openDeleteModal(mainRow.querySelector('.delete-btn'));
                    }
                };
                row.appendChild(del);

                div.appendChild(row);
            });
            body.appendChild(div);
        });
    }

    async function runDupesScan() {
        const threshold = parseInt(document.getElementById('dupes-threshold').value) / 100;
        const body = document.getElementById('dupes-body');
        body.innerHTML = '<div class="dupes-empty">Scanning…</div>';
        try {
            const resp = await fetch('/dupes?threshold=' + threshold);
            if (!resp.ok) throw new Error(await resp.text());
            const data = await resp.json();
            _renderDupes(data.groups);
        } catch (e) {
            body.innerHTML = `<div class="dupes-empty">Error: ${escHtml(e.message)}</div>`;
        }
    }

    function openDupesModal() {
        document.getElementById('dupes-modal').style.display = 'flex';
        runDupesScan();
    }

    function closeDupesModal() {
        document.getElementById('dupes-modal').style.display = 'none';
    }

    document.addEventListener('keydown', e => {
        if (e.key === 'Escape' && document.getElementById('dupes-modal').style.display !== 'none') {
            closeDupesModal();
        }
    });

    async function confirmRename() {
        if (!_renameRow) return;
        const path    = _renameRow.dataset.path;
        const newName = document.getElementById('rename-input').value.trim();
        const current = path.split('/').pop();
        if (!newName || newName === current) { closeRenameModal(); return; }
        const res = await fetch(
            '/rename?path=' + encodeURIComponent(path) + '&new_name=' + encodeURIComponent(newName),
            { method: 'POST', headers: { 'X-Delete-Token': DELETE_TOKEN } }
        );
        if (res.ok) {
            const data = await res.json();
            // Update the row in place
            _renameRow.dataset.path = data.path;
            _renameRow.dataset.name = data.name.toLowerCase();
            const lastDot = data.name.lastIndexOf('.');
            const newExt  = lastDot >= 0 ? data.name.slice(lastDot).toLowerCase() : '';
            if (newExt) _renameRow.dataset.ext = newExt;
            const stemEl = _renameRow.querySelector('.file-stem');
            if (stemEl) stemEl.textContent = lastDot >= 0 ? data.name.slice(0, lastDot) : data.name;
            const extTag = _renameRow.querySelector('.tag[class*="ext-"]');
            if (extTag && newExt) extTag.textContent = newExt;
            closeRenameModal();
        } else {
            const msg = res.status === 409 ? 'A file with that name already exists.'
                      : res.status === 400 ? 'Invalid filename.'
                      : 'Rename failed (' + res.status + ')';
            alert(msg);
        }
    }
