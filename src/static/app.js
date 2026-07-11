
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
    const _CUSTOM_PRESETS_KEY = 'mediastat_custom_encode_presets';

    // ── User-savable encode presets (localStorage — per-browser, no backend
    // needed) ───────────────────────────────────────────────────────────
    function _loadCustomPresets() {
        try {
            const raw = localStorage.getItem(_CUSTOM_PRESETS_KEY);
            return raw ? JSON.parse(raw) : {};
        } catch (e) {
            return {};
        }
    }

    function _saveCustomPresets(presets) {
        try {
            localStorage.setItem(_CUSTOM_PRESETS_KEY, JSON.stringify(presets));
        } catch (e) {
            showToast('Could not save preset (browser storage unavailable)', 'error');
        }
    }

    function _renderCustomPresetButtons() {
        const slot = document.getElementById('custom-presets-slot');
        if (!slot) return;
        const presets = _loadCustomPresets();
        slot.innerHTML = Object.keys(presets).map(name => `
            <span style="position:relative;display:inline-block">
                <button class="encode-preset-btn" onclick="applyEncodePreset(${JSON.stringify(name)})" title="Custom preset">${escHtml(name)}</button>
                <button onclick="event.stopPropagation();deleteCustomPreset(${JSON.stringify(name)})"
                        title="Delete this preset" style="position:absolute;top:-6px;right:-6px;width:16px;height:16px;line-height:14px;padding:0;border-radius:50%;font-size:10px;background:var(--surface2,#333);border:1px solid var(--border);color:var(--muted);cursor:pointer">✕</button>
            </span>`).join('');
    }

    function saveCurrentAsPreset() {
        const name = prompt('Save current settings as a preset named:');
        if (!name || !name.trim()) return;
        const presets = _loadCustomPresets();
        presets[name.trim()] = {
            qp:      parseInt(document.getElementById('enc-qp').value, 10),
            preset:  document.getElementById('enc-preset').value,
            codec:   document.getElementById('enc-codec').value,
            gpu:     document.getElementById('enc-gpu').value,
            format:  document.getElementById('enc-format').value,
            denoise: document.getElementById('enc-denoise').value || '',
            crop:    document.getElementById('enc-crop').checked,
            lang:    document.getElementById('enc-lang').value.trim() || 'eng',
            width:   document.getElementById('enc-width').value || '',
        };
        _saveCustomPresets(presets);
        _renderCustomPresetButtons();
        applyEncodePreset(name.trim());
        showToast(`Preset "${escHtml(name.trim())}" saved`, 'success');
    }

    function deleteCustomPreset(name) {
        const presets = _loadCustomPresets();
        delete presets[name];
        _saveCustomPresets(presets);
        _renderCustomPresetButtons();
    }

    // ── Per-folder default encode settings (localStorage, remembers the
    // last settings used for a given folder instead of always resetting to
    // the quality preset) ──────────────────────────────────────────────
    const _FOLDER_DEFAULTS_KEY = 'mediastat_folder_encode_defaults';

    function _loadFolderDefaults() {
        try {
            const raw = localStorage.getItem(_FOLDER_DEFAULTS_KEY);
            return raw ? JSON.parse(raw) : {};
        } catch (e) {
            return {};
        }
    }

    function _saveFolderDefault(folderPath, config) {
        try {
            const all = _loadFolderDefaults();
            all[folderPath] = config;
            localStorage.setItem(_FOLDER_DEFAULTS_KEY, JSON.stringify(all));
        } catch (e) { /* localStorage unavailable — just skip remembering */ }
    }

    function _applyFolderDefault(folderPath) {
        const saved = _loadFolderDefaults()[folderPath];
        if (!saved) return false;
        document.getElementById('enc-qp').value      = saved.qp;
        document.getElementById('enc-preset').value  = saved.preset;
        document.getElementById('enc-codec').value   = saved.codec;
        document.getElementById('enc-gpu').value     = saved.gpu;
        document.getElementById('enc-format').value  = saved.format;
        document.getElementById('enc-denoise').value = saved.denoise || '';
        document.getElementById('enc-crop').checked  = !!saved.crop;
        document.getElementById('enc-lang').value    = saved.lang || 'eng';
        document.getElementById('enc-width').value   = saved.width || '';
        return true;
    }

    function openEncodeModal(btn) {
        const entry = btn.closest('.file-entry');
        const path  = entry.dataset.path;
        const name  = entry.querySelector('.file-stem').textContent.trim();
        document.getElementById('encode-modal-name').textContent = name;
        document.getElementById('encode-file-path').value = path;
        _renderCustomPresetButtons();
        applyEncodePreset('quality');
        document.getElementById('encode-modal').style.display = 'flex';
        document.getElementById('estimate-btn').disabled = false;
        document.getElementById('estimate-stop-btn').style.display = 'none';
        _loadEstimateForModal(path);
    }

    const _ESTIMATE_RUNNING_STATUSES = ['starting', 'probing', 'extracting', 'encoding'];

    // Reopening the modal previously always showed a blank panel until a
    // running estimate finished, even though the sweep kept running in the
    // background — the only source consulted was the finished-only history
    // endpoint. Check the live estimate state first so a run already in
    // progress for this exact file reattaches immediately with current
    // progress, instead of going dark until it completes.
    async function _loadEstimateForModal(path) {
        const panel = document.getElementById('estimate-panel');
        panel.style.display = 'none';
        document.getElementById('estimate-rows').innerHTML = '';
        document.getElementById('estimate-summary').innerHTML = '';

        try {
            const liveResp = await fetch('/encode/estimate/state');
            if (liveResp.ok) {
                const live = await liveResp.json();
                if (live.path === path && _ESTIMATE_RUNNING_STATUSES.includes(live.status)) {
                    if (document.getElementById('encode-file-path').value !== path) return;
                    document.getElementById('estimate-status-line').textContent =
                        'Sampling 60s from the middle of the file…';
                    panel.style.display = 'block';
                    _renderEstimateState(live);
                    _attachEstimateSource();
                    return;
                }
            }
        } catch (e) { /* live-state check failed — fall through to history */ }

        _loadCachedEstimate(path);
    }

    // Show a previous estimate for this exact file, if one is cached
    // server-side, instead of always starting blank — each file keeps its
    // own last result, so switching files never loses another file's numbers.
    async function _loadCachedEstimate(path) {
        const panel = document.getElementById('estimate-panel');
        try {
            const resp = await fetch('/encode/estimate/history?path=' + encodeURIComponent(path));
            if (!resp.ok) return;
            const state = await resp.json();
            document.getElementById('estimate-status-line').textContent =
                'Showing a previous estimate for this file — click Estimate to re-run.';
            panel.style.display = 'block';
            _renderEstimateState(state);
        } catch (e) {
            // No cached estimate for this file — leave the panel hidden.
        }
    }
    function closeEncodeModal() {
        document.getElementById('encode-modal').style.display = 'none';
        if (_estimateSource) { _estimateSource.close(); _estimateSource = null; }
        // Restore original onclick if it was overridden by batch mode
        const btn = document.querySelector('#encode-modal .btn-primary');
        if (btn && btn._originalOnclick) { btn.onclick = btn._originalOnclick; btn._originalOnclick = null; }
    }
    function applyEncodePreset(name) {
        const custom = _loadCustomPresets();
        const p = _ENCODE_PRESETS[name] || custom[name];
        if (!p) return;
        document.getElementById('enc-qp').value        = p.qp;
        document.getElementById('enc-preset').value    = p.preset;
        document.getElementById('enc-denoise').value   = p.denoise;
        document.getElementById('enc-crop').checked    = p.crop;
        // Custom presets also capture codec/gpu/format/lang/width — built-in
        // presets don't specify these, so applying one leaves them as-is.
        if (p.codec !== undefined) document.getElementById('enc-codec').value = p.codec;
        if (p.gpu !== undefined) document.getElementById('enc-gpu').value = p.gpu;
        if (p.format !== undefined) document.getElementById('enc-format').value = p.format;
        if (p.lang !== undefined) document.getElementById('enc-lang').value = p.lang;
        if (p.width !== undefined) document.getElementById('enc-width').value = p.width;
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

    // ── QP estimate ──────────────────────────────────────────────
    // Must match _ESTIMATE_QPS in main.py.
    const _ESTIMATE_QPS = [16, 17, 18, 19, 20, 21, 22, 23, 24];
    let _estimateSource = null;

    function _fmtBytes(n) {
        if (n == null) return '—';
        const units = ['B', 'KB', 'MB', 'GB'];
        let i = 0, v = n;
        while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
        return v.toFixed(1) + ' ' + units[i];
    }

    function _renderEstimateState(state) {
        const rows = state.results.map(r => {
            const recommended = state.suggested_qp === r.qp;
            return `
            <tr data-qp="${r.qp}" style="${recommended ? 'font-weight:600;background:color-mix(in srgb, var(--accent) 12%, transparent)' : ''}">
                <td style="padding:4px">${r.qp}${recommended ? ' ★' : ''}</td>
                <td style="padding:4px">${_fmtBytes(r.bytes)}</td>
                <td style="padding:4px">${r.pct_of_sample != null ? r.pct_of_sample + '%' : '—'}</td>
                <td style="padding:4px">${r.ssim != null ? r.ssim.toFixed(4) : '—'}</td>
                <td style="padding:4px">${r.seconds}s</td>
                <td style="padding:4px">${_fmtBytes(r.estimated_full_bytes)}</td>
                <td style="padding:4px;white-space:nowrap">
                    <button class="btn" style="padding:2px 6px;font-size:var(--fs-xs)" onclick="_useEstimatedQp(${r.qp})" title="Fill the QP field with ${r.qp} — review before starting">Use</button>
                    <button class="btn ${recommended ? 'btn-primary' : ''}" style="padding:2px 6px;font-size:var(--fs-xs)" onclick="_applyEstimatedQp(${r.qp})" title="Queue a real encode at QP ${r.qp} right now">▶ Encode</button>
                </td>
            </tr>`;
        }).join('');
        const pending = _ESTIMATE_QPS.filter(qp => !state.results.some(r => r.qp === qp));
        const pendingRows = pending.map(qp => {
            const active = state.current_qp === qp && state.status === 'encoding';
            const pct = active ? Math.max(0, Math.min(99, state.qp_progress || 0)) : 0;
            const cell = active
                ? `<div style="display:flex;align-items:center;gap:8px">
                       <div style="flex:1;height:6px;border-radius:3px;background:var(--border);overflow:hidden">
                           <div style="width:${pct}%;height:100%;background:var(--accent);transition:width 0.4s linear"></div>
                       </div>
                       <span style="font-size:var(--fs-xs);color:var(--muted);min-width:3em;text-align:right">${pct.toFixed(0)}%</span>
                   </div>`
                : 'pending…';
            return `
            <tr data-qp="${qp}" style="color:var(--muted)">
                <td style="padding:4px">${qp}</td>
                <td colspan="6" style="padding:4px">${cell}</td>
            </tr>`;
        }).join('');
        document.getElementById('estimate-rows').innerHTML = rows + pendingRows;

        const summary = document.getElementById('estimate-summary');
        if (state.status === 'error') {
            summary.innerHTML = `<span style="color:var(--danger,#c0392b)">Estimate failed: ${escHtml(state.error || 'unknown error')}</span>`;
        } else if (state.status === 'done') {
            let html = `<span>Suggested: <strong>QP ${state.suggested_qp}</strong> ★</span>`;
            html += `<button class="btn" style="padding:4px 10px;font-size:var(--fs-sm)"
                        onclick="_applyEstimateToSelectedBatch(${state.suggested_qp})"
                        title="Queue a real encode at this QP for every other file currently checked in batch mode — skips re-sampling each one">📤 Apply to selected files</button>`;
            if (state.warning) html += `<span style="color:var(--muted);font-size:var(--fs-xs)">${escHtml(state.warning)}</span>`;
            summary.innerHTML = html;
        } else {
            summary.innerHTML = '';
        }
    }

    // Wires a fresh EventSource to the live-estimate SSE stream, driving both
    // the results panel and the Estimate/Stop button pair. Shared by
    // startEstimate() (a run this tab just kicked off) and openEncodeModal()
    // (reattaching to a run already in progress from a previous modal open).
    function _attachEstimateSource() {
        if (_estimateSource) _estimateSource.close();
        document.getElementById('estimate-btn').disabled = true;
        document.getElementById('estimate-stop-btn').style.display = 'inline-block';
        _estimateSource = new EventSource('/encode/estimate/events');
        _estimateSource.onmessage = (evt) => {
            const msg = JSON.parse(evt.data);
            if (msg.type !== 'state') return;
            _renderEstimateState(msg.state);
            if (msg.state.status === 'done' || msg.state.status === 'error' || msg.state.status === 'cancelled') {
                document.getElementById('estimate-btn').disabled = false;
                document.getElementById('estimate-stop-btn').style.display = 'none';
                _estimateSource.close();
                _estimateSource = null;
            }
        };
    }

    async function _stopEstimate() {
        const btn = document.getElementById('estimate-stop-btn');
        btn.disabled = true;
        try {
            await fetch('/encode/estimate/cancel', { method: 'POST' });
        } catch (e) {
            showToast('Error: ' + escHtml(e.message), 'error');
        } finally {
            btn.disabled = false;
        }
    }

    async function startEstimate() {
        const path = document.getElementById('encode-file-path').value;
        if (!path) return;
        document.getElementById('estimate-panel').style.display = 'block';
        document.getElementById('estimate-status-line').textContent = 'Sampling 60s from the middle of the file…';
        document.getElementById('estimate-rows').innerHTML = '';
        document.getElementById('estimate-summary').innerHTML = '';

        const config = {
            preset:  document.getElementById('enc-preset').value,
            codec:   document.getElementById('enc-codec').value,
            gpu:     document.getElementById('enc-gpu').value,
            format:  document.getElementById('enc-format').value,
            denoise: document.getElementById('enc-denoise').value || null,
            crop:    document.getElementById('enc-crop').checked,
            lang:    document.getElementById('enc-lang').value.trim().toLowerCase() || 'eng',
            width:   document.getElementById('enc-width').value ? parseInt(document.getElementById('enc-width').value, 10) : null,
        };

        try {
            const resp = await fetch('/encode/estimate?path=' + encodeURIComponent(path), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
                body: JSON.stringify(config),
            });
            if (!resp.ok) {
                const txt = await resp.text();
                showToast('Estimate failed: ' + escHtml(txt), 'error');
                return;
            }
        } catch (e) {
            showToast('Error: ' + escHtml(e.message), 'error');
            return;
        }

        _attachEstimateSource();
    }

    function _useEstimatedQp(qp) {
        document.getElementById('enc-qp').value = qp;
        document.getElementById('estimate-panel').style.display = 'none';
    }

    async function _applyEstimatedQp(qp) {
        const path = document.getElementById('encode-file-path').value;
        if (!path) return;
        try {
            const resp = await fetch('/encode/estimate/apply?path=' + encodeURIComponent(path), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
                body: JSON.stringify({ qp }),
            });
            if (!resp.ok) {
                const txt = await resp.text();
                showToast('Encode failed: ' + escHtml(txt), 'error');
                return;
            }
            _encodingPaths.add(path);
            _markEncodingRows();
            closeEncodeModal();
            showToast('Encode started at QP ' + qp + ' · <a href="' + BASE_PATH + '/encode">View progress →</a>', 'success', 6000);
        } catch (e) {
            showToast('Error: ' + escHtml(e.message), 'error');
        }
    }

    // Apply this estimate's QP (and the modal's other settings) to every
    // other file currently checked in batch mode — for a season/franchise
    // where sampling each file individually would be wasteful once one
    // file's estimate looks representative of the rest.
    async function _applyEstimateToSelectedBatch(qp) {
        const currentPath = document.getElementById('encode-file-path').value;
        const others = [...document.querySelectorAll('.batch-cb:checked')]
            .map(cb => cb.closest('.file-entry')?.dataset.path)
            .filter(p => p && p !== currentPath);
        if (!others.length) {
            showToast('No other files selected — check some in batch mode first', 'error');
            return;
        }
        const config = {
            qp,
            preset:  document.getElementById('enc-preset').value,
            codec:   document.getElementById('enc-codec').value,
            gpu:     document.getElementById('enc-gpu').value,
            format:  document.getElementById('enc-format').value,
            denoise: document.getElementById('enc-denoise').value || null,
            crop:    document.getElementById('enc-crop').checked,
            lang:    document.getElementById('enc-lang').value.trim().toLowerCase() || 'eng',
            width:   document.getElementById('enc-width').value ? parseInt(document.getElementById('enc-width').value, 10) : null,
        };
        const results = await Promise.all(others.map(path =>
            fetch('/encode?path=' + encodeURIComponent(path), {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
                body: JSON.stringify(config),
            }).then(r => r.ok).catch(() => false)
        ));
        others.forEach((path, i) => { if (results[i]) _encodingPaths.add(path); });
        _markEncodingRows();
        const started = results.filter(Boolean).length;
        closeEncodeModal();
        showToast(
            `QP ${qp} applied to ${started} file${started !== 1 ? 's' : ''}` +
            (started < others.length ? ` · ${others.length - started} failed` : '') +
            ` · <a href="${BASE_PATH}/encode">View progress →</a>`,
            'success', 7000
        );
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

    // Shift-click a checkbox to select every checkbox between it and the last
    // one clicked (in visual/DOM order), skipping rows hidden by a filter.
    let _lastCheckedCb = null;

    function batchCbClick(event, cb) {
        event.stopPropagation();
        if (event.shiftKey && _lastCheckedCb) {
            const all = [...document.querySelectorAll('.batch-cb')];
            const a = all.indexOf(_lastCheckedCb);
            const b = all.indexOf(cb);
            if (a !== -1 && b !== -1) {
                const [start, end] = a < b ? [a, b] : [b, a];
                for (let i = start; i <= end; i++) {
                    const entry = all[i].closest('.file-entry');
                    if (entry && entry.style.display !== 'none') all[i].checked = cb.checked;
                }
            }
        }
        _lastCheckedCb = cb;
        updateBatchFab();
    }

    function selectAllVisible() {
        document.querySelectorAll('.file-entry').forEach(entry => {
            if (entry.style.display === 'none') return;
            const cb = entry.querySelector('.batch-cb');
            if (cb) cb.checked = true;
        });
        updateBatchFab();
    }

    // One-click "encode everything matching the current filter" — selects
    // exactly the currently-visible files (clearing any prior selection) and
    // jumps straight to the batch encode modal.
    function encodeAllFiltered() {
        document.querySelectorAll('.file-entry').forEach(entry => {
            const cb = entry.querySelector('.batch-cb');
            if (cb) cb.checked = (entry.style.display !== 'none');
        });
        updateBatchFab();
        startBatchEncode();
    }

    async function startBatchEncode() {
        const checked = [...document.querySelectorAll('.batch-cb:checked')];
        if (!checked.length) return;
        // Open encode modal pre-filled with count, then confirm
        const modal = document.getElementById('encode-modal');
        document.getElementById('encode-modal-name').textContent = `${checked.length} file${checked.length !== 1 ? 's' : ''}`;
        document.getElementById('encode-file-path').value = '';  // sentinel: batch mode
        _renderCustomPresetButtons();
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

    // ── Drag-and-drop batch move onto the directory tree ───────────
    // /move-to-folder always moves into a subfolder of the dragged file's OWN
    // current directory (dest = src.parent / folder_name) — it can't move a
    // file into an arbitrary directory elsewhere in the tree. So a drop is
    // only accepted onto a subfolder that's a visible sibling of the file in
    // the same .dir-block; anything else would silently do the wrong thing
    // (create/use a same-named folder under the file's own directory instead
    // of actually moving it into the folder that was visually dropped on).
    function _onFileDragStart(event, el) {
        const checked = [...document.querySelectorAll('.batch-cb:checked')];
        const paths = checked.length && el.querySelector('.batch-cb')?.checked
            ? checked.map(cb => cb.closest('.file-entry')?.dataset.path).filter(Boolean)
            : [el.dataset.path];
        event.dataTransfer.effectAllowed = 'move';
        event.dataTransfer.setData('application/json', JSON.stringify(paths));
        event.dataTransfer.setData('text/plain', paths.join('\n'));
    }

    function _onDirDragOver(event, el) {
        if (!event.dataTransfer.types.includes('application/json')) return;
        event.preventDefault();
        event.dataTransfer.dropEffect = 'move';
        el.classList.add('drop-target');
    }

    function _onDirDragLeave(event, el) {
        el.classList.remove('drop-target');
    }

    async function _onDirDrop(event, el) {
        event.preventDefault();
        el.classList.remove('drop-target');
        let paths;
        try { paths = JSON.parse(event.dataTransfer.getData('application/json')); }
        catch (e) { return; }
        if (!paths || !paths.length) return;

        const sameDirBlock = paths.every(p => {
            const row = document.querySelector(`.file-entry[data-path="${CSS.escape(p)}"]`);
            return row && row.closest('.dir-block') === el.closest('.dir-block');
        });
        if (!sameDirBlock) {
            showToast("Can only drop onto a subfolder shown under the file's own directory", 'error');
            return;
        }

        const folderName = el.dataset.dirName;
        const resp = await fetch('/move-to-folder', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'X-Delete-Token': DELETE_TOKEN },
            body: JSON.stringify({ paths, folder: folderName }),
        });
        const result = await resp.json();
        if (!resp.ok) { showToast('Move failed: ' + (result.detail || resp.status), 'error'); return; }
        const moved = result.moved?.length ?? 0;
        const failed = result.errors?.length ?? 0;
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
        _renderCustomPresetButtons();
        applyEncodePreset('quality');
        _applyFolderDefault(rawPath);  // override the preset if this folder has remembered settings
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
                _saveFolderDefault(_folderEncodePath, config);
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
        // Poster thumbnail, hotlinked directly from TMDB's image CDN — no
        // server-side fetch/caching, the browser handles it like any <img>.
        const poster = row.querySelector('.poster-thumb');
        if (poster) {
            if (info.poster_path) {
                poster.src = 'https://image.tmdb.org/t/p/w92' + info.poster_path;
                poster.style.display = '';
            } else {
                poster.removeAttribute('src');
                poster.style.display = 'none';
            }
        }
    }

    function _imdbClearBadge(row) {
        const tag = row.querySelector('.imdb-tag');
        if (tag) { tag.textContent = 'IMDb'; tag.classList.remove('matched'); }
        const ratingBadge = row.querySelector('.imdb-rating-badge');
        if (ratingBadge) { ratingBadge.style.display = 'none'; ratingBadge.textContent = ''; }
        const poster = row.querySelector('.poster-thumb');
        if (poster) { poster.removeAttribute('src'); poster.style.display = 'none'; }
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

    class TaskQueue {
        constructor(concurrency = 2) {
            this.concurrency = concurrency;
            this.running = 0;
            this.queue = [];
        }
        add(fn) {
            return new Promise((resolve, reject) => {
                this.queue.push({ fn, resolve, reject });
                this.next();
            });
        }
        next() {
            while (this.running < this.concurrency && this.queue.length > 0) {
                const { fn, resolve, reject } = this.queue.shift();
                this.running++;
                Promise.resolve()
                    .then(() => fn())
                    .then(resolve)
                    .catch(reject)
                    .finally(() => {
                        this.running--;
                        this.next();
                    });
            }
        }
    }

    const _dirSizeQueue = new TaskQueue(2);
    const _dirCheckQueue = new TaskQueue(1);

    // ── File scan progress via SSE ───────────────────────────────
    // staleTable: existing .files-table to update in-place (may be null for fresh scans)
    function startFileScan(el, staleTable, onDone) {
        if (el._scanStarted) {
            if (onDone) onDone();
            return;
        }
        el._scanStarted = true;
        const path = el.dataset.path;
        const total = parseInt(el.dataset.total) || 0;
        const source = new EventSource('/dir-scan?path=' + encodeURIComponent(path));

        let table = staleTable;
        if (!table) {
            table = document.createElement('div');
            table.className = 'files-table';
            const block = el.closest('.dir-block');
            if (block) {
                block.appendChild(table);
            }
        }

        source.onmessage = (e) => {
            const data = JSON.parse(e.data);
            if (data.type === 'start' || data.type === 'progress') {
                const done = data.done || 0;
                const tot  = data.total || total;
                el.querySelector('.scan-done').textContent = done;
                el.querySelector('.scan-total').textContent = tot;
                el.querySelector('.scan-bar-fill').style.width = (tot > 0 ? done / tot * 100 : 0) + '%';

                if (table && data.file_html) {
                    const tmp = document.createElement('div');
                    tmp.innerHTML = data.file_html;
                    const newRow = tmp.querySelector('.file-entry');
                    if (newRow) {
                        const existing = table.querySelector(
                            `.file-entry[data-path="${CSS.escape(newRow.dataset.path)}"]`
                        );
                        if (existing) {
                            existing.replaceWith(newRow);
                        } else {
                            table.appendChild(newRow);
                        }
                    }
                }
            } else if (data.type === 'done') {
                source.close();
                const block = el.closest('.dir-block');
                el.remove();
                if (table) {
                    if (data.html) {
                        const tmp = document.createElement('div');
                        tmp.innerHTML = data.html;
                        const newTable = tmp.querySelector('.files-table');
                        if (newTable) table.replaceWith(newTable);
                        else table.remove();
                    } else {
                        table.remove();
                    }
                } else if (data.html) {
                    const tmp = document.createElement('div');
                    tmp.innerHTML = data.html;
                    block && block.appendChild(tmp.firstElementChild);
                }
                applyFilters();
                rebuildDynamicFilters();
                loadImdbBadges(path);
                if (onDone) onDone();
            }
        };

        source.onerror = () => {
            source.close();
            el.remove();
            if (table) table.classList.remove('stale');
            if (onDone) onDone();
        };
    }

    function enqueueFreshScan(el) {
        if (el._scanEnqueued) return;
        el._scanEnqueued = true;
        _dirCheckQueue.add(() => {
            return new Promise((resolve) => {
                startFileScan(el, null, resolve);
            });
        }).catch(() => {});
    }

    // ── Lazy dir sizes ────────────────────────────────────────────
    function loadDirSizes(container) {
        const entries = container.querySelectorAll('.dir-entry[data-size="-1"]');
        entries.forEach(el => {
            const p = el.dataset.dirPath;
            if (!p) return;
            _dirSizeQueue.add(() =>
                fetch('/dir-size?path=' + p)
                    .then(r => r.json())
                    .then(d => {
                        el.dataset.size = d.size;
                        const span = el.querySelector('.dir-size-inline');
                        if (span) span.textContent = d.human;
                    })
            ).catch(() => {});
        });
    }

    // ── Dir-check: verify cached files are still up to date ──────
    function startDirCheck(el) {
        if (el._checkStarted) return;
        el._checkStarted = true;
        const path = el.dataset.path;
        _dirCheckQueue.add(() =>
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
                    scanEl._scanEnqueued = true;
                    scanEl.innerHTML =
                        '<div class="scan-progress">' +
                        '<span class="scan-text">updating <span class="scan-done">0</span>' +
                        '/<span class="scan-total">' + count + '</span></span>' +
                        '<div class="scan-bar"><div class="scan-bar-fill"></div></div>' +
                        '</div>';
                    if (table) {
                        table.before(scanEl);
                    } else {
                        el.replaceWith(scanEl);
                    }
                    el.remove();
                    return new Promise((resolve) => {
                        startFileScan(scanEl, table || null, resolve);
                    });
                })
                .catch(() => el.remove())
        );
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
            _applySort(sort);
        }
        _refreshSortArrows();
    }

    restoreFromUrl();

    // The file/dir listing loads asynchronously via htmx (hx-trigger="load"
    // on the root container, "toggle once" for lazily-expanded subdirs), so
    // at the point restoreFromUrl() runs there's nothing in the DOM yet to
    // sort. Re-apply whatever sort is currently active every time htmx swaps
    // in new listing content, so both the initial load and later-expanded
    // subdirectories end up in the URL-restored (or last clicked) order.
    document.body.addEventListener('htmx:afterSwap', () => {
        const activeBtn = document.querySelector('#global-sort-bar .sort-btn.active');
        if (activeBtn) _applySort(activeBtn.dataset.sort);
        _applyWatchBadges();
    });

    // Kick off scans/checks for placeholders on initial page load
    document.querySelectorAll('.files-scanning').forEach(enqueueFreshScan);
    document.querySelectorAll('.files-checking').forEach(startDirCheck);
    loadDirSizes(document);
    loadFolderHeader();

    // ── Live encode job-count badge + progress fill on the Jobs link ───
    // Only present on pages with the Jobs nav button (index.html).
    if (document.getElementById('encode-jobs-count')) {
        const _refreshJobsBadge = () => {
            fetch('/encode/active-count')
                .then(r => r.ok ? r.json() : { count: 0, avg_progress: null })
                .then(d => {
                    const badge = document.getElementById('encode-jobs-count');
                    const dot   = document.getElementById('encode-jobs-dot');
                    const fill  = document.getElementById('encode-jobs-fill');
                    if (!badge || !dot || !fill) return;
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
        _refreshJobsBadge();
        setInterval(_refreshJobsBadge, 5000);
    }

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
                if (node.classList.contains('files-scanning')) { enqueueFreshScan(node); continue; }
                if (node.classList.contains('files-checking')) { startDirCheck(node); continue; }
                node.querySelectorAll('.files-scanning').forEach(enqueueFreshScan);
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

    // Recognized search tokens — e.g. "1080p x265 -hdr" narrows by resolution/
    // codec/HDR directly in the search box instead of needing the filter-button
    // bar, with a leading "-" excluding instead of requiring. Anything else is
    // treated as a plain substring match against the filename (AND-combined
    // across multiple such terms).
    const _RES_TOKENS = { '4k': '4k', '2160p': '4k', 'uhd': '4k', '1080p': '1080p', 'fhd': '1080p',
                           '720p': '720p', 'hd': '720p', '480p': 'sd', 'sd': 'sd' };
    const _CODEC_TOKENS = { 'h265': 'h265', 'x265': 'h265', 'hevc': 'h265',
                             'h264': 'h264', 'x264': 'h264', 'avc': 'h264',
                             'av1': 'av1', 'vp9': 'vp9' };
    const _HDR_TOKENS = { 'hdr': 'HDR', 'hdr10': 'HDR', 'dv': 'DV', 'dolbyvision': 'DV',
                           'hlg': 'HLG', 'sdr': 'SDR' };

    function _parseSearchQuery(raw) {
        const q = {
            nameTerms: [], includeRes: new Set(), excludeRes: new Set(),
            includeCodec: new Set(), excludeCodec: new Set(),
            includeHdr: new Set(), excludeHdr: new Set(),
        };
        for (const rawTok of raw.trim().toLowerCase().split(/\s+/).filter(Boolean)) {
            const negate = rawTok.startsWith('-');
            const tok = negate ? rawTok.slice(1) : rawTok;
            if (!tok) continue;
            if (_RES_TOKENS[tok])          (negate ? q.excludeRes   : q.includeRes).add(_RES_TOKENS[tok]);
            else if (_CODEC_TOKENS[tok])   (negate ? q.excludeCodec : q.includeCodec).add(_CODEC_TOKENS[tok]);
            else if (_HDR_TOKENS[tok])     (negate ? q.excludeHdr   : q.includeHdr).add(_HDR_TOKENS[tok]);
            else if (!negate)              q.nameTerms.push(tok);
        }
        return q;
    }

    function applyFilters() {
        const rawTerm = document.getElementById('search-input').value || '';
        const q = _parseSearchQuery(rawTerm);
        document.querySelectorAll('.files-table > .file-entry').forEach(e => {
            const name = e.dataset.name || '';
            if (q.nameTerms.length && !q.nameTerms.every(t => name.includes(t))) { e.style.display = 'none'; return; }
            const res = resTag(parseInt(e.dataset.width) || 0, parseInt(e.dataset.height) || 0);
            if (q.includeRes.size && !q.includeRes.has(res)) { e.style.display = 'none'; return; }
            if (q.excludeRes.has(res)) { e.style.display = 'none'; return; }
            const codec = _normalizeCodecVal(e.dataset.vcodec || '');
            if (q.includeCodec.size && !q.includeCodec.has(codec)) { e.style.display = 'none'; return; }
            if (q.excludeCodec.has(codec)) { e.style.display = 'none'; return; }
            const hdr = e.dataset.hdr || '';
            if (q.includeHdr.size && !q.includeHdr.has(hdr)) { e.style.display = 'none'; return; }
            if (q.excludeHdr.has(hdr)) { e.style.display = 'none'; return; }
            if (_filterState.codec.size > 0 && ![..._filterState.codec].some(v => codecMatches(e.dataset.vcodec || '', v))) { e.style.display = 'none'; return; }
            if (_filterState.res.size > 0 && !_filterState.res.has(res)) { e.style.display = 'none'; return; }
            if (_filterState.ext.size > 0 && !_filterState.ext.has(e.dataset.ext || '')) { e.style.display = 'none'; return; }
            if (_filterState.audio.size > 0 && !_filterState.audio.has(e.dataset.audio || '')) { e.style.display = 'none'; return; }
            if (_filterState.hdr.size > 0 && !_filterState.hdr.has(e.dataset.hdr || '')) { e.style.display = 'none'; return; }
            if (_filterState.imdb.has('unmatched') && e.dataset.imdbTitle) { e.style.display = 'none'; return; }
            e.style.display = '';
        });
        document.querySelectorAll('.dirs-container > .dir-entry')
                .forEach(e => {
                    const name = e.dataset.name || '';
                    e.style.display = (!q.nameTerms.length || q.nameTerms.every(t => name.includes(t))) ? '' : 'none';
                });
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

    function _applySort(key) {
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
            const dc = block.querySelector(':scope > .dirs-container');
            if (dc) reorder(dc, ':scope > .dir-entry');
            const ft = block.querySelector(':scope > .files-table');
            if (ft) reorder(ft, ':scope > .file-entry');
        });
    }

    function sortAll(btn) {
        const key = btn.dataset.sort;
        const wasActive = btn.classList.contains('active');
        _sortDir = wasActive ? (_sortDir === 'asc' ? 'desc' : 'asc') : (_sortDefaults[key] ?? 'asc');
        _applySort(key);
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
        _recordLastPlayed(_playerPath);
        _updateWatchedBtn();
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

    // ── Lightweight watch tracking (localStorage, no backend) ──────
    const _WATCH_HISTORY_KEY = 'mediastat_watch_history';

    function _loadWatchHistory() {
        try {
            const raw = localStorage.getItem(_WATCH_HISTORY_KEY);
            return raw ? JSON.parse(raw) : {};
        } catch (e) { return {}; }
    }

    function _saveWatchEntry(path, updates) {
        try {
            const all = _loadWatchHistory();
            all[path] = { ...(all[path] || {}), ...updates };
            localStorage.setItem(_WATCH_HISTORY_KEY, JSON.stringify(all));
        } catch (e) { /* localStorage unavailable — just skip remembering */ }
        _applyWatchBadges();
    }

    function _recordLastPlayed(path) {
        _saveWatchEntry(path, { lastPlayed: Date.now() });
    }

    function toggleWatched() {
        if (!_playerPath) return;
        const current = _loadWatchHistory()[_playerPath] || {};
        _saveWatchEntry(_playerPath, { watched: !current.watched });
        _updateWatchedBtn();
    }

    function _updateWatchedBtn() {
        const btn = document.getElementById('watched-btn');
        if (!btn || !_playerPath) return;
        const watched = !!(_loadWatchHistory()[_playerPath] || {}).watched;
        btn.classList.toggle('active', watched);
        btn.textContent = watched ? '👁 Watched' : '👁 Mark watched';
    }

    function _fmtRelativeTime(ms) {
        const days = Math.floor((Date.now() - ms) / 86400000);
        if (days <= 0) return 'today';
        if (days === 1) return 'yesterday';
        if (days < 30) return `${days}d ago`;
        const months = Math.floor(days / 30);
        return months < 12 ? `${months}mo ago` : `${Math.floor(months / 12)}y ago`;
    }

    // Decorate each visible file row with a small watched/last-played badge.
    // Re-run after htmx loads new rows (see htmx:afterSwap listener) since
    // watch state lives client-side and rows are rendered server-side.
    function _applyWatchBadges() {
        const history = _loadWatchHistory();
        document.querySelectorAll('.file-entry[data-path]').forEach(entry => {
            const info = history[entry.dataset.path];
            let badge = entry.querySelector('.watch-badge');
            if (!info || (!info.watched && !info.lastPlayed)) {
                if (badge) badge.remove();
                return;
            }
            if (!badge) {
                badge = document.createElement('span');
                badge.className = 'watch-badge';
                badge.style.cssText = 'font-size:var(--fs-xs);color:var(--muted);margin-left:6px';
                const nameCell = entry.querySelector('.file-name-cell');
                if (nameCell) nameCell.appendChild(badge);
            }
            badge.textContent = info.watched
                ? '✓ watched'
                : (info.lastPlayed ? `▶ ${_fmtRelativeTime(info.lastPlayed)}` : '');
        });
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

                if (group.length > 1) {
                    const keepBtn = document.createElement('button');
                    keepBtn.className = 'delete-btn';
                    keepBtn.title = 'Keep only this file — deletes every other file in this group';
                    keepBtn.textContent = '✓ Keep only this';
                    keepBtn.onclick = () => _keepOnlyThisDupe(div, group, file);
                    row.appendChild(keepBtn);
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

    async function _keepOnlyThisDupe(groupEl, group, keepFile) {
        const others = group.map(g => g.file).filter(f => f.path !== keepFile.path);
        if (!others.length) return;
        const keepName = keepFile.path.split('/').pop();
        const otherNames = others.map(f => f.path.split('/').pop()).join('\n');
        if (!confirm(`Keep "${keepName}" and delete ${others.length} other file(s)?\n\n${otherNames}`)) return;

        const results = await Promise.all(others.map(f =>
            fetch('/file?path=' + encodeURIComponent(f.path), {
                method: 'DELETE',
                headers: { 'X-Delete-Token': DELETE_TOKEN },
            }).then(r => r.ok).catch(() => false)
        ));
        const deleted = results.filter(Boolean).length;
        const failed = others.length - deleted;

        // Best-effort: drop the deleted files' rows from the main browser table too
        others.forEach((f, i) => {
            if (!results[i]) return;
            const mainRow = document.querySelector(`.file-entry[data-path="${CSS.escape(f.path)}"]`);
            if (mainRow) mainRow.remove();
        });

        if (deleted === others.length) {
            groupEl.remove();
        } else {
            runDupesScan();  // partial failure — refresh from server to reflect real state
        }
        showToast(
            `Kept "${keepName}" · deleted ${deleted}${failed ? ` · ${failed} failed` : ''}`,
            failed ? 'error' : 'success', 5000
        );
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
