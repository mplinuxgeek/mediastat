# Mobile-Friendly Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all four pages (Library, Encode Jobs, Databases, IMDb Scan) usable on a phone-width screen, without changing anything above a 700px viewport.

**Architecture:** Pure CSS media queries at the existing `max-width: 700px` breakpoint, following the pattern already established in `app.css`. New JS is added only for interactions with no CSS-only equivalent (two dropdown toggles, both using the outside-click-close pattern already used by the IMDb popup in `app.js`). No backend/route logic changes except passing one new template variable (`active_tab`) from four existing route handlers.

**Tech Stack:** Jinja2 templates, plain CSS (no preprocessor), vanilla JS (no framework) — matches the existing codebase exactly.

## Global Constraints

- Breakpoint is `max-width: 700px` everywhere (matches the existing partial pass in `app.css` line 1024) — do not introduce a second breakpoint value.
- Every new CSS rule for mobile behavior must live inside a `@media (max-width: 700px)` block (or be an always-hidden default that a mobile block reveals). Nothing above 700px may change visually.
- `app.css` is loaded only by `index.html`. `encode.html`, `databases.html`, and `imdb_scan.html` each have their own inline `<style>` block and no shared stylesheet — this is the existing pattern (e.g. `.back-link`/`.page-title` are already duplicated three times); follow it rather than introducing a new shared CSS file.
- `app.js` is loaded only by `index.html`. The other three pages have their own inline `<script>` blocks.
- No pytest coverage applies (CSS/template/JS only, no backend behavior changes) — this was confirmed in the approved spec. Verification is via `grep` against rendered/static output (deterministic, scriptable) plus a manual visual check.
- Reference spec: `docs/superpowers/specs/2026-07-12-mobile-responsive-design.md`.

---

### Task 1: File table mobile simplification

**Files:**
- Modify: `src/templates/_files_table.html:26,29,31,32`
- Modify: `src/static/app.css:1024-1042` (existing mobile media query block)

**Interfaces:**
- Produces: CSS classes `res-badge-desktop`, `res-badge-mobile`, `badge-cell-ext`, `badge-cell-audio`, `badge-cell-duration` on file-row badge-cells — no other task depends on these.

- [ ] **Step 1: Add the simplified-resolution label and new classes to `_files_table.html`**

Replace line 26:
```html
    <span class="badge-cell"><span class="tag {{ file.ext_class }}">{{ file.ext }}</span></span>
```
with:
```html
    <span class="badge-cell badge-cell-ext"><span class="tag {{ file.ext_class }}">{{ file.ext }}</span></span>
```

Replace line 29:
```html
    <span class="badge-cell">{% if file.width and file.height %}<span class="tag {{ file.res_class }}">{{ file.width }}×{{ file.height }}</span>{% endif %}</span>
```
with:
```html
    {% set _w = file.width or 0 %}
    {% set _h = file.height or 0 %}
    {% set mobile_res_label = '4K' if (_w >= 3000 or _h >= 2000) else ('1080p' if (_w >= 1700 or _h >= 900) else ('720p' if (_w >= 900 or _h >= 500) else ('SD' if (_w or _h) else ''))) %}
    <span class="badge-cell res-badge-desktop">{% if file.width and file.height %}<span class="tag {{ file.res_class }}">{{ file.width }}×{{ file.height }}</span>{% endif %}</span>
    <span class="badge-cell res-badge-mobile">{% if mobile_res_label %}<span class="tag {{ file.res_class }}">{{ mobile_res_label }}</span>{% endif %}</span>
```

Replace line 31 (now shifted down 3 lines, was line 31):
```html
    <span class="badge-cell">{% if file.audio_codec and file.audio_codec != 'N/A' %}<span class="tag audio">{{ file.audio_codec }}</span>{% endif %}</span>
```
with:
```html
    <span class="badge-cell badge-cell-audio">{% if file.audio_codec and file.audio_codec != 'N/A' %}<span class="tag audio">{{ file.audio_codec }}</span>{% endif %}</span>
```

Replace line 32 (was line 32):
```html
    <span class="badge-cell">{% if file.duration_label %}<span class="tag duration">{{ file.duration_label }}</span>{% endif %}</span>
```
with:
```html
    <span class="badge-cell badge-cell-duration">{% if file.duration_label %}<span class="tag duration">{{ file.duration_label }}</span>{% endif %}</span>
```

The `4k`/`1080p`/`720p`/`SD` thresholds mirror `resTag()` in `src/static/app.js:2095-2101` exactly — keep them in sync if that function's thresholds ever change.

- [ ] **Step 2: Verify the template renders without a Jinja error**

Run:
```bash
cd /home/martin/dev/mediastat && python3 -c "
import sys; sys.path.insert(0, 'src')
from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader('src/templates'))
tpl = env.get_template('_files_table.html')
files = [{'name':'a.mkv','stem':'a','path':'/a.mkv','ext':'mkv','ext_class':'ext-mkv',
          'size':100,'human_size':'100 B','size_class':'size-ok','mtime':0,
          'video_codec':'hevc','codec_class':'codec-h265','width':3840,'height':2160,
          'res_class':'res-4k','hdr_type':'HDR','hdr_class':'hdr-hdr',
          'audio_codec':'AAC','duration_label':'1h 30m','duration_sec':5400,'duration_min':90,
          'needs_transcode':False}]
out = tpl.render(files=files)
assert 'res-badge-desktop' in out and 'res-badge-mobile' in out, 'missing badge classes'
assert '4K' in out, 'expected 4K label for a 3840x2160 file'
assert 'badge-cell-ext' in out and 'badge-cell-audio' in out and 'badge-cell-duration' in out
print('OK')
"
```
Expected: `OK` printed, no traceback.

- [ ] **Step 3: Add the mobile CSS rules**

In `src/static/app.css`, the default (always-on, not inside any media query) rule for `.res-badge-mobile` must exist so it stays hidden above 700px. Add it immediately before the existing `/* ── Basic responsive layout ── */` comment block (currently at line 1015):

```css
        .res-badge-mobile { display: none; }

        /* ── Basic responsive layout ──────────────────────────────────
```

Then, inside the existing `@media (max-width: 700px) { ... }` block (currently lines 1024-1042), add these rules right before the closing `}` (after the existing `.modal { padding: 16px 18px; }` line):

```css
            .poster-thumb, .imdb-rating-badge,
            .badge-cell-ext, .badge-cell-audio, .badge-cell-duration,
            .res-badge-desktop { display: none; }
            .res-badge-mobile { display: inline-block; }
```

- [ ] **Step 4: Verify the CSS rules are present and well-formed**

Run:
```bash
grep -n "res-badge-mobile { display: none; }" src/static/app.css
grep -n "badge-cell-ext, .badge-cell-audio, .badge-cell-duration" src/static/app.css
python3 -c "
css = open('src/static/app.css').read()
assert css.count('{') == css.count('}'), 'unbalanced braces'
print('OK')
"
```
Expected: both `grep` calls print a matching line, `OK` printed.

- [ ] **Step 5: Commit**

```bash
git add src/templates/_files_table.html src/static/app.css
git commit -m "feat: simplify file-table badges on mobile

Hide the poster preview, IMDb rating badge, extension, audio, and
duration badges below 700px, and swap the exact WxH resolution badge
for a simplified 4K/1080p/720p/SD tier (thresholds mirror resTag() in
app.js). Desktop is unaffected."
```

---

### Task 2: Mobile tab bar on all four pages

**Files:**
- Create: `src/templates/_mobile_tabs.html`
- Modify: `src/templates/index.html:25,58-66` (header)
- Modify: `src/templates/encode.html:278-282` (header), `:7-275` (style block)
- Modify: `src/templates/databases.html:129-132` (header), `:7-126` (style block)
- Modify: `src/templates/imdb_scan.html:150-153` (header), `:7-147` (style block)
- Modify: `src/main.py:1631-1637` (`imdb_scan_page`), `:2216-2224` (`index`), `:4153-4158` (`encode_page`), `:4160-4164` (`databases_page`)
- Modify: `src/static/app.css` (new rules, always-on defaults + mobile block)

**Interfaces:**
- Produces: template context variable `active_tab` (one of `"library"`, `"jobs"`, `"imdb"`, `"db"`) passed into all four page templates.
- Produces: CSS classes `.mobile-tabs`, `.mobile-tab`, `.mobile-tab.active`, `.nav-link-desktop` — Task 4 (tools menu) builds on `.nav-link-desktop` existing already on the three header nav links.

- [ ] **Step 1: Create the tab-bar partial**

Create `src/templates/_mobile_tabs.html`:
```html
<nav class="mobile-tabs">
    <a href="{{ ingress_path }}/" class="mobile-tab{{ ' active' if active_tab == 'library' else '' }}">Library</a>
    <a href="{{ ingress_path }}/encode" class="mobile-tab{{ ' active' if active_tab == 'jobs' else '' }}">Jobs</a>
    <a href="{{ ingress_path }}/imdb-scan" class="mobile-tab{{ ' active' if active_tab == 'imdb' else '' }}">IMDb</a>
    <a href="{{ ingress_path }}/databases" class="mobile-tab{{ ' active' if active_tab == 'db' else '' }}">DB</a>
</nav>
```

- [ ] **Step 2: Pass `active_tab` from each route handler in `src/main.py`**

In `index()` (currently `src/main.py:2216-2224`), change:
```python
    return templates.TemplateResponse("index.html", {
        "request": request,
        "media_root": str(current_root),
        "configured_dirs": CONFIGURED_DIRS,
        "delete_token": DELETE_TOKEN,
        "error": request.query_params.get("error"),
        "ingress_path": request.state.ingress_path,
    })
```
to:
```python
    return templates.TemplateResponse("index.html", {
        "request": request,
        "media_root": str(current_root),
        "configured_dirs": CONFIGURED_DIRS,
        "delete_token": DELETE_TOKEN,
        "error": request.query_params.get("error"),
        "ingress_path": request.state.ingress_path,
        "active_tab": "library",
    })
```

In `encode_page()` (currently `src/main.py:4153-4158`), change:
```python
    return templates.TemplateResponse("encode.html", {
        "request": request,
        "delete_token": DELETE_TOKEN,
        "ingress_path": request.state.ingress_path,
    })
```
to:
```python
    return templates.TemplateResponse("encode.html", {
        "request": request,
        "delete_token": DELETE_TOKEN,
        "ingress_path": request.state.ingress_path,
        "active_tab": "jobs",
    })
```

In `databases_page()` (currently `src/main.py:4160-4164`), change:
```python
    return templates.TemplateResponse("databases.html", {
        "request": request,
        "ingress_path": request.state.ingress_path,
    })
```
to:
```python
    return templates.TemplateResponse("databases.html", {
        "request": request,
        "ingress_path": request.state.ingress_path,
        "active_tab": "db",
    })
```

In `imdb_scan_page()` (currently `src/main.py:1631-1637`), change:
```python
    return templates.TemplateResponse("imdb_scan.html", {
        "request": request,
        "ingress_path": request.state.ingress_path,
        "delete_token": DELETE_TOKEN,
        "tmdb_configured": bool(_config.get("tmdb_api_key")),
    })
```
to:
```python
    return templates.TemplateResponse("imdb_scan.html", {
        "request": request,
        "ingress_path": request.state.ingress_path,
        "delete_token": DELETE_TOKEN,
        "tmdb_configured": bool(_config.get("tmdb_api_key")),
        "active_tab": "imdb",
    })
```

- [ ] **Step 3: Include the partial in all four templates**

In `src/templates/index.html`, after the `</header>` line (currently line 68), add:
```html
    {% include "_mobile_tabs.html" %}
```

In `src/templates/encode.html`, after `</header>` (currently line 282), add:
```html
{% include "_mobile_tabs.html" %}
```

In `src/templates/databases.html`, after `</header>` (currently line 132), add:
```html
{% include "_mobile_tabs.html" %}
```

In `src/templates/imdb_scan.html`, after `</header>` (currently line 153), add:
```html
{% include "_mobile_tabs.html" %}
```

- [ ] **Step 4: Mark the three desktop nav links in `index.html`'s header**

In `src/templates/index.html`, the three nav links (currently lines 58, 65, 66 — note: line numbers shifted by whatever Task 1 changes elsewhere didn't touch this file, so these are still accurate) go from:
```html
        <a href="{{ ingress_path }}/encode" class="btn" id="encode-jobs-btn" title="View encode jobs" style="text-decoration:none;position:relative;overflow:hidden">
```
to:
```html
        <a href="{{ ingress_path }}/encode" class="btn nav-link-desktop" id="encode-jobs-btn" title="View encode jobs" style="text-decoration:none;position:relative;overflow:hidden">
```
and:
```html
        <a href="{{ ingress_path }}/imdb-scan" class="btn" title="IMDb background scan" style="margin-left:auto;text-decoration:none">⬡ IMDb</a>
        <a href="{{ ingress_path }}/databases" class="btn" title="Databases &amp; system info" style="text-decoration:none">⬡ DB</a>
```
to:
```html
        <a href="{{ ingress_path }}/imdb-scan" class="btn nav-link-desktop" title="IMDb background scan" style="margin-left:auto;text-decoration:none">⬡ IMDb</a>
        <a href="{{ ingress_path }}/databases" class="btn nav-link-desktop" title="Databases &amp; system info" style="text-decoration:none">⬡ DB</a>
```
(Only the `class` attribute changes on these three tags — nothing else.)

- [ ] **Step 5: Add tab-bar CSS to `app.css`**

Add this immediately before the `/* ── Basic responsive layout ── */` comment (same insertion point as Task 1 Step 3 — if Task 1 already ran, add below its `.res-badge-mobile` rule):

```css
        .mobile-tabs { display: none; }
```

Then inside the `@media (max-width: 700px) { ... }` block, add:

```css
            .mobile-tabs {
                display: flex;
                overflow-x: auto;
                white-space: nowrap;
                gap: 4px;
                padding: 6px 12px;
                background: var(--surface);
                border-bottom: 1px solid var(--border);
                -webkit-overflow-scrolling: touch;
            }
            .mobile-tab {
                flex-shrink: 0;
                padding: 6px 14px;
                border-radius: var(--r-md);
                color: var(--muted);
                text-decoration: none;
                font-size: var(--fs-sm);
                border: 1px solid transparent;
            }
            .mobile-tab.active { color: var(--accent); border-color: var(--accent); }
            .nav-link-desktop { display: none; }
```

- [ ] **Step 6: Duplicate the same tab-bar CSS into the three secondary pages**

`encode.html`, `databases.html`, and `imdb_scan.html` each have their own `<style>` block with no shared stylesheet (existing pattern — e.g. `.back-link` is already triplicated). Add the identical block from Step 5 into each of their `<style>` blocks, right before the closing `</style>` tag:

For `encode.html` (closing `</style>` currently at line 275), insert before it:
```css
        .mobile-tabs { display: none; }
        @media (max-width: 700px) {
            .mobile-tabs {
                display: flex;
                overflow-x: auto;
                white-space: nowrap;
                gap: 4px;
                padding: 6px 12px;
                background: var(--surface);
                border-bottom: 1px solid var(--border);
                -webkit-overflow-scrolling: touch;
            }
            .mobile-tab {
                flex-shrink: 0;
                padding: 6px 14px;
                border-radius: var(--r-md);
                color: var(--muted);
                text-decoration: none;
                font-size: var(--fs-sm);
                border: 1px solid transparent;
            }
            .mobile-tab.active { color: var(--accent); border-color: var(--accent); }
        }
```

Do the same for `databases.html` (closing `</style>` currently at line 126) and `imdb_scan.html` (closing `</style>` currently at line 147) — identical CSS block, no `.nav-link-desktop` rule needed there since those pages don't have the desktop nav links.

- [ ] **Step 7: Verify all four pages include the partial and render it**

```bash
cd /home/martin/dev/mediastat && python3 -c "
import sys; sys.path.insert(0, 'src')
from jinja2 import Environment, FileSystemLoader
env = Environment(loader=FileSystemLoader('src/templates'))
for name, tab in [('index.html','library'), ('encode.html','jobs'), ('databases.html','db'), ('imdb_scan.html','imdb')]:
    tpl = env.get_template(name)
    out = tpl.render(ingress_path='', active_tab=tab, media_root='/media', configured_dirs=[],
                      delete_token='x', error=None, tmdb_configured=False)
    assert 'mobile-tabs' in out, f'{name}: missing mobile-tabs'
    assert f'mobile-tab active\">Library' in out or tab != 'library' or 'active\">Library' in out
    print(name, 'OK')
"
```
Expected: `index.html OK`, `encode.html OK`, `databases.html OK`, `imdb_scan.html OK` — no traceback. (If any template needs additional context variables to render standalone, add minimal stand-in values the same way `configured_dirs=[]` etc. were added above — check the template's other `{{ }}` references for what's required.)

- [ ] **Step 8: Commit**

```bash
git add src/templates/_mobile_tabs.html src/templates/index.html src/templates/encode.html \
        src/templates/databases.html src/templates/imdb_scan.html src/main.py src/static/app.css
git commit -m "feat: add mobile sliding tab bar for page navigation

New _mobile_tabs.html partial (Library/Jobs/IMDb/DB), included on all
four pages, visible only below 700px as a horizontally-scrollable strip
with the current page highlighted. index.html's desktop Jobs/IMDb/DB
header links are hidden on mobile (nav-link-desktop) since the tab bar
replaces them there. Desktop nav is unchanged on all four pages."
```

---

### Task 3: Filter/sort bar mobile dropdown

**Files:**
- Modify: `src/templates/index.html:70` (before `#global-sort-bar`)
- Modify: `src/static/app.css`
- Modify: `src/static/app.js`

**Interfaces:**
- Consumes: none from other tasks.
- Produces: `toggleMobileFilters()` global function (referenced only from this task's own markup).

- [ ] **Step 1: Add the toggle button before the sort bar in `index.html`**

Before the existing line (currently line 70):
```html
    <div id="global-sort-bar" class="sort-bar" style="border-bottom:1px solid var(--border);padding:4px 12px;">
```
add:
```html
    <button id="mobile-filter-toggle" class="btn" onclick="toggleMobileFilters()" title="Show filters">⚗ Filters</button>
    <div id="global-sort-bar" class="sort-bar" style="border-bottom:1px solid var(--border);padding:4px 12px;">
```

- [ ] **Step 2: Add the JS toggle function**

In `src/static/app.js`, find `function toggleFilter(btn) {` (currently around line 2260) and add the new functions immediately before it:

```javascript
    function toggleMobileFilters() {
        const bar = document.getElementById('global-sort-bar');
        bar.classList.toggle('open');
        if (bar.classList.contains('open')) {
            setTimeout(() => document.addEventListener('click', _mobileFiltersOutside, { once: true }), 0);
        }
    }

    function _mobileFiltersOutside(e) {
        const bar = document.getElementById('global-sort-bar');
        const toggle = document.getElementById('mobile-filter-toggle');
        if (!bar.contains(e.target) && !toggle.contains(e.target)) bar.classList.remove('open');
    }

    function toggleFilter(btn) {
```

- [ ] **Step 3: Add the mobile CSS**

Add this always-on default rule at the same insertion point used in Tasks 1/2 (before the `/* ── Basic responsive layout ── */` comment):

```css
        #mobile-filter-toggle { display: none; }
```

Inside the `@media (max-width: 700px) { ... }` block, add:

```css
            #mobile-filter-toggle { display: inline-flex; }
            #global-sort-bar { display: none; }
            #global-sort-bar.open {
                display: flex;
                flex-direction: column;
                align-items: flex-start;
                gap: 6px;
                padding: 10px 12px;
                max-height: 60vh;
                overflow-y: auto;
            }
```

- [ ] **Step 4: Verify**

```bash
grep -n "mobile-filter-toggle" src/templates/index.html src/static/app.css src/static/app.js
node --check src/static/app.js && echo "JS syntax OK"
python3 -c "
css = open('src/static/app.css').read()
assert css.count('{') == css.count('}'), 'unbalanced braces'
print('CSS OK')
"
```
Expected: `grep` shows matches in all three files, `JS syntax OK`, `CSS OK`.

- [ ] **Step 5: Commit**

```bash
git add src/templates/index.html src/static/app.css src/static/app.js
git commit -m "feat: collapse the filter/sort bar into a mobile dropdown

Below 700px, #global-sort-bar is hidden behind a new 'Filters' toggle
button and expands in normal document flow (pushing content down) when
open, closing on a repeat tap or an outside click. Desktop keeps the
bar always visible inline, unchanged."
```

---

### Task 4: Header tools overflow menu (index.html only)

**Files:**
- Modify: `src/templates/index.html:47-57` (the 8 action buttons between the search input and the nav links)
- Modify: `src/static/app.css`
- Modify: `src/static/app.js`

**Interfaces:**
- Consumes: `.nav-link-desktop` class added to the three nav links in Task 2 Step 4 (this task wraps those same links, now already classed, into the same overflow container).
- Produces: `toggleToolsMenu()` global function.

- [ ] **Step 1: Wrap the action buttons and nav links in a `tools-group` container**

In `src/templates/index.html`, the block from the `searches` button through the `theme-toggle-btn` button (currently lines 47-67, i.e. everything in `<header>` after the search `<input>` and before `</header>`) currently reads:
```html
        <button class="btn" onclick="openSearchSettings()" title="Configure search sites">⚙ searches</button>
        <button class="btn" onclick="openDupesModal()" title="Find likely duplicate files by name similarity">⊕ dupes</button>
        <button class="btn" onclick="openExportModal()" title="Export file list">↓ export</button>
        <button class="btn" onclick="openHealthModal()" title="Health check — scan all files for issues">✦ health</button>
        <button class="btn" id="batch-toggle-btn" onclick="toggleBatchMode()" title="Select files for batch encode">☐ select</button>
        <button class="btn" id="select-all-visible-btn" onclick="selectAllVisible()" title="Select every file currently visible after filtering (shift-click a checkbox to select a range)">☑ select all visible</button>
        <button class="btn" onclick="encodeAllFiltered()" title="Batch-encode every file currently visible after filtering, in one step">⚙ encode filtered</button>
        <button class="btn" title="Clear cached metadata and re-scan all files"
                onclick="if(confirm('Rescan will clear all cached file metadata. Continue?'))fetch('/rescan',{method:'POST',headers:{'X-Delete-Token':DELETE_TOKEN}}).then(()=>location.reload())">
            ↺ Rescan
        </button>
        <a href="{{ ingress_path }}/encode" class="btn nav-link-desktop" id="encode-jobs-btn" title="View encode jobs" style="text-decoration:none;position:relative;overflow:hidden">
            <span id="encode-jobs-fill" style="position:absolute;left:0;top:0;bottom:0;width:0%;background:color-mix(in srgb, var(--accent) 25%, transparent);transition:width 1s linear;z-index:0"></span>
            <span style="position:relative;z-index:1;display:inline-flex;align-items:center;gap:5px">
                <span id="encode-jobs-dot" style="display:none;width:7px;height:7px;border-radius:50%;background:var(--accent);animation:ms-pulse 1.4s ease-in-out infinite"></span>
                ⚙ Jobs<span id="encode-jobs-count" style="display:none;margin-left:2px;font-size:11px;background:var(--accent);color:var(--bg);border-radius:8px;padding:0 6px"></span>
            </span>
        </a>
        <a href="{{ ingress_path }}/imdb-scan" class="btn nav-link-desktop" title="IMDb background scan" style="margin-left:auto;text-decoration:none">⬡ IMDb</a>
        <a href="{{ ingress_path }}/databases" class="btn nav-link-desktop" title="Databases &amp; system info" style="text-decoration:none">⬡ DB</a>
        <button class="btn" id="theme-toggle-btn" onclick="toggleTheme()" title="Switch between dark and light theme">🌓</button>
```

Replace it with (wrapping the same content, unchanged, in a `tools-group` div, and adding a toggle button before it):
```html
        <button id="tools-toggle-btn" class="btn" onclick="toggleToolsMenu()" title="More tools">⋯</button>
        <div class="tools-group" id="tools-group">
        <button class="btn" onclick="openSearchSettings()" title="Configure search sites">⚙ searches</button>
        <button class="btn" onclick="openDupesModal()" title="Find likely duplicate files by name similarity">⊕ dupes</button>
        <button class="btn" onclick="openExportModal()" title="Export file list">↓ export</button>
        <button class="btn" onclick="openHealthModal()" title="Health check — scan all files for issues">✦ health</button>
        <button class="btn" id="batch-toggle-btn" onclick="toggleBatchMode()" title="Select files for batch encode">☐ select</button>
        <button class="btn" id="select-all-visible-btn" onclick="selectAllVisible()" title="Select every file currently visible after filtering (shift-click a checkbox to select a range)">☑ select all visible</button>
        <button class="btn" onclick="encodeAllFiltered()" title="Batch-encode every file currently visible after filtering, in one step">⚙ encode filtered</button>
        <button class="btn" title="Clear cached metadata and re-scan all files"
                onclick="if(confirm('Rescan will clear all cached file metadata. Continue?'))fetch('/rescan',{method:'POST',headers:{'X-Delete-Token':DELETE_TOKEN}}).then(()=>location.reload())">
            ↺ Rescan
        </button>
        <a href="{{ ingress_path }}/encode" class="btn nav-link-desktop" id="encode-jobs-btn" title="View encode jobs" style="text-decoration:none;position:relative;overflow:hidden">
            <span id="encode-jobs-fill" style="position:absolute;left:0;top:0;bottom:0;width:0%;background:color-mix(in srgb, var(--accent) 25%, transparent);transition:width 1s linear;z-index:0"></span>
            <span style="position:relative;z-index:1;display:inline-flex;align-items:center;gap:5px">
                <span id="encode-jobs-dot" style="display:none;width:7px;height:7px;border-radius:50%;background:var(--accent);animation:ms-pulse 1.4s ease-in-out infinite"></span>
                ⚙ Jobs<span id="encode-jobs-count" style="display:none;margin-left:2px;font-size:11px;background:var(--accent);color:var(--bg);border-radius:8px;padding:0 6px"></span>
            </span>
        </a>
        <a href="{{ ingress_path }}/imdb-scan" class="btn nav-link-desktop" title="IMDb background scan" style="margin-left:auto;text-decoration:none">⬡ IMDb</a>
        <a href="{{ ingress_path }}/databases" class="btn nav-link-desktop" title="Databases &amp; system info" style="text-decoration:none">⬡ DB</a>
        <button class="btn" id="theme-toggle-btn" onclick="toggleTheme()" title="Switch between dark and light theme">🌓</button>
        </div>
```
(Only the new `<button id="tools-toggle-btn">` line and the wrapping `<div class="tools-group" id="tools-group">` / `</div>` pair are added — every button/link inside is byte-for-byte unchanged.)

- [ ] **Step 2: Add the JS toggle function**

In `src/static/app.js`, add immediately after the `_mobileFiltersOutside` function added in Task 3 Step 2 (or, if Task 3 hasn't run yet in this session, immediately before `function toggleFilter(btn) {`):

```javascript
    function toggleToolsMenu() {
        const group = document.getElementById('tools-group');
        group.classList.toggle('open');
        if (group.classList.contains('open')) {
            setTimeout(() => document.addEventListener('click', _toolsMenuOutside, { once: true }), 0);
        }
    }

    function _toolsMenuOutside(e) {
        const group = document.getElementById('tools-group');
        const toggle = document.getElementById('tools-toggle-btn');
        if (!group.contains(e.target) && !toggle.contains(e.target)) group.classList.remove('open');
    }
```

- [ ] **Step 3: Add the mobile CSS**

Add this always-on default (desktop-safe) rule at the same insertion point as prior tasks:

```css
        .tools-group { display: contents; }
        #tools-toggle-btn { display: none; }
```

`display: contents` makes the wrapper invisible to layout on desktop — its children act as direct flex items of `<header>`, exactly as before wrapping them.

Inside the `@media (max-width: 700px) { ... }` block, add:

```css
            #tools-toggle-btn { display: inline-flex; }
            .tools-group {
                display: none;
                flex-direction: column;
                align-items: stretch;
                gap: 6px;
                width: 100%;
                order: 99;
                padding-top: 8px;
            }
            .tools-group.open { display: flex; }
```

(`order: 99` combined with `width: 100%` forces the group onto its own wrapped line at the end of the header, since `header { flex-wrap: wrap; }` is already set in the existing mobile block.)

- [ ] **Step 4: Verify**

```bash
grep -n "tools-group\|tools-toggle-btn" src/templates/index.html src/static/app.css src/static/app.js
node --check src/static/app.js && echo "JS syntax OK"
python3 -c "
css = open('src/static/app.css').read()
assert css.count('{') == css.count('}'), 'unbalanced braces'
print('CSS OK')
"
```
Expected: matches in all three files, `JS syntax OK`, `CSS OK`.

- [ ] **Step 5: Commit**

```bash
git add src/templates/index.html src/static/app.css src/static/app.js
git commit -m "feat: collapse header action buttons into a mobile tools menu

Below 700px, the 9 non-navigation header buttons (search-sites
settings, dupes, export, health, select/select-all/encode-filtered,
rescan, theme) move behind a new '...' toggle button instead of
wrapping across several rows. Desktop layout is byte-identical (the
wrapper uses display:contents there)."
```

---

### Task 5: Full-screen modals on mobile

**Files:**
- Modify: `src/static/app.css`

**Interfaces:**
- Consumes: none.
- Produces: none (leaf change — affects only visual presentation of the existing `.modal`/`.modal-overlay` classes already used by `_modals.html`).

- [ ] **Step 1: Add the mobile modal override**

Inside the existing `@media (max-width: 700px) { ... }` block in `src/static/app.css`, replace the current line:
```css
            .modal { padding: 16px 18px; }
```
with:
```css
            .modal-overlay { align-items: stretch; }
            .modal {
                width: 100vw !important;
                max-width: none !important;
                height: 100vh !important;
                max-height: none !important;
                border-radius: 0;
                padding: 16px 18px;
            }
```

The `!important` on `width`/`max-width`/`height`/`max-height` is needed because several modals set inline `style="max-width:...` on the same element (e.g. `.export-modal`, `.dupes-modal`, the encode modal) — an inline style otherwise beats any class-based rule regardless of media query, so without `!important` those specific modals would stay their fixed desktop width on a phone.

- [ ] **Step 2: Verify**

```bash
grep -n "width: 100vw !important" src/static/app.css
python3 -c "
css = open('src/static/app.css').read()
assert css.count('{') == css.count('}'), 'unbalanced braces'
print('CSS OK')
"
```
Expected: one match, `CSS OK`.

- [ ] **Step 3: Commit**

```bash
git add src/static/app.css
git commit -m "fix: make modals full-screen on mobile instead of a fixed box

All 8 modals in _modals.html share the .modal/.modal-overlay base, so
one media-query override (with !important to beat each modal's inline
max-width) covers encode/rename/delete/mediainfo/export/dupes/health/
search-settings at once. Desktop styling is untouched."
```

---

### Task 6: Encode Jobs preview overlay mobile tweak

**Files:**
- Modify: `src/templates/encode.html:139-149` (existing 768px block)

**Interfaces:**
- Consumes: none.
- Produces: none (leaf change).

- [ ] **Step 1: Add a mobile tweak to `.preview-box`**

The existing `@media (max-width: 768px) { ... }` block in `src/templates/encode.html` (lines 139-149) currently only adjusts `.job-card`/`.job-actions`. Add `.preview-box` rules to the same block:
```css
        @media (max-width: 768px) {
            .job-card {
                flex-direction: column;
                align-items: stretch;
                gap: 12px;
            }
            .job-actions {
                justify-content: flex-start;
                max-width: none;
            }
            .preview-box {
                padding: 8px;
                width: 95vw;
            }
            .preview-box video {
                max-height: 80vh;
            }
        }
```
(Only the two new `.preview-box`/`.preview-box video` rules are added; `.job-card`/`.job-actions` are unchanged.)

- [ ] **Step 2: Verify**

```bash
grep -n "preview-box video" src/templates/encode.html
python3 -c "
html = open('src/templates/encode.html').read()
style = html.split('<style>')[1].split('</style>')[0]
assert style.count('{') == style.count('}'), 'unbalanced braces in style block'
print('OK')
"
```
Expected: one match, `OK`.

- [ ] **Step 3: Commit**

```bash
git add src/templates/encode.html
git commit -m "fix: give the encode preview box a bit more room on mobile

Reduce padding and raise the video max-height slightly below 768px —
the box was already near-fullscreen (95vw) but had room to use more
of a phone screen."
```

---

## Final verification (after all six tasks)

- [ ] **Run the full pytest suite to confirm no backend regression:**
```bash
python3 -m pytest tests/ -q
```
Expected: all tests pass (this plan makes no backend/route-logic changes beyond adding one dict key per route, but confirms nothing else broke).

- [ ] **Manual visual check** (use the `run` skill or start the app locally and open it in a browser, or Chrome DevTools' device toolbar at a ~390px-wide viewport, in both light and dark theme, on all four pages):
  - File table: no preview thumbnail/rating/ext/audio/duration badges; resolution shows as a 4K/1080p/720p/SD tag.
  - Filters collapse behind "⚗ Filters"; opening pushes content down; tapping outside or the button again closes it.
  - A horizontally-scrollable tab strip (Library/Jobs/IMDb/DB) appears on all four pages with the current page highlighted.
  - The "⋯" button on the Library page reveals the moved-out action buttons; each still works (spot-check `openHealthModal()` and the theme toggle).
  - Every modal (spot-check the encode modal and the export modal, since those have the widest desktop `max-width` overrides) fills the screen.
  - Resize back above 700px (or view on desktop) and confirm every page looks exactly as it did before this work.
