# Mobile-friendly layout

## Problem

The app is desktop-first: the file table crams eight badge columns plus a
poster thumbnail into one row, the filter/sort bar is a permanently-visible
row of ~20 buttons, the header on the main page has ~13 controls that wrap
into several rows on a phone, and every modal is a fixed-width centered box
that doesn't use the available screen. A partial pass already exists at
`max-width: 700px` in `app.css` (file table becomes a wrapping block instead
of a table) but its own comment flags it as "a basic pass... not a full
mobile redesign" and explicitly excludes the Encode Jobs / Databases / IMDb
Scan pages.

## Scope

All four pages (`index.html`, `encode.html`, `databases.html`,
`imdb_scan.html`). Desktop layout and behavior are unchanged — every rule
below lives inside `@media (max-width: 700px)` blocks (the breakpoint the
existing partial pass already uses) or is otherwise gated so it has zero
effect above that width.

Implementation is pure CSS wherever possible (matching the existing
approach), with JS added only where a mobile-only interaction has no CSS-only
equivalent: the filter-dropdown open/close toggle, the tools overflow menu,
and marking the active tab in the new tab bar.

## 1. File table row (index.html / `_files_table.html` / `app.css`)

In the mobile media query, hide:
- `.poster-thumb` (poster preview image)
- `.imdb-rating-badge` (star rating shown once IMDb-matched)
- the extension badge-cell, audio badge-cell, and duration badge-cell

`_files_table.html` gets three new classes so these are targetable directly
instead of via fragile `nth-child` selectors: `badge-cell-ext`,
`badge-cell-audio`, `badge-cell-duration` (added alongside the existing
`badge-cell` class, which keeps its current shared styling on desktop).

Kept as-is: filename (tap → media-info modal via `handleFileClick`), ▶ play
button (`openPlayer`), video-codec badge, HDR badge, the IMDb tag/match
button, search-sites button, encode/rename/delete action buttons, batch
checkbox.

**Resolution badge:** desktop keeps the exact `{{ file.width }}×{{
file.height }}` text. Add a second badge-cell in `_files_table.html`,
computed inline from `file.width`/`file.height` using the same thresholds as
`resTag()` in `app.js` (`>=3000w or >=2000h → 4K`, `>=1700w or >=900h →
1080p`, `>=900w or >=500h → 720p`, else `SD`/unknown). Give the existing
exact-resolution badge-cell a `res-badge-desktop` class and the new one a
`res-badge-mobile` class; CSS shows exactly one of the two per breakpoint.
No JS involved — both are always rendered server-side, only visibility
toggles.

## 2. Filter/sort bar → dropdown (index.html)

`#global-sort-bar` (sort buttons + all `filter-btn` groups) gets
`display: none` by default on mobile. A new button, visible only on mobile,
toggles it: `<button id="mobile-filter-toggle" class="btn mobile-only"
onclick="toggleMobileFilters()">⚗ Filters</button>` placed where the bar
currently sits.

When open (class `.open` added to `#global-sort-bar`), the bar renders as an
absolutely-positioned panel spanning the content width, `flex-direction:
column`, scrollable if tall, `z-index` above the file list. Closes on
toggle-button click again or on an outside tap (a click listener on
`document` that closes it when the click target is outside both the bar and
the toggle button — same pattern already used for the IMDb popup's
outside-click close in `app.js`).

Desktop: `#global-sort-bar` and the new toggle button are unaffected/hidden
respectively — bar stays the current always-visible inline row.

## 3. Sliding tab bar (all four pages)

New partial, e.g. `_mobile_tabs.html`, included immediately after the opening
`<header>` tag in all four templates:

```html
<nav class="mobile-tabs">
  <a href="{{ ingress_path }}/" class="mobile-tab{{ ' active' if active_tab == 'library' }}">Library</a>
  <a href="{{ ingress_path }}/encode" class="mobile-tab{{ ' active' if active_tab == 'jobs' }}">Jobs</a>
  <a href="{{ ingress_path }}/imdb-scan" class="mobile-tab{{ ' active' if active_tab == 'imdb' }}">IMDb</a>
  <a href="{{ ingress_path }}/databases" class="mobile-tab{{ ' active' if active_tab == 'db' }}">DB</a>
</nav>
```

Each of the four route handlers passes its own `active_tab` value to the
template context (`library` / `jobs` / `imdb` / `db`). `.mobile-tabs` is
`display: none` on desktop, and on mobile is a horizontally-scrollable flex
row (`overflow-x: auto; white-space: nowrap`) with the `.active` tab visually
distinguished (accent underline/background, matching the existing
`.filter-btn.active` / `.sort-btn.active` treatment for consistency).

**Header decluttering (index.html only):** on mobile, the non-navigation
action buttons — search-sites settings, dupes, export, health, select /
select-all-visible / encode-filtered, rescan, theme toggle — move into a
"⋯ Tools" overflow menu (small dropdown panel, same open/close pattern as
the filter dropdown in #2, reusing the outside-click-close helper). The
folder picker and search input stay inline in the header; the Jobs/IMDb/DB
links are removed from the header on mobile since the new tab bar (#3)
already covers that navigation. Desktop header markup/behavior is untouched.

## 4. Full-screen modals

All eight modals on index.html (`encode`, `rename`, `delete`, `mediainfo`,
`export`, `dupes`, `health`, `search-settings`) share one `.modal` /
`.modal-overlay` base in `app.css`. One mobile media-query block covers all
of them:

```css
@media (max-width: 700px) {
    .modal-overlay { align-items: stretch; }
    .modal {
        width: 100vw; max-width: none;
        height: 100vh; max-height: none;
        border-radius: 0;
    }
}
```

Per-modal inline `style="max-width:..."` overrides in `_modals.html` are
written as inline styles on the `.modal` element, which would otherwise beat
a class-based mobile rule at equal specificity by source order — add
`!important` only on the mobile `max-width`/`width` overrides to guarantee
the fullscreen rule wins regardless of each modal's inline style, since this
is exactly the kind of narrow, deliberate exception `!important` is for.

**Encode Jobs preview overlay** (`encode.html`'s `.preview-box`, used for the
QP-estimate video/image preview): already close to full-screen via
`max-width: min(960px, 95vw)`. Add a small mobile tweak — reduce `.preview-box`
padding and raise `video` `max-height` — rather than a full rewrite, since
the existing sizing already suits a phone screen reasonably well.

## 5. Encode Jobs / Databases / IMDb Scan pages

Databases' card grid (`grid-template-columns: repeat(auto-fill,
minmax(300px, 1fr))`) and Encode Jobs' `.job-card` (already has a
`max-width: 768px` stacking rule) both degrade reasonably at phone widths
already. No changes planned beyond adding the tab bar (#3) to these pages'
headers. If review after implementation turns up rough edges, treat as a
follow-up, not part of this pass.

## Testing

No pytest coverage applies (CSS/template/JS only, no backend behavior
changes). Verify manually via the `run` skill / a real browser at a mobile
viewport width (or Chrome DevTools device toolbar) against each of the four
pages, both light and dark themes:
- File table shows simplified badges + resolution tier, no preview/rating/
  ext/audio/duration badges.
- Filters collapse behind the toggle; open/close and outside-tap-close work.
- Tab bar shows on all four pages, correct tab marked active, scrolls
  horizontally without breaking page layout.
- Tools menu on index.html contains the moved-out buttons and each still
  works.
- Every modal fills the screen on mobile; desktop modals are pixel-identical
  to before this change.
- Resize back above 700px and confirm every page is visually unchanged from
  before this work (git stash the CSS/template changes and compare if in
  doubt).
