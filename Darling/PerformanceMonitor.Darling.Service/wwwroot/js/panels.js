/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The panel renderer + viz registry (#1562) — the #1563 seam. Every data view on the server/alerts pages is a
 * PANEL DESCRIPTOR run through renderPanel({read, params, viz, span, ...}):
 *   read    — an MCP tool name (-> GET /api/read/{read}) or, via `path`, a raw API path (-> GET path)
 *   params  — query-string params
 *   viz     — one of the registry keys below: "table" | "line" | "stat" | "bandlist"
 *   span    — 2 to span both columns of a grid
 * Phase-1 pages hand-build descriptor ARRAYS (js/pages/*). #1563 later replaces those arrays with stored JSON
 * loaded through this same renderPanel — so nothing downstream of here is built now, but the seam is stable.
 *
 * R4 (XSS): every value reaches the DOM through util.el()'s text/textContent path — the table cells, stat
 * values, band rows, and chart tooltip never touch innerHTML, so query text / object names / wait types /
 * server names render inert.
 */

import { notePanelKey } from "./refresh-in-place.js";
import {
  storeHash,
  el,
  mount,
  loadingStrip,
  waitingStrip,
  retryButton,
  isWindowTooWide,
  retryOnceWhenBusy,
  busyRetryStrip,
  errorStrip,
  readErrorStrip,
  emptyStrip,
  noticeStrip,
  captionStrip,
  keptWindowStrip,
  windowFloorStrip,
  leftOutStrip,
  leftOutNoteText,
  LEFT_OUT_NOTE_KEY,
  readTool,
  readWithinKeptHistory,
  apiGet,
  buildQuery,
  getPath,
  parseUtc,
  applyFormat,
  bandClass,
  sevClass,
  windowFromHours,
  dbScopeChip,
  dbFilteredEmptyText,
  sourceSpan,
  applySourceSubtitle,
  clampText,
  plainEmptyStrip,
  noteStrip,
  disclosure,
  markPartialSubtitle,
  applyDataSubtitle,
  localizeIsoText,
  localizeAlertDetail,
  noValuesText,
  SAMPLED_BLOCKING_SOURCE,
} from "./util.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS } from "./charts.js";
import { bucketMsOf } from "./chart-gaps.js";
import { plainText, plainTextNamed, plainTextCounts, shortEmptyLine } from "./plain-text.js";
import { panelPlotPx } from "./trend-points.js";
import { withheldDisplayText } from "./mute-context.js";
import { toTsv, toCsv, isListValue, csvFileName, copyText, downloadCsv } from "./grid-tools.js";
import {
  VALUE_LIST_MAX_LEN,
  VALUE_LIST_CAP,
  FILTER_STORE_KEY,
  NO_VALUES,
  valueKey,
  listableColumn,
  valuesOn,
  collectValues,
  compileValues,
  valuePasses,
  nextValues,
  valuesPhrase,
  serializeFilters,
  parseFilters,
} from "./grid-value-filter.js";

/* The AbortSignal for the render currently building panels (#4191). A page sets it (setPanelSignal)
   synchronously, immediately before calling a tab's build()/a page's descriptor array, and renderPanel below
   captures the CURRENT VALUE into a local const at the moment each panel is built — a later render's signal
   swap can only affect panels renderPanel has not been called for yet, never one already in flight. This is
   what lets every page fall in line without threading a signal through table()/stat()/line() and every
   build(server, ctx) signature: the one seam every panel read already shares picks it up implicitly. A page
   that never calls setPanelSignal (most of them, today) leaves this undefined, and fetch(path, {signal:
   undefined}) is exactly the unabortable request every panel already made. */
let panelSignal;

/** Set (or clear, with no argument) the AbortSignal the NEXT renderPanel() calls will capture — see above. */
export function setPanelSignal(signal) {
  panelSignal = signal;
}

/** The signal the next renderPanel() would capture, so a caller can swap its own in for one panel and put this back (#5227). */
export function getPanelSignal() {
  return panelSignal;
}

/**
 * The sentence an empty envelope shows (#5644 W16). A panel that sets `emptyWins` has ONE agreed sentence for its
 * empty state (the Blocking tab's deadlock panels, whose reads each worded "nothing happened" differently), and it
 * replaces the read's own message; every other panel keeps the server's message.
 */
export function emptyHeadline(desc, res) {
  /* Only a TRUE negative (status "empty") takes the panel's sentence. A "not_collected" (an Azure SQL Database has no system_health
     session), "unavailable" or "precondition" envelope is a different statement, and "No deadlocks" over it would be a false
     all-clear that also drops the server's own engine-gate line; those keep the read's message. */
  return desc && desc.emptyWins && desc.emptyText && res.status === "empty" ? desc.emptyText : res.message;
}

/**
 * #7410: the strip an empty reply draws in a panel. A panel's agreed sentence (emptyWins) or a reply with no structured reason shows
 * the message as before. A reply that names a missing extension, a source the login cannot read, or plan capture that is not ready
 * shows one plain sentence built from its `hints`, and the tool's full text is behind "More detail". A panel whose
 * `extensionMissingLine` says what is not collected uses that sentence for a missing extension.
 */
export function panelEmptyStrip(desc, res) {
  if (emptyHeadline(desc, res) !== res.message) return emptyStrip(emptyHeadline(desc, res));
  /* A panel with `emptyLine` says that plain sentence for a true negative and keeps the tool's own text behind "More detail" (#7410). */
  if (desc.emptyLine && res.status === "empty") return plainEmptyStrip(res, desc.emptyLine === true ? desc.emptyText : desc.emptyLine);
  const extension = res.hints && res.hints.reason === "extension_missing" ? desc.extensionMissingLine : undefined;
  return plainEmptyStrip(res, shortEmptyLine(res, extension));
}

/**
 * A panel whose wording depends on which collector answered (#5644 W16, the blocking trend: blocked process reports, or the DMV
 * snapshot rows it falls back to). `desc.bySource` maps the answer's `desc.sourceKey` value to `{ title, label, unit }`; the
 * returned descriptor carries the series label and axis unit, and the heading's title text is replaced in place. A descriptor
 * without `bySource`, or an answer whose source has no entry, comes back as given.
 */
export function applySourceWording(desc, data, panel) {
  const alt = desc.bySource && desc.sourceKey ? desc.bySource[getPath(data, desc.sourceKey)] : null;
  if (!alt) return desc;
  const head = panel ? Array.from(panel.children || [])[0] : null;
  const titleNode = head ? head.firstChild : null;
  if (titleNode && alt.title) titleNode.textContent = alt.title;
  return {
    ...desc,
    title: alt.title || desc.title,
    unit: alt.unit ?? desc.unit,
    series: Array.isArray(desc.series) ? desc.series.map((x) => ({ ...x, label: alt.label || x.label })) : desc.series,
  };
}

/**
 * Build a panel node. It returns immediately with a loading strip and fills itself once the fetch resolves,
 * mapping the API response kinds (data / empty envelope / error / aborted / auth) to the right UI.
 */
export function renderPanel(desc, onSettled, gate = null) {
  const signal = panelSignal;
  const body = el("div", { class: "panel-body" }, [gate ? waitingStrip() : loadingStrip()]);
  /* #5701: `data-panel` (the read) and `data-title` are the hooks the web UI smoke check finds a panel by. */
  const panel = el("div", { class: "panel card" + (desc.span === 2 ? " span-2" : ""), "data-panel": desc.read || desc.title, "data-title": desc.title }, [
    /* The database-scope chip (#5245): while the page's database filter is active it says whether this panel's read took it
       (dbScopeChip in util.js; `desc.dbScope` overrides the read's own class for a panel fed by a shared read). */
    el("h3", {}, [desc.title, desc.subtitle ? el("span", { class: "panel-sub", text: " " + desc.subtitle }) : null, sourceSpan(desc), dbScopeChip(desc.read, desc.dbScope)]),
    /* #5226: an optional control node under the title (the Top Queries / Top Procedures ranking selector), drawn before the body so a
       load that replaces the body never replaces it. Absent on every other panel. */
    desc.control || null,
    body,
  ]);
  /* #6933: the key a poll matches this panel by, so it keeps the panel on screen while the same read runs again. */
  notePanelKey(panel, JSON.stringify([desc.read || desc.path || null, desc.params || null]));
  /* #7257: a saved view's `gate` starts the read when the page's long-read slots allow it. */
  if (gate) {
    /* A failed read gets a Retry button that runs this one panel again through the same gate, and a busy refusal is sent once more. */
    const retry = () => {
      mount(body, waitingStrip());
      gate(() => loadPanel(desc, body, signal, null, retry), true);
    };
    gate(() => loadPanel(desc, body, signal, onSettled, retry));
  } else {
    loadPanel(desc, body, signal, onSettled);
  }
  return panel;
}

/* `onSettled` (#4222) is an optional completion callback fired exactly once when this panel's load reaches a
   terminal state (data, empty, error, aborted, or auth) — every renderPanel caller today omits it (a no-op), but
   it is what lets a caller with its OWN concurrency budget (the alert-notebook doc's max-3 in-flight cell loader,
   views.js) know when a slot frees, since renderPanel itself returns synchronously with the fetch still in flight. */
async function loadPanel(desc, body, signal, onSettled, onRetry = null) {
  try {
    await loadPanelBody(desc, body, signal, onRetry);
  } finally {
    if (onSettled) onSettled();
  }
}

async function loadPanelBody(desc, body, signal, onRetry = null) {
  /* A read that keeps less history than the page's Range is asked again for what it keeps, and the panel says
     so through keptWindowStrip (readWithinKeptHistory in util.js, shared with the server-tab composites). */
  /* #7257: a panel of a saved view (it has onRetry) sends a read the service refused busy (429) once more after its Retry-After. */
  /* #7661: a trend chart asks for about one point per 2 px of its plot (trend-points.js), so a day does not come back as 1-minute buckets the
     plot cannot show. Measured here, when the panel loads: from its body when it is on the page, else estimated from the window. */
  /* Only a panel that draws a line has a plot: a table, a stat or a band list sends no `plotPx`, so its read keeps the old budget and its rows. */
  const plotPx = panelPlotPx(desc, body.clientWidth, typeof window !== "undefined" ? window.innerWidth : 0);
  const readOptions = plotPx > 0 ? { plotPx } : undefined;
  const readOnce = (tool, params) => onRetry
    ? retryOnceWhenBusy(() => readTool(tool, params, signal, readOptions), (seconds) => mount(body, busyRetryStrip(seconds)), signal)
    : readTool(tool, params, signal, readOptions);
  const res = await readWithinKeptHistory(
    (params) => (desc.read ? readOnce(desc.read, params) : apiGet(desc.path + buildQuery(params), signal)),
    desc.params
  );

  /* A superseded render's own reads (#4191) or a session that just expired (#4187, its own shell-wide
     takeover — see util.js) — either way this panel's slot is no longer this code's to fill; the render that
     owns the screen now already replaced it or is about to. */
  if (res.kind === "aborted" || res.kind === "auth") {
    return;
  }

  if (res.kind === "error") {
    /* readErrorStrip degrades the "window too wide" validation error to a notice; every other error stays red
       (#2780). Shared with the server-tab composites so the whole page degrades the same way. A window refusal
       only reaches it now when the one retry above could not settle it. */
    /* #7257: the "pick a shorter range" notice gets no Retry button, since the same range gives the same notice. */
    mount(body, onRetry && !isWindowTooWide(res.message) ? [readErrorStrip(res.message), retryButton(onRetry)] : readErrorStrip(res.message));
    return;
  }
  const kept = keptWindowStrip(res);
  /* A panel that does not apply to this kind of server and says `hideWhenNotCollected` is not drawn at all (the Instance CPU
     panel on a PostgreSQL server that is not Aurora): one panel that can never fill is clutter, not information. */
  if (res.kind === "empty" && res.status === "not_collected" && desc.hideWhenNotCollected && body.parentNode) {
    if (body.parentNode.style) body.parentNode.style.display = "none";
    body.parentNode.hidden = true;
    return;
  }
  /* A panel on a PostgreSQL FinOps target that does not apply says one short line (`notCollectedLine`) instead of the server's gate paragraph. */
  if (res.kind === "empty" && res.status === "not_collected" && desc.notCollectedLine) {
    mount(body, emptyStrip(desc.notCollectedLine));
    return;
  }
  if (res.kind === "empty") {
    /* #4966: a grid that looked and found nothing still says where its table's data starts when that is after the
       window's start (the server adds the note to that envelope only, never to an unavailable or not_collected one), so a
       new server's empty week does not read as a quiet one. windowFloorStrip is null for a chart and for an envelope
       without the note. */
    mount(body, [kept, windowFloorStrip(res.data, desc), leftOutStrip(res.data, desc), panelEmptyStrip(desc, res)]);
    return;
  }

  /* #6293: a panel whose picture is its own (the Query Heatmap grid) hands its draw function as `desc.render`. */
  const render = typeof desc.render === "function" ? desc.render : VIZ[desc.viz];
  if (!render) {
    mount(body, errorStrip("Unknown visualization: " + desc.viz));
    return;
  }
  try {
    /* A SERVER-SUPPLIED caveat, above the rendered body (#3278). Opt-in: a descriptor without `noteKey` is
       untouched, which is every panel but the one that needs it.

       The empty envelope already carries the server's sentence through emptyStrip above, so before this the
       populated path was the ONLY one that dropped it — and that is the path the caveat matters on. A panel
       whose rows are a capped page of a much larger population looks like it worked; nothing in the rows
       shows the difference, and a client-authored subtitle cannot carry the figures because it is written
       before the read. get_pg_index_bloat is the case: its answerless rows sort FIRST by design, so a
       capped page is 100% of them whenever they outnumber the cap.

       Read through getPath and rendered as TEXT by noticeStrip, so a note is inert markup like every other
       server value on this page (R4). */
    /* #7538, #7651: the left-out note is the one the page words itself and gives the names (leftOutNoteText); any other note is the server's text. */
    const note = desc.noteKey === LEFT_OUT_NOTE_KEY ? leftOutNoteText(res.data) : desc.noteKey ? getPath(res.data, desc.noteKey) : null;
    /* #4925: a panel may carry further server notes (`moreNoteKeys`), each rendered as its own line when non-null. */
    const moreNotes = (desc.moreNoteKeys || []).map((k) => (k === LEFT_OUT_NOTE_KEY ? leftOutNoteText(res.data) : getPath(res.data, k)));
    /* #7428: a note the page composes from the answer's fields (`noteFrom(data)`), for a sentence the server does not write, such as why a tile is blank. */
    if (typeof desc.noteFrom === "function") moreNotes.push(desc.noteFrom(res.data));
    /* #4966: a grid says where its table's data starts when that is after the window's start (windowFloorStrip). */
    const floor = windowFloorStrip(res.data, desc);
    /* #5644 W12: a row-capped panel stops claiming the window it was asked for in its heading. */
    applyDataSubtitle(body.parentNode, res.data, desc);
    markPartialSubtitle(body.parentNode, res.data, desc);
    /* A narrowed read draws its chart over the hours it answered for, not the Range it was asked for (#2802).
       A copy, so the caller's descriptor keeps the window it asked for. */
    if (res.keptHours) desc = { ...desc, windowHours: res.keptHours };
    desc = applySourceWording(desc, res.data, body.parentNode);
    /* #5244, #7543: which collector answered a blocking read, for a descriptor that names its field (`sourceKey`), goes in the heading
       beside the window label, so no row above the chart takes height from the plot. Nothing for every other panel. */
    applySourceSubtitle(body.parentNode, res.data, desc);
    const rendered = render(res.data, desc);

    mount(body, [
      kept,
      floor,
      typeof note === "string" && note.trim() ? noteStrip(desc, note, res.data) : null,
      ...moreNotes.map((n) => (typeof n === "string" && n.trim() ? noteStrip(desc.noteQuiet === true ? { noteQuiet: true } : null, n) : null)),
      rendered,
    ]);
  } catch (e) {
    mount(body, errorStrip("Could not render this panel: " + (e && e.message ? e.message : String(e))));
  }
}

/* ─────────────────────────── viz registry ─────────────────────────── */

/**
 * The viz registry — the four phase-1 renderers. Each is (data, desc) -> Node. Adding a viz here is the ONLY
 * change #1563 needs to grow the descriptor vocabulary.
 */
export const VIZ = {
  table: vizTable,
  line: vizLine,
  stat: vizStat,
  bandlist: vizBandlist,
};

/**
 * A descriptor whose load-bearing field array (table.columns / stat.stats / line.series) is missing or empty —
 * a stored/imported/AI-drafted/older-JSON panel that never got a field-config. Rather than let `.map` throw a raw
 * "Cannot read properties of undefined" at EVERY seat (including the read-only network viewer), each viz below
 * guards its array and renders this instead. The shared renderer must tolerate any structurally-valid-but-
 * incomplete descriptor.
 */
const NO_FIELDS_MSG = "No fields configured — edit this view and run Auto-detect fields.";

/* table: desc = { rowsKey, columns:[{key,label,format,align,wrap,clamp,mono,pre,sevKey,statusSev,sortable,sortValue,csv,copy,copyValue,exportOnly}], sortable, sortId, onRow(row, tr), rowClass(row)|string, tools:false }

   Column-header sort (#4843). A header click cycles ascending -> descending -> the server's own order, with a ▲/▼
   indicator and aria-sort; Enter and Space on the focused header do the same. The comparison reads the RAW row value
   (never the formatted cell): numbers numerically, `time`/`reltime` columns by the stored instant, text without case,
   null/undefined/"" last in BOTH directions, and ties keep the server's order. `sortable: false` on the descriptor
   opts a table out (an order that carries meaning, e.g. a chronological trend the desktop grid also leaves unsorted);
   on a column it opts that column out. A column's `sortValue(row)` supplies the sort value when the cell is a custom
   render over a derived value. */
function vizTable(data, desc) {
  if (!(Array.isArray(desc.columns) && desc.columns.length)) return emptyStrip(NO_FIELDS_MSG);
  /* rowsKey "." is a read whose payload is one object, drawn as one row. */
  const read = desc.rowsKey === "." ? (data ? [data] : []) : getPath(data, desc.rowsKey) || [];
  /* A grid whose natural order is not the one to open on (a calendar oldest first, collectors A to Z) names `orderRows`,
     a function from the rows to the rows in the order to show. A header click still sorts from there. */
  const rows = typeof desc.orderRows === "function" && Array.isArray(read) ? desc.orderRows(read) : read;
  /* #5244: a filtered read that has a snapshot but no row for the chosen databases must not say there is no snapshot. */
  const grid = gridTable(rows, rows.length ? desc : { ...desc, emptyText: dbFilteredEmptyText(desc.read, desc.emptyText, desc.dbScope) });
  /* #5506 part 3: `statusKey` names a live-state object in the payload (get_read_latency's admission, never stored) that
     draws as one line above the grid; a payload without it draws the grid alone. */
  const line = desc.statusKey === "admission" ? admissionLine(getPath(data, desc.statusKey)) : null;
  return line ? el("div", {}, [noticeStrip(line), grid]) : grid;
}

/**
 * #5506 part 3: the status line of get_read_latency's grid, from the answer's live `admission` object: the long reads
 * running and waiting in this service process right now, against the limit (0 is no limit). Null when the answer has none.
 */
function admissionLine(admission) {
  if (!admission || typeof admission !== "object") return null;
  const running = Number(admission.running) || 0;
  const queued = Number(admission.queued) || 0;
  const limit = Number(admission.limit) || 0;
  const slots = limit > 0 ? `${running} of ${limit} long-read slots in use` : `${running} long reads running (no limit set)`;
  return `Now, in this service process: ${slots}, ${queued} waiting.`;
}

/** The grid every table on the web draws through (#4843): sort, column groups, per-column filters, Copy cell / row /
    all and Export CSV, with their state at module scope under gridSortKey(). vizTable feeds it a read's rows; a page
    that builds its own rows (a composed panel, the alert-rule test, the sweep tables) calls it with the rows and a
    descriptor of the same shape, so every table gets the same tools from one implementation. Beyond the vizTable
    column fields, a column may carry `display(row)` (the text to show, over a raw value or none) and
    `cellClass(row)` (a class for the cell, such as a severity colour). `desc.id` (or `sortId`) names the table and
    MUST carry whatever else tells two tables with the same columns apart (the server, the panel): the key is what
    keeps their sort, filter and picked cell separate. */
export function gridTable(rows, desc) {
  const allCols = Array.isArray(desc.columns) ? desc.columns : [];
  if (!allCols.length) return emptyStrip(NO_FIELDS_MSG);
  if (!rows.length) return emptyStrip(desc.emptyText || "No rows in this window.");
  const cols = visibleColumns(allCols, rows);

  const grid = desc.sortable === false ? null : makeGridSort(desc, cols, rows);
  const bodyRows = rows.map((row) => {
    const rc = typeof desc.rowClass === "function" ? desc.rowClass(row) : desc.rowClass;
    const tr = el("tr", { class: typeof rc === "string" && rc ? rc : null }, cols.map((c) => exportOnlyCell(row, c)));
    trRow.set(tr, row);
    if (typeof desc.onRow === "function") desc.onRow(row, tr);
    return tr;
  });
  /* The rows are classified when the body attaches, so the headers are built after the grid has seen them. */
  const tbody = el("tbody", {}, bodyRows);
  if (grid) grid.attach(tbody, bodyRows);
  const head = el(
    "tr",
    {},
    cols.map((c, i) => (grid && grid.eligible[i] ? grid.headerCell(c, i) : headerCell(c)))
  );
  if (grid) grid.syncHeads();

  const filterBar = desc.filter === false ? null : gridFilter(desc, cols, head, tbody);
  const table = el("table", { class: "data" }, [el("thead", {}, [head]), tbody]);
  const wrap = el("div", { class: "table-wrap" }, filterBar ? [filterBar, table] : [table]);
  pinIdentityColumn(wrap, table, head, tbody, cols);
  const picker = columnPicker(desc, cols, head, tbody);
  /* #5701: `data-grid` names the grid for the web UI smoke check. A tools-less, picker-less grid is the bare wrapper, which
     carries the hook itself so the check finds every grid the same way. */
  const gridName = String(desc.id || desc.sortId || desc.read || desc.title || "grid");
  if (desc.tools === false && !picker) {
    wrap.setAttribute("data-grid", gridName);
    return wrap;
  }
  if (desc.tools === false) return el("div", { class: "grid-box", "data-grid": gridName }, [picker, wrap]);
  return el("div", { class: "grid-box", "data-grid": gridName }, picker ? [picker, gridTools(desc, cols, tbody), wrap] : [gridTools(desc, cols, tbody), wrap]);
}

/* #7729: a grid wider than its box (Database Configuration 3456 px in 1189, High Impact 2160 in 1204) scrolled sideways to figures with no row name
   beside them. The first column that names the row (`name: true`, or `pin: true` where the name is drawn by a render) sticks to the left edge
   while the box scrolls. Only a grid that overflows takes it (the box gets `pin-first` while it does), so a grid that fits is as it was.
   Where there is no ResizeObserver (a test's fake DOM) nothing is pinned. */
function pinIdentityColumn(wrap, table, head, tbody, cols) {
  if (typeof ResizeObserver !== "function") return;
  const at = cols.findIndex((c) => c.pin === true || c.name === true);
  if (at < 0) return;
  head.children[at].classList.add("pin-col");
  Array.from(tbody.children).forEach((tr) => { if (tr.children[at]) tr.children[at].classList.add("pin-col"); });
  const sync = () => wrap.classList[wrap.scrollWidth > wrap.clientWidth + 1 ? "add" : "remove"]("pin-first");
  const watch = new ResizeObserver(sync);
  watch.observe(wrap);
  watch.observe(table);
}

/* Column groups (#4843). A descriptor with `groups: ["Memory detail", ...]` and `defaultGroups: [...]` marks wide
   grids: a column carrying `group: "<name>"` is drawn only while its group is on, and a column with no group (or a
   group the list does not name) is always shown. A "Columns:" strip above the table carries one toggle per group.
   Every column is still built, and a hidden one is only display:none, so the sort, Copy and CSV indexes stay
   aligned and Copy / CSV carry ALL columns, the way the desktop grid's export iterates every column whether or not
   it is shown. A column the user sorted by stays the sort key after its group is hidden. The toggles live at module
   scope under the table's key (route + identity + columns), so the 60 s repaint keeps them and another server's
   grid starts from the defaults. Returns null for a descriptor without groups. */
function columnPicker(desc, cols, head, tbody) {
  const groups = Array.isArray(desc.groups) ? desc.groups.filter((g) => cols.some((c) => c.group === g)) : [];
  if (!groups.length) return null;
  const key = gridSortKey(desc, cols);
  const on = () => gridGroupsOn.get(key) || new Set(Array.isArray(desc.defaultGroups) ? desc.defaultGroups : []);
  const shown = (c) => !c.group || !groups.includes(c.group) || on().has(c.group);
  const show = (node, yes) => {
    if (node && node.style) node.style.display = yes ? "" : "none";
  };
  const buttons = new Map();
  const apply = () => {
    const rowsAll = [head, ...tbody.children];
    cols.forEach((c, i) => {
      const yes = shown(c);
      for (const tr of rowsAll) show(tr.children[i], yes);
    });
    for (const [g, b] of buttons) {
      const live = on().has(g);
      b.setAttribute("aria-pressed", live ? "true" : "false");
      b.className = "btn col-toggle" + (live ? " on" : "");
    }
  };
  const toggles = groups.map((g) => {
    const b = el("button", { type: "button", class: "btn col-toggle", text: g, title: "Show or hide the " + g + " columns" });
    b.addEventListener("click", () => {
      const next = new Set(on());
      if (next.has(g)) next.delete(g);
      else next.add(g);
      gridGroupsOn.set(key, next);
      apply();
    });
    buttons.set(g, b);
    return b;
  });
  apply();
  return el("div", { class: "col-picker" }, [el("span", { class: "col-picker-label", text: "Columns:" }), ...toggles]);
}

/* Copy and CSV for one table (#4843). Both read the tbody as it stands, so they follow the active sort and any
   in-place reconcile. Copy puts what the user sees on the clipboard: the cells' text, tab-separated, with a header
   row on Copy All. The CSV carries RAW values (the stored ISO instant, the unformatted number) under the column
   labels, quoted per RFC 4180 and with a leading ' on a text cell a spreadsheet would run as a formula. A column
   whose values are arrays or objects has no one-cell form, so the CSV leaves it out, and so does `csv: false` on a column (a link-only cell), and `copy: false` leaves a column out of Copy row and Copy all; a column's `copyValue(row)` supplies the full text for Copy and the CSV when its cell shows a summary (Copy keeps it: it copies the
   cell's text). A custom-render column over a key the rows do not carry has no raw value, so its CSV cell is the
   text it shows. Set `tools: false` on the descriptor to draw a table without the strip. */
function gridTools(desc, cols, tbody) {
  const status = el("span", { class: "grid-tools-status", role: "status", "aria-live": "polite" });
  const key = gridSortKey(desc, cols);
  const say = (m) => {
    status.textContent = m;
  };
  const allTrs = () => [...tbody.children];
  /* Copy all and the CSV carry the rows the column filters leave showing, in the order shown. */
  const trs = () => allTrs().filter((tr) => !filteredOut.has(tr));
  /* The text a cell copies: the column's copyValue(row) when it has one (a custom-render cell whose visible text is a
     summary), else the text shown. */
  const textOf = (tr, i) => {
    const c = cols[i];
    const row = trRow.get(tr);
    return c && typeof c.copyValue === "function" && row ? String(c.copyValue(row) ?? "") : tr.children[i].textContent;
  };
  const textRows = (list) => list.map((tr) => [...tr.children].map((_, i) => textOf(tr, i)));
  /* Copy row and Copy table leave out a `copy: false` column (a control column such as a checkbox); the picked-cell
     signature and Copy cell still see every column. */
  const copyIdx = (n) => Array.from({ length: n }, (_, i) => i).filter((i) => !(cols[i] && cols[i].copy === false));
  const copyRows = (list) => textRows(list).map((r) => copyIdx(r.length).map((i) => r[i]));
  /* The picked cell is remembered as the row's text plus the column, at module scope under the table's key, so the
     60 s repaint (a new tbody, new cells) and an in-place reconcile both resolve it against what is on screen now;
     a row that is gone resolves to nothing. */
  const rowSig = (tr) => textRows([tr])[0].join("\u0001");
  const pickedCell = () => {
    const pick = gridPicked.get(key);
    const tr = pick ? allTrs().find((t) => rowSig(t) === pick.sig) : null;
    return tr && tr.children[pick.col] ? { tr, td: tr.children[pick.col], col: pick.col } : null;
  };
  const initial = pickedCell();
  if (initial && initial.td.classList) initial.td.classList.add("cell-picked");
  tbody.addEventListener("click", (e) => {
    const td = e && e.target && typeof e.target.closest === "function" ? e.target.closest("td") : null;
    const tr = td && td.parentNode;
    if (!tr) return;
    const prev = pickedCell();
    if (prev && prev.td.classList) prev.td.classList.remove("cell-picked");
    gridPicked.set(key, { sig: rowSig(tr), col: [...tr.children].indexOf(td) });
    if (td.classList) td.classList.add("cell-picked");
  });
  const finish = async (text, what) => {
    const r = await copyText(text);
    say(r.ok ? "Copied " + what + "." : r.message);
  };
  const copyCell = () => {
    const p = pickedCell();
    if (!p) return say("Click a cell first, then choose Copy cell.");
    return finish(textOf(p.tr, p.col), "the cell");
  };
  const copyRow = () => {
    const p = pickedCell();
    if (!p) return say("Click a cell first, then choose Copy row.");
    return finish(toTsv(copyRows([p.tr])), "the row");
  };
  const copyAll = () => finish(toTsv([cols.filter((c) => c.copy !== false).map((c) => c.label), ...copyRows(trs())]), "the table");
  const exportCsv = () => {
    const list = trs();
    const objs = list.map((tr) => trRow.get(tr));
    const keep = cols
      .map((c, i) => {
        if (c.csv === false) return null;
        const hasCopy = typeof c.copyValue === "function";
        const raw = objs.map((r) => (r ? (hasCopy ? c.copyValue(r) : getPath(r, c.key)) : undefined));
        if (raw.some(isListValue)) return null;
        const textOnly = (typeof c.render === "function" || typeof c.display === "function") && raw.every((v) => v === undefined);
        return { i, c, raw, textOnly };
      })
      .filter(Boolean);
    const lines = [keep.map((k) => k.c.label), ...list.map((tr, ri) => keep.map((k) => (k.textOnly ? tr.children[k.i].textContent : k.raw[ri])))];
    try {
      downloadCsv(csvFileName(desc.title ?? desc.sortId ?? desc.id ?? desc.rowsKey), toCsv(lines));
      say("Exported " + list.length + " row" + (list.length === 1 ? "" : "s") + ".");
    } catch (e) {
      say("Export failed: " + (e && e.message ? e.message : "the browser refused the download."));
    }
  };
  const btn = (label, title, fn) => el("button", { type: "button", class: "btn grid-tool", title, onClick: fn, text: label });
  return el("div", { class: "grid-tools" }, [
    btn("Copy cell", "Copy the last cell you clicked", copyCell),
    btn("Copy row", "Copy the row of the last cell you clicked", copyRow),
    btn("Copy all", "Copy the table with its header row, tab-separated", copyAll),
    btn("Export CSV", "Download the rows in their current order as a CSV file", exportCsv),
    status,
  ]);
}

/* Column filters (#4843). Every column that has text to match carries a small header button (`filter: false` on a
   column or the descriptor opts out; a `copy: false` control column has none) that opens a text box above the
   table. Typing alone is a case-insensitive substring match of the cell's rendered text, so it matches what the
   user sees, a list-valued cell included. A small "Match" list beside the box offers the desktop's other operators
   for the column's kind (the kind the sort decides, sortKindOf): a number column adds Equals, Not equals, >, >=, <
   and <=, which compare the SAME raw value the sort orders by (sortKeyOf over columnValue), never the formatted text,
   so "> 900" does not match "1,000" the way a string comparison would; a text column adds Equals, Not equals, Starts
   with and Ends with (a comma separates alternatives); a time column offers only the two empty tests. Is empty and Is
   not empty work on every column: a cell is empty when its raw value is null, undefined, "", blank, NaN or an empty
   list (a column drawn by display() or render() with no value of its own is empty when it shows nothing or the
   dash); 0 and false are values, and a literal "-" is a value. Several filters combine with AND. A row that fails is display:none and is recorded
   in `filteredOut`, which Copy all and Export CSV read so they carry exactly the shown rows (all columns, hidden
   and grouped-away ones too, in the current sort order). A strip under the header lists each active filter with a
   button to clear it, a Clear all button and "Showing N of M rows". The filter texts and which box is open live at
   module scope under gridSortKey(), so the 60 s repaint keeps them and another server's grid starts unfiltered.
   Escape closes the box and returns focus to its header button; focus leaving the box closes it.

   A text column also offers a list of its values to tick, like Excel's (#5565): "Select All", a search box, a
   "(Blanks)" entry and a checkbox per distinct value of the rows the grid holds now, compared ignoring case. The
   filter then is { op, text, values: { mode, set, blank } }, the value part as grid-value-filter.js defines it, and a
   row has to pass both parts. The list is not offered on a number or time column, on a column marked
   `valueList: false` (query and statement text, XML, definitions) or on one whose longest value is over
   VALUE_LIST_MAX_LEN characters; those keep the text match. A column is also left without a list when its key is
   named like a statement or prose (the shared name rule in grid-value-filter.js: text, sql, query, plan, xml, message,
   detail, description, error, script, info and the rest, which the desktop pins to the same list). The list holds the
   cell's raw value, not the text it draws, and the search folds case as .NET OrdinalIgnoreCase does. Every value
   reaches the page as a text node. The active filters are also kept in localStorage (FILTER_STORE_KEY), per route
   (which carries the server) + grid + columns, so a reload or a restart of the browser keeps them; the write waits
   300 ms of quiet and is flushed on pagehide. A text match on a column that gets no list is kept for the session only
   (never written, never read back); a value part kept on such a column shows read-only in the box with a Clear button.
   A missing, unreadable or refused store starts empty and never stops a grid from drawing. */
const gridFilters = new Map(); // table key -> Map(column id -> { op, text, values? }), least recently used first; the kept copy holds the newest 200 grids
let gridFiltersLoaded = false;
let gridFilterStoreWarned = false;
const gridValueSearch = new Map(); // table key -> the value list's search text while its box is open
const gridValueTyping = new Set(); // table keys whose value search box had the focus when the page was last drawn
const gridFilterOpen = new Map(); // table key -> column id of the open box
const gridFilterTyping = new Set(); // table keys whose box had the focus when the page was last drawn
const filteredOut = new WeakSet();
const tbodyFilter = new WeakMap();

const FILTER_OP_LABELS = {
  contains: "Contains",
  equals: "Equals",
  notEquals: "Not equals",
  gt: ">",
  gte: ">=",
  lt: "<",
  lte: "<=",
  startsWith: "Starts with",
  endsWith: "Ends with",
  isEmpty: "Is empty",
  isNotEmpty: "Is not empty",
};
const FILTER_OPS_BY_KIND = {
  number: ["contains", "equals", "notEquals", "gt", "gte", "lt", "lte", "isEmpty", "isNotEmpty"],
  time: ["contains", "isEmpty", "isNotEmpty"],
  text: ["contains", "equals", "notEquals", "startsWith", "endsWith", "isEmpty", "isNotEmpty"],
};
const filterNeedsNoText = (op) => op === "isEmpty" || op === "isNotEmpty";
/* Whether the text part of a filter narrows anything, and whether the filter as a whole does. */
const filterTextOn = (f) => !!f && (filterNeedsNoText(f.op) || !!String(f.text ?? "").trim());
const filterIsOn = (f) => filterTextOn(f) || (!!f && valuesOn(f.values));

/* The kept copy of the active filters (#5565). Every touch of the store is guarded: a browser that refuses
   localStorage, or holds something unreadable, leaves the filters in the page only, and says so once. */
function warnFilterStore(e) {
  if (gridFilterStoreWarned) return;
  gridFilterStoreWarned = true;
  if (typeof console !== "undefined" && console && typeof console.warn === "function") {
    console.warn("Column filters are not kept across a reload: " + (e && e.message ? e.message : String(e)));
  }
}

function loadGridFilters() {
  if (gridFiltersLoaded) return;
  gridFiltersLoaded = true;
  try {
    if (typeof localStorage === "undefined") return;
    const text = localStorage.getItem(FILTER_STORE_KEY);
    if (!text) return;
    for (const [k, cols] of parseFilters(text, Object.keys(FILTER_OP_LABELS))) {
      if (!gridFilters.has(k)) gridFilters.set(k, cols);
    }
  } catch (e) {
    warnFilterStore(e);
  }
}

function writeGridFilters() {
  try {
    if (typeof localStorage === "undefined") return;
    if (gridFilters.size) localStorage.setItem(FILTER_STORE_KEY, serializeFilters(gridFilters));
    else localStorage.removeItem(FILTER_STORE_KEY);
  } catch (e) {
    warnFilterStore(e);
  }
}

/* The kept copy is written at most once per FILTER_SAVE_DELAY_MS of quiet, so typing in the text match or ticking
   through a long list does not serialize and write the whole store on every keystroke and click. The page's copy
   is the live one; the write is flushed when the page is hidden or closed (pagehide), so nothing a reader did is lost. */
const FILTER_SAVE_DELAY_MS = 300;
let gridFilterSaveTimer = null;

function saveGridFilters() {
  if (gridFilterSaveTimer !== null) clearTimeout(gridFilterSaveTimer);
  gridFilterSaveTimer = setTimeout(() => {
    gridFilterSaveTimer = null;
    writeGridFilters();
  }, FILTER_SAVE_DELAY_MS);
}

/** Write a pending change now. Runs on pagehide; the tests call it for "the reader closed the page". */
export function flushGridFilters() {
  if (gridFilterSaveTimer === null) return;
  clearTimeout(gridFilterSaveTimer);
  gridFilterSaveTimer = null;
  writeGridFilters();
}

if (typeof window !== "undefined" && window && typeof window.addEventListener === "function") {
  window.addEventListener("pagehide", flushGridFilters);
}

/* A typed number: thousands separators, a percent sign, a dollar sign and spaces are dropped, as the desktop does. */
function parseFilterNumber(t) {
  const clean = String(t ?? "").trim().replace(/[,%$\s]/g, "");
  if (clean === "") return null;
  const n = Number(clean);
  return Number.isNaN(n) ? null : n;
}

function filterCellEmpty(c, raw, text) {
  const rawEmpty = isEmptyValue(raw) || (typeof raw === "string" && raw.trim() === "") || (Array.isArray(raw) && raw.length === 0);
  if (!rawEmpty) return false;
  if (typeof c.display !== "function" && typeof c.render !== "function") return true;
  const t = text.trim();
  return t === "" || t === "\u2014";
}

/* Whether one cell passes one filter. `kind` is the column's sort kind, `text` the rendered cell text, `raw` the
   value the sort reads. A numeric operator needs a number on both sides; otherwise the row does not match. */
function filterPasses(op, kind, term, c, raw, text) {
  if (op === "isEmpty") return filterCellEmpty(c, raw, text);
  if (op === "isNotEmpty") return !filterCellEmpty(c, raw, text);
  const lower = text.toLowerCase();
  const t = String(term).trim().toLowerCase();
  if (op === "contains") return lower.includes(t);
  if (kind === "number" && op !== "startsWith" && op !== "endsWith") {
    const want = parseFilterNumber(term);
    const have = sortKeyOf("number", raw);
    if (want !== null && have !== null) {
      return op === "equals" ? have === want : op === "notEquals" ? have !== want : op === "gt" ? have > want : op === "gte" ? have >= want : op === "lt" ? have < want : have <= want;
    }
    /* Not both numbers: Equals and Not equals fall back to the text, as the desktop does; an ordering has no answer. */
    if (op === "equals") return lower === t;
    if (op === "notEquals") return lower !== t;
    return false;
  }
  const terms = t.split(",").map((x) => x.trim()).filter(Boolean);
  if (!terms.length) return true;
  if (op === "equals") return terms.some((x) => lower === x);
  if (op === "notEquals") return terms.every((x) => lower !== x);
  if (op === "startsWith") return terms.some((x) => lower.startsWith(x));
  if (op === "endsWith") return terms.some((x) => lower.endsWith(x));
  return true;
}

function gridFilter(desc, cols, head, tbody) {
  const key = gridSortKey(desc, cols);
  const idx = cols.map((_, i) => i).filter((i) => cols[i].filter !== false && cols[i].copy !== false);
  if (!idx.length) return null;
  loadGridFilters();
  /* Drawing a grid uses it: the one the reader came back to is the last the store lets go of. */
  if (gridFilters.has(key)) {
    const mine = gridFilters.get(key);
    gridFilters.delete(key);
    gridFilters.set(key, mine);
  }
  /* A text match read back from the kept copy on a column that gets no list (valueList: false) is dropped: that text lasts
     the session only. A value part it holds stays, shown read-only in the box with a way to clear it. */
  const loaded = gridFilters.get(key);
  if (loaded) {
    let pruned = false;
    for (const c of cols) {
      const f = loaded.get(colId(c));
      if (!f || f.session || listableColumn(c)) continue;
      pruned = true;
      if (valuesOn(f.values)) loaded.set(colId(c), { op: "contains", text: "", values: f.values, session: true });
      else loaded.delete(colId(c));
    }
    if (pruned) {
      if (!loaded.size) gridFilters.delete(key);
      saveGridFilters();
    }
  }
  const active = () => gridFilters.get(key) || new Map();
  const bar = el("div", { class: "grid-filter-bar", role: "group", "aria-label": "Column filters" });
  const popHolder = el("div", { class: "grid-filter-holder" });
  const chips = el("div", { class: "grid-filter-chips" });
  bar.appendChild(popHolder);
  bar.appendChild(chips);
  const btns = new Map();
  /* A filter is { op, text, values? }; the value-less operators are active with no text, and a value part is active
     when it hides or shows a chosen set. */
  const isOn = filterIsOn;
  let valuesRefresh = null; // rebuilds the open value list from the rows now
  let valueSearchBox = null; // the open value list's search box
  /* The sort kind of column i over the rows now in the body: number, time or text. */
  const kindOf = (i) =>
    sortKindOf(
      cols[i],
      Array.from(tbody.children, (tr) => (trRow.has(tr) ? columnValue(trRow.get(tr), cols[i]) : undefined))
    );
  /* The operator in force: a stored one the column's kind does not offer (the rows changed under it) falls back to Contains. */
  const opFor = (f, kind) => (f && FILTER_OPS_BY_KIND[kind].includes(f.op) ? f.op : "contains");

  /* What one cell is in the value list: the column's raw value, not the rendered text (#5565). A cell is blank when its
     raw value is null, empty or whitespace, whatever the cell draws. A text column's raw value is the string the cell
     renders, so for plain text the two are the same; they differ only where a column draws something other than its
     value (a format, a badge), and then the raw value is what the desktop's list reads too. A raw value that is a list
     or an object has no text of its own, so the rendered text stands for it. A row with no raw field at all
     (undefined) under a column that draws its cell with `render` or `display` is a column built from other fields
     (the Alerts Status column): the shown text stands for it, as it did before the list read raw values, so the list
     holds what the cells show and a stored value that names a shown label still matches. */
  function listCell(c, raw, text) {
    if (raw === undefined && (typeof c.display === "function" || typeof c.render === "function")) {
      return filterCellEmpty(c, raw, text) ? { blank: true, text: "" } : { blank: false, text };
    }
    if (isEmptyValue(raw) || (typeof raw === "string" && raw.trim() === "") || (Array.isArray(raw) && raw.length === 0)) return { blank: true, text: "" };
    if (typeof raw === "string") return { blank: false, text: raw };
    if (typeof raw === "number" || typeof raw === "boolean" || typeof raw === "bigint") return { blank: false, text: String(raw) };
    return { blank: text.trim() === "", text };
  }

  function applyRows() {
    const tests = [];
    cols.forEach((c, i) => {
      const f = active().get(colId(c));
      if (isOn(f)) {
        const kind = kindOf(i);
        tests.push({ i, op: opFor(f, kind), kind, term: f.text ?? "", textOn: filterTextOn(f), vals: valuesOn(f.values) ? compileValues(f.values) : null });
      }
    });
    let shown = 0;
    let total = 0;
    for (const tr of tbody.children) {
      total++;
      const out = tests.some(({ i, op, kind, term, textOn, vals }) => {
        const td = tr.children[i];
        if (!td) return true;
        const raw = trRow.has(tr) ? columnValue(trRow.get(tr), cols[i]) : undefined;
        if (textOn && !filterPasses(op, kind, term, cols[i], raw, td.textContent)) return true;
        if (!vals) return false;
        const cell = listCell(cols[i], raw, td.textContent);
        return !valuePasses(vals, cell.blank, cell.text);
      });
      if (out) filteredOut.add(tr);
      else {
        filteredOut.delete(tr);
        shown++;
      }
      if (tr.style) tr.style.display = out ? "none" : "";
    }
    return { shown, total, tests: tests.length };
  }

  /* Change one column's filter: `patch` is merged over what it holds, a filter that narrows nothing is dropped, and
     the active filters are kept. The text part and the value part change on their own. */
  function patchFilter(c, patch) {
    const next = new Map(active());
    const cur = next.get(colId(c)) || { op: "contains", text: "" };
    const merged = { ...cur, ...patch };
    if (!valuesOn(merged.values)) delete merged.values;
    /* A text match on a column that gets no list (statement, query, plan, XML, message...) lasts the session: it is
       never written to the kept copy, and never read back (serializeFilters, parseFilters). */
    if (!listableColumn(c)) merged.session = true;
    if (isOn(merged)) next.set(colId(c), merged);
    else next.delete(colId(c));
    gridFilters.delete(key);
    if (next.size) gridFilters.set(key, next);
    saveGridFilters();
  }
  const setFilter = (c, op, text) => patchFilter(c, { op, text });
  const clearColumn = (c) => patchFilter(c, { op: "contains", text: "", values: NO_VALUES });

  function renderChips() {
    const r = applyRows();
    chips.textContent = "";
    if (r.tests) {
      for (const i of idx) {
        const c = cols[i];
        const f = active().get(colId(c));
        if (!isOn(f)) continue;
        const op = opFor(f, kindOf(i));
        const textChip = !filterTextOn(f) ? "" : op === "contains" ? String(f.text).trim() : FILTER_OP_LABELS[op] + (filterNeedsNoText(op) ? "" : " " + String(f.text).trim());
        const chipText = [valuesPhrase(f.values), textChip].filter(Boolean).join(", ");
        const x = el("button", { type: "button", class: "grid-filter-x", text: "×", title: "Clear the filter on " + c.label, "aria-label": "Clear the filter on " + c.label });
        x.addEventListener("click", () => {
          clearColumn(c);
          renderPop();
          renderChips();
        });
        chips.appendChild(el("span", { class: "grid-filter-chip" }, [el("span", { text: c.label + ": " + chipText }), x]));
      }
      const all = el("button", { type: "button", class: "btn grid-filter-clear-all", text: "Clear all filters" });
      all.addEventListener("click", () => {
        gridFilters.delete(key);
        saveGridFilters();
        renderPop();
        renderChips();
      });
      chips.appendChild(all);
      chips.appendChild(el("span", { class: "grid-filter-count", role: "status", "aria-live": "polite", text: "Showing " + r.shown + " of " + r.total + " rows" }));
    }
    for (const [i, b] of btns) {
      const on = isOn(active().get(colId(cols[i])));
      b.className = "col-filter-btn" + (on ? " on" : "");
      b.setAttribute("aria-pressed", on ? "true" : "false");
    }
    if (bar.style) bar.style.display = r.tests || gridFilterOpen.has(key) ? "" : "none";
  }

  function close(refocus) {
    const open = gridFilterOpen.get(key);
    gridFilterOpen.delete(key);
    gridFilterTyping.delete(key);
    gridValueTyping.delete(key);
    gridValueSearch.delete(key);
    renderPop();
    renderChips();
    const i = cols.findIndex((c) => colId(c) === open);
    const b = btns.get(i);
    if (refocus && b && typeof b.focus === "function") b.focus();
  }

  /* The distinct values of column i over every row the body holds now (before any filter), and whether the column is
     offered as a list: a text column, not opted out by `valueList: false` or by the shared name rule (query and
     statement text, plans, XML, scripts, messages, details, descriptions, errors), and no value longer than
     VALUE_LIST_MAX_LEN. */
  function universeOf(i) {
    const c = cols[i];
    return collectValues(
      Array.from(tbody.children, (tr) => {
        const td = tr.children[i];
        return listCell(c, trRow.has(tr) ? columnValue(trRow.get(tr), c) : undefined, td ? td.textContent : "");
      })
    );
  }
  const listed = (i, uni) => listableColumn(cols[i]) && kindOf(i) === "text" && uni.maxLen <= VALUE_LIST_MAX_LEN;

  /* The checklist under the text match: a search box, Select All, "(Blanks)" when any cell is blank and one checkbox
     per value, at most VALUE_LIST_CAP of them (the search reaches the rest). Ticking works on the whole list, not on
     what the search shows, except Select All, which ticks or unticks every value the search matches. Returns null for a
     column that is not offered a list. */
  function valuesSection(c, i) {
    if (!listed(i, universeOf(i))) return null;
    const id = colId(c);
    const search = el("input", { type: "search", class: "gfv-search", "aria-label": "Search the values of " + c.label, placeholder: "Search values…" });
    search.value = gridValueSearch.get(key) || "";
    const allBox = el("input", { type: "checkbox", "aria-label": "Select All" });
    const allRow = el("label", { class: "gfv-item gfv-all" }, [allBox, el("span", { text: "Select All" })]);
    const list = el("div", { class: "gfv-list", role: "group", "aria-label": "Values of " + c.label });
    const capNote = el("div", { class: "gfv-note gfv-cap-note", role: "status", "aria-live": "polite" });
    const noneNote = el("div", { class: "gfv-note gfv-none-note", role: "status", "aria-live": "polite" });
    const node = el("div", { class: "grid-filter-values" }, [search, allRow, list, capNote, noneNote]);
    let uni = null;
    let matches = [];
    let items = []; // { box, key } for a value, { box, blank: true } for (Blanks)
    const current = () => (active().get(id) || {}).values || NO_VALUES;
    const query = () => valueKey(search.value.trim()); // folded like the values, as .NET OrdinalIgnoreCase does
    const blankListed = () => uni.blank && query() === "";

    /* Set the boxes from the filter in force; the boxes stay where they are, so keyboard focus is not lost. */
    function sync() {
      const v = compileValues(current());
      let ticked = 0;
      for (const it of items) {
        const on = it.blank ? valuePasses(v, true, "") : valuePasses(v, false, it.value);
        it.box.checked = on;
        if (on) ticked++;
      }
      allBox.checked = items.length > 0 && ticked === items.length;
      allBox.indeterminate = ticked > 0 && ticked < items.length;
      const f = current();
      noneNote.textContent = f.mode === "showOnly" && f.set.length === 0 && !f.blank ? "No values are ticked, so no rows show." : "";
    }

    /* Rebuild the rows from the grid and the search text. */
    function rebuild() {
      uni = universeOf(i);
      const q = query();
      matches = q ? uni.values.filter((v) => valueKey(v).includes(q)) : uni.values;
      const shownValues = matches.slice(0, VALUE_LIST_CAP);
      items = [];
      const rows = [];
      if (blankListed()) {
        const box = el("input", { type: "checkbox", "aria-label": "(Blanks)" });
        box.addEventListener("change", () => change((m) => (m.blank = box.checked)));
        items.push({ box, blank: true });
        rows.push(el("label", { class: "gfv-item gfv-blank" }, [box, el("span", { text: "(Blanks)" })]));
      }
      for (const v of shownValues) {
        const box = el("input", { type: "checkbox", "aria-label": v });
        box.addEventListener("change", () => change((m) => (box.checked ? m.ticked.add(valueKey(v)) : m.ticked.delete(valueKey(v)))));
        items.push({ box, value: v });
        rows.push(el("label", { class: "gfv-item" }, [box, el("span", { text: v })]));
      }
      list.textContent = "";
      for (const r of rows) list.appendChild(r);
      if (!rows.length) list.appendChild(el("div", { class: "gfv-empty", text: q ? "No value matches the search." : "No values." }));
      capNote.textContent = matches.length > VALUE_LIST_CAP ? "Showing " + VALUE_LIST_CAP.toLocaleString("en-US") + " of " + matches.length.toLocaleString("en-US") + " values. Search to find the rest." : "";
      sync();
    }

    function change(mutate) {
      patchFilter(c, { values: nextValues(uni, current(), mutate) });
      renderChips();
      sync();
    }

    allBox.addEventListener("change", () => {
      const on = allBox.checked;
      change((m) => {
        for (const v of matches) {
          if (on) m.ticked.add(valueKey(v));
          else m.ticked.delete(valueKey(v));
        }
        if (blankListed()) m.blank = on;
      });
    });
    search.addEventListener("input", () => {
      gridValueSearch.set(key, search.value);
      rebuild();
    });
    search.addEventListener("focus", () => gridValueTyping.add(key));
    rebuild();
    return { node, search, refresh: rebuild, sync };
  }

  function renderPop() {
    popHolder.textContent = "";
    valuesRefresh = null;
    const open = gridFilterOpen.get(key);
    const i = open == null ? -1 : cols.findIndex((c) => colId(c) === open);
    if (i < 0 || !btns.has(i)) {
      gridFilterOpen.delete(key);
      return null;
    }
    const c = cols[i];
    const input = el("input", { type: "text", class: "grid-filter-input", "aria-label": "Filter " + c.label, placeholder: "contains…" });
    const stored = active().get(colId(c));
    const kind = kindOf(i);
    const ops = FILTER_OPS_BY_KIND[kind];
    let op = opFor(stored, kind);
    input.value = stored ? stored.text ?? "" : "";
    const opSel = el("select", { class: "grid-filter-op", "aria-label": "Match " + c.label }, ops.map((o) => el("option", { value: o, text: FILTER_OP_LABELS[o] })));
    opSel.value = op;
    const syncInput = () => {
      if (input.style) input.style.display = filterNeedsNoText(op) ? "none" : "";
    };
    syncInput();
    const clear = el("button", { type: "button", class: "btn grid-filter-clear", text: "Clear" });
    const done = el("button", { type: "button", class: "btn grid-filter-close", text: "Close" });
    const values = valuesSection(c, i);
    /* A value part kept on a column that no longer gets a list: shown read-only, with a way to clear it. */
    let kept = null;
    if (!values && stored && valuesOn(stored.values)) {
      const clearKept = el("button", { type: "button", class: "btn gfv-kept-clear", text: "Clear the value filter" });
      clearKept.addEventListener("click", () => {
        patchFilter(c, { values: NO_VALUES });
        renderPop();
        renderChips();
      });
      kept = el("div", { class: "gfv-kept" }, [el("span", { class: "gfv-note gfv-kept-note", text: "A kept value filter " + valuesPhrase(stored.values) + ". This column has no value list." }), clearKept]);
    }
    const panel = el("div", { class: "grid-filter-pop", role: "dialog", "aria-label": "Filter " + c.label }, [el("span", { class: "grid-filter-label", text: c.label }), ops.length > 1 ? opSel : null, input, clear, done, values ? values.node : kept].filter(Boolean));
    if (values) valuesRefresh = values.refresh;
    valueSearchBox = values ? values.search : null;
    input.addEventListener("input", () => {
      setFilter(c, op, input.value);
      renderChips();
    });
    opSel.addEventListener("change", () => {
      op = opSel.value;
      setFilter(c, op, input.value);
      syncInput();
      renderChips();
    });
    input.addEventListener("focus", () => gridFilterTyping.add(key));
    input.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        close(true);
      }
    });
    clear.addEventListener("click", () => {
      clearColumn(c);
      input.value = "";
      if (values) values.sync();
      renderChips();
      if (typeof input.focus === "function") input.focus();
    });
    done.addEventListener("click", () => close(true));
    panel.addEventListener("keydown", (e) => {
      if (e.key === "Escape") {
        e.preventDefault();
        close(true);
      }
    });
    panel.addEventListener("focusout", (e) => {
      const to = e && e.relatedTarget;
      if (to && (to === btns.get(i) || (typeof panel.contains === "function" && panel.contains(to)))) return;
      setTimeout(() => {
        if (panel.isConnected === false || gridFilterOpen.get(key) !== colId(c)) return;
        const at = typeof document !== "undefined" ? document.activeElement : null;
        if (at && typeof panel.contains === "function" && panel.contains(at)) return;
        gridFilterTyping.delete(key);
        gridValueTyping.delete(key);
        close(false);
      }, 0);
    });
    popHolder.appendChild(panel);
    return input;
  }

  for (const i of idx) {
    const c = cols[i];
    const b = el("button", { type: "button", class: "col-filter-btn", title: "Filter " + c.label, "aria-label": "Filter " + c.label, "aria-pressed": "false" });
    /* The header's own click and key handlers sort; this button must not reach them. */
    b.addEventListener("click", (e) => {
      if (e && typeof e.stopPropagation === "function") e.stopPropagation();
      if (gridFilterOpen.get(key) === colId(c)) return close(true);
      gridFilterOpen.set(key, colId(c));
      const input = renderPop();
      renderChips();
      if (input && typeof input.focus === "function") input.focus();
    });
    b.addEventListener("keydown", (e) => {
      if (e && typeof e.stopPropagation === "function") e.stopPropagation();
    });
    btns.set(i, b);
    if (head.children[i]) head.children[i].appendChild(b);
  }
  const input = renderPop();
  renderChips();
  tbodyFilter.set(tbody, {
    reapply: () => {
      renderChips();
      if (valuesRefresh) valuesRefresh();
    },
  });
  /* The page was redrawn while a box had the focus: give it back once the new box is in the document. */
  const refocus = gridValueTyping.has(key) && valueSearchBox ? valueSearchBox : input && gridFilterTyping.has(key) ? input : null;
  if (refocus) {
    setTimeout(() => {
      if (refocus.isConnected !== false && typeof refocus.focus === "function") refocus.focus();
    }, 0);
  }
  return bar;
}

/* An unsortable header cell. The sortable one is built by makeGridSort().headerCell; both are the place a later
   header affordance (a filter, a menu) attaches. */
function headerCell(c) {
  return el("th", { text: c.label, class: (isNumericCol(c) ? "num" : "") + (c.exportOnly ? " export-only" : "") + (c.wrapHead ? " wrap-head" : "") || null, title: c.hint || null });
}

/* The sort state of every grid, at MODULE scope so the 60 s poll's rebuild of a page re-applies the chosen sort.
   Keyed by gridSortKey(): the route (the hash without its query, so a server or tab switch is a different table)
   plus the descriptor's identity plus its column set. State: { col, dir } with dir "asc" | "desc"; absent means
   the server's order. */
const gridSortState = new Map(); // grows by one entry per table the session sorts (routes x tables), so it is bounded and never pruned
/* tr -> its row object, so a grid whose rows are reconciled in place (Alert History) can re-sort what is in the DOM. */
const trRow = new WeakMap();
const tbodyGrid = new WeakMap();
/* The cell each grid's Copy cell / Copy row act on (see gridTools), keyed like the sort state. */
const gridPicked = new Map(); // one entry per table the session clicks in; bounded by routes x tables
/* The column groups switched on for each grid (see columnPicker), keyed like the sort state; absent means defaultGroups. */
const gridGroupsOn = new Map(); // one entry per grouped table the session toggles; bounded by routes x tables

/* The table identity: `desc.sortId`, else `desc.id`, else `desc.title`, else `desc.rowsKey`. Panels built by
   renderPanel carry a title; the FinOps tabs call VIZ.table with a rowsKey, and two grids of one tab can share one,
   so the identity also carries the column keys. */
function gridSortKey(desc, cols) {
  const route = typeof location !== "undefined" && location && typeof location.hash === "string" ? location.hash.split("?")[0] : "";
  const id = desc.sortId ?? desc.id ?? desc.title ?? desc.rowsKey ?? "";
  return route + "|" + id + "|" + cols.map(colId).join(",");
}

function colId(c) {
  return String(c.key ?? "") + "\u0001" + String(c.label ?? "");
}

function isEmptyValue(v) {
  return v == null || v === "" || (typeof v === "number" && Number.isNaN(v));
}

/* "time" | "number" | "text" for a column, from how it declares its format; a column with no format decides from
   its values (every present one a number is a number). */
function sortKindOf(c, values) {
  if (c.format === "time" || c.format === "reltime") return "time";
  if (c.format === "bool") return "number";
  if (isNumericCol(c)) return "number";
  const present = values.filter((v) => !isEmptyValue(v));
  return present.length && present.every((v) => typeof v === "number") ? "number" : "text";
}

/* The raw value a column holds for a row: what the sort orders by and what the numeric filters compare. */
function columnValue(row, c) {
  return typeof c.sortValue === "function" ? c.sortValue(row) : getPath(row, c.key);
}

function sortKeyOf(kind, v) {
  if (isEmptyValue(v)) return null;
  if (kind === "time") {
    const d = v instanceof Date ? v : parseUtc(typeof v === "string" ? v : null);
    return d && !Number.isNaN(d.getTime()) ? d.getTime() : typeof v === "number" ? v : null;
  }
  if (kind === "number") {
    const n = typeof v === "boolean" ? (v ? 1 : 0) : Number(v);
    return Number.isNaN(n) ? null : n;
  }
  return String(v).toLowerCase();
}

/** Compare two sort keys (null last, regardless of dir). Exported for the behaviour test. */
export function compareSortKeys(a, b, dir) {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  const r = a < b ? -1 : a > b ? 1 : 0;
  return dir === "desc" ? -r : r;
}

function makeGridSort(desc, cols, rows) {
  const stateKey = gridSortKey(desc, cols);
  let kinds = [];
  let sortable = [];
  /* A column sorts when it is not opted out, some row has a value to sort on (a custom-render column over a key the
     rows do not carry has nothing to order), and that value is not an array or object (a list cell has no order
     unless the column supplies a sortValue). Recomputed from the rows whenever they change. */
  function classify(rowList) {
    kinds = cols.map((c) => sortKindOf(c, rowList.map((r) => valueOf(r, c))));
    sortable = cols.map((c) => {
      if (c.sortable === false) return false;
      const present = rowList.map((r) => valueOf(r, c)).filter((v) => !isEmptyValue(v));
      return present.length > 0 && (typeof c.sortValue === "function" || !present.some((v) => typeof v === "object" && !(v instanceof Date)));
    });
    grid.sortable = sortable;
  }
  const ths = [];
  let tbody = null;
  let moved = false;

  const valueOf = columnValue;

  function indicate() {
    const st = gridSortState.get(stateKey);
    cols.forEach((c, i) => {
      const th = ths[i];
      if (!th) return;
      const live = sortable[i];
      th.className = (live ? "sortable" + (isNumericCol(c) ? " num" : "") : isNumericCol(c) ? "num" : "") + (c.exportOnly ? " export-only" : "") + (c.wrapHead ? " wrap-head" : "");
      th.setAttribute("tabindex", live ? "0" : "-1");
      /* The column's `hint` is its header tooltip; this refresh runs after the header is built, so it must not replace it. */
      th.setAttribute("title", live ? (c.hint ? c.hint + " (click to sort)" : "Sort by " + c.label) : c.hint || "");
      const on = live && st && st.col === colId(c);
      th.setAttribute("aria-sort", on ? (st.dir === "asc" ? "ascending" : "descending") : "none");
      th.sortInd.textContent = on ? (st.dir === "asc" ? " ▲" : " ▼") : "";
    });
  }

  /* Orders `trs` (taken as the server's order) by the current state and puts them in the tbody. */
  function apply(trs) {
    const st = gridSortState.get(stateKey);
    let ordered = trs;
    const ci = st ? cols.findIndex((c) => colId(c) === st.col) : -1;
    if (ci >= 0 && sortable[ci]) {
      const keyed = trs.map((tr, i) => ({ tr, i, k: sortKeyOf(kinds[ci], valueOf(trRow.get(tr), cols[ci])) }));
      keyed.sort((x, y) => compareSortKeys(x.k, y.k, st.dir) || x.i - y.i);
      ordered = keyed.map((x) => x.tr);
    }
    /* A grid still in the server's order (no sort chosen, none undone) is left as built: the DOM is only touched
       once a sort has been in play. */
    if (ordered !== trs || moved) {
      for (const tr of ordered) tbody.appendChild(tr);
      moved = ordered !== trs;
    }
    indicate();
  }

  const grid = {
    sortable: [],
    /* A header for every column that is not opted out: whether it carries the sort affordance is re-decided by
       indicate() against the current rows. */
    eligible: cols.map((c) => c.sortable !== false),
    headerCell(c, i) {
      const ind = el("span", { class: "sort-ind", "aria-hidden": "true" });
      const th = el("th", { class: "sortable" + (isNumericCol(c) ? " num" : "") + (c.exportOnly ? " export-only" : "") + (c.wrapHead ? " wrap-head" : ""), tabindex: "0", "aria-sort": "none", title: c.hint ? c.hint + " (click to sort)" : "Sort by " + c.label }, [c.label, ind]);
      th.sortInd = ind;
      const cycle = () => {
        if (!sortable[i]) return;
        const st = gridSortState.get(stateKey);
        const id = colId(c);
        if (!st || st.col !== id) gridSortState.set(stateKey, { col: id, dir: "asc" });
        else if (st.dir === "asc") gridSortState.set(stateKey, { col: id, dir: "desc" });
        else gridSortState.delete(stateKey);
        apply(grid.serverOrder);
      };
      th.addEventListener("click", cycle);
      th.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          cycle();
        }
      });
      ths[i] = th;
      return th;
    },
    syncHeads: () => indicate(),
    serverOrder: [],
    attach(body, trs) {
      tbody = body;
      grid.serverOrder = trs.slice();
      classify(trs.map((tr) => trRow.get(tr)));
      tbodyGrid.set(body, grid);
      apply(grid.serverOrder);
    },
    /* The tbody's rows were reconciled in place into the server's order: take that as the new server order. */
    reapply() {
      grid.serverOrder = [...tbody.children];
      classify(grid.serverOrder.map((tr) => trRow.get(tr)));
      apply(grid.serverOrder);
    },
  };
  return grid;
}

/** The row object a rendered grid row was built from (a sorted grid's DOM order is not the row order). */
export function gridRowOf(tr) {
  return trRow.get(tr);
}

/** For a grid whose rows are reconciled in place (Alert History): after putting the rows back in the server's order,
    call this with the tbody to re-apply the chosen sort. A tbody that is not a sortable grid is left alone. */
export function reapplyGridSort(tbody) {
  const g = tbody && tbodyGrid.get(tbody);
  if (g) g.reapply();
  const f = tbody && tbodyFilter.get(tbody);
  if (f) f.reapply();
}

/* A table column may depend on the rows: `hideWhenEmpty: true` drops it when no row has a value at its key (null,
   undefined or "" is empty; 0 and false are values). Database Sizes uses it for its Note column, which only the row
   for another database on an Azure SQL Database server fills, so every other server shows no column of dashes. A
   column without the option is always kept. When the option would drop every column the list is kept as it is, so
   the table never renders with no columns. */
export function visibleColumns(cols, rows) {
  const kept = cols.filter((c) => {
    /* #7416: `hideWhenAllZero` drops a count column that is zero (or missing) on every row: a column of zeros on a healthy server says nothing, and a
       column that has a number says why it is there in its header hint. */
    if (c.hideWhenAllZero && !rows.some((row) => Number(getPath(row, c.key)) > 0)) return false;
    if (!c.hideWhenEmpty && !c.hideWhenZero) return true;
    return rows.some((row) => {
      const v = getPath(row, c.key);
      /* #7410: `hideWhenZero` also drops a column whose every value is 0, the counters only some engines fill (Top Query Shapes' temp blocks, WAL and peak memory). */
      return v != null && v !== "" && !(c.hideWhenZero && Number(v) === 0);
    });
  });
  return kept.length ? kept : cols;
}

function isNumericCol(c) {
  return c.align === "right" || ["int", "id", "num1", "num2", "rate", "ms", "mb", "mbfit", "bytes", "pct"].includes(c.format);
}

/* #7410: a column with `exportOnly: true` is built and not drawn (`.export-only`), so Copy and the CSV carry its value under its label. The Logging
   Settings Audit folds Unit and Source into the Value cell and the cost into the remedy cell, and the export still keeps each as its own field. */
function exportOnlyCell(row, c) {
  const td = cell(row, c);
  if (c.exportOnly) td.classList.add("export-only");
  return td;
}

function cell(row, c) {
  /* A column may supply a custom cell renderer (row) -> Node — used for the alert status/detail columns and the
     query-text expander. It owns its own content; wrap/mono classes still apply if the column asks for them. */
  if (typeof c.render === "function") {
    const rcls = [];
    /* #6353: a render column keeps the numeric alignment its header already has (`align: "right"` or a numeric format) and the
       server-computed band its `sevKey` names, as a plain column does. The Compare delta cells set both and were drawn left-aligned and
       uncoloured. Only the Compare deltas set `sevKey` with `render`; the two FinOps size columns that set `align: "right"` with `render`
       now align their cells under their right-aligned headers. */
    if (isNumericCol(c)) rcls.push("num");
    if (c.clamp) rcls.push("clamp");
    else if (c.wrap) rcls.push("wrap");
    /* #7607: `follow: true` on a wrap column makes its width follow the window (`wrap-follow`). A column flag, not a cellClass function, because a stored dashboard template spreads these column lists and reads only literals. */
    if (c.follow) rcls.push("wrap-follow");
    if (c.mono) rcls.push("mono");
    if (c.pre) rcls.push("pre");
    if (c.sevKey) rcls.push(sevClass(getPath(row, c.sevKey)));
    if (typeof c.cellClass === "function") { const k = c.cellClass(row); if (k) rcls.push(k); }
    return el("td", { class: rcls.join(" ") || null }, [c.render(row)]);
  }
  const found = getPath(row, c.key);
  /* #7578: `nullAs` is the value a missing figure means (see the stat tile). */
  const raw = found == null && c.nullAs != null ? c.nullAs : found;
  const cls = [];
  if (isNumericCol(c)) cls.push("num");
  if (c.name) cls.push(c.path ? "name-cell path-cell" : "name-cell");
  else if (c.clamp) cls.push("clamp");
  else if (c.wrap) cls.push("wrap");
  if (c.follow) cls.push("wrap-follow");
  if (c.mono) cls.push("mono");
  if (c.pre) cls.push("pre");
  if (c.sevKey) cls.push(sevClass(getPath(row, c.sevKey)));
  if (c.statusSev) cls.push(sevClass(statusToSev(raw)));
  if (typeof c.cellClass === "function") { const k = c.cellClass(row); if (k) cls.push(k); }
  /* nullKey names another field of the same row that says why this one is empty (get_file_io_stats' size_note:
     "n/a (log service)" for the log file of a Hyperscale database). The server wrote the sentence; the page only
     shows it in place of the bare em dash. */
  const why = raw == null && c.nullKey ? getPath(row, c.nullKey) : null;
  const shownPlain =
    why != null && why !== ""
      ? String(why)
      : typeof c.display === "function"
        ? String(c.display(row) ?? "—")
        : c.format
        ? applyFormat(c.format, raw)
        : raw == null || raw === ""
          ? "—"
          : c.plain === "named"
            ? plainTextNamed(String(raw))
            : c.plain === "counts"
              ? plainTextCounts(String(raw))
              : c.plain
                ? plainText(String(raw))
                : String(raw);
  /* #7725: a row the server marked database_dropped (work in a database the server no longer lists) reads "<name> (dropped)" in its Database cell,
     on every grid that has one (Top Procedures, Top Queries, and the FinOps lists). The name itself is untouched in the row. */
  const shown = c.key === "database_name" && getPath(row, "database_dropped") === true ? shownPlain + " (dropped)" : shownPlain;
  /* #5644 W20: a column marked `localizeText` (a message or note: collection-health notes, collector errors) shows any ISO
     instant in its sentence in local time, like the grids beside it. Opt-in: query text, statement text, details and
     identifiers are data, and a literal such as '2026-01-01T00:00:00' inside them must show as written. */
  /* #5644 W20: `localizeAlertDetail` is the same opt-in for an alert's stored body (get_alert_history's detail_text): only the
     labelled time lines ("Incident Since:", "Started:", "From:", "To:", "Window:", ...) and the sample-age line shift, because the
     same body carries query and SQL lines that must print as written. Never use `localizeText` on a column that holds SQL. */
  const text = c.localizeAlertDetail && !c.format
    ? localizeAlertDetail(shown, getPath(row, "newest_sample"))
    : c.localizeText && !c.format ? localizeIsoText(shown) : withheldDisplayText(shown); /* #7548: the stored marker reads as words, nothing else changes */
  /* #7533: `name: true` is a name column (Database, File, Path, Job, Instance): one line under the stylesheet's window-following cap, an ellipsis, and the whole name in the cell's title.
     `path: true` (a file path) cuts the START instead, so the file name at the end stays visible (#7533 review): see clampText.
     It does not open on a click, because a name is read and copied, not expanded. Copy cell and the CSV read the row, so they carry the whole name too. */
  if (c.name) return el("td", { class: cls.join(" ") || null }, [clampText(text, { expandable: false, path: c.path === true })]);
  if (c.clamp) return el("td", { class: cls.join(" ") || null }, [clampText(text)]);
  /* #6154: `detailKey` names a field of the same row that holds the long explanation. The cell shows only its short value and the
     explanation opens under it on a click (a <details>, so the keyboard works), so a row stays one line tall. Text that is the same
     for every row belongs in the column's `hint` (the header tooltip) instead. */
  if (c.detailKey) {
    const full = getPath(row, c.detailKey);
    if (typeof full === "string" && full !== "") {
      const detail = el("p", { class: "cell-detail-text", text: plainText(full) });
      const box = disclosure(text, detail, { max: c.detailMax || 60, cell: true });
      return el("td", { class: cls.join(" ") || null }, [box]);
    }
  }
  return el("td", { class: cls.join(" ") || null, text });
}

/* A stat tile may depend on another value in the same payload: `hideWhen: { key, equals }` drops it when that value
   equals `equals`, `showWhen: { key, equals }` keeps it only then. The Server Properties list uses the pair on
   `engine_edition`: an Azure SQL Database (5) reports the HOST's sockets, cores per socket, hyperthread ratio and
   physical memory, which are not the database's, so those tiles are not drawn and its vCores tile is (a tile with
   neither field is always drawn, so its own logical CPU count still is). */
export function visibleStats(stats, data) {
  return stats.filter((s) => {
    if (s.hideWhen && getPath(data, s.hideWhen.key) === s.hideWhen.equals) return false;
    if (s.showWhen && getPath(data, s.showWhen.key) !== s.showWhen.equals) return false;
    return true;
  });
}

/* #7729: the stat grid is auto-fit, so each row fills with as many 150 px tiles as fit and the rest wrap: 15 tiles at 1536 px ran 7, 7 and 1, with
   Buffer pool alone on a third row. Once the grid is laid out, the column count is the fewest columns that keep the same number of rows
   (15 tiles, at most 7 across: 3 rows, so 5 across). It follows the width, so 1280 and 2560 px each balance on their own. A grid that fits on one
   row keeps one column a tile. Where there is no ResizeObserver (a test's fake DOM) the CSS auto-fit stays. */
function balanceTiles(grid, count) {
  if (count < 3 || typeof ResizeObserver !== "function") return;
  new ResizeObserver(() => {
    const width = grid.clientWidth;
    if (!width) return;
    const gap = parseFloat(getComputedStyle(grid).columnGap) || 12;
    const across = Math.max(1, Math.floor((width + gap) / (150 + gap)));
    const rows = Math.ceil(count / across);
    grid.style.gridTemplateColumns = "repeat(" + Math.ceil(count / rows) + ", minmax(0, 1fr))";
  }).observe(grid);
}

/* stat: desc = { stats:[{key,label,format,small?,sev?,nullKey?}], emptyText? } over the tool's top-level object. A stat
   descriptor may carry a PRE-COMPUTED severity (`sev`/`severity`, e.g. "Critical") — colored here from that hint
   only (R1: the browser never re-derives a band); absent the hint the value keeps the default color. `nullKey` is
   the table cell's rule (cell() above) for a tile: another field of the payload that says why this one is empty
   (get_memory_stats' system_memory_state_note, "n/a (...)" on an Azure SQL Database), shown in place of the dash. */
function vizStat(data, desc) {
  const stats = visibleStats(Array.isArray(desc.stats) ? desc.stats : [], data);
  if (!stats.length) return emptyStrip(NO_FIELDS_MSG);
  /* The stat twin of vizLine's zero-points guard, and it exists for the same failure (#2530). Several reads
     answer their HEALTHY case with a data body carrying a prose `finding` and none of the summary keys —
     get_pg_xmin_horizon's {status:"no_holder", finding} is the clearest: it is not the {status,message}
     envelope, so classifyResponse calls it data, it reaches a viz, and a tile set over keys the body does not
     have renders as a row of em-dashes that says nothing. Every key resolving to null is the only state in
     which the descriptor's sentence is more informative than the tiles, so that is exactly when it wins; one
     key with a value still renders the tiles, and a descriptor with no emptyText (every stored view, and
     every SQL Server tile on the server page) falls through unchanged. */
  if (desc.emptyText && stats.every((s) => getPath(data, s.key) == null)) return emptyStrip(desc.emptyText);
  /* `hideWhenEmpty` drops a tile whose value is missing (and that has no sentence of its own to show instead): a panel
     of fifteen tiles, half of them dashes, said less than the seven that had a value. */
  const shown = stats.filter((s) => !s.pageCut && !(s.hideWhenEmpty && getPath(data, s.key) == null && !(s.nullKey && getPath(data, s.nullKey))));
  /* #7410: a stat with `pageCut` is no tile. The read's "the window held more than this page" flag is about the grid, not the server,
     so when it is true the panel says so in words (the stat's `pageCut` sentence, or what its `pageCut(data)` returns when the sentence carries a count, #7551) under the tiles. */
  const cutNotes = stats.filter((s) => s.pageCut && getPath(data, s.key) === true).map((s) => noticeStrip(typeof s.pageCut === "function" ? s.pageCut(data) : s.pageCut));
  const tile = (s) => {
    const sev = s.sev || s.severity;
    /* `display(data)` is a tile whose text is made from more than one field of the answer (the Alert setup tile says how many gaps there are). */
    const made = typeof s.display === "function" ? s.display(data) : null;
    const found = made != null ? made : getPath(data, s.key);
    /* #7578: `nullAs` is the value a missing figure means when the read answered at all (Runnable %: no other request was running, so the share is 0). */
    const raw = found == null && s.nullAs != null ? s.nullAs : found;
    const why = raw == null && s.nullKey ? getPath(data, s.nullKey) : null;
    /* `small` shrinks a long text value (a state name, a database name). A dash is not text, and at 1rem it drew
       smaller than the dashes of the tiles beside it (#7409), so a missing value keeps the full tile size. */
    const hasValue = raw != null || (why != null && why !== "");
    const valueClass = "value" + (s.small && hasValue ? " small" : "") + (sev ? " " + sevClass(sev) : "");
    /* `appendKey` adds a second field of the payload after the value, when it has one: the product level "RTM" and its update level "CU20" read "RTM CU20" (#6159). */
    const extra = raw != null && s.appendKey ? getPath(data, s.appendKey) : null;
    const shownValue = applyFormat(made != null ? "text" : s.format, raw) + (extra != null && extra !== "" ? " " + String(extra) : "");
    const valueText = why != null && why !== "" ? String(why) : shownValue;
    /* #7552: a wait name (SOS_SCHEDULER_YIELD) may break after an underscore, so a narrow tile wraps between words and not at its last letter. */
    const valueKids = s.breakAfterUnderscore ? valueText.split(/(?<=_)/).flatMap((part, i) => (i ? [el("wbr"), part] : [part])) : null;
    return el("div", { class: "stat", title: s.hint || null }, [
      valueKids ? el("div", { class: valueClass }, valueKids) : el("div", { class: valueClass, text: valueText }),
      el("div", { class: "label", text: s.label }),
    ]);
  };
  /* #7416: a `sentence` stat is prose, not a figure. Drawn as a bold tile it filled a tile track and pushed its neighbour wide;
     it is a line of text across the panel instead, and the tiles that remain keep their natural width. */
  const sentences = shown.filter((s) => s.sentence && getPath(data, s.key) != null && getPath(data, s.key) !== "");
  if (!sentences.length) {
    const tiles = el("div", { class: "stats" }, shown.map(tile));
    balanceTiles(tiles, shown.length);
    return cutNotes.length ? el("div", {}, [tiles, ...cutNotes]) : tiles;
  }
  const rest = shown.filter((s) => !s.sentence);
  return el("div", { class: "stat-block" }, [
    ...sentences.map((s) => el("p", { class: "stat-sentence", text: s.localizeText ? localizeIsoText(String(getPath(data, s.key)), s.localizeMinutes ? { minutes: true, since: s.sinceKey ? getPath(data, s.sinceKey) : null } : undefined) : String(getPath(data, s.key)) })),
    rest.length ? el("div", { class: "stats natural" }, rest.map(tile)) : null,
    ...cutNotes,
  ]);
}

/* line: desc = { rowsKey, xKey, series:[{key,label,color?}], format?, emptyText? }. `format: "int"` declares the
   series are COUNTS, so the chart also puts its gridlines on whole numbers (see integerTicks in charts.js). */
/* #7571: a read that answers one row per (time, group) -- a wait type, a database -- is drawn one series per group, not one line
   through every row. `groupKey` names the row field that holds the group and the descriptor's one `series` entry names the number.
   Rows at one time fold into one point, a group repeated at one time adds up, and past MAX_GROUP_SERIES groups the smallest by
   total fold into "(other)" so the stack's height at each time is still the whole figure the grid beside it adds up to. */
const MAX_GROUP_SERIES = 8;
const OTHER_GROUP = "(other)";
const GROUP_KEY_PREFIX = "g:";
export function groupedPoints(rows, xKey, groupKey, valueField) {
  const totals = new Map();
  for (const r of rows) {
    if (!r || r[xKey] == null) continue;
    const g = r[groupKey] == null || r[groupKey] === "" ? "(none)" : String(r[groupKey]);
    totals.set(g, (totals.get(g) || 0) + (Number(r[valueField]) || 0));
  }
  const ranked = [...totals.keys()].sort((a, b) => totals.get(b) - totals.get(a) || (a < b ? -1 : a > b ? 1 : 0));
  const kept = ranked.length > MAX_GROUP_SERIES ? ranked.slice(0, MAX_GROUP_SERIES - 1) : ranked;
  const folded = ranked.length > kept.length;
  const byTime = new Map();
  for (const r of rows) {
    if (!r || r[xKey] == null) continue;
    const g0 = r[groupKey] == null || r[groupKey] === "" ? "(none)" : String(r[groupKey]);
    const g = kept.includes(g0) ? g0 : OTHER_GROUP;
    const v = Number(r[valueField]);
    if (!Number.isFinite(v)) continue;
    /* A group is a database or wait type NAME, so it is data: the point holds no prototype (a database named __proto__ is an ordinary
       key) and each group's key carries a prefix (a database named like xKey, collection_time, does not overwrite the point's time). */
    if (!byTime.has(r[xKey])) {
      const first = Object.create(null);
      first[xKey] = r[xKey];
      byTime.set(r[xKey], first);
    }
    const pt = byTime.get(r[xKey]);
    pt[GROUP_KEY_PREFIX + g] = (pt[GROUP_KEY_PREFIX + g] || 0) + v;
  }
  const names = folded ? [...kept, OTHER_GROUP] : kept;
  return {
    points: [...byTime.values()].sort((a, b) => String(a[xKey]).localeCompare(String(b[xKey]))),
    series: names.map((n, i) => ({ key: GROUP_KEY_PREFIX + n, label: n, color: SERIES_COLORS[i % SERIES_COLORS.length] })),
  };
}

function vizLine(data, desc) {
  const baseSeriesCfg = Array.isArray(desc.series) ? desc.series : [];
  if (!baseSeriesCfg.length) return emptyStrip(NO_FIELDS_MSG);
  const rawPoints = getPath(data, desc.rowsKey) || [];
  const grouped = desc.groupKey && Array.isArray(rawPoints) ? groupedPoints(rawPoints, desc.xKey, desc.groupKey, baseSeriesCfg[0].key) : null;
  const points = grouped ? grouped.points : rawPoints;
  const allSeriesCfg = grouped ? grouped.series : baseSeriesCfg;
  /* #7394: `hideEmptySeries` drops a series with no measured value at any point, as the desktop's PostgreSQL trend charts do: a
     latency that is null on every point (avg read ms with track_io_timing off, the default) is a fact about the configuration, and
     a legend entry with no line would read as a flat zero. A chart left with no series says `noSeriesText` (or its emptyText). */
  const measured = (s) => points.some((p) => p && p[s.key] != null && p[s.key] !== "" && !Number.isNaN(Number(p[s.key])));
  const seriesCfg = desc.hideEmptySeries && points.length ? allSeriesCfg.filter(measured) : allSeriesCfg;
  /* `noSeriesText` may be a function of the payload, for a sentence whose cause the payload itself can name. */
  const noSeries = typeof desc.noSeriesText === "function" ? desc.noSeriesText(data) : desc.noSeriesText;
  if (!seriesCfg.length) return emptyStrip(noSeries || desc.emptyText || NO_FIELDS_MSG);
  /* ZERO points is a different statement from ONE point, and only the descriptor knows which sentence is true.
     A read whose empty array means the thing simply did not happen must say so, not inherit a warming-up
     message about a condition it never had: get_blocking_trend and get_deadlock_trend used to return
     `trend: []` with no {status,message} envelope on an idle server, so a healthy server got exactly that
     wrong message. Those two now answer with an envelope (#2485) and are classified as "empty" before they
     reach a viz at all; this guard still stands for every OTHER line read, which has no envelope of its own.
     A descriptor's emptyText wins at exactly zero. The one-point case falls through to renderLineChart, which
     now draws that lone bucket as a marker (a single reading IS data) rather than the old "not enough data
     points" strip — so a series that reached one bucket reads consistently beside siblings that reached two. */
  if (!points.length && desc.emptyText) return emptyStrip(desc.emptyText);
  /* #7402: points came back and none holds a number for any series (an idle server just after a restart: its one point has no rate yet).
     Say what happened, in the read's own words when it has a message, instead of a chart with nothing on it. */
  const drawable = (v) => v != null && v !== "" && Number.isFinite(Number(v));
  if (points.length && !points.some((p) => seriesCfg.some((s) => drawable(p && p[s.key])))) return emptyStrip(noValuesText(data));
  /* #7549: `allZeroText` is the one line drawn in place of a chart whose every measured value, in every series, is 0: a healthy server's counters are flat
     lines on a 0 to 1 axis, a full card each, that say less than a sentence. One value above 0, in any series, keeps the chart. */
  if (desc.allZeroText && !points.some((p) => seriesCfg.some((s) => drawable(p && p[s.key]) && Number(p[s.key]) > 0))) {
    /* The line keeps the stats-reset fact the chart's marker would have carried: a window that crossed a reset differenced against a fresh baseline. */
    const crossedReset = desc.resetKey && points.some((p) => p && p[desc.resetKey] === true);
    return emptyStrip(crossedReset ? desc.allZeroText + " (statistics were reset in this window)" : desc.allZeroText);
  }
  const series = seriesCfg.map((s, i) => ({
    key: s.key,
    label: s.label,
    color: s.color || SERIES_COLORS[i % SERIES_COLORS.length],
  }));
  /* The second argument is the axis tick step the chart passes (#6158), so a small range prints enough decimals that no two ticks read alike. */
  const formatValue = desc.format ? (v, o) => applyFormat(desc.format, v, o) : (v, o) => applyFormat("int", v, o); /* #7679: with the thousands separator (60,000, not 60000), as every other whole-number axis prints it */
  /* Percentage charts cap the y-domain at 100 so a 96% reading never rounds the axis up past 100% (B3). */
  const clampMax = desc.clampMax ?? (desc.format === "pct" ? 100 : null);
  /* #2802: span the x-axis over the REQUESTED window ("last N hours" ending now), not the data's own extent, so a
     sparse trend (blocking/deadlocks) plots at its true position instead of the axis zooming to its burst. The
     width is the panel's own `hours` param — `windowHours` when a fanout injects it (a fanout spec carries no
     params) or when loadPanelBody narrowed the read to the history it keeps, else desc.params.hours. Absent ⇒
     null ⇒ the chart keeps its data-extent domain, unchanged. */
  const win = windowFromHours(desc.windowHours != null ? desc.windowHours : desc.params && desc.params.hours);
  /* Drag-to-zoom over the loaded points (zoomableLineChart, charts.js). The zoom is held under this chart's
     identity (title, read and series) and the page's scope (server + tab + preset range), so the poll's rebuild
     draws it again and a different server, tab or range does not. */
  const zoomId = [desc.title || "", desc.read || desc.path || "", desc.xKey, seriesCfg.map((s) => s.key).join(",")].join("|");
  /* #7394: `resetKey` names a boolean on each point (counter_reset) that marks an interval across a statistics reset. Each one is drawn as
     a vertical marker with a key entry ("Stats reset 2") under the chart, because the step a reset leaves in a differenced series is the
     instrument re-baselining, not the workload, and the old table's Stats Reset column was the only place that said one happened. */
  const resetTimes = desc.resetKey ? points.filter((p) => p && p[desc.resetKey] === true).map((p) => p[desc.xKey]) : [];
  const annotations = resetTimes.length ? [{ source: "stats_reset", displayName: desc.resetLabel || "Stats reset", events: resetTimes.map((ts) => ({ ts, label: "" })) }] : null;
  /* #7508: `seamKey` names the field of the reply holding the instant where the series changes grain (routing.raw_from on the Query
     Store Duration Trend), so the chart's gap rule judges each grain against its own spacing. Absent or unparseable: no seam. */
  const seam = desc.seamKey ? parseUtc(getPath(data, desc.seamKey)) : null;
  /* #7692: `bucketKey` names the field of the reply stating its bucket (bucket_minutes). A read that returns rows only where something ran (the Query
     Duration Trend on a quiet server) has uneven steps, so the chart's own cadence guess reads it as events and never breaks it, and a four hour hole
     drew one diagonal. The step is the bucket, or the rows' own cadence when a collector runs slower (bucketMsOf), judged from EVERY row's time, a row
     with no figure included. A panel with a seam (Query Store) names no bucketKey: its two grains are judged by the seam. */
  const bucketMs = desc.bucketKey
    ? bucketMsOf(getPath(data, desc.bucketKey), (Array.isArray(rawPoints) ? rawPoints : []).map((p) => { const d = p ? parseUtc(p[desc.xKey]) : null; return d ? d.getTime() : NaN; }))
    : null;
  /* #7682: `footNoteKey` names a text field of the reply drawn as a notice under the chart, only when a series was left out for having no measured value
     (get_cpu_utilization's host_cpu_note: this server cannot report host CPU, so Other and Total are not drawn). */
  const footNote = desc.footNoteKey && seriesCfg.length < allSeriesCfg.length ? getPath(data, desc.footNoteKey) : null;
  /* `zeroFill` draws the buckets an event-count read left out at 0 (zeroFilledPoints): the read records every event, so a bucket with no row is a
     count of 0. Not when the answer's source (`sourceKey`) is the DMV snapshot, a sample taken on a schedule: a bucket it has no row for is time
     nobody observed, so those points reach the chart as they came. */
  const fill = desc.zeroFill && !(desc.sourceKey && getPath(data, desc.sourceKey) === SAMPLED_BLOCKING_SOURCE);
  /* #7850: the reply's `covered` ranges are the time the collector's successful runs read; the fill is 0 only inside them. A reply without the field (an
     older service) fills the whole window, as #7839 did. */
  const covered = fill ? coveredRanges(getPath(data, "covered")) : null;
  /* #7850: with a `covered` list every covered bucket has a point, so two neighbouring points more than one bucket apart have a stretch nobody
     observed between them, and the line breaks there (a lone point is drawn as a dot). The chart's own spacing guess cannot see it: three events
     hours apart are uneven steps, read as events, and were joined into one slope. Without `covered` the chart keeps its own rule, as before. */
  const fillBucketMs = covered ? zeroFillStepMs(points, desc.xKey, getPath(data, "bucket_minutes")) : null;
  const chart = zoomableLineChart({
    points: fill ? zeroFilledPoints(points, desc.xKey, seriesCfg, getPath(data, "bucket_minutes"), win, covered) : points,
    xKey: desc.xKey,
    series,
    annotations,
    gapSeamMs: seam ? seam.getTime() : null,
    bucketMs: fillBucketMs != null ? fillBucketMs : bucketMs,
    formatValue,
    clampMax,
    /* A count chart's ticks are whole numbers: on a small domain a fractional step (0.2, 0.5) prints through the
       whole-number formatter as the same label several times over ("1 1 1 0 0 0" on a blocking-events axis). */
    integerTicks: desc.format === "int",
    /* #7571: "stacked-bar" draws a grouped read as one bar per time, one segment per group. Absent: the chart's default line. */
    mode: desc.mode || undefined,
    /* #7488: a size chart picks its ticks in the unit its labels step up to (fmtMb prints GB from 1,024). #7555: a bytes chart likewise. */
    sizeUnit: desc.format === "mb" ? "mb" : desc.format === "bytes" ? "bytes" : desc.format === "ms" ? "ms" : null,
    /* #7586: a duration axis carries its unit on every tick (fmtMs), so an "ms" caption above it would name a second unit. */
    unit: desc.format === "ms" && desc.unit === "ms" ? null : (desc.unit ?? null),
    title: desc.title || null,
    atTime: desc.atTime || null,
    source: desc.read || desc.path ? { read: desc.read || desc.path, params: desc.params || null } : null,
    windowStart: win ? win.windowStart : null,
    windowEnd: win ? win.windowEnd : null,
  }, zoomId, chartZoomScope(desc.windowHours != null ? desc.windowHours : desc.params && desc.params.hours));
  /* #7699: `seamCaption` turns the seam instant into the sentence that says why the line changes grain there. A caption, not a warning. */
  const seamCaption = seam && typeof desc.seamCaption === "function" ? desc.seamCaption(seam) : null;
  const under = [];
  if (typeof footNote === "string" && footNote) under.push(noticeStrip(footNote));
  if (typeof seamCaption === "string" && seamCaption) under.push(captionStrip(seamCaption));
  return under.length ? el("div", {}, [chart, ...under]) : chart;
}

/**
 * The `covered` field of a trend reply (#7850) as [startMs, endMs] pairs, or null when the reply has none (an older service: the fill keeps #7839's
 * whole-window behaviour). An empty list is a window no run covered, so nothing is filled. A range with an unreadable time is dropped.
 */
export function coveredRanges(raw) {
  if (!Array.isArray(raw)) return null;
  const out = [];
  for (const r of raw) {
    const from = r ? parseUtc(r.start) : null;
    const to = r ? parseUtc(r.end) : null;
    if (from instanceof Date && to instanceof Date && Number.isFinite(from.getTime()) && Number.isFinite(to.getTime()) && to > from) out.push([from.getTime(), to.getTime()]);
  }
  return out;
}

/**
 * The bucket step, in ms, of a zero-filled trend (#7850): the reply's own `bucket_minutes` when it states one, else the smallest step between the
 * points it returned, and never under a minute. zeroFilledPoints lays its grid at this step and the chart breaks the line at a jump longer than it.
 */
export function zeroFillStepMs(points, xKey, bucketMinutes) {
  let stepMs = Number(bucketMinutes) > 0 ? Number(bucketMinutes) * 60000 : Infinity;
  if (!Number.isFinite(stepMs)) {
    const times = [...new Set((Array.isArray(points) ? points : []).map((p) => { const d = p ? parseUtc(p[xKey]) : null; return d instanceof Date ? d.getTime() : NaN; }).filter(Number.isFinite))].sort((a, b) => a - b);
    for (let i = 1; i < times.length; i++) stepMs = Math.min(stepMs, times[i] - times[i - 1]);
    if (!Number.isFinite(stepMs)) stepMs = 60000;
  }
  return Math.max(60000, stepMs);
}

/**
 * The points of an event-count trend with every bucket of the window present (`zeroFill`). get_blocking_trend and get_deadlock_trend
 * return only the buckets that have events, so a bucket they leave out is a count of 0 and not a gap in collection: two reports a
 * minute apart must not read as a line floating between 1 and 4. The bucket is the reply's own (`bucket_minutes`) when it states one,
 * else the smallest step between the points it returned, and never under a minute. The window is the chart's own (windowStart and
 * windowEnd in epoch ms); without one the fill spans the points. The points the read returned are kept as they are.
 *
 * `covered` (#7850) is the reply's `covered` ranges as [startMs, endMs] pairs (coveredRanges), the time the collector's successful runs read. A
 * bucket is filled only when the whole bucket lies inside one range; a bucket outside them is time nobody observed and is left out, as it was before
 * the fill. Null (no `covered` field, an older service) fills the whole window.
 */
export function zeroFilledPoints(points, xKey, seriesCfg, bucketMinutes, win, covered = null) {
  const timeOf = (p) => {
    const d = p ? parseUtc(p[xKey]) : null;
    return d instanceof Date && Number.isFinite(d.getTime()) ? d.getTime() : null;
  };
  const have = new Map();
  for (const p of points) {
    const t = timeOf(p);
    if (t != null && !have.has(t)) have.set(t, p);
  }
  const times = [...have.keys()].sort((a, b) => a - b);
  if (!times.length) return points;
  const stepMs = zeroFillStepMs(points, xKey, bucketMinutes);
  const start = win && Number.isFinite(Number(win.windowStart)) ? Number(win.windowStart) : times[0];
  const end = win && Number.isFinite(Number(win.windowEnd)) ? Number(win.windowEnd) : times[times.length - 1];
  /* The grid is aligned on the first returned point, so every returned point sits on it. At most 20,000 buckets are drawn. */
  const first = times[0] - Math.ceil((times[0] - start) / stepMs) * stepMs;
  const out = [];
  for (let t = first, n = 0; t <= end && n < 20000; t += stepMs, n++) {
    const p = have.get(t);
    if (p) out.push(p);
    else if (covered && !covered.some(([from, to]) => t >= from && t + stepMs <= to)) continue;
    else {
      const zero = { [xKey]: new Date(t).toISOString() };
      for (const s of seriesCfg) zero[s.key] = 0;
      out.push(zero);
    }
  }
  /* A returned point off the grid (a second row for one time, or one past the bucket cap) is kept too. A set, not a scan of `out` per point:
     a week of minute buckets is 10,000 points against as many buckets, redrawn on every poll. */
  const placed = new Set(out);
  for (const p of points) if (!placed.has(p) && timeOf(p) != null) out.push(p);
  return out.sort((a, b) => timeOf(a) - timeOf(b));
}

/* bandlist: desc = { rowsKey, primaryKey, bandKey, bandLabelKey?, reasonKey?, navKey?, emptyText? } */
function vizBandlist(data, desc) {
  const rows = getPath(data, desc.rowsKey) || [];
  if (!rows.length) return emptyStrip(desc.emptyText || "Nothing to show.");
  return el(
    "div",
    { class: "bandlist" },
    rows.map((r) => {
      let band = getPath(r, desc.bandKey);
      let bandLabel = getPath(r, desc.bandLabelKey) || band;
      /* #7729: a row listed only for tiles that no band reads (`advisoryKey`, the fleet's advisory_tiles) is not Healthy: this list's dot and
         pill say Attention, in the Warning colours. The server's band and the strip counts are the service's and stay as they are. */
      const advisory = desc.advisoryKey ? getPath(r, desc.advisoryKey) : null;
      if (band === "Healthy" && Array.isArray(advisory) && advisory.length) {
        band = "Warning";
        bandLabel = "Attention";
      }
      const props = { class: "row " + bandClass(band) };
      if (desc.navKey) {
        const target = getPath(r, desc.navKey);
        if (target) props.onActivate = () => navigateServer(target);
      }
      return el("div", props, [
        el("span", { class: "dot " + bandClass(band) }),
        el("span", { class: "primary", text: getPath(r, desc.primaryKey) }),
        desc.reasonKey ? el("span", { class: "reason", text: getPath(r, desc.reasonKey) }) : null,
        band ? el("span", { class: "badge " + bandClass(band), text: bandLabel }) : null,
      ]);
    })
  );
}

/* ─────────────────────────── shared helpers ─────────────────────────── */

/** Set the hash route to a server's detail page. */
export function navigateServer(serverName, store) {
  location.hash = storeHash("#/server/" + encodeURIComponent(serverName), store);
}

/**
 * Open one panel of a server's tab (#7416): a tile that says a column is nearly full goes to the panel that names it, not to the top of a tab
 * that runs to tens of thousands of pixels. The panel is found by its `data-title` once the tab has drawn it (the tab builds its panels
 * after the server's card arrives), then scrolled to the top of the view. It is looked for for four seconds and the scroll is repeated once
 * after the tab's reads settle, because panels above it change height as they fill in.
 */
export function navigateServerPanel(serverName, store, tab, panelTitle) {
  location.hash = storeHash("#/server/" + encodeURIComponent(serverName) + "/" + encodeURIComponent(tab), store);
  let tries = 0;
  const look = () => {
    const panel = typeof document.querySelector === "function" ? document.querySelector('[data-title="' + panelTitle + '"]') : null;
    if (panel && typeof panel.scrollIntoView === "function") {
      panel.scrollIntoView({ block: "start" });
      setTimeout(() => panel.scrollIntoView({ block: "start" }), 1500);
      return;
    }
    if (++tries < 40) setTimeout(look, 100);
  };
  setTimeout(look, 100);
}

/** Collector rows worst first (Failing, then Warning, then the rest, then Healthy, then not applicable), each group in
    name order: the order a person reading the grid wants, instead of A to Z. A new array; the rows are not changed. */
export function worstFirst(rows) {
  const rank = (row) => {
    if (String(row && row.status).toLowerCase() === "not_collected") return 4;
    switch (statusToSev(row && row.status)) {
      case "Critical": return 0;
      case "Warning": return 1;
      case "Unknown": return 2;
      default: return 3;
    }
  };
  return rows
    .map((row, i) => ({ row, i }))
    .sort((a, b) => rank(a.row) - rank(b.row) || String(a.row && a.row.collector).localeCompare(String(b.row && b.row.collector)) || a.i - b.i)
    .map((x) => x.row);
}

/** Rows newest first by a text date key (an ISO day sorts as text): `newestFirst("summary_date")`. */
export function newestFirst(key) {
  return (rows) =>
    rows
      .map((row, i) => ({ row, i }))
      .sort((a, b) => String(b.row && b.row[key]).localeCompare(String(a.row && a.row[key])) || a.i - b.i)
      .map((x) => x.row);
}

/**
 * Map a collector/status STRING (already computed server-side) to a severity CSS class — this is coloring a
 * pre-computed label, not re-deriving a band from raw metrics (R1: the browser never re-computes thresholds).
 */
export function statusToSev(status) {
  switch (String(status || "").toUpperCase()) {
    case "HEALTHY":
    case "OK":
    case "SUCCESS":
    case "ANALYZED":
    case "ONLINE":
    case "LOW": /* #7699: the Latch Stats Severity band words */
      return "Healthy";
    case "STALE":
    case "WARNING":
    case "REVIEW":
    case "PERMISSIONS":
    case "SKIPPED":
    case "MEDIUM":
      return "Warning";
    case "FAILING":
    case "ERROR":
    case "OFFLINE":
    case "HIGH":
      return "Critical";
    default:
      return "Unknown";
  }
}
