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

import {
  el,
  mount,
  loadingStrip,
  errorStrip,
  readErrorStrip,
  emptyStrip,
  noticeStrip,
  keptWindowStrip,
  windowFloorStrip,
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
} from "./util.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS } from "./charts.js";
import { toTsv, toCsv, isListValue, csvFileName, copyText, downloadCsv } from "./grid-tools.js";

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
 * Build a panel node. It returns immediately with a loading strip and fills itself once the fetch resolves,
 * mapping the API response kinds (data / empty envelope / error / aborted / auth) to the right UI.
 */
export function renderPanel(desc, onSettled) {
  const signal = panelSignal;
  const body = el("div", { class: "panel-body" }, [loadingStrip()]);
  const panel = el("div", { class: "panel card" + (desc.span === 2 ? " span-2" : "") }, [
    el("h3", {}, [desc.title, desc.subtitle ? el("span", { class: "panel-sub", text: " " + desc.subtitle }) : null]),
    /* #5226: an optional control node under the title (the Top Queries / Top Procedures ranking selector), drawn before the body so a
       load that replaces the body never replaces it. Absent on every other panel. */
    desc.control || null,
    body,
  ]);
  loadPanel(desc, body, signal, onSettled);
  return panel;
}

/* `onSettled` (#4222) is an optional completion callback fired exactly once when this panel's load reaches a
   terminal state (data, empty, error, aborted, or auth) — every renderPanel caller today omits it (a no-op), but
   it is what lets a caller with its OWN concurrency budget (the alert-notebook doc's max-3 in-flight cell loader,
   views.js) know when a slot frees, since renderPanel itself returns synchronously with the fetch still in flight. */
async function loadPanel(desc, body, signal, onSettled) {
  try {
    await loadPanelBody(desc, body, signal);
  } finally {
    if (onSettled) onSettled();
  }
}

async function loadPanelBody(desc, body, signal) {
  /* A read that keeps less history than the page's Range is asked again for what it keeps, and the panel says
     so through keptWindowStrip (readWithinKeptHistory in util.js, shared with the server-tab composites). */
  const res = await readWithinKeptHistory(
    (params) => (desc.read ? readTool(desc.read, params, signal) : apiGet(desc.path + buildQuery(params), signal)),
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
    mount(body, readErrorStrip(res.message));
    return;
  }
  const kept = keptWindowStrip(res);
  if (res.kind === "empty") {
    /* #4966: a grid that looked and found nothing still says where its table's data starts when that is after the
       window's start (the server adds the note to that envelope only, never to an unavailable or not_collected one), so a
       new server's empty week does not read as a quiet one. windowFloorStrip is null for a chart and for an envelope
       without the note. */
    mount(body, [kept, windowFloorStrip(res.data, desc), emptyStrip(res.message)]);
    return;
  }

  const render = VIZ[desc.viz];
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
    const note = desc.noteKey ? getPath(res.data, desc.noteKey) : null;
    /* #4925: a panel may carry further server notes (`moreNoteKeys`), each rendered as its own line when non-null. */
    const moreNotes = (desc.moreNoteKeys || []).map((k) => getPath(res.data, k));
    /* #4966: a grid says where its table's data starts when that is after the window's start (windowFloorStrip). */
    const floor = windowFloorStrip(res.data, desc);
    /* A narrowed read draws its chart over the hours it answered for, not the Range it was asked for (#2802).
       A copy, so the caller's descriptor keeps the window it asked for. */
    if (res.keptHours) desc = { ...desc, windowHours: res.keptHours };
    const rendered = render(res.data, desc);

    mount(body, [
      kept,
      floor,
      typeof note === "string" && note.trim() ? noticeStrip(note) : null,
      ...moreNotes.map((n) => (typeof n === "string" && n.trim() ? noticeStrip(n) : null)),
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

/* table: desc = { rowsKey, columns:[{key,label,format,align,wrap,mono,pre,sevKey,statusSev,sortable,sortValue,csv,copy,copyValue}], sortable, sortId, onRow(row, tr), rowClass(row)|string, tools:false }

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
  const rows = desc.rowsKey === "." ? (data ? [data] : []) : getPath(data, desc.rowsKey) || [];
  return gridTable(rows, desc);
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
    const tr = el("tr", { class: typeof rc === "string" && rc ? rc : null }, cols.map((c) => cell(row, c)));
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
  const picker = columnPicker(desc, cols, head, tbody);
  if (desc.tools === false) return picker ? el("div", { class: "grid-box" }, [picker, wrap]) : wrap;
  return el("div", { class: "grid-box" }, picker ? [picker, gridTools(desc, cols, tbody), wrap] : [gridTools(desc, cols, tbody), wrap]);
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
   Escape closes the box and returns focus to its header button; focus leaving the box closes it. */
const gridFilters = new Map(); // table key -> Map(column id -> text); bounded by routes x tables x columns the session filters
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
  const active = () => gridFilters.get(key) || new Map();
  const bar = el("div", { class: "grid-filter-bar", role: "group", "aria-label": "Column filters" });
  const popHolder = el("div", { class: "grid-filter-holder" });
  const chips = el("div", { class: "grid-filter-chips" });
  bar.appendChild(popHolder);
  bar.appendChild(chips);
  const btns = new Map();
  const needle = (t) => String(t ?? "").trim().toLowerCase();
  /* A filter is { op, text }; the value-less operators are active with no text. */
  const isOn = (f) => !!f && (filterNeedsNoText(f.op) || !!needle(f.text));
  /* The sort kind of column i over the rows now in the body: number, time or text. */
  const kindOf = (i) =>
    sortKindOf(
      cols[i],
      Array.from(tbody.children, (tr) => (trRow.has(tr) ? columnValue(trRow.get(tr), cols[i]) : undefined))
    );
  /* The operator in force: a stored one the column's kind does not offer (the rows changed under it) falls back to Contains. */
  const opFor = (f, kind) => (f && FILTER_OPS_BY_KIND[kind].includes(f.op) ? f.op : "contains");

  function applyRows() {
    const tests = [];
    cols.forEach((c, i) => {
      const f = active().get(colId(c));
      if (isOn(f)) {
        const kind = kindOf(i);
        tests.push([i, opFor(f, kind), kind, f.text ?? ""]);
      }
    });
    let shown = 0;
    let total = 0;
    for (const tr of tbody.children) {
      total++;
      const out = tests.some(([i, op, kind, term]) => {
        const td = tr.children[i];
        if (!td) return true;
        return !filterPasses(op, kind, term, cols[i], trRow.has(tr) ? columnValue(trRow.get(tr), cols[i]) : undefined, td.textContent);
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

  function setFilter(c, op, text) {
    const next = new Map(active());
    if (isOn({ op, text })) next.set(colId(c), { op, text });
    else next.delete(colId(c));
    if (next.size) gridFilters.set(key, next);
    else gridFilters.delete(key);
  }

  function renderChips() {
    const r = applyRows();
    chips.textContent = "";
    if (r.tests) {
      for (const i of idx) {
        const c = cols[i];
        const f = active().get(colId(c));
        if (!isOn(f)) continue;
        const op = opFor(f, kindOf(i));
        const chipText = op === "contains" ? String(f.text).trim() : FILTER_OP_LABELS[op] + (filterNeedsNoText(op) ? "" : " " + String(f.text).trim());
        const x = el("button", { type: "button", class: "grid-filter-x", text: "×", title: "Clear the filter on " + c.label, "aria-label": "Clear the filter on " + c.label });
        x.addEventListener("click", () => {
          setFilter(c, "contains", "");
          renderPop();
          renderChips();
        });
        chips.appendChild(el("span", { class: "grid-filter-chip" }, [el("span", { text: c.label + ": " + chipText }), x]));
      }
      const all = el("button", { type: "button", class: "btn grid-filter-clear-all", text: "Clear all filters" });
      all.addEventListener("click", () => {
        gridFilters.delete(key);
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
    renderPop();
    renderChips();
    const i = cols.findIndex((c) => colId(c) === open);
    const b = btns.get(i);
    if (refocus && b && typeof b.focus === "function") b.focus();
  }

  function renderPop() {
    popHolder.textContent = "";
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
    const panel = el("div", { class: "grid-filter-pop", role: "dialog", "aria-label": "Filter " + c.label }, [el("span", { class: "grid-filter-label", text: c.label }), ops.length > 1 ? opSel : null, input, clear, done].filter(Boolean));
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
      setFilter(c, op, "");
      input.value = "";
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
  tbodyFilter.set(tbody, { reapply: renderChips });
  /* The page was redrawn while the box had the focus: give it back once the new box is in the document. */
  if (input && gridFilterTyping.has(key)) {
    setTimeout(() => {
      if (input.isConnected !== false && typeof input.focus === "function") input.focus();
    }, 0);
  }
  return bar;
}

/* An unsortable header cell. The sortable one is built by makeGridSort().headerCell; both are the place a later
   header affordance (a filter, a menu) attaches. */
function headerCell(c) {
  return el("th", { text: c.label, class: isNumericCol(c) ? "num" : null });
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
      th.className = live ? "sortable" + (isNumericCol(c) ? " num" : "") : isNumericCol(c) ? "num" : "";
      th.setAttribute("tabindex", live ? "0" : "-1");
      th.setAttribute("title", live ? "Sort by " + c.label : "");
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
      const th = el("th", { class: "sortable" + (isNumericCol(c) ? " num" : ""), tabindex: "0", "aria-sort": "none", title: "Sort by " + c.label }, [c.label, ind]);
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
    if (!c.hideWhenEmpty) return true;
    return rows.some((row) => {
      const v = getPath(row, c.key);
      return v != null && v !== "";
    });
  });
  return kept.length ? kept : cols;
}

function isNumericCol(c) {
  return c.align === "right" || ["int", "num1", "num2", "rate", "ms", "mb", "pct"].includes(c.format);
}

function cell(row, c) {
  /* A column may supply a custom cell renderer (row) -> Node — used for the alert status/detail columns and the
     query-text expander. It owns its own content; wrap/mono classes still apply if the column asks for them. */
  if (typeof c.render === "function") {
    const rcls = [];
    if (c.wrap) rcls.push("wrap");
    if (c.mono) rcls.push("mono");
    if (c.pre) rcls.push("pre");
    if (typeof c.cellClass === "function") { const k = c.cellClass(row); if (k) rcls.push(k); }
    return el("td", { class: rcls.join(" ") || null }, [c.render(row)]);
  }
  const raw = getPath(row, c.key);
  const cls = [];
  if (isNumericCol(c)) cls.push("num");
  if (c.wrap) cls.push("wrap");
  if (c.mono) cls.push("mono");
  if (c.pre) cls.push("pre");
  if (c.sevKey) cls.push(sevClass(getPath(row, c.sevKey)));
  if (c.statusSev) cls.push(sevClass(statusToSev(raw)));
  if (typeof c.cellClass === "function") { const k = c.cellClass(row); if (k) cls.push(k); }
  /* nullKey names another field of the SAME row that says why this one is empty (get_file_io_stats' size_note:
     "n/a (log service)" for the log file of a Hyperscale database). The server wrote the sentence; the page only
     shows it in place of the bare em dash. */
  const why = raw == null && c.nullKey ? getPath(row, c.nullKey) : null;
  const text =
    why != null && why !== ""
      ? String(why)
      : typeof c.display === "function"
        ? String(c.display(row) ?? "—")
        : c.format
        ? applyFormat(c.format, raw)
        : raw == null || raw === ""
          ? "—"
          : String(raw);
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
  return el(
    "div",
    { class: "stats" },
    stats.map((s) => {
      const sev = s.sev || s.severity;
      const valueClass = "value" + (s.small ? " small" : "") + (sev ? " " + sevClass(sev) : "");
      const raw = getPath(data, s.key);
      const why = raw == null && s.nullKey ? getPath(data, s.nullKey) : null;
      return el("div", { class: "stat" }, [
        el("div", { class: valueClass, text: why != null && why !== "" ? String(why) : applyFormat(s.format, raw) }),
        el("div", { class: "label", text: s.label }),
      ]);
    })
  );
}

/* line: desc = { rowsKey, xKey, series:[{key,label,color?}], format?, emptyText? }. `format: "int"` declares the
   series are COUNTS, so the chart also puts its gridlines on whole numbers (see integerTicks in charts.js). */
function vizLine(data, desc) {
  const seriesCfg = Array.isArray(desc.series) ? desc.series : [];
  if (!seriesCfg.length) return emptyStrip(NO_FIELDS_MSG);
  const points = getPath(data, desc.rowsKey) || [];
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
  const series = seriesCfg.map((s, i) => ({
    key: s.key,
    label: s.label,
    color: s.color || SERIES_COLORS[i % SERIES_COLORS.length],
  }));
  const formatValue = desc.format ? (v) => applyFormat(desc.format, v) : (v) => String(Math.round(v));
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
  return zoomableLineChart({
    points,
    xKey: desc.xKey,
    series,
    formatValue,
    clampMax,
    /* A count chart's ticks are whole numbers: on a small domain a fractional step (0.2, 0.5) prints through the
       whole-number formatter as the same label several times over ("1 1 1 0 0 0" on a blocking-events axis). */
    integerTicks: desc.format === "int",
    unit: desc.unit ?? null,
    title: desc.title || null,
    atTime: desc.atTime || null,
    source: desc.read || desc.path ? { read: desc.read || desc.path, params: desc.params || null } : null,
    windowStart: win ? win.windowStart : null,
    windowEnd: win ? win.windowEnd : null,
  }, zoomId, chartZoomScope(desc.windowHours != null ? desc.windowHours : desc.params && desc.params.hours));
}

/* bandlist: desc = { rowsKey, primaryKey, bandKey, bandLabelKey?, reasonKey?, navKey?, emptyText? } */
function vizBandlist(data, desc) {
  const rows = getPath(data, desc.rowsKey) || [];
  if (!rows.length) return emptyStrip(desc.emptyText || "Nothing to show.");
  return el(
    "div",
    { class: "bandlist" },
    rows.map((r) => {
      const band = getPath(r, desc.bandKey);
      const props = { class: "row " + bandClass(band) };
      if (desc.navKey) {
        const target = getPath(r, desc.navKey);
        if (target) props.onActivate = () => navigateServer(target);
      }
      return el("div", props, [
        el("span", { class: "dot " + bandClass(band) }),
        el("span", { class: "primary", text: getPath(r, desc.primaryKey) }),
        desc.reasonKey ? el("span", { class: "reason", text: getPath(r, desc.reasonKey) }) : null,
        band ? el("span", { class: "badge " + bandClass(band), text: getPath(r, desc.bandLabelKey) || band }) : null,
      ]);
    })
  );
}

/* ─────────────────────────── shared helpers ─────────────────────────── */

/** Set the hash route to a server's detail page. */
export function navigateServer(serverName) {
  location.hash = "#/server/" + encodeURIComponent(serverName);
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
    case "ONLINE":
      return "Healthy";
    case "STALE":
    case "WARNING":
    case "PERMISSIONS":
    case "SKIPPED":
      return "Warning";
    case "FAILING":
    case "ERROR":
    case "OFFLINE":
      return "Critical";
    default:
      return "Unknown";
  }
}
