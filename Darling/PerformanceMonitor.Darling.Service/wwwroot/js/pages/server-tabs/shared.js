/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, readTool, readToolWithinKeptHistory, keptWindowStrip, windowFloorStrip, markPartialSubtitle, mount, clear, tagPill, truncate, loadingStrip, errorStrip, readErrorStrip, emptyStrip, disclosure, noticeStrip, chartNote, getPath, localTime, parseUtc, windowFromHours, dbScopeChip, sourceStrip, setQueryWaitFilter, waitIsLinked } from "../../util.js";
import { renderPanel, emptyHeadline, setPanelSignal, getPanelSignal, VIZ } from "../../panels.js";
import { SNAPSHOT_LIST_CAP, snapshotTruncatedNote } from "../active-snapshots.js";
import { zoomableLineChart, chartZoomScope, SERIES_COLORS, CATEGORICAL_COLORS } from "../../charts.js";
import { historyTarget, procedurePoints } from "../stats-history.js";
import { mergeSeriesRows } from "../../multi-picker.js";
import { findingTitle, severityLabel } from "../analysis-findings.js";
import { timeSlicer, windowOf, rangeKeyOf, getSlice, setSlice, onSliceChange, effectiveContext, windowParams } from "../../time-slicer.js";
import { comparePick, compareControl, compareCard, COMPARE_TOP } from "../top-compare.js";

/* ─────────────────────────── shared cell renderers ─────────────────────────── */

/** Query-text cell: a truncated one-liner that expands to the full statement (mono) — the B2 disclosure. */
export function codeDisclosure(text) {
  if (text == null || text === "") return document.createTextNode("—");
  return disclosure(text, el("pre", { class: "code" }, [text]), { max: 100 });
}

/**
 * An array-valued cell — a blocking chain's databases, a lock cycle's PIDs. The generic path stringifies an
 * array, which is nearly right for a populated one and renders an EMPTY one as a blank cell rather than as the
 * "—" every other absent value on the page uses, so the two absences would not look alike.
 */
export function listCell(values) {
  return document.createTextNode(Array.isArray(values) && values.length ? values.join(", ") : "—");
}

/* `read` is the panel's main read and `dbScope` its own scope when one read feeds panels of different kinds ("server",
   "unfiltered" or "process-rows"): together they draw the database-scope chip while the page's database filter is active
   (#5245, dbScopeChip in util.js). A panel with no read of its own passes none and draws no chip. `control` (#5226) is a node
   drawn under the title, before the body: the ranking selector of the Top Queries card. */
export function panelShell(title, subtitle, span = 2, read = null, dbScope = null, control = null) {
  const body = el("div", { class: "panel-body" }, [loadingStrip()]);
  /* #5701: `data-panel` (the read) and `data-title` are the hooks the web UI smoke check finds a panel by. */
  const panel = el("div", { class: "panel card" + (span === 2 ? " span-2" : ""), "data-panel": read || title, "data-title": title }, [
    el("h3", {}, [title, subtitle ? el("span", { class: "panel-sub", text: " " + subtitle }) : null, dbScopeChip(read, dbScope)]),
    control,
    body,
  ]);
  return { panel, body };
}

/**
 * Several panels over ONE fetch of one read.
 *
 * A descriptor owns its own fetch, which is the right default and is what makes the seam composable — but a
 * read that feeds two or three panels on the SAME tab then runs two or three times, and `readTool`/`apiGet` have
 * no cache to absorb it. That is a real cost rather than a tidiness point: `get_collection_health` rolls up
 * seven days of collector logs and computes sweep pressure, and the Collection Health tab renders three slices
 * of one payload (`sweep_pressure.*`, `collectors`, `sweep_pressure.heaviest_collectors`), so opening it was
 * running that query three times. Same shape for plan corrections (recommendations + automatic tuning), plan
 * cache (summary + per-type), sessions (summary + per-application) and system_health (chart + entries).
 *
 * Each spec is an ordinary panel descriptor minus `read`/`params` — the same viz registry, the same three-kind
 * response mapping renderPanel does — so nothing about the seam changes except how many times the wire is used.
 *
 * The same kept-history rule as the descriptor loader, too (util.js readWithinKeptHistory): a read that keeps
 * less history than the page's Range answers for the hours it keeps, every panel it feeds says so through
 * keptWindowStrip, and a line spec is windowed over those hours rather than the Range.
 */
/* `hideWhenNoRows`: a spec that reports a problem is not drawn at all when its rows are absent, as the desktop viewer
   collapses its section; an unreadable payload hides it too, since the panels beside it on the same read say so.
   The shell is built HIDDEN, so a clean server never shows the card or its loading strip while the read runs, and it is
   revealed only when rows arrive. The 60 s rebuild makes a fresh shell every time, so whether the last answer for this
   server had rows is kept at module scope: a server that has caveats keeps the card up across a rebuild instead of
   hiding and re-showing it on every poll. */
const panelHadRows = new Map();
function panelMemoryKey(read, params, spec) {
  return read + "|" + JSON.stringify(params || {}) + "|" + spec.title;
}
function hidesPanel(spec, res, shell, key) {
  if (!spec.hideWhenNoRows) return false;
  /* A failed read is not "no rows": hiding the card would leave a problem list that could not load looking like a clean server.
     The card stays (or comes back) and shows the read's error (release walk, W12). */
  if (res.kind === "error") {
    shell.panel.style.display = "";
    return false;
  }
  const hasRows = res.kind === "data" && (getPath(res.data, spec.rowsKey) || []).length > 0;
  panelHadRows.set(key, hasRows);
  if (hasRows) {
    shell.panel.style.display = "";
    return false;
  }
  shell.panel.style.display = "none";
  mount(shell.body, []);
  return true;
}

/* `intro` (#6439): an optional `{ text, when }`. An empty full-width slot is put before the panels and gets the notice only once the
   read's data passes `when(data)`, so a sentence about what the cards below draw is shown only when they draw it. */
export function fanout(read, params, specs, intro = null) {
  for (const spec of specs) {
    if ((spec.viz === "table" || spec.viz === "line") && !spec.emptyText) {
      throw new Error("fanout(" + spec.title + "): a data panel must explain its own empty state.");
    }
  }
  const shells = specs.map((s) => panelShell(s.title, s.subtitle, s.span ?? 2, read, s.dbScope));
  const keys = specs.map((s) => panelMemoryKey(read, params, s));
  specs.forEach((s, i) => {
    if (s.hideWhenNoRows && !panelHadRows.get(keys[i])) shells[i].panel.style.display = "none";
  });
  const introNode = intro ? el("div", { class: "panel-intro" }, []) : null;
  /* #6793: the batch's signal, captured while the tab is built (setPanelSignal holds it then), so leaving the page or a redraw cancels this read. */
  const signal = getPanelSignal();
  (async () => {
    const res = await readToolWithinKeptHistory(read, params, signal);
    if (res.kind === "aborted") return;
    if (introNode && res.kind === "data" && intro.when(res.data)) {
      mount(introNode, [noticeStrip(intro.text)]);
    }
    specs.forEach((spec, i) => {
      const body = shells[i].body;
      if (hidesPanel(spec, res, shells[i], keys[i])) return;
      if (res.kind === "error") return mount(body, readErrorStrip(res.message));
      if (res.kind === "empty") return mount(body, [keptWindowStrip(res), windowFloorStrip(res.data, spec), emptyStrip(emptyHeadline(spec, res))]);
      try {
        /* #2802: a fanout spec carries no `params` of its own (the window lives on the shared fetch above), so
           hand vizLine the fetch's `hours` as `windowHours` — otherwise a fanout line panel (Current Waits,
           Blocking/Deadlock Severity, ...) would fall back to its sparse data extent. Inert for the table specs. */
        /* A SERVER-SUPPLIED caveat above the rows, the same hook renderPanel carries (#3278) and for the
           same reason: a capped page whose ordering displaces the rows a reader came for looks like a
           working read, and only the server holds the population figures that say otherwise. Opt-in per
           SPEC rather than per fetch, because a fanout's specs slice one response into panels that are not
           all pages of a population - the aggregate one beside a capped row list is exactly the pairing
           where only one of them needs saying so. */
        const note = spec.noteKey ? getPath(res.data, spec.noteKey) : null;
        /* A fanout line chart names its chart-menu "at This Time" item with spec.atTimeItem, as line() does with
           opts.atTimeItem; Active Queries when it names none. */
        const rendered = VIZ[spec.viz](res.data, { ...spec, read, windowHours: res.keptHours || (params && params.hours), atTime: spec.atTime === false || !params || !params.server ? null : { server: params.server, item: spec.atTimeItem || "queries" } });

        markPartialSubtitle(shells[i].panel, res.data, spec);
        mount(body, [keptWindowStrip(res), windowFloorStrip(res.data, spec), sourceStrip(res.data, spec), typeof note === "string" && note.trim() ? noticeStrip(note) : null, rendered]);
      } catch (e) {
        mount(body, errorStrip("Could not render this panel: " + (e && e.message ? e.message : String(e))));
      }
    });
  })();
  return introNode ? [introNode, ...shells.map((s) => s.panel)] : shells.map((s) => s.panel);
}

/**
 * #3653 A5: the baseline-discontinuity sentences a trend payload's trailing `discontinuities[]` carries, one
 * per entry, ready for a noticeStrip beside the chart — the web twin of the dashed marker the desktop viewers
 * draw. The identity-epoch carriers (#3694, #3705) forget a server's delta baselines when the target restarts,
 * fails over, is renamed or has its statistics reset, so a delta-rate trend across that instant shows a step
 * that is the instrument re-baselining, not the workload; without the sentence a reader takes the step for a
 * change in load. The wording is the shared `baseline discontinuity at {time} ({reason})` both desktop viewers
 * use (BaselineDiscontinuities.Sentence), with the payload's detail after a dash so the browser reader sees
 * what moved without opening the JSON; `at` is the store's naive-UTC instant and localTime renders it on the
 * reader's clock like every other timestamp on this page. Empty array in, empty array out — a fully
 * continuous window adds no strip. Every value reaches the DOM through noticeStrip's text path (R4).
 */
export function discontinuityNotes(data) {
  const list = data && Array.isArray(data.discontinuities) ? data.discontinuities : [];
  return list.map(
    (d) => "baseline discontinuity at " + localTime(d.at) + " (" + d.reason + ")" + (d.detail ? " — " + d.detail : "")
  );
}

/**
 * A labelled <select>, calling back with the chosen value. An option is a plain string when its value IS its
 * label (a wait type, a counter name) and a {value, label} pair when it is not — the query picker's value is a
 * row index, because a query has no short name to key on and its text is far too long to be one. Both the value
 * and the label are set through el()'s text/attribute paths, so neither a counter name nor a statement captured
 * from a monitored server can become markup (R4). `selected` (#5226), when it names an option, is the one shown first: a choice kept
 * across a rebuild.
 */
export function pickerControl(label, options, onPick, selected) {
  const items = options.map((o) => (typeof o === "string" ? { value: o, label: o } : o));
  const sel = el(
    "select",
    { class: "range-select-inline", "aria-label": label },
    items.map((o) => el("option", { value: o.value, text: o.label }))
  );
  sel.value = selected !== undefined && items.some((o) => o.value === selected) ? selected : items[0].value;
  sel.addEventListener("change", () => onPick(sel.value));
  return el("label", { class: "range-control" }, [el("span", { text: label }), sel]);
}

/* ─────────── #5226: the ranking choice on the Top Queries and Top Procedures cards ─────────── */

/* The rankings the server's whitelist accepts (Mcp/TopRanking.cs), in the order the selector lists them. `cpu` is the default and is
   sent as nothing, so the request for the default page is exactly what it was before the choice existed. */
const TOP_RANKINGS = [
  { value: "cpu", label: "CPU" },
  { value: "duration", label: "Duration" },
  { value: "reads", label: "Reads" },
  { value: "executions", label: "Executions" },
];

/* The pick, one per list, at MODULE scope so the 60 s rebuild of a tab (a new card every time) keeps it. The CPU tab and the Queries tab draw the
   same lists, so a pick on one tab is the pick on the other. */
const topRankingPick = { queries: "cpu", procedures: "cpu" };

/* A reads ranking reads raw, or (#5329) the io hourly rollup (query_stats_io_hourly, procedure_stats_io_hourly) once that reaches the window's
   start or reaches further back than raw, and which of them answered is the tool's business, not the page's: the stitched hourly rollups keep no logical reads, but the io
   ones do. So the card holds no sentence of its own about the source (the one it had named raw as the only source, which is
   false on the io route). What a reader needs from the answer is whether it covers the window, and that rides on the window note: over a
   window past what raw keeps, a raw-routed read says so in `retention_notice`, the retention notice the composed panels carry, and a
   partial io rollup says so in `truncation_note`. The notice is drawn above the grid as text, once: readTool puts it in the window note's
   place (util.js localizeWindowNote, #5299), so the cards below name `truncation_note` as their one note and need no key of their own for
   it, and a ranking is never silently partial. The tool's `precision_note` names the relation that answered for a caller of the tool. */

export function topRankingLabel(ranking) {
  return (TOP_RANKINGS.find((r) => r.value === ranking) || TOP_RANKINGS[0]).label;
}

/** A read's params for a ranking: `order_by` only when the pick is not the default (cpu), so the default request is unchanged. */
export function rankedParams(params, ranking, kind) {
  const ranked = ranking === "cpu" ? params : { ...params, order_by: ranking };
  return withTagPick(ranked, kind);
}

/* ─────────── #5987: the query tag column and filter on the query grids ─────────── */

/* The tag each grid is filtered to ("" = no filter, "untagged" = rows with no tag), one per grid at MODULE scope so the 60 s rebuild keeps it, as the
   ranking pick does. The pick is SENT to the read (the server applies it before the top-N cut); the page never filters a list on the client. */
const tagPicks = { queries: "", procedures: "", querystore: "", longqueries: "", active: "" };
const UNTAGGED_TAG = "untagged";

/* The query tags the filter offers, with their colours, from list_query_tags: read once and kept. A failed read leaves "All tags" and "untagged". */
let queryTagList = null;
let queryTagRead = null;
function loadQueryTags() {
  if (!queryTagRead) {
    queryTagRead = readTool("list_query_tags", {}).then((res) => {
      const tags = res && res.kind === "data" && res.data && Array.isArray(res.data.tags) ? res.data.tags : [];
      queryTagList = tags.filter((t) => t && typeof t.path === "string").map((t) => ({ path: t.path, colour: t.colour || null }));
      return queryTagList;
    });
    queryTagRead.catch(() => {
      queryTagRead = null;
    });
  }
  return queryTagRead;
}

/** A read's params with the grid's tag pick added; no key at all for no filter, so an unfiltered request is exactly what it was before the filter existed. */
export function withTagPick(params, kind) {
  const pick = tagPicks[kind];
  return pick ? { ...params, tag: pick } : params;
}

/**
 * The tag filter beside a grid's title: All tags, untagged, then every query tag by path. A pick stores the choice and calls `onPick`, which draws
 * the grid again with the new `tag` parameter. The option text reaches the DOM through el() as text.
 */
export function tagFilterPicker(kind, onPick) {
  const options = () => {
    const paths = (queryTagList || []).map((t) => t.path);
    const pick = tagPicks[kind];
    if (pick && pick !== UNTAGGED_TAG && !paths.includes(pick)) paths.push(pick);
    return [{ value: "", label: "All tags" }, { value: UNTAGGED_TAG, label: UNTAGGED_TAG }, ...paths.map((p) => ({ value: p, label: p }))];
  };
  const control = pickerControl(
    "Tag",
    options(),
    (value) => {
      tagPicks[kind] = value;
      onPick();
    },
    tagPicks[kind]
  );
  if (!queryTagList) {
    loadQueryTags()
      .then(() => {
        const sel = control.children[1];
        if (!sel || !control.isConnected) return;
        clear(sel);
        for (const o of options()) sel.appendChild(el("option", { value: o.value, text: o.label }));
        sel.value = tagPicks[kind];
      })
      .catch(() => {});
  }
  return control;
}

/** The tag column: a coloured pill with the row's tag path, nothing for a row with no tag, and the column dropped when no row has one. */
export function tagColumn() {
  return {
    key: "tag",
    label: "Tag",
    hideWhenEmpty: true,
    render: (r) => {
      if (!r || typeof r.tag !== "string" || r.tag === "") return el("span", {});
      const known = (queryTagList || []).find((t) => t.path === r.tag);
      return tagPill(r.tag, known ? known.colour : null);
    },
  };
}

/* #6300: the Compare picker's delta card for a grid (top-compare.js). `cmp` is { server, ctx }; the read is the grid's own, asked for
   the top COMPARE_TOP rows, once for the page's window and once with `as_of` shifted. `plain()` draws the grid as it is with Compare off. */
export function compareDrawn(kind, cmp, ranking, title, picker, signal, plain) {
  const base = windowParams({ server: cmp.server, hours: cmp.ctx.hours, top: COMPARE_TOP }, cmp.ctx);
  return compareCard({
    kind,
    server: cmp.server,
    endMs: cmp.ctx.narrowed ? cmp.ctx.endMs : undefined,
    pick: comparePick.value,
    title,
    subtitle: cmp.ctx.label,
    picker,
    signal,
    shell: panelShell,
    viz: VIZ,
    params: kind === "querystore" ? withTagPick(base, "querystore") : rankedParams(base, ranking, kind),
    /* The plain grid is drawn after the two reads, so it takes this draw's signal again: a pick that redraws still aborts its read. */
    plain: () => {
      const outer = getPanelSignal();
      setPanelSignal(signal);
      try {
        return plain();
      } finally {
        setPanelSignal(outer);
      }
    },
  });
}

/**
 * A Top Queries or Top Procedures card (`kind` "queries" or "procedures") with its ranking selector, under a time slicer (#6301).
 * `build(ranking, picker, w, onRow)` draws the card for the current pick and puts `picker` under its title; `w` is the window the grid reads
 * (the page range, or the slicer's selection while one is held) and `onRow` the row pick that overlays that row's timeline on the slicer
 * (queries, procedures and Query Store). A pick stores the choice at module
 * scope and draws the card again, so the title ("Top Queries by Duration") and the rows follow it. Each draw aborts the one before it, as the
 * Collection Log slot does. A slicer release stores the narrowed window at page scope (time-slicer.js) and every card on screen for the
 * server draws its grid again for it; the strip itself is not redrawn, so a sort pick or a slice never reads the strip's activity again.
 */
export function rankedCard(kind, build, cmp) {
  const slot = el("div", {}, []);
  slot.style.display = "contents";
  const gridSlot = el("div", {}, []);
  gridSlot.style.display = "contents";
  const slicer = rankedSlicer(kind, cmp);
  let drawing = null;
  const draw = () => {
    if (drawing) drawing.abort();
    const mine = new AbortController();
    drawing = mine;
    const outer = getPanelSignal();
    if (outer) outer.addEventListener("abort", () => mine.abort(), { once: true });
    const ranking = topRankingPick[kind];
    const w = effectiveContext(cmp.server, cmp.ctx);
    const picker = el("div", { class: "picker-row" }, [
      pickerControl(
        "Rank by",
        TOP_RANKINGS,
        (value) => {
          topRankingPick[kind] = value;
          draw();
        },
        ranking
      ),
      tagFilterPicker(kind, draw),
      compareControl(draw, slot),
    ]);
    setPanelSignal(mine.signal);
    const onRow = slicer ? slicer.onRow(ranking) : null;
    const card =
      comparePick.value !== "off"
        ? compareDrawn(kind, { server: cmp.server, ctx: w }, ranking, (kind === "queries" ? "Top Queries by " : "Top Procedures by ") + topRankingLabel(ranking), picker, mine.signal, () => build(ranking, picker, w, onRow))
        : build(ranking, picker, w, onRow);
    setPanelSignal(outer);
    mount(gridSlot, [card]);
  };
  mountSliced(slot, gridSlot, slicer, cmp, draw);
  draw();
  return slot;
}

/* The slicer panel over a card's grid slot, and the card drawing again when ANY card narrows the page for this server (#6301). */
function mountSliced(slot, gridSlot, slicer, cmp, draw, follow = false) {
  /* `follow`: a grid with no strip of its own (a slicer for another grid narrows it too) still draws again when the page narrows. */
  if ((slicer && slicer.node) || (follow && cmp)) {
    const off = onSliceChange((server) => {
      if (slot.isConnected === false) return off();
      if (server !== cmp.server) return;
      if (slicer) slicer.follow();
      draw();
    });
    /* The page redraws (the 60 s poll, a range or tab change) abort this signal, and a card that dies with it must not stay subscribed: a rebuilt
       grid used to add its listener every time and keep the dead card tree alive until some later slice found it detached (#6301 review). */
    const outer = getPanelSignal();
    if (outer) {
      if (outer.aborted) off();
      else outer.addEventListener("abort", off, { once: true });
    }
  }
  mount(slot, slicer && slicer.node ? [el("div", { class: "panel card span-2 slicer-panel" }, [slicer.node]), gridSlot] : [gridSlot]);
}

/* The slot every sliced grid shares: the strip (when `strip`) over a grid slot that draws again, aborting the draw before it, whenever the page narrows. */
export function slicedSlot(kind, cmp, strip, drawCard) {
  const slot = el("div", {}, []);
  slot.style.display = "contents";
  const gridSlot = el("div", {}, []);
  gridSlot.style.display = "contents";
  const slicer = strip ? rankedSlicer(kind, cmp) : null;
  let drawing = null;
  const draw = () => {
    if (drawing) drawing.abort();
    const mine = new AbortController();
    drawing = mine;
    const outer = getPanelSignal();
    if (outer) outer.addEventListener("abort", () => mine.abort(), { once: true });
    const w = effectiveContext(cmp.server, cmp.ctx);
    setPanelSignal(mine.signal);
    const card = drawCard(w, slicer);
    setPanelSignal(outer);
    mount(gridSlot, [card]);
  };
  mountSliced(slot, gridSlot, slicer, cmp, draw, true);
  draw();
  return slot;
}

/* ─────────── #6301: the time slicer over the Top Queries and Top Procedures cards ─────────── */

/* The slicer's pick of a query row, one per server and list at MODULE scope so the 60 s rebuild (a new card every time) keeps the overlay on the same row. */
const overlayPicks = new Map();

/* The get_query_trend column the overlay plots for a ranking: the metric the grid is ranked by where the trend carries it. The trend has no
   reads column, so a Reads ranking plots the average CPU, and the note on the strip names the column. */
const OVERLAY_METRICS = {
  cpu: { key: "avg_cpu_ms", label: "avg CPU ms" },
  duration: { key: "avg_elapsed_ms", label: "avg elapsed ms" },
  executions: { key: "execution_count", label: "executions" },
  reads: { key: "avg_cpu_ms", label: "avg CPU ms" },
};
/* The same, for get_procedure_duration_trend (#6301 slice 2b), through procedurePoints (stats-history.js): average elapsed is derived there from the two rates. */
const PROCEDURE_OVERLAY_METRICS = {
  cpu: { key: "avg_cpu_ms", label: "avg CPU ms" },
  duration: { key: "avg_elapsed_ms", label: "avg elapsed ms" },
  executions: { key: "executions_per_second", label: "executions/s" },
  reads: { key: "avg_cpu_ms", label: "avg CPU ms" },
};

/**
 * What each slicer charts on its strip, and what a row pick overlays on it (#6301). The strip reads once over the PAGE range, whatever the
 * grid below holds:
 *   queries     get_query_duration_trend; a row overlays get_query_trend for that query.
 *   procedures  get_procedure_duration_trend; a row overlays the same read limited to that procedure (the History button's read).
 *   blocking    get_blocking_trend, reports per bucket; no row overlay.
 *   deadlocks   get_deadlock_trend, deadlocks per bucket; no row overlay.
 *   active      the snapshot list of get_active_queries (snapshots_only), sessions per snapshot; no row overlay.
 *   querystore  get_query_store_duration_trend; a row overlays get_query_store_query_history for that query's plan, the read the grid's
 *               own History button opens.
 */
function stripOf(kind, server, ctx) {
  if (kind === "active") {
    return { title: "Active queries", tool: "get_active_queries", params: { server, hours: ctx.hours, limit: SNAPSHOT_LIST_CAP, snapshots_only: true }, rows: (d) => d.snapshots, time: "collection_time", value: "row_count", note: (d) => snapshotTruncatedNote(d.truncated === true, Array.isArray(d.snapshots) ? d.snapshots.length : 0, d.total_snapshot_times) };
  }
  if (kind === "blocking" || kind === "deadlocks") {
    const blocking = kind === "blocking";
    return { title: blocking ? "Blocked process reports" : "Deadlocks", tool: blocking ? "get_blocking_trend" : "get_deadlock_trend", params: { server, hours: ctx.hours }, rows: (d) => d.trend, time: "time", value: "count" };
  }
  if (kind === "querystore") {
    return { title: "Query Store duration", tool: "get_query_store_duration_trend", params: { server, hours: ctx.hours }, rows: (d) => d.trend, time: "time", value: "value" };
  }
  const queries = kind === "queries";
  return { title: queries ? "Query duration" : "Procedure duration", tool: queries ? "get_query_duration_trend" : "get_procedure_duration_trend", params: { server, hours: ctx.hours }, rows: (d) => d.trend, time: "time", value: "value" };
}

const timeOf = (v) => (parseUtc(v) || { getTime: () => NaN }).getTime();

/* The row overlay of a kind: { eligible(row), key(row), read(row, ranking, signal) -> { points, label } | null }, or null when the kind has none. */
function overlayOf(kind, server, ctx) {
  if (kind === "queries") {
    return {
      /* get_query_trend keys on both values, so a row carrying neither is not offered, as in the per-query picker. */
      eligible: (row) => !!(row && row.query_hash && row.database_name),
      key: (row) => row.database_name + "|" + row.query_hash,
      read: async (row, ranking, signal) => {
        const metric = OVERLAY_METRICS[ranking] || OVERLAY_METRICS.cpu;
        const trend = await readToolWithinKeptHistory("get_query_trend", { server, query_hash: row.query_hash, database_name: row.database_name, hours: ctx.hours }, signal);
        if (trend.kind !== "data" || !trend.data) return null;
        const points = (Array.isArray(trend.data.trend) ? trend.data.trend : []).map((r) => ({ t: timeOf(r.collection_time), v: Number(r[metric.key]) }));
        return { points, label: row.database_name + " · " + truncate(row.query_text || "(no statement text captured)", 40) + " (" + metric.label + ")" };
      },
    };
  }
  if (kind === "procedures") {
    return {
      /* get_procedure_duration_trend limited to one procedure keys on the database and the grid's own full_name, as the History button does. */
      eligible: (row) => !!historyTarget("procedures", row),
      key: (row) => row.database_name + "|" + row.full_name,
      read: async (row, ranking, signal) => {
        const metric = PROCEDURE_OVERLAY_METRICS[ranking] || PROCEDURE_OVERLAY_METRICS.cpu;
        const trend = await readToolWithinKeptHistory("get_procedure_duration_trend", { server, hours: ctx.hours, ...historyTarget("procedures", row) }, signal);
        if (trend.kind !== "data" || !trend.data) return null;
        const points = procedurePoints(trend.data.trend).map((p) => ({ t: timeOf(p.time), v: p[metric.key] == null ? NaN : Number(p[metric.key]) }));
        return { points, label: row.database_name + " · " + truncate(row.full_name, 40) + " (" + metric.label + ")" };
      },
    };
  }
  if (kind === "querystore") {
    return {
      /* The history read keys on the database and the query id; the grid's row is one plan of that query. */
      eligible: (row) => !!(row && row.database_name != null && row.query_id != null),
      key: (row) => row.database_name + "|" + row.query_id + "|" + row.plan_id,
      read: async (row, ranking, signal) => {
        const res = await readToolWithinKeptHistory("get_query_store_query_history", { server, database_name: row.database_name, query_id: row.query_id, hours: ctx.hours }, signal);
        if (res.kind !== "data" || !res.data) return null;
        const all = Array.isArray(res.data.points) ? res.data.points : [];
        const mine = row.plan_id == null ? all : all.filter((p) => p.plan_id === row.plan_id);
        const points = mine.map((p) => ({ t: timeOf(p.collection_time), v: Number(p.avg_duration_ms) }));
        return { points, label: row.database_name + " · query " + row.query_id + (row.plan_id == null ? "" : " plan " + row.plan_id) + " (avg duration ms)" };
      },
    };
  }
  return null;
}

/**
 * The slicer for one card (`kind`: queries, procedures, active or querystore): the strip charts the kind's activity over the page range
 * (stripOf), a release narrows the page's window, and selecting a row of a kind with a row overlay (overlayOf) draws that row's own timeline over
 * the strip.
 */
function rankedSlicer(kind, cmp) {
  const { server, ctx } = cmp;
  const win = windowOf(ctx);
  const rangeKey = rangeKeyOf(ctx);
  const strip = stripOf(kind, server, ctx);
  const overlay = overlayOf(kind, server, ctx);
  const slicer = timeSlicer({
    win,
    selection: getSlice(server, rangeKey),
    title: strip.title,
    onCommit: (sel) => setSlice(server, rangeKey, sel),
  });
  if (!slicer.node) return null;
  const signal = getPanelSignal();
  readToolWithinKeptHistory(strip.tool, strip.params, signal).then((res) => {
    if (res.kind !== "data" || !res.data) return;
    const rows = strip.rows(res.data);
    slicer.setPoints((Array.isArray(rows) ? rows : []).map((r) => ({ t: timeOf(r[strip.time]), v: Number(r[strip.value]) })));
    /* The Active strip reads the newest snapshots only: say so when the list was cut, so a blank stretch of a long range is not read as quiet. */
    if (strip.note) slicer.setNote(strip.note(res.data));
  });

  const pickKey = server + "|" + kind;
  let ranking = topRankingPick[kind];
  let loading = 0;
  /* The page range does not change within one slicer, so a row's overlay read is made once per row and ranking: a sort, a narrowing or any other
     grid redraw that puts the pick back on its row draws from here instead of reading again. A failed or empty read is not kept. */
  const drawnReads = new Map();
  const readOverlay = (row) => {
    const cacheKey = overlay.key(row) + "|" + ranking;
    let pending = drawnReads.get(cacheKey);
    if (!pending) {
      pending = overlay.read(row, ranking, signal).then(
        (drawn) => {
          if (!drawn) drawnReads.delete(cacheKey);
          return drawn;
        },
        (e) => {
          drawnReads.delete(cacheKey);
          throw e;
        }
      );
      drawnReads.set(cacheKey, pending);
    }
    return pending;
  };
  const show = async (row) => {
    const mine = ++loading;
    const drawn = await readOverlay(row);
    if (mine !== loading || !drawn) return;
    slicer.setOverlay(drawn.points, drawn.label);
  };
  const mark = (tr, on) => {
    tr.className = on ? (tr.className + " row-selected").trim() : tr.className.split(" ").filter((c) => c !== "row-selected").join(" ");
    tr.setAttribute("aria-pressed", on ? "true" : "false");
  };
  /* A row is picked by a click or Enter or Space ON THE ROW: a button inside it (the plan and History buttons) keeps its own click and its own keys. */
  const pickable = (tr, handler) => {
    tr.setAttribute("role", "button");
    tr.setAttribute("tabindex", "0");
    tr.addEventListener("click", (e) => {
      if (e && e.target && e.target.closest && e.target.closest("button")) return;
      handler(e);
    });
    tr.addEventListener("keydown", (e) => {
      if (e.target !== tr || (e.key !== "Enter" && e.key !== " ")) return;
      e.preventDefault();
      handler(e);
    });
  };
  return {
    node: slicer.node,
    /* Another card narrowed the page: take its selection without committing it again. */
    follow: () => slicer.setSelection(getSlice(server, rangeKey)),
    onRow: (rankedBy) => {
      ranking = rankedBy;
      if (!overlay) return null;
      /* A draw starts with no overlay: it comes back below for the picked row when that row is still among the rows. */
      loading++;
      slicer.clearOverlay();
      return (row, tr) => {
        if (!overlay.eligible(row)) return;
        const key = overlay.key(row);
        tr.style.cursor = "pointer";
        tr.setAttribute("title", kind === "procedures" ? "Overlay this procedure's timeline on the time slicer" : "Overlay this query's timeline on the time slicer");
        pickable(tr, (e) => {
          if (e && e.type === "click" && selectionInside(tr)) return;
          if (overlayPicks.get(pickKey) === key) {
            overlayPicks.delete(pickKey);
            mark(tr, false);
            loading++;
            slicer.clearOverlay();
            return;
          }
          overlayPicks.set(pickKey, key);
          for (const other of tr.parentNode ? tr.parentNode.children : []) if (other !== tr) mark(other, false);
          mark(tr, true);
          show(row);
        });
        tr.setAttribute("aria-pressed", "false");
        /* A redraw (a sort, a narrowing, the 60 s rebuild) puts the pick back on the same row when it is still in the grid. */
        if (overlayPicks.get(pickKey) === key) {
          mark(tr, true);
          show(row);
        }
      };
    },
  };
}

/** Mount what `render` returns into `body`, or an error strip naming the fault if it throws. */
export function renderInto(body, render) {
  try {
    mount(body, render());
  } catch (e) {
    mount(body, errorStrip("Could not render this panel: " + (e && e.message ? e.message : String(e))));
  }
}

/* ─────────────────────────── descriptor helpers ─────────────────────────── */

/**
 * A table panel over a read, spanning both grid columns (the default for anything wider than ~5 columns).
 *
 * `emptyText` is REQUIRED, and the throw is the reason it is a parameter rather than an option. renderPanel
 * already shows a read's own {status,message} envelope when the read has nothing, and that sentence is better
 * than anything a descriptor could carry. What it does not cover is the read returning data whose row array is
 * empty — where vizTable falls back to a generic "No rows in this window", which on a collector that is off,
 * opt-in, or daily reads as a fault. Every tab is built during the DOM-shim run, so a missing sentence fails
 * there rather than shipping as a blank rectangle nobody notices.
 */
/**
 * `noteKey` names a field on the READ's own response to render above the rows (#3278) — a caveat the server
 * computed and the client could not, because a subtitle is written before the read. Optional and absent on
 * every panel but one: a capped page of rows is normally just the top of a ranking, and a panel whose rows
 * cannot be read as a population figure is the exception that needs saying so with figures.
 *
 * `moreNoteKeys` is a list of further fields on the same response, each rendered as its own note beneath the
 * `noteKey` one; an empty or absent value draws nothing. Optional, like `noteKey`.
 *
 * `columnGroups` ({ groups: [names], defaultGroups: [names] }) is for a wide grid: a column with `group: "<name>"`
 * is shown only while its group's toggle is on, and the ungrouped columns are always shown (see vizTable's
 * columnPicker).
 *
 * `dbScope` ("server", "unfiltered" or "process-rows") is the panel's own database-scope chip when it differs from its
 * read's class (#5245); null takes the read's class. Deadlock Graphs declares "process-rows" here (#5244): each graph stays whole
 * and its process rows follow the database filter.
 * `control` (#5226) is a node drawn under the title, before the rows: the ranking selector on the Top Queries and Top Procedures cards.
 */
export function table(title, read, params, rowsKey, columns, subtitle, emptyText, span = 2, noteKey = null, moreNoteKeys = null, columnGroups = null, control = null, dbScope = null, extensionMissingLine = null, emptyWins = false, onRow = null) {
  if (!emptyText) throw new Error("table(" + title + "): a table panel must explain its own empty state.");
  const desc = { title, subtitle, read, params, viz: "table", rowsKey, columns, emptyText, moreNoteKeys, span, noteKey, dbScope, extensionMissingLine, emptyWins };
  if (columnGroups) Object.assign(desc, { groups: columnGroups.groups, defaultGroups: columnGroups.defaultGroups });
  if (control) desc.control = control;
  if (onRow) desc.onRow = onRow;
  return renderPanel(desc);
}

/**
 * A stat-tile panel over a read's top-level object (dotted keys reach into a nested summary).
 *
 * `emptyText` is OPTIONAL here where table()'s and line()'s are required, and the asymmetry is the honest one:
 * a read that always returns its summary object cannot reach the empty case, and forcing a sentence onto all
 * thirty of those would be noise around the handful that need it. What needs it is a read whose HEALTHY answer
 * is a data body carrying prose and none of the summary keys — get_pg_xmin_horizon's {status:"no_holder",
 * finding} is the type — which is data, not the {status,message} envelope, so it reaches vizStat and renders
 * as a row of em-dashes. vizStat spends this at exactly the all-null case; see the guard there.
 */
export function stat(title, read, params, stats, subtitle, span = 1, emptyText) {
  return renderPanel({ title, subtitle, read, params, viz: "stat", stats, span, emptyText });
}

/**
 * A line panel over a read's row array. `opts.emptyText` is REQUIRED for the same reason table()'s is: without
 * a sentence, a read whose empty array means the thing simply did not happen inherits renderLineChart's
 * "Not enough data points to chart yet", which describes a condition it never had. get_blocking_trend and
 * get_deadlock_trend used to be the sharpest example; #2485 gave those two a {status,message} envelope, so they
 * now answer above this layer and their emptyText is the fallback rather than the sentence you see. Every other
 * line read still lands here at zero rows.
 */
export function line(title, read, params, rowsKey, xKey, series, opts = {}) {
  if (!opts.emptyText) throw new Error("line(" + title + "): a chart panel must explain its own empty state.");
  return renderPanel({
    title,
    subtitle: opts.subtitle,
    read,
    params,
    viz: "line",
    rowsKey,
    xKey,
    series,
    format: opts.format,
    unit: opts.unit,
    emptyText: opts.emptyText,
    /* #5644 W16: the page's one sentence replaces the read's own empty message. */
    emptyWins: opts.emptyWins === true,
    /* #5244: the answer's field naming the collector that answered (get_blocking_trend's `source`); absent on every other line. */
    sourceKey: opts.sourceKey,
    /* #5644 W16: wording that follows the collector that answered (see BLOCKING_BY_SOURCE); absent on every other line. */
    bySource: opts.bySource,
    /* The panel is not drawn when the server answers "not collected" (it does not apply to this kind of server). */
    hideWhenNotCollected: opts.hideWhenNotCollected === true,
    span: opts.span ?? 1,
    /* The chart menu's "at This Time" item opens the SQL Server Queries or Blocking tab, so a panel on a registry
       without them (the one PostgreSQL line) passes atTime: false. A chart offers the one item that matches it, as the
       desktop does: atTimeItem is "blocking" or "deadlocks" on those charts, and Active Queries otherwise. */
    atTime: opts.atTime === false || !params || !params.server ? null : { server: params.server, item: opts.atTimeItem || "queries" },
  });
}

/* The subtitle every latest-snapshot panel carries. These reads take no time window at all — they return the
   most recent collected snapshot — so letting the page's range label sit above them would claim a window the
   data does not have. */
export const SNAPSHOT = "latest snapshot";

/* An Azure SQL Database (engine_edition 5) reports the HOST's sockets, cores per socket, hyperthread ratio and physical
   memory, none of which is the database's allocation (a 1-vCore database read "0 sockets, 32 cores/socket, HT ratio 64,
   about 912 GB"). get_server_properties returns those four as null there, and these tiles are not drawn. Logical CPUs
   is the database's own scheduler count (a 1-vCore database reads 2), so that tile is drawn; the service objective and its
   vCores say what the database is given. */
export const AZURE_SQL_DATABASE = { key: "engine_edition", equals: 5 };

export const PROPERTY_STATS = [
  { key: "product_version", label: "Version", format: "text", small: true },
  { key: "edition", label: "Edition", format: "text", small: true },
  { key: "product_level", label: "Level", format: "text", small: true, appendKey: "product_update_level" },
  { key: "cpu_count", label: "Logical CPUs", format: "int" },
  { key: "socket_count", label: "Sockets", format: "int", hideWhen: AZURE_SQL_DATABASE },
  { key: "cores_per_socket", label: "Cores/socket", format: "int", hideWhen: AZURE_SQL_DATABASE },
  { key: "hyperthread_ratio", label: "Logical CPUs per socket", format: "int", hideWhen: AZURE_SQL_DATABASE },
  { key: "physical_memory_mb", label: "Physical memory", format: "mb", hideWhen: AZURE_SQL_DATABASE },
  { key: "is_clustered", label: "Clustered", format: "bool" },
  { key: "is_hadr_enabled", label: "Always On", format: "bool" },
  { key: "service_objective", label: "Service objective", format: "text", small: true },
  { key: "vcore_count", label: "vCores", format: "int", showWhen: AZURE_SQL_DATABASE },
];

/* #3013: the alerting subsystem's own swallowed store reads. Its own panel object (not just a stats array)
   because the WINDOW is the point: every other panel on this tab is the trailing seven days, and this one is
   an in-memory count since the service process started, which a restart takes to zero. The subtitle says so,
   because a reader who assumed otherwise would read a zero as seven quiet days.

   Deliberately carries no severity hint and feeds no band: a threshold here would have to guess how many
   blind reads make alerting unhealthy, and on this surface a wrong guess fails by saying nothing is wrong.
   "Newest" beside the counts for the same reason the Last Error column has a timestamp - the count never
   ages out of a window, so a nonzero value with an old stamp is a healed episode. */
const ALERT_READ_STATS = [
  { key: "alert_read_health.server_read_failures", label: "Blind reads (server)", format: "int" },
  { key: "alert_read_health.server_alert_passes", label: "Alert passes", format: "int" },
  { key: "alert_read_health.instance_read_failures", label: "Blind reads (service)", format: "int" },
  { key: "alert_read_health.last_failure_read", label: "Which read", format: "text", small: true, hideWhenEmpty: true },
  /* Beside "Which read" because the two answer one question together: which condition went blind, and
     whose deadline ended it. An elapsed at or about the alert pass's command deadline is this service
     giving up while the statement still ran on the store; well below it is a fault the store returned. */
  { key: "alert_read_health.last_failure_elapsed_ms", label: "Ran for", format: "ms", small: true, hideWhenEmpty: true },
  { key: "alert_read_health.last_failure_at", label: "Newest", format: "reltime", small: true, hideWhenEmpty: true },
  /* The fleet scope: failures that belong to NO server, so no per-server row can hold them and the
     service count beside a server zero would otherwise span two populations an operator acts on
     differently - a blind read on another server (read this panel there) and a blind read on a condition
     belonging to no server (the store's own self-alerts went quiet). The failures on other servers are
     the service count less the server count less this one, and are deliberately not a column: nothing
     holds a newest failure for that population, and a count with no stamp is what these stamps fix. */
  { key: "alert_read_health.fleet_read_failures", label: "Blind reads (no server)", format: "int" },
  { key: "alert_read_health.fleet_last_failure_read", label: "Which read (no server)", format: "text", small: true, hideWhenEmpty: true },
  { key: "alert_read_health.fleet_last_failure_elapsed_ms", label: "Ran for (no server)", format: "ms", small: true, hideWhenEmpty: true },
  { key: "alert_read_health.fleet_last_failure_at", label: "Newest (no server)", format: "reltime", small: true, hideWhenEmpty: true },
  /* The service count's own currency and identity terms. Scope-free by construction: the newest failure
     anywhere may be on a server this tab is not showing, which is why the fleet trio above is its own
     set rather than something to infer from these three. */
  { key: "alert_read_health.instance_last_failure_read", label: "Which read (service)", format: "text", small: true, hideWhenEmpty: true },
  { key: "alert_read_health.instance_last_failure_elapsed_ms", label: "Ran for (service)", format: "ms", small: true, hideWhenEmpty: true },
  { key: "alert_read_health.instance_last_failure_at", label: "Newest (service)", format: "reltime", small: true, hideWhenEmpty: true },
  /* #3848: the second population, beside the failure counts it is read against rather than replacing
     them. A read that crossed the 10 s deadline once and answered on the retry two seconds later is the
     store's write bands showing through - it used to be a blind condition, and it is now a count. Read
     the pair: retries with no failures is write pressure with alerting intact; both moving is a band that
     was still on twelve seconds later. Neither tile has a "Newest" or "Which read" companion, unlike
     every count above, because a retried read did not go blind - there is no episode to date. */
  { key: "alert_read_health.retried_reads", label: "Retried reads (server)", format: "int" },
  { key: "alert_read_health.instance_retried_reads", label: "Retried reads (service)", format: "int" },
  { key: "alert_read_health.counting_since", label: "Counting since", format: "reltime", small: true },
];

export const ALERT_READ_PANEL = {
  title: "Alerting Reads",
  subtitle: "since this service started, not the trailing 7 days",
  viz: "stat",
  stats: ALERT_READ_STATS,
};

/* #4843: the data families the scheduled analysis pass could not read on this server, from the same
   get_collection_health read as the panels beside it (its `stored_caveats` array, which the service omits when the
   store records none). The desktop viewer's "Analysis could not read these data families" grid, same four columns, and
   hidden when empty like it. Shared const so the SQL Server and PostgreSQL tabs cannot drift apart on it. */
export const COLLECTION_CAVEATS_PANEL = {
  title: "Analysis could not read these data families",
  subtitle: "as of the latest analysis pass",
  viz: "table",
  rowsKey: "stored_caveats",
  hideWhenNoRows: true,
  columns: [
    { key: "family", label: "Family" },
    { key: "reason", label: "Reason", wrap: true },
    { key: "first_seen_utc", label: "Since", format: "time" },
    { key: "last_seen_utc", label: "Last seen", format: "time" },
  ],
  emptyText: "The analysis pass read every data family.",
};

export const SWEEP_STATS = [
  { key: "sweep_pressure.verdict", label: "Verdict", format: "text", small: true },
  { key: "sweep_pressure.busy_percent", label: "Sweep busy %", format: "num1" },
  { key: "sweep_pressure.busy_ms_per_minute", label: "Busy ms/min", format: "int" },
  { key: "sweep_pressure.peak_cycle_ms", label: "Peak cycle", format: "ms" },
  { key: "sweep_pressure.peak_cycle_percent", label: "Peak cycle %", format: "num1" },
  { key: "sweep_pressure.peak_cycle_risk", label: "Peak risk", format: "text", small: true },
];

/* #5735: the `skipped_cycles` block of get_collection_health. A slot is one scheduled run of one collector the loop stepped
   over, summed across the server's collectors. Zero is real (count 0, the other tiles dashes); the message tile is dropped
   when there is none rather than showing a dash. The Read tile says "ok", or "unavailable" when the read failed (every figure is
   then a dash, which is not the zero a real empty window reports). */
export const SKIPPED_CYCLES_STATS = [
  { key: "skipped_cycles.status", label: "Read", format: "text", small: true },
  { key: "skipped_cycles.count_24h", label: "Late cycles", format: "int" },
  { key: "skipped_cycles.slots_skipped_24h", label: "Slots skipped", format: "int" },
  { key: "skipped_cycles.last_at", label: "Last", format: "reltime", small: true },
  { key: "skipped_cycles.last_slowest_step", label: "Cause", format: "text", small: true },
  { key: "skipped_cycles.last_slowest_step_ms", label: "Cause ran for", format: "ms" },
  { key: "skipped_cycles.last_message", label: "Last message", format: "text", small: true, hideWhenEmpty: true },
];

/* ───────────────────────── wait links (#5235) ───────────────────────── */

/* A grid's Wait cell as a link to Active Queries filtered to that wait (the desktop's wait drill-down). The column keeps
   its key, so sort, filter, CSV and copy read the raw wait; only the cell is a button, whose text is the wait. `timeKey`
   names the row's own instant: the hour around it is the range Active Queries opens on (the hour holds the clicked minute
   because different collectors capture at different instants). With no timeKey (Wait Stats, whose rows are window totals)
   the range stays as it is. A row with no wait, a QDS_* wait (waitIsLinked) or a time that does not parse is plain text. */
export function withWaitLink(columns, server, timeKey) {
  return columns.map((c) => (c.key === "wait_type" ? { ...c, render: (row) => waitLinkCell(server, row, timeKey) } : c));
}

function waitLinkCell(server, row, timeKey) {
  const wait = row ? row.wait_type : null;
  const plain = () => el("span", { text: wait == null || wait === "" ? "—" : String(wait) });
  if (!waitIsLinked(wait)) return plain();
  let tMs = null;
  if (timeKey) {
    const at = parseUtc(row[timeKey]);
    if (!at) return plain();
    tMs = at.getTime();
  }
  /* A range the page refuses is said beside the button; the filter is not set and the tab does not move. */
  const note = el("span", { class: "wait-link-note", role: "status", "aria-live": "polite" });
  const btn = el("button", {
    type: "button",
    class: "wait-link",
    title: "Show the active queries waiting on " + wait.trim() + (tMs == null ? "" : " around this time"),
    text: wait,
  });
  btn.addEventListener("click", async () => {
    note.textContent = "";
    /* Loaded on the click, not at import: a page that stubs the chart module for a test needs no export it never uses. */
    const { openServerTabAt } = await import("../../charts.js");
    await openServerTabAt(server, tMs, "queries", {
      beforeRoute: () => setQueryWaitFilter(server, wait),
      say: (msg) => {
        note.textContent = msg;
      },
    });
  });
  return el("span", {}, [btn, note]);
}

/* #1949 ordering, which every query grid in both apps follows: the time/identity anchor, then the QUERY TEXT,
   then the metrics. Text pushed behind the metrics is text nobody scrolls to.
   The desktop grid's columns in its order, except that Query and Module sit right of Database (the rule above)
   rather than behind the two timestamps. The ungrouped columns are the core set and always show; the rest follow the toggles in
   TOP_QUERY_GROUPS, all off at first. Last Execution and Creation Time arrive as UTC instants (the read converts them from the monitored
   server's clock) and print in the browser's local time; every grouped field is omitted by the read on the hourly tier and prints a dash. */
export const TOP_QUERY_COLUMNS = [
  { key: "database_name", label: "Database" },
  tagColumn(),
  { key: "query_text", label: "Query", valueList: false, render: (r) => codeDisclosure(r.query_text) },
  { key: "host_object", label: "Module" },
  { key: "last_execution_time", label: "Last Execution", format: "time", group: "Times" },
  { key: "creation_time", label: "Creation Time", format: "time", group: "Times" },
  { key: "execution_count", label: "Execs", format: "int" },
  { key: "total_cpu_ms", label: "Total CPU", format: "ms" },
  { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
  { key: "worker_time_per_second", label: "Peak CPU ms/s", format: "num1", group: "Times" },
  { key: "plan_generation_num", label: "Plan Gen", format: "int", group: "Times" },
  { key: "total_clr_ms", label: "Total CLR", format: "ms", group: "Times" },
  { key: "total_elapsed_ms", label: "Total Duration", format: "ms" },
  { key: "avg_elapsed_ms", label: "Avg Duration", format: "ms" },
  { key: "total_logical_reads", label: "Total Reads", format: "int" },
  { key: "avg_reads", label: "Avg Reads", format: "int" },
  { key: "total_logical_writes", label: "Total Writes", format: "int", group: "I/O and rows" },
  { key: "total_physical_reads", label: "Physical Reads", format: "int", group: "I/O and rows" },
  { key: "total_rows", label: "Total Rows", format: "int", group: "I/O and rows" },
  { key: "total_spills", label: "Total Spilled Pages", format: "int" },
  { key: "min_cpu_ms", label: "Min CPU", format: "ms", group: "Extremes" },
  { key: "max_cpu_ms", label: "Max CPU", format: "ms", group: "Extremes" },
  { key: "min_elapsed_ms", label: "Min Duration", format: "ms", group: "Extremes" },
  { key: "max_elapsed_ms", label: "Max Duration", format: "ms", group: "Extremes" },
  { key: "min_physical_reads", label: "Min Phys Reads", format: "int", group: "Extremes" },
  { key: "max_physical_reads", label: "Max Phys Reads", format: "int", group: "Extremes" },
  { key: "min_rows", label: "Min Rows", format: "int", group: "Extremes" },
  { key: "max_rows", label: "Max Rows", format: "int", group: "Extremes" },
  { key: "min_grant_kb", label: "Min Grant KB", format: "int", group: "Memory grants" },
  { key: "max_grant_kb", label: "Max Grant KB", format: "int", group: "Memory grants" },
  { key: "min_used_grant_kb", label: "Min Used Grant KB", format: "int", group: "Memory grants" },
  { key: "max_used_grant_kb", label: "Max Used Grant KB", format: "int", group: "Memory grants" },
  { key: "min_ideal_grant_kb", label: "Min Ideal Grant KB", format: "int", group: "Memory grants" },
  { key: "max_ideal_grant_kb", label: "Max Ideal Grant KB", format: "int", group: "Memory grants" },
  { key: "min_spills", label: "Min Spilled Pages", format: "int", group: "Extremes" },
  { key: "max_spills", label: "Max Spilled Pages", format: "int", group: "Extremes" },
  { key: "min_dop", label: "Min DOP", format: "int", group: "Parallelism" },
  { key: "max_dop", label: "Max DOP", format: "int", group: "Parallelism" },
  { key: "min_reserved_threads", label: "Min Rsvd Threads", format: "int", group: "Parallelism" },
  { key: "max_reserved_threads", label: "Max Rsvd Threads", format: "int", group: "Parallelism" },
  { key: "min_used_threads", label: "Min Used Threads", format: "int", group: "Parallelism" },
  { key: "max_used_threads", label: "Max Used Threads", format: "int", group: "Parallelism" },
  { key: "query_hash", label: "Query Hash", mono: true },
  { key: "query_plan_hash", label: "Plan Hash", group: "Hashes and handles", mono: true },
  { key: "sql_handle", label: "SQL Handle", group: "Hashes and handles", mono: true },
  { key: "plan_handle", label: "Plan Handle", group: "Hashes and handles", mono: true },
];

export const TOP_QUERY_GROUPS = {
  groups: ["Times", "I/O and rows", "Extremes", "Memory grants", "Parallelism", "Hashes and handles"],
  defaultGroups: [],
};

/* The desktop Top Procedures grid's columns in its order (its Query Plan column is procedurePlanColumn, added where the grid is built).
   The ungrouped columns are the core set and always show; the rest follow the toggles in TOP_PROC_GROUPS, all off at
   first. Last Execution and Cached Time arrive as UTC instants (the read converts them from the monitored server's
   clock) and print in the browser's local time. The hourly tier carries none of the grouped fields, nor Type, the
   write and read totals or Avg Reads, and prints a dash for each. */
export const TOP_PROC_COLUMNS = [
  { key: "database_name", label: "Database" },
  tagColumn(),
  { key: "full_name", label: "Procedure" },
  { key: "object_type", label: "Type" },
  { key: "last_execution_time", label: "Last Execution", format: "time", group: "Times" },
  { key: "cached_time", label: "Cached Time", format: "time", group: "Times" },
  { key: "execution_count", label: "Execs", format: "int" },
  { key: "total_cpu_ms", label: "Total CPU", format: "ms" },
  { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
  { key: "total_elapsed_ms", label: "Total Duration", format: "ms" },
  { key: "avg_elapsed_ms", label: "Avg Duration", format: "ms" },
  { key: "total_logical_reads", label: "Total Reads", format: "int" },
  { key: "avg_reads", label: "Avg Reads", format: "int" },
  { key: "total_logical_writes", label: "Total Writes", format: "int", group: "I/O and spills" },
  { key: "total_physical_reads", label: "Physical Reads", format: "int", group: "I/O and spills" },
  { key: "total_spills", label: "Total Spilled Pages", format: "int" },
  { key: "avg_spills", label: "Avg Spilled Pages", format: "num2", group: "I/O and spills" },
  { key: "min_cpu_ms", label: "Min CPU", format: "ms", group: "Extremes" },
  { key: "max_cpu_ms", label: "Max CPU", format: "ms", group: "Extremes" },
  { key: "min_elapsed_ms", label: "Min Duration", format: "ms", group: "Extremes" },
  { key: "max_elapsed_ms", label: "Max Duration", format: "ms", group: "Extremes" },
  { key: "min_logical_reads", label: "Min Reads", format: "int", group: "Extremes" },
  { key: "max_logical_reads", label: "Max Reads", format: "int", group: "Extremes" },
  { key: "min_physical_reads", label: "Min Phys Reads", format: "int", group: "Extremes" },
  { key: "max_physical_reads", label: "Max Phys Reads", format: "int", group: "Extremes" },
  { key: "min_logical_writes", label: "Min Writes", format: "int", group: "Extremes" },
  { key: "max_logical_writes", label: "Max Writes", format: "int", group: "Extremes" },
  { key: "min_spills", label: "Min Spilled Pages", format: "int", group: "Extremes" },
  { key: "max_spills", label: "Max Spilled Pages", format: "int", group: "Extremes" },
];

export const TOP_PROC_GROUPS = {
  groups: ["Times", "I/O and spills", "Extremes"],
  defaultGroups: [],
};


/**
 * The instance trends get_server_trend serves as one line per field (#5117): the CPU tab's scheduler pressure and the
 * Memory tab's plan cache. The desktop's CPU Scheduler chart plots the runnable, blocked and queued task counts under a
 * "Task Count" axis; its Plan Cache chart plots single-use and multi-use plan cache size under "Plan Cache Size (MB)".
 * Both are levels the read averages per bucket, so the unit is the read's own.
 */
const SERVER_TRENDS = {
  cpu_scheduler: {
    title: "CPU Scheduler",
    metric: "cpu_scheduler",
    unit: "tasks",
    series: [
      { key: "runnable_tasks", label: "Runnable Tasks" },
      { key: "blocked_tasks", label: "Blocked Tasks" },
      { key: "queued_requests", label: "Queued Requests" },
    ],
    emptyText: "No CPU scheduler samples in this window.",
  },
  plan_cache: {
    title: "Plan Cache",
    metric: "plan_cache",
    unit: "MB",
    series: [
      { key: "single_use_mb", label: "Single-Use" },
      { key: "multi_use_mb", label: "Multi-Use" },
    ],
    emptyText: "No plan cache samples in this window.",
  },
  /* #6297: the desktop's tempdb tab draws the reserved plus unallocated space as one "Allocated MB" line. */
  tempdb_size: {
    title: "tempdb Allocated Size",
    metric: "tempdb_size",
    unit: "MB",
    series: [{ key: "allocated_mb", label: "Allocated MB" }],
    emptyText: "No tempdb space samples in this window.",
    emptyWins: true,
  },
};

/* #6793: the hand-built chart panels below read with `ctx.signal`, but the server page's ctx carries none, so leaving the page (or a redraw)
   cancelled none of their reads. A panel takes the batch's signal when it is built, the one setPanelSignal holds while redrawPanels calls
   build(), and keeps it in its own ctx, so a later picker draw reads on the batch it belongs to and not on whatever batch is current by then. */
export function withPanelSignal(ctx) {
  return ctx && ctx.signal ? ctx : { ...ctx, signal: getPanelSignal() };
}

/** A get_server_trend line panel for `kind` (a key of SERVER_TRENDS), over the page's range and its `as_of`. */
export function serverTrendPanel(server, ctx, kind) {
  ctx = withPanelSignal(ctx);
  const spec = SERVER_TRENDS[kind];
  const { panel, body } = panelShell(spec.title + " Trend", ctx.label, 2, "get_server_trend");
  (async () => {
    const res = await readToolWithinKeptHistory("get_server_trend", { server, metric: spec.metric, hours: ctx.hours }, ctx && ctx.signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, [keptWindowStrip(res), emptyStrip(emptyHeadline(spec, res) || spec.emptyText)]);
    const points = res.data.trend || [];
    if (!points.length) return mount(body, [keptWindowStrip(res), emptyStrip(spec.emptyText)]);
    /* #3653 A5: the payload's baseline discontinuities as a notice above the chart. */
    const notes = discontinuityNotes(res.data);
    mount(body, [
      keptWindowStrip(res),
      notes.length ? noticeStrip(notes.join(" ")) : null,
      zoomableLineChart({
        atTime: { server },
        points,
        xKey: "time",
        series: spec.series.map((s, i) => ({ key: s.key, label: s.label, color: CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length] })),
        formatValue: (v) => (Math.round(v * 100) / 100).toLocaleString(),
        unit: spec.unit,
        ...windowFromHours(res.keptHours || ctx.hours),
      }, "server-trend|" + server + "|" + spec.metric, chartZoomScope(ctx.hours)),
      chartNote(res.data, "level"),
    ]);
  })();
  return panel;
}

/**
 * The get_server_trend metrics that follow named series, drawn as the desktop draws them (#6296, #6298, #6297): one line per
 * file or collector, over the page's range and its `as_of`, with no `names` so the read picks its own heaviest five (the desktop
 * keeps the top ten files by throughput, the top twelve tempdb files by latency, and every collector; the read takes no count, so the page shows five). A chart's `value` is the
 * figure one point plots, and a point with none is left out of that line, never drawn as 0. The buckets are the read's own: nothing
 * is re-aggregated here.
 */
const SERIES_TRENDS = {
  file_io_throughput: {
    title: "File I/O Throughput",
    subtitle: "read and write MB/s per file",
    seriesKey: "file_label",
    /* #6359: what chartNote says besides the width: "summed" for a rate over summed bytes. The latency and longest-run
       specs below are ratios and extremes, where the smaller-counts fact does not apply, so they say the width only. */
    noteKind: "summed",
    emptyText: "No file I/O samples in this window.",
    emptyWins: true,
    note: "One line per file, the five that moved the most bytes in the window.",
    charts: [
      { title: "Read throughput (MB/s)", unit: "MB/s", value: (r) => r.read_mb_per_sec, empty: "No reads in this window." },
      { title: "Write throughput (MB/s)", unit: "MB/s", value: (r) => r.write_mb_per_sec, empty: "No writes in this window." },
    ],
  },
  tempdb_file_io: {
    title: "tempdb File I/O Latency",
    subtitle: "avg read plus write latency per tempdb file",
    seriesKey: "file_name",
    emptyText: "No tempdb file I/O samples in this window.",
    emptyWins: true,
    note: "One line per tempdb file: its average read latency plus its average write latency, the five files with the most I/O in the window.",
    charts: [
      {
        title: "Read plus write latency (ms)",
        unit: "ms",
        value: (r) => (r.avg_read_latency_ms == null && r.avg_write_latency_ms == null ? null : (r.avg_read_latency_ms || 0) + (r.avg_write_latency_ms || 0)),
        empty: "No tempdb file I/O samples in this window.",
      },
    ],
  },
  collector_duration: {
    title: "Collector Duration Trends",
    subtitle: "longest successful run per collector",
    seriesKey: "collector_name",
    emptyText: "No successful collector runs in this window.",
    emptyWins: true,
    note: "One line per collector, the five with the longest run in the window; the Viewer's chart draws every collector. Each point is the longest run in its bucket, and a collector needs runs in two buckets to draw a line, so a collector that ran in only one bucket of a short window has no line.",
    minPoints: 2,
    charts: [
      { title: "Longest run (ms)", unit: "ms", value: (r) => r.max_duration_ms, empty: "No collector ran in two or more buckets of this window, so there is no line to draw." },
    ],
  },
};

/** A get_server_trend panel for a named-series metric (a key of SERIES_TRENDS): one chart per spec chart over one read. */
export function seriesTrendPanel(server, ctx, kind) {
  ctx = withPanelSignal(ctx);
  const spec = SERIES_TRENDS[kind];
  const { panel, body } = panelShell(spec.title, spec.subtitle + ", " + ctx.label, 2, "get_server_trend");
  (async () => {
    const res = await readToolWithinKeptHistory("get_server_trend", { server, metric: kind, hours: ctx.hours }, ctx && ctx.signal);
    if (res.kind === "aborted") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, [keptWindowStrip(res), emptyStrip(emptyHeadline(spec, res) || spec.emptyText)]);
    const series = (res.data.series || []).filter((s) => s && s[spec.seriesKey] != null);
    if (!series.length) return mount(body, [keptWindowStrip(res), emptyStrip(spec.emptyText)]);
    /* #3653 A5: the payload's baseline discontinuities as a notice above the charts. */
    const notes = discontinuityNotes(res.data);
    const draw = (chart) => {
      /* A file or collector keeps its colour in every chart of the panel: the palette index is its place in the read, not in the
         chart it has points in (the write chart skips a file that wrote nothing). */
      const drawn = series
        .map((s, i) => ({ index: i, label: s[spec.seriesKey], rows: (s.trend || []).map((r) => ({ time: r.time, v: chart.value(r) })).filter((r) => r.v != null) }))
        .filter((d) => d.rows.length >= (spec.minPoints || 1))
        .map((d) => ({ ...d, key: "s" + d.index, color: SERIES_COLORS[d.index % SERIES_COLORS.length] }));
      return drawn.length
        ? [
            el("h4", { text: chart.title }),
            zoomableLineChart({
              atTime: { server },
              points: mergeSeriesRows(drawn, "time", "v"),
              xKey: "time",
              series: drawn.map((d) => ({ key: d.key, label: d.label, color: d.color })),
              formatValue: (v) => (Math.round(v * 100) / 100).toLocaleString() + " " + chart.unit,
              unit: chart.unit,
              ...windowFromHours(res.keptHours || ctx.hours),
            }, "series-trend|" + server + "|" + kind + "|" + chart.unit, chartZoomScope(ctx.hours)),
          ]
        : [el("h4", { text: chart.title }), emptyStrip(chart.empty)];
    };
    mount(body, [
      keptWindowStrip(res),
      notes.length ? noticeStrip(notes.join(" ")) : null,
      ...spec.charts.flatMap(draw),
      el("div", { class: "mp-metric-note", text: spec.note }),
      chartNote(res.data, spec.noteKind),
    ]);
  })();
  return panel;
}

export const FINDING_COLUMNS = [
  { key: "last_seen", label: "Last seen", format: "time" },
  { key: "category", label: "Category" },
  /* The finding's headline, not its story key; severity as a word, not the score (the cards on the Recommendations tab do the same). */
  { key: "story_path", label: "Finding", display: (r) => findingTitle(r), sortValue: (r) => findingTitle(r), wrap: true },
  { key: "severity", label: "Severity", display: (r) => severityLabel(r.severity), sortValue: (r) => Number(r.severity), cellClass: (r) => { const w = severityLabel(r.severity); return w === "Info" ? null : "sev-" + w; } },
  { key: "occurrences", label: "Occurrences", format: "int" },
  { key: "first_seen", label: "First seen", format: "time" },
];

/* #2484: the raw log's columns. The duration SPLIT is the reason this table earns its place beside the
   rollup -- total time cannot separate a collector that is slow because the monitored server is slow from
   one that is slow because the store is, and that is the first question anyone asks of a slow collector.

   #3192: this column was headed "On Server", and on the collectors that fetch plan XML or statement text
   that header was false. The deferred fetches run inside the driver's per-item SQL stopwatch and each one
   round-trips the STORE before writing back what came off the target, so a measured 107,334 ms of a
   124,972 ms query_store figure was the monitoring store -- 86% -- under a header naming the monitored
   server. Headed "SQL" now, matching what the WPF viewer's grid has always called it - the HEADER only: that
   grid has no "Store (in SQL)" breakout and cannot get one cheaply, because its row type and both backing
   queries are verbatim copies of Lite's and Lite runs no deferred fetch. Its own comment says so. Here the
   store share does get its own column beside the total. "Store (in SQL)" is blank on the ~98% of runs that perform no deferred
   fetch, and blank there means "nothing to attribute", not "no store time": the per-item watermark refresh
   is a store read inside the same stopwatch and is recorded nowhere, which is why that column is a floor
   and "On Store" (the binary COPY of the collected rows) is not where the probe went either. */
export const COLLECTION_LOG_COLUMNS = [
  { key: "collection_time", label: "When", format: "time" },
  { key: "collector", label: "Collector" },
  { key: "status", label: "Status", statusSev: true },
  { key: "duration_ms", label: "Total", format: "ms" },
  { key: "sql_duration_ms", label: "SQL", format: "ms" },
  { key: "sql_store_ms", label: "Store (in SQL)", format: "ms" },
  { key: "store_duration_ms", label: "On Store", format: "ms" },
  { key: "rows_collected", label: "Rows", format: "int" },
  { key: "error_message", label: "Error", wrap: true, localizeText: true, plain: "counts" },
];

export function selectionInside(node) {
  const sel = typeof window !== "undefined" && window.getSelection ? window.getSelection() : null;
  return !!sel && !sel.isCollapsed && String(sel).length > 0 && !!sel.anchorNode && node.contains(sel.anchorNode);
}

export const COLLECTOR_COLUMNS = [
  { key: "collector", label: "Collector" },
  { key: "status", label: "Status", statusSev: true },
  { key: "total_runs", label: "Runs", format: "int" },
  { key: "errors", label: "Errors", format: "int" },
  { key: "yields", label: "Yields", format: "int" },
  /* #2804: cycles the wall-clock budget gave up on. Beside Yields because both are guards firing rather
     than faults, but this one is data LOSS — the cycle stored nothing and advanced no watermark — and it
     is why a WARNING in the Status column may have nothing to do with the Errors beside it. */
  { key: "abandoned", label: "Abandoned", format: "int" },
  { key: "failure_rate_pct", label: "Failure %", format: "num1" },
  { key: "avg_duration_ms", label: "Avg Dur", format: "ms" },
  { key: "p95_duration_ms", label: "p95 Dur", format: "ms" },
  /* #3017: what the spend BOUGHT, in the columns right after what it cost. Every column to the left of
     these describes cost and none said whether any of it bought anything; the rows figure lived on
     get_collector_cost, a different read over a different (hourly, fleet-wide) series. Rows beside Runs
     w/ Rows because a rows total with no run count behind it cannot tell a collector that is productive
     occasionally from one productive throughout — get_pg_blocking already reports its captures the same
     way. Deliberately NOT a Status input: zero rows is the correct resting state for an event collector
     on a quiet target, and a band keyed on cost-plus-zero-rows would light up the healthy install. */
  { key: "rows_stored", label: "Rows", format: "int" },
  { key: "runs_with_rows", label: "Runs w/ Rows", format: "int" },
  { key: "last_success", label: "Last Success", format: "time" },
  /* #4938: a daily collector's run time on the monitored server's clock, when it is next due (UTC, shown in local
     time like Last Success), and the note a skipped day carries. The Heaviest Collectors table below ranks by cost
     per minute, which puts a once-a-day collector last, so this is the table that lists every collector with a run
     time. Each column is left out unless a row fills it, so a server with no run times shows none of them. */
  { key: "run_at", label: "Run at", hideWhenEmpty: true },
  { key: "next_run_utc", label: "Next run", format: "time", hideWhenEmpty: true },
  { key: "run_time_note", label: "Run time", clamp: true, localizeText: true, hideWhenEmpty: true },
  { key: "last_error", label: "Last Error", clamp: true, localizeText: true, plain: "named" },
  /* #1837: what a NON-failing run reported (an enumeration that came back with 0 items). Blank for a
     plainly healthy collector; the same column the two WPF grids carry, so the web view is not the one
     Collection Health surface that still hides it. note_summary, not the raw last_note: it carries the
     "(all N runs)" qualifier that separates a persistently empty collector from an occasionally quiet
     one, composed server-side from the shared formatter so this table cannot render it a third way.
     A collector's label=value counts read as its latest NOTED run's ("latest run:" only when every run
     carried them), never a window total. */
  { key: "note_summary", label: "Note", clamp: true, localizeText: true, plain: true },
  /* #5646: a collector that recovered (its newest 3 runs succeeded after failures) is HEALTHY, and this says so with the
     time of the last failure. Partial rows carry no note_summary, so the sentence rides in its own field. */
  { key: "recovered_note", label: "Recovered", clamp: true, localizeText: true, plain: true, hideWhenEmpty: true },
  /* #6254: where the Query Store collector's newest run read its rows from ("replica 12, primary 2"). Only a server with the Query
     Store replica setting on has one, so the column is left out unless a row has it and every other server's table is as it was. */
  { key: "read_source", label: "Read source", clamp: true, hideWhenEmpty: true },
  /* #3017: which of the two zero-output readings a collector that spent and stored nothing is — read and
     found nothing, or could not read. Blank whenever Rows is positive, for the same reason the Note
     column is blank on a plainly healthy collector. Composed server-side from the shared formatter, so
     this table cannot render the sentence a second way. */
  { key: "output_finding", label: "Output", clamp: true, localizeText: true, plain: true },
  /* #4620: why a collector that stopped doing what it used to do reads WARNING. The Status column already
     showed the floor, but no column showed the sentence behind it, so a regressed row read WARNING with
     every other cell blank. Composed server-side from the shared formatter, like the two columns above. */
  { key: "regression_finding", label: "Regression", clamp: true, localizeText: true, plain: true },
  /* #5249: a PostgreSQL collector this server's engine does not collect (a collector this engine kind can never run: an Aurora-only reader on stock PostgreSQL, the sampler on Aurora) has no log row,
     so the server lists it with status not_collected and the sentence saying why. The status paints with the
     neutral Unknown band, not an error one: a gate is not a fault. Left out unless a row has a message, so a
     server with no such collector (and every SQL Server tab) shows no column of dashes. */
  { key: "message", label: "Why not collected", clamp: true, localizeText: true, hideWhenEmpty: true, plain: true },
];

export const HEAVIEST_COLUMNS = [
  { key: "collector", label: "Collector" },
  { key: "avg_duration_ms", label: "Avg", format: "ms" },
  { key: "p95_duration_ms", label: "p95", format: "ms" },
  { key: "max_duration_ms", label: "Max", format: "ms" },
  { key: "frequency_minutes", label: "Every (min)", format: "num1" },
  /* #4938: the collector's run time on the monitored server's clock, beside the cadence it runs on. Left out unless a
     row has one, so a server with no run times shows no column of dashes. */
  { key: "run_at", label: "Run at", hideWhenEmpty: true },
  { key: "amortized_ms_per_minute", label: "ms/min", format: "num1" },
  { key: "pct_of_sweep_budget_per_run", label: "% of run budget", format: "num1" },
];

/* ─────────────────────────── PostgreSQL stat descriptors ─────────────────────────── */

/* Severity reaches these tiles and grids as the read's own token — `critical_far_past_threshold`,
   `warning_inactive_and_growing`, `ok`. It is NOT translated here. Two lexicons for one vocabulary drift, and
   the one that drifts is never the one being read; the tokens are also what the MCP surface already hands an
   agent, so an operator reading the screen and an agent reading the tool see the same word. */

export const PG_WRAPAROUND_STATS = [
  { key: "worst_database", label: "Closest to wraparound", format: "text", small: true },
  { key: "worst_severity", label: "Severity", format: "text", small: true },
  { key: "worst_pct_toward_wraparound", label: "Toward wraparound (%)", format: "num2" },
];

export const PG_XMIN_STATS = [
  { key: "winning_source", label: "Holding the horizon", format: "text", small: true },
  { key: "winning_holder", label: "Holder", format: "text", small: true },
  { key: "winning_xmin_age", label: "xmin age (XIDs)", format: "int" },
];

export const PG_SLOT_STATS = [
  { key: "slot_count", label: "Slots", format: "int" },
  { key: "inactive_count", label: "Inactive", format: "int" },
  { key: "total_retained_wal_gb", label: "Retained WAL (GB)", format: "num2" },
  { key: "worst_slot", label: "Worst slot", format: "text", small: true },
  { key: "worst_severity", label: "Severity", format: "text", small: true },
];

export const PG_AUTOVACUUM_STATS = [
  { key: "tables_returned", label: "Tables returned", format: "int" },
  { key: "past_threshold_count", label: "Past threshold (of those)", format: "int" },
  { key: "growing_count", label: "Still growing (of those)", format: "int" },
  { key: "autovacuum_disabled_count", label: "Autovacuum off (of those)", format: "int" },
  { key: "worst_table", label: "Worst table", format: "text", small: true },
  { key: "worst_severity", label: "Severity", format: "text", small: true },
];

