/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #6300: the Compare picker on the Top Queries, Top Procedures and Query Store grids, the web twin of the Viewer's
 * "Compare:" box (ViewerServerTab.QueriesComparison.cs, PerformanceMonitor.Ui/ComparisonItemBase.cs). One pick, shared by
 * the three grids, default Off. A pick turns a grid into a delta grid: the same read is made twice, for the page's window
 * and for the window shifted back (Yesterday -1 day, Last week and Same day last week -7 days, the Viewer's own shifts,
 * named to the read as `as_of`), and the rows are matched here, in the browser. No new read.
 *
 * Matching and flags, as the Viewer's: rows match on database plus query hash (Top Queries, Query Store) or database plus
 * procedure name (Top Procedures); a key with more than one row in a window is one row, executions summed and the
 * averages weighted by executions. NEW is executions in this window and none in the earlier one, GONE is the reverse.
 * A delta is (now - before) / before, nothing when `before` is 0. Deltas are on executions, average duration and average
 * CPU. There is no logical reads column (Erik's ruling). Rows sort NEW first, then by duration delta, biggest first, GONE last.
 *
 * The grids are the top 100 of each window (the Viewer's own cut), so a row ranked past 100 in the earlier window reads
 * NEW here though it ran. When the earlier window holds nothing, or either read fails, the page says so above the plain
 * grid: it never draws every row as NEW.
 */
import { el, mount, noticeStrip, readToolWithinKeptHistory, windowEndMs, localTime } from "../util.js";

const DAY_MS = 86400000;

/* The picker's choices, in the Viewer's order. "Last week" and "Same day last week" shift alike there too. */
export const COMPARE_OPTIONS = [
  { value: "off", label: "Off", shiftDays: 0 },
  { value: "yesterday", label: "Yesterday", shiftDays: 1 },
  { value: "lastweek", label: "Last week", shiftDays: 7 },
  { value: "sameday", label: "Same day last week", shiftDays: 7 },
];

/** The pick, at MODULE scope so the 60 s rebuild of a tab keeps it, and the three grids share it. */
export const comparePick = { value: "off" };

/** How many rows each window's read asks for: the Viewer unions the top 100 of both windows. */
export const COMPARE_TOP = 100;

export const NO_COMPARISON_DATA = "No data for the comparison window";

/** The shift in ms for a pick, 0 for Off or an unknown value. */
export function compareShiftMs(pick) {
  const o = COMPARE_OPTIONS.find((x) => x.value === pick);
  return o ? o.shiftDays * DAY_MS : 0;
}

/** The `as_of` (ISO instant) for the earlier window when the current one ends at `endMs`. */
export function comparisonAsOf(pick, endMs) {
  return new Date(endMs - compareShiftMs(pick)).toISOString();
}

/* What each grid is, for matching: its read, the rows' key, and the fields that carry the figures. */
const KINDS = {
  queries: { read: "get_top_queries_by_cpu", rowsKey: "queries", keyOf: (r) => [r.database_name, r.query_hash], nameOf: (r) => r.query_text, nameLabel: "Query", duration: "avg_elapsed_ms", cpu: "avg_cpu_ms" },
  procedures: { read: "get_top_procedures_by_cpu", rowsKey: "procedures", keyOf: (r) => [r.database_name, r.full_name], nameOf: (r) => r.full_name, nameLabel: "Procedure", duration: "avg_elapsed_ms", cpu: "avg_cpu_ms" },
  querystore: { read: "get_query_store_top", rowsKey: "queries", keyOf: (r) => [r.database_name, r.query_hash], nameOf: (r) => r.query_text, nameLabel: "Query", duration: "avg_duration_ms", cpu: "avg_cpu_ms" },
};

export function compareKind(kind) {
  return KINDS[kind] || null;
}

const num = (v) => (typeof v === "number" && Number.isFinite(v) ? v : null);

/** One row per key: executions summed, each average weighted by executions (a row with no figure adds no weight to it). */
export function aggregateByKey(kind, rows) {
  const k = KINDS[kind];
  const byKey = new Map();
  for (const r of Array.isArray(rows) ? rows : []) {
    if (!r || typeof r !== "object") continue;
    const id = JSON.stringify(k.keyOf(r).map((v) => (v === undefined ? null : v)));
    const execs = num(r.execution_count) || 0;
    let a = byKey.get(id);
    if (!a) {
      a = { database_name: r.database_name ?? null, name: k.nameOf(r) ?? null, query_text: r.query_text ?? null, module: r.host_object ?? r.module_name ?? null, execs: 0, dW: 0, dN: 0, cW: 0, cN: 0 };
      byKey.set(id, a);
    }
    if (a.name == null || a.name === "") a.name = k.nameOf(r) ?? a.name;
    a.execs += execs;
    const d = num(r[k.duration]);
    const c = num(r[k.cpu]);
    if (d != null && execs > 0) { a.dW += d * execs; a.dN += execs; }
    if (c != null && execs > 0) { a.cW += c * execs; a.cN += execs; }
  }
  const out = new Map();
  for (const [id, a] of byKey) {
    out.set(id, { ...a, avgDuration: a.dN > 0 ? a.dW / a.dN : 0, avgCpu: a.cN > 0 ? a.cW / a.cN : 0 });
  }
  return out;
}

/** (now - before) / before * 100, or null when `before` is 0 (the Viewer's ComputeDelta). */
export function deltaPct(now, before) {
  if (!before) return null;
  return ((now - before) / before) * 100;
}

/** "+12.3%" or "-4.0%", a dash for no delta (the Viewer's FormatDelta). */
export function formatDelta(pct) {
  if (pct == null) return "—";
  return (pct >= 0 ? "+" : "") + pct.toFixed(1) + "%";
}

/* Red above `red`, green below -`green`, else nothing: the Viewer's brushes (duration and CPU 25 / 25, executions 100 / 50). */
function band(pct, red, green) {
  if (pct == null) return null;
  if (pct > red) return "Critical";
  if (pct < -green) return "Healthy";
  return null;
}

/**
 * The delta rows for a grid: the union of the two windows' keys, each with this window's figures, the earlier window's,
 * the deltas, and `status` NEW / GONE / "". Sorted NEW first, then duration delta descending (a row with no duration delta
 * last in its group), GONE last; equal rows keep the order the reads gave.
 */
export function compareRows(kind, currentRows, baselineRows) {
  const cur = aggregateByKey(kind, currentRows);
  const base = aggregateByKey(kind, baselineRows);
  const rows = [];
  const ids = new Set([...cur.keys(), ...base.keys()]);
  for (const id of ids) {
    const c = cur.get(id);
    const b = base.get(id);
    const execs = c ? c.execs : 0;
    const bExecs = b ? b.execs : 0;
    const isNew = execs > 0 && bExecs === 0;
    const isGone = execs === 0 && bExecs > 0;
    const durationPct = deltaPct(c ? c.avgDuration : 0, b ? b.avgDuration : 0);
    const cpuPct = deltaPct(c ? c.avgCpu : 0, b ? b.avgCpu : 0);
    const execPct = deltaPct(execs, bExecs);
    const src = c || b;
    rows.push({
      database_name: src.database_name,
      name: src.name,
      query_text: src.query_text,
      module: src.module,
      status: isNew ? "NEW" : isGone ? "GONE" : "",
      execution_count: execs,
      baseline_execution_count: bExecs,
      execution_delta_pct: execPct,
      avg_duration_ms: c ? c.avgDuration : 0,
      baseline_avg_duration_ms: b ? b.avgDuration : 0,
      duration_delta_pct: durationPct,
      avg_cpu_ms: c ? c.avgCpu : 0,
      baseline_avg_cpu_ms: b ? b.avgCpu : 0,
      cpu_delta_pct: cpuPct,
      duration_delta_sev: band(durationPct, 25, 25),
      cpu_delta_sev: band(cpuPct, 25, 25),
      execution_delta_sev: band(execPct, 100, 50),
      _group: isNew ? 0 : isGone ? 2 : 1,
      _sort: durationPct != null ? durationPct : isNew ? Infinity : -Infinity,
    });
  }
  const indexed = rows.map((r, i) => [r, i]);
  indexed.sort((x, y) => x[0]._group - y[0]._group || (y[0]._sort === x[0]._sort ? 0 : y[0]._sort > x[0]._sort ? 1 : -1) || x[1] - y[1]);
  return indexed.map(([r]) => r);
}

/** True when the rows hold at least one execution (the Viewer's "no baseline data" is the opposite). */
export function hasExecutions(kind, rows) {
  const aggregated = aggregateByKey(kind, rows);
  for (const a of aggregated.values()) if (a.execs > 0) return true;
  return false;
}

/**
 * What to draw for the two answers. `{ mode: "delta", rows }` when both reads answered with rows and the earlier window ran
 * something; otherwise `{ mode: "plain", notice }`: the plain grid under a sentence saying why there is no comparison. The
 * earlier window's answer being empty or an error is "no data for the comparison window", never every row NEW.
 */
export function planComparison(kind, currentRes, baselineRes) {
  const k = KINDS[kind];
  const rowsOf = (res) => (res && res.kind === "data" && res.data && Array.isArray(res.data[k.rowsKey]) ? res.data[k.rowsKey] : []);
  if (!baselineRes || baselineRes.kind !== "data" || !hasExecutions(kind, rowsOf(baselineRes))) {
    const why = baselineRes && baselineRes.kind === "error" ? ": " + (baselineRes.message || "the read failed") : "";
    return { mode: "plain", notice: NO_COMPARISON_DATA + why + "." };
  }
  if (!currentRes || currentRes.kind !== "data") {
    return { mode: "plain", notice: NO_COMPARISON_DATA + ": this window has nothing to compare, or could not be read." };
  }
  return { mode: "delta", rows: compareRows(kind, rowsOf(currentRes), rowsOf(baselineRes)) };
}

/* ─────────── the picker ─────────── */

/* Every grid on the page that offers the picker: a pick redraws all of those still on the page, so the one pick is the pick of
   all three. Entries whose slot the page threw away (a tab switch, a rebuild) are dropped as they are met. */
const drawers = new Set();

/** The Compare select. `draw` redraws the grid it sits in; `slot` is that grid's holder, to tell when the page has dropped it. */
export function compareControl(draw, slot) {
  const entry = { draw, slot };
  drawers.add(entry);
  const sel = el("select", { class: "range-select-inline", "aria-label": "Compare" }, COMPARE_OPTIONS.map((o) => el("option", { value: o.value, text: o.label })));
  sel.value = comparePick.value;
  sel.addEventListener("change", () => {
    comparePick.value = sel.value;
    for (const d of [...drawers]) {
      if (d.slot && d.slot.isConnected === false) drawers.delete(d);
      else d.draw();
    }
  });
  return el("label", { class: "range-control" }, [el("span", { text: "Compare" }), sel]);
}

/* ─────────── the card ─────────── */

const pctCell = (key, sevKey) => ({
  key,
  label: null,
  sevKey,
  render: (r) => document.createTextNode(formatDelta(r[key])),
  sortValue: (r) => r[key],
});

function deltaColumns(kind) {
  const k = KINDS[kind];
  const col = (c, label) => ({ ...c, label });
  return [
    {
      key: "status",
      label: "Status",
      render: (r) => (r.status ? el("span", { class: "badge band-" + (r.status === "NEW" ? "Warning" : "Offline"), text: r.status }) : document.createTextNode("")),
      sortValue: (r) => r._group,
    },
    { key: "database_name", label: "Database" },
    { key: "name", label: k.nameLabel, valueList: false },
    { key: "execution_count", label: "Execs", format: "int" },
    { key: "baseline_execution_count", label: "Before Execs", format: "int" },
    col(pctCell("execution_delta_pct", "execution_delta_sev"), "Execs Δ"),
    { key: "avg_duration_ms", label: "Avg Duration", format: "ms" },
    { key: "baseline_avg_duration_ms", label: "Before Avg Duration", format: "ms" },
    col(pctCell("duration_delta_pct", "duration_delta_sev"), "Duration Δ"),
    { key: "avg_cpu_ms", label: "Avg CPU", format: "ms" },
    { key: "baseline_avg_cpu_ms", label: "Before Avg CPU", format: "ms" },
    col(pctCell("cpu_delta_pct", "cpu_delta_sev"), "CPU Δ"),
  ];
}

/**
 * The card for a grid with Compare on. `o`: `kind`, `params` (the read's params for the current window, `top` already
 * COMPARE_TOP), `pick`, `server`, `title`, `subtitle`, `picker`, `signal`, `shell` (panelShell), `viz` (VIZ), and `plain()`, which
 * draws the grid as it is with Compare off. A read that fails, or an earlier window with nothing in it, puts the notice
 * above `plain()`'s grid.
 */
export function compareCard(o) {
  const { panel, body } = o.shell(o.title, o.subtitle, 2, KINDS[o.kind].read, null, o.picker);
  const holder = el("div", { style: "grid-column: 1 / -1" }, [panel]);
  (async () => {
    /* #6301: a narrowed window names its own end (`endMs`); otherwise the page's window ends where windowEndMs says. */
    const windowEnd = Number.isFinite(o.endMs) ? o.endMs : windowEndMs(o.server);
    const asOf = comparisonAsOf(o.pick, windowEnd);
    const [currentRes, baselineRes] = await Promise.all([
      readToolWithinKeptHistory(KINDS[o.kind].read, o.params, o.signal),
      readToolWithinKeptHistory(KINDS[o.kind].read, { ...o.params, as_of: asOf }, o.signal),
    ]);
    if (o.signal && o.signal.aborted) return;
    const plan = planComparison(o.kind, currentRes, baselineRes);
    if (plan.mode === "plain") {
      mount(holder, [noticeStrip(plan.notice), o.plain()]);
      return;
    }
    const end = windowEnd - compareShiftMs(o.pick);
    const hours = Number(o.params.hours) || 0;
    const grid = o.viz.table({ rows: plan.rows }, { rowsKey: "rows", columns: deltaColumns(o.kind), emptyText: NO_COMPARISON_DATA + "." });
    mount(body, [
      el("div", { class: "mp-metric-note", text: "Comparing against " + localTime(new Date(end - hours * 3600000).toISOString()) + " to " + localTime(new Date(end).toISOString()) + ". Rows are matched within the top " + COMPARE_TOP + " of each window." }),
      grid,
    ]);
  })();
  return holder;
}
