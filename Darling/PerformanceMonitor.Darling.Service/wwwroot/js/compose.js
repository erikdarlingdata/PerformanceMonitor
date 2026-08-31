/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The Custom Views v2 render path (#1563) — the twin of panels.js's v1 renderPanel, for COMPOSED (`source`) panels.
 * A composed panel is not a GET read; it is a spec {source, measure|ratio, aggregate, unit, timeBucket|topN, filters,
 * groupBy, viz} that the composer previews and a saved view renders by POSTing to /api/compose/run (with the view's
 * server/time/variable scope) and charting the returned {sql, rows}. The compiler emits three row shapes, keyed by
 * mode (ComposeCompiler): TIME SERIES `{bucket, <groupDim>…, value}` (bucket ASC), RANKED `{<groupDim>…, value}`
 * (value DESC, LIMIT topN), and SCALAR `{value}` (one row). This module shapes those rows for charts.js and formats
 * every value in the panel's chosen unit (the value is already scaled server-side, so we format — never re-scale).
 *
 * R4 (XSS): every value — dimension values, series/category labels, the compiled SQL — reaches the DOM through
 * util.el()'s text/textContent path (never innerHTML), so an untrusted wait type / object name / server name renders
 * inert. The chart SVG lives entirely in charts.js (one SVG_NS occurrence, the air-gap allowlist); this file has no SVG.
 */

import { el, mount, loadingStrip, errorStrip, emptyStrip, disclosure, fmtInt, fmtNum, apiSend, noticeStrip } from "./util.js";
import { renderLineChart, renderBarChart, renderPieChart, renderScatterChart, CATEGORICAL_COLORS } from "./charts.js";
import { navigateServer } from "./panels.js";
import { getCatalog } from "./views-api.js";

/** The most series a time chart draws before pooling the rest into a "+N more" note (readability + palette size). */
const MAX_SERIES = 8;

/* ─────────────────────────── panel card + fill ─────────────────────────── */

/**
 * A composed panel as a self-filling `.panel.card` (the saved-view grid's unit) — matches panels.js's renderPanel
 * shape (title + span-2 + a body that shows a loading strip, then the chart / a state). `scope` is the view-level
 * run context {server, hours, variables, values}; flipping it and re-rendering re-scopes every panel at once.
 */
export function renderComposedPanelCard(panelSpec, scope) {
  const body = el("div", { class: "panel-body" }, [loadingStrip()]);
  const panel = el("div", { class: "panel card" + (panelSpec.span === 2 ? " span-2" : "") }, [
    el("h3", {}, [panelSpec.title || measureLabel(panelSpec), pinBadge(panelSpec)]),
    body,
  ]);
  driveComposedPanel(body, panelSpec, scope);
  return panel;
}

/** A panel spec's per-cell window pin (#2735), normalized: {windowStart, windowEnd} (absolute) or {hours}
 *  (relative), or null when the spec carries no usable `range` (= live: it follows the view scope). Kept
 *  tolerant on purpose — this is the READ path, and a malformed stored range must degrade to "live", never
 *  crash the card (the write path already rejects one). */
export function cellRange(spec) {
  const r = spec && spec.range;
  if (!r || typeof r !== "object" || Array.isArray(r)) return null;
  if (typeof r.windowStart === "string" && typeof r.windowEnd === "string" && r.windowStart && r.windowEnd) {
    return { windowStart: r.windowStart, windowEnd: r.windowEnd };
  }
  if (typeof r.hours === "number" && r.hours >= 1) return { hours: r.hours };
  return null;
}

/**
 * The pinned-window badge (#2735): shown in the card header of any panel whose spec carries its own `range`,
 * so it is always visible WHICH cells are pinned to their own window and which are live on the scope bar —
 * without it, the scope bar's time range would silently lie for pinned cells. Server scope still applies to
 * pinned cells (the pin is a window, not a server), which the tooltip spells out.
 */
function pinBadge(panelSpec) {
  const range = cellRange(panelSpec);
  if (!range) return null;
  const label = pinRangeLabel(range);
  return el("span", {
    class: "pin-badge",
    title:
      "This cell is pinned to its own time window (" + label + "), so the scope bar's time range does not " +
      "apply to it. The server scope still does.",
    text: "Pinned: " + label,
  });
}

/** A normalized pin's display label ("last 2 days" / "8/21/26, 12:00 AM → 8/22/26, 12:00 AM") — shared with
 *  the notebook editor's pin strip so the two surfaces describe a pin identically. */
export function pinRangeLabel(range) {
  return range.windowStart ? pinWindowLabel(range.windowStart, range.windowEnd) : "last " + pinHoursLabel(range.hours);
}

/** The badge's window text for an absolute pin — compact local date+time, "start → end". */
function pinWindowLabel(startIso, endIso) {
  const from = new Date(startIso);
  const to = new Date(endIso);
  if (isNaN(from.getTime()) || isNaN(to.getTime())) return "custom window";
  const opts = { dateStyle: "short", timeStyle: "short" };
  return from.toLocaleString(undefined, opts) + " → " + to.toLocaleString(undefined, opts);
}

/** The badge's window text for a relative pin (whole days render as days, mirroring the range picker's labels). */
function pinHoursLabel(hours) {
  if (hours % 24 === 0 && hours >= 24) {
    const d = hours / 24;
    return d + (d === 1 ? " day" : " days");
  }
  return hours + (hours === 1 ? " hour" : " hours");
}

/**
 * The per-card drill controller (design D6): runs the panel, and — when the viewer clicks a categorical mark —
 * re-runs it with a TRANSIENT drill filter pinning that mark's group value(s). The drill is view-state only: it is
 * never persisted (the stored definition renders verbatim), and a "clear" chip pops back to the base spec. Each new
 * selection REPLACES the drill (a simple "you are viewing X" model), so drilling never stacks into a dead end.
 */
function driveComposedPanel(body, panelSpec, scope) {
  let drill = null; // { keys: [{dimension, value}] } or null
  let zoom = null; // { startIso, endIso } or null (#1606 brush-zoom — view-state only, like the drill)
  function onDrill(next) {
    drill = next && Array.isArray(next.keys) && next.keys.length ? next : null;
    run();
  }
  function onZoomChange(next) {
    zoom = next && next.startIso && next.endIso ? next : null;
    run();
  }
  function run() {
    const spec = drill ? withDrillFilters(panelSpec, drill) : panelSpec;
    renderComposedInto(body, spec, scope, { drill, onDrill, zoom, onZoomChange });
  }
  run();
}

/** A base panel spec + the drill's pinned filters (design D6): each key becomes an `eq` filter appended to the
 *  panel's own filters. The value is a scalar string — the run endpoint treats it as a single literal (no comma
 *  splitting server-side), so a category containing a comma pins correctly. Never mutates the base spec. */
function withDrillFilters(spec, drill) {
  const extra = drill.keys.map((k) => ({ dimension: k.dimension, op: "eq", value: k.value }));
  return { ...spec, filters: [...(Array.isArray(spec.filters) ? spec.filters : []), ...extra] };
}

/** Run a composed panel and fill `body` with the chart (or the empty/error state). Used by the card + the live
 *  preview. `opts` (design D5/D6): {drill, onDrill} drive the transient drill-down chip + mark clicks. */
export async function renderComposedInto(body, panelSpec, scope, opts = {}) {
  mount(body, loadingStrip());
  const res = await runCompose(panelSpec, scope, opts.zoom);
  if (res.kind === "error") {
    /* Keep the drill AND zoom chips (with their clears) above a failed run so the viewer can always pop
       back — a historical zoom can legitimately land on a window this store can't serve. */
    const nodes = [errorStrip(res.message || "Could not run this panel.")];
    if (opts.zoom && opts.onZoomChange) nodes.unshift(zoomChip(opts.zoom, opts.onZoomChange));
    if (opts.drill && opts.onDrill) nodes.unshift(drillChip(opts.drill, opts.onDrill));
    mount(body, nodes);
    return;
  }
  const data = res.data || {};
  /* Resolve annotation display names off the catalog (data-driven, not hardcoded) only when there are markers to
     draw; getCatalog is a cached promise, so this is effectively free after the first page load. */
  let annotationMeta = null;
  if (isTimeSeriesViz(panelSpec.viz) && Array.isArray(data.annotations) && data.annotations.length) {
    annotationMeta = await annotationMetaMap().catch(() => null);
  }
  try {
    const nodes = [renderComposedResult(data, panelSpec, { ...opts, annotationMeta })];
    /* The run endpoint's partial-window notice (#1665): the chosen tier could not retain the whole
       requested window on this store — good data, honestly caveated, above the chart. */
    if (typeof data.notice === "string" && data.notice) nodes.unshift(noticeStrip(data.notice));
    mount(body, nodes);
  } catch (e) {
    mount(body, errorStrip("Could not render this panel: " + (e && e.message ? e.message : String(e))));
  }
}

/** POST the composed panel to /api/compose/run with the view scope; returns the apiSend result ({sql, rows} on data). */
export function runCompose(panelSpec, scope, zoom = null) {
  return apiSend("POST", "/api/compose/run", buildRunBody(panelSpec, scope, zoom));
}

/** Build the /api/compose/run body from a panel spec + scope (a per-panel `hours` or `range` overrides the
 *  view range). */
function buildRunBody(panelSpec, scope, zoom = null) {
  const body = { panel: toRunPanel(panelSpec) };
  /* Brush-zoom (#1606): an absolute window wins over `hours` server-side; hours still rides along
     untouched so clearing the zoom needs no special casing. */
  const zoomed = !!(zoom && zoom.startIso && zoom.endIso);
  if (zoomed) {
    body.windowStart = zoom.startIso;
    body.windowEnd = zoom.endIso;
  }
  const s = scope || {};
  if (Array.isArray(s.variables) && s.variables.length) body.variables = s.variables;
  if (s.values && Object.keys(s.values).length) body.values = s.values;
  if (s.server != null) body.server = s.server;
  if (s.hours != null) body.hours = s.hours;
  if (panelSpec.hours != null) body.hours = panelSpec.hours;
  /* The per-cell window pin (#2735): a notebook panel cell's own `range` wins over the view scope (and over
     the legacy per-panel `hours` above). A transient brush-zoom still wins over the pin — it is the viewer's
     explicit request on THIS panel, and clearing it pops back to the pinned window, exactly the zoom-over-
     hours precedence a live panel has. */
  const range = cellRange(panelSpec);
  if (range) {
    if (range.windowStart) {
      if (!zoomed) {
        body.windowStart = range.windowStart;
        body.windowEnd = range.windowEnd;
      }
    } else {
      body.hours = range.hours;
    }
  }
  return body;
}

/** The exact compose fields the run endpoint reads — never the frontend-only keys (kind/title/span/hours). */
function toRunPanel(p) {
  const out = { source: p.source, viz: p.viz };
  if (p.measure != null) out.measure = p.measure;
  if (p.ratio != null) out.ratio = p.ratio;
  if (p.aggregate != null && p.aggregate !== "") out.aggregate = p.aggregate;
  if (p.unit != null && p.unit !== "") out.unit = p.unit;
  if (p.timeBucket != null && p.timeBucket !== "" && p.timeBucket !== "none") out.timeBucket = p.timeBucket;
  if (p.topN != null && p.topN !== "") out.topN = p.topN;
  if (Array.isArray(p.filters) && p.filters.length) out.filters = p.filters;
  if (Array.isArray(p.groupBy) && p.groupBy.length) out.groupBy = p.groupBy;
  /* The second measure (#1606) — passed through verbatim; the run endpoint validates coherence. */
  if (p.overlay && typeof p.overlay === "object" && p.overlay.measure) out.overlay = p.overlay;
  /* Event-annotation sources (design D5) ride only on a time-series spec — the run endpoint rejects them on a
     ranked/scalar panel — so gate on the emitted timeBucket, mirroring the server's rule. */
  if (out.timeBucket && Array.isArray(p.annotations) && p.annotations.length) out.annotations = p.annotations;
  return out;
}

/* ─────────────────────────── result -> chart ─────────────────────────── */

/**
 * Shape a run result ({sql, rows}) into the panel body: the chart for the panel's viz, any partial-fleet / capped
 * note, and a folded "compiled SQL" disclosure (read-only transparency). Empty rows are the honest NO-DATA state
 * (not an error). Returns an array of nodes for the caller to mount.
 */
export function renderComposedResult(result, panelSpec, opts = {}) {
  const rows = Array.isArray(result.rows) ? result.rows : [];
  const nodes = [];

  /* The transient drill-down chip (design D6) sits above the chart whenever a drill is active — with the pinned
     filter(s), a clear, and (for a server dimension) a jump to that server's detail page. The zoom chip (#1606)
     does the same for a brushed window — BOTH must survive the empty state (a historical zoom legitimately
     returns one coarse bucket or nothing, and the viewer must be able to pop back out). */
  const drillNode = opts.drill && opts.onDrill ? drillChip(opts.drill, opts.onDrill) : null;
  const zoomNode = opts.zoom && opts.onZoomChange ? zoomChip(opts.zoom, opts.onZoomChange) : null;

  if (!rows.length) {
    if (drillNode) nodes.push(drillNode);
    if (zoomNode) nodes.push(zoomNode);
    nodes.push(emptyStrip("No data in this window."));
    nodes.push(sqlDisclosure(result.sql));
    return nodes;
  }

  const unit = panelSpec.unit || "";
  const fmt = (v) => formatComposedValue(v, unit);
  const groupDims = Array.isArray(panelSpec.groupBy) ? panelSpec.groupBy : [];
  /* Render-only reference lines (design D3), in the panel's unit — the chart draws each in-domain value. */
  const thresholds = Array.isArray(panelSpec.thresholds) ? panelSpec.thresholds : null;
  /* A mark-click drills IN by pinning that mark's group value(s) as a filter (design D6); only offered when the
     panel groups by real declared dimensions (the chart shapers attach `drill` per mark). */
  const onSelect = opts.onDrill ? (keys) => opts.onDrill(keys && keys.length ? { keys } : null) : null;

  if (drillNode) nodes.push(drillNode);
  if (zoomNode) nodes.push(zoomNode);

  /* Brush-zoom (#1606): available on every time-series viz; the brushed range re-RUNS the panel on that
     absolute window (bucket resolution + tier routing + the partial-window notice all stay honest). */
  const onZoom = opts.onZoomChange
    ? (fromMs, toMs) => opts.onZoomChange({ startIso: new Date(fromMs).toISOString(), endIso: new Date(toMs).toISOString() })
    : null;

  /* Dual-axis overlay (#1606): an ungrouped line/area with an overlay draws the second measure against its
     own right-hand axis; the rows carry it as `value2`. */
  const overlaySeries =
    panelSpec.overlay && !groupDims.length && (panelSpec.viz === "line" || panelSpec.viz === "area")
      ? {
          key: "value2",
          label: humanize(panelSpec.overlay.measure || "overlay"),
          color: OVERLAY_COLOR,
          formatValue: (v) => formatComposedValue(v, panelSpec.overlay.unit || ""),
          unit: axisUnit(panelSpec.overlay.unit || ""),
        }
      : null;

  switch (panelSpec.viz) {
    case "line":
    case "area":
    case "stacked":
    case "stacked-bar": {
      const { points, series, hidden } = pivotTimeSeries(rows, groupDims, measureLabel(panelSpec));
      nodes.push(
        renderLineChart({
          points,
          xKey: "bucket",
          series,
          formatValue: fmt,
          unit: axisUnit(unit),
          clampMax: unit === "percent" ? 100 : null,
          mode: panelSpec.viz,
          thresholds,
          annotations: annotationLayers(result.annotations, opts.annotationMeta),
          onSelect,
          series2: overlaySeries,
          onZoom,
        })
      );
      if (hidden > 0) nodes.push(el("div", { class: "chart-note", text: `+${hidden} more series not shown.` }));
      break;
    }
    case "bar":
      nodes.push(renderBarChart({ items: rankedItems(rows, groupDims), formatValue: fmt, unit: axisUnit(unit), thresholds, onSelect }));
      break;
    case "pie":
      nodes.push(renderPieChart({ items: rankedItems(rows, groupDims), formatValue: fmt, onSelect }));
      break;
    case "scatter": {
      /* Two-measure scatter (#1606): x = the primary (ranked) value, y = the overlay's value2, one point
         per group. Formatters/captions follow each measure's own unit. */
      const overlayUnit = panelSpec.overlay ? panelSpec.overlay.unit || "" : "";
      nodes.push(
        renderScatterChart({
          items: scatterItems(rows, groupDims),
          formatX: fmt,
          formatY: (v) => formatComposedValue(v, overlayUnit),
          unitX: axisUnit(unit),
          unitY: axisUnit(overlayUnit),
          onSelect,
        })
      );
      break;
    }
    case "stat":
      nodes.push(renderScalar(rows, panelSpec, fmt));
      break;
    case "table":
    default:
      nodes.push(renderComposedTable(rows, unit));
      break;
  }

  const partial = partialFleetNote(groupDims);
  if (partial) nodes.push(partial);
  nodes.push(sqlDisclosure(result.sql));
  return nodes;
}

/* ─────────────────────────── row shaping ─────────────────────────── */

/**
 * Pivot TIME SERIES rows into charts.js's {points, series} shape. With no group-by it is one series (the measure);
 * with a group-by, one series per distinct dimension combination, values re-keyed per bucket. Series are capped to
 * MAX_SERIES by total magnitude (the rest reported as `hidden`) so a high-cardinality group stays legible.
 */
export function pivotTimeSeries(rows, groupDims, label) {
  if (!groupDims.length) {
    /* value2 (#1606) rides into each point so a dual-axis overlay can read it; absent = undefined = inert. */
    const points = rows.map((r) => ({ bucket: r.bucket, value: numOrNull(r.value), value2: numOrNull(r.value2) }));
    return { points, series: [{ key: "value", label, color: CATEGORICAL_COLORS[0] }], hidden: 0 };
  }

  const totals = new Map(); // seriesKey -> running total (for the top-N cut)
  const labels = new Map(); // seriesKey -> display label
  const drills = new Map(); // seriesKey -> drill keys [{dimension, value}] (design D6)
  const byBucket = new Map(); // bucket -> point object
  const order = []; // seriesKey first-seen order (stable colors)

  for (const r of rows) {
    /* "s:"-prefixed so a group value equal to "bucket"/"value" can never collide with a point object's own keys. */
    const key = "s:" + JSON.stringify(groupDims.map((d) => (r[d] == null ? "" : String(r[d]))));
    if (!labels.has(key)) {
      labels.set(key, comboLabel(r, groupDims));
      drills.set(key, buildDrillKeys(r, groupDims));
      totals.set(key, 0);
      order.push(key);
    }
    const v = numOrNull(r.value);
    totals.set(key, totals.get(key) + (v || 0));
    let pt = byBucket.get(r.bucket);
    if (!pt) {
      pt = { bucket: r.bucket };
      byBucket.set(r.bucket, pt);
    }
    pt[key] = v;
  }

  let keptKeys = order;
  let hidden = 0;
  if (order.length > MAX_SERIES) {
    keptKeys = [...order].sort((a, b) => totals.get(b) - totals.get(a)).slice(0, MAX_SERIES);
    hidden = order.length - keptKeys.length;
  }

  const series = keptKeys.map((key, i) => ({
    key,
    label: labels.get(key),
    color: CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length],
    drill: drills.get(key),
  }));
  return { points: [...byBucket.values()], series, hidden };
}

/** Shape RANKED rows into charts.js's items[] ({label, value, color, drill}); the category is the group-by combo,
 *  and `drill` (design D6) carries the row's declared group-dimension values so a click can filter to that category. */
export function rankedItems(rows, groupDims) {
  const dims = groupDims.length ? groupDims : otherColumns(rows[0]);
  return rows.map((r, i) => ({
    label: dims.length ? comboLabel(r, dims) : "value",
    value: numOrNull(r.value),
    color: CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length],
    /* Only DECLARED group-by dims are drillable — a fallback otherColumns() dim isn't a catalog dimension the run
       endpoint would accept as a filter, so it stays non-drillable. */
    drill: groupDims.length ? buildDrillKeys(r, groupDims) : null,
  }));
}

/** A group-by combo's display label — the dimension values joined by " / " (a null/empty value reads as "(none)"). */
function comboLabel(row, dims) {
  return dims
    .map((d) => {
      const v = row[d];
      return v == null || v === "" ? "(none)" : String(v);
    })
    .join(" / ");
}

/** The non-`value` columns of a row (the fallback category keys when a ranked result carries no declared group-by).
 *  `value2` (#1606, the overlay's column) is excluded the same way — it is a measure, never a category. */
function otherColumns(row) {
  return row ? Object.keys(row).filter((k) => k !== "value" && k !== "bucket" && k !== "value2") : [];
}

/** Shape scatter rows (#1606) into charts.js's items[]: x = the primary value, y = the overlay's value2, one
 *  point per ranked group; `drill` carries the declared group-dimension values exactly like rankedItems. */
function scatterItems(rows, groupDims) {
  return rows.map((r) => ({
    label: groupDims.length ? comboLabel(r, groupDims) : "value",
    x: numOrNull(r.value),
    y: numOrNull(r.value2),
    drill: groupDims.length ? buildDrillKeys(r, groupDims) : null,
  }));
}

/* ─────────────────────────── scalar + table renderers ─────────────────────────── */

/** A single-aggregate SCALAR result as one big stat tile (the same .stats/.stat markup the v1 stat viz uses). */
function renderScalar(rows, panelSpec, fmt) {
  const value = rows[0] ? rows[0].value : null;
  return el("div", { class: "stats" }, [
    el("div", { class: "stat" }, [
      el("div", { class: "value", text: fmt(value) }),
      el("div", { class: "label", text: panelSpec.title || measureLabel(panelSpec) }),
    ]),
  ]);
}

/** Any result as a table of its returned columns — bucket localized, value formatted in the unit, dims as text. */
function renderComposedTable(rows, unit) {
  const cols = Object.keys(rows[0] || {});
  if (!cols.length) return emptyStrip("No columns to show.");
  const head = el(
    "tr",
    {},
    cols.map((c) => el("th", { text: columnLabel(c), class: c === "value" ? "num" : null }))
  );
  const bodyRows = rows.map((row) =>
    el(
      "tr",
      {},
      cols.map((c) => {
        if (c === "value") return el("td", { class: "num", text: formatComposedValue(row[c], unit) });
        if (c === "bucket") return el("td", { text: localBucket(row[c]) });
        const v = row[c];
        return el("td", { text: v == null || v === "" ? "—" : String(v) });
      })
    )
  );
  return el("div", { class: "table-wrap" }, [
    el("table", { class: "data" }, [el("thead", {}, [head]), el("tbody", {}, bodyRows)]),
  ]);
}

/** A table header for a result column: "Value" gains its unit, "bucket" -> "Time", a dim name is humanized. */
function columnLabel(c) {
  if (c === "bucket") return "Time";
  if (c === "value") return "Value";
  return humanize(c);
}

/** Localize a naive-UTC bucket timestamp for the table's Time column. */
function localBucket(s) {
  if (!s || typeof s !== "string") return "—";
  const hasZone = /[zZ]$|[+\-]\d\d:?\d\d$/.test(s);
  const d = new Date(hasZone ? s : s + "Z");
  return isNaN(d.getTime()) ? String(s) : d.toLocaleString();
}

/* ─────────────────────────── formatting ─────────────────────────── */

/**
 * Format a composed value already scaled to `unit` (the compiler did the conversion, so we FORMAT, never re-scale):
 * percent -> "n.n%", count -> integer, ratio -> a 0..1 fraction, bytes/pages -> integer + unit, any other unit
 * (ms/s/min/kb/mb/gb/…) -> "n.n unit", and a bare number when there is no unit.
 */
export function formatComposedValue(v, unit) {
  if (v == null || isNaN(v)) return "—";
  const n = Number(v);
  if (unit === "percent") return fmtNum(n, 1) + "%";
  if (unit === "count") return fmtInt(n);
  if (unit === "ratio") return fmtNum(n, 3);
  if (unit === "bytes" || unit === "pages") return fmtInt(n) + " " + unit;
  if (unit) return fmtNum(n, 1) + " " + unit;
  return fmtNum(n, 1);
}

/** The short y-axis unit caption: "%" for percent, the bare unit for a real unit, null for count/ratio (unitless-ish). */
function axisUnit(unit) {
  if (unit === "percent") return "%";
  if (!unit || unit === "count" || unit === "ratio") return null;
  return unit;
}

/* ─────────────────────────── small helpers ─────────────────────────── */

/** The measure/ratio's catalog key as a fallback panel title (the composer usually sets a real title). */
function measureLabel(panelSpec) {
  return panelSpec.title || panelSpec.measure || panelSpec.ratio || "Metric";
}

/** snake_case -> Title Case, mirroring derive.js's humanizeKey (kept local so this module imports nothing DOM-y). */
function humanize(key) {
  return String(key == null ? "" : key)
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/** A number or null (never NaN) — the chart readers treat null as a gap. */
function numOrNull(v) {
  if (v == null || v === "") return null;
  const n = Number(v);
  return isNaN(n) ? null : n;
}

/* ─────────────────────────── D5 annotations + D6 drill-down ─────────────────────────── */

/** Whether a viz is a time-series chart (the only shape event annotations overlay). */
function isTimeSeriesViz(viz) {
  return viz === "line" || viz === "area" || viz === "stacked" || viz === "stacked-bar";
}

/** The drill keys for a row (design D6): one {dimension, value} per declared group dimension, so a click pins that
 *  exact category/combo. Returns null when any dimension value is null/empty (a "(none)" bucket can't be filtered to). */
function buildDrillKeys(row, dims) {
  if (!Array.isArray(dims) || !dims.length) return null;
  const keys = [];
  for (const d of dims) {
    const v = row[d];
    if (v == null || v === "") return null;
    keys.push({ dimension: d, value: String(v) });
  }
  return keys.length ? keys : null;
}

/**
 * Reshape the run response's `annotations` ([{source, events:[{ts,label}]}], in requested order) into the chart's
 * layer shape ([{source, displayName, events}]), resolving each source's display name off the catalog map (falling
 * back to a humanized key). Sources with no events in the window are dropped so the annotation key stays meaningful.
 */
function annotationLayers(resultAnnotations, meta) {
  if (!Array.isArray(resultAnnotations) || !resultAnnotations.length) return null;
  const layers = [];
  for (const a of resultAnnotations) {
    const events = Array.isArray(a.events) ? a.events : [];
    if (!events.length) continue;
    const m = meta && typeof meta.get === "function" ? meta.get(a.source) : null;
    layers.push({ source: a.source, displayName: (m && m.displayName) || humanize(a.source), events });
  }
  return layers.length ? layers : null;
}

/** A cached key -> {displayName} map of the catalog's annotation sources (design D5), so the chart legend/tooltip
 *  use the same display names the composer offered. getCatalog() is itself cached, so this resolves once per load. */
let _annotationMetaPromise = null;
function annotationMetaMap() {
  if (!_annotationMetaPromise) {
    _annotationMetaPromise = getCatalog().then((cat) => {
      const map = new Map();
      for (const a of ((cat && cat.compose) || {}).annotationSources || []) {
        if (a && a.key) map.set(a.key, { displayName: a.displayName || a.key });
      }
      return map;
    });
  }
  return _annotationMetaPromise;
}

/**
 * The transient drill-down chip (design D6): the pinned filter(s) as text, a "clear" that pops back to the base
 * spec, and — when a pinned dimension is `server` — a jump to that server's existing detail page. Every value is
 * textContent (R4/XSS); the chip is view-state only (the stored definition is never touched).
 */
/** The dual-axis overlay's FIXED series color (#1606) — the tail of CATEGORICAL_COLORS, so it can never
 *  collide with the primary series' CATEGORICAL_COLORS[0] on an ungrouped chart. */
const OVERLAY_COLOR = "#ba68c8";

/**
 * The transient zoom chip (#1606): the brushed window as local time, plus a clear that re-runs the panel on
 * its original window. View-state only — the stored definition is never touched (the drill-chip idiom).
 */
function zoomChip(zoom, onZoomChange) {
  const from = new Date(zoom.startIso);
  const to = new Date(zoom.endIso);
  const label = isNaN(from.getTime()) || isNaN(to.getTime())
    ? "custom window"
    : from.toLocaleString() + " → " + to.toLocaleString();
  const chip = el("div", { class: "drill-chip zoom-chip" }, [
    el("span", { class: "drill-label", text: "Zoomed: " + label }),
  ]);
  const clear = el("button", { class: "btn small drill-clear", type: "button", title: "Reset zoom", "aria-label": "Reset zoom", text: "×" });
  clear.addEventListener("click", () => onZoomChange(null));
  chip.appendChild(clear);
  return chip;
}

function drillChip(drill, onDrill) {
  const parts = drill.keys.map((k) => humanize(k.dimension) + " = " + k.value);
  const chip = el("div", { class: "drill-chip" }, [
    el("span", { class: "drill-label", text: "Filtered: " + parts.join(", ") }),
  ]);
  const serverKey = drill.keys.find((k) => k.dimension === "server");
  if (serverKey) {
    const open = el("button", { class: "btn small drill-open", type: "button", text: "Open server page →" });
    open.addEventListener("click", () => navigateServer(serverKey.value));
    chip.appendChild(open);
  }
  const clear = el("button", { class: "btn small drill-clear", type: "button", title: "Clear drill-down", "aria-label": "Clear drill-down", text: "×" });
  clear.addEventListener("click", () => onDrill(null));
  chip.appendChild(clear);
  return chip;
}

/** A soft note when a panel groups by server: a fleet-wide group only lists servers that collect the measure. */
function partialFleetNote(groupDims) {
  if (!groupDims.includes("server")) return null;
  return el("div", { class: "chart-note", text: "Only servers collecting this measure appear." });
}

/** The folded, read-only "compiled SQL" disclosure (transparency; progressive disclosure keeps it out of the way). */
function sqlDisclosure(sql) {
  if (!sql) return null;
  return disclosure("View compiled SQL", el("pre", { class: "compiled-sql", text: sql }), { max: 40 });
}
