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
  emptyStrip,
  noticeStrip,
  readTool,
  apiGet,
  buildQuery,
  getPath,
  applyFormat,
  bandClass,
  sevClass,
} from "./util.js";
import { renderLineChart, SERIES_COLORS } from "./charts.js";

/**
 * Build a panel node. It returns immediately with a loading strip and fills itself once the fetch resolves,
 * mapping the three API response kinds (data / empty envelope / error) to the right UI.
 */
export function renderPanel(desc) {
  const body = el("div", { class: "panel-body" }, [loadingStrip()]);
  const panel = el("div", { class: "panel card" + (desc.span === 2 ? " span-2" : "") }, [
    el("h3", {}, [desc.title, desc.subtitle ? el("span", { class: "panel-sub", text: " " + desc.subtitle }) : null]),
    body,
  ]);
  loadPanel(desc, body);
  return panel;
}

async function loadPanel(desc, body) {
  const res = desc.read ? await readTool(desc.read, desc.params) : await apiGet(desc.path + buildQuery(desc.params));

  if (res.kind === "error") {
    /* A window wider than a read can serve comes back as a raw "hours_back value 'N' exceeds maximum of M
       hours (D days)..." validation string. That is a range choice, not a fault: render it as a notice naming
       the window this view keeps, rather than a red error carrying the API's own wording (#2780). */
    const overRange = /exceeds maximum of (\d+) hours/.exec(res.message);
    if (overRange) {
      const maxHours = Number(overRange[1]);
      const days = Math.round(maxHours / 24);
      mount(body, noticeStrip(
        "This view keeps up to " + maxHours + " hours (~" + days + " day" + (days === 1 ? "" : "s") +
        ") of history — pick a shorter range."));
      return;
    }
    mount(body, errorStrip(res.message));
    return;
  }
  if (res.kind === "empty") {
    mount(body, emptyStrip(res.message));
    return;
  }

  const render = VIZ[desc.viz];
  if (!render) {
    mount(body, errorStrip("Unknown visualization: " + desc.viz));
    return;
  }
  try {
    mount(body, render(res.data, desc));
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

/* table: desc = { rowsKey, columns:[{key,label,format,align,wrap,mono,sevKey,statusSev}] } */
function vizTable(data, desc) {
  const cols = Array.isArray(desc.columns) ? desc.columns : [];
  if (!cols.length) return emptyStrip(NO_FIELDS_MSG);
  const rows = getPath(data, desc.rowsKey) || [];
  if (!rows.length) return emptyStrip(desc.emptyText || "No rows in this window.");

  const head = el(
    "tr",
    {},
    cols.map((c) => el("th", { text: c.label, class: isNumericCol(c) ? "num" : null }))
  );
  const bodyRows = rows.map((row) => el("tr", {}, cols.map((c) => cell(row, c))));

  return el("div", { class: "table-wrap" }, [
    el("table", { class: "data" }, [el("thead", {}, [head]), el("tbody", {}, bodyRows)]),
  ]);
}

function isNumericCol(c) {
  return c.align === "right" || ["int", "num1", "num2", "ms", "mb", "pct"].includes(c.format);
}

function cell(row, c) {
  /* A column may supply a custom cell renderer (row) -> Node — used for the alert status/detail columns and the
     query-text expander. It owns its own content; wrap/mono classes still apply if the column asks for them. */
  if (typeof c.render === "function") {
    const rcls = [];
    if (c.wrap) rcls.push("wrap");
    if (c.mono) rcls.push("mono");
    return el("td", { class: rcls.join(" ") || null }, [c.render(row)]);
  }
  const raw = getPath(row, c.key);
  const cls = [];
  if (isNumericCol(c)) cls.push("num");
  if (c.wrap) cls.push("wrap");
  if (c.mono) cls.push("mono");
  if (c.sevKey) cls.push(sevClass(getPath(row, c.sevKey)));
  if (c.statusSev) cls.push(sevClass(statusToSev(raw)));
  const text = c.format ? applyFormat(c.format, raw) : raw == null || raw === "" ? "—" : String(raw);
  return el("td", { class: cls.join(" ") || null, text });
}

/* stat: desc = { stats:[{key,label,format,small?,sev?}], emptyText? } over the tool's top-level object. A stat
   descriptor may carry a PRE-COMPUTED severity (`sev`/`severity`, e.g. "Critical") — colored here from that hint
   only (R1: the browser never re-derives a band); absent the hint the value keeps the default color. */
function vizStat(data, desc) {
  const stats = Array.isArray(desc.stats) ? desc.stats : [];
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
      return el("div", { class: "stat" }, [
        el("div", { class: valueClass, text: applyFormat(s.format, getPath(data, s.key)) }),
        el("div", { class: "label", text: s.label }),
      ]);
    })
  );
}

/* line: desc = { rowsKey, xKey, series:[{key,label,color?}], format?, emptyText? } */
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
  return renderLineChart({ points, xKey: desc.xKey, series, formatValue, clampMax, unit: desc.unit ?? null });
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
