/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* #6293: the Query Heatmap as a colour grid, as the desktop Viewer draws it: time across, the 7 magnitude buckets down (the slowest at
   the top), each cell shaded by how many distinct queries fell in it. A metric picker (the Viewer's, minus logical reads), the cell's
   values on hover, and on a selected cell "Show Active Queries at This Time", which opens the Active Queries tab at that moment through
   the same chart-menu mechanism the line charts use (openServerTabAt). "Show as table" keeps the old table of the same cells, for a
   screen reader (the grid is a picture) and for a phone. The read is get_query_heatmap and has not changed. */

import { el, mount, axisTime, localTime } from "../util.js";
import { renderPanel, gridTable, getPanelSignal, setPanelSignal } from "../panels.js";
import { HEATMAP_METRICS, HEATMAP_SHADES, heatmapBinMinutes, buildHeatmap, heatmapDrillMs, heatmapCellText } from "./query-heatmap-model.js";

/* The picks, at module scope so the 60 s rebuild of the tab (a new card every time) keeps them. `view` is null until the reader chooses;
   until then a phone-width window opens on the table. */
/* The chart menu's own item text (charts.js CHART_MENU_LABELS.atQueries), so the two read the same. */
const AT_QUERIES_LABEL = "Show Active Queries at This Time";
const pick = { metric: HEATMAP_METRICS[0].value, view: null };

function narrowScreen() {
  return typeof window !== "undefined" && typeof window.matchMedia === "function" && window.matchMedia("(max-width: 600px)").matches;
}

function oneLine(text, max) {
  const flat = String(text || "").replace(/\s+/g, " ").trim();
  return flat.length > max ? flat.slice(0, max) + "..." : flat;
}

/**
 * The grid, its legend, the hover/selection detail line and the table, for one answer of get_query_heatmap.
 * `opts` is { server, columns, desc }: the table's columns and descriptor (`desc` is what renderPanel passed).
 */
export function renderHeatmapBody(data, opts) {
  const grid = buildHeatmap(data);
  const metric = HEATMAP_METRICS.find((m) => m.value === data.metric) || HEATMAP_METRICS[0];
  const metricWord = metric.label.replace(/ \(ms\)$/, "");
  const spansDays = grid.empty ? false : grid.columns * grid.binMs > 20 * 3600000;
  const whenOf = (ms) => localTime(new Date(ms).toISOString());

  const tableView = el("div", { class: "qh-table" }, [
    gridTable(data.cells || [], { ...(opts.desc || {}), id: "qh:" + opts.server, columns: opts.columns, emptyText: (opts.desc && opts.desc.emptyText) || "No cells." }),
  ]);

  const detailText = el("span", { class: "qh-detail-text", text: "Hover a cell for its values; click one to open Active Queries at that time." });
  const openButton = el("button", { type: "button", class: "btn qh-open", text: AT_QUERIES_LABEL });
  openButton.hidden = true;
  const detail = el("div", { class: "qh-detail", "aria-live": "polite" }, [detailText, openButton]);

  let gridView = null;
  if (!grid.empty) {
    let selected = null;
    let selectedTd = null;
    const textOf = (g, r) => {
      const top = oneLine(g.cell.top_query_text, 120);
      return heatmapCellText(whenOf(g.ms), grid.labels[r] || g.cell.bucket_label || "bucket " + r, g.count, metricWord) + (top ? "\n" + top : "");
    };
    const showSelected = () => {
      detailText.textContent = selected ? textOf(selected.g, selected.r) : "Hover a cell for its values; click one to open Active Queries at that time.";
      openButton.hidden = !selected;
    };
    openButton.addEventListener("click", () => {
      /* charts.js is loaded on the click, as charts.js itself loads pages/server.js: the tabs build without it until a reader asks for a moment. */
      if (selected) void import("../charts.js").then((m) => m.openServerTabAt(opts.server, heatmapDrillMs(grid, selected.g.ms), "queries"));
    });

    const table = el("table", {
      class: "qh-grid",
      role: "img",
      "aria-label":
        "Query Heatmap by " + metricWord + ": " + grid.cellCount + " cells across " + grid.columns + " time bins of " + grid.binMinutes +
        " minutes. A picture; use Show as table for the values.",
    });
    /* A label every 12 columns: hourly at the Viewer's 5 minute bins. */
    const tick = grid.columns < 12 ? 1 : 12;
    const head = el("tr", {}, [el("th", { class: "qh-corner" })]);
    for (let c = 0; c < grid.columns; c += tick) {
      const span = Math.min(tick, grid.columns - c);
      head.appendChild(el("th", { class: "qh-tick", colspan: String(span), text: axisTime(new Date(grid.startMs + c * grid.binMs), spansDays) }));
    }
    table.appendChild(head);
    for (let r = grid.rows.length - 1; r >= 0; r--) {
      const tr = el("tr", {}, [el("th", { class: "qh-rowhead", text: grid.labels[r] || "bucket " + r })]);
      for (let c = 0; c < grid.columns; c++) {
        const g = grid.rows[r][c];
        if (!g) {
          tr.appendChild(el("td", { class: "qh-cell qh-empty", title: whenOf(grid.startMs + c * grid.binMs) + " | " + (grid.labels[r] || r) + " | no query" }));
          continue;
        }
        const td = el("td", { class: "qh-cell qh-band-" + g.shade, "data-shade": String(g.shade), title: textOf(g, r) });
        td.addEventListener("mouseenter", () => {
          detailText.textContent = textOf(g, r);
        });
        td.addEventListener("mouseleave", showSelected);
        td.addEventListener("click", () => {
          if (selectedTd) selectedTd.className = selectedTd.className.replace(" qh-selected", "");
          selected = { g, r };
          selectedTd = td;
          td.className += " qh-selected";
          showSelected();
        });
        tr.appendChild(td);
      }
      table.appendChild(tr);
    }

    const swatches = [];
    for (let s = 1; s <= HEATMAP_SHADES; s++) swatches.push(el("span", { class: "qh-cell qh-swatch qh-band-" + s, "aria-hidden": "true" }));
    const legend = el("div", { class: "qh-legend muted" }, [
      el("span", { class: "qh-cell qh-swatch qh-empty", "aria-hidden": "true" }),
      el("span", { text: " none   1 " }),
      ...swatches,
      el("span", { text: " " + grid.max.toLocaleString("en-US") + " queries (log scale). Each column is " + grid.binMinutes + " minutes; " + metric.top + " are at the top." }),
    ]);
    gridView = el("div", { class: "qh-grid-view" }, [legend, el("div", { class: "qh-wrap" }, [table]), detail]);
  }

  const showTable = pick.view === "table" || (pick.view == null && narrowScreen()) || grid.empty;
  if (gridView) gridView.hidden = showTable;
  tableView.hidden = !showTable;
  const toggle = el("button", { type: "button", class: "btn qh-toggle", text: "Show as table", "aria-pressed": showTable ? "true" : "false" });
  toggle.addEventListener("click", () => {
    const toTable = toggle.getAttribute("aria-pressed") !== "true";
    pick.view = toTable ? "table" : "grid";
    toggle.setAttribute("aria-pressed", toTable ? "true" : "false");
    if (gridView) gridView.hidden = toTable;
    tableView.hidden = !toTable;
  });
  const truncatedNote = data.truncated
    ? el("p", { class: "muted", text: "The read returned only the most recent bins (" + data.cell_count + " cells); the oldest are cut. Pick a shorter range to see them." })
    : null;
  return el("div", { class: "qh" }, [grid.empty ? null : el("div", { class: "qh-bar" }, [toggle]), truncatedNote, gridView, tableView]);
}

function metricPicker(onPick) {
  const sel = el(
    "select",
    { class: "range-select-inline", "aria-label": "Metric" },
    HEATMAP_METRICS.map((m) => el("option", { value: m.value, text: m.label }))
  );
  sel.value = pick.metric;
  sel.addEventListener("change", () => onPick(sel.value));
  return el("label", { class: "range-control" }, [el("span", { text: "Metric" }), sel]);
}

/**
 * The Query Heatmap card of the Queries tab: the metric picker under the title, the grid (or table) under it. A metric pick draws the card
 * again with the new `metric`, aborting the read before it. `columns` are the table view's columns, `hours` and `label` the tab's range.
 */
export function queryHeatmapCard({ server, hours, label, columns, emptyText }) {
  const slot = el("div", {}, []);
  slot.style.display = "contents";
  let drawing = null;
  const draw = () => {
    if (drawing) drawing.abort();
    const mine = new AbortController();
    drawing = mine;
    const outer = getPanelSignal();
    if (outer) outer.addEventListener("abort", () => mine.abort(), { once: true });
    const bin = heatmapBinMinutes(hours);
    /* Defaults are left out so the plain request is the one the table always sent: 5 minute bins, the duration metric. */
    const params = { server, hours, limit: 1000 };
    if (bin !== 5) params.bucket_minutes = bin;
    if (pick.metric !== "duration") params.metric = pick.metric;
    const control = el("div", { class: "picker-row" }, [
      metricPicker((value) => {
        pick.metric = value;
        draw();
      }),
    ]);
    setPanelSignal(mine.signal);
    const card = renderPanel({
      title: "Query Heatmap",
      subtitle: label,
      read: "get_query_heatmap",
      params,
      /* viz stays "table": the page's window note, source wording and database chip key off it. `render` draws the grid in its place. */
      viz: "table",
      render: (data, desc) => renderHeatmapBody(data, { server, columns, desc }),
      emptyText,
      span: 2,
      control,
    });
    setPanelSignal(outer);
    mount(slot, [card]);
  };
  draw();
  return slot;
}
