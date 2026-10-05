/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Dependency-free inline SVG line charts for Darling Web (#1562) — multi-series polylines, theme colors via
 * CSS vars / the caller's series color, and a mousemove tooltip. No chart library, no build step, no remote
 * anything (the WPF viewer uses ScottPlot; this is the honest phase-1 browser equivalent).
 *
 * NOTE (air-gap): SVG_NS is the W3C XML *namespace identifier* required by createElementNS — it is never
 * dereferenced over the network. The self-containment test (DarlingWebSelfContainmentTests) allowlists exactly
 * this string; keep it as the single occurrence in wwwroot.
 */

import { el, mount, parseUtc, axisTime, emptyStrip } from "./util.js";

const SVG_NS = "http://www.w3.org/2000/svg";

/* viewBox geometry — the SVG scales to its container width via CSS (width:100%, height:auto). */
const W = 1000;
const H = 320;
/* Top margin leaves headroom for the y-axis unit caption to sit fully clear of the top tick's label. */
const M = { l: 58, r: 16, t: 26, b: 30 };
const PLOT_H = H - M.t - M.b;
const Y_TICKS = 4;
let clipSeq = 0;

function svg(tag, attrs) {
  const node = document.createElementNS(SVG_NS, tag);
  if (attrs) for (const [k, v] of Object.entries(attrs)) if (v != null) node.setAttribute(k, String(v));
  return node;
}

/**
 * Render a multi-series time chart into a returned `.chart` node.
 * spec: { points, xKey, series:[{key,label,color}], formatValue?, clampMax?, unit?, mode?, thresholds? }
 *   points     — array of row objects; each row[xKey] is a naive-UTC ISO string, each row[series.key] a number.
 *   clampMax   — cap the y-axis top at this value (percentage charts pass 100 so the domain never exceeds 100).
 *   unit       — a short y-axis unit caption ("%", "ms", "ms/s", ...).
 *   mode       — "line" (default; plain polylines), "area" (each series filled to the baseline), "stacked"
 *                (series stacked on one another — the "parts of a whole over time" view; the y-domain is the
 *                per-bucket stack SUM), or "stacked-bar" (the same stack, drawn as a vertical bar per bucket
 *                segmented by series). "line"/absent is byte-for-byte the original behavior.
 *   thresholds — optional array of render-only reference-line values in the chart's unit (design D3); each
 *                in-domain value draws a dashed guide line + label, out-of-domain values are skipped.
 *   annotations— optional event-marker overlays (design D5): [{ source, displayName, events:[{ts, label}] }] in
 *                catalog order; each source draws vertical markers in its own ANNOTATION_COLORS hue with a hover
 *                title (source · label · local time) and an entry in a small annotation key below the chart.
 *   onSelect   — optional drill callback (design D6): a legend entry for a grouped series calls onSelect(series.drill)
 *                — [{dimension, value}] — so the caller can re-run the panel filtered to that series' group value.
 *   series2    — optional dual-axis overlay series (#1606): { key, label, color, formatValue?, unit?, integerTicks? }. Drawn
 *                against its OWN right-hand y-axis (own nice scale, own unit caption) so two measures with
 *                different magnitudes read together. Only line/area modes carry one (validation upstream).
 *   integerTicks — optional: put the y gridlines (and the series2 axis, via series2.integerTicks) on whole numbers
 *                only, never stepping below 1. For a chart of COUNTS: its values print through a whole-number
 *                formatter, so fractional ticks (0.2, 0.4 ...) would repeat the same label down the axis.
 *                Absent/false ⇒ the original tick steps, byte-for-byte.
 *   onZoom     — optional brush-zoom callback (#1606): a pointer drag across ≥8px of plot selects a time
 *                range and calls onZoom(fromMs, toMs) so the caller can RE-RUN the panel on that window
 *                (server-side re-run keeps bucket resolution + tier routing + the partial-window notice honest).
 *   title      — optional chart name; it names the saved image and the exported CSV.
 *   source     — optional { read, params }: the read name and parameters behind the chart. The chart menu's
 *                Show Data Source item appears only when this is given.
 *   zoomed / onResetZoom — optional: when zoomed is true and onResetZoom is a function, the chart menu offers Reset zoom.
 *   atTime     — optional { server }: a server-tab chart. A right-click on the plot then also offers Show Active Queries /
 *                Blocking / Deadlocks at This Time, which set the server's custom range to the clicked time ±30 minutes
 *                and open that tab. A menu opened without a pointer (the ⋯ button) has no clicked time and offers none.
 *   windowStart— optional x-axis DOMAIN start, windowEnd its end, both UTC-epoch ms (#2802). When both are given
 *   windowEnd    and windowEnd > windowStart, the axis spans [windowStart, windowEnd] — the REQUESTED time window
 *                — instead of the data's own first/last-point extent, so a sparse discrete-event series (blocking,
 *                deadlocks: rows only inside a short burst) plots at its true position across a "last N hours"
 *                chart rather than the renderer zooming to the burst AND dropping the calendar date. windowEnd is
 *                the caller's query-time "now", NEVER the last data point — anchoring to the last point would slide
 *                an old burst to the right edge and read as current. Absent/degenerate ⇒ the original data-extent
 *                domain, byte-for-byte. Data times stay naive-UTC-parsed and tick labels stay browser-local.
 */
export function renderLineChart(spec) {
  const { points, xKey, series: allSeries, formatValue = (v) => String(v), clampMax = null, unit = null, mode = "line", thresholds = null, annotations = null, onSelect = null, series2: series2Spec = null, onZoom = null, integerTicks = false, windowStart = null, windowEnd = null } = spec;
  const { title = null, source = null, zoomed = false, onResetZoom = null, menuKey = null, exportPoints = null } = spec;
  const { hiddenKeys = null, onLegend = null, atTime = null } = spec;
  /* Legend hide/isolate: a hidden series is dropped from everything below (the y-domain, the stack, the drawn
     marks, the hover tooltip and the CSV) so the axis rescales to what is visible. The legend still lists it,
     marked off, so it can be brought back. Hiding every series is never honoured: the chart keeps all of them. */
  const hiddenSet = new Set(hiddenKeys || []);
  const visibleSeries = allSeries.filter((s) => !hiddenSet.has(s.key));
  const series2 = series2Spec && !hiddenSet.has(series2Spec.key) ? series2Spec : null;
  const anyVisible = visibleSeries.length > 0 || series2 !== null;
  const series = anyVisible ? visibleSeries : allSeries;
  const legendHidden = anyVisible ? hiddenSet : new Set();
  const stacked = mode === "stacked";
  const stackedBar = mode === "stacked-bar";
  /* Both stacked modes share the cumulative pre-pass, the sum-based y-domain, and the hover-at-stack-top dots. */
  const usesStack = stacked || stackedBar;
  const filled = mode === "area" || stacked;

  /* Parse + sort the x axis (naive UTC -> real Date via parseUtc). */
  const rows = (points || [])
    .map((r) => ({ t: parseUtc(r[xKey]), r }))
    .filter((p) => p.t)
    .sort((a, b) => a.t - b.t);

  if (rows.length === 0) {
    return el("div", { class: "chart" }, [emptyStrip("Not enough data points to chart yet.")]);
  }
  /* ONE collected bucket is data, not a warming-up absence. A single point has no segment to stroke, so the
     branches below draw it as a marker (a polyline needs two); zero rows took the strip above. Without this a
     panel whose series reached one bucket showed "not enough data points" beside siblings that had reached two
     — the same tab reading as half-broken while it warmed up. The single-point geometry (a centered x, one
     tick, a dot per series) is guarded on spanMs === 0 / linePts.length === 1 throughout. */

  /* The x DOMAIN. Default: the data's own first/last-point extent. #2802: when the caller passes the window it
     fetched over (windowStart/windowEnd, UTC-epoch ms, windowEnd = the query-time now), the axis spans THAT window
     instead, so a sparse series (blocking/deadlocks — rows only inside a short burst) plots at its true position
     across a "last N hours" chart rather than the renderer zooming to the burst and dropping the date. A degenerate
     window (non-number, non-finite, or end <= start) is ignored so a bad input can never collapse the axis; with no
     window passed the data-extent domain below is byte-for-byte the original. */
  const dataTMin = rows[0].t.getTime();
  const dataTMax = rows[rows.length - 1].t.getTime();
  const hasWindow =
    typeof windowStart === "number" && typeof windowEnd === "number" &&
    isFinite(windowStart) && isFinite(windowEnd) && windowEnd > windowStart;
  const tMin = hasWindow ? windowStart : dataTMin;
  const tMax = hasWindow ? windowEnd : dataTMax;
  const spanMs = tMax - tMin;

  /* A dual-axis overlay reserves a right gutter for its own tick labels + unit caption (#1606); without
     one the geometry is byte-for-byte the original. All right-edge math below uses these, never M.r. */
  const plotRight = series2 ? W - 56 : W - M.r;
  const plotW = plotRight - M.l;

  /* A numeric reader: null/NaN reads as null for line/area (a gap), or 0 for a stacked mode (a continuous baseline). */
  const readVal = (r, key) => {
    const v = r[key];
    return v == null || isNaN(v) ? (usesStack ? 0 : null) : Number(v);
  };

  /* Stacked pre-pass: cumulative top per series at each row (stackTops[i][k] = sum of series 0..k at row i). */
  const stackTops = usesStack ? rows.map(() => new Array(series.length).fill(0)) : null;

  /* y domain. Stacked: 0..max stack sum. Line/area: across every series (0-baselined; non-negative metrics). */
  let dataMax = -Infinity;
  let dataMin = Infinity;
  if (usesStack) {
    for (let i = 0; i < rows.length; i++) {
      let running = 0;
      for (let k = 0; k < series.length; k++) {
        running += readVal(rows[i].r, series[k].key);
        stackTops[i][k] = running;
      }
      if (running > dataMax) dataMax = running;
    }
    dataMin = 0;
    if (dataMax === -Infinity) dataMax = 1;
  } else {
    for (const { r } of rows) {
      for (const s of series) {
        const v = readVal(r, s.key);
        if (v == null) continue;
        if (v > dataMax) dataMax = v;
        if (v < dataMin) dataMin = v;
      }
    }
    if (dataMax === -Infinity) {
      /* With a series hidden the legend must stay on screen to bring it back, so draw an empty axis instead. */
      if (legendHidden.size === 0) return el("div", { class: "chart" }, [emptyStrip("No numeric values to chart.")]);
      dataMax = 1;
    }
    dataMin = Math.min(0, dataMin);
  }
  if (dataMax === dataMin) dataMax = dataMin + 1;

  /* Nice-rounded domain so gridline labels land on round values; percentage charts (clampMax=100) cap the top
     at 100 and never exceed it, so a 96% reading no longer rounds the axis up to a "120%" tick. */
  const scale = niceScale(dataMin, dataMax, Y_TICKS, clampMax, integerTicks);
  const yMin = scale.min;
  const yMax = scale.max;

  /* A single bucket spans no time (spanMs === 0), so every point shares tMin; center it rather than pinning
     it to the left axis, where a lone dot reads as a glitch. */
  const scaleX = (t) => (spanMs === 0 ? M.l + plotW / 2 : M.l + ((t - tMin) / spanMs) * plotW);
  const scaleY = (v) => M.t + (1 - (v - yMin) / (yMax - yMin)) * PLOT_H;
  /* Plotted points clamp into the plot box so a value above a clamped (pct) domain can't draw outside it. */
  const plotY = (v) => Math.max(M.t, Math.min(M.t + PLOT_H, scaleY(v)));
  const baseY = plotY(0);

  const root = svg("svg", { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", role: "img" });
  /* The series clip to the plot box. A zoomed chart keeps one neighbour point just outside each edge of the span
     (applyChartZoom) so the line reaches the axis edge; this clip cuts that segment off at the edge. */
  const clipId = "plot-clip-" + (++clipSeq);
  const clipDef = svg("clipPath", { id: clipId });
  clipDef.appendChild(svg("rect", { x: M.l, y: M.t, width: plotW, height: PLOT_H }));
  root.appendChild(clipDef);
  const clipAttr = `url(#${clipId})`;

  /* Horizontal gridlines + y labels (on the nice tick values). */
  const axis = svg("g", { class: "axis" });
  for (const val of scale.ticks) {
    const y = scaleY(val);
    axis.appendChild(svg("line", { class: "grid-line", x1: M.l, y1: y, x2: plotRight, y2: y }));
    const label = svg("text", { x: M.l - 8, y: y + 4, "text-anchor": "end" });
    label.textContent = formatValue(val);
    axis.appendChild(label);
  }

  /* Y-axis unit caption. Skipped for "%" (the tick labels already carry the unit, and stacking a caption
     on the top tick collided with its label — the design review's must-fix); for bare-number axes it sits
     in the extra top headroom reserved above (well clear of the top tick's label). */
  if (unit && unit !== "%") {
    const cap = svg("text", { class: "axis-unit", x: M.l - 8, y: 11, "text-anchor": "end" });
    cap.textContent = unit;
    axis.appendChild(cap);
  }

  /* Vertical gridlines + x labels (6 ticks). The label widens to include the calendar date when the domain
     spans more than one day, so a window crossing midnight is unambiguous even if it is under 24h wide. */
  /* #2802: derived from the DOMAIN bounds, not the data's first/last point — a same-day burst inside a 24h window
     that crosses midnight must still carry the calendar date. Byte-for-byte the old value with no window passed
     (the domain bounds ARE the first/last data point then). */
  const crossesDay = new Date(tMin).toDateString() !== new Date(tMax).toDateString();
  const X_TICKS = 5;
  /* One bucket spans no time, so the evenly-spaced loop would stack X_TICKS identical labels on the centered
     point. Draw a single centered gridline + time label instead. */
  const xTickTimes = spanMs === 0 ? [tMin] : Array.from({ length: X_TICKS + 1 }, (_, i) => tMin + (spanMs * i) / X_TICKS);
  for (let i = 0; i < xTickTimes.length; i++) {
    const t = xTickTimes[i];
    const x = scaleX(t);
    axis.appendChild(svg("line", { class: "grid-line", x1: x, y1: M.t, x2: x, y2: M.t + PLOT_H }));
    const anchor = xTickTimes.length === 1 ? "middle" : i === 0 ? "start" : i === xTickTimes.length - 1 ? "end" : "middle";
    const label = svg("text", {
      x: Math.min(Math.max(x, M.l + 2), plotRight - 2),
      y: H - 8,
      "text-anchor": anchor,
    });
    label.textContent = axisTime(new Date(t), crossesDay);
    axis.appendChild(label);
  }
  /* Dual-axis overlay (#1606): the series2 values get their OWN nice scale on a right-hand axis — tick
     labels + unit caption in the reserved right gutter, so two measures of different magnitudes read
     together without either flattening the other. */
  let scaleY2 = null;
  if (series2) {
    let m2 = -Infinity;
    let n2 = Infinity;
    for (const { r } of rows) {
      const v = r[series2.key];
      if (v == null || isNaN(v)) continue;
      const num = Number(v);
      if (num > m2) m2 = num;
      if (num < n2) n2 = num;
    }
    if (m2 === -Infinity) { m2 = 1; n2 = 0; }
    n2 = Math.min(0, n2);
    if (m2 === n2) m2 = n2 + 1;
    const s2 = niceScale(n2, m2, Y_TICKS, null, series2.integerTicks === true);
    scaleY2 = (v) => M.t + (1 - (v - s2.min) / (s2.max - s2.min)) * PLOT_H;
    const fmt2 = series2.formatValue || ((v) => String(v));
    for (const val of s2.ticks) {
      const y = scaleY2(val);
      const label = svg("text", { x: plotRight + 8, y: y + 4, "text-anchor": "start", fill: normalizeColor(series2.color) });
      label.textContent = fmt2(val);
      axis.appendChild(label);
    }
    if (series2.unit && series2.unit !== "%") {
      const cap2 = svg("text", { class: "axis-unit", x: plotRight + 8, y: 11, "text-anchor": "start", fill: normalizeColor(series2.color) });
      cap2.textContent = series2.unit;
      axis.appendChild(cap2);
    }
  }

  root.appendChild(axis);

  const xs = rows.map((p) => scaleX(p.t.getTime()));

  if (stackedBar) {
    /* Time-series stacked BAR: one vertical bar per bucket, segmented bottom-up by series using the same cumulative
       tops as the stacked area. Bar width is a fraction of the per-bucket spacing, centered on the bucket and clamped
       into the plot; a sub-pixel segment is dropped so a dense window degrades cleanly toward a filled band. */
    /* Bar width = one bucket's on-screen width. With no window the buckets tile the axis, so plotW/rows.length IS
       the bucket width (byte-for-byte the original). With a #2802 window the buckets can be sparse — plotW/rows.length
       would then draw each bar far wider than a bucket and overlap its neighbours — so measure the tightest adjacent
       gap (one bucket) and use that; fall back to the tiling width when there is only one bar to place. */
    let barW;
    if (hasWindow && xs.length > 1) {
      let minGap = Infinity;
      for (let i = 1; i < xs.length; i++) {
        const g = xs[i] - xs[i - 1];
        if (g > 0 && g < minGap) minGap = g;
      }
      barW = Math.max(1, (isFinite(minGap) ? minGap : plotW / rows.length) * 0.7);
    } else {
      barW = Math.max(1, (plotW / rows.length) * 0.7);
    }
    for (let i = 0; i < rows.length; i++) {
      const bx = Math.max(M.l, Math.min(M.l + plotW - barW, xs[i] - barW / 2));
      for (let k = 0; k < series.length; k++) {
        const yTop = plotY(stackTops[i][k]);
        const yBot = plotY(k === 0 ? 0 : stackTops[i][k - 1]);
        const h = yBot - yTop;
        if (h < 0.5) continue;
        root.appendChild(
          svg("rect", { class: "series-bar", x: bx, y: yTop, width: barW, height: h, fill: normalizeColor(series[k].color) })
        );
      }
    }
  } else if (stacked) {
    if (rows.length === 1) {
      /* A single bucket has no horizontal extent, so each band's polygon collapses to a zero-area sliver that
         paints nothing (.series-area has no stroke). Draw a dot at each series' cumulative stack top instead —
         the position the hover dots already use in stacked mode — so a warming-up stacked panel shows its one
         reading rather than a blank grid. */
      for (let k = 0; k < series.length; k++) {
        root.appendChild(svg("circle", { class: "series-dot", cx: xs[0], cy: plotY(stackTops[0][k]), r: 4, fill: normalizeColor(series[k].color) }));
      }
    } else {
      /* Filled bands drawn top series first so lower bands paint over the seams; each band is bounded above by its
         own cumulative top and below by the previous series' cumulative top (the x-axis for series 0). */
      for (let k = series.length - 1; k >= 0; k--) {
        const top = [];
        const bottom = [];
        for (let i = 0; i < rows.length; i++) {
          top.push(xs[i] + "," + plotY(stackTops[i][k]));
          bottom.push(xs[i] + "," + plotY(k === 0 ? 0 : stackTops[i][k - 1]));
        }
        bottom.reverse();
        root.appendChild(
          svg("polygon", {
            class: "series-area",
            "clip-path": clipAttr,
            points: top.concat(bottom).join(" "),
            fill: normalizeColor(series[k].color),
            "fill-opacity": "0.72",
          })
        );
      }
    }
  } else {
    /* Area fill (each series to the baseline) then the line on top; or just the line. Nulls drop the gap. */
    for (const s of series) {
      const linePts = [];
      for (let i = 0; i < rows.length; i++) {
        const v = readVal(rows[i].r, s.key);
        if (v == null) continue;
        linePts.push(xs[i] + "," + plotY(v));
      }
      /* A lone plottable point has no segment to stroke, so draw it as a dot — one bucket still shows its
         reading. (Its resting marker matches the hover dot; static class so it does not vanish on mouseout.) */
      if (linePts.length === 1) {
        const [cx, cy] = linePts[0].split(",");
        root.appendChild(svg("circle", { class: "series-dot", cx, cy, r: 4, fill: normalizeColor(s.color) }));
        continue;
      }
      if (linePts.length < 2) continue;
      if (filled) {
        const first = linePts[0].split(",")[0];
        const last = linePts[linePts.length - 1].split(",")[0];
        root.appendChild(
          svg("polygon", {
            class: "series-area",
            "clip-path": clipAttr,
            points: `${first},${baseY} ${linePts.join(" ")} ${last},${baseY}`,
            fill: normalizeColor(s.color),
            "fill-opacity": "0.15",
          })
        );
      }
      root.appendChild(svg("polyline", { class: "series-line", "clip-path": clipAttr, points: linePts.join(" "), stroke: normalizeColor(s.color) }));
    }
  }

  /* The dual-axis overlay line (#1606): plotted against ITS axis (scaleY2), clamped into the plot box,
     nulls dropped as gaps — same discipline as a primary line. Always a plain line (never filled/stacked). */
  if (series2 && scaleY2) {
    const pts2 = [];
    for (let i = 0; i < rows.length; i++) {
      const v = rows[i].r[series2.key];
      if (v == null || isNaN(v)) continue;
      const y2 = Math.max(M.t, Math.min(M.t + PLOT_H, scaleY2(Number(v))));
      pts2.push(xs[i] + "," + y2);
    }
    if (pts2.length === 1) {
      /* Same single-bucket rule as the primary series: a lone overlay reading draws as a dot, not a dropped
         series, so "a dot per series" holds for the right axis too. */
      const [cx, cy] = pts2[0].split(",");
      root.appendChild(svg("circle", { class: "series-dot", cx, cy, r: 4, fill: normalizeColor(series2.color) }));
    } else if (pts2.length >= 2) {
      root.appendChild(svg("polyline", { class: "series-line series-line-overlay", "clip-path": clipAttr, points: pts2.join(" "), stroke: normalizeColor(series2.color) }));
    }
  }

  /* Render-only threshold reference lines (design D3): a horizontal dashed guide at each in-domain value (the value
     runs along the y axis here). An out-of-domain threshold is skipped, never clamped onto an edge — a clamped line
     would read as a real reference at the wrong value. Drawn above the series, below the hover overlay. */
  if (Array.isArray(thresholds)) {
    for (const tv of thresholds) {
      if (tv == null || isNaN(tv) || tv < yMin || tv > yMax) continue;
      const ty = scaleY(tv);
      root.appendChild(thresholdLine(M.l, ty, plotRight, ty, plotRight - 4, ty - 4, "end", formatValue(tv)));
    }
  }

  /* Hover overlay: a transparent rect over the plot capturing mousemove. */
  const hoverLine = svg("line", { class: "hover-line", y1: M.t, y2: M.t + PLOT_H, style: "display:none" });
  root.appendChild(hoverLine);
  const hoverDots = svg("g", { style: "display:none" });
  root.appendChild(hoverDots);
  const overlay = svg("rect", { x: M.l, y: M.t, width: plotW, height: PLOT_H, fill: "transparent" });
  root.appendChild(overlay);

  /* Event-annotation overlays (design D5): a vertical marker at each event's time, one color per source (drawn ON
     TOP of the hover overlay so each marker's native <title> — source · label · local time — is hoverable, matching
     the ranked charts' native-title idiom). The visible line is thin + non-interactive; a wider transparent hit line
     carries the title. Out-of-window events are skipped, not clamped (the backend scopes them to the panel window,
     but a marker outside the plotted domain would otherwise pile onto an edge); dense windows just fill toward a band. */
  const annotationKey = [];
  if (Array.isArray(annotations) && annotations.length) {
    const g = svg("g", { class: "annotations" });
    annotations.forEach((layer, li) => {
      const color = normalizeColor(ANNOTATION_COLORS[li % ANNOTATION_COLORS.length]);
      let drawn = 0;
      for (const ev of layer.events || []) {
        const t = parseUtc(ev.ts);
        if (!t) continue;
        const ms = t.getTime();
        if (ms < tMin || ms > tMax) continue;
        const mx = scaleX(ms);
        g.appendChild(svg("line", { class: "annotation-line", x1: mx, y1: M.t, x2: mx, y2: M.t + PLOT_H, stroke: color }));
        const hit = svg("line", { class: "annotation-hit", x1: mx, y1: M.t, x2: mx, y2: M.t + PLOT_H });
        const title = svg("title");
        const lbl = ev.label == null || ev.label === "" ? "" : String(ev.label);
        title.textContent = layer.displayName + (lbl ? " · " + lbl : "") + " · " + t.toLocaleString();
        hit.appendChild(title);
        g.appendChild(hit);
        drawn++;
      }
      if (drawn > 0) annotationKey.push({ label: layer.displayName, color, count: drawn });
    });
    root.appendChild(g);
  }

  const chart = el("div", { class: "chart" }, [root]);
  const tooltip = el("div", { class: "chart-tooltip" });
  chart.appendChild(tooltip);
  chart.appendChild(buildLegend(series2Spec ? allSeries.concat([{ key: series2Spec.key, label: series2Spec.label, color: series2Spec.color }]) : allSeries, onSelect, legendHidden, onLegend));
  if (annotationKey.length) chart.appendChild(buildAnnotationLegend(annotationKey));

  /* Brush-zoom (#1606): pointerdown + setPointerCapture on the overlay (capture keeps the drag alive across
     the annotation hit-lines drawn above, and off-plot release still lands here). A drag ≥ 8 viewBox px draws
     a selection band and calls onZoom(fromMs, toMs); anything shorter is a click, ignored. The mousemove
     tooltip suppresses while a drag is live so it never repaints under the band. */
  let dragFromX = null;
  const brushRect = svg("rect", { class: "brush-rect", y: M.t, height: PLOT_H, style: "display:none" });
  root.appendChild(brushRect);
  const toVbX = (clientX) => {
    const rect = root.getBoundingClientRect();
    return ((clientX - rect.left) / rect.width) * W;
  };
  const vbToTime = (vbX) => tMin + (Math.max(0, Math.min(1, (vbX - M.l) / (plotW || 1))) * spanMs);
  if (onZoom) {
    overlay.addEventListener("pointerdown", (ev) => {
      if (ev.button !== 0) return;
      dragFromX = toVbX(ev.clientX);
      overlay.setPointerCapture(ev.pointerId);
    });
    overlay.addEventListener("pointermove", (ev) => {
      if (dragFromX == null) return;
      const x = toVbX(ev.clientX);
      const left = Math.max(M.l, Math.min(dragFromX, x));
      const right = Math.min(plotRight, Math.max(dragFromX, x));
      brushRect.setAttribute("x", left);
      brushRect.setAttribute("width", Math.max(0, right - left));
      brushRect.style.display = "";
    });
    overlay.addEventListener("pointerup", (ev) => {
      if (dragFromX == null) return;
      const from = dragFromX;
      dragFromX = null;
      brushRect.style.display = "none";
      const to = toVbX(ev.clientX);
      if (Math.abs(to - from) < 8) return; /* a click, not a brush */
      const t1 = vbToTime(Math.min(from, to));
      const t2 = vbToTime(Math.max(from, to));
      if (t2 > t1) onZoom(t1, t2);
    });
    overlay.addEventListener("pointercancel", () => {
      dragFromX = null;
      brushRect.style.display = "none";
    });
  }

  overlay.addEventListener("mousemove", (ev) => {
    if (dragFromX != null) return; /* brushing — the band owns the pointer */
    const rect = root.getBoundingClientRect();
    const vbX = ((ev.clientX - rect.left) / rect.width) * W;
    let idx = -1;
    let best = Infinity;
    for (let i = 0; i < xs.length; i++) {
      /* A zoom's off-plot neighbour points exist only to carry the line to the edge; they are not hoverable. */
      if (xs[i] < M.l - 0.5 || xs[i] > plotRight + 0.5) continue;
      const d = Math.abs(xs[i] - vbX);
      if (d < best) {
        best = d;
        idx = i;
      }
    }
    if (idx < 0) return;
    const { t, r } = rows[idx];
    const px = xs[idx];

    hoverLine.setAttribute("x1", px);
    hoverLine.setAttribute("x2", px);
    hoverLine.style.display = "";

    /* Dots sit at each series' plotted position: its cumulative top when stacked, its own value otherwise. */
    while (hoverDots.firstChild) hoverDots.removeChild(hoverDots.firstChild);
    for (let k = 0; k < series.length; k++) {
      const v = readVal(r, series[k].key);
      if (!usesStack && v == null) continue;
      const cy = usesStack ? plotY(stackTops[idx][k]) : plotY(v);
      hoverDots.appendChild(svg("circle", { class: "hover-dot", cx: px, cy, r: 3.5, fill: normalizeColor(series[k].color) }));
    }
    if (series2 && scaleY2) {
      const v2 = r[series2.key];
      if (v2 != null && !isNaN(v2)) {
        const cy2 = Math.max(M.t, Math.min(M.t + PLOT_H, scaleY2(Number(v2))));
        hoverDots.appendChild(svg("circle", { class: "hover-dot", cx: px, cy: cy2, r: 3.5, fill: normalizeColor(series2.color) }));
      }
    }
    hoverDots.style.display = "";

    /* Tooltip is built with textContent only (values may include untrusted series labels). */
    while (tooltip.firstChild) tooltip.removeChild(tooltip.firstChild);
    tooltip.appendChild(el("div", { class: "t-time", text: t.toLocaleString() }));
    for (const s of series) {
      const v = r[s.key];
      tooltip.appendChild(
        el("div", { class: "t-row" }, [
          el("span", { class: "swatch", style: "background:" + normalizeColor(s.color) }),
          el("span", { text: s.label }),
          el("span", { class: "t-val", text: v == null || isNaN(v) ? "—" : formatValue(v) }),
        ])
      );
    }
    if (series2) {
      const v2 = r[series2.key];
      const fmt2 = series2.formatValue || ((x) => String(x));
      tooltip.appendChild(
        el("div", { class: "t-row" }, [
          el("span", { class: "swatch", style: "background:" + normalizeColor(series2.color) }),
          el("span", { text: series2.label }),
          el("span", { class: "t-val", text: v2 == null || isNaN(v2) ? "—" : fmt2(v2) }),
        ])
      );
    }
    const renderedX = (px / W) * rect.width;
    tooltip.style.display = "block";
    tooltip.style.left = Math.min(renderedX + 12, rect.width - tooltip.offsetWidth - 4) + "px";
    tooltip.style.top = "8px";
  });

  overlay.addEventListener("mouseleave", () => {
    hoverLine.style.display = "none";
    hoverDots.style.display = "none";
    tooltip.style.display = "none";
  });

  /* Export Data to CSV writes every loaded point, the same as the Viewer, not just the zoomed span (exportPoints
     is the unzoomed set a zoomable chart passes). */
  const exportRows = exportPoints
    ? exportPoints.map((r) => ({ t: parseUtc(r[xKey]), r })).filter((p) => p.t).sort((a, b) => a.t - b.t)
    : rows;
  /* The right-click time: the pointer's x mapped through vbToTime, which clamps it to the chart's own x range. */
  const timeAt = atTime ? (clientX) => vbToTime(toVbX(clientX)) : null;
  attachChartMenu(chart, root, { title, source, zoomed, onResetZoom, xKey, series, series2, menuKey, atTime, timeAt }, exportRows);
  return chart;
}

/**
 * Render a horizontal RANKED bar chart into a returned `.chart` node (the compose "bar" viz — a topN result).
 * spec: { items:[{label,value,color?}], formatValue?, unit? } — items already ordered (value DESC) + bounded by
 * the query's topN. Bars scale to the largest value; each carries a native SVG <title> for the full label+value.
 * Rendered at a per-row height so a tall list stays readable (the container scrolls; the SVG never squashes).
 * onSelect (design D6): when an item carries a `drill` ([{dimension, value}]), its bar becomes clickable and calls
 * onSelect(item.drill) so the caller can re-run the panel filtered to that category (a transient drill-down).
 */
export function renderBarChart(spec) {
  const { items, formatValue = (v) => String(v), thresholds = null, onSelect = null } = spec;
  const rows = (items || []).filter((d) => d && d.value != null && !isNaN(d.value));
  if (!rows.length) return el("div", { class: "chart" }, [emptyStrip("No values to chart.")]);

  const shown = rows.slice(0, MAX_BARS);
  const maxVal = Math.max(0, ...shown.map((d) => Number(d.value)));
  const domainMax = maxVal > 0 ? maxVal : 1;

  const rowH = 26;
  const gap = 8;
  const labelW = 220;
  const valueW = 110;
  const barLeft = labelW + 8;
  const barW = W - barLeft - valueW;
  const height = M.t + shown.length * (rowH + gap);

  const root = svg("svg", { viewBox: `0 0 ${W} ${height}`, preserveAspectRatio: "xMinYMin meet", role: "img" });

  shown.forEach((d, i) => {
    const y = M.t + i * (rowH + gap);
    const val = Number(d.value);
    const w = Math.max(1, (val / domainMax) * barW);
    const color = normalizeColor(d.color);

    const label = svg("text", { class: "bar-label", x: labelW, y: y + rowH * 0.7, "text-anchor": "end" });
    label.textContent = trunclabel(d.label);

    const drillable = !!(onSelect && d.drill);
    const track = svg("rect", { class: drillable ? "bar-track drillable" : "bar-track", x: barLeft, y, width: barW, height: rowH, rx: 3 });
    const bar = svg("rect", { class: drillable ? "bar drillable" : "bar", x: barLeft, y, width: w, height: rowH, rx: 3, fill: color });
    const title = svg("title");
    title.textContent = (d.label == null || d.label === "" ? "—" : String(d.label)) + " · " + formatValue(val) + (drillable ? " · click to filter" : "");
    bar.appendChild(title);
    if (drillable) {
      const fire = () => onSelect(d.drill);
      bar.addEventListener("click", fire);
      track.addEventListener("click", fire);
    }

    const value = svg("text", { class: "bar-value", x: barLeft + w + 6, y: y + rowH * 0.7 });
    value.textContent = formatValue(val);

    root.appendChild(label);
    root.appendChild(track);
    root.appendChild(bar);
    root.appendChild(value);
  });

  /* Render-only threshold reference lines (design D3): a ranked bar's value runs along the x axis, so each in-domain
     threshold draws a VERTICAL dashed guide across the bars (value label at the top). Out-of-domain values skip. */
  if (Array.isArray(thresholds)) {
    for (const tv of thresholds) {
      if (tv == null || isNaN(tv) || tv < 0 || tv > domainMax) continue;
      const tx = barLeft + (tv / domainMax) * barW;
      root.appendChild(thresholdLine(tx, M.t, tx, height, tx, M.t - 4, "middle", formatValue(tv)));
    }
  }

  const chart = el("div", { class: "chart chart-bar" }, [root]);
  if (rows.length > MAX_BARS) {
    chart.appendChild(el("div", { class: "chart-note", text: `Showing the top ${MAX_BARS} of ${rows.length}.` }));
  }
  return chart;
}

/**
 * Render a RANKED donut chart into a returned `.chart` node (the compose "pie" viz). Slices beyond MAX_SLICES are
 * pooled into an "Other" wedge so a long tail stays legible; the center carries the grand total. A legend with
 * per-slice values sits below. Slice/legend text is textContent-only (labels may be untrusted).
 */
export function renderPieChart(spec) {
  const { items, formatValue = (v) => String(v), onSelect = null } = spec;
  const rows = (items || [])
    .filter((d) => d && d.value != null && !isNaN(d.value) && Number(d.value) > 0)
    .map((d) => ({ label: d.label == null || d.label === "" ? "—" : String(d.label), value: Number(d.value), color: d.color, drill: d.drill || null }));
  if (!rows.length) return el("div", { class: "chart" }, [emptyStrip("No positive values to chart.")]);

  /* Pool the long tail into "Other" so the donut never fans into unreadable slivers (the pooled wedge is a mix of
     categories, so it carries no drill — D6). */
  let slices = rows;
  if (rows.length > MAX_SLICES) {
    const head = rows.slice(0, MAX_SLICES - 1);
    const tail = rows.slice(MAX_SLICES - 1);
    const otherValue = tail.reduce((sum, d) => sum + d.value, 0);
    slices = head.concat([{ label: `Other (${tail.length})`, value: otherValue, color: NEUTRAL_SLICE, drill: null }]);
  }
  slices.forEach((d, i) => {
    if (!d.color) d.color = CATEGORICAL_COLORS[i % CATEGORICAL_COLORS.length];
  });

  const total = slices.reduce((sum, d) => sum + d.value, 0);
  const size = 260;
  const cx = size / 2;
  const cy = size / 2;
  const rOuter = size / 2 - 6;
  const rInner = rOuter * 0.58;

  const root = svg("svg", { viewBox: `0 0 ${size} ${size}`, class: "pie-svg", role: "img" });
  let angle = -90;
  for (const d of slices) {
    /* Clamp a lone 100% slice below a full turn — a 360° SVG arc (start == end point) renders nothing. */
    const sweep = Math.min((d.value / total) * 360, 359.999);
    const drillable = !!(onSelect && d.drill);
    const path = svg("path", { class: drillable ? "pie-slice drillable" : "pie-slice", d: donutArc(cx, cy, rOuter, rInner, angle, angle + sweep), fill: normalizeColor(d.color) });
    const title = svg("title");
    title.textContent = `${d.label} · ${formatValue(d.value)} (${Math.round((d.value / total) * 100)}%)` + (drillable ? " · click to filter" : "");
    path.appendChild(title);
    if (drillable) path.addEventListener("click", () => onSelect(d.drill));
    root.appendChild(path);
    angle += sweep;
  }

  const centerTotal = svg("text", { class: "pie-center", x: cx, y: cy - 2, "text-anchor": "middle" });
  centerTotal.textContent = formatValue(total);
  const centerLabel = svg("text", { class: "pie-center-label", x: cx, y: cy + 14, "text-anchor": "middle" });
  centerLabel.textContent = "total";
  root.appendChild(centerTotal);
  root.appendChild(centerLabel);

  const legend = el(
    "div",
    { class: "chart-legend pie-legend" },
    slices.map((d) => {
      const drillable = !!(onSelect && d.drill);
      const props = { class: drillable ? "item drillable" : "item" };
      if (drillable) {
        props.onActivate = () => onSelect(d.drill);
        props.title = "Filter to " + d.label;
      }
      return el("span", props, [
        el("span", { class: "swatch dot", style: "background:" + normalizeColor(d.color) }),
        el("span", { text: d.label }),
        el("span", { class: "leg-val", text: formatValue(d.value) }),
      ]);
    })
  );

  return el("div", { class: "chart chart-pie" }, [el("div", { class: "pie-wrap" }, [root]), legend]);
}

/**
 * Render a two-measure SCATTER into a returned `.chart` node (#1606 — the compose "scatter" viz): one point
 * per ranked group, x = the primary measure's value, y = the overlay's. Both axes get nice scales + unit
 * captions; each point carries a native <title> (label · x · y) and an optional drill click (design D6).
 * spec: { items:[{label, x, y, drill?}], formatX?, formatY?, unitX?, unitY?, onSelect? }
 */
export function renderScatterChart(spec) {
  const { items, formatX = (v) => String(v), formatY = (v) => String(v), unitX = null, unitY = null, onSelect = null } = spec;
  const pts = (items || []).filter((d) => d && d.x != null && !isNaN(d.x) && d.y != null && !isNaN(d.y));
  if (!pts.length) return el("div", { class: "chart" }, [emptyStrip("No paired values to plot.")]);

  const xMax = Math.max(...pts.map((d) => Number(d.x)));
  const yMax = Math.max(...pts.map((d) => Number(d.y)));
  const sx = niceScale(0, xMax > 0 ? xMax : 1, Y_TICKS, null);
  const sy = niceScale(0, yMax > 0 ? yMax : 1, Y_TICKS, null);
  const plotRight = W - M.r;
  const plotW = plotRight - M.l;
  const scaleX = (v) => M.l + ((v - sx.min) / (sx.max - sx.min)) * plotW;
  const scaleY = (v) => M.t + (1 - (v - sy.min) / (sy.max - sy.min)) * PLOT_H;

  const root = svg("svg", { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: "none", role: "img" });
  const axis = svg("g", { class: "axis" });
  for (const val of sy.ticks) {
    const y = scaleY(val);
    axis.appendChild(svg("line", { class: "grid-line", x1: M.l, y1: y, x2: plotRight, y2: y }));
    const label = svg("text", { x: M.l - 8, y: y + 4, "text-anchor": "end" });
    label.textContent = formatY(val);
    axis.appendChild(label);
  }
  for (const val of sx.ticks) {
    const x = scaleX(val);
    axis.appendChild(svg("line", { class: "grid-line", x1: x, y1: M.t, x2: x, y2: M.t + PLOT_H }));
    const label = svg("text", { x: Math.min(Math.max(x, M.l + 2), plotRight - 2), y: H - 8, "text-anchor": "middle" });
    label.textContent = formatX(val);
    axis.appendChild(label);
  }
  if (unitY && unitY !== "%") {
    const cap = svg("text", { class: "axis-unit", x: M.l - 8, y: 11, "text-anchor": "end" });
    cap.textContent = unitY;
    axis.appendChild(cap);
  }
  if (unitX) {
    const cap = svg("text", { class: "axis-unit", x: plotRight, y: H - 8, "text-anchor": "end" });
    cap.textContent = unitX;
    axis.appendChild(cap);
  }
  root.appendChild(axis);

  for (const d of pts) {
    const drillable = !!(onSelect && d.drill);
    const dot = svg("circle", {
      class: drillable ? "scatter-dot drillable" : "scatter-dot",
      cx: scaleX(Number(d.x)),
      cy: Math.max(M.t, Math.min(M.t + PLOT_H, scaleY(Number(d.y)))),
      r: 5,
      fill: normalizeColor(d.color || CATEGORICAL_COLORS[0]),
      "fill-opacity": "0.75",
    });
    const title = svg("title");
    title.textContent =
      (d.label == null || d.label === "" ? "—" : String(d.label)) +
      " · " + formatX(Number(d.x)) + " · " + formatY(Number(d.y)) +
      (drillable ? " · click to filter" : "");
    dot.appendChild(title);
    if (drillable) dot.addEventListener("click", () => onSelect(d.drill));
    root.appendChild(dot);
  }

  return el("div", { class: "chart chart-scatter" }, [root]);
}

/** Bounds on how much a ranked chart draws before it stops reading (the container scrolls a bar list; a pie pools). */
const MAX_BARS = 30;
const MAX_SLICES = 9;

/** Truncate a bar's category label to keep it inside the label gutter. */
function trunclabel(s) {
  const flat = String(s == null || s === "" ? "—" : s);
  return flat.length > 30 ? flat.slice(0, 29) + "…" : flat;
}

/** SVG path `d` for a donut wedge from a1° to a2° (0° = 3 o'clock, angles increase clockwise in SVG's y-down space). */
function donutArc(cx, cy, rOuter, rInner, a1, a2) {
  const p = (r, a) => {
    const rad = (a * Math.PI) / 180;
    return [cx + r * Math.cos(rad), cy + r * Math.sin(rad)];
  };
  const large = a2 - a1 > 180 ? 1 : 0;
  const [ox1, oy1] = p(rOuter, a1);
  const [ox2, oy2] = p(rOuter, a2);
  const [ix2, iy2] = p(rInner, a2);
  const [ix1, iy1] = p(rInner, a1);
  return (
    `M ${ox1} ${oy1} A ${rOuter} ${rOuter} 0 ${large} 1 ${ox2} ${oy2} ` +
    `L ${ix2} ${iy2} A ${rInner} ${rInner} 0 ${large} 0 ${ix1} ${iy1} Z`
  );
}

/**
 * One render-only threshold reference line (design D3): a dashed, neutral line from (x1,y1) to (x2,y2) with a small
 * value label at (lx,ly). Shared by the time-series charts (horizontal, value on the y axis) and the ranked bar
 * (vertical, value on the x axis) so the two can never drift in styling. `text` goes through textContent (R4/XSS).
 */
function thresholdLine(x1, y1, x2, y2, lx, ly, anchor, text) {
  const g = svg("g", { class: "threshold" });
  g.appendChild(svg("line", { class: "threshold-line", x1, y1, x2, y2 }));
  const label = svg("text", { class: "threshold-label", x: lx, y: ly, "text-anchor": anchor });
  label.textContent = text;
  g.appendChild(label);
  return g;
}

/** The series key below a time chart. onSelect (design D6): a grouped series' entry becomes an activatable (mouse +
 *  keyboard) control that calls onSelect(series.drill) to re-run the panel filtered to that series' group value.
 *  onLegend (#5247): when given, a click on an entry (on a drillable entry, its swatch) hides or shows that series,
 *  a double-click or Shift+click isolates it (double-click on the isolated one shows all), and a hidden entry is
 *  marked off (struck through, hollow swatch, aria-pressed=false). While anything is hidden a "Show all" button
 *  follows the entries. onLegend(action, key) takes "toggle" | "isolate" | "all". */
function buildLegend(series, onSelect, hidden, onLegend) {
  const hiddenSet = hidden || new Set();
  const items = series.map((s) => {
    const drillable = !!(onSelect && s.drill);
    const switchable = !!(onLegend && s.key != null);
    const off = switchable && hiddenSet.has(s.key);
    const props = { class: "item" + (drillable ? " drillable" : "") + (switchable ? " switchable" : "") + (off ? " legend-off" : "") };
    const swatch = el("span", { class: "swatch", style: "background:" + normalizeColor(s.color) });
    const label = el("span", { text: s.label });
    if (drillable) {
      label.addEventListener("click", () => onSelect(s.drill));
      props.title = "Filter to " + s.label;
    }
    const hit = drillable ? null : "item";
    const wire = (node) => {
      node.setAttribute("role", "button");
      node.setAttribute("tabindex", "0");
      node.setAttribute("aria-pressed", off ? "false" : "true");
      node.setAttribute("title", (off ? "Show " : "Hide ") + s.label + " (double-click: show only this one)");
      node.addEventListener("click", (e) => onLegend(e && e.shiftKey ? "isolate" : "toggle", s.key));
      node.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onLegend(e.shiftKey ? "isolate" : "toggle", s.key);
        }
      });
      node.addEventListener("dblclick", () => onLegend("isolate", s.key));
    };
    const node = el("span", props, [swatch, label]);
    if (switchable) wire(hit ? node : swatch);
    else if (drillable) {
      node.setAttribute("role", "button");
      node.setAttribute("tabindex", "0");
      node.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSelect(s.drill);
        }
      });
    }
    return node;
  });
  if (onLegend && hiddenSet.size > 0) {
    const all = el("button", { class: "btn small legend-show-all", type: "button", text: "Show all (" + hiddenSet.size + " hidden)" });
    all.addEventListener("click", () => onLegend("all", null));
    items.push(all);
  }
  return el("div", { class: "chart-legend" }, items);
}

/** The hidden-series keys after a legend action (#5247). "toggle" flips one series but never hides the last visible
 *  one; "isolate" leaves only that series visible, or shows all when it already is the only one; "all" shows all. */
export function nextHiddenKeys(allKeys, hiddenKeys, action, key) {
  const hidden = new Set((hiddenKeys || []).filter((k) => allKeys.includes(k)));
  if (action === "all") return [];
  if (!allKeys.includes(key)) return [...hidden];
  if (action === "isolate") {
    const alone = allKeys.every((k) => (k === key) !== hidden.has(k));
    return alone ? [] : allKeys.filter((k) => k !== key);
  }
  if (hidden.has(key)) hidden.delete(key);
  else if (allKeys.length - hidden.size > 1) hidden.add(key);
  return [...hidden];
}

/** The event-annotation key below a time chart (design D5): one entry per active source (its marker color + a count
 *  of markers drawn in view). A plain key — annotation sources are not a data series, so it is never drillable. */
function buildAnnotationLegend(entries) {
  return el(
    "div",
    { class: "chart-legend annotation-legend" },
    entries.map((e) =>
      el("span", { class: "item" }, [
        el("span", { class: "swatch annotation-swatch", style: "background:" + normalizeColor(e.color) }),
        el("span", { text: e.label }),
        el("span", { class: "leg-val", text: String(e.count) }),
      ])
    )
  );
}

/**
 * A NEUTRAL categorical ramp for chart series — deliberately NOT the ok/warn/err severity colors, so a chart's
 * lines never imply a health state (the alert/band palette stays severity-only). Distinct, colorblind-tolerant.
 */
export const SERIES_COLORS = ["#2eaef1", "#4dd0e1", "#b39ddb", "#7f8fa6", "#e0e0e0"];

/**
 * The wider categorical palette for composed multi-series / bar / pie charts (#1563) — the five SERIES_COLORS
 * (so the family reads the same) plus five more cool/neutral hues, staying clear of the ok/warn/err severity
 * colors for the same reason. All #rrggbb literals (air-gap safe); cycled when a chart has more categories.
 */
export const CATEGORICAL_COLORS = [
  "#2eaef1", "#4dd0e1", "#b39ddb", "#7f8fa6", "#e0e0e0",
  "#64b5f6", "#9575cd", "#4fc3f7", "#ba68c8", "#90a4ae",
];

/** The muted color for a pie's pooled "Other" wedge — a neutral gray that recedes behind the real categories. */
const NEUTRAL_SLICE = "#5b626e";

/**
 * The event-annotation marker palette (#1563 D5) — deliberately DISJOINT from both CATEGORICAL_COLORS (the cool
 * blues/purples/grays the data series use) and the ok/warn/err severity colors, so an annotation marker never
 * reads as a data series OR as a health state. Warm/distinct hues (pink, orange, brown, indigo), one per source,
 * cycled at the D5 cap of four. All #rrggbb literals (air-gap safe); pass through normalizeColor like every color.
 */
export const ANNOTATION_COLORS = ["#ec407a", "#ff8f00", "#8d6e63", "#5e35b1"];

/**
 * Presentation guard (defense-in-depth behind the server-side ValidateDefinition authority): a series color
 * reaches a style="background:<color>" sink in the tooltip/legend swatches, so a stored definition's color must
 * be a #rrggbb hex literal (case-insensitive). Anything else — a named color, a short #abc, a CSS function that
 * would fetch off-origin and defeat the air-gap, or a non-string — falls back to a safe palette default. Mirrors
 * the composer's normalizeColor (editor.js imports this one) so the two can never drift.
 */
export function normalizeColor(c) {
  return typeof c === "string" && /^#[0-9a-fA-F]{6}$/.test(c) ? c : SERIES_COLORS[0];
}

/** Classic "nice number" rounding (Heckbert): the round-friendly value at or just past `range`. */
function niceNum(range, round) {
  if (!(range > 0) || !isFinite(range)) return 1;
  const exp = Math.floor(Math.log10(range));
  const frac = range / Math.pow(10, exp);
  let nf;
  if (round) {
    nf = frac < 1.5 ? 1 : frac < 3 ? 2 : frac < 7 ? 5 : 10;
  } else {
    nf = frac <= 1 ? 1 : frac <= 2 ? 2 : frac <= 5 ? 5 : 10;
  }
  return nf * Math.pow(10, exp);
}

/**
 * A "nice" y-axis over [min, max]: rounded bounds and evenly-spaced tick values that land on round numbers.
 * `clampMax` caps the top (percentage charts pass 100 so the axis never exceeds 100%).
 * `integer` keeps every tick on a whole number: a domain narrower than about five units would otherwise step by
 * 0.2 or 0.5, and a chart of COUNTS (events, sessions) labels those ticks through a whole-number formatter, so a
 * 0-1 axis read "1 1 1 0 0 0". With the step held at 1 or more the ticks are distinct whole numbers, and a
 * small-count chart shows a few honest gridlines (0, 1, 2) instead of a column of repeated labels.
 */
function niceScale(min, max, maxTicks, clampMax, integer = false) {
  const range = niceNum(max - min || 1, false);
  const niceStep = niceNum(range / Math.max(1, maxTicks), true) || 1;
  const step = integer ? Math.max(1, Math.ceil(niceStep)) : niceStep;
  const niceMin = Math.floor(min / step) * step;
  let niceMax = Math.ceil(max / step) * step;
  if (clampMax != null && niceMax > clampMax) niceMax = clampMax;
  const n = Math.max(1, Math.round((niceMax - niceMin) / step));
  const ticks = [];
  for (let i = 0; i <= n; i++) {
    const v = niceMin + i * step;
    ticks.push(v > niceMax ? niceMax : v);
  }
  return { min: niceMin, max: niceMax, step, ticks };
}


/* ─────────────────────────── chart menu (#4843) ─────────────────────────── */

/* The Viewer's chart context menu, in the browser: Copy Image, Save Image As, Reset zoom, Export Data to CSV and
   Show Data Source, in that order. It opens from a small visible button (keyboard reachable) and from right-click.
   Items are plain text. grid-tools.js is loaded on first use, so a page that never opens the menu never pulls it. */
export const CHART_MENU_LABELS = {
  copy: "Copy Image",
  save: "Save Image As...",
  reset: "Reset zoom",
  csv: "Export Data to CSV...",
  source: "Show Data Source",
  atQueries: "Show Active Queries at This Time",
  atBlocking: "Show Blocking at This Time",
  atDeadlocks: "Show Deadlocks at This Time",
};

/** Half of the range an "at this time" item opens: the smallest custom range the picker allows is one hour. */
export const AT_TIME_HALF_WINDOW_MS = 30 * 60000;

/** The "at this time" items: label, and the server sub-tab the item opens (Deadlocks is a panel of the Blocking tab). */
const AT_TIME_TARGETS = [
  { label: CHART_MENU_LABELS.atQueries, tab: "queries" },
  { label: CHART_MENU_LABELS.atBlocking, tab: "blocking" },
  { label: CHART_MENU_LABELS.atDeadlocks, tab: "blocking" },
];

const SVG_STYLE_PROPS = ["fill", "stroke", "stroke-width", "stroke-dasharray", "stroke-linecap", "stroke-linejoin", "opacity", "fill-opacity", "stroke-opacity", "font-family", "font-size", "font-weight", "font-variant-numeric", "text-anchor", "display"];
const IMAGE_SCALE = 2;

/** The chart's series as CSV rows: a header, then one row per (time, series) with a value. Times are UTC. */
export function chartCsvRows(rows, xKey, series, series2) {
  const all = series2 ? series.concat([series2]) : series;
  const out = [["DateTime (UTC)", "Series", "Value"]];
  for (const { t, r } of rows) {
    const stamp = t.toISOString().slice(0, 19).replace("T", " ");
    for (const s of all) {
      const v = r[s.key];
      if (v == null || v === "" || isNaN(v)) continue;
      out.push([stamp, s.label || s.key, Number(v)]);
    }
  }
  return out;
}

/** The text Show Data Source displays: the read name, then each parameter on its own line. */
export function chartSourceLines(source) {
  const lines = ["Read: " + String(source.read)];
  const params = source.params && typeof source.params === "object" ? Object.entries(source.params) : [];
  if (params.length === 0) lines.push("Parameters: none");
  else for (const [k, v] of params) lines.push(k + " = " + (v != null && typeof v === "object" ? JSON.stringify(v) : String(v)));
  return lines;
}

/* A copy of the live SVG with the computed style of every element written onto it, so the PNG matches the screen
   (the page's CSS and theme variables do not travel with a serialized SVG). */
function inlineStyledSvgClone(root) {
  const clone = root.cloneNode(true);
  const live = [root].concat(Array.from(root.querySelectorAll("*")));
  const copy = [clone].concat(Array.from(clone.querySelectorAll("*")));
  live.forEach((node, i) => {
    const cs = getComputedStyle(node);
    const decl = SVG_STYLE_PROPS.map((p) => p + ":" + cs.getPropertyValue(p)).join(";");
    copy[i].setAttribute("style", decl);
  });
  const box = root.getBoundingClientRect();
  clone.setAttribute("width", String(Math.round(box.width) || W));
  clone.setAttribute("height", String(Math.round(box.height) || H));
  clone.setAttribute("xmlns", SVG_NS);
  return clone;
}

/* Rasterizes the chart to a PNG blob on a canvas: no network, the SVG goes in as a data URL. */
async function chartPngBlob(root) {
  const clone = inlineStyledSvgClone(root);
  const width = Number(clone.getAttribute("width"));
  const height = Number(clone.getAttribute("height"));
  const url = "data:image/svg+xml;charset=utf-8," + encodeURIComponent(new XMLSerializer().serializeToString(clone));
  const img = new Image();
  await new Promise((resolve, reject) => {
    img.onload = resolve;
    img.onerror = () => reject(new Error("the browser could not draw the chart"));
    img.src = url;
  });
  const canvas = document.createElement("canvas");
  canvas.width = width * IMAGE_SCALE;
  canvas.height = height * IMAGE_SCALE;
  const ctx = canvas.getContext("2d");
  ctx.fillStyle = getComputedStyle(document.body).backgroundColor || "#fff";
  ctx.fillRect(0, 0, canvas.width, canvas.height);
  ctx.drawImage(img, 0, 0, canvas.width, canvas.height);
  return new Promise((resolve, reject) => canvas.toBlob((b) => (b ? resolve(b) : reject(new Error("the browser could not encode the image"))), "image/png"));
}

function menuFileStem(title) {
  return String(title || "chart").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, 60) || "chart";
}

/* What a chart's menu and Show Data Source panel have open, at MODULE scope so the 60 s poll's rebuild of the
   panel puts them back. Keyed by chart identity (the key zoomableLineChart passes: id + scope); an entry older than
   MENU_STATE_TTL_MS is ignored, so a chart left while its menu was open does not reopen it much later. */
const chartMenuStates = new Map();
const MENU_STATE_TTL_MS = 90000;

function attachChartMenu(chart, root, opts, rows) {
  const { title, source, zoomed, onResetZoom, xKey, series, series2, menuKey, atTime, timeAt } = opts;
  const owner = {};
  const held = menuKey ? chartMenuStates.get(menuKey) : null;
  const restore = held && Date.now() - held.at < MENU_STATE_TTL_MS ? held : null;
  const hold = (patch) => {
    if (!menuKey) return;
    const cur = chartMenuStates.get(menuKey) || {};
    chartMenuStates.set(menuKey, { ...cur, ...patch, owner, at: Date.now() });
  };
  const release = (field) => {
    const cur = menuKey ? chartMenuStates.get(menuKey) : null;
    if (!cur || cur.owner !== owner) return;
    if (field === "menu") cur.menu = null;
    else cur.source = false;
    if (!cur.menu && !cur.source) chartMenuStates.delete(menuKey);
  };
  const hasSource = !!(source && source.read);
  const canReset = zoomed === true && typeof onResetZoom === "function";
  const button = el("button", { class: "chart-menu-btn", type: "button", title: "Chart menu", "aria-label": "Chart menu", "aria-haspopup": "menu", "aria-expanded": "false", text: "⋯" });
  const status = el("div", { class: "chart-menu-status", role: "status" });
  const sourceBox = el("div", { class: "chart-source" });
  sourceBox.style.display = "none";
  let popup = null;
  let outside = null;

  const say = (msg) => {
    status.textContent = msg;
  };
  const close = () => {
    if (popup) {
      chart.removeChild(popup);
      popup = null;
    }
    button.setAttribute("aria-expanded", "false");
    release("menu");
    if (outside && typeof document.removeEventListener === "function") document.removeEventListener("click", outside);
    outside = null;
  };
  const actions = [];
  actions.push({
    label: CHART_MENU_LABELS.copy,
    run: async () => {
      try {
        const nav = typeof navigator !== "undefined" ? navigator : null;
        if (!nav || !nav.clipboard || typeof nav.clipboard.write !== "function" || typeof ClipboardItem === "undefined" || window.isSecureContext === false) {
          say("Copy isn't available here: the browser only allows it on a secure (HTTPS or localhost) page.");
          return;
        }
        /* Safari only honours the click for a clipboard write that is started at once, so the item takes the
           still-rendering PNG as a promise instead of awaiting it first. */
        await nav.clipboard.write([new ClipboardItem({ "image/png": chartPngBlob(root) })]);
        say("Image copied.");
      } catch (e) {
        say("Copy failed: " + (e && e.message ? e.message : "the browser refused."));
      }
    },
  });
  actions.push({
    label: CHART_MENU_LABELS.save,
    run: async () => {
      try {
        const blob = await chartPngBlob(root);
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = menuFileStem(title) + ".png";
        a.style.display = "none";
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        setTimeout(() => URL.revokeObjectURL(url), 10000);
        say("Image saved.");
      } catch (e) {
        say("Save failed: " + (e && e.message ? e.message : "the browser refused."));
      }
    },
  });
  if (canReset) actions.push({ label: CHART_MENU_LABELS.reset, run: () => onResetZoom() });
  actions.push({
    label: CHART_MENU_LABELS.csv,
    run: async () => {
      try {
        const tools = await import("./grid-tools.js");
        tools.downloadCsv(tools.csvFileName(menuFileStem(title)), tools.toCsv(chartCsvRows(rows, xKey, series, series2)));
        say("CSV exported.");
      } catch (e) {
        say("Export failed: " + (e && e.message ? e.message : "the browser refused."));
      }
    },
  });
  if (hasSource) {
    actions.push({
      label: CHART_MENU_LABELS.source,
      run: () => {
        sourceBox.textContent = "";
        for (const line of chartSourceLines(source)) sourceBox.appendChild(el("div", { text: line }));
        sourceBox.style.display = sourceBox.style.display === "none" ? "" : "none";
        if (sourceBox.style.display === "none") release("source");
        else hold({ source: true });
      },
    });
  }
  const showSource = () => {
    sourceBox.textContent = "";
    for (const line of chartSourceLines(source)) sourceBox.appendChild(el("div", { text: line }));
    sourceBox.style.display = "";
  };

  /* The custom range an at-this-time item applies: t ± 30 minutes. A range may not end in the future, so near "now"
     the hour is shifted back to end at the current time (it still holds t). */
  const goToTime = async (t, tab) => {
    try {
      const mod = await import("./pages/server.js");
      let start = t - AT_TIME_HALF_WINDOW_MS;
      let end = t + AT_TIME_HALF_WINDOW_MS;
      const now = Date.now();
      if (end > now) {
        end = now;
        start = now - 2 * AT_TIME_HALF_WINDOW_MS;
      }
      const err = mod.applyCustomRange(atTime.server, start, end, now, { redraw: false });
      if (err) {
        say(err);
        return;
      }
      /* A full render, not a panel redraw, so the range picker in the page head shows the new range too (#5230). Setting
         the hash to the tab already open fires no hashchange, so then the event is raised here. */
      const target = "#/server/" + encodeURIComponent(atTime.server) + "/" + tab;
      if (location.hash === target) window.dispatchEvent(new Event("hashchange"));
      else location.hash = target;
    } catch (e) {
      say("Could not open that time: " + (e && e.message ? e.message : "the page refused."));
    }
  };

  const open = (x, y, t) => {
    if (popup) close();
    const hasTime = !!atTime && Number.isFinite(t);
    const timed = hasTime ? AT_TIME_TARGETS.map((g) => ({ label: g.label, run: () => goToTime(t, g.tab) })) : [];
    const items = actions.concat(timed).map((a) => {
      const b = el("button", { class: "chart-menu-item", type: "button", role: "menuitem", text: a.label });
      b.addEventListener("click", () => {
        close();
        button.focus && button.focus();
        a.run();
      });
      return b;
    });
    popup = el("div", { class: "chart-menu", role: "menu" }, items);
    const at0 = typeof x === "number" && typeof y === "number" ? (hasTime ? { x, y, t } : { x, y }) : null;
    if (at0) {
      /* The stylesheet pins the menu to the right edge; a click position needs left/top alone. */
      popup.style.right = "auto";
      popup.style.left = at0.x + "px";
      popup.style.top = at0.y + "px";
    }
    hold({ menu: at0 || {} });
    popup.addEventListener("keydown", (e) => {
      const at = items.indexOf(typeof document !== "undefined" ? document.activeElement : null);
      if (e.key === "Tab") {
        /* Leaving the menu closes it; focus goes back to the button so Tab carries on from there. */
        button.focus && button.focus();
        close();
      } else if (e.key === "Escape") {
        close();
        button.focus && button.focus();
      } else if (e.key === "ArrowDown") {
        e.preventDefault();
        items[(at + 1) % items.length].focus();
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        items[(at - 1 + items.length) % items.length].focus();
      }
    });
    chart.appendChild(popup);
    if (at0) {
      /* Keep the whole menu inside the chart's box. */
      const box = chart.getBoundingClientRect();
      const w = popup.offsetWidth || 0;
      const h = popup.offsetHeight || 0;
      popup.style.left = Math.max(0, Math.min(at0.x, box.width - w)) + "px";
      popup.style.top = Math.max(0, Math.min(at0.y, box.height - h)) + "px";
    }
    button.setAttribute("aria-expanded", "true");
    if (items[0] && items[0].focus) items[0].focus();
    if (typeof document.addEventListener === "function") {
      outside = (e) => {
        if (e && e.target === button) return;
        close();
      };
      /* After this click finishes, so the click that opened the menu does not close it. */
      setTimeout(() => outside && document.addEventListener("click", outside), 0);
    }
  };

  button.addEventListener("click", () => (popup ? close() : open()));
  chart.addEventListener("contextmenu", (e) => {
    e.preventDefault();
    const box = chart.getBoundingClientRect();
    open(Math.max(0, e.clientX - box.left), Math.max(0, e.clientY - box.top), timeAt ? timeAt(e.clientX) : undefined);
  });
  chart.appendChild(button);
  chart.appendChild(status);
  chart.appendChild(sourceBox);
  if (restore) {
    if (restore.source && hasSource) {
      showSource();
      hold({ source: true });
    }
    if (restore.menu) {
      const m = restore.menu;
      open(typeof m.x === "number" ? m.x : undefined, typeof m.y === "number" ? m.y : undefined, typeof m.t === "number" ? m.t : undefined);
    }
  }
}

/* ─────────────────────────── client-side brush zoom ─────────────────────────── */

/* The zoom each zoomable chart holds, at MODULE scope so the 60 s poll's rebuild of the panel grid re-applies it.
   Keyed by chart identity (a title plus its read); each entry remembers the scope (server + the page's preset
   range) it was made under, and a lookup under any other scope drops it — so switching server or range starts
   every chart at its full domain. Bounded by the charts of the servers visited in one session. */
const chartZooms = new Map();

/* Hidden legend series per chart (#5247), held like the zoom: keyed by chart id, valid for one scope (server + tab +
   range). A rebuild of the same chart under the same scope redraws with the same series hidden; another server, tab
   or range is a different scope and starts with everything shown. */
const chartHiddenSeries = new Map();

/** The hidden series keys held for chart `id` under `scope` (a different scope clears the entry). */
export function getChartHidden(id, scope) {
  const h = chartHiddenSeries.get(id);
  if (!h) return [];
  if (h.scope !== scope) {
    chartHiddenSeries.delete(id);
    return [];
  }
  return h.keys;
}

/** Hold the hidden series keys for chart `id` under `scope`; an empty list clears the entry. */
export function setChartHidden(id, scope, keys) {
  if (!keys || !keys.length) chartHiddenSeries.delete(id);
  else chartHiddenSeries.set(id, { scope, keys: [...keys] });
}

/** The scope a chart's zoom is held under: the page address (server + tab, or the FinOps tab) plus the page's
 *  preset range in hours. A different server, tab or range is a different scope, so its charts start unzoomed. */
export function chartZoomScope(hours) {
  /* The `?…` query is not part of the scope (the same strip the grid sort state uses): a query-only change keeps the zoom. */
  const hash = typeof location !== "undefined" && location.hash ? location.hash.split("?")[0] : "";
  return hash + "|" + String(hours);
}

/** The zoom held for chart `id` under `scope`, or null. A different scope clears the entry. */
export function getChartZoom(id, scope) {
  const z = chartZooms.get(id);
  if (!z) return null;
  if (z.scope !== scope) {
    chartZooms.delete(id);
    return null;
  }
  return z;
}

/** Hold a zoom span (UTC-epoch ms) for chart `id` under `scope`; null from/to clears it. */
export function setChartZoom(id, scope, fromMs, toMs) {
  if (fromMs == null || toMs == null || !(toMs > fromMs)) chartZooms.delete(id);
  else chartZooms.set(id, { scope, from: fromMs, to: toMs });
}

/**
 * A line-chart spec narrowed to a zoom span: only the points already loaded that fall inside [from, to], with the
 * x-axis domain set to the span. Nothing is refetched. A span holding no loaded point leaves the spec untouched
 * (zoomed: false), so a stale zoom can never blank a chart.
 */
export function applyChartZoom(spec, zoom) {
  if (!zoom) return { spec, zoomed: false };
  const timed = [];
  for (const r of spec.points || []) {
    const d = parseUtc(r[spec.xKey]);
    if (d) timed.push({ ms: d.getTime(), r });
  }
  timed.sort((a, b) => a.ms - b.ms);
  const first = timed.findIndex((p) => p.ms >= zoom.from && p.ms <= zoom.to);
  if (first < 0) return { spec, zoomed: false };
  let last = first;
  while (last + 1 < timed.length && timed[last + 1].ms <= zoom.to) last++;
  /* One neighbour on each side of the span, so the line runs to both axis edges (the plot clips it there). */
  const lo = first > 0 ? first - 1 : first;
  const hi = last + 1 < timed.length ? last + 1 : last;
  const inside = timed.slice(lo, hi + 1).map((p) => p.r);
  return { spec: { ...spec, points: inside, windowStart: zoom.from, windowEnd: zoom.to }, zoomed: true };
}

/**
 * The reset chip shown above a zoomed chart (the Custom Views chip, shared): the span and a × that calls
 * onZoomChange(null). `zoom` is { startIso, endIso }.
 */
export function zoomChip(zoom, onZoomChange) {
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

/**
 * renderLineChart with drag-to-zoom over the points it already has. Dragging narrows the x-domain to the brushed
 * span and shows a reset chip; the zoom is held at module scope under (id, scope) so a rebuild of the same chart
 * (the poll) draws it zoomed again. `id` names the chart within its tab; `scope` names what the data was loaded
 * for (server + preset range) — a different scope draws the full domain.
 */
export function zoomableLineChart(spec, id, scope) {
  const host = el("div", { class: "zoomable-chart" });
  const allKeys = (spec.series || []).map((s) => s.key).concat(spec.series2 ? [spec.series2.key] : []);
  const draw = () => {
    const { spec: shown, zoomed } = applyChartZoom(spec, getChartZoom(id, scope));
    /* A held zoom that no longer holds a loaded point (the span aged out of the window) is dropped, not kept dormant. */
    if (!zoomed && getChartZoom(id, scope)) setChartZoom(id, scope, null, null);
    const chart = renderLineChart({
      ...shown,
      zoomed,
      menuKey: id + "|" + scope,
      exportPoints: spec.points,
      hiddenKeys: getChartHidden(id, scope),
      onLegend: (action, key) => {
        setChartHidden(id, scope, nextHiddenKeys(allKeys, getChartHidden(id, scope), action, key));
        draw();
      },
      onResetZoom: () => {
        setChartZoom(id, scope, null, null);
        draw();
      },
      onZoom: (fromMs, toMs) => {
        /* Only a span that holds a loaded point is kept; an empty brush stores nothing, so a later poll cannot zoom unasked. */
        if (applyChartZoom(spec, { from: fromMs, to: toMs }).zoomed) {
          setChartZoom(id, scope, fromMs, toMs);
          draw();
        }
      },
    });
    const z = zoomed ? getChartZoom(id, scope) : null;
    const chip = z
      ? zoomChip({ startIso: new Date(z.from).toISOString(), endIso: new Date(z.to).toISOString() }, () => {
          setChartZoom(id, scope, null, null);
          draw();
        })
      : null;
    mount(host, [chip, chart]);
  };
  draw();
  return host;
}
