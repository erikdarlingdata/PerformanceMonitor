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

import { el, parseUtc, axisTime, emptyStrip } from "./util.js";

const SVG_NS = "http://www.w3.org/2000/svg";

/* viewBox geometry — the SVG scales to its container width via CSS (width:100%, height:auto). */
const W = 1000;
const H = 320;
/* Top margin leaves headroom for the y-axis unit caption to sit fully clear of the top tick's label. */
const M = { l: 58, r: 16, t: 26, b: 30 };
const PLOT_H = H - M.t - M.b;
const Y_TICKS = 4;

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
 *   series2    — optional dual-axis overlay series (#1606): { key, label, color, formatValue?, unit? }. Drawn
 *                against its OWN right-hand y-axis (own nice scale, own unit caption) so two measures with
 *                different magnitudes read together. Only line/area modes carry one (validation upstream).
 *   onZoom     — optional brush-zoom callback (#1606): a pointer drag across ≥8px of plot selects a time
 *                range and calls onZoom(fromMs, toMs) so the caller can RE-RUN the panel on that window
 *                (server-side re-run keeps bucket resolution + tier routing + the partial-window notice honest).
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
  const { points, xKey, series, formatValue = (v) => String(v), clampMax = null, unit = null, mode = "line", thresholds = null, annotations = null, onSelect = null, series2 = null, onZoom = null, windowStart = null, windowEnd = null } = spec;
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
    if (dataMax === -Infinity) return el("div", { class: "chart" }, [emptyStrip("No numeric values to chart.")]);
    dataMin = Math.min(0, dataMin);
  }
  if (dataMax === dataMin) dataMax = dataMin + 1;

  /* Nice-rounded domain so gridline labels land on round values; percentage charts (clampMax=100) cap the top
     at 100 and never exceed it, so a 96% reading no longer rounds the axis up to a "120%" tick. */
  const scale = niceScale(dataMin, dataMax, Y_TICKS, clampMax);
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
    const s2 = niceScale(n2, m2, Y_TICKS, null);
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
            points: `${first},${baseY} ${linePts.join(" ")} ${last},${baseY}`,
            fill: normalizeColor(s.color),
            "fill-opacity": "0.15",
          })
        );
      }
      root.appendChild(svg("polyline", { class: "series-line", points: linePts.join(" "), stroke: normalizeColor(s.color) }));
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
      root.appendChild(svg("polyline", { class: "series-line series-line-overlay", points: pts2.join(" "), stroke: normalizeColor(series2.color) }));
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
  chart.appendChild(buildLegend(series2 ? series.concat([{ label: series2.label, color: series2.color }]) : series, onSelect));
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
    let idx = 0;
    let best = Infinity;
    for (let i = 0; i < xs.length; i++) {
      const d = Math.abs(xs[i] - vbX);
      if (d < best) {
        best = d;
        idx = i;
      }
    }
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
 *  keyboard) control that calls onSelect(series.drill) to re-run the panel filtered to that series' group value. */
function buildLegend(series, onSelect) {
  return el(
    "div",
    { class: "chart-legend" },
    series.map((s) => {
      const drillable = !!(onSelect && s.drill);
      const props = { class: drillable ? "item drillable" : "item" };
      if (drillable) {
        props.onActivate = () => onSelect(s.drill);
        props.title = "Filter to " + s.label;
      }
      return el("span", props, [
        el("span", { class: "swatch", style: "background:" + normalizeColor(s.color) }),
        el("span", { text: s.label }),
      ]);
    })
  );
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
 */
function niceScale(min, max, maxTicks, clampMax) {
  const range = niceNum(max - min || 1, false);
  const step = niceNum(range / Math.max(1, maxTicks), true) || 1;
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
