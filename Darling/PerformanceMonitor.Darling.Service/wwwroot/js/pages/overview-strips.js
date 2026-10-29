/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #6291: the Overview's five stacked strips, as the desktop viewer draws them: CPU, total waits per second, blocking and
 * deadlocks, buffer pool and file I/O latency on ONE time axis, under ONE crosshair. The crosshair is a vertical line through
 * every strip, and each strip prints its own value at that time in its header. The five reads run apart and sample at their own
 * times (CPU every minute or so, a wait bucket every few minutes, a blocking bucket only when something blocked), so a strip's
 * value at the crosshair is the sample nearest to that time inside a tolerance, or "no sample" - never the neighbour's number.
 * A drag across any strip zooms all five over the points already loaded; the held zoom lives in charts.js's zoom store.
 *
 * Reads are the ones the separate charts used: get_cpu_utilization, get_server_trend (metric total_waits),
 * get_blocking_trend, get_deadlock_trend, get_memory_trend and get_file_io_trend. Every figure drawn is the read's own; the
 * only arithmetic here is the one the desktop does too: the file I/O strip is the read latency of every file at a bucket,
 * weighted by that file's reads. The reads' times are UTC stamps; the axis and the readout show them in local time. A strip with no samples says so; it is never a zero line.
 */
import { el, mount, parseUtc, axisTime, windowFromHours, readTool, readToolWithinKeptHistory, getActiveDatabaseFilter, keptWindowStrip, emptyStrip, readErrorStrip } from "../util.js";
import { chartZoomScope } from "../charts.js";
import { gapThresholdMs, splitAtGaps } from "../chart-gaps.js";

/* The held zoom, one entry per server, valid for one scope (page address + preset range): the same rule charts.js keeps for its own
   charts, held here because the strips are one chart of five lines and the zoom is a span of time, not of a chart's points. */
const heldZooms = new Map();
const getZoom = (id, scope) => {
  const z = heldZooms.get(id);
  if (!z) return null;
  if (z.scope !== scope) {
    heldZooms.delete(id);
    return null;
  }
  return z;
};
const setZoom = (id, scope, from, to) => {
  if (from == null || to == null || !(to > from)) heldZooms.delete(id);
  else heldZooms.set(id, { scope, from, to });
};

/** The reset chip above a zoomed strip stack: the span, and a × that clears it. */
function zoomChip(zoom, onReset) {
  const from = new Date(zoom.from);
  const to = new Date(zoom.to);
  const chip = el("div", { class: "drill-chip zoom-chip" }, [
    el("span", { class: "drill-label", text: "Zoomed: " + from.toLocaleString() + " → " + to.toLocaleString() }),
  ]);
  const clear = el("button", { class: "btn small drill-clear", type: "button", title: "Reset zoom", "aria-label": "Reset zoom", text: "×" });
  clear.addEventListener("click", onReset);
  chip.appendChild(clear);
  return chip;
}

const SVG_NS = "http://www.w3.org/2000/svg";
const svg = (tag, attrs) => {
  const node = document.createElementNS(SVG_NS, tag);
  if (attrs) for (const [k, v] of Object.entries(attrs)) if (v != null) node.setAttribute(k, String(v));
  return node;
};

const M_LEFT = 54;
const M_RIGHT = 12;
const HEADER_H = 30;
const PLOT_H = 54;
const STRIP_GAP = 8;
const STRIP_H = HEADER_H + PLOT_H;
const AXIS_H = 22;
const BRUSH_MIN_PX = 8;
const DEFAULT_W = 1000;

/** The sample time of a point list, in epoch ms, sorted; points with no parseable time are dropped. */
export function toTimed(rows, xKey, valueOf) {
  const out = [];
  for (const r of rows || []) {
    const d = parseUtc(r[xKey]);
    const v = valueOf(r);
    if (d && v != null && isFinite(v)) out.push({ t: d.getTime(), v });
  }
  out.sort((a, b) => a.t - b.t);
  return out;
}

/** The usual gap between a series' samples (the median step), or 0 for fewer than two. */
export function typicalStepMs(points) {
  if (points.length < 2) return 0;
  const steps = [];
  for (let i = 1; i < points.length; i++) steps.push(points[i].t - points[i - 1].t);
  steps.sort((a, b) => a - b);
  return steps[Math.floor(steps.length / 2)];
}

/**
 * The point of one series that the crosshair at `tMs` reads, or null when the series has no sample near it. "Near" is half a
 * step on each side for a level series, and 1.5% of the window for an event series, so a bar a few pixels wide is still hit. Each
 * series answers from its own times: a crosshair in a gap of the CPU samples reads no CPU value, never its neighbour's.
 */
export function valueAt(points, tMs, spanMs, isEvent = false) {
  if (!points.length) return null;
  let lo = 0;
  let hi = points.length - 1;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (points[mid].t < tMs) lo = mid + 1;
    else hi = mid;
  }
  let best = lo;
  if (lo > 0 && Math.abs(points[lo - 1].t - tMs) <= Math.abs(points[lo].t - tMs)) best = lo - 1;
  const step = typicalStepMs(points);
  /* A level sample covers half a step each side. An event series has no steady step (a bucket exists only where something happened), so it takes 1.5% of the window. */
  const tol = isEvent ? spanMs * 0.015 : step ? step * 0.5 : spanMs * 0.02;
  return Math.abs(points[best].t - tMs) <= tol ? points[best] : null;
}

/**
 * The strips' series from the six reads' replies (each `{ kind, data, message }` as readTool returns). A strip whose read failed
 * or answered nothing keeps its reason in `note` and has no series, so the page says why rather than drawing an empty axis.
 */
export function buildStrips(reads) {
  const note = (res, fallback) => (!res ? fallback : res.kind === "error" ? res.message : res.kind === "empty" ? res.message || fallback : null);
  const rowsOf = (res, key) => (res && res.kind === "data" && res.data && Array.isArray(res.data[key]) ? res.data[key] : []);

  const cpuRows = rowsOf(reads.cpu, "samples");
  const cpu = {
    key: "cpu", title: "CPU", unit: "%", min: 0, max: 100,
    series: [
      { label: "SQL", color: "#2eaef1", points: toTimed(cpuRows, "sample_time", (r) => r.sql_server_cpu) },
      { label: "Total", color: "#9aa5b1", points: toTimed(cpuRows, "sample_time", (r) => (r.sql_server_cpu ?? 0) + (r.other_process_cpu ?? 0)) },
    ],
    note: note(reads.cpu, "No CPU samples in this window."),
  };

  const waits = {
    /* From 0 like the other four strips: an axis zoomed to 292-500 made a small swing in wait time look like a spike. */
    key: "waits", title: "Waits", unit: "ms/sec", min: 0,
    series: [{ label: "Wait time", color: "#ffb74d", points: toTimed(rowsOf(reads.waits, "trend"), "time", (r) => r.wait_time_ms_per_second) }],
    note: note(reads.waits, "No wait samples in this window."),
  };

  const blocking = {
    key: "blocking", title: "Blocking", unit: "events", event: true, min: 0,
    series: [
      { label: "Blocking", color: "#ef5350", points: toTimed(rowsOf(reads.blocking, "trend"), "time", (r) => r.count) },
      { label: "Deadlocks", color: "#ab47bc", points: toTimed(rowsOf(reads.deadlocks, "trend"), "time", (r) => r.count) },
    ],
    note: null,
  };
  /* Blocking and deadlocks are events: a window with none is a true "nothing happened", said on the strip, but a read that FAILED is an error. */
  const blockNote = [reads.blocking, reads.deadlocks].find((r) => r && r.kind === "error");
  blocking.note = blockNote ? blockNote.message : "No blocking or deadlocks in this window.";

  const memory = {
    key: "memory", title: "Buffer pool", unit: "MB", min: 0,
    series: [{ label: "Buffer pool", color: "#ce93d8", points: toTimed(rowsOf(reads.memory, "trend"), "time", (r) => r.buffer_pool_mb) }],
    note: note(reads.memory, "No memory samples in this window."),
  };

  const io = {
    key: "io", title: "I/O latency", unit: "ms", min: 0,
    series: [{ label: "Read latency", color: "#81c784", points: weightedReadLatency(rowsOf(reads.io, "trend")) }],
    note: note(reads.io, "No file I/O samples in this window."),
  };

  return [cpu, waits, blocking, memory, io];
}

/** Below this a point above the band is not called anomalous (the Viewer's floors: "1% CPU above 0.5% baseline" is not news on an idle server). */
const ANOMALY_FLOOR = { cpu: 10, waits: 100, io: 2 };
/** The strips whose baseline is server-wide while the strip's own read can follow the page's database filter. */
const DATABASE_SCOPED = new Set(["io", "blocking"]);

/**
 * The Viewer's baseline band and anomaly markers (CorrelatedTimelineLanesControl), from get_overview_baselines. `res` is the read's
 * reply as readTool returns it (`{ kind, data, message }`); its `data.baselines` holds `{ mean, effective_std_dev, sample_count }` per
 * lane or null where the store has too little history. A strip gets a flat band, mean +- 2 effective sigma (lower clamped at 0), over
 * the whole window, and a marker on every point above the band at or above the lane's floor (CPU 10, waits 100, I/O 2) or below it.
 * Markers sit on the SQL CPU line only; blocking gets a band and no markers. A lane with no usable baseline gets NO band and NO
 * markers and a `baselineNote` that says why, never a band at 0. `opts.databaseFiltered` is true when the page's database filter
 * narrows the I/O and blocking reads: their baseline is server-wide, so it is not drawn against a filtered line.
 */
export function applyBaselines(strips, res, window, opts) {
  const baselines = res && res.kind === "data" && res.data && res.data.baselines ? res.data.baselines : null;
  const reason = !res ? "" : res.kind === "error" ? "Baseline unavailable." : "No baseline yet: not enough history.";
  for (const s of strips) {
    s.band = undefined;
    s.markers = undefined;
    s.baselineNote = null;
    if (!s.series.some((ser) => ser.points.length)) continue; /* nothing drawn: the strip's own note says so */
    if (opts && opts.databaseFiltered && DATABASE_SCOPED.has(s.key)) { s.baselineNote = "Baseline is server-wide: not drawn for a database filter."; continue; }
    if (!(s.key in ANOMALY_FLOOR) && s.key !== "blocking") continue; /* the buffer pool has no baseline */
    const b = baselines ? baselines[s.key] : null;
    if (!b || !(b.sample_count > 0) || !(b.effective_std_dev > 0)) { if (reason) s.baselineNote = reason; continue; }
    const upper = b.mean + 2 * b.effective_std_dev;
    const lower = Math.max(0, b.mean - 2 * b.effective_std_dev);
    s.band = [{ from: window ? window.windowStart : 0, to: window ? window.windowEnd : Number.MAX_SAFE_INTEGER, lower, upper, mean: b.mean }];
    if (s.key in ANOMALY_FLOOR) {
      const floor = ANOMALY_FLOOR[s.key];
      const line = s.series[0]; /* SQL CPU, the lane's wait time, the weighted read latency */
      s.markers = line.points.filter((p) => (p.v > upper && p.v >= floor) || p.v < lower).map((p) => ({ t: p.t, v: p.v }));
    }
  }
  return strips;
}

/**
 * One server-wide read latency per bucket from get_file_io_trend's per-file rows: each row's average weighted by its reads, as the
 * desktop's lane does (a file with few reads must not pull the line as hard as a busy one). The read already folds files past
 * its charted few into an "(other)" row, so the weights cover every file. A bucket with no reads has no latency and is left out.
 */
export function weightedReadLatency(rows) {
  const byTime = new Map();
  for (const r of rows || []) {
    if (r.avg_read_latency_ms == null || !(r.reads > 0)) continue;
    const d = parseUtc(r.time);
    if (!d) continue;
    const t = d.getTime();
    const acc = byTime.get(t) || { ms: 0, reads: 0 };
    acc.ms += r.avg_read_latency_ms * r.reads;
    acc.reads += r.reads;
    byTime.set(t, acc);
  }
  return [...byTime.entries()].sort((a, b) => a[0] - b[0]).map(([t, a]) => ({ t, v: a.ms / a.reads }));
}

/** The time axis the strips share: the zoom when one is held, else the read window; widened to the data when no window is known. */
export function sharedDomain(strips, window, zoom) {
  if (zoom) return { from: zoom.from, to: zoom.to };
  let from = window ? window.windowStart : Infinity;
  let to = window ? window.windowEnd : -Infinity;
  if (!window) {
    for (const s of strips) for (const ser of s.series) for (const p of ser.points) {
      if (p.t < from) from = p.t;
      if (p.t > to) to = p.t;
    }
  }
  return isFinite(from) && to > from ? { from, to } : null;
}

/** The strips' points inside a span, one neighbour either side so a line runs to the axis edge (the charts' own rule). */
export function clipToSpan(points, from, to) {
  const first = points.findIndex((p) => p.t >= from && p.t <= to);
  if (first < 0) return [];
  let last = first;
  while (last + 1 < points.length && points[last + 1].t <= to) last++;
  return points.slice(first > 0 ? first - 1 : first, last + 2 > points.length ? points.length : last + 2);
}

const fmtNum = (v) => (Math.abs(v) >= 100 ? Math.round(v) : Math.round(v * 10) / 10).toLocaleString();

function niceMax(v) {
  if (!(v > 0)) return 1;
  const pow = Math.pow(10, Math.floor(Math.log10(v)));
  for (const m of [1, 2, 2.5, 5, 10]) if (m * pow >= v) return m * pow;
  return 10 * pow;
}

/* The axis labels and the crosshair readout read in the browser's local time, through the same formatter the line charts use, so they
   agree with the zoom chip and the rest of the page (#6360); the date joins the clock when the window is longer than a day and a half. */
function timeLabel(ms, spanMs) {
  return axisTime(new Date(ms), spanMs > 36 * 3600000);
}

/**
 * The drawn strips: one SVG, one block per strip, a shared x scale. `onZoom(fromMs, toMs)` is called after a drag across any strip.
 * `state` is read at draw time: `{ strips, domain }`. Returns the node and `crosshairAt(tMs)` (the same call a pointer move makes), so
 * a behaviour test can drive the crosshair without a pointer.
 */
export function drawStrips(strips, domain, width, onZoom) {
  const W = width > 120 ? width : DEFAULT_W;
  const plotL = M_LEFT;
  const plotR = W - M_RIGHT;
  const plotW = plotR - plotL;
  const span = domain.to - domain.from;
  const H = strips.length * (STRIP_H + STRIP_GAP) + AXIS_H;
  const x = (t) => plotL + ((t - domain.from) / span) * plotW;
  const root = svg("svg", { class: "overview-strips", width: W, height: H, viewBox: "0 0 " + W + " " + H, role: "img", "aria-label": "Overview strips on a shared time axis" });
  root.style.touchAction = "pan-y";
  const readouts = [];

  strips.forEach((s, i) => {
    const top = i * (STRIP_H + STRIP_GAP);
    const py0 = top + HEADER_H;
    const clipped = s.series.map((ser) => ({ ...ser, shown: clipToSpan(ser.points, domain.from, domain.to) }));
    const all = clipped.flatMap((c) => c.shown.filter((p) => p.t >= domain.from && p.t <= domain.to).map((p) => p.v));
    const hasData = all.length > 0;
    let yMin = s.min != null ? s.min : hasData ? Math.min(...all) : 0;
    let yMax = s.max != null ? s.max : niceMax(hasData ? Math.max(...all) * 1.05 : 1);
    if (s.min == null && yMax <= yMin) yMax = yMin + 1;
    const y = (v) => py0 + PLOT_H - ((v - yMin) / (yMax - yMin || 1)) * PLOT_H;

    const g = svg("g", { class: "strip", "data-strip": s.key });
    g.appendChild(svg("line", { class: "grid-line", x1: plotL, x2: plotR, y1: py0 + PLOT_H, y2: py0 + PLOT_H }));
    g.appendChild(svg("line", { class: "grid-line", x1: plotL, x2: plotR, y1: py0, y2: py0 }));
    const axis = svg("g", { class: "axis" });
    const topLabel = svg("text", { x: plotL - 6, y: py0 + 9, "text-anchor": "end" });
    topLabel.textContent = fmtNum(yMax);
    const botLabel = svg("text", { x: plotL - 6, y: py0 + PLOT_H, "text-anchor": "end" });
    botLabel.textContent = fmtNum(yMin);
    axis.appendChild(topLabel);
    axis.appendChild(botLabel);
    g.appendChild(axis);

    const title = svg("text", { class: "strip-title", x: plotL, y: top + 12 });
    title.textContent = s.title + " (" + s.unit + ")";
    g.appendChild(title);
    const readout = svg("text", { class: "strip-readout", x: plotL, y: top + 25 });
    readout.textContent = "";
    g.appendChild(readout);
    readouts.push({ strip: s, node: readout });

    /* Part B's baseline band sits behind the line; drawn only when the strip carries one. */
    for (const b of s.band || []) {
      const x1 = Math.max(plotL, x(b.from));
      const x2 = Math.min(plotR, x(b.to));
      if (x2 > x1) {
        g.appendChild(svg("rect", { class: "strip-band", x: x1, y: y(Math.min(b.upper, yMax)), width: x2 - x1, height: Math.max(1, y(Math.max(b.lower, yMin)) - y(Math.min(b.upper, yMax))) }));
        /* The Viewer's dashed mean line, when the mean sits inside the strip's scale. */
        if (b.mean != null && b.mean >= yMin && b.mean <= yMax) g.appendChild(svg("line", { class: "strip-band-mean", x1, x2, y1: y(b.mean), y2: y(b.mean) }));
      }
    }
    /* A strip with data but no band says why, in its header: never a band at 0. */
    if (s.baselineNote && hasData) {
      const bn = svg("text", { class: "strip-baseline-note", x: plotR, y: top + 12, "text-anchor": "end" });
      bn.textContent = s.baselineNote;
      g.appendChild(bn);
    }

    if (!hasData) {
      const none = svg("text", { class: "strip-none", x: plotL + plotW / 2, y: py0 + PLOT_H / 2 + 4, "text-anchor": "middle" });
      none.textContent = s.note || "No samples in this window.";
      g.appendChild(none);
    } else {
      for (const c of clipped) {
        if (!c.shown.length) continue;
        if (s.event) {
          const barW = Math.max(2, Math.min(8, plotW / 120));
          for (const p of c.shown) {
            if (p.t < domain.from || p.t > domain.to) continue;
            g.appendChild(svg("rect", { x: x(p.t) - barW / 2, y: y(p.v), width: barW, height: Math.max(1, py0 + PLOT_H - y(p.v)), fill: c.color, opacity: 0.85 }));
          }
        } else if (c.shown.length === 1) {
          g.appendChild(svg("circle", { cx: x(c.shown[0].t), cy: y(c.shown[0].v), r: 2.5, fill: c.color }));
        } else {
          /* A stretch with no samples (the service was off) is a break in the line, not a diagonal across it: charts.js's gap rule, judged over the whole series so a zoom cannot invent a gap. */
          const times = c.shown.map((p) => p.t);
          const runs = splitAtGaps(times.map((_, i) => i), times, gapThresholdMs(c.points.map((p) => p.t)));
          for (const run of runs) {
            if (run.length === 1) {
              g.appendChild(svg("circle", { cx: x(c.shown[run[0]].t), cy: y(c.shown[run[0]].v), r: 2.5, fill: c.color }));
              continue;
            }
            const pts = run.map((i) => Math.max(plotL, Math.min(plotR, x(c.shown[i].t))).toFixed(1) + "," + y(c.shown[i].v).toFixed(1)).join(" ");
            g.appendChild(svg("polyline", { points: pts, fill: "none", stroke: c.color, "stroke-width": 1.6, "stroke-linejoin": "round" }));
          }
        }
      }
    }
    for (const m of s.markers || []) {
      if (m.t < domain.from || m.t > domain.to) continue;
      g.appendChild(svg("circle", { class: "strip-anomaly", cx: x(m.t), cy: y(Math.min(Math.max(m.v, yMin), yMax)), r: 3 }));
    }
    root.appendChild(g);
  });

  /* The shared time axis under the last strip. */
  const axisTop = strips.length * (STRIP_H + STRIP_GAP) - STRIP_GAP;
  const ticks = Math.max(2, Math.min(8, Math.floor(plotW / 90)));
  const axisG = svg("g", { class: "axis" });
  for (let i = 0; i <= ticks; i++) {
    const t = domain.from + (span * i) / ticks;
    const lab = svg("text", { x: x(t), y: axisTop + 15, "text-anchor": i === 0 ? "start" : i === ticks ? "end" : "middle" });
    lab.textContent = timeLabel(t, span);
    axisG.appendChild(lab);
  }
  root.appendChild(axisG);

  /* One crosshair through every strip, one readout per strip. */
  const hover = svg("line", { class: "hover-line", y1: 0, y2: axisTop, style: "display:none" });
  const brush = svg("rect", { class: "brush-rect", y: 0, height: axisTop, style: "display:none" });
  root.appendChild(hover);
  root.appendChild(brush);

  const crosshairAt = (tMs) => {
    const found = [];
    hover.setAttribute("x1", x(tMs));
    hover.setAttribute("x2", x(tMs));
    hover.style.display = "";
    for (const r of readouts) {
      const parts = [];
      for (const ser of r.strip.series) {
        const p = valueAt(ser.points, tMs, span, r.strip.event === true);
        if (p) parts.push(ser.label + " " + fmtNum(p.v) + " " + r.strip.unit);
        found.push({ strip: r.strip.key, series: ser.label, t: p ? p.t : null, value: p ? p.v : null });
      }
      const none = parts.length === 0;
      r.node.textContent = (timeLabel(tMs, span) + "  ") + (none ? "no sample here" : parts.join("  |  "));
    }
    return found;
  };
  const clearCrosshair = () => {
    hover.style.display = "none";
    for (const r of readouts) r.node.textContent = "";
  };
  const timeOf = (clientX) => {
    const rect = root.getBoundingClientRect();
    const vb = ((clientX - rect.left) / (rect.width || W)) * W;
    return domain.from + (Math.max(0, Math.min(1, (vb - plotL) / plotW)) * span);
  };
  const vbOf = (clientX) => {
    const rect = root.getBoundingClientRect();
    return ((clientX - rect.left) / (rect.width || W)) * W;
  };

  let dragFrom = null;
  root.addEventListener("pointerdown", (ev) => {
    if (ev.button != null && ev.button !== 0) return;
    dragFrom = vbOf(ev.clientX);
    if (typeof root.setPointerCapture === "function") root.setPointerCapture(ev.pointerId);
  });
  root.addEventListener("pointermove", (ev) => {
    crosshairAt(timeOf(ev.clientX));
    if (dragFrom == null) return;
    const xNow = vbOf(ev.clientX);
    const left = Math.max(plotL, Math.min(dragFrom, xNow));
    const right = Math.min(plotR, Math.max(dragFrom, xNow));
    brush.setAttribute("x", left);
    brush.setAttribute("width", Math.max(0, right - left));
    brush.style.display = "";
  });
  root.addEventListener("pointerup", (ev) => {
    if (dragFrom == null) return;
    const from = dragFrom;
    dragFrom = null;
    brush.style.display = "none";
    const to = vbOf(ev.clientX);
    if (Math.abs(to - from) < BRUSH_MIN_PX) return;
    const clampT = (vb) => domain.from + (Math.max(0, Math.min(1, (vb - plotL) / plotW)) * span);
    const t1 = clampT(Math.min(from, to));
    const t2 = clampT(Math.max(from, to));
    if (t2 > t1 && onZoom) onZoom(t1, t2);
  });
  root.addEventListener("pointerleave", () => {
    if (dragFrom == null) clearCrosshair();
  });
  root.addEventListener("pointercancel", () => {
    dragFrom = null;
    brush.style.display = "none";
  });

  return { node: root, crosshairAt, clearCrosshair };
}

const READS = [
  ["cpu", "get_cpu_utilization", {}],
  ["waits", "get_server_trend", { metric: "total_waits" }],
  ["blocking", "get_blocking_trend", {}],
  ["deadlocks", "get_deadlock_trend", {}],
  ["memory", "get_memory_trend", {}],
  ["io", "get_file_io_trend", {}],
];

/**
 * Fill `body` (a panel's body) with the five strips for `server`. `ctx` is the tab's `{ hours, label, signal }`. The six reads go
 * out together; a strip whose read failed keeps its error on its own row while the others draw.
 */
export function fillOverviewStrips(body, server, ctx) {
  (async () => {
    const asked = windowFromHours(ctx.hours);
    const baselineStart = new Date(asked ? asked.windowStart : Date.now() - ctx.hours * 3600000).toISOString();
    const [results, baselineRes] = await Promise.all([
      Promise.all(READS.map(([, tool, extra]) =>
        readToolWithinKeptHistory(tool, { server, hours: ctx.hours, ...extra }, ctx && ctx.signal).catch((e) => ({ kind: "error", message: String((e && e.message) || e) })))),
      /* The Viewer's four baselines at the window start, from the one web-only read; a failure costs the bands, never the lines. */
      readTool("get_overview_baselines", { server, window_start: baselineStart }, ctx && ctx.signal).catch((e) => ({ kind: "error", message: String((e && e.message) || e) })),
    ]);
    /* #6793: the batch was cancelled (a redraw, or the user left the page). Nothing is drawn into a body nobody is looking at. */
    if (baselineRes.kind === "aborted" || results.some((r) => r.kind === "aborted")) return;
    const reads = {};
    READS.forEach(([key], i) => { reads[key] = results[i]; });
    const strips = buildStrips(reads);
    const anyData = strips.some((s) => s.series.some((ser) => ser.points.length));
    const allFailed = results.every((r) => r.kind === "error");
    if (allFailed) return mount(body, readErrorStrip(results[0].message));
    if (!anyData) return mount(body, [results.map((r) => keptWindowStrip(r)).find(Boolean) || null, emptyStrip("No overview samples in this window.")]);

    const kept = results.map((r) => r.keptHours).filter(Boolean);
    const window = windowFromHours(kept.length ? Math.min(...kept) : ctx.hours);
    const dbFilter = getActiveDatabaseFilter();
    applyBaselines(strips, baselineRes, window, { databaseFiltered: !!(dbFilter && dbFilter.server === server) });
    const id = "overview-strips|" + server;
    const scope = chartZoomScope(ctx.hours);
    const host = el("div", { class: "chart overview-strips-host" });
    /* The width the strips were last drawn at; 0 when the host was not laid out yet (drawStrips then falls back to its default width). */
    let drawnW = 0;
    const draw = () => {
      const held = getZoom(id, scope);
      const domain = sharedDomain(strips, window, held);
      if (!domain) return mount(host, emptyStrip("No overview samples in this window."));
      const width = host.getBoundingClientRect ? host.getBoundingClientRect().width : 0;
      drawnW = width;
      const { node } = drawStrips(strips, domain, width, (from, to) => {
        setZoom(id, scope, from, to);
        draw();
      });
      const chip = held
        ? zoomChip(held, () => {
            setZoom(id, scope, null, null);
            draw();
          })
        : null;
      mount(host, [chip, node]);
    };
    draw();
    mount(body, [keptWindowStrip(results.find((r) => r.keptHours || r.presetHours) || null), host]);
    /* The first draw ran before the host was in the page, so it measured 0 and drew at the default width, which the browser then scaled
       down to the real one (#6360). A resize (a phone turned, a panel widened) redraws at the new width, as the line charts do, and so
       does the observer's first report, which is the host's laid-out width: it is compared with the width the strips were drawn at, not
       with one read right after mounting. The zoom is held, so it carries over. */
    if (typeof ResizeObserver === "function") {
      new ResizeObserver(() => {
        const w = host.getBoundingClientRect().width;
        if (host.isConnected !== false && w > 0 && Math.abs(w - drawnW) > 2) draw();
      }).observe(host);
    }
  })();
  return body;
}
