/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The time-range slicer (#6301): the web twin of the Darling Viewer's TimeRangeSlicerControl. A compact strip of activity over the page's
 * current range, with a selection you drag; on release the page's grids read again for the selected window only. Chart drag-zoom
 * (charts.js) changes one chart and never a grid; this narrows what the grid asks for.
 *
 *   - The strip is a fluid SVG (viewBox + preserveAspectRatio none) under a div track, so it follows the panel's width with no resize
 *     watching and works at 390 px. The selection, its two handles and the dimmed ends are divs positioned in percent.
 *   - A drag (mouse, touch or pen: pointer events) makes a new selection, moves the selection or pulls one edge; the release commits it
 *     through `onCommit`. The keyboard does the same (arrow keys on a handle move that edge, on the selection move all of it, Home and End
 *     jump it to either end, Escape resets), committing once the keys stop so a held key does not read a grid per step.
 *   - Reset puts the whole page range back. A selection that spans the whole range is no narrowing at all and is committed as `null`.
 *   - `setOverlay` draws one row's own timeline over the strip, scaled to its own peak (the Viewer's overlay-on-select).
 *
 * The narrowed window is page state, held here at module scope per server and keyed by the page range's `rangeKey`: it survives a
 * re-sort, a 60 s rebuild and a tab switch, and is dropped the moment the page range is a different one.
 *
 * Pure helpers (windowOf, narrowedContext, dragSelection, ...) take the now and the window as arguments, so a Node harness drives them
 * without a clock.
 */

import { el, localTime, TRIM_TO } from "./util.js";
import { MINIMUM_SPAN_MS } from "./time-range.js";

const HOUR_MS = 3600000;
const MINUTE_MS = 60000;
const SVG_NS = "http://www.w3.org/2000/svg";
/* The viewBox the strip is drawn in: x is a percent of the range, y a height of 40 units. */
const VB_W = 100;
const VB_H = 40;
/* A selection covering this much of the range or more is the whole range. */
const FULL_FRACTION = 0.995;
/* The keyboard commits this long after the last arrow key. */
const KEY_COMMIT_MS = 350;

/* ───────────────────────────── page state ───────────────────────────── */

const slices = new Map();
const listeners = new Set();

/** The narrowed window held for `server` under page range `rangeKey` as `{ startMs, endMs }`, or null. A window held under another range
 *  is dropped here: the page range changing resets the narrowing. */
export function getSlice(server, rangeKey) {
  const held = slices.get(server);
  if (!held) return null;
  if (held.rangeKey !== rangeKey) {
    slices.delete(server);
    return null;
  }
  return { startMs: held.startMs, endMs: held.endMs };
}

/** Hold `sel` ({ startMs, endMs }, or null for the whole range) for `server` under `rangeKey`, and tell every listener (the cards on screen). */
export function setSlice(server, rangeKey, sel) {
  /* An unchanged window changes nothing, so no card reads again: a click with no travel on a handle or the selection, or Escape with nothing narrowed. */
  const held = slices.get(server);
  const valid = sel && Number.isFinite(sel.startMs) && Number.isFinite(sel.endMs) && sel.endMs > sel.startMs;
  if (!valid && !held) return;
  if (valid && held && held.rangeKey === rangeKey && held.startMs === sel.startMs && held.endMs === sel.endMs) return;
  if (sel && Number.isFinite(sel.startMs) && Number.isFinite(sel.endMs) && sel.endMs > sel.startMs) slices.set(server, { rangeKey, startMs: sel.startMs, endMs: sel.endMs });
  else slices.delete(server);
  for (const fn of [...listeners]) fn(server);
}

/** Calls `fn(server)` after every setSlice. Returns the unsubscribe. */
export function onSliceChange(fn) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

/* ───────────────────────────── pure helpers ───────────────────────────── */

/** The page range as `{ startMs, endMs }`: the exact pair the page context carries, or `hours` ending at `nowMs`. */
export function windowOf(ctx, nowMs = Date.now()) {
  if (ctx && Number.isFinite(ctx.startMs) && Number.isFinite(ctx.endMs) && ctx.endMs > ctx.startMs) return { startMs: ctx.startMs, endMs: ctx.endMs };
  const hours = Math.max(1, Number(ctx && ctx.hours) || 1);
  return { startMs: nowMs - hours * HOUR_MS, endMs: nowMs };
}

/** The key that says which page range a narrowing belongs to. */
export function rangeKeyOf(ctx) {
  return ctx && ctx.rangeKey ? String(ctx.rangeKey) : "hours:" + (ctx && ctx.hours);
}

/** `sel` clipped to `win`, rounded to whole minutes and kept at least `minSpanMs` wide; null when it lies outside `win` or is the whole of it. */
export function clampSlice(sel, win, minSpanMs) {
  if (!sel || !win) return null;
  const minSpan = minSpanMs || MINIMUM_SPAN_MS;
  const span = win.endMs - win.startMs;
  if (!(span > minSpan)) return null;
  let start = Math.max(win.startMs, Math.min(sel.startMs, sel.endMs));
  let end = Math.min(win.endMs, Math.max(sel.startMs, sel.endMs));
  if (!(end > start)) return null;
  start = Math.round(start / MINUTE_MS) * MINUTE_MS;
  end = Math.round(end / MINUTE_MS) * MINUTE_MS;
  if (end - start < minSpan) {
    end = Math.min(win.endMs, start + minSpan);
    start = Math.max(win.startMs, end - minSpan);
  }
  if (end - start >= span * FULL_FRACTION) return null;
  return { startMs: start, endMs: end };
}

/**
 * The page context the grids are built from while narrowed: the same context with `hours` (whole hours, at least 1), `asOf` (the selection's
 * end) and a label for the selection. The reads take whole hours back from an end, so a selection that is not a whole number of hours is
 * read as the hours that hold it, and the label says so: a ranking is a total over that span, and cannot be trimmed to the minute.
 */
export function narrowedContext(ctx, sel) {
  const span = sel.endMs - sel.startMs;
  const hours = Math.max(1, Math.ceil(span / HOUR_MS - 1e-9));
  const from = localTime(new Date(sel.startMs).toISOString());
  const to = localTime(new Date(sel.endMs).toISOString());
  const rounded = hours * HOUR_MS > span + MINUTE_MS ? "; totals and rankings aggregate over the " + hours + " whole hour" + (hours === 1 ? "" : "s") + " ending then" : "";
  return { ...ctx, hours, asOf: new Date(sel.endMs).toISOString(), startMs: sel.startMs, endMs: sel.endMs, narrowed: true, label: "narrowed to " + from + " to " + to + rounded };
}

/** The context a card builds its grid from: the page context itself, or its narrowed form while `server` holds a selection inside the range. */
export function effectiveContext(server, ctx, nowMs = Date.now()) {
  const sel = clampSlice(getSlice(server, rangeKeyOf(ctx)), windowOf(ctx, nowMs));
  return sel ? narrowedContext(ctx, sel) : ctx;
}

/** `params` for a read over the context's window: `hours`, and `as_of` when the context is narrowed (an explicit end is never re-anchored by the page range).
 *  `trim` (event lists: blocking, deadlocks, active snapshots) also cuts the answer's rows to the selection itself, because the narrowed read is whole hours
 *  back from its end and would list events from before the selection; totals and rankings leave it off, their label says they aggregate. */
export function windowParams(params, w, trim = false) {
  if (!(w && w.narrowed)) return { ...params, hours: w.hours };
  const out = { ...params, hours: w.hours, as_of: w.asOf };
  if (trim && Number.isFinite(w.startMs) && Number.isFinite(w.endMs)) out[TRIM_TO] = { startMs: w.startMs, endMs: w.endMs };
  return out;
}

/**
 * What a History button reads (#6301): the grid's EFFECTIVE window. `w` is the context the grid was built from (the page context, or its
 * narrowed form while the time slicer holds a selection) or bare hours. Returns the `hours` to read, the extra read parameters (`as_of` while
 * narrowed, so the history ends where the selection does), a `stamp` part that tells a read made for another window from this one, and the
 * chart `axis` ({ windowStart, windowEnd }) for a narrowed window, or null where the caller's own clock-based axis applies.
 */
export function historyWindow(w) {
  const ctx = typeof w === "number" ? { hours: w } : w || {};
  if (!ctx.narrowed) return { hours: ctx.hours, extra: {}, stamp: "", axis: null };
  const end = Date.parse(ctx.asOf);
  return { hours: ctx.hours, extra: { as_of: ctx.asOf }, stamp: "@" + ctx.asOf, axis: { windowStart: end - ctx.hours * HOUR_MS, windowEnd: end } };
}

/**
 * One pointer move of a drag, in fractions (0..1) of the range. `mode`: "new" (from `anchor` to `cur`), "move" (the whole selection by
 * `cur - anchor`), "start" or "end" (that edge to `cur`). `sel` is `{ a, b }` as the drag began; `minFrac` the narrowest selection.
 */
export function dragSelection(mode, anchor, cur, sel, minFrac) {
  const unit = (v) => Math.max(0, Math.min(1, v));
  if (mode === "move") {
    const width = sel.b - sel.a;
    const a = unit(sel.a + (cur - anchor));
    const left = Math.min(a, 1 - width);
    return { a: left, b: left + width };
  }
  if (mode === "start") return { a: Math.max(0, Math.min(unit(cur), sel.b - minFrac)), b: sel.b };
  if (mode === "end") return { a: sel.a, b: Math.min(1, Math.max(unit(cur), sel.a + minFrac)) };
  let a = unit(Math.min(anchor, cur));
  let b = unit(Math.max(anchor, cur));
  if (b - a < minFrac) {
    b = Math.min(1, a + minFrac);
    a = Math.max(0, b - minFrac);
  }
  return { a, b };
}

/** The SVG path d strings (area and line) for points [{ t, v }] over a window, scaled to their own peak. */
export function stripPaths(points, win, peak = null) {
  const span = win.endMs - win.startMs;
  const usable = (points || []).filter((p) => p && Number.isFinite(p.t) && Number.isFinite(p.v) && p.t >= win.startMs - 1 && p.t <= win.endMs + 1).sort((x, y) => x.t - y.t);
  if (!usable.length || !(span > 0)) return { line: "", area: "", peak: 0 };
  const top = peak || Math.max(...usable.map((p) => p.v), 0) || 1;
  const xy = usable.map((p) => [((p.t - win.startMs) / span) * VB_W, VB_H - 2 - Math.max(0, p.v / top) * (VB_H - 6)]);
  const line = xy.map(([x, y], i) => (i ? "L" : "M") + x.toFixed(2) + " " + y.toFixed(2)).join(" ");
  const area = line + " L" + xy[xy.length - 1][0].toFixed(2) + " " + VB_H + " L" + xy[0][0].toFixed(2) + " " + VB_H + " Z";
  return { line, area, peak: top };
}

/* ───────────────────────────── the control ───────────────────────────── */

function svgNode(tag, attrs) {
  const node = document.createElementNS(SVG_NS, tag);
  for (const [k, v] of Object.entries(attrs || {})) node.setAttribute(k, v);
  return node;
}

const pct = (f) => (f * 100).toFixed(3) + "%";

/**
 * Builds one slicer. Returns `{ node, setPoints, setOverlay, clearOverlay, setSelection, selection }`, or `node: null` when the range is too
 * short to narrow (no more than the minimum span).
 *
 * @param {object} o
 * @param {{startMs:number,endMs:number}} o.win the page range the strip spans
 * @param {{startMs:number,endMs:number}|null} [o.selection] the narrowed window held now
 * @param {string} [o.title] what the strip charts ("Query duration")
 * @param {(sel:{startMs:number,endMs:number}|null)=>void} o.onCommit called on release (or after the keys stop) with the new window, null for the whole range
 * @param {number} [o.minSpanMs] the narrowest selection
 * @param {number} [o.keyDelayMs] how long after the last arrow key the selection commits
 */
export function timeSlicer(o) {
  const win = o.win;
  const span = win.endMs - win.startMs;
  const minSpan = o.minSpanMs || MINIMUM_SPAN_MS;
  const keyDelay = o.keyDelayMs == null ? KEY_COMMIT_MS : o.keyDelayMs;
  if (!(span > minSpan)) return { node: null, setPoints() {}, setNote() {}, setOverlay() {}, clearOverlay() {}, setSelection() {}, selection: () => null };
  const minFrac = minSpan / span;
  const stepFrac = Math.max(minFrac, 1 / 40);

  let frac = { a: 0, b: 1 };
  let narrowed = false;
  let points = [];
  let overlay = null;
  let overlayLabel = "";
  let keyTimer = null;

  const toFrac = (sel) => ({ a: (sel.startMs - win.startMs) / span, b: (sel.endMs - win.startMs) / span });
  const toSel = (f) => ({ startMs: win.startMs + f.a * span, endMs: win.startMs + f.b * span });

  const line = svgNode("path", { class: "slicer-line" });
  const area = svgNode("path", { class: "slicer-area" });
  const overlayLine = svgNode("path", { class: "slicer-overlay" });
  const svg = svgNode("svg", { class: "slicer-svg", viewBox: "0 0 " + VB_W + " " + VB_H, preserveAspectRatio: "none", "aria-hidden": "true", focusable: "false" });
  svg.appendChild(area);
  svg.appendChild(line);
  svg.appendChild(overlayLine);

  const shadeL = el("div", { class: "slicer-shade slicer-shade-l" });
  const shadeR = el("div", { class: "slicer-shade slicer-shade-r" });
  const hStart = el("div", { class: "slicer-handle slicer-handle-start", role: "slider", tabindex: "0", "aria-label": "Start of the narrowed range" });
  const hEnd = el("div", { class: "slicer-handle slicer-handle-end", role: "slider", tabindex: "0", "aria-label": "End of the narrowed range" });
  const body = el("div", { class: "slicer-sel", role: "group", tabindex: "0", "aria-label": "Narrowed range. Arrow keys move it, Home and End jump it, Escape resets it." }, [hStart, hEnd]);
  const track = el("div", { class: "slicer-track" }, [svg, shadeL, shadeR, body]);
  const readout = el("span", { class: "slicer-readout", role: "status", "aria-live": "polite" });
  const reset = el("button", { type: "button", class: "btn slicer-reset", text: "Reset", title: "Back to the whole range" });
  const overlayNote = el("span", { class: "slicer-overlay-note" });
  const stripNote = el("span", { class: "slicer-strip-note" });
  const node = el("div", { class: "slicer", "data-slicer": o.title || "slicer" }, [
    el("div", { class: "slicer-head" }, [el("span", { class: "slicer-title", text: o.title || "Time range" }), readout, stripNote, overlayNote, reset]),
    track,
    el("div", { class: "slicer-axis" }, [
      el("span", { text: localTime(new Date(win.startMs).toISOString()) }),
      el("span", { text: localTime(new Date(win.endMs).toISOString()) }),
    ]),
  ]);

  const timeText = (ms) => localTime(new Date(ms).toISOString());
  const paint = () => {
    body.style.left = pct(frac.a);
    body.style.width = pct(frac.b - frac.a);
    shadeL.style.width = pct(frac.a);
    shadeR.style.width = pct(1 - frac.b);
    const sel = toSel(frac);
    const mins = Math.round((sel.endMs - sel.startMs) / MINUTE_MS);
    readout.textContent = narrowed
      ? "Narrowed to " + timeText(sel.startMs) + " to " + timeText(sel.endMs) + " (" + (mins >= 120 ? Math.round(mins / 6) / 10 + " hours" : mins + " min") + "). The grids show this window."
      : "Whole range. Drag across the strip to narrow the grids.";
    reset.style.display = narrowed ? "" : "none";
    hStart.setAttribute("aria-valuemin", "0");
    hStart.setAttribute("aria-valuemax", "100");
    hStart.setAttribute("aria-valuenow", String(Math.round(frac.a * 100)));
    hStart.setAttribute("aria-valuetext", timeText(sel.startMs));
    hEnd.setAttribute("aria-valuemin", "0");
    hEnd.setAttribute("aria-valuemax", "100");
    hEnd.setAttribute("aria-valuenow", String(Math.round(frac.b * 100)));
    hEnd.setAttribute("aria-valuetext", timeText(sel.endMs));
  };
  const paintStrip = () => {
    const p = stripPaths(points, win);
    line.setAttribute("d", p.line);
    area.setAttribute("d", p.area);
    const q = overlay ? stripPaths(overlay, win) : { line: "" };
    overlayLine.setAttribute("d", q.line);
    overlayNote.textContent = overlay && q.line ? "Overlay: " + overlayLabel : "";
  };

  const commit = () => {
    if (keyTimer) {
      clearTimeout(keyTimer);
      keyTimer = null;
    }
    const sel = frac.a <= 0 && frac.b >= 1 ? null : clampSlice(toSel(frac), win, minSpan);
    narrowed = !!sel;
    if (sel) frac = toFrac(sel);
    else frac = { a: 0, b: 1 };
    paint();
    o.onCommit(sel);
  };
  const commitSoon = () => {
    if (keyTimer) clearTimeout(keyTimer);
    keyTimer = setTimeout(commit, keyDelay);
    narrowed = !(frac.a <= 0 && frac.b >= 1);
    paint();
  };

  /* ── pointer drag ── */
  let drag = null;
  const fracAt = (e) => {
    const r = track.getBoundingClientRect();
    return r && r.width > 0 ? (e.clientX - r.left) / r.width : 0;
  };
  const begin = (mode) => (e) => {
    if (e.button != null && e.button !== 0) return;
    if (e.stopPropagation) e.stopPropagation();
    const m = mode === "move" && !narrowed ? "new" : mode;
    drag = { mode: m, anchor: fracAt(e), sel: { ...frac }, id: e.pointerId };
    if (track.setPointerCapture && e.pointerId != null) {
      try {
        track.setPointerCapture(e.pointerId);
      } catch {
        /* a pointer that is already gone: the drag still ends on pointerup */
      }
    }
    if (e.preventDefault) e.preventDefault();
  };
  track.addEventListener("pointerdown", begin("new"));
  body.addEventListener("pointerdown", begin("move"));
  hStart.addEventListener("pointerdown", begin("start"));
  hEnd.addEventListener("pointerdown", begin("end"));
  track.addEventListener("pointermove", (e) => {
    if (!drag) return;
    frac = dragSelection(drag.mode, drag.anchor, fracAt(e), drag.sel, minFrac);
    narrowed = true;
    paint();
  });
  const end = (e) => {
    if (!drag) return;
    const moved = Math.abs(fracAt(e) - drag.anchor) > 0.002;
    const was = drag;
    drag = null;
    if (track.releasePointerCapture && was.id != null) {
      try {
        track.releasePointerCapture(was.id);
      } catch {
        /* released already */
      }
    }
    /* A press with no travel on the bare track is a click, not a selection: nothing changes. */
    if (!moved && was.mode === "new") {
      frac = was.sel;
      narrowed = !(frac.a <= 0 && frac.b >= 1);
      paint();
      return;
    }
    commit();
  };
  track.addEventListener("pointerup", end);
  track.addEventListener("pointercancel", () => {
    if (!drag) return;
    frac = drag.sel;
    narrowed = !(frac.a <= 0 && frac.b >= 1);
    drag = null;
    paint();
  });

  /* ── keyboard ── */
  const onKey = (which) => (e) => {
    const k = e.key;
    if (k === "Escape") {
      if (e.preventDefault) e.preventDefault();
      frac = { a: 0, b: 1 };
      commit();
      return;
    }
    const dir = k === "ArrowLeft" || k === "ArrowDown" ? -1 : k === "ArrowRight" || k === "ArrowUp" ? 1 : 0;
    const jump = k === "Home" ? -1 : k === "End" ? 1 : 0;
    if (!dir && !jump) return;
    if (e.preventDefault) e.preventDefault();
    if (e.stopPropagation) e.stopPropagation();
    const step = (e.shiftKey ? 5 : 1) * stepFrac;
    if (which === "start") {
      const a = jump ? (jump < 0 ? 0 : frac.b - minFrac) : frac.a + dir * step;
      frac = { a: Math.max(0, Math.min(a, frac.b - minFrac)), b: frac.b };
    } else if (which === "end") {
      const b = jump ? (jump < 0 ? frac.a + minFrac : 1) : frac.b + dir * step;
      frac = { a: frac.a, b: Math.min(1, Math.max(b, frac.a + minFrac)) };
    } else {
      const width = frac.b - frac.a;
      const a = jump ? (jump < 0 ? 0 : 1 - width) : frac.a + dir * step;
      const left = Math.max(0, Math.min(a, 1 - width));
      frac = { a: left, b: left + width };
    }
    commitSoon();
  };
  hStart.addEventListener("keydown", onKey("start"));
  hEnd.addEventListener("keydown", onKey("end"));
  body.addEventListener("keydown", (e) => {
    if (e.target !== body) return;
    onKey("body")(e);
  });
  reset.addEventListener("click", () => {
    frac = { a: 0, b: 1 };
    commit();
  });

  if (o.selection) {
    const held = clampSlice(o.selection, win, minSpan);
    if (held) {
      frac = toFrac(held);
      narrowed = true;
    }
  }
  paint();
  paintStrip();

  return {
    node,
    /** The activity the strip draws: [{ t: epoch ms, v: number }]. */
    setPoints(next) {
      points = next || [];
      paintStrip();
    },
    /** One row's timeline over the strip, scaled to its own peak, with the name it is shown under. */
    setOverlay(next, label) {
      overlay = next && next.length ? next : null;
      overlayLabel = label || "";
      paintStrip();
    },
    /** A note about the strip itself (the activity list was cut), shown beside the readout; empty text clears it. */
    setNote(text) {
      stripNote.textContent = text || "";
    },
    clearOverlay() {
      overlay = null;
      overlayLabel = "";
      paintStrip();
    },
    /** Move the selection without committing (another card on the page narrowed). */
    setSelection(sel) {
      const held = sel ? clampSlice(sel, win, minSpan) : null;
      frac = held ? toFrac(held) : { a: 0, b: 1 };
      narrowed = !!held;
      paint();
    },
    selection: () => (narrowed ? toSel(frac) : null),
  };
}
