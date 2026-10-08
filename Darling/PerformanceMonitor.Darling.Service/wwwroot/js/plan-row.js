/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Where an open plan (or history) panel is drawn: under the clicked row, as wide as the grid's visible area, not inside the
   last grid column.

   A Plan cell sits in the last column of a grid that scrolls sideways, so a panel drawn inside that cell started past the
   right edge of the screen and made the row a tall blank band. The cell now keeps only its button and a spacer. The panel
   is positioned against the row itself (`tr:has(.plan-cell.plan-open)` in app.css is the containing block), sits at the
   bottom of it, and the spacer makes the row exactly tall enough to hold it. No row is added to the table, so sorting,
   filtering, Copy and CSV see the same rows as before.

   The row is as wide as the whole table, and the table scrolls sideways inside its `.table-wrap`. A user reaches the Plan
   column by scrolling right, so a panel that started at the row's left edge had its text off-screen to the left. The panel
   is therefore pinned to the wrap's VISIBLE area: `left` follows the wrap's scroll position and `width` is the wrap's
   client width (pinGeometry), and both are refreshed when the wrap scrolls or any watched element changes size.

   One ResizeObserver per open panel watches the panel (it loads and grows), the row (a window resize re-wraps the other
   cells and changes the row's height, which the spacer must follow) and the wrap (the visible width). It is disconnected,
   with the scroll listener, when the panel closes (undockPanel), when the cell draws again, or when the cell has left the
   page (a grid re-render), so a closed or discarded panel is never watched. */

import { el } from "./util.js";

/** The class a plan cell carries while its panel is open. */
export const OPEN_CLASS = "plan-cell plan-open";

/** The class a plan cell carries while it is only a button. */
export const CLOSED_CLASS = "plan-cell";

/** What each open host is watching, so closing the panel (or drawing it again) can let go of every observer. */
const docked = new WeakMap();

/**
 * Where the panel sits inside the row so that it covers the wrap's visible area. All numbers are pixels: the wrap's visible
 * left edge (its box left plus its border) against the row's left edge. The panel is never wider than the row and never
 * pushed past its right end.
 */
export function pinGeometry({ wrapLeft, wrapClientLeft, wrapClientWidth, rowLeft, rowWidth }) {
  const width = Math.max(0, Math.min(wrapClientWidth, rowWidth));
  const visibleLeft = wrapLeft + wrapClientLeft - rowLeft;
  const left = Math.min(Math.max(0, visibleLeft), Math.max(0, rowWidth - width));
  return { left, width };
}

/**
 * Puts `panel` in `host` (the plan cell) as a panel under the host's row, as wide as the grid's visible area. Call it after
 * the cell's button is in `host`. The spacer follows the panel's height as it loads and grows.
 */
export function dockPanelUnderRow(host, panel) {
  release(host);
  host.className = OPEN_CLASS;
  const spacer = el("div", { class: "plan-spacer", "aria-hidden": "true" });
  host.appendChild(spacer);
  host.appendChild(panel);
  const state = { observer: null, row: null, wrap: null, onScroll: null, seen: false };
  docked.set(host, state);
  const fit = () => {
    if (docked.get(host) !== state) return;
    if (state.seen && !host.isConnected) { release(host); return; }
    fitSpacer(host, spacer, panel);
    watchRowAndWrap(host, panel, state, fit);
    pinPanel(host, panel);
  };
  if (typeof ResizeObserver === "function") {
    state.observer = new ResizeObserver(fit);
    state.observer.observe(panel);
  }
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(fit);
}

/** Marks `host` as a plain button cell (no open panel) and lets go of what the open panel was watching. */
export function undockPanel(host) {
  release(host);
  host.className = CLOSED_CLASS;
}

/** Disconnects the observer and the scroll listener a docked host holds, if any. */
function release(host) {
  const state = docked.get(host);
  if (!state) return;
  docked.delete(host);
  if (state.observer) state.observer.disconnect();
  if (state.wrap && state.onScroll) state.wrap.removeEventListener("scroll", state.onScroll);
}

/** Starts watching the row and the wrap the first time the host is in a row (it may be docked before it is on the page). */
function watchRowAndWrap(host, panel, state) {
  if (state.row || typeof host.closest !== "function" || !host.isConnected) return;
  const row = host.closest("tr");
  if (!row) return;
  state.seen = true;
  state.row = row;
  state.wrap = host.closest(".table-wrap");
  if (state.observer) {
    state.observer.observe(row);
    if (state.wrap) state.observer.observe(state.wrap);
  }
  if (state.wrap) {
    state.onScroll = () => pinPanel(host, panel);
    state.wrap.addEventListener("scroll", state.onScroll, { passive: true });
  }
}

/** Sets the panel's left edge and width from the wrap's visible area. A cell outside a scrolling wrap keeps the CSS (the row's full width). */
function pinPanel(host, panel) {
  const state = docked.get(host);
  if (!state || !state.row || !state.wrap || !host.isConnected) return;
  const w = state.wrap.getBoundingClientRect();
  const r = state.row.getBoundingClientRect();
  const g = pinGeometry({
    wrapLeft: w.left,
    wrapClientLeft: state.wrap.clientLeft || 0,
    wrapClientWidth: state.wrap.clientWidth,
    rowLeft: r.left,
    rowWidth: r.width,
  });
  panel.style.right = "auto";
  panel.style.left = g.left + "px";
  panel.style.width = g.width + "px";
}

/**
 * Sizes the spacer so the row holds its own cells plus the panel: the row's height without the spacer, less what the
 * cell's own button already takes, plus the panel's height. Does nothing for a cell that is not in a row yet.
 */
function fitSpacer(host, spacer, panel) {
  const row = typeof host.closest === "function" ? host.closest("tr") : null;
  if (!row || !host.isConnected) return;
  spacer.style.height = "0px";
  const rowHeight = row.offsetHeight;
  const own = host.offsetHeight;
  spacer.style.height = Math.max(0, rowHeight - own) + panel.offsetHeight + "px";
}
