/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Where an open plan (or history) panel is drawn: full width under the clicked row, not inside the last grid column.

   A Plan cell sits in the last column of a grid that scrolls sideways, so a panel drawn inside that cell started past the
   right edge of the screen and made the row a tall blank band. The cell now keeps only its button and a spacer. The panel
   is positioned against the row itself (`tr:has(.plan-cell.plan-open)` in app.css is the containing block), spans the
   row's whole width and sits at the bottom of it, and the spacer makes the row exactly tall enough to hold it. No row is
   added to the table, so sorting, filtering, Copy and CSV see the same rows as before. */

import { el } from "./util.js";

/** The class a plan cell carries while its panel is open. */
export const OPEN_CLASS = "plan-cell plan-open";

/** The class a plan cell carries while it is only a button. */
export const CLOSED_CLASS = "plan-cell";

/**
 * Puts `panel` in `host` (the plan cell) as a full-width panel under the host's row. Call it after the cell's button is
 * in `host`. The spacer follows the panel's height as it loads and grows.
 */
export function dockPanelUnderRow(host, panel) {
  host.className = OPEN_CLASS;
  const spacer = el("div", { class: "plan-spacer", "aria-hidden": "true" });
  host.appendChild(spacer);
  host.appendChild(panel);
  const fit = () => fitSpacer(host, spacer, panel);
  if (typeof ResizeObserver === "function") new ResizeObserver(fit).observe(panel);
  if (typeof requestAnimationFrame === "function") requestAnimationFrame(fit);
}

/** Marks `host` as a plain button cell (no open panel). */
export function undockPanel(host) {
  host.className = CLOSED_CLASS;
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
