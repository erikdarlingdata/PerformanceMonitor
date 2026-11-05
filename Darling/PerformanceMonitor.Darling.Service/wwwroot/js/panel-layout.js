/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* #7534: a half-width panel with no partner takes the whole row.
   `.panel-grid` has two columns and a `span: 1` panel takes one of them. Two span-1 panels side by side fill a row, but a span-1
   panel followed by a full-row panel (or the end of the page) sat at half width beside a page-wide gap. The rows are decided by
   the panels that are on screen, so this cannot be a rule on one panel: a partner that hides itself (empty, or not collected for
   this server) changes who is alone. soloPanels() walks the visible items in order, closes a run of span-1 items at each full-row
   item, and marks the last panel of an odd run `span-solo`, which app.css lays out as a full row. */

export const SOLO = "span-solo";

function classes(node) {
  return " " + String(node.className || "") + " ";
}

function hasClass(node, name) {
  return classes(node).includes(" " + name + " ");
}

function isHidden(node) {
  return node.hidden === true || (node.style && node.style.display === "none");
}

/* A wrapper with `display: contents` (a sliced grid's slot) lays its children out in the grid, so the walk goes through it. */
function isTransparent(node) {
  return !!node.style && node.style.display === "contents";
}

/* A sentence over a group of cards (`.panel-intro`) takes the whole row, and draws nothing while it is empty. */
function isEmptyIntro(node) {
  return hasClass(node, "panel-intro") && (!node.children || node.children.length === 0) && !node.textContent;
}

/* An item can also take the whole row with an inline `grid-column` (the Active Queries live bar, the Compare holder, the Collection Log
   status bar), so the walk reads `style.gridColumn` too: "1 / -1", "1 / span 2" or "1 / 3". */
function spansWholeRow(node) {
  const gc = node.style && typeof node.style.gridColumn === "string" ? node.style.gridColumn : "";
  return /^\s*1\s*\/\s*(-1|3|span\s+2)\s*$/.test(gc);
}

function wholeRow(node) {
  return hasClass(node, "span-2") || hasClass(node, "panel-intro") || spansWholeRow(node);
}

function collect(node, items) {
  for (const child of node.children || []) {
    if (isHidden(child)) continue;
    if (isTransparent(child)) {
      collect(child, items);
      continue;
    }
    if (isEmptyIntro(child)) continue;
    items.push(child);
  }
}

/** Mark the panel left alone on its row in `grid` `span-solo`, and clear the mark from every other panel. */
export function soloPanels(grid) {
  const items = [];
  collect(grid, items);
  const solo = new Set();
  let run = [];
  const close = () => {
    if (run.length % 2 === 1) {
      const last = run[run.length - 1];
      if (hasClass(last, "panel")) solo.add(last);
    }
    run = [];
  };
  for (const item of items) {
    if (wholeRow(item)) close();
    else run.push(item);
  }
  close();

  const all = [];
  const gather = (node) => {
    for (const child of node.children || []) {
      if (hasClass(child, "panel")) all.push(child);
      else if (isTransparent(child)) gather(child);
    }
  };
  gather(grid);
  for (const panel of all) {
    const want = solo.has(panel);
    if (hasClass(panel, SOLO) !== want) panel.classList.toggle(SOLO, want);
  }
}

/* A change that can move a panel to another row: a panel or wrapper added or removed, or one hidden or shown. The charts inside the
   panels redraw and restyle all the time, and none of that moves a row. */
function movesARow(grid, record) {
  if (record.type === "childList") {
    const touched = [...record.addedNodes, ...record.removedNodes];
    return record.target === grid || touched.some((n) => n && n.nodeType === 1 && (hasClass(n, "panel") || isTransparent(n)));
  }
  const t = record.target;
  return !!t && (t.parentNode === grid || hasClass(t, "panel") || isTransparent(t));
}

/** Keep `grid`'s lone panels full width, now and whenever a panel is added, removed, hidden or shown. Returns a function that stops. */
export function keepLonePanelsWide(grid) {
  soloPanels(grid);
  if (typeof MutationObserver !== "function") return () => {};
  const observer = new MutationObserver((records) => {
    if (!records || records.some((r) => movesARow(grid, r))) soloPanels(grid);
  });
  observer.observe(grid, { childList: true, subtree: true, attributes: true, attributeFilter: ["hidden", "style"] });
  return () => observer.disconnect();
}
