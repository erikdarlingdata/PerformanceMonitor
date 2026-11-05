/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, mount, clear } from "./util.js";
import { SOLO } from "./panel-layout.js";

/* The auto-refresh keeps what is on screen (#6933). A poll used to clear the page and draw a "Loading..." strip, so every
   minute the screen blinked and a reader lost their place. Now a poll reads again with the old content still showing and
   swaps the new content in one step when its read has returned. Nothing here animates: the swap is a single DOM change,
   so prefers-reduced-motion needs no special case. A reader's own navigation never comes through here, it renders fresh. */

/* ─────────────────────────── whole pages: Fleet, Availability Groups, saved views ─────────────────────────── */

const held = new WeakMap();

/** True when `container` still shows what showPage() last drew for `key` and this render is the poll's. */
export function holdsPage(container, key, isPoll) {
  const h = held.get(container);
  return isPoll === true && !!h && h.key === key && !!h.first && container.firstChild === h.first;
}

/** Draw `nodes` into `container` in one step, keeping the reader's scroll position, and remember them as page `key`. */
export function showPage(container, key, nodes) {
  cancelPending(container);
  keepScroll(() => mount(container, nodes));
  held.set(container, { key, first: container.firstChild });
}

/* #7393: the swap a poll has waiting for its reads, one per container. A newer render of the container drops it. */
const pending = new WeakMap();

function cancelPending(container) {
  const cancel = pending.get(container);
  if (cancel) cancel();
  pending.delete(container);
}

/* The reader acting on the page on screen while a swap waits: a click, a change, text typed, or Enter or Space (a key that activates a
   row, as util.js makeActivatable does). Scrolling, Tab and the mouse moving are not. */
const READER_ACTS = ["click", "change", "input", "keydown"];
const isReaderAct = (e) => e.type !== "keydown" || e.key === "Enter" || e.key === " ";

/** Drop the swap `container` has waiting, if any: its build never lands, so its `onDropped` runs. For a page that replaces the
 *  container's content by some other route than showPage (an empty-state strip) while a poll's build is still waiting. */
export function dropWaitingSwap(container) {
  cancelPending(container);
}

/**
 * showPage for a page that fills `nodes` from reads of its own (the loading strips it draws first). The first paint, a sub-tab click
 * and a span change put `nodes` on the page at once. A poll over the same page (`holdsPage`) leaves the page on screen, so a grid the
 * reader has scrolled down stays where it is, and swaps `nodes` in one step once none of them has a loading strip left, with the
 * page's and every grid's scroll position put back (keepScroll). A newer render of `container` drops the waiting swap, and so does
 * the reader going to another page. So does the reader acting on the page on screen (a click, a change, typing): `nodes` were built from
 * the page's state as it stood when the poll began, and what the reader did since is newer than that, so it must not be swapped out for
 * a page that shows the old state (#7393). The next poll draws from the new state. Returns a cancel function.
 *
 * `hooks` is how a page that owns an AbortController per build keeps the page on screen working (#7393). The page on screen keeps its
 * reads, its listeners and its signal for as long as it is on screen, so a poll's build must not abort them when it starts, only when it
 * lands. Exactly one of the two runs for each call, once:
 *   `onShown()`   `nodes` are on the page now (at once, or after the wait): the page that was on screen has left it, so end its reads.
 *   `onDropped()` `nodes` never reach the page (a newer render, the reader acting, the reader leaving): end the reads of the build.
 */
export function showPageWhenSettled(container, key, nodes, isPoll, hooks = {}) {
  cancelPending(container);
  if (!holdsPage(container, key, isPoll)) {
    showPage(container, key, nodes);
    if (hooks.onShown) hooks.onShown();
    return () => {};
  }
  let over = false;
  const drop = (e) => {
    if (isReaderAct(e)) cancelPending(container);
  };
  const release = () => READER_ACTS.forEach((t) => container.removeEventListener(t, drop, true));
  /* Capture, so the drop runs before the control's own handler redraws the page on screen. */
  READER_ACTS.forEach((t) => container.addEventListener(t, drop, true));
  let stop = () => {};
  const cancel = () => {
    if (over) return;
    over = true;
    release();
    stop();
    if (hooks.onDropped) hooks.onDropped();
  };
  stop = whenSettled(nodes, () => {
    if (over) return;
    over = true;
    release();
    pending.delete(container);
    if (holdsPage(container, key, true)) {
      showPage(container, key, nodes);
      if (hooks.onShown) hooks.onShown();
    } else if (hooks.onDropped) {
      hooks.onDropped();
    }
  });
  /* whenSettled runs `done` at once when nothing is loading: then the swap is over and there is nothing left to cancel. */
  if (!over) pending.set(container, cancel);
  return cancel;
}

/** The poll's page when the grid `keep` stays where it is: the other children are replaced by `before`, in one step, and `keep` is never detached. */
export function showAround(container, key, keep, before) {
  keepScroll(() => {
    Array.from(container.children).forEach((c) => {
      if (c !== keep) container.removeChild(c);
    });
    before.forEach((n) => container.insertBefore(n, keep));
  });
  held.set(container, { key, first: container.firstChild });
}

/** A poll whose read failed: put the error in `box` at child position `at`, in place of an earlier one, and leave the rest as it is. */
export function noteRefreshError(box, message, at = 0) {
  const prior = Array.from(box.children).find(isRefreshError);
  if (prior) box.removeChild(prior);
  const notice = el("div", { class: "strip error refresh-error", role: "alert" }, ["Could not refresh, showing the previous data. " + message]);
  box.insertBefore(notice, Array.from(box.children)[at] || null);
}

/** noteRefreshError for a whole page (under its heading) when `container` still shows page `key`. False when it does not. */
export function showRefreshError(container, key, message) {
  if (!holdsPage(container, key, true)) return false;
  noteRefreshError(container, message, 1);
  return true;
}

/** Put `fresh` where `old` is, node for node, leaving every other child of `container` (the grid between them) alone. */
export function replaceNodes(container, old, fresh) {
  old.forEach((o, i) => {
    container.insertBefore(fresh[i], o);
    container.removeChild(o);
  });
}

/* #7393: a long grid scrolls inside its own `.table-wrap`. A poll swaps in a new wrap, which starts at the top, so the grid's own
   scroll position is kept the way the page's is. A grid is told apart by its `data-grid` name (on the wrap, or on the box around
   it), and by its place among the grids of that name when a page draws two with one name. */
function gridWraps() {
  const wraps = new Map();
  const seen = new Map();
  Array.from(document.querySelectorAll(".table-wrap")).forEach((w) => {
    const box = typeof w.closest === "function" ? w.closest("[data-grid]") : null;
    const name = box ? box.getAttribute("data-grid") : "";
    const n = seen.get(name) || 0;
    seen.set(name, n + 1);
    wraps.set(name + "\u0001" + n, w);
  });
  return wraps;
}

/** Run `fn`, then put the page, and every grid that was scrolled inside its own box, back where the reader had scrolled it if the swap moved it. */
export function keepScroll(fn) {
  const y = globalThis.scrollY;
  const grids = [];
  /* Every wrap on the page before the swap. A wrap in this set is one the swap did not make: it keeps its own position. */
  const before = new Set();
  gridWraps().forEach((w, id) => {
    before.add(w);
    if (w.scrollTop > 0 || w.scrollLeft > 0) grids.push({ id, top: w.scrollTop, left: w.scrollLeft });
  });
  fn();
  if (y > 0 || grids.length) {
    /* The browser's scroll anchoring moves the page when it next lays out, after the nodes above the reader changed, so lay out
       now and then put the page back. Reading scrollY alone does not lay out. */
    void document.documentElement.offsetHeight;
    if (y > 0 && globalThis.scrollY !== y) globalThis.scrollTo(0, y);
    if (grids.length) {
      const now = gridWraps();
      grids.forEach((g) => {
        const w = now.get(g.id);
        /* A wrap the swap left in place keeps its own position, even when a wrap ahead of it went and moved its name and place
           (a panel that drew zero rows has no wrap left, so the same-name wrap after it takes its slot). Only a wrap that is new
           since the swap takes a remembered position: it is set after layout, and clamps to its new height. */
        if (!w || before.has(w)) return;
        w.scrollTop = g.top;
        w.scrollLeft = g.left;
      });
    }
  }
}

/* ─────────────────────────── a grid of panels: the server page ─────────────────────────── */

function hasClass(n, name) {
  return typeof n.className === "string" && (" " + n.className + " ").includes(" " + name + " ");
}

function find(n, pred) {
  if (pred(n)) return n;
  for (const c of Array.from(n.children || [])) {
    const f = find(c, pred);
    if (f) return f;
  }
  return null;
}

const isLoading = (n) => hasClass(n, "loading");
const isRefreshError = (n) => hasClass(n, "refresh-error");
const isError = (n) => hasClass(n, "strip") && hasClass(n, "error") && !isRefreshError(n);

/** Whether a panel has drawn: no loading strip is left anywhere inside it. A hidden element is skipped: a panel that does not
 *  apply to this server (`hideWhenNotCollected`, the Instance CPU panel on PostgreSQL that is not Aurora) hides itself and leaves
 *  its loading strip behind, and waiting on that strip would hold every poll of the page forever. */
function settled(n) {
  if (n.hidden === true) return true;
  if (isLoading(n)) return false;
  return Array.from(n.children || []).every(settled);
}

/* The panel's key: the page's own markers for the read and its title, plus the read's parameters when renderPanel recorded
   them (panels.js), in the order the grid draws them. A poll reuses a panel only when the key still matches. */
const panelParams = new WeakMap();

/** Called by renderPanel so the key includes the read's parameters, not only its name. */
export function notePanelKey(panel, key) {
  panelParams.set(panel, key);
}

/* #7534: panel-layout.js marks a lone half-width panel `span-solo` on the live panel, and a freshly built panel never has it. The mark
   is left out of the key so a poll still finds the same panels, and swapUnit keeps it. */
function classKey(n) {
  return typeof n.className === "string" ? n.className.split(/\s+/).filter((c) => c && c !== SOLO).join(" ") : "";
}

function unitSig(n) {
  const attr = (k) => (typeof n.getAttribute === "function" ? n.getAttribute(k) : null);
  return [n.tag || n.tagName, classKey(n), attr("data-panel"), attr("data-title"), panelParams.get(n) || ""].join("|");
}

/**
 * Refresh the grid `live` (already on screen) from `staged`, the nodes a tab's build() just returned and which have not been
 * put on the page, so their reads run off screen. When `staged` has the same panels, in the same order, as `live` holds and
 * every live panel has drawn, each live panel's element stays and its body is replaced in one step when its own new read has
 * returned. A panel whose new read failed keeps its old content and shows the error. Otherwise (a different set of panels,
 * or a live panel still loading) the whole grid is swapped in one step once every staged panel has drawn. Returns a cancel
 * function: a newer render calls it so a late answer cannot land on a page that has moved on. A whole-grid swap also drops itself when `live`'s children are no longer the
 * ones it was started against.
 */
export function refreshGridInPlace(live, staged) {
  staged = Array.isArray(staged) ? staged : [staged]; /* a tab may return one node, as mount() accepts */
  const cancels = [];
  const liveUnits = Array.from(live.children || []);
  const sameShape = staged.length === liveUnits.length && staged.every((n, i) => unitSig(n) === unitSig(liveUnits[i]) && settled(liveUnits[i]));

  if (!sameShape) {
    /* The grid may have been redrawn while the staged panels loaded (a saved view's scope control mounts new-scope panels). The
       staged panels were built for the old scope, so they only go in over the units they were built against. */
    const unchanged = () => live.children.length === liveUnits.length && liveUnits.every((u, i) => live.children[i] === u);
    cancels.push(whenSettled(staged, () => { if (unchanged()) keepScroll(() => mount(live, staged)); }));
  } else {
    staged.forEach((fresh, i) => {
      cancels.push(whenSettled([fresh], () => swapUnit(live, liveUnits[i], fresh)));
    });
  }
  return () => {
    cancels.forEach((c) => c());
  };
}

function swapUnit(live, old, fresh) {
  if (!Array.from(live.children || []).includes(old)) return;
  const failed = find(fresh, isError);
  if (failed && !find(old, isError)) {
    noteRefreshError(old, failed.textContent, 1);
    return;
  }
  keepScroll(() => {
    const solo = hasClass(old, SOLO);
    clear(old);
    while (fresh.firstChild) old.appendChild(fresh.firstChild);
    old.className = fresh.className + (solo ? " " + SOLO : "");
    old.hidden = fresh.hidden;
    if (old.style && fresh.style) old.style.display = fresh.style.display;
  });
}

/** Run `done` once none of `nodes` has a loading strip left; at once when none has one now. Returns a cancel function. */
export function whenSettled(nodes, done) {
  const pending = () => nodes.some((n) => !settled(n));
  if (!pending()) {
    done();
    return () => {};
  }
  const obs = new MutationObserver(() => {
    if (pending()) return;
    obs.disconnect();
    done();
  });
  nodes.forEach((n) => obs.observe(n, { childList: true, subtree: true }));
  return () => obs.disconnect();
}
