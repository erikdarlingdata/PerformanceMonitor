/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { el, mount, clear } from "./util.js";

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
  keepScroll(() => mount(container, nodes));
  held.set(container, { key, first: container.firstChild });
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

/** Run `fn`, then put the page back where the reader had scrolled it if the swap moved it. */
export function keepScroll(fn) {
  const y = globalThis.scrollY;
  fn();
  if (y > 0) {
    /* The browser's scroll anchoring moves the page when it next lays out, after the nodes above the reader changed, so lay out
       now and then put the page back. Reading scrollY alone does not lay out. */
    void document.documentElement.offsetHeight;
    if (globalThis.scrollY !== y) globalThis.scrollTo(0, y);
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

function unitSig(n) {
  const attr = (k) => (typeof n.getAttribute === "function" ? n.getAttribute(k) : null);
  return [n.tag || n.tagName, typeof n.className === "string" ? n.className : "", attr("data-panel"), attr("data-title"), panelParams.get(n) || ""].join("|");
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
    clear(old);
    while (fresh.firstChild) old.appendChild(fresh.firstChild);
    old.className = fresh.className;
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
