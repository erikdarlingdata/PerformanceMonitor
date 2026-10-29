/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The Custom Views list in the sidebar (#1563, folders and personal views #6214). The order and the grouping come from
 * view-folders.js (My views, then shared folders A-Z, then shared views with no folder); this only paints the rows. Which
 * groups are collapsed is remembered per browser (viewer-local.js), the way the server-tag groups are. Every name reaches
 * the page as text.
 */

import { el, mount, storeHash } from "./util.js";
import { sidebarViewRows } from "./view-folders.js";
import { isViewGroupCollapsed, toggleViewGroup } from "./viewer-local.js";

const INDENT_REM = 0.75;

function viewLink(v, depth, activeId) {
  const active = activeId != null && String(activeId) === String(v.id);
  /* The summary carries the view kind (definition->>'kind'), so a notebook links to its own route with a document glyph. */
  const isNotebook = v.kind === "notebook";
  return el("a", {
    class: "view-item" + (active ? " active" : "") + (isNotebook ? " is-notebook" : "") + (v.personal === true ? " is-personal" : ""),
    href: storeHash((isNotebook ? "#/notebook/" : "#/view/") + encodeURIComponent(v.id)),
    dataset: { view: String(v.id) },
    style: depth > 0 ? "padding-left:" + (1.25 + depth * INDENT_REM) + "rem" : null,
    title: v.description || v.name,
    text: v.name,
  });
}

function groupHeader(g) {
  return el(
    "div",
    {
      class: "sidebar-group-header view-group-header",
      style: "padding-left:0.75rem",
      "aria-expanded": g.collapsed ? "false" : "true",
      dataset: { viewGroup: g.id },
      onActivate: () => toggleViewGroup(g.id),
    },
    [
      el("span", { class: "sidebar-group-chevron", text: g.collapsed ? "▸" : "▾" }),
      el("span", { class: "sidebar-group-name", text: g.name }),
      el("span", { class: "sidebar-group-count", text: "(" + g.count + ")" }),
    ]
  );
}

/**
 * Paints the view list into `container`. `views` is /api/views's list (empty when the read failed), `session` is
 * /api/session's answer, `activeId` the view the route shows (or null). The "New view" and "New notebook" links follow the
 * views, for a session that can edit.
 */
export function paintViewList(container, views, session, activeId) {
  const rows = sidebarViewRows(views, isViewGroupCollapsed);
  const items = rows.map((r) => (r.kind === "group" ? groupHeader(r) : viewLink(r.view, r.depth, activeId)));

  if (session && session.can_edit) {
    items.push(el("a", { class: "view-item new-view", href: storeHash("#/view/new"), text: "＋ New view" }));
    items.push(el("a", { class: "view-item new-view", href: storeHash("#/notebook/new"), text: "＋ New notebook" }));
  }

  if (!items.length) {
    items.push(el("div", { class: "muted", style: "padding:0.35rem 1.25rem", text: "No views yet" }));
  }

  /* A repaint replaces every row, which destroys the header a keyboard user just activated: hand focus back to the same
     group's new header, so Enter or Space can toggle it again. */
  const focused = document.activeElement;
  const refocus = focused && focused.dataset && focused.dataset.viewGroup != null && Array.from(container.children).includes(focused) ? focused.dataset.viewGroup : null;
  mount(container, items);
  if (refocus !== null) {
    const header = Array.from(container.children).find((n) => n.dataset && n.dataset.viewGroup === refocus);
    if (header) header.focus();
  }
}
