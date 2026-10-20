/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The rules for Custom View folders, personal views and owners (#6214), shared by the sidebar list (view-sidebar.js), the
 * view and notebook pages (pages/views.js) and both editors (view-meta-fields.js). Pure: no imports and no DOM, so the rules
 * run under Node.
 *
 * The store decides who may do what; these rules only decide what the page shows, so a button the store would refuse is not
 * offered. The store's words always win over ours: an error it returns is shown as it came.
 */

/** The sidebar group id of the caller's own personal views. A folder's id is FOLDER_PREFIX + its name. */
export const MY_VIEWS_ID = "my";
export const FOLDER_PREFIX = "folder:";

/** The longest folder name the store accepts. */
export const MAX_FOLDER_LENGTH = 100;

/* Same-name folders differ by case in the store (names compare exactly), so the sort breaks ties by code order. */
function byName(a, b) {
  return a.localeCompare(b, undefined, { sensitivity: "base" }) || (a < b ? -1 : a > b ? 1 : 0);
}

function text(v) {
  return typeof v === "string" && v !== "" ? v : null;
}

/** The identity of the signed-in session, or null when it has none (the shared token, or no sign-in). */
export function sessionSubject(session) {
  return session && typeof session === "object" ? text(session.subject) : null;
}

/** The folder a shared view sits in, or null for the top level. A personal view has no sidebar folder. */
export function folderOf(view) {
  return view && view.personal !== true ? text(view.folder) : null;
}

/** The distinct shared folder names across the views, A-Z, for the editor's folder suggestions. */
export function folderNames(views) {
  const seen = new Set();
  for (const v of Array.isArray(views) ? views : []) {
    const f = folderOf(v);
    if (f !== null) seen.add(f);
  }
  return [...seen].sort(byName);
}

/**
 * The Custom Views sidebar as an ordered row model, so the page only paints it. `isCollapsed(id)` says whether a group is
 * collapsed (id is MY_VIEWS_ID or FOLDER_PREFIX + name). Order: "My views" first (only for a session with an identity, and
 * only the personal views that session created), then the shared folders A-Z, then the shared views with no folder at the
 * top level. An empty group is not listed. A personal view that is not the caller's is never listed, whatever the server
 * sent. Rows: { kind: "group", id, name, count, collapsed } and { kind: "view", view, depth }; a view inside a group is at
 * depth 1, a top-level view at depth 0. Views inside a group are by name.
 */
export function sidebarViewRows(views, subject, isCollapsed) {
  const list = Array.isArray(views) ? views.filter((v) => v && typeof v === "object") : [];
  const mine = subject ? list.filter((v) => v.personal === true && v.created_by === subject) : [];
  const shared = list.filter((v) => v.personal !== true);
  const viewName = (v) => (typeof v.name === "string" ? v.name : "");
  const sorted = (arr) => arr.slice().sort((a, b) => byName(viewName(a), viewName(b)));

  const groups = [];
  if (mine.length) groups.push({ id: MY_VIEWS_ID, name: "My views", views: sorted(mine) });
  const byFolder = new Map();
  for (const v of shared) {
    const f = folderOf(v);
    if (f === null) continue;
    if (!byFolder.has(f)) byFolder.set(f, []);
    byFolder.get(f).push(v);
  }
  for (const f of [...byFolder.keys()].sort(byName)) groups.push({ id: FOLDER_PREFIX + f, name: f, views: sorted(byFolder.get(f)) });

  const rows = [];
  for (const g of groups) {
    const collapsed = typeof isCollapsed === "function" && isCollapsed(g.id) === true;
    rows.push({ kind: "group", id: g.id, name: g.name, count: g.views.length, collapsed });
    if (!collapsed) for (const v of g.views) rows.push({ kind: "view", view: v, depth: 1 });
  }
  for (const v of sorted(shared.filter((x) => folderOf(x) === null))) rows.push({ kind: "view", view: v, depth: 0 });
  return rows;
}

/**
 * What the session may do with one stored view (the full view, or a list summary): { personal, creator, readOnly, note }.
 * A personal view is read-only for anyone but the person who created it; `note` then says whose it is. An own personal view
 * says so too. A shared view has no note.
 */
export function viewAccess(view, session) {
  const subject = sessionSubject(session);
  const personal = !!view && view.personal === true;
  const creator = view ? text(view.created_by) : null;
  const own = personal && creator !== null && creator === subject;
  return {
    personal,
    creator,
    readOnly: personal && !own,
    note: personal ? (own ? "Personal view (listed only for you)" : "Personal view of " + (creator === null ? "unknown" : creator)) : "",
  };
}

/** "Created by X · changed by Y" for a stored view; "unknown" for a creator the store never recorded. Empty for a new view. */
export function ownerLine(view) {
  if (!view || view.id == null) return "";
  const parts = ["Created by " + (text(view.created_by) || "unknown")];
  const changer = text(view.updated_by);
  if (changer) parts.push("changed by " + changer);
  return parts.join(" · ");
}

/**
 * The state of the editor's "Personal (listed only for you)" checkbox: { disabled, reason }. `view` is the stored view being
 * edited, or null for a new one. It is disabled, with the reason as short text, when the session has no identity (nothing
 * to own it), and for a shared view the session did not create (the store lets only its creator make it personal; a view
 * with no recorded creator can not be made personal by anyone).
 */
export function personalChoice(view, session) {
  const subject = sessionSubject(session);
  if (subject === null) return { disabled: true, reason: "Personal views need a signed-in identity, and this session has none." };
  if (view && view.personal !== true && view.id != null) {
    const creator = text(view.created_by);
    if (creator === null) return { disabled: true, reason: "This shared view has no recorded creator, so it can't be made personal." };
    if (creator !== subject) return { disabled: true, reason: "Only the person who created this view can make it personal." };
  }
  return { disabled: false, reason: "" };
}

/**
 * The folder and personal fields for a save body. `folder` is the typed text trimmed, or null (the top level) when blank;
 * `personal` is true only when the box is ticked AND the choice is open to this session, so a disabled box never sends true.
 */
export function metaForSave(meta, view, session) {
  const typed = typeof meta.folder === "string" ? meta.folder.trim() : "";
  return {
    folder: typed === "" ? null : typed,
    personal: meta.personal === true && !personalChoice(view, session).disabled,
  };
}
