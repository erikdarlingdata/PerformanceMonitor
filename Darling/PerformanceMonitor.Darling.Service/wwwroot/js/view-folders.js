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

/** The name MY_VIEWS_ID is shown under. The store refuses it (in any letter case) as a shared folder name. */
export const MY_VIEWS_NAME = "My views";

/**
 * Whether the session can own a personal view: the server's `can_own` (a sign-in that carries an owner key). The page never
 * compares names: a display name can change and can be reissued, so ownership is the server's `mine` on each view.
 */
export function canOwn(session) {
  return !!session && typeof session === "object" && session.can_own === true;
}

/** Unicode White_Space, spelled out to match the store: the Z categories, U+0009-U+000D and U+0085. */
const WHITESPACE_RUN = /[\p{Z}\t-\r\u0085]+/gu;

/** Control and format characters, plus the Unicode Default_Ignorable_Code_Point set as explicit ranges (the store's list). An
 *  ignorable code point draws nothing, so two names that differ only by one look the same. */
const INVISIBLE = /[\p{Cc}\p{Cf}­͏؜ᅟᅠ឴឵᠋-᠏​-‏‪-‮⁠-⁯ㅤ︀-️﻿ﾠ￰-￸\u{1BCA0}-\u{1BCA3}\u{1D173}-\u{1D17A}\u{E0000}-\u{E0FFF}]/u;

/**
 * The folder name as the store keeps it: every whitespace character (a no-break space, a tab) becomes a plain space, runs collapse
 * to one, and the ends are trimmed. Anything that is not text gives "". Mirrors CustomViewStore.NormalizeFolder.
 */
export function normalizeFolder(name) {
  return typeof name === "string" ? name.replace(WHITESPACE_RUN, " ").trim() : "";
}

/**
 * Why a folder name would be refused, or null when it is fine (the typed text is normalized first, see normalizeFolder; blank is
 * the top level). Mirrors the store: at most MAX_FOLDER_LENGTH characters, no control, invisible formatting or default-ignorable
 * character (such as U+202E, U+200B or U+FE0F, which make two different names look alike), and "My views" in any letter case,
 * whatever kind of space sits between the words, is reserved. The store decides; this only saves the round trip.
 */
export function folderProblem(name) {
  const typed = normalizeFolder(name);
  if (typed === "") return null;
  if (typed.length > MAX_FOLDER_LENGTH) return "A folder name can be at most " + MAX_FOLDER_LENGTH + " characters.";
  if (INVISIBLE.test(typed)) return "A folder name can't contain control or invisible formatting characters.";
  if (typed.toLowerCase() === MY_VIEWS_NAME.toLowerCase()) return "\"" + MY_VIEWS_NAME + "\" is reserved for your own personal views. Pick another folder name.";
  return null;
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
 * collapsed (id is MY_VIEWS_ID or FOLDER_PREFIX + name). Order: "My views" first (only the personal views the server marks
 * `mine`), then the shared folders A-Z, then the shared views with no folder at the
 * top level. An empty group is not listed. A personal view that is not `mine` is never listed, whatever the server
 * sent. Rows: { kind: "group", id, name, count, collapsed } and { kind: "view", view, depth }; a view inside a group is at
 * depth 1, a top-level view at depth 0. Views inside a group are by name.
 */
export function sidebarViewRows(views, isCollapsed) {
  const list = Array.isArray(views) ? views.filter((v) => v && typeof v === "object") : [];
  const mine = list.filter((v) => v.personal === true && v.mine === true);
  const shared = list.filter((v) => v.personal !== true);
  const viewName = (v) => (typeof v.name === "string" ? v.name : "");
  const sorted = (arr) => arr.slice().sort((a, b) => byName(viewName(a), viewName(b)));

  const groups = [];
  if (mine.length) groups.push({ id: MY_VIEWS_ID, name: MY_VIEWS_NAME, views: sorted(mine) });
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
 * A personal view is read-only unless the server marks it `mine`; `note` then says whose it is (created_by is the name to show). An own personal view
 * says so too. A shared view has no note.
 */
export function viewAccess(view, session) {
  const personal = !!view && view.personal === true;
  const creator = view ? text(view.created_by) : null;
  const own = personal && view.mine === true;
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
 * edited, or null for a new one. It is disabled, with the reason as short text, when the session can't own a view (`can_own` is
 * false: the shared token, no sign-in, or a sign-in made before owner keys existed), and for a shared view that is not `mine` (the
 * store lets only the person who made it, signed in, make it personal; a view the shared token or MCP made, or one from before
 * personal views, can not be made personal by anyone).
 */
export function personalChoice(view, session) {
  if (!canOwn(session)) {
    return { disabled: true, reason: "Personal views need a sign-in that can own them, and this session has none. If you are signed in, open /auth/logout, then sign in again." };
  }
  if (view && view.personal !== true && view.id != null && view.mine !== true) {
    return {
      disabled: true,
      reason: text(view.created_by) === null
        ? "This shared view has no recorded creator, so it can't be made personal."
        : "Only the person who created this view can make it personal (a view made with the shared token or over MCP can't be).",
    };
  }
  return { disabled: false, reason: "" };
}

/**
 * The folder and personal fields for a save body. `folder` is the typed text trimmed, or null (the top level) when blank;
 * `personal` is true only when the box is ticked AND the choice is open to this session, so a disabled box never sends true.
 */
export function metaForSave(meta, view, session) {
  const typed = normalizeFolder(meta.folder);
  return {
    folder: typed === "" ? null : typed,
    personal: meta.personal === true && !personalChoice(view, session).disabled,
  };
}
