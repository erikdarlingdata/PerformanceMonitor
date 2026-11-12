/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Tag grouping and name/tag matching shared by the Fleet page (pages/fleet.js) and the sidebar server list
 * (sidebar.js), so the two views group and filter the same way. Pure: no imports and no DOM, so the rules run
 * under Node.
 */

/* The web twin of FleetView's projection: the tag forest depth-first (child tags before a tag's own servers),
   then an Untagged group last. Each entry is one group header with its DIRECTLY-assigned server cards; a server
   carrying multiple tags appears under each, and an untagged server appears only under Untagged. Cycle- and
   dangling-parent-safe (an orphaned tag surfaces as a root rather than vanishing). No Favorites group — the web
   fleet has no per-user favourites. */
export function buildTagGroups(forest, cards, sortFn) {
  const known = new Set(forest.map((t) => t.id));
  const byParent = new Map();
  for (const t of forest) {
    const p = t.parent_id != null && known.has(t.parent_id) ? t.parent_id : 0; // dangling parent -> root
    if (!byParent.has(p)) byParent.set(p, []);
    byParent.get(p).push(t);
  }
  for (const list of byParent.values()) list.sort((a, b) => a.sort_order - b.sort_order || a.name.localeCompare(b.name));

  const serversByTag = new Map();
  for (const c of cards) {
    for (const t of c.tags || []) {
      if (!serversByTag.has(t.id)) serversByTag.set(t.id, []);
      serversByTag.get(t.id).push(c);
    }
  }

  const groups = [];
  const visited = new Set();
  function emit(tag, depth) {
    if (visited.has(tag.id)) return;
    visited.add(tag.id);
    const servers = (serversByTag.get(tag.id) || []).slice().sort(sortFn);
    const kids = byParent.get(tag.id) || [];
    groups.push({ key: "tag:" + tag.id, name: tag.name, depth, cards: servers, hasChildren: kids.length > 0 || servers.length > 0 });
    for (const kid of kids) emit(kid, depth + 1);
  }
  for (const root of byParent.get(0) || []) emit(root, 0);
  for (const t of forest) if (!visited.has(t.id)) emit(t, 0); // cycle / disconnected -> surface as a root

  const untagged = cards.filter((c) => !(c.tags || []).length).slice().sort(sortFn);
  if (untagged.length) groups.push({ key: "untagged", name: "Untagged", depth: 0, cards: untagged, hasChildren: true });

  return groups;
}

/* The bound on a walk up the tag forest: the desktop rules' TagTreeRules.MaxWalkSteps, far above the four levels the
   tag writers allow, so it only ever ends a malformed chain. */
export const MAX_TAG_WALK_STEPS = 64;

/* #7984: tag id -> the names of the tags above it, nearest first, from the forest /api/fleet returns (each tag with its
   parent_id). A parent id with no row ends the walk, a cycle ends it at the first tag seen twice, and no walk takes more
   than MAX_TAG_WALK_STEPS steps. Built once per render and handed to cardMatches. */
export function tagAncestorNames(forest) {
  const byId = new Map((forest || []).map((t) => [t.id, t]));
  const names = new Map();
  for (const t of byId.values()) {
    const seen = new Set([t.id]);
    const up = [];
    let current = t;
    for (let step = 0; step < MAX_TAG_WALK_STEPS && current.parent_id != null; step++) {
      const parent = byId.get(current.parent_id);
      if (!parent || seen.has(parent.id)) break;
      seen.add(parent.id);
      up.push(parent.name || "");
      current = parent;
    }
    names.set(t.id, up);
  }
  return names;
}

/* Name/tag filter, matching the desktop apps' ServerOverviewFilter rule: an empty term matches everything,
   otherwise a case-insensitive substring of the display name, the instance name, any of the server's tag
   names (#2020), or the name of any tag above one of its tags (#7984, `ancestorNames` from tagAncestorNames) — so
   `prod` finds sql-prod-01, everything tagged Production and everything tagged under Production, as on the desktop. */
export function cardMatches(c, q, ancestorNames) {
  const needle = (q || "").trim().toLowerCase();
  if (!needle) return true;
  const has = (s) => (s || "").toLowerCase().includes(needle);
  return (
    has(c.display_name) ||
    has(c.server_name) ||
    (c.tags || []).some((t) => has(t.name) || ((ancestorNames && ancestorNames.get(t.id)) || []).some(has))
  );
}


/**
 * The sidebar's server list as an ordered row model, so the page only paints it. `opts`:
 *   term        the search box text (matched by cardMatches; empty matches everything)
 *   grouped     false: one flat list; true: Favourites, then the tag tree depth-first, then Untagged
 *   sortFn      the card order (the sidebar passes favourites-first by display name)
 *   isFavorite  (card) => bool
 *   isCollapsed (id) => bool, id being a tag id or the fixed ids "favourites" and "untagged"
 * Rows are { kind: "server", card, depth } and
 * { kind: "group", id, name, depth, count, subtreeCount, collapsed, hasChildren }: `count` is the group's own
 * servers, `subtreeCount` the DISTINCT servers in the group and everything under it (what a collapsed header hides).
 * A group appears only when it or one of its descendants holds a matching server, so an unused tag never takes a
 * line in a narrow list. A server in several tags is listed under each. Collapsing a group hides its whole
 * subtree, except while the (trimmed) term is non-empty: a search ignores collapse state so a group holding a
 * match is never shown closed over it (the stored state is the caller's and is untouched). `empty` is true when
 * the term left nothing to show.
 */
export function sidebarRows(cards, forest, opts) {
  const ancestors = tagAncestorNames(forest || []);
  const matched = cards.filter((c) => cardMatches(c, opts.term, ancestors));
  const sortFn = opts.sortFn;
  if (!opts.grouped) {
    return { rows: matched.slice().sort(sortFn).map((card) => ({ kind: "server", card, depth: 0 })), empty: matched.length === 0 };
  }

  const groups = [];
  const favourites = matched.filter(opts.isFavorite).sort(sortFn);
  if (favourites.length) groups.push({ id: "favourites", name: "Favourites", depth: 0, cards: favourites, hasChildren: true });
  for (const g of buildTagGroups(forest || [], matched, sortFn)) {
    groups.push({ id: g.key === "untagged" ? "untagged" : Number(g.key.slice(4)), name: g.name, depth: g.depth, cards: g.cards, hasChildren: g.hasChildren });
  }

  const keep = groups.map((g) => g.cards.length > 0);
  for (let i = groups.length - 2; i >= 0; i--) {
    for (let j = i + 1; j < groups.length && groups[j].depth > groups[i].depth; j++) if (keep[j]) keep[i] = true;
  }

  const searching = (opts.term || "").trim() !== "";
  const rows = [];
  let hideBelow = Infinity;
  groups.forEach((g, i) => {
    if (!keep[i] || g.depth > hideBelow) return;
    hideBelow = Infinity;
    const collapsed = !searching && opts.isCollapsed(g.id);
    const subtree = new Set(g.cards.map((c) => c.server_id));
    for (let j = i + 1; j < groups.length && groups[j].depth > g.depth; j++) for (const c of groups[j].cards) subtree.add(c.server_id);
    rows.push({ kind: "group", id: g.id, name: g.name, depth: g.depth, count: g.cards.length, subtreeCount: subtree.size, collapsed, hasChildren: g.hasChildren });
    if (collapsed) {
      hideBelow = g.depth;
      return;
    }
    for (const card of g.cards) rows.push({ kind: "server", card, depth: g.depth + 1 });
  });
  return { rows, empty: rows.length === 0 };
}
