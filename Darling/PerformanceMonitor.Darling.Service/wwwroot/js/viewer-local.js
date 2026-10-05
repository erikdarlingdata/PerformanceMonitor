/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The desktop viewer's per-machine conveniences (#4843), kept per BROWSER in localStorage: favourite servers,
 * alert acknowledgements, the alert-count badge that respects them, the per-severity colours and the collapsed
 * sidebar and the sidebar's tag grouping. Nothing here reaches the store: like the desktop's own per-PC files, a favourite or an acknowledgement
 * does not follow a user to another browser or machine, and an acknowledgement quiets only this browser's badge
 * (alert delivery is untouched).
 *
 * Storage discipline: every key is namespaced under "darling.local." and carries a version, so a later shape can
 * change without misreading an old value. A stored value that is absent, corrupt, from another version or hand-edited
 * reads as the default and never throws; the page still works. Only server ids, instants, booleans and #rrggbb
 * literals are stored - never a name, a connection string or a token.
 *
 * The live state is held at module scope (the 60 s poll rebuilds the sidebar and the fleet page); localStorage is
 * the durable copy, read once at load and rewritten on every change.
 *
 * Pure: no imports and no DOM, so the rules run under Node with a stubbed Storage (setStorage).
 */

export const KEY_FAVORITES = "darling.local.favorites.v1";
export const KEY_ACKS = "darling.local.acks.v1";
export const KEY_SEVERITY_COLORS = "darling.local.severityColors.v1";
export const KEY_SIDEBAR = "darling.local.sidebar.v1";
const VERSION = 1;
const MAX_IDS = 5000;

/** The severities with a colour choice, in the order the settings list them, and the palette each falls back to. */
export const SEVERITIES = ["critical", "warning", "info"];
export const DEFAULT_SEVERITY_COLORS = { critical: "#e57373", warning: "#ffd54f", info: "#a8aeba" };
const HEX = /^#[0-9a-fA-F]{6}$/;

let storage = null;
try {
  storage = typeof localStorage === "undefined" ? null : localStorage;
} catch {
  storage = null;
}

let favorites = new Set();
let acks = new Map(); // server_id -> epoch ms of the acknowledgement
let colors = {}; // severity -> "#rrggbb" (only valid choices)
let sidebarCollapsed = false;
let sidebarGrouped = false;
let sidebarGroups = new Set(); // collapsed sidebar groups: tag ids, plus the fixed ids "favourites" and "untagged"
let attention = new Map(); // server_id -> { count, critical, latestMs } from the last alert read
const listeners = new Set();

function readJson(key) {
  try {
    const raw = storage && storage.getItem(key);
    if (typeof raw !== "string") return null;
    const v = JSON.parse(raw);
    return v && typeof v === "object" && !Array.isArray(v) && v.v === VERSION ? v : null;
  } catch {
    return null;
  }
}

function writeJson(key, value) {
  try {
    if (storage) storage.setItem(key, JSON.stringify({ v: VERSION, ...value }));
  } catch {
    /* private mode or a full quota: the choice holds for this page load only */
  }
}

function isServerId(n) {
  return Number.isSafeInteger(n) && n > 0;
}

/** The fixed sidebar group ids that are not tags. */
const FIXED_GROUPS = ["favourites", "untagged"];

function isGroupId(g) {
  return isServerId(g) || FIXED_GROUPS.includes(g);
}

/** Re-reads every value from storage (the page load, and the tests' stand-in for a rebuilt page). */
export function reload() {
  const f = readJson(KEY_FAVORITES);
  favorites = new Set(f && Array.isArray(f.ids) ? f.ids.filter(isServerId).slice(0, MAX_IDS) : []);

  acks = new Map();
  const a = readJson(KEY_ACKS);
  if (a && a.acks && typeof a.acks === "object" && !Array.isArray(a.acks)) {
    for (const [k, t] of Object.entries(a.acks)) {
      const id = /^\d{1,9}$/.test(k) ? Number(k) : NaN;
      if (isServerId(id) && typeof t === "number" && Number.isFinite(t) && t > 0 && acks.size < MAX_IDS) acks.set(id, t);
    }
  }

  colors = {};
  const c = readJson(KEY_SEVERITY_COLORS);
  if (c && c.colors && typeof c.colors === "object" && !Array.isArray(c.colors)) {
    for (const sev of SEVERITIES) {
      const v = c.colors[sev];
      if (typeof v === "string" && HEX.test(v)) colors[sev] = v.toLowerCase();
    }
  }

  const s = readJson(KEY_SIDEBAR);
  sidebarCollapsed = !!(s && s.collapsed === true);
  sidebarGrouped = !!(s && s.grouped === true);
  sidebarGroups = new Set(s && Array.isArray(s.collapsedGroups) ? s.collapsedGroups.filter(isGroupId).slice(0, MAX_IDS) : []);
}

/** Test seam: replaces the Storage and re-reads. */
export function setStorage(s) {
  storage = s;
  attention = new Map();
  reload();
}

export function onChange(fn) {
  listeners.add(fn);
}

function changed() {
  for (const fn of listeners) {
    try {
      fn();
    } catch {
      /* one listener's failure must not stop the others */
    }
  }
}

/* ─────────────────────────── favourites ─────────────────────────── */

export function isFavorite(serverId) {
  return favorites.has(serverId);
}

export function toggleFavorite(serverId) {
  if (!isServerId(serverId)) return false;
  if (favorites.has(serverId)) favorites.delete(serverId);
  else if (favorites.size < MAX_IDS) favorites.add(serverId);
  writeJson(KEY_FAVORITES, { ids: [...favorites] });
  changed();
  return favorites.has(serverId);
}

/** Wraps a card comparator so favourites sort ahead of everything else, the comparator ordering each side. */
export function favoritesFirst(cmp) {
  return (a, b) => (isFavorite(b.server_id) ? 1 : 0) - (isFavorite(a.server_id) ? 1 : 0) || cmp(a, b);
}

/* ─────────────────────────── alert count and acknowledgement ─────────────────────────── */

/** The instant of a stored alert_time in epoch ms. The store's instants are naive UTC, so a value with no zone is read as UTC. */
export function alertMs(value) {
  if (typeof value !== "string") return NaN;
  return Date.parse(/(Z|[+-]\d\d:?\d\d)$/.test(value) ? value : value + "Z");
}

/**
 * Per-server attention from a get_alert_history page: the actionable rows only. A muted row (a mute rule suppressed
 * it) and a resolution row (good news) never count, the desktop badge's own rule.
 */
export function summarizeAlerts(rows) {
  const out = new Map();
  for (const r of Array.isArray(rows) ? rows : []) {
    if (!r || !isServerId(r.server_id) || r.muted === true || r.severity === "resolution" || r.dismissed === true) continue;
    const t = alertMs(r.alert_time);
    const s = out.get(r.server_id) || { count: 0, critical: false, latestMs: 0 };
    s.count++;
    if (r.severity === "critical") s.critical = true;
    if (Number.isFinite(t) && t > s.latestMs) s.latestMs = t;
    out.set(r.server_id, s);
  }
  return out;
}

/** Applies a fresh alert read: an acknowledgement is cleared by an alert newer than it (ack-until-worse). */
export function updateAttention(rows) {
  attention = summarizeAlerts(rows);
  let cleared = false;
  for (const [id, ackMs] of [...acks]) {
    const s = attention.get(id);
    if (s && s.latestMs > ackMs) {
      acks.delete(id);
      cleared = true;
    }
  }
  if (cleared) persistAcks();
  changed();
}

/** What a server's badge shows: { count, critical, show } - show is false while acknowledged or with nothing to show. */
export function attentionFor(serverId) {
  const s = attention.get(serverId);
  if (!s) return { count: 0, critical: false, show: false };
  return { count: s.count, critical: s.critical, show: s.count > 0 && !acks.has(serverId) };
}

export function isAcknowledged(serverId) {
  return acks.has(serverId);
}

function persistAcks() {
  const obj = {};
  for (const [id, t] of acks) obj[String(id)] = t;
  writeJson(KEY_ACKS, { acks: obj });
}

export function acknowledge(serverId, nowMs = Date.now()) {
  if (!isServerId(serverId)) return;
  acks.set(serverId, nowMs);
  persistAcks();
  changed();
}

const ATTENTION_TTL_MS = 30000;
let attentionRead = null;
let attentionReadAt = 0;

/**
 * Refreshes the badge counts with ONE get_alert_history read for the whole fleet. Callers within the TTL share the
 * one request, so the sidebar poll and the fleet page cost the store one read per tick, not one each. A failed read
 * leaves the last counts in place.
 */
export function refreshAttention(readTool, now = Date.now()) {
  if (attentionRead && now - attentionReadAt < ATTENTION_TTL_MS) return attentionRead;
  attentionReadAt = now;
  attentionRead = Promise.resolve(readTool("get_alert_history", { hours_back: 24, limit: 1000 }))
    .then((res) => {
      if (res && res.kind === "data" && res.data && Array.isArray(res.data.alerts)) updateAttention(res.data.alerts);
    })
    .catch(() => {
      attentionReadAt = 0;
    });
  return attentionRead;
}

/* ─────────────────────────── severity colours ─────────────────────────── */

/** The stored choice for a severity, or null for the palette default. Always a #rrggbb literal when not null. */
export function severityColor(severity) {
  return Object.prototype.hasOwnProperty.call(colors, severity) ? colors[severity] : null;
}

/** Sets one severity's colour. Anything but a #rrggbb literal clears the choice (the palette default). */
export function setSeverityColor(severity, value) {
  if (!SEVERITIES.includes(severity)) return;
  if (typeof value === "string" && HEX.test(value)) colors[severity] = value.toLowerCase();
  else delete colors[severity];
  writeJson(KEY_SEVERITY_COLORS, { colors });
  changed();
}

export function resetSeverityColors() {
  colors = {};
  writeJson(KEY_SEVERITY_COLORS, { colors });
  changed();
}

/** Puts the choices on a style declaration as --sev-* custom properties; a severity with no choice is removed. */
export function applySeverityColors(style) {
  if (!style || typeof style.setProperty !== "function" || typeof style.removeProperty !== "function") return;
  for (const sev of SEVERITIES) {
    const v = severityColor(sev);
    if (v) style.setProperty("--sev-" + sev, v);
    else style.removeProperty("--sev-" + sev);
  }
}

/* ─────────────────────────── sidebar ─────────────────────────── */

export function isSidebarCollapsed() {
  return sidebarCollapsed;
}

export function setSidebarCollapsed(collapsed) {
  sidebarCollapsed = collapsed === true;
  writeSidebar();
  changed();
}

/** One writer for the sidebar key, so a change to one field never drops the others. */
function writeSidebar() {
  writeJson(KEY_SIDEBAR, { collapsed: sidebarCollapsed, grouped: sidebarGrouped, collapsedGroups: [...sidebarGroups] });
}

/** Whether the sidebar lists servers grouped by tag (false: the flat list). */
export function isSidebarGrouped() {
  return sidebarGrouped;
}

export function setSidebarGrouped(grouped) {
  sidebarGrouped = grouped === true;
  writeSidebar();
  changed();
}

/** Whether a sidebar group (a tag id, "favourites" or "untagged") is collapsed. */
export function isSidebarGroupCollapsed(id) {
  return sidebarGroups.has(id);
}

/** Collapses or expands one sidebar group; an id that is neither a tag id nor a fixed group id is ignored. */
export function toggleSidebarGroup(id) {
  if (!isGroupId(id)) return;
  if (sidebarGroups.has(id)) sidebarGroups.delete(id);
  else if (sidebarGroups.size < MAX_IDS) sidebarGroups.add(id);
  writeSidebar();
  changed();
}

reload();
