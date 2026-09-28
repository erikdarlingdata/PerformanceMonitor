/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * PURE client-side derivation for the #1563 custom-view composer — NO DOM, NO fetch, and it imports nothing (so
 * it unit-tests in isolation and can never pull a browser/chart dependency in). Given a parsed read SAMPLE (the
 * JSON object a /api/read/{tool} returned) and a target viz, it infers a sensible starting vizcfg so the
 * composer seeds its sub-editor instead of making the operator hand-author every column:
 *   - rowsKey  = the top-level key whose value is a non-empty array (the row set the table/line/bandlist plots)
 *   - columns / stats / series = the row's / object's scalar keys, each with a humanized label and a FORMATTERS
 *     key guessed from the column NAME + the sample VALUE (mirrors util.js's registry: int/num1/ms/mb/pct/...)
 *   - line xKey = the first timestamp-looking key; series = the remaining numeric keys, colored from the palette
 * The palette is a PARAMETER (the composer passes charts.js SERIES_COLORS) so this file stays import-free.
 * Everything is deterministic; the composer always lets the operator override the guesses.
 */

/** An ISO-8601-ish naive/zoned timestamp at the START of a string (matches the store's naive-UTC columns). */
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}/;

/** Whether a sample VALUE looks like a timestamp string (so a *_time-less key still formats as time). */
export function looksLikeTimestamp(value) {
  return typeof value === "string" && ISO_DATE_RE.test(value);
}

/** A scalar is anything that can render in a cell/stat: string, number, boolean, or null (never an object/array). */
export function isScalar(value) {
  return value === null || typeof value !== "object";
}

/** snake_case / lower_case key -> a Title Case label ("wait_type" -> "Wait Type"). */
export function humanizeKey(key) {
  return String(key == null ? "" : key)
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * Guess a FORMATTERS registry key (util.js) for a column from its NAME and a sample VALUE. A concrete
 * value wins first (a boolean is bool, an ISO string is time, any other string is text — a non-numeric string
 * can't take a numeric formatter); otherwise the name's unit suffix decides (ms / mb / pct), then a number
 * falls back to int (whole) or num1 (fractional), and finally a count/id name hint or plain text.
 */
export function inferFormat(key, value) {
  const k = String(key == null ? "" : key).toLowerCase();

  if (typeof value === "boolean") return "bool";
  if (looksLikeTimestamp(value)) return k.startsWith("last_") ? "reltime" : "time";
  if (typeof value === "string") return "text";

  /* value is a number, null, or undefined -> lean on the name. */
  if (/(_ms$|_ms_|milliseconds|latency|duration|elapsed)/.test(k)) return "ms";
  if (/(_mb$|_mb_|megabytes)/.test(k)) return "mb";
  if (/(pct|percent)/.test(k)) return "pct";
  if (k.endsWith("_enabled") || /^(is_|has_|can_|are_)/.test(k)) return "bool";
  if (k.endsWith("_at") || k.endsWith("_date") || k.startsWith("last_")) return "time";
  if (typeof value === "number") return Number.isInteger(value) ? "int" : "num1";
  if (/(_id$|_count$|count|_runs$|tasks)/.test(k)) return "int";
  return "text";
}

/** The top-level key whose value is a (preferably non-empty) array — the row set a table/line/bandlist plots. */
export function findRowsKey(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) return null;
  for (const [k, v] of Object.entries(data)) {
    if (Array.isArray(v) && v.length > 0) return k;
  }
  /* Fall back to an empty array key so a windowed read with no rows still maps to the right key. */
  for (const [k, v] of Object.entries(data)) {
    if (Array.isArray(v)) return k;
  }
  return null;
}

/** The first row object under `rowsKey`, or null. */
function firstRow(data, rowsKey) {
  const rows = data && rowsKey ? data[rowsKey] : null;
  return Array.isArray(rows) && rows.length && rows[0] && typeof rows[0] === "object" ? rows[0] : null;
}

/** Derive table columns from a row array — every scalar key of the first row, labeled + formatted. */
export function deriveColumns(rows) {
  const first = Array.isArray(rows) && rows.length ? rows[0] : null;
  if (!first || typeof first !== "object") return [];
  return Object.keys(first)
    .filter((k) => isScalar(first[k]))
    .map((k) => ({ key: k, label: humanizeKey(k), format: inferFormat(k, first[k]) }));
}

/** Derive stat tiles from a flat top-level object — every scalar top-level key. */
export function deriveStats(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) return [];
  return Object.entries(data)
    .filter(([, v]) => isScalar(v))
    .map(([k, v]) => ({ key: k, label: humanizeKey(k), format: inferFormat(k, v) }));
}

/** Derive a line config: rowsKey + a timestamp-ish xKey + one series per remaining numeric key (palette-colored). */
export function deriveLine(data, palette) {
  const rowsKey = findRowsKey(data) || "";
  const cfg = { rowsKey, xKey: "", series: [] };
  const first = firstRow(data, rowsKey);
  if (!first) return cfg;

  const keys = Object.keys(first);
  cfg.xKey =
    keys.find((k) => looksLikeTimestamp(first[k])) ||
    keys.find((k) => /(_at$|_time$|^time$|_date$)/.test(k.toLowerCase())) ||
    keys[0] ||
    "";

  const colors = Array.isArray(palette) ? palette : [];
  let i = 0;
  for (const k of keys) {
    if (k === cfg.xKey) continue;
    if (typeof first[k] === "number") {
      cfg.series.push({
        key: k,
        label: humanizeKey(k),
        color: colors.length ? colors[i % colors.length] : undefined,
      });
      i++;
    }
  }
  return cfg;
}

/** Derive a bandlist config: rowsKey + a best-guess primary/band/reason key (all operator-overridable). */
export function deriveBandlist(data) {
  const rowsKey = findRowsKey(data) || "";
  const first = firstRow(data, rowsKey) || {};
  const keys = Object.keys(first);
  const lc = (s) => String(s).toLowerCase();
  const bandKey = keys.find((k) => lc(k).includes("band") || lc(k).includes("severity")) || "";
  const primaryKey = keys.find((k) => typeof first[k] === "string" && k !== bandKey) || keys[0] || "";
  const reasonKey =
    keys.find((k) => /(reason|message|detail|status)/.test(lc(k)) && k !== primaryKey && k !== bandKey) || "";
  return { rowsKey, primaryKey, bandKey, reasonKey };
}

/** Suggest a viz for a sample: a table when it carries a row array, else a stat block over its flat scalars. */
export function suggestViz(data) {
  return findRowsKey(data) ? "table" : "stat";
}

/** Derive the full vizcfg for a given viz from a sample (the composer's seed; empty {} for an unknown viz). */
export function deriveVizConfig(data, viz, palette) {
  switch (viz) {
    case "table": {
      const rowsKey = findRowsKey(data) || "";
      const rows = rowsKey && data ? data[rowsKey] : [];
      return { rowsKey, columns: deriveColumns(rows) };
    }
    case "line":
      return deriveLine(data, palette);
    case "stat":
      return { stats: deriveStats(data) };
    case "bandlist":
      return deriveBandlist(data);
    default:
      return {};
  }
}
