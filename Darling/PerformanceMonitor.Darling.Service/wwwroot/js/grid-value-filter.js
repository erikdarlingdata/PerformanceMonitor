/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The value-list half of a table's column filter (#5565): the pure logic behind the Excel-style checklist that sits
 * beside the text match, and the bounded copy of the active filters that lives in the browser's localStorage. panels.js
 * owns the DOM and the live filters; this file owns the arithmetic, so a test can drive it without a page.
 *
 * A column's value filter is { mode, set, blank }:
 *   - mode "none": every value passes (the list is all ticked), set is empty and blank false;
 *   - mode "hide": the values in `set` do not pass, and neither do the blank cells when `blank` is true;
 *   - mode "showOnly": only the values in `set` pass, and the blank cells only when `blank` is true.
 * Blank is its own flag, never the string "(Blanks)", so a real value spelled that way is an ordinary value. Values
 * compare ordinal, ignoring case the way .NET's OrdinalIgnoreCase does (valueKey: "app" and "APP" are one value,
 * "ss" and the sharp s are two), and a cell is blank when it is null, empty or whitespace. The mode is chosen whenever the list changes (nextValues): all ticked is "none", fewer unticked than
 * ticked is "hide" the unticked, anything else is "showOnly" the ticked. Hide is what lets a login that first shows
 * up after a refresh still show; showOnly is what keeps a refresh from showing a value nobody ticked.
 */

/** The longest value (in characters) a column may hold and still be offered as a list; longer text keeps the text match. */
export const VALUE_LIST_MAX_LEN = 256;
/** At most this many values are listed; the search box still reaches every one. */
export const VALUE_LIST_CAP = 1000;
/** The localStorage key holding the active filters of every grid (version 1 of the format). */
export const FILTER_STORE_KEY = "pm.gridFilters.v1";
/** At most this many grids are kept; the least recently used go first. */
export const FILTER_STORE_MAX_GRIDS = 200;
/** A column whose value set is longer than this is not kept across a restart (a cut set would change what it hides). */
export const FILTER_STORE_MAX_VALUES = 1000;
const STORE_MAX_TEXT = 1000;
const STORE_MAX_KEY = 2000;

export const NO_VALUES = Object.freeze({ mode: "none", set: Object.freeze([]), blank: false });

/**
 * The comparison key of a value: ordinal, ignoring case, the way .NET's StringComparison.OrdinalIgnoreCase does it (the
 * desktop twin): each character is uppercased on its own with the simple mapping, so a character whose uppercase form
 * is longer than one character (the sharp s, which String.toUpperCase turns into "SS") stays as it is, and so do the
 * dotless i and the long s, which .NET does not fold into "I" and "S". JavaScript has no simple-mapping call, so a
 * character with a longer full mapping that also has a different simple one (a few Greek capitals with iota
 * subscript) is the one place this can differ.
 */
export const valueKey = (s) => {
  const text = String(s);
  if (/^[\x00-\x7f]*$/.test(text)) return text.toUpperCase();
  let out = "";
  for (const ch of text) {
    const up = ch.toUpperCase();
    const folds = up.length === ch.length && (ch.charCodeAt(0) < 128 || up.charCodeAt(0) >= 128);
    out += folds ? up : ch;
  }
  return out;
};

/**
 * The name rule for a column that never gets a value list (#5565), the twin of the desktop's ColumnValueListColumns:
 * query and statement text, plans, XML, scripts, prose (messages, details, descriptions, errors), and the display
 * strings that stand in for a number or a time. A list of them is a wall of near-unique strings, and its ticked
 * values would be kept in the browser. The key is compared with its punctuation dropped, lower-cased, so the page's
 * snake_case keys ("blocked_sql_text") and the desktop's property names ("BlockedSqlText") read the same. A column
 * marked `valueList: false` is excluded as well. ColumnValueListNameRuleTests holds these two arrays to the desktop's.
 */
export const NO_LIST_SUFFIXES = [
  "text", "formatted", "display", "xml", "plan", "message", "definition", "preview", "sql", "detail", "details",
  "description", "statement", "query", "command", "json", "graph", "script", "error", "info",
];
export const NO_LIST_FRAGMENTS = ["querytext", "queryplan", "sqltext", "statementtext", "batchtext", "planxml", "textdata"];

/** Whether a column's key (or a stored column id, whose part before the label separator is the key) is one that gets no list. */
export function noListName(key) {
  const name = String(key ?? "").split("\u0001")[0].replace(/[^A-Za-z0-9]/g, "").toLowerCase();
  if (!name) return true;
  return NO_LIST_FRAGMENTS.some((f) => name.includes(f)) || NO_LIST_SUFFIXES.some((s) => name.endsWith(s));
}

/** Whether a table column may be offered a value list by its declaration: not opted out and not named like prose or a statement. */
export const listableColumn = (c) => !!c && c.valueList !== false && !noListName(c.key);

/** Whether the value filter narrows anything. */
export const valuesOn = (v) => !!v && (v.mode === "hide" || v.mode === "showOnly");

/**
 * The distinct values of a column. `cells` is [{ blank, text }] in grid order. Returns { blank, values, maxLen }: blank
 * is true when any cell is blank, values are the distinct non-blank texts (the first spelling seen stands for a value
 * that differs only in case) sorted ordinal ignoring case, maxLen is the longest text.
 */
export function collectValues(cells) {
  const seen = new Map();
  let blank = false;
  let maxLen = 0;
  for (const cell of cells) {
    if (cell.blank) {
      blank = true;
      continue;
    }
    if (cell.text.length > maxLen) maxLen = cell.text.length;
    const k = valueKey(cell.text);
    if (!seen.has(k)) seen.set(k, cell.text);
  }
  const keys = [...seen.keys()].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
  return { blank, values: keys.map((k) => seen.get(k)), maxLen };
}

/** A value filter ready to test many cells: its set as comparison keys. */
export function compileValues(v) {
  const f = v || NO_VALUES;
  return { mode: f.mode, keys: new Set(f.set.map(valueKey)), blank: !!f.blank };
}

/** Whether a cell is ticked in the list (so passes the value part). */
export function valuePasses(c, blank, text) {
  if (c.mode === "none") return true;
  if (blank) return c.mode === "showOnly" ? c.blank : !c.blank;
  const has = c.keys.has(valueKey(text));
  return c.mode === "showOnly" ? has : !has;
}

/**
 * The value filter after the reader changes the ticks. `universe` is collectValues over the rows now; `prev` the
 * filter in force; `mutate(model)` edits model = { ticked: Set of comparison keys, blank: bool } (blank is the
 * (Blanks) entry's tick). A value the filter holds that the rows no longer have is not in the universe: it keeps the
 * state it had (hidden stays hidden under "hide", shown stays shown under "showOnly") for as long as the new filter
 * is the same kind, and drops when the kind changes.
 */
export function nextValues(universe, prev, mutate) {
  const before = compileValues(prev);
  const known = new Set(universe.values.map(valueKey));
  const model = { ticked: new Set(), blank: universe.blank ? valuePasses(before, true, "") : true };
  for (const v of universe.values) if (valuePasses(before, false, v)) model.ticked.add(valueKey(v));
  mutate(model);
  const entries = universe.values.length + (universe.blank ? 1 : 0);
  const tickedN = universe.values.filter((v) => model.ticked.has(valueKey(v))).length + (universe.blank && model.blank ? 1 : 0);
  const unticked = entries - tickedN;
  if (unticked === 0) return NO_VALUES;
  const prevSet = prev ? prev.set : [];
  const gone = prevSet.filter((s) => !known.has(valueKey(s)));
  if (unticked < tickedN) {
    const hidden = universe.values.filter((v) => !model.ticked.has(valueKey(v)));
    const keep = prev && prev.mode === "hide" ? gone : [];
    return { mode: "hide", set: [...hidden, ...keep], blank: universe.blank ? !model.blank : !!(prev && prev.mode === "hide" && prev.blank) };
  }
  const shown = universe.values.filter((v) => model.ticked.has(valueKey(v)));
  const keep = prev && prev.mode === "showOnly" ? gone : [];
  return { mode: "showOnly", set: [...shown, ...keep], blank: universe.blank ? model.blank : !!(prev && prev.mode === "showOnly" && prev.blank) };
}

/** "hides 3 values" / "shows 2 values" for a chip or tooltip; the blank entry counts as a value. */
export function valuesPhrase(v) {
  if (!valuesOn(v)) return "";
  const n = v.set.length + (v.blank ? 1 : 0);
  return (v.mode === "hide" ? "hides " : "shows ") + n + (n === 1 ? " value" : " values");
}

/* ---- the kept copy ---- */

const isStr = (x, max) => typeof x === "string" && x.length <= max;

function parseValues(v) {
  if (!v || typeof v !== "object") return null;
  if (v.mode !== "hide" && v.mode !== "showOnly") return null;
  if (!Array.isArray(v.set) || v.set.length > FILTER_STORE_MAX_VALUES || typeof v.blank !== "boolean") return null;
  if (!v.set.every((s) => isStr(s, STORE_MAX_TEXT))) return null;
  return { mode: v.mode, set: [...v.set], blank: v.blank };
}

/** Whether a stored column filter narrows anything (a text match, an Is empty test, or a value part). */
const entryOn = (e) => e.op === "isEmpty" || e.op === "isNotEmpty" || String(e.text ?? "").trim() !== "" || !!e.values;

/** One stored column filter, checked field by field; null when it is not a usable one. `ops` lists the operators the page knows. */
function parseColumn(f, ops) {
  if (!f || typeof f !== "object") return null;
  const op = typeof f.op === "string" && ops.includes(f.op) ? f.op : "contains";
  const text = typeof f.text === "string" && f.text.length <= STORE_MAX_TEXT ? f.text : "";
  const values = f.values === undefined ? null : parseValues(f.values);
  const textOn = text.trim() !== "" || op === "isEmpty" || op === "isNotEmpty";
  if (!textOn && !values) return null;
  return values ? { op, text, values } : { op, text };
}

/**
 * The text kept for `grids` (a Map of grid key -> Map of column id -> { op, text, values? }, least recently used
 * first): at most FILTER_STORE_MAX_GRIDS grids, the newest kept, the value part of a column dropped when its set is
 * longer than FILTER_STORE_MAX_VALUES (it stays in the page, it is just not kept, and the text match is), and a column
 * flagged `session` (a text match on a column that gets no list) left out altogether.
 */
export function serializeFilters(grids) {
  const list = [...grids].map(([key, cols]) => {
    const out = {};
    for (const [id, f] of cols) {
      /* A column named like a statement, a plan or a message keeps nothing. A text match typed on a column the page
         marked `session` (valueList: false) lasts the session only, so only its value part, if it still holds one, is kept. */
      if (noListName(id)) continue;
      const v = f.values && valuesOn(f.values) && f.values.set.length <= FILTER_STORE_MAX_VALUES ? f.values : null;
      const entry = f.session ? { op: "contains", text: "" } : { op: f.op, text: f.text ?? "" };
      if (v) entry.values = { mode: v.mode, set: [...v.set], blank: v.blank };
      if (entryOn(entry)) out[id] = entry;
    }
    return [key, out];
  }).filter(([, out]) => Object.keys(out).length > 0).slice(-FILTER_STORE_MAX_GRIDS);
  return JSON.stringify({ v: 1, grids: list });
}

/**
 * The filters in a stored text, as a Map like serializeFilters takes. Anything unreadable gives an empty map, and a
 * grid, a column or a value part that does not check out is left out on its own. Never throws.
 */
export function parseFilters(text, ops) {
  const result = new Map();
  let doc;
  try {
    doc = JSON.parse(text);
  } catch {
    return result;
  }
  if (!doc || typeof doc !== "object" || doc.v !== 1 || !Array.isArray(doc.grids)) return result;
  for (const g of doc.grids.slice(-FILTER_STORE_MAX_GRIDS)) {
    if (!Array.isArray(g) || g.length !== 2 || !isStr(g[0], STORE_MAX_KEY) || !g[1] || typeof g[1] !== "object") continue;
    const cols = new Map();
    for (const [id, f] of Object.entries(g[1])) {
      if (noListName(id)) continue;
      const col = parseColumn(f, ops);
      if (col) cols.set(id, col);
    }
    if (cols.size) {
      result.delete(g[0]);
      result.set(g[0], cols);
    }
  }
  return result;
}
