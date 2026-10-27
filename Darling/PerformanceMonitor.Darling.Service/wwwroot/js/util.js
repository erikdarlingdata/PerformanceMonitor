/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Shared leaf utilities for Darling Web (#1562): DOM builders, UTC->local time, value formatters, and the API
 * fetch helper. This module imports only database-filter-reads.js (pure data) and plain-text.js (pure text rules), neither of which imports
 * anything (so it is
 * still the base of the module DAG: app/panels/charts/pages all import it, so there is no import cycle). Two rules are
 * enforced HERE so every caller inherits them:
 *   R4 (XSS): the el() builder assigns untrusted text ONLY through textContent / text nodes; it throws if a
 *     caller ever tries to pass raw HTML, so no data path can reach innerHTML.
 *   R5 (time): every timestamp from the API is naive UTC ISO-8601 (no zone suffix) — parseUtc() appends 'Z'
 *     so the browser reads it as UTC, then formatting localizes it to the viewer's zone.
 */

import { FILTERED, readScope } from "./database-filter-reads.js";
import { plainText } from "./plain-text.js";

/* ─────────────────────────── DOM builders (textContent-only) ─────────────────────────── */

/**
 * Build an element. `props` supports: class, text (safe textContent), onClick, dataset (object), and any
 * other key set as an attribute. `children` may be nodes, strings (-> text nodes), arrays, or null/false
 * (skipped). There is deliberately NO html/innerHTML affordance — untrusted data can only ever become text.
 */
export function el(tag, props = {}, children = []) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (v == null) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v; // safe: text, never markup
    else if (k === "html") throw new Error("el(): the 'html' prop is banned — render data as text (R4/XSS).");
    else if (k === "onClick") node.addEventListener("click", v);
    else if (k === "onActivate") makeActivatable(node, v);
    else if (k === "dataset") Object.assign(node.dataset, v);
    else node.setAttribute(k, v);
  }
  appendChildren(node, children);
  return node;
}

/**
 * Make a non-button element behave like a button for keyboard users: role=button, focusable, and Enter/Space
 * activate it exactly like a click. Used by the clickable-div affordances (fleet cards, band rows, sidebar
 * servers) so they get a keyboard path + a :focus-visible ring without becoming real <button>s (a11y).
 */
export function makeActivatable(node, handler) {
  node.setAttribute("role", "button");
  node.setAttribute("tabindex", "0");
  node.addEventListener("click", handler);
  node.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      handler(e);
    }
  });
}

function appendChildren(node, children) {
  if (children == null || children === false) return;
  if (Array.isArray(children)) {
    for (const c of children) appendChildren(node, c);
    return;
  }
  if (children instanceof Node) {
    node.appendChild(children);
    return;
  }
  node.appendChild(document.createTextNode(String(children))); // safe: text node
}

/**
 * One coloured tag pill (#2020, #5987): the stored #RRGGBB when it is well formed, else the neutral pill. The hex is format-checked
 * before it reaches the style attribute. Server tags on the fleet cards and query tags on the query grids both draw through this one helper.
 */
export function tagPill(name, colour) {
  const safe = /^#[0-9a-fA-F]{6}$/.test(colour || "") ? colour : null;
  return el("span", { class: "tag-pill", text: name, title: name, style: safe ? "background:" + safe : null });
}

/** Remove all children from a node. */
export function clear(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
  return node;
}

/** Replace a container's contents with `content` (a node or array of nodes). */
export function mount(container, content) {
  clear(container);
  appendChildren(container, content);
  return container;
}

/** Collapse whitespace/newlines to single spaces and truncate to `max` chars with an ellipsis. */
export function truncate(s, max = 120) {
  const flat = String(s == null ? "" : s).replace(/\s+/g, " ").trim();
  return flat.length > max ? flat.slice(0, max - 1) + "…" : flat;
}

/**
 * A native <details> disclosure: a one-line truncated `summaryText` that expands to reveal `detail`. Built on
 * the platform <details>/<summary> so it is keyboard-accessible with no JS handlers; every piece of text goes
 * through el()/textContent (R4 — never innerHTML). `detail` may be a node, an array of nodes, or a string.
 */
export function disclosure(summaryText, detail, opts = {}) {
  const summary = el("summary", { class: "disc-summary", title: opts.title || null }, [
    truncate(summaryText, opts.max || 120),
  ]);
  return el("details", { class: "disclosure" }, [summary, el("div", { class: "disc-body" }, detail)]);
}

/* Text longer than this many characters (or holding a line break) gets the expand toggle; shorter text fits a cell as is. */
const CLAMP_MIN_CHARS = 60;

/**
 * A long text cell shown on ONE line (#5644 W11): the full text stays in the node (so a selection, the tooltip and a
 * screen reader read all of it) and the stylesheet clips it with an ellipsis (`.clamp-text`); a click, or Enter/Space,
 * opens the rest in place (`.open`) and closes it again. Long collector notes, errors and job messages made rows
 * hundreds of pixels tall. Copy and CSV export read the row data, not this node, so they still give the full text.
 * `expandable: false` is for a cell whose row already opens the whole text elsewhere (the Job History detail pane).
 * The toggle stops its event so a clickable row (Collectors) is not also activated.
 */
export function clampText(text, opts = {}) {
  const full = String(text == null ? "" : text);
  const node = el("span", { class: "clamp-text", title: full || null, text: full });
  const long = full.length > CLAMP_MIN_CHARS || /[\r\n]/.test(full);
  if (opts.expandable === false || !long) return node;
  node.setAttribute("role", "button");
  node.setAttribute("tabindex", "0");
  node.setAttribute("aria-expanded", "false");
  const toggle = (e) => {
    /* A click that ends a text selection inside the cell is the reader copying, not opening it. */
    if (e && e.type === "click" && typeof window !== "undefined" && window.getSelection) {
      const sel = window.getSelection();
      if (sel && !sel.isCollapsed && String(sel).length > 0 && sel.anchorNode && node.contains(sel.anchorNode)) return;
    }
    if (e && e.stopPropagation) e.stopPropagation();
    const open = node.getAttribute("aria-expanded") !== "true";
    node.setAttribute("aria-expanded", open ? "true" : "false");
    node.classList.toggle("open", open);
  };
  node.addEventListener("click", toggle);
  node.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") {
      e.preventDefault();
      toggle(e);
    }
  });
  return node;
}

/*
 * #3031: a stable id for one of a rollup tile's text rows, so the tile's NUMBER can point at it.
 *
 * Derived from the label rather than from a render counter so the relationship stays inspectable: whoever
 * reads the DOM — an operator, a review, a check — can tell which figure "rollup-deadlocks-recent-sub"
 * belongs to without counting siblings. `used` de-duplicates within one render, because two tiles sharing a
 * label would emit a duplicate id and aria-describedby resolves a duplicate to the FIRST match: a silent
 * mis-association rather than a visible break.
 *
 * Shared rather than per-page (#3045): both the fleet and the AG rollup wire their numbers with it. The
 * de-duplication rule above is subtle enough that a second copy of it is a copy written without the `used`
 * set — correct on a page whose labels all differ, silently wrong the day a tile repeats one. `part` names
 * the row ("lbl", "sub"); a page with only labels passes only "lbl" and needs nothing else from this.
 */
export function rollupTextId(lbl, part, used) {
  const slug = String(lbl == null ? "" : lbl).toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  const base = "rollup-" + (slug || "tile") + "-" + part;
  let id = base;
  for (let n = 2; used.has(id); n++) id = base + "-" + n;
  used.add(id);
  return id;
}

/* ─────────────────────────── state strips ─────────────────────────── */

export function errorStrip(message) {
  readErrors++;
  return el("div", { class: "strip error", role: "alert" }, [message]);
}
export function emptyStrip(message) {
  return el("div", { class: "strip empty" }, [localizeIsoText(plainText(message))]);
}
/** #6445: the note a page that lists only shows. The advice depends on the store: a managed store refuses postgres.webConnectionString,
    so it says only that this dashboard lists and the Viewer app makes the change; a store you brought yourself may give the web an
    admin store login. `lists` finishes "so ..." and `toChange` starts the sentence that names the Viewer. The service's own 403
    sentences come from DarlingWebStoreWrites and say the same. #6600: `adminWritesOff` (the session's admin_writes_off) is true on a managed
    store whose web.allowAdminWrites is off, and then the note also names that switch, in the 403's words. */
export function listOnlyNote(managedStore, lists, toChange, adminWritesOff) {
  return managedStore === true
    ? "This dashboard lists only on this store, so " + lists + ". " + toChange + ", use the Viewer app." +
      (adminWritesOff === true ? " To let a signed-in admin make it here, set web.allowAdminWrites to true in darling.json and restart the service." : "")
    : "This dashboard's store login cannot make changes, so " + lists + ". " + toChange +
      ", use the Viewer app, or give the web an admin store login (set postgres.webConnectionString to an admin login).";
}

/** A non-fatal caveat about otherwise-good data (e.g. the #1665 partial-window notice). The sentence may be one the service
    wrote for an MCP caller (a width note, a truncation note), so it passes through plainText like every message (#5644 W8). */
export function noticeStrip(message) {
  return el("div", { class: "strip notice", role: "status" }, [localizeIsoText(plainText(message))]);
}
/** A note line under a chart, from a payload's own `aggregate_note`: written for an MCP caller, said in the page's words (#5644 W8). */
export function metricNote(text) {
  return el("div", { class: "mp-metric-note", text: plainText(text) });
}

/**
 * What one point of a bucketed trend covers, from the read's own `bucket_minutes` / `bucket` fields: "covers 1 hour",
 * "is one collection", or null when the read states neither (#6359).
 */
export function bucketWidthText(d) {
  const minutes = d && typeof d.bucket_minutes === "number" && d.bucket_minutes > 0 ? d.bucket_minutes : null;
  if (minutes !== null) {
    if (minutes % 1440 === 0) return "covers " + (minutes === 1440 ? "1 day" : minutes / 1440 + " days");
    if (minutes % 60 === 0) return "covers " + (minutes === 60 ? "1 hour" : minutes / 60 + " hours");
    return "covers " + (minutes === 1 ? "1 minute" : minutes + " minutes");
  }
  return d && d.bucket === "per-collection" ? "is one collection" : null;
}

/**
 * The sentence a chart owes its reader about what a point is (#6359), said from the read's own width fields in the page's
 * words. The payload's `aggregate_note` is written for an MCP caller (it names `peak_*` fields and says to pass a
 * `bucket_minutes`), so no panel prints it. `kind` is what the PANEL's read returns, never read from the note's text:
 * "summed" for counts, waits and I/O (a bucket the window cuts short holds smaller counts; rates are true rates over the
 * time it covers), "level" for CPU, memory, tempdb and other gauges (each point averages the samples in its bucket), or
 * omitted when only the width is worth saying. One collection per point needs neither fact. Null when the read states no width.
 */
export function chartNoteText(data, kind) {
  const width = bucketWidthText(data);
  if (!width) return null;
  if (width === "is one collection") return "Each point is one collection.";
  const sentence = (kind === "level" ? "Each point " + width + " and is the average of the samples in it." : "Each point " + width + ".") +
    (kind === "summed" ? " A bucket the window cuts short holds smaller counts; rates are true rates over the time the bucket covers." : "");
  return data.source === "hourly" ? sentence + " " + HOURLY_TRAIL_TEXT : sentence;
}

/**
 * What a read served from the hourly rollup owes its reader: the series trails the clock by up to two hours, because the
 * current hour is never materialized and the one before it lands on the next refresh (DurationTrendRouting, #6359). Said when
 * the read's own `source` field is "hourly", never from the note's text; without it the empty right edge reads as missing data.
 */
const HOURLY_TRAIL_TEXT = "The newest hour or two is not in this chart yet: hourly totals are added after each hour ends.";

/** The note line under a chart for a trend read (see chartNoteText), or null when the read states no width. */
export function chartNote(data, kind) {
  const text = chartNoteText(data, kind);
  return text ? el("div", { class: "mp-metric-note", text }) : null;
}

/**
 * Rewrites every UTC instant inside a sentence (a server message, a status line, a note, an alert body) into the grids'
 * local-time format (#5644 W20): "stored at 2026-10-09T14:37:35.0158630", "since 2026-10-12T19:18:38Z", "until
 * 2026-10-11 12:48:52Z" (the alert text's space-separated form) and "2026-10-12 23:13:00 UTC" printed raw UTC beside grids
 * in local time. The separator is a "T" or a space and the fraction is 0-7 digits. A "T" stamp with no zone is UTC, as
 * parseUtc reads it everywhere; a space-separated one is rewritten ONLY with a "Z" or " UTC" after it, because the same
 * shape without one is a monitored server's own wall clock ("2026-09-27 21:32:43 (server clock)") and shifting it would
 * state the wrong hour. Text with no such stamp, and a non-string, come back as given.
 */
export function localizeIsoText(text) {
  if (typeof text !== "string") return text;
  return text.replace(/(\d{4}-\d\d-\d\d)([T ])(\d\d:\d\d:\d\d)(?:\.(\d{1,7}))?(Z(?![A-Za-z])| UTC(?![A-Za-z])|[+-]\d\d:?\d\d)?/g, (m, date, sep, time, frac, zone) => {
    if (sep === " " && !zone) return m;
    const iso = date + "T" + time + (frac ? "." + frac.slice(0, 3).padEnd(3, "0") : "") + (zone === " UTC" ? "Z" : zone || "");
    const d = parseUtc(iso);
    return d ? d.toLocaleString() : m;
  });
}

/**
 * An alert's stored body text in the grids' local-time format (#5644 W20, Alert History and Triage). The instants the alert
 * builders write ("Incident Since: 2026-10-12 23:13:00Z", "Failed At: 2026-10-12 07:00:00Z", and the Started, Snapshot, From,
 * To and Window lines) go through localizeIsoText. Only those labelled lines do: the same body carries "Blocked Query", "Query"
 * and "Victim SQL" lines, and a literal such as '2026-01-01 00:00:00Z' inside a statement is data. The sample-age line ("Newest blocked process sample: 23:12:45 UTC, 5m 20s old") carries a time of day
 * with no date, which cannot be shifted safely alone, so it takes its date from the row's own `newest_sample.instant_utc`
 * (the carrier the engine persisted when the alert fired) and only when that instant's UTC time of day IS the one written
 * on the line. The stored and emailed text is not changed (the same line feeds email, webhooks and Lite's windows); only
 * what this page shows is. A copy of the text for the clipboard or CSV keeps the stored form.
 */
export function localizeAlertDetail(text, sample) {
  if (typeof text !== "string") return text;
  /* #6649: the collection-history audit's sentence says "N source(s)"; the count is known, so the word agrees with it. The audit's
     body (CollectionHistoryAudit.Render) is ONE line of ". "-joined clauses, so the match is the sentence, up to its ". " or the
     end of the text, never the line: the clauses after it (the source names, the ranges, the "Not read" list) print as written. */
  let out = text.replace(/Retained history for .*?(?:\.(?=\s)|$)/gs, (sentence) => plainText(sentence));
  out = out.replace(/^(\s*(?:Incident Since|Started|Failed At|Snapshot|From|To|Window): )(.*)$/gm,
    (m, lead, value) => lead + localizeIsoText(value));
  const at = sample && typeof sample.instant_utc === "string" ? parseUtc(sample.instant_utc) : null;
  if (at) {
    const two = (n) => String(n).padStart(2, "0");
    const clock = two(at.getUTCHours()) + ":" + two(at.getUTCMinutes()) + ":" + two(at.getUTCSeconds());
    out = out.replace(/^(\s*(?:Newest .+? sample|.+? reading)): (\d\d:\d\d:\d\d) UTC(?=, .* old\s*$)/gm,
      (m, lead, time) => (time === clock ? lead + ": " + at.toLocaleString() : m));
  }
  return out;
}

/**
 * The two collectors the blocking reads draw from (#5244): the blocked-process-report XE session when it holds a row in the window
 * for the chosen databases, and the DMV snapshot only when it holds none. The MCP answers name the one that answered in `source`;
 * these are the same two tags.
 */
export const BLOCKING_SOURCES = ["blocked-process-report", "DMV snapshot"];

/**
 * Which collector answered a blocking read, as a line above the chart (#5244). A filter of [A] can draw A's DMV-only event that
 * [A, B] drops, and nothing on the chart said which collector it was reading. `desc.sourceKey` names the answer's field (`source`
 * on get_blocking_trend and get_blocking_stats). Null when the descriptor names no field, when the answer carries no source (no
 * rows: the empty answer and the zero-row answer both leave it null), or when the value is not one of the two tags. The tag is
 * text, never markup (R4).
 */
export function sourceStrip(data, desc) {
  if (!desc || !desc.sourceKey) return null;
  const tag = getPath(data, desc.sourceKey);
  return BLOCKING_SOURCES.includes(tag) ? el("div", { class: "strip source", role: "status" }, ["Source: " + tag]) : null;
}
/**
 * Render a read error, degrading the "window too wide" case to a notice (#2780). A range wider than a read can
 * serve comes back as a raw `hours_back value 'N' exceeds maximum of M hours (D days)...` validation string
 * (McpHelpers.ValidateHoursBack); that is a range choice, not a fault, so it becomes a status notice naming the
 * widest window the read takes rather than a red error carrying the API's own wording. Every other message stays an
 * error. SHARED by every read-error site — the descriptor loader AND the hand-built server-tab composites — so
 * a tab cannot show a friendly notice on one panel and the raw string on its neighbour.
 */
export function readErrorStrip(message) {
  const hours = keptHoursOf(message);
  if (hours != null) {
    return noticeStrip(readLimitText(hours) + ". Pick a shorter range.");
  }
  return errorStrip(message);
}

/* The M of a "window too wide" refusal (`... exceeds maximum of M hours ...`), or null for any other message.
   A `top` refusal (`exceeds maximum of 1000.`) carries no " hours" and so is never one. */
function keptHoursOf(message) {
  const m = /exceeds maximum of (\d+) hours/.exec(message || "");
  return m ? Number(m[1]) : null;
}

export function daysText(hours) {
  const days = Math.round(hours / 24);
  return days + " day" + (days === 1 ? "" : "s");
}

/* The read's own limit, not the store's history: most tables keep far longer than the 7 days most reads take. */
function readLimitText(hours) {
  return "This view reads at most " + hours + " hours (" + daysText(hours) + ") at a time";
}

/**
 * Run a read, and when it refuses the page's window as wider than it takes, ask it again ONCE for the widest window
 * it does take. Most reads take at most 7 days. The server page's Range stops there, but a Custom View's
 * read panel stores its own hours and can still ask for more: before this, such a panel showed only
 * readErrorStrip's "pick a shorter range" notice and no data. Now it shows the last M hours with keptWindowStrip's
 * notice saying so.
 *
 * `fetchWith(params)` is the read itself (readTool, or apiGet over a raw path), so the descriptor loader and the
 * hand-built server-tab composites share this one rule. The retry happens only for the window refusal and only
 * when `params.hours` asked for more than M, so a read that accepts the window makes one call, a second refusal
 * is never retried again, and every other error comes back unchanged. A successful retry carries
 * `keptHours: M`: the caller shows the notice and draws its chart axis over M hours, not the asked window.
 */
export async function readWithinKeptHistory(fetchWith, params) {
  const res = await fetchWith(params);
  if (res.kind !== "error") return res;
  const kept = keptHoursOf(res.message);
  const asked = Number(params && params.hours);
  if (kept == null || !(kept >= 1 && asked > kept)) return res;
  /* The narrowed ask remembers the hours it replaces (NARROWED_FROM, a symbol, so it never reaches the query string):
     a custom range on the server page carries through the retry instead of the read falling back to "ending now". */
  const retry = await fetchWith({ ...params, hours: kept, [NARROWED_FROM]: asked });
  return retry.kind === "data" || retry.kind === "empty" ? { ...retry, keptHours: kept } : retry;
}

/** readWithinKeptHistory over a read-only tool by its MCP name. `signal`: see apiGet (#4191). */
export function readToolWithinKeptHistory(tool, params, signal) {
  return readWithinKeptHistory((p) => readTool(tool, p, signal), params);
}

/** The notice for a read readWithinKeptHistory narrowed to the widest window it takes, or null for any other result. */
export function keptWindowStrip(res) {
  if (res && !res.keptHours && res.presetHours) {
    return noticeStrip("This panel shows the last " + res.presetHours + " hours, not the custom range: its read takes no end time.");
  }
  if (!res || !res.keptHours) return null;
  if (res.keptCustom) {
    return noticeStrip(readLimitText(res.keptHours) + ", so it shows the part of the custom range the store still holds.");
  }
  return noticeStrip(readLimitText(res.keptHours) + ", so it shows the last " + daysText(res.keptHours) + ".");
}

/**
 * #6654: the strip over a grid that names `databases_left_out_note` as its `noteKey` (or in `moreNoteKeys`), drawn on an EMPTY answer too. The
 * populated path already draws a named note; the empty path draws only the read's message, and "no regressions" over a read that left
 * out a dropped database is not the same statement as "nothing regressed". Null for any other panel, so a note that is not about
 * omitted databases is not pulled onto an empty answer by this.
 */
export function leftOutStrip(data, desc) {
  if (!desc || !data) return null;
  const key = "databases_left_out_note";
  if (desc.noteKey !== key && !(desc.moreNoteKeys || []).includes(key)) return null;
  const note = data[key];
  return typeof note === "string" && note.trim() ? noticeStrip(note) : null;
}

/**
 * The notice for a grid whose table starts covering the server after the window does (#4966): the response says so with
 * `window_truncated: true` and a `truncation_note` naming where the data starts (in the browser's zone by the time a
 * read reaches here: readTool composes it with windowNoteText). Null for any other response,
 * which is every window the table covered, a quiet start included. `desc` is the panel's descriptor: a grid or a stat
 * tile draws the note, a chart does not (its time axis already spans the asked range and shows the empty span), and a grid that names
 * `truncation_note` as its own note (the Queries tab's grids, #4231) draws it there, not twice. A stat is safe to draw: a tile over a
 * snapshot read that takes no `hours` never gets the fields from the server. A panel over the NEWEST snapshot of a read that also
 * serves a window (the `grants` half of the memory reads, Automatic Tuning) sets `windowNote: false`, because its rows are a moment,
 * not the window. A read that measures its own floor in a nested block (the Query Store clutter read's `window`) names it in
 * `floorKey`, and the fields are read from there instead of from the top level. The note is text, never markup (R4).
 */
export function windowFloorStrip(data, desc) {
  if (!desc || desc.windowNote === false) return null;
  const chart = desc.viz === "line";
  if (desc.viz !== "table" && desc.viz !== "stat" && !chart) return null;
  if (desc.noteKey === "truncation_note" || (desc.moreNoteKeys || []).includes("truncation_note")) return null;
  const source = desc.floorKey ? getPath(data, desc.floorKey) : data;
  if (source && source.window_truncated === true) {
    const note = source.truncation_note;
    if (typeof note !== "string" || !note.trim()) return null;
    if (!chart) return noticeStrip(note);
    /* A chart draws only the row-capped note (#5644 W12): its time axis already shows the empty span of a data start that
       falls after the window's start, but a chart built from the newest N rows plots nothing before its oldest row while
       the axis and the title claim the whole window. That note names the oldest row shown (oldest_shown_utc). */
    if (typeof source.oldest_shown_utc !== "string") return null;
    return noticeStrip(note.replace("this grid shows", "this chart shows").replace("The grid covers", "The chart covers"));
  }
  const capped = cappedReadNote(data, desc);
  return capped ? noticeStrip(capped.text) : null;
}

/*
 * The partial-window facts of a read that stopped at its row limit (#5644 W12), or null. Two shapes carry them: the server's
 * window-floor fields (`window_truncated` with `oldest_shown_utc`: the reads WebDataStartNote lists, rewritten by readTool), and a
 * newest-first page that says it was cut (`truncated: true` with `oldest_returned_collection_time` and an `*_desc` order: the
 * active-query page, whose cap of 50 rows covers about an hour of a "last 24 hours" panel). A cut that names no time (a list
 * ranked by something other than time) is not a partial window, so it gets nothing. `{ text, oldest }`: the sentence for a
 * strip above the panel, and the page's oldest instant.
 */
export function cappedReadNote(data, desc) {
  if (!data || typeof data !== "object" || Array.isArray(data)) return null;
  if (desc && (desc.windowNote === false || (desc.viz !== "table" && desc.viz !== "line"))) return null;
  if (desc && (desc.noteKey === "truncation_note" || (desc.moreNoteKeys || []).includes("truncation_note"))) return null;
  if (data.window_truncated === true) return null;
  if (data.truncated !== true) return null;
  const oldest = data.oldest_returned_collection_time;
  if (typeof oldest !== "string" || !parseUtc(oldest)) return null;
  if (!/_desc$|newest/.test(String(data.order == null ? "" : data.order))) return null;
  const noun = desc && desc.viz === "line" ? "chart" : "grid";
  const shown = Number(data.snapshots_returned);
  const total = Number(data.total_snapshots);
  const counts = shown > 0 && total > shown ? shown.toLocaleString() + " of " + total.toLocaleString() + " snapshots" : "rows";
  return {
    text: "partial window: this " + noun + " shows only the newest " + counts + ", back to " + localTime(oldest) + ", because it stops at its row limit.",
    oldest,
  };
}

/**
 * The subtitle a row-capped panel gets in place of its window label (#5644 W12): "last 24 hours" over a page that reaches back
 * one hour claimed a window the data does not cover. Null when the panel is not a row-capped one (the label stays).
 */
export function partialWindowSubtitle(data, desc) {
  if (desc && desc.windowNote === false) return null;
  if (data && typeof data === "object" && data.window_truncated === true) {
    if (typeof data.oldest_shown_utc !== "string" || !parseUtc(data.oldest_shown_utc)) return null;
    if (desc && desc.viz !== "table" && desc.viz !== "line") return null;
    return "newest rows only, back to " + localTime(data.oldest_shown_utc);
  }
  const capped = cappedReadNote(data, desc);
  return capped ? "newest rows only, back to " + localTime(capped.oldest) : null;
}

/** Replaces the window label in a panel's heading (its `.panel-sub` span) when the read was row-capped. A no-op otherwise. */
export function markPartialSubtitle(panel, data, desc) {
  const text = partialWindowSubtitle(data, desc);
  if (!text || !panel) return;
  const find = (node) => {
    for (const c of Array.from(node.children || [])) {
      const k = c.className || (typeof c.getAttribute === "function" ? c.getAttribute("class") : null);
      if (k === "panel-sub") return c;
      const deeper = find(c);
      if (deeper) return deeper;
    }
    return null;
  };
  const head = Array.from(panel.children || [])[0];
  const sub = head ? find(head) : null;
  if (sub) sub.textContent = " " + text;
}

/**
 * A read's window note (`truncation_note`) in the page's own clock (#4966). The server writes the sentence with its
 * instants in UTC, which is all an MCP client can use, but every time this page prints is in the browser's zone
 * (localTime): a note above a grid that said "2026-01-02 00:00 UTC" mixed two clocks. So the answer also carries the
 * instants as fields (`data_start_utc` or `oldest_shown_utc`, with `window_start_utc` and `window_end_utc`), and this
 * composes the sentence again from them with localTime, the way keptWindowStrip composes its strip from `keptHours`.
 * The Queries tab's notes (#4231) carry no such fields: the Query Store note names an instant only as the
 * `effective_start` the answer already carries, so that instant is shown in the browser's zone inside the sentence.
 * A field that is missing or unreadable, or a note that names no instant, draws the sentence as sent. Null when the
 * answer carries no note.
 */
export function windowNoteText(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) return null;
  const sent = data.truncation_note;
  if (typeof sent !== "string" || !sent.trim()) return null;
  if (data.window_truncated !== true) return sent;

  const at = (value) => (typeof value === "string" && parseUtc(value) ? localTime(value) : null);
  const start = at(data.window_start_utc);
  const end = at(data.window_end_utc);
  const oldest = at(data.oldest_shown_utc);
  const first = at(data.data_start_utc);
  if (start && end && oldest) {
    return (
      "partial window: this grid shows only the newest rows, back to " + oldest + ", because it stops at its row limit. " +
      "The window started at " + start + ". The grid covers " + oldest + " to " + end + "."
    );
  }
  if (start && end && first) {
    return (
      "partial window: this panel's data starts at " + first + ", after the window's start at " + start + ". " +
      "The panel covers " + first + " to " + end + "."
    );
  }

  const effective = data.effective_start;
  if (typeof effective === "string" && effective && parseUtc(effective) && sent.includes(effective)) {
    return sent.split(effective).join(localTime(effective));
  }
  return sent;
}

/* The retention notice a Top Queries or Top Procedures answer carries for the Reads ranking (#5226), or null. Reads are ranked from raw, or
   (#5329) from the io hourly rollup when that reaches the window's start or reaches further back than raw (the stitched hourly rollups keep no logical reads). When raw
   answers, over a window past what raw keeps the answer says so in `retention_notice`: the raw
   route's partial-window sentence, with the store's measured raw reach. The io route carries none; its partial window is `truncation_note`. It says what the answer's `truncation_note` says (the older part of
   the window was not read), and it carries the figures. */
function retentionNoticeOf(data) {
  if (!data || typeof data !== "object" || Array.isArray(data)) return null;
  const sent = data.retention_notice;
  return typeof sent === "string" && sent.trim() ? sent : null;
}

/* A read's answer with its window note composed in the browser's zone (windowNoteText), so every strip that draws
   `truncation_note` (the grids' floor note, the Queries tab's noteKey notes, the hand-built composites) shows one
   clock. An empty envelope carries the note too, in its `data` (#4966: a grid that looked and found nothing says where its
   data starts, so a new server's empty week does not read as a quiet one). Anything but a data or empty answer, and an answer
   whose note is already as it should be, comes back as it is.

   #5299: an answer that carries a `retention_notice` (the Reads ranking past raw's reach) has ONE window note, and it is that sentence:
   it stands in the `truncation_note`'s place, so the grid draws it once instead of two near-identical lines (the floor sentence and the
   retention notice both said the older part of the window was not read). Whoever draws `truncation_note` draws it, as text. */
function localizeWindowNote(res) {
  if (!res || (res.kind !== "data" && res.kind !== "empty")) return res;
  const note = retentionNoticeOf(res.data) ?? windowNoteText(res.data);
  return note === null || note === res.data.truncation_note ? res : { ...res, data: { ...res.data, truncation_note: note } };
}

export function loadingStrip(label) {
  return el("div", { class: "strip loading" }, [label || "Loading…"]);
}

/* ─────────────────────────── time (UTC -> local) ─────────────────────────── */

/** Parse a naive-UTC ISO string (no zone) as UTC; pass through strings that already carry a zone. */
export function parseUtc(s) {
  if (!s || typeof s !== "string") return null;
  const hasZone = /[zZ]$|[+\-]\d\d:?\d\d$/.test(s);
  const d = new Date(hasZone ? s : s + "Z");
  return isNaN(d.getTime()) ? null : d;
}

/** The rows of a read's reply under `key`, or the reply itself when it is an array; [] for anything else. One definition for the pages that unwrap a tool's reply (alerts, investigations). */
export function rowsUnder(data, key) {
  if (Array.isArray(data)) return data;
  if (data && Array.isArray(data[key])) return data[key];
  return [];
}

/**
 * #6831: an empty answer's sentence for the page. The shared "cut window" sentence says the store's data starts at `effective_start (see
 * hints)`, which points an MCP caller at a field the page never shows, so the time itself goes in its place, in the page's own clock, from the
 * `effective_start` the same answer carries under `hints`. With no readable instant the clause says the data starts partway into the window.
 */
export function emptyMessageText(body) {
  const said = String(body.message);
  const at = body.hints && typeof body.hints.effective_start === "string" ? body.hints.effective_start : null;
  if (!(at && parseUtc(at))) return plainText(said);
  return plainText(said.replace(/\beffective_start \(see hints\)/g, localTime(at)));
}

/** Full localized date-time, or an em dash for null/unparseable. */
export function localTime(s) {
  const d = parseUtc(s);
  return d ? d.toLocaleString() : "—";
}

/** Time-of-day only (HH:MM local), or an em dash — for compact "last collect" lines where the date is implied. */
export function localClock(s) {
  const d = parseUtc(s);
  return d ? d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" }) : "—";
}

/** Relative time from now: "just now", "2m ago", "3h ago", "5d ago"; older than ~30d or a future instant falls
 *  back to the full local time. Null/unparseable -> em dash. */
export function relTime(s) {
  const d = parseUtc(s);
  if (!d) return "—";
  const diffMs = Date.now() - d.getTime();
  if (diffMs < 0) return d.toLocaleString();
  const sec = Math.floor(diffMs / 1000);
  if (sec < 45) return "just now";
  const min = Math.floor(sec / 60);
  if (min < 60) return min + "m ago";
  const hr = Math.floor(min / 60);
  if (hr < 24) return hr + "h ago";
  const day = Math.floor(hr / 24);
  if (day < 30) return day + "d ago";
  return d.toLocaleString();
}

/** Compact axis label for a Date: HH:MM, widening to include the calendar date when `withDate` is set (the
 *  chart passes true whenever the domain's start and end fall on different calendar days). `withSeconds` adds the
 *  seconds, for a window too short to give each tick its own minute (#5586). `withZone` adds the zone's short name ("EDT"),
 *  for a window across a clock change, where one wall-clock hour happens twice (#5586). */
export function axisTime(date, withDate, withSeconds = false, withZone = false) {
  if (!date) return "";
  const time = withSeconds ? { hour: "2-digit", minute: "2-digit", second: "2-digit" } : { hour: "2-digit", minute: "2-digit" };
  if (withZone) time.timeZoneName = "short";
  if (withDate) {
    return date.toLocaleString([], { month: "numeric", day: "numeric", ...time });
  }
  return date.toLocaleTimeString([], time);
}

/**
 * The x-axis DOMAIN window for a "last N hours" trend chart (#2802): [now - hours, now] as UTC-epoch ms, for
 * renderLineChart's windowStart/windowEnd. windowEnd is the client's render-time now, NOT the last data point —
 * the trend reads echo only hours_back (the requested width), never a window-end/as-of timestamp, and the SPA
 * never sends as_of, so the server anchors the window at ITS request-time now, which the render-time now matches
 * to within request latency (sub-second to a couple of seconds; negligible at the charts' minute-granularity
 * tick labels). Anchoring to the last data point instead would slide an old sparse burst to the right edge and
 * read as current — the #2802 bug. Returns null for a missing/non-positive hours so the chart falls back to its
 * data-extent domain unchanged (byte-for-byte the pre-#2802 behavior). `Date.now()` is a UTC epoch, directly
 * comparable to the parseUtc'd data times.
 */
export function windowFromHours(hours) {
  const h = Number(hours);
  if (!isFinite(h) || h < 1) return null;
  /* A custom range's own reads span the exact pair the reader picked, not the whole hours they were fetched over. */
  if (liveRange() && h === activeRange.hours) return { windowStart: activeRange.startMs, windowEnd: activeRange.endMs };
  /* A read narrowed to the kept history draws the part of the custom range that history holds. */
  if (liveRange() && h === activeRange.narrowedTo) {
    return { windowStart: Math.max(activeRange.startMs, activeRange.endMs - h * 3600000), windowEnd: activeRange.endMs };
  }
  const windowEnd = Date.now();
  return { windowStart: windowEnd - h * 3600000, windowEnd };
}

/**
 * #6359: the window a panel draws when it reads the WHOLE hours the grid ranked over (a row's History; see readTool's
 * `wholeWindow`): [end - hours, end], where end is the custom range's end (or now), not the exact pair the reader picked.
 * Without a custom range it is windowFromHours's window.
 */
export function wholeWindowFromHours(hours) {
  const h = Number(hours);
  if (!isFinite(h) || h < 1) return null;
  if (liveRange() && h === activeRange.hours) {
    const windowEnd = activeRange.asOf ? activeRange.endMs : Date.now();
    return { windowStart: windowEnd - h * 3600000, windowEnd };
  }
  return windowFromHours(hours);
}

/**
 * #6359: the sentence a History panel owes when the window it reads is wider than the range the reader picked, or null when
 * they are the same (a preset, or a range of whole hours). The grids rank over whole hours back from the end of the range, so
 * "Past 5 minutes" ranks over an hour; History reads that same window (readTool's `wholeWindow`), and says so here.
 */
export function widenedWindowNote(hours) {
  const range = liveRange();
  const h = Number(hours);
  if (!range || !isFinite(h) || h < 1 || h !== range.hours) return null;
  if (h * 3600000 - (range.endMs - range.startMs) < 60000) return null;
  const end = range.asOf ? range.endMs : Date.now();
  const at = (ms) => localTime(new Date(ms).toISOString());
  return (
    "This history covers the window the grid ranked over, " + at(end - h * 3600000) + " to " + (range.asOf ? at(end) : "now") +
    ", which is wider than the range you picked (" + at(range.startMs) + " to " + (range.asOf ? at(range.endMs) : "now") + ")."
  );
}

/* ─────────────────────────── value formatters ─────────────────────────── */

/* Axis tick labels (#6158). A chart's tick step is a nice 1, 2 or 5 times a power of ten, so on a small range it is 0.2 or
   0.5, and a formatter that rounds to whole numbers printed one label several times over ("1 ms, 1 ms, 0 ms, 0 ms").
   The chart passes each formatter { step } as a second argument; a formatter that gets one prints enough decimals that two
   ticks one step apart never read the same. Called without it (a tooltip, a table cell) a formatter reads as it always did.
   The second argument is checked to be that object, so `array.map(fmtMs)` (which passes an index) is unaffected. */
export function tickDecimals(step) {
  const s = Number(step);
  if (!isFinite(s) || s <= 0 || s >= 1) return 0;
  return Math.min(8, Math.ceil(-Math.log10(s) - 1e-9));
}
function stepOf(o) {
  return o && typeof o === "object" && isFinite(o.step) && o.step > 0 ? Number(o.step) : null;
}
/* The decimals a tick label needs: the step's own, in the unit the label is printed in (`per` source units to one shown unit). */
function placesFor(o, min = 0, per = 1) {
  const step = stepOf(o);
  return Math.max(min, step == null ? 0 : tickDecimals(step / per));
}
function fixed(n, places) {
  return n.toLocaleString(undefined, { minimumFractionDigits: places, maximumFractionDigits: places });
}
const stepArg = (o) => (stepOf(o) == null ? null : { step: stepOf(o) });

export function fmtInt(v, o) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  const places = placesFor(o);
  return places > 0 ? fixed(n, places) : n.toLocaleString(undefined, { maximumFractionDigits: 0 });
}
export function fmtNum(v, d = 1) {
  if (v == null) return "—";
  const n = Number(v);
  return isFinite(n) ? n.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d }) : "—";
}
/* A per-second rate. From 1 up it reads as fmtNum does without padding (66, 1,234.57). Below 1 it keeps two
   significant digits (0.22, 0.0033, 0.000012), so a real rate never reads as 0: one deadlock in a 300 s collection
   is 0.0033 a second, and one count over a day-wide bucket is 0.000012. Only a true 0 reads 0. */
export function fmtRate(v) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  return Math.abs(n) >= 1 || n === 0
    ? n.toLocaleString(undefined, { maximumFractionDigits: 2 })
    : n.toLocaleString(undefined, { maximumSignificantDigits: 2 });
}
export function fmtPct(v, o) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  const places = placesFor(o);
  return places > 0 ? fixed(n, places) + "%" : Math.round(n) + "%";
}
export function fmtMs(v, o) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  const abs = Math.abs(n);
  if (abs < 1000) return fmtInt(n, stepArg(o)) + " ms";
  if (abs < 60000) return fmtNum(n / 1000, placesFor(o, 1, 1000)) + " s";
  if (abs < 3600000) return fmtNum(n / 60000, placesFor(o, 1, 60000)) + " min";
  return fmtNum(n / 3600000, placesFor(o, 1, 3600000)) + " h";
}
export function fmtMb(v, o) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  return n >= 1024 ? fmtNum(n / 1024, placesFor(o, 1, 1024)) + " GB" : fmtInt(n, stepArg(o)) + " MB";
}
/* A size in `unit` (kb, mb, gb or tb, any case) stepped up to the unit that keeps it under 1,024, written "MB", "GB": the
   same rule fmtMb applies, so 80,000 MB reads "78.1 GB" and never a cut-off "80,000.0 mb". Null when `unit` is not a size. */
const SIZE_UNITS = ["KB", "MB", "GB", "TB"];
export function fmtSize(v, unit, o) {
  const at = SIZE_UNITS.indexOf(String(unit == null ? "" : unit).toUpperCase());
  if (at < 0) return null;
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  let value = n;
  let i = at;
  let per = 1;
  while (Math.abs(value) >= 1024 && i < SIZE_UNITS.length - 1) {
    value /= 1024;
    per *= 1024;
    i++;
  }
  /* A tooltip or table cell keeps one decimal, as every composed value does; an axis tick prints as many as its step needs. */
  const places = stepOf(o) == null ? 1 : placesFor(o, i > at ? 1 : 0, per);
  return fixed(value, places) + " " + SIZE_UNITS[i];
}
/* A byte count in human units (B, KB, MB, GB; 1024-based). A null is the dash, never 0 bytes: 0 is a real answer. The table sorts
   a column on the raw number, so "900 KB" sorts below "2.0 MB" whatever the text says. */
export function fmtBytes(v) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  const abs = Math.abs(n);
  if (abs < 1024) return fmtInt(n) + " B";
  if (abs < 1024 * 1024) return fmtNum(n / 1024, 1) + " KB";
  if (abs < 1024 * 1024 * 1024) return fmtNum(n / (1024 * 1024), 1) + " MB";
  return fmtNum(n / (1024 * 1024 * 1024), 1) + " GB";
}
export function fmtText(v) {
  return v == null || v === "" ? "—" : String(v);
}
export function fmtBool(v) {
  return v === true ? "yes" : v === false ? "no" : "—";
}

/** Named formatters a panel descriptor can reference (the #1563-friendly, string-keyed registry). */
export const FORMATTERS = {
  int: fmtInt,
  num1: (v, o) => fmtNum(v, placesFor(o, 1)),
  num2: (v, o) => fmtNum(v, placesFor(o, 2)),
  rate: fmtRate,
  pct: fmtPct,
  ms: fmtMs,
  mb: fmtMb,
  bytes: fmtBytes,
  time: localTime,
  reltime: relTime,
  bool: fmtBool,
  text: fmtText,
};

export function applyFormat(name, value, opts) {
  const f = (name && FORMATTERS[name]) || FORMATTERS.text;
  return f(value, opts);
}

/* ─────────────────────────── band / severity classes ─────────────────────────── */

/* The band vocabulary on the wire is ONE spelling — the PascalCase enum token (HealthSeverity / FleetHealthBand /
   DailyHealthBand: Healthy, Warning, Critical, Unknown, Offline, NoData) — and the CSS classes below are keyed on it.
   #3653 (one vocabulary) made every MCP band spell it that way; this map is the belt to that brace: a class
   builder that compares strings case-insensitively, so a band that arrives as "warning" or "WARNING" from a
   surface the census does not sweep still colours, instead of silently producing a class no rule matches.
   Anything outside the vocabulary passes through unchanged (a caller that maps "No Data" itself keeps working). */
const CANONICAL_BANDS = ["Healthy", "Warning", "Critical", "Unknown", "Offline", "NoData"];
export function canonicalBand(band) {
  if (band == null || band === "") return "Unknown";
  const wanted = String(band).toLowerCase();
  return CANONICAL_BANDS.find((b) => b.toLowerCase() === wanted) || String(band);
}
/** CSS class for a fleet band ("Healthy"/"Warning"/"Critical"/"Offline") — colors live in CSS. */
export function bandClass(band) {
  return "band-" + canonicalBand(band);
}
/** CSS class for a per-metric severity ("Unknown"/"Healthy"/"Warning"/"Critical"). */
export function sevClass(sev) {
  return "sev-" + canonicalBand(sev);
}

/* ─────────────────────────── object access ─────────────────────────── */

/** Read a possibly-dotted path out of an object; undefined-safe. */
export function getPath(obj, path) {
  if (obj == null || !path) return undefined;
  if (!path.includes(".")) return obj[path];
  return path.split(".").reduce((o, k) => (o == null ? undefined : o[k]), obj);
}

/* ─────────────────────────── API fetch ─────────────────────────── */

/** Build a query string from a params object, skipping null/undefined/empty values. An array is written as repeated keys
 *  (`database_name=A&database_name=B`), one encodeURIComponent per item, and as nothing at all when it is empty (#5245):
 *  a name that holds a comma, a bracket or a space is one key holding exactly that name, never several. Its items are
 *  written as given, with no trim and no dropping, so a value the server cannot use is refused there instead of being
 *  read as "all". An array used to serialize as ONE comma-joined value, and no caller passed one before this. */
export function buildQuery(params) {
  if (!params) return "";
  const parts = [];
  for (const [k, v] of Object.entries(params)) {
    if (Array.isArray(v)) {
      for (const item of v) parts.push(encodeURIComponent(k) + "=" + encodeURIComponent(item));
      continue;
    }
    if (v == null || v === "") continue;
    parts.push(encodeURIComponent(k) + "=" + encodeURIComponent(v));
  }
  return parts.length ? "?" + parts.join("&") : "";
}

/* In-flight read counter (#4191, per page since #6793): every apiGet/readTool call counts itself while its fetch is outstanding, so
   the poll loop (app.js refresh()) can tell whether the page it is about to re-render has already settled
   before firing a whole new set of the same reads on top of it. apiSendRead (a read that must travel as a POST,
   the composed-panel run) IS counted, so a slow panel holds the poll off and the refresh back-off measures it.
   apiGetFleet IS counted too (each caller counts its own wait on the shared request), so a render whose reads are
   fleet reads is timed for as long as it ran. apiSend is deliberately NOT counted: a mutation is not a "page read"
   a poll tick should wait out. */
let pageReads = { n: 0 };

/* The page scope (#6793). Reads are counted per page, not globally: a read counts itself on the scope that was current
   when it started and gives the count back on that same scope. beginPageScope() runs when the user goes to another page.
   It aborts the reads of the page being left through the hooks the page modules registered with onPageLeave, and it opens
   a fresh count, so a read of the page left (one that has no signal, or one that has not yet ended after its abort) never
   holds the next page's poll, Refresh button, tab-return refresh or render-time back-off. A poll or Refresh re-render of
   the same page does not call it: that page aborts its own older load, as before. One exception: readWithinKeptHistory's retry
   starts after an await, so it counts on the scope current at the retry. A read with no signal that is refused for its window
   while the user leaves holds the new page for that one round trip. */
const pageLeaveHooks = [];

/** Register what a page module aborts when the user goes to another page (its module-level AbortControllers). */
export function onPageLeave(fn) {
  pageLeaveHooks.push(fn);
}

/** Leave the current page: abort its reads and start a fresh read count for the next one. See the scope comment above. */
export function beginPageScope() {
  for (const fn of pageLeaveHooks) fn();
  pageReads = { n: 0 };
}

/* How many red error strips the page has drawn since it loaded (errorStrip counts each one). The page scheduler compares it before
   and after a render, so the footer's "Updated" time moves only when the render ended without a red strip. It is counted where
   the strip is drawn, not where a read fails: a read refused for a window wider than it takes (readWithinKeptHistory) is asked
   again for the widest window, and a retry that answers draws data and a notice, so that first refusal is no fault of the page.
   readErrorStrip turns the same refusal into a notice and does not call errorStrip. Aborted reads and a signed-out session
   draw no strip and never count. */
let readErrors = 0;

/** The running count of red error strips drawn — see the counter comment above. */
export function readErrorCount() {
  return readErrors;
}

/** True while at least one apiGet/readTool call is outstanding — see the counter comment above. */
export function hasInFlightReads() {
  return pageReads.n > 0;
}

/**
 * GET a same-origin API path and classify the response into one of four kinds, mirroring the service's
 * response shapes (DarlingWebEndpoints):
 *   { kind: "data",  data }                 — a data object/array (HTTP 200 JSON passthrough)
 *   { kind: "empty", status, message, ... } — the {status,message[,hints]} empty envelope (HTTP 200)
 *   { kind: "error", message, status }      — { "error": ... } (HTTP 400/500) or a transport failure
 *   { kind: "auth",  message, login }       — the session is gone (#4187); see classifyResponse
 * `signal` (#4191) is an optional AbortSignal for a superseded render's reads — see panels.js's setPanelSignal
 * and server.js's redrawPanels, which own creating and aborting it. A caller with no render to supersede (the
 * sidebar, the view list) simply omits it, exactly as before.
 */
export async function apiGet(path, signal) {
  const scope = pageReads;
  scope.n++;
  try {
    let resp;
    try {
      resp = await fetch(path, { headers: { Accept: "application/json" }, signal });
    } catch (e) {
      if (e && e.name === "AbortError") return { kind: "aborted" };
      return { kind: "error", message: "Network error: " + (e && e.message ? e.message : String(e)) };
    }
    return await classifyResponse(resp);
  } finally {
    scope.n--;
  }
}

/* The /api/read/ requests in flight right now, by path - see apiGetJoined. */
const joinedReads = new Map();

/**
 * apiGet for a tool read, with ONE request shared by every caller that asks for the same path while it is in flight
 * (#5489, the apiGetFleet idea for the tool reads). The Wait Stats tab asked for get_spinlock_stats twice in one load:
 * the Spinlock Stats grid, and the Spinlock Trend picker that lists the same top spinlocks (the latch pair did the
 * same). At 7 days on a large store that read took 3.0 s and 5.7 s, so the page paid for it twice. The second caller
 * of a load now joins the first's request instead of sending its own.
 *
 * Nothing outlives the response: a caller that starts after it landed sends a fresh request, so no page renders an
 * older answer than it would have. Each caller classifies (so parses) the shared body for itself, so every panel owns
 * the rows it was handed. A caller's `signal` (#4191) cancels only that caller: it gets { kind: "aborted" } at once,
 * and the request itself is cancelled only when no caller is left waiting on it.
 */
export async function apiGetJoined(path, signal) {
  if (signal && signal.aborted) return { kind: "aborted" };
  const scope = pageReads;
  scope.n++;
  let entry = null;
  let onAbort = null;
  try {
    entry = joinedReads.get(path);
    if (!entry) {
      const controller = new AbortController();
      const joined = { controller, waiters: 0, body: null };
      joined.body = fetchBody(path, controller.signal).finally(() => {
        if (joinedReads.get(path) === joined) joinedReads.delete(path);
      });
      joinedReads.set(path, joined);
      entry = joined;
    }

    entry.waiters++;
    const cancelled = new Promise((resolve) => {
      if (!signal) return;
      onAbort = () => resolve(null);
      signal.addEventListener("abort", onAbort, { once: true });
    });
    const shared = await Promise.race([entry.body, cancelled]);
    if (!shared || shared.aborted) return { kind: "aborted" };
    if (shared.transportError) return { kind: "error", message: shared.transportError };
    return await classifyResponse({ ok: shared.ok, status: shared.status, text: async () => shared.raw, retryAfter: shared.retryAfter });
  } finally {
    if (signal && onAbort) signal.removeEventListener("abort", onAbort);
    if (entry) {
      entry.waiters--;
      if (entry.waiters === 0 && joinedReads.get(path) === entry) {
        /* Nobody is waiting: stop the fetch, and let the next caller start a fresh one. */
        joinedReads.delete(path);
        entry.controller.abort();
      }
    }
    scope.n--;
  }
}

/* The /api/fleet request every caller in flight at the same moment shares — see apiGetFleet. */
let fleetRequest = null;

/**
 * GET /api/fleet, classified exactly like apiGet, with ONE request shared by every caller that asks while it is
 * in flight (#3895). The 60s poll re-renders the sidebar and the current page in one synchronous pass, and the
 * sidebar, the fleet page, the server page and a saved view each read the fleet roll-up — the store's widest
 * read — so every visible tab computed the whole overview twice a minute. The second caller of a tick now joins
 * the first's request instead of sending its own.
 *
 * Nothing outlives the response: a caller that starts after it has landed sends a fresh request, exactly as
 * before, so no page renders an older roll-up than it would have — only the duplicate is gone. And each caller
 * classifies (so parses) the shared body for itself, so every page still owns the cards it was handed.
 */
export async function apiGetFleet() {
  const scope = pageReads;
  scope.n++;
  try {
    if (!fleetRequest) {
      fleetRequest = fetchBody("/api/fleet").finally(() => {
        fleetRequest = null;
      });
    }

    const shared = await fleetRequest;
    if (shared.transportError) return { kind: "error", message: shared.transportError };
    return classifyResponse({ ok: shared.ok, status: shared.status, text: async () => shared.raw });
  } finally {
    scope.n--;
  }
}

/** Fetch a path and read its whole body once, for a response several callers classify. A failure comes back as a
    value rather than a rejection, so one lost request cannot surface as an unhandled rejection per caller. */
async function fetchBody(path, signal) {
  try {
    const resp = await fetch(path, { headers: { Accept: "application/json" }, signal });
    return { ok: resp.ok, status: resp.status, raw: await resp.text(), retryAfter: retryAfterOf(resp) };
  } catch (e) {
    if (e && e.name === "AbortError") return { aborted: true };
    return { transportError: "Network error: " + (e && e.message ? e.message : String(e)) };
  }
}

/**
 * Send a MUTATING request (POST / PUT / DELETE) with an optional JSON body, classified exactly like apiGet
 * (#1563 custom-view CRUD). A 204/empty body yields { kind: "data", data: null }; an { "error": ... } body on a
 * non-2xx yields { kind: "error", message, status } — the status is preserved so a caller can branch on it
 * (409 = a duplicate name or a stale optimistic-concurrency version, 415 = a non-JSON write was refused, ...).
 * Every mutation ALWAYS declares Content-Type: application/json — the server rejects a mutation without it (415),
 * which is what forces a CORS preflight on any cross-origin write and thereby kills the simple-request CSRF
 * vector; a bodyless DELETE carries the header too (it sends no body, but must still satisfy that gate).
 */
export async function apiSend(method, path, body) {
  const hasBody = body !== undefined && body !== null;
  let resp;
  try {
    resp = await fetch(path, {
      method,
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: hasBody ? JSON.stringify(body) : undefined,
    });
  } catch (e) {
    return { kind: "error", message: "Network error: " + (e && e.message ? e.message : String(e)) };
  }
  return classifyResponse(resp);
}

/**
 * Send a MUTATING request and hand back what the server said, the HTTP status and the parsed body both, for a caller
 * that branches on the status AND the body's status word (#5240: the Admin page's server edit answers a stale token
 * and a taken address with the same 409 and tells them apart by the word; apiSend drops a non-2xx body, so it cannot).
 * It is the transport Manage Tags' write always had, lifted here with its behaviour unchanged:
 *   { status, body }                    - the answer. `body` is the parsed JSON, or null when the text is not JSON (an
 *                                         empty body included). A 400, 404, 409, 429, 500 and the rest come back this way.
 *   { status: 0, body: null, message }  - no answer at all (a network error); `message` is "Network error: ..."
 *   { status, body, expired: true }     - the session is gone: a 401, or a 2xx whose text is not JSON at all (a sign-in
 *                                         page in front of the API, or an empty body). The shell has been told once
 *                                         (reportSessionExpired, with the house message and "/", never the body's own
 *                                         login), so the caller drops what it has open and shows no success. This is
 *                                         stricter than apiGet, which reads an empty 2xx as data.
 *   { status, body, unexpected: true }  - a 2xx whose text IS JSON but not a JSON object (an array, a string, a number, null).
 *                                         A sign-in page is never valid JSON, so this is not the session going; it is an
 *                                         answer no write of this service gives (#5356). The shell is NOT told: the caller
 *                                         keeps what it has open and says so, and shows no success.
 * Like apiSend it ALWAYS declares Content-Type: application/json, which the server demands of a mutation (a 415
 * otherwise; it is what forces a CORS preflight on a cross-origin write), and it counts as no in-flight read.
 * An undefined body sends none.
 */
export async function apiWrite(method, path, body) {
  let resp;
  try {
    resp = await fetch(path, {
      method,
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    return { status: 0, body: null, message: "Network error: " + (e && e.message ? e.message : String(e)) };
  }
  let parsed = null;
  let isJson = false;
  try {
    parsed = JSON.parse(await resp.text());
    isJson = true;
  } catch {
    parsed = null;
  }
  const succeeded = resp.status >= 200 && resp.status < 300;
  if (resp.status === 401 || (succeeded && !isJson)) {
    reportSessionExpired("Your session has expired. Sign in again.", "/");
    return { status: resp.status, body: parsed, expired: true };
  }
  if (succeeded && (parsed === null || typeof parsed !== "object" || Array.isArray(parsed))) return { status: resp.status, body: parsed, unexpected: true };
  return { status: resp.status, body: parsed };
}

/** #4666: a READ that has to travel as a POST (the composed-panel run, /api/compose/run). Counted in the page's read count
    exactly like apiGet, so the poll's overlap guard (#4191) waits it out and the refresh back-off measures the render
    that contains it. Mutations (saves, deletes, alert validate/test) keep using apiSend, uncounted. */
export async function apiSendRead(method, path, body) {
  const scope = pageReads;
  scope.n++;
  try {
    return await apiSend(method, path, body);
  } finally {
    scope.n--;
  }
}

/* Session-expired takeover (#4187). A module-level one-shot latch: the FIRST read that reports the session is
   gone rewrites the whole shell into a sign-in prompt (app.js registers the one listener that does it) rather
   than leaving every open panel to separately render its own "signed out" guess as an unrelated-looking error.
   One-shot because the only way out is the sign-in link, which reloads the page — nothing here ever un-latches
   it, and a page reload starts every module fresh anyway. */
let sessionExpired = false;
const sessionExpiredListeners = [];

/** True once a read has reported the session is gone — see classifyResponse's 401 / non-JSON-200 arms. */
export function isSessionExpired() {
  return sessionExpired;
}

/** Register a callback for the FIRST detected session expiry. Fired at most once per page load. */
export function onSessionExpired(fn) {
  sessionExpiredListeners.push(fn);
}

export function reportSessionExpired(message, login) {
  if (sessionExpired) return;
  sessionExpired = true;
  for (const fn of sessionExpiredListeners) fn(message, login);
}

/**
 * Classify a completed Response into the same shape apiGet returns (shared by apiGet + apiGetFleet + apiSend):
 * a 401 -> "auth" (#4187: the network-mode auth gate's answer to an /api/* call with no valid session — a
 * service restart rotates the cookie signing key, so an open tab's every read starts failing this way); a
 * non-2xx -> "error", with the message read from an { "error": ... } body (the 500 arm and the bare-string
 * 400 arm) or from the {status:"invalid", message} envelope (#3739: a REFUSAL — a parameter the tool cannot
 * honor, a server name that resolves to nothing — is the envelope itself as a 400 body on /api/read/*, exactly
 * as the mute-rule write routes have always answered invalid; its `message` is the sentence readErrorStrip and
 * the tab error strips render, so the "window too wide" degrade keeps working); the {status, message[, hints]}
 * envelope under 2xx -> "empty" for the four miss words and "error" for status "error" or "invalid" (#3653 Q11:
 * every tool's caught exception is that envelope on the MCP wire; the service maps error to a 500 and invalid to
 * a 400 before either reaches this page, so the 2xx arm below is the belt-and-braces for a body that arrived
 * unmapped — a failure or a refusal must never render as a quiet "nothing here" card); a 200 whose body is NOT
 * JSON -> also "auth" (#4187: before the server could tell an /api/* call apart from a page load, an expired
 * session answered every /api/* call with the 200 HTML login form — this is that shape, kept as a second, belt-
 * and-braces detector in case a future gate answers the same way again); anything else (including a 204/empty
 * body -> data:null) -> "data".
 */
/** The Retry-After header of a response, or null when it has none (or no headers at all). */
function retryAfterOf(resp) {
  return resp && resp.headers && typeof resp.headers.get === "function" ? resp.headers.get("Retry-After") : null;
}

/** Whole seconds from a Retry-After header in its delta-seconds form, or null for none or any other form. */
function retryAfterSecondsOf(header) {
  const seconds = Number(header);
  return header != null && String(header).trim() !== "" && Number.isInteger(seconds) && seconds >= 0 ? seconds : null;
}

/** #6800: when a read was refused busy (HTTP 429), the seconds to wait before trying again (the reply's Retry-After, at least 1). */
export function busyRetryAfter(res) {
  return res && res.kind === "error" && res.status === 429 ? Math.max(1, res.retryAfterSeconds || 5) : null;
}

/** The strip a page shows while it waits to send a read refused busy again. */
export function busyRetryStrip(seconds) {
  return noticeStrip("The service is running as many long reads as it allows. Retrying in " + seconds + (seconds === 1 ? " second…" : " seconds…"));
}

/**
 * #6800: runs `read`, and when the service refuses it busy (429) calls `onBusy(seconds)`, waits the Retry-After the reply named and
 * runs it once more. The second answer is the caller's, whatever it is, so a second refusal shows as the error it is. An abort of
 * `signal` during the wait returns { kind: "aborted" } without the second read.
 */
export async function retryOnceWhenBusy(read, onBusy, signal) {
  const first = await read();
  const seconds = busyRetryAfter(first);
  if (seconds === null) return first;
  if (signal && signal.aborted) return { kind: "aborted" };
  if (onBusy) onBusy(seconds);
  const waited = await new Promise((resolve) => {
    let onAbort = null;
    const timer = setTimeout(() => {
      if (signal && onAbort) signal.removeEventListener("abort", onAbort);
      resolve(true);
    }, seconds * 1000);
    if (signal) {
      onAbort = () => {
        clearTimeout(timer);
        resolve(false);
      };
      signal.addEventListener("abort", onAbort, { once: true });
    }
  });
  return waited ? read() : { kind: "aborted" };
}

async function classifyResponse(resp) {
  const raw = await resp.text();
  let body = null;
  if (raw) {
    try {
      body = JSON.parse(raw);
    } catch {
      body = null;
    }
  }

  if (resp.status === 401) {
    const message = body && typeof body.error === "string" ? body.error : "Session expired";
    const login = body && typeof body.login === "string" ? body.login : "/";
    reportSessionExpired(message, login);
    return { kind: "auth", message, login };
  }

  const isEnvelope = body && !Array.isArray(body) && typeof body.status === "string" && typeof body.message === "string";

  if (!resp.ok) {
    const msg = body && typeof body.error === "string" ? body.error
      : isEnvelope ? plainText(body.message)
      : "Request failed (HTTP " + resp.status + ")";
    /* #6800: a read refused busy (429) carries the seconds its Retry-After names, for retryOnceWhenBusy. */
    const retryAfterSeconds = resp.status === 429 ? retryAfterSecondsOf(resp.retryAfter !== undefined ? resp.retryAfter : retryAfterOf(resp)) : null;
    return retryAfterSeconds === null ? { kind: "error", message: msg, status: resp.status } : { kind: "error", message: msg, status: resp.status, retryAfterSeconds };
  }

  /* The envelope is exactly {status, message[, hints]}: a top-level string status AND string message.
     Data payloads never carry a top-level message, so this never misfires on real data. */
  if (isEnvelope) {
    if (body.status === "error" || body.status === "invalid") {
      return { kind: "error", message: plainText(body.message), status: resp.status };
    }
    /* The message is the one the page shows: the service wrote it for an MCP client, so its internal names are put into
       words here (click-through 3 and 4). The envelope itself, in `data`, stays as the service sent it. */
    return { kind: "empty", status: body.status, message: emptyMessageText(body), hints: body.hints || null, data: body };
  }

  if (resp.ok && raw && body === null) {
    const message = "Session expired";
    reportSessionExpired(message, "/");
    return { kind: "auth", message, login: "/" };
  }

  return { kind: "data", data: body };
}

/* ── alert delivery state (#3169) ──────────────────────────────────────────────────────────────────
 * A config_alert_log row's notification_type either NAMES A CHANNEL (email / webhook / email+webhook)
 * or STATES A DELIVERY STATE. The state values are listed here once, with the label every surface owes
 * them, because there are three surfaces: this web dashboard, the WPF Darling Viewer and Lite's grid.
 * The other two share PerformanceMonitor.Notifications.AlertDeliveryStatus; JS cannot call it, so this is
 * the restatement, and Darling.Tests.AlertTrayStatusLoggedTests parses THIS OBJECT and compares it to
 * those constants rather than scanning the branch text it used to.
 *
 * "tray" is a state here rather than a channel because this surface is HEADLESS - no system tray, no toast
 * code - so a stored tray row records nothing that happened. #2781/#2814 established that and chose
 * "Logged"; #3169 stopped the service writing it at all, so only rows recorded before then reach it.
 *
 * "digest" (#3712) is an analysis finding the corroboration gate routed to the daily digest and this surface
 * instead of a paging channel - reported, not paged; the row's routing_reason says why.
 */
export const ALERT_STATE_LABELS = {
  none: "No channel",
  unconfigured: "No channel configured",
  muted: "Muted",
  undelivered: "Not sent",
  tray: "Logged",
  throttled: "Throttled",
  folded: "Reported elsewhere",
  failed: "Failed",
  digest: "Digest",
};

/**
 * The delivery state of an alert-history row, or null when its notification_type names a real channel and
 * the caller should fall through to its own sent/failed handling.
 *
 * Carries the one legacy signature that decodes: alert_sent true alongside "tray" is unreachable for a row
 * written after #3169 - a row that delivered names its channel - so it can only be the resolution builder's
 * old hardcoded true, which meant "no send channel applies" and never meant a delivery.
 *
 * "throttled", "folded" and "failed" are the three conditions a stored "undelivered" cannot tell apart
 * (#3427). A retained "undelivered" row is all three at once and keeps its outcome-only "Not sent" label -
 * relabelling it "Throttled" would assert the likeliest of the three as the certain one.
 */
export function alertDeliveryState(a) {
  if (a.alert_sent && a.notification_type === "tray") return ALERT_STATE_LABELS.none;
  if (a.alert_sent) return null;
  return ALERT_STATE_LABELS[a.notification_type] || null;
}

/** GET a read-only tool by its MCP name with query-string params. `signal` — see apiGet (#4191). `options.wholeWindow` (#6359): under
 *  a custom range the read is still anchored at the range's end, but its rows are NOT cut to the exact picked pair, so a read
 *  that answers over the whole hours the grid ranked over (a row's History) shows all of that window. */
export function readTool(tool, params, signal, options) {
  const plan = planCustomRange(tool, params, !!(options && options.wholeWindow));
  const selection = params ? params[TRIM_TO] : undefined;
  return apiGetJoined("/api/read/" + tool + buildQuery(withDatabaseFilter(tool, plan.params)), signal)
    .then(localizeWindowNote)
    .then((res) => finishCustomRange(res, plan))
    .then((res) => (selection && res && res.kind === "data" ? { ...res, data: trimToRange(res.data, selection.startMs, selection.endMs, 0) } : res));
}

/** #6301: a read of an event list (blocking, deadlocks, active snapshots) that carries this key, as `{ startMs, endMs }`, has its rows cut to
 *  that exact pair afterwards. A narrowed read is whole hours back from the selection's end, so without the cut the grid would list events from
 *  before the selection. A symbol, so it never reaches the query string. Totals and rankings do not carry it: they aggregate. */
export const TRIM_TO = Symbol("trimTo");

/* ─────────────────────────── custom range (server page) ─────────────────────────── */

/* The server page's custom start/end, or null for a preset. The page sets it before it builds a tab and clears it for
   a preset and when the reader leaves the server page. It is applied here, to every read, so the ~100 `hours: ctx.hours`
   call sites on the server tabs need no change: a read of the active server whose `hours` is the range's own whole-hour
   count and that names no `as_of` is anchored at the range's end (`as_of`), and its rows are trimmed to the exact pair
   afterwards. The window the read covers is then [end - hours, end], which starts at or before the picked start. */
let activeRange = null;

/** `{ server, hours, startMs, endMs, asOf }` for the page's custom range, or null to go back to the presets. `asOf` is
 *  null for a range that ends now: its reads then name no `as_of` and the server anchors them at its own clock, and only
 *  the start of the trim applies. */
export function setActiveRange(range) {
  activeRange = range || null;
}

/** #6300: where server `server`'s current window ends, in ms: the custom range's end, or now for a live range. A read that
 *  looks at an earlier period (the Compare picker) shifts this end back and names it as `as_of`. */
export function windowEndMs(server) {
  const range = liveRange();
  const end = range && range.server === server && range.asOf ? Date.parse(range.asOf) : NaN;
  return Number.isFinite(end) ? end : Date.now();
}

/* The wait the Active Queries grid is filtered to, per server (#5235): set when a wait row or the wait trend chart opens
   Active Queries at that wait. In memory only, like activeRange: not persisted and not in the URL, so a reload or a pasted
   link shows every active query. A blank wait deletes the entry. */
const queryWaitFilters = new Map();

/** The wait type server `server`'s Active Queries grid is filtered to, or "" when it shows every query. */
export function queryWaitFilter(server) {
  return queryWaitFilters.get(server) || "";
}

/** Filter server `server`'s Active Queries to wait `w`; a blank (or non-text) `w` clears the filter. */
export function setQueryWaitFilter(server, w) {
  const v = typeof w === "string" ? w.trim() : "";
  if (v === "") queryWaitFilters.delete(server);
  else queryWaitFilters.set(server, v);
}

/** Whether wait `w` gets a link to Active Queries (#5235): any named wait except Query Store's QDS_* waits, which are the
 *  background tasks' own and never a request's wait at a capture. The wait rows and the wait trend chart's menu item both
 *  ask this, so the two cannot disagree. */
export function waitIsLinked(w) {
  const v = typeof w === "string" ? w.trim() : "";
  return v !== "" && !/^QDS_/i.test(v);
}

/* The windowed reads that take no `as_of` (the catalog's `hours` without an `as_of`): they keep answering "the last N
   hours ending now", and say so. Every other windowed read takes `as_of`. WebServerPageRangeTests pins this list
   against the read catalog. */
const READS_WITHOUT_AS_OF = new Set(["get_fleet_overview", "get_investigation_reports", "get_read_latency", "get_slow_reads", "get_finops", "get_store_query_history"]);

/* The row fields that stamp one sample, event or run at an instant. A list under a read's answer whose rows carry one of
   these is cut to the exact range. Fields that say when something last happened (last_execution_time and its kin, and
   the captured_at / measured_at that some aggregate reads stamp with their LAST sample) are left alone: those rows are
   totals over the window, not points in it. */
const INSTANT_FIELDS = ["sample_time", "time", "collection_time", "event_time", "occurred_at", "deadlock_time", "change_time", "time_bucket", "run_time"];

/* The range belongs to the server page: a read from any other page is never anchored by it. */
function liveRange() {
  const hash = typeof location !== "undefined" && location && typeof location.hash === "string" ? location.hash : "";
  return activeRange && (hash === "" || hash.startsWith("#/server/")) ? activeRange : null;
}

/** A string that changes when the page's custom range does (its end and span), for a panel that caches a read per window. */
export function activeRangeStamp() {
  const range = liveRange();
  return range ? [range.server, range.hours, range.asOf || ""].join("|") : "";
}

/* Marks the retry readWithinKeptHistory sends for fewer hours, with the hours the read first asked for. */
const NARROWED_FROM = Symbol("narrowedFrom");

function planCustomRange(tool, params, wholeWindow) {
  const range = liveRange();
  const narrowedFrom = params ? params[NARROWED_FROM] : undefined;
  const asked = narrowedFrom != null ? Number(narrowedFrom) : Number(params && params.hours);
  if (!range || !params || params.server !== range.server || asked !== range.hours || params.as_of != null) {
    return { params, range: null, ignored: false };
  }
  if (READS_WITHOUT_AS_OF.has(tool)) return { params, range: null, ignored: true };
  const withEnd = range.asOf ? { ...params, as_of: range.asOf } : params;
  if (wholeWindow) return { params: withEnd, range: null, ignored: false };
  if (narrowedFrom == null) return { params: withEnd, range, ignored: false };
  /* The kept-history retry: the same end, the hours the store keeps, and only the part of the range those hours reach. */
  const kept = Number(params.hours);
  range.narrowedTo = kept;
  return { params: withEnd, range: { ...range, startMs: Math.max(range.startMs, range.endMs - kept * 3600000) }, ignored: false, narrowed: true };
}

function finishCustomRange(res, plan) {
  if (plan.ignored) return res && (res.kind === "data" || res.kind === "empty") ? { ...res, presetHours: Number(plan.params.hours) } : res;
  if (plan.narrowed && res && (res.kind === "data" || res.kind === "empty")) res = { ...res, keptCustom: true };
  if (!plan.range || !res || res.kind !== "data") return res;
  return { ...res, data: trimToRange(res.data, plan.range.startMs, plan.range.asOf ? plan.range.endMs : Infinity, 0) };
}

function trimToRange(node, startMs, endMs, depth) {
  if (!node || typeof node !== "object" || Array.isArray(node) || depth > 2) return node;
  let out = node;
  for (const [key, value] of Object.entries(node)) {
    let next = value;
    if (Array.isArray(value)) {
      const first = value.find((r) => r && typeof r === "object");
      const field = first ? INSTANT_FIELDS.find((f) => f in first) : null;
      if (field) {
        next = value.filter((r) => {
          const at = r && parseUtc(r[field]);
          return !at || (at.getTime() >= startMs && at.getTime() <= endMs);
        });
        if (next.length === value.length) next = value;
      }
    } else {
      next = trimToRange(value, startMs, endMs, depth + 1);
    }
    if (next !== value) {
      if (out === node) out = { ...node };
      out[key] = next;
    }
  }
  return out;
}

/* ─────────────────────────── database filter (server page) ─────────────────────────── */

/* The server page's database filter (#5244, #5245), or null for none: `{ server, databases }`. The page sets it before it
   redraws a panel and clears it when the reader leaves the server page, as it does the custom range. It is applied here, to
   every FILTERED read of the active server (database-filter-reads.js), so the page's ~100 `readTool` call sites need no
   change: readTool adds one `database_name` key per chosen database. dbScopeChip below says, per panel, whether it applied. */
let activeDatabaseFilter = null;

/** `{ server, databases }` for the page's database filter. `databases` is a list of names, kept exactly as given (never
 *  trimmed, case-folded or split on commas: a name may hold a comma). No list, an empty list or no server is no filter. */
export function setActiveDatabaseFilter(filter) {
  const databases = filter && Array.isArray(filter.databases) ? filter.databases.slice() : [];
  const server = filter && typeof filter.server === "string" ? filter.server : "";
  activeDatabaseFilter = databases.length > 0 && server !== "" ? { server, databases } : null;
}

/** A copy of the filter set by setActiveDatabaseFilter, or null for none. It does not look at the page: the server page's
 *  own code asks it for the chosen names (readTool and dbScopeChip also need the page to be a server page). */
export function getActiveDatabaseFilter() {
  return activeDatabaseFilter ? { server: activeDatabaseFilter.server, databases: activeDatabaseFilter.databases.slice() } : null;
}

/* The filter belongs to the server page: a read or a panel on any other page is never touched by it. */
function liveDatabaseFilter() {
  const hash = typeof location !== "undefined" && location && typeof location.hash === "string" ? location.hash : "";
  return activeDatabaseFilter && hash.startsWith("#/server/") ? activeDatabaseFilter : null;
}

/* Whether a read's own `database_name` names anything the query string would carry (buildQuery skips null, undefined, "" and
   an empty array). */
const namesADatabase = (value) => (Array.isArray(value) ? value.length > 0 : value != null && value !== "");

/* The params a read goes out with: the chosen databases are added as an array (buildQuery writes it as repeated keys) only
   when the page is a server page, the read is in FILTERED, `params.server` is the active server and the params name no
   database of their own, so an explicit name wins. The caller's object is never changed. */
function withDatabaseFilter(tool, params) {
  const filter = liveDatabaseFilter();
  if (!filter || !params || params.server !== filter.server || !FILTERED.has(tool) || namesADatabase(params.database_name)) return params;
  return { ...params, database_name: filter.databases.slice() };
}

/* The deadlock reads are unfiltered by design: a deadlock spans several databases, and they are inside each graph. */
const DEADLOCK_READS = new Set(["get_deadlock_trend", "get_deadlocks", "get_deadlock_detail"]);

/**
 * What a panel's database scope chip says, or null for no chip (#5244 D4). Only while a filter is active on the server page:
 * an empty filter shows no chip anywhere. `read` is the panel's main read; `override` is the panel's own `dbScope` when one
 * read feeds panels of different kinds ("server", "unfiltered" or "process-rows"), and wins over the read's class.
 *   "filtered"     the read takes the filter
 *   "unfiltered"   a database-scoped read that cannot take it, so it shows every database
 *   "server"       the data has no database
 *   "process-rows" the Deadlock Graphs panel: each graph whole, its process rows filtered in the browser
 *   null           an identity read (the query drill and the seven plan-viewer reads), or a read no class names
 */
export function dbScopeState(read, override) {
  if (!liveDatabaseFilter()) return null;
  if (override === "server" || override === "unfiltered" || override === "process-rows") return override;
  const scope = readScope(read);
  return scope === "identity" ? null : scope;
}

/* Reads whose "no row" text names a missing snapshot: a filter that matches no database leaves the snapshot in place with an
   empty `databases` list, so that text would be false. database-filter-reads.js stays pure data, so the sentence lives here. */
const FILTERED_NO_ROW_READS = new Set(["get_database_config", "get_database_scoped_config", "get_query_store_health"]);

/**
 * The text a table panel shows when its read answered with no rows (#5244). Normally the panel's own `emptyText`. While a database
 * filter is active and the read takes it, a snapshot-state text (get_database_config, get_database_scoped_config and
 * get_query_store_health) is replaced by one that says the chosen databases have no row, because the snapshot itself exists:
 * a server with no snapshot answers through the read's empty envelope instead, which never reaches this text.
 */
export function dbFilteredEmptyText(read, emptyText, override) {
  if (!FILTERED_NO_ROW_READS.has(read) || dbScopeState(read, override) !== "filtered") return emptyText;
  return "No database in the chosen databases has a row in this snapshot.";
}

/**
 * The chip for dbScopeState(read, override), or null. It reuses the badge look (warning for "unfiltered", muted for "server")
 * and tags itself with `data-db-scope`. A database name is only ever text here: the label is `el(..., { text })` and the
 * title, which lists the names one per line, is set as an attribute, so no name is read as markup (R4).
 */
export function dbScopeChip(read, override) {
  const state = dbScopeState(read, override);
  if (state === null) return null;
  const names = liveDatabaseFilter().databases;
  const countText = (n) => (n === 1 ? "1 database" : n + " databases");
  let text;
  let title;
  let look = "";
  if (state === "filtered") {
    text = names.length === 1 ? names[0] : countText(names.length);
    title = names.join("\n");
  } else if (state === "unfiltered") {
    text = "All databases";
    title =
      "This panel's read cannot take the database filter, so it shows every database." +
      (DEADLOCK_READS.has(read) ? " The databases are inside each deadlock graph." : "");
    look = " band-Warning";
  } else if (state === "process-rows") {
    text = "Graphs: all; process rows: " + countText(names.length);
    title = "Each graph is whole; process rows outside the chosen databases are hidden.";
  } else {
    text = "Server-wide";
    title = "This data has no database, so the filter does not apply.";
    look = " engine";
  }
  return el("span", { class: "badge db-scope db-scope-" + state + look, title, text, dataset: { dbScope: state } });
}
