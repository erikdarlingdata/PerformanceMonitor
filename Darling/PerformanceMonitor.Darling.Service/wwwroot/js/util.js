/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Shared leaf utilities for Darling Web (#1562): DOM builders, UTC->local time, value formatters, and the API
 * fetch helper. This module imports nothing (it is the base of the module DAG: app/panels/charts/pages all
 * import it, so there is no import cycle). Two rules are enforced HERE so every caller inherits them:
 *   R4 (XSS): the el() builder assigns untrusted text ONLY through textContent / text nodes; it throws if a
 *     caller ever tries to pass raw HTML, so no data path can reach innerHTML.
 *   R5 (time): every timestamp from the API is naive UTC ISO-8601 (no zone suffix) — parseUtc() appends 'Z'
 *     so the browser reads it as UTC, then formatting localizes it to the viewer's zone.
 */

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
  return el("div", { class: "strip error", role: "alert" }, [message]);
}
export function emptyStrip(message) {
  return el("div", { class: "strip empty" }, [message]);
}
/** A non-fatal caveat about otherwise-good data (e.g. the #1665 partial-window notice). */
export function noticeStrip(message) {
  return el("div", { class: "strip notice", role: "status" }, [message]);
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
  if (!desc || desc.windowNote === false || (desc.viz !== "table" && desc.viz !== "stat")) return null;
  if (desc.noteKey === "truncation_note" || (desc.moreNoteKeys || []).includes("truncation_note")) return null;
  const source = desc.floorKey ? getPath(data, desc.floorKey) : data;
  if (!source || source.window_truncated !== true) return null;
  const note = source.truncation_note;
  return typeof note === "string" && note.trim() ? noticeStrip(note) : null;
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

/* A read's answer with its window note composed in the browser's zone (windowNoteText), so every strip that draws
   `truncation_note` (the grids' floor note, the Queries tab's noteKey notes, the hand-built composites) shows one
   clock. An empty envelope carries the note too, in its `data` (#4966: a grid that looked and found nothing says where its
   data starts, so a new server's empty week does not read as a quiet one). Anything but a data or empty answer, and an answer
   whose note is already as it should be, comes back as it is. */
function localizeWindowNote(res) {
  if (!res || (res.kind !== "data" && res.kind !== "empty")) return res;
  const note = windowNoteText(res.data);
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
 *  chart passes true whenever the domain's start and end fall on different calendar days). */
export function axisTime(date, withDate) {
  if (!date) return "";
  if (withDate) {
    return date.toLocaleString([], { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit" });
  }
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
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

/* ─────────────────────────── value formatters ─────────────────────────── */

export function fmtInt(v) {
  if (v == null) return "—";
  const n = Number(v);
  return isFinite(n) ? n.toLocaleString(undefined, { maximumFractionDigits: 0 }) : "—";
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
export function fmtPct(v) {
  if (v == null) return "—";
  const n = Number(v);
  return isFinite(n) ? Math.round(n) + "%" : "—";
}
export function fmtMs(v) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  const abs = Math.abs(n);
  if (abs < 1000) return fmtInt(n) + " ms";
  if (abs < 60000) return fmtNum(n / 1000, 1) + " s";
  if (abs < 3600000) return fmtNum(n / 60000, 1) + " min";
  return fmtNum(n / 3600000, 1) + " h";
}
export function fmtMb(v) {
  if (v == null) return "—";
  const n = Number(v);
  if (!isFinite(n)) return "—";
  return n >= 1024 ? fmtNum(n / 1024, 1) + " GB" : fmtInt(n) + " MB";
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
  num1: (v) => fmtNum(v, 1),
  num2: (v) => fmtNum(v, 2),
  rate: fmtRate,
  pct: fmtPct,
  ms: fmtMs,
  mb: fmtMb,
  time: localTime,
  reltime: relTime,
  bool: fmtBool,
  text: fmtText,
};

export function applyFormat(name, value) {
  const f = (name && FORMATTERS[name]) || FORMATTERS.text;
  return f(value);
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

/** Build a query string from a params object, skipping null/undefined/empty values. */
export function buildQuery(params) {
  if (!params) return "";
  const parts = [];
  for (const [k, v] of Object.entries(params)) {
    if (v == null || v === "") continue;
    parts.push(encodeURIComponent(k) + "=" + encodeURIComponent(v));
  }
  return parts.length ? "?" + parts.join("&") : "";
}

/* In-flight read counter (#4191): every apiGet/readTool call counts itself while its fetch is outstanding, so
   the poll loop (app.js refresh()) can tell whether the page it is about to re-render has already settled
   before firing a whole new set of the same reads on top of it. apiSendRead (a read that must travel as a POST,
   the composed-panel run) IS counted, so a slow panel holds the poll off and the refresh back-off measures it.
   apiGetFleet IS counted too (each caller counts its own wait on the shared request), so a render whose reads are
   fleet reads is timed for as long as it ran. apiSend is deliberately NOT counted: a mutation is not a "page read"
   a poll tick should wait out. */
let inFlightReads = 0;

/** True while at least one apiGet/readTool call is outstanding — see the counter comment above. */
export function hasInFlightReads() {
  return inFlightReads > 0;
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
  inFlightReads++;
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
    inFlightReads--;
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
  inFlightReads++;
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
    inFlightReads--;
  }
}

/** Fetch a path and read its whole body once, for a response several callers classify. A failure comes back as a
    value rather than a rejection, so one lost request cannot surface as an unhandled rejection per caller. */
async function fetchBody(path) {
  try {
    const resp = await fetch(path, { headers: { Accept: "application/json" } });
    return { ok: resp.ok, status: resp.status, raw: await resp.text() };
  } catch (e) {
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

/** #4666: a READ that has to travel as a POST (the composed-panel run, /api/compose/run). Counted in inFlightReads
    exactly like apiGet, so the poll's overlap guard (#4191) waits it out and the refresh back-off measures the render
    that contains it. Mutations (saves, deletes, alert validate/test) keep using apiSend, uncounted. */
export async function apiSendRead(method, path, body) {
  inFlightReads++;
  try {
    return await apiSend(method, path, body);
  } finally {
    inFlightReads--;
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
      : isEnvelope ? body.message
      : "Request failed (HTTP " + resp.status + ")";
    return { kind: "error", message: msg, status: resp.status };
  }

  /* The envelope is exactly {status, message[, hints]}: a top-level string status AND string message.
     Data payloads never carry a top-level message, so this never misfires on real data. */
  if (isEnvelope) {
    if (body.status === "error" || body.status === "invalid") {
      return { kind: "error", message: body.message, status: resp.status };
    }
    return { kind: "empty", status: body.status, message: body.message, hints: body.hints || null, data: body };
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

/** GET a read-only tool by its MCP name with query-string params. `signal` — see apiGet (#4191). */
export function readTool(tool, params, signal) {
  const plan = planCustomRange(tool, params);
  return apiGet("/api/read/" + tool + buildQuery(plan.params), signal)
    .then(localizeWindowNote)
    .then((res) => finishCustomRange(res, plan));
}

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

/* The windowed reads that take no `as_of` (the catalog's `hours` without an `as_of`): they keep answering "the last N
   hours ending now", and say so. Every other windowed read takes `as_of`. WebServerPageRangeTests pins this list
   against the read catalog. */
const READS_WITHOUT_AS_OF = new Set(["get_fleet_overview", "get_read_latency", "get_slow_reads", "get_finops", "get_store_query_history"]);

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

/* Marks the retry readWithinKeptHistory sends for fewer hours, with the hours the read first asked for. */
const NARROWED_FROM = Symbol("narrowedFrom");

function planCustomRange(tool, params) {
  const range = liveRange();
  const narrowedFrom = params ? params[NARROWED_FROM] : undefined;
  const asked = narrowedFrom != null ? Number(narrowedFrom) : Number(params && params.hours);
  if (!range || !params || params.server !== range.server || asked !== range.hours || params.as_of != null) {
    return { params, range: null, ignored: false };
  }
  if (READS_WITHOUT_AS_OF.has(tool)) return { params, range: null, ignored: true };
  const withEnd = range.asOf ? { ...params, as_of: range.asOf } : params;
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
