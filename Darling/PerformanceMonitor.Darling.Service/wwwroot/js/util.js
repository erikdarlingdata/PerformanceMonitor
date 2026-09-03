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
function makeActivatable(node, handler) {
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
 * window the view keeps rather than a red error carrying the API's own wording. Every other message stays an
 * error. SHARED by every read-error site — the descriptor loader AND the hand-built server-tab composites — so
 * a tab cannot show a friendly notice on one panel and the raw string on its neighbour.
 */
export function readErrorStrip(message) {
  const m = /exceeds maximum of (\d+) hours/.exec(message || "");
  if (m) {
    const hours = Number(m[1]);
    const days = Math.round(hours / 24);
    return noticeStrip(
      "This view keeps up to " + hours + " hours (" + days + " day" + (days === 1 ? "" : "s") +
      ") of history — pick a shorter range.");
  }
  return errorStrip(message);
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
 *  chart passes true whenever the domain's first and last samples fall on different calendar days). */
export function axisTime(date, withDate) {
  if (!date) return "";
  if (withDate) {
    return date.toLocaleString([], { month: "numeric", day: "numeric", hour: "2-digit", minute: "2-digit" });
  }
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
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

/** CSS class for a fleet band ("Healthy"/"Warning"/"Critical"/"Offline") — colors live in CSS. */
export function bandClass(band) {
  return "band-" + (band || "Unknown");
}
/** CSS class for a per-metric severity ("Unknown"/"Healthy"/"Warning"/"Critical"). */
export function sevClass(sev) {
  return "sev-" + (sev || "Unknown");
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

/**
 * GET a same-origin API path and classify the response into one of three kinds, mirroring the service's
 * three response shapes (DarlingWebEndpoints):
 *   { kind: "data",  data }                 — a data object/array (HTTP 200 JSON passthrough)
 *   { kind: "empty", status, message, ... } — the {status,message[,hints]} empty envelope (HTTP 200)
 *   { kind: "error", message, status }      — { "error": ... } (HTTP 400/500) or a transport failure
 * Auth is handled entirely server-side (loopback is tokenless; network mode sets the session cookie before
 * the SPA loads), so requests carry it automatically — nothing to do here.
 */
export async function apiGet(path) {
  let resp;
  try {
    resp = await fetch(path, { headers: { Accept: "application/json" } });
  } catch (e) {
    return { kind: "error", message: "Network error: " + (e && e.message ? e.message : String(e)) };
  }
  return classifyResponse(resp);
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
 * Classify a completed Response into the same three-kind shape apiGet returns (shared by apiGet + apiSend):
 * an { "error": ... } body / non-2xx -> "error"; the {status, message[, hints]} envelope -> "empty"; anything
 * else (including a 204/empty body -> data:null) -> "data".
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

  if (!resp.ok) {
    const msg = body && typeof body.error === "string" ? body.error : "Request failed (HTTP " + resp.status + ")";
    return { kind: "error", message: msg, status: resp.status };
  }

  /* The empty envelope is exactly {status, message[, hints]}: a top-level string status AND string message.
     Data payloads never carry a top-level message, so this never misfires on real data. */
  if (body && !Array.isArray(body) && typeof body.status === "string" && typeof body.message === "string") {
    return { kind: "empty", status: body.status, message: body.message, hints: body.hints || null, data: body };
  }

  return { kind: "data", data: body };
}

/** GET a read-only tool by its MCP name with query-string params. */
export function readTool(tool, params) {
  return apiGet("/api/read/" + tool + buildQuery(params));
}
