/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Known Patterns page (#6334) - the web editor for the entries that say "this alert at this time is expected" (the backup job
 * pushes High CPU every night at 02:00). Lists every entry from get_known_patterns (include_expired=true) and, for a seat the
 * session reports can_edit, writes through POST /api/known-patterns, PUT /api/known-patterns/{id} (replaces the whole entry)
 * and DELETE. The server is the authority for every write and for the refusal text; this page only shows what it said.
 * Entries are written by operators only: there is no MCP write tool.
 *
 * Polling: app.js re-calls renderKnownPatterns on its 60 s tick. The form and its typed values live in module scope, so a tick
 * redraws the LIST only and never touches an open form.
 *
 * What is sent (formToBody, interpretWrite, noteCounter) is plain functions with no DOM, run under Node by
 * KnownPatternsPageBehaviourTests. All entry text, the note above all, reaches the DOM through el()/textContent.
 */

import { storeHeaders, el, mount, apiGet, readTool, loadingStrip, errorStrip, emptyStrip, localTime, reportSessionExpired, networkErrorText } from "../util.js";
import * as api from "../alerts-api.js";

/** The most characters a note holds (KnownPatternMatcher.MaxNoteLength). The server refuses more; the counter only warns early. */
export const MAX_NOTE = 500;
/** The most minutes either side of the time of day (KnownPatternMatcher.MaxWindowMinutes). */
export const MAX_WINDOW = 720;
/** The window the server fills in when none is sent (KnownPatternMatcher.DefaultWindowMinutes). */
export const DEFAULT_WINDOW = 30;

/** The browser's IANA zone, or "" when it cannot say. */
export function browserZone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "";
  } catch {
    return "";
  }
}

/** A datetime-local value ("2026-08-01T10:30", local clock) to an ISO UTC string, or null when blank or unparseable. */
export function localInputToIso(v) {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

/** An ISO timestamp (the store returns UTC with or without a zone suffix) to a datetime-local value on the local clock, or "". */
export function isoToLocalInput(iso) {
  if (!iso) return "";
  const hasZone = /[zZ]$|[+\-]\d\d:?\d\d$/.test(iso);
  const d = new Date(hasZone ? iso : iso + "Z");
  if (Number.isNaN(d.getTime())) return "";
  const p = (n) => String(n).padStart(2, "0");
  return d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate()) + "T" + p(d.getHours()) + ":" + p(d.getMinutes());
}

/** The write body for a form's raw string values. Blank server, metric, time and expiry are null (any / never); blank zone is the
 *  browser's zone only when a time of day is set. The whole entry is sent every time: PUT replaces. */
export function formToBody(values, zoneFallback) {
  const text = (k) => (values[k] == null ? "" : String(values[k]).trim());
  const time = text("time_of_day");
  const zone = text("time_zone");
  const server = text("server_id");
  const windowText = text("window_minutes");
  return {
    server_id: /^\d+$/.test(server) ? Number(server) : null,
    metric_name: text("metric_name") || null,
    time_of_day: time || null,
    time_zone: time ? zone || zoneFallback || null : null,
    window_minutes: time && /^\d+$/.test(windowText) ? Number(windowText) : null,
    note: values.note == null ? "" : String(values.note),
    expires_at_utc: localInputToIso(text("expires_at_utc")),
  };
}

/** The form values for an entry read from get_known_patterns. */
export function valuesOf(e, zoneFallback) {
  return {
    server_id: e.server_id == null ? "" : String(e.server_id),
    metric_name: e.metric_name || "",
    time_of_day: e.time_of_day || "",
    time_zone: e.time_zone || zoneFallback || "",
    window_minutes: e.window_minutes == null ? "" : String(e.window_minutes),
    note: e.note || "",
    expires_at_utc: isoToLocalInput(e.expires_at_utc),
  };
}

/** "123 / 500" under the note box; over is true once the note passes the limit. */
export function noteCounter(note) {
  const length = (note == null ? "" : String(note).trim()).length;
  return { text: length + " / " + MAX_NOTE, over: length > MAX_NOTE };
}

/** Turns a write response ({status, body}) into what the page acts on. A 400 carries the server's refusal text verbatim. */
export function interpretWrite(status, body) {
  const b = body && typeof body === "object" ? body : {};
  const message = typeof b.error === "string" ? b.error : typeof b.message === "string" ? b.message : "Request failed (HTTP " + status + ")";
  if (status === 401) return { kind: "expired", message: "Your session has expired. Sign in again." };
  if (status === 403) return { kind: "readonly", message: "This session is read-only, so the change was not made." };
  if (status === 404) return { kind: "notfound", message: "That known pattern is gone. It may have been deleted elsewhere." };
  if (status === 400) return { kind: "invalid", message };
  if (status >= 200 && status < 300) return { kind: "ok", id: b.id == null ? null : b.id };
  return { kind: "error", message };
}

/** Whether an entry's expiry has passed. */
export function isExpired(e, now) {
  if (e.expired === true) return true;
  const iso = e.expires_at_utc;
  if (!iso) return false;
  const d = new Date(/[zZ]$|[+\-]\d\d:?\d\d$/.test(iso) ? iso : iso + "Z");
  return !Number.isNaN(d.getTime()) && d.getTime() <= (now || Date.now());
}

/** One line for the Scope column. */
export function scopeText(e, serverLabel) {
  const server = e.server_id == null ? "Every server" : serverLabel(e.server_id);
  const metric = e.metric_name ? e.metric_name : "any alert";
  const when = e.time_of_day
    ? "around " + e.time_of_day + " " + (e.time_zone || "UTC") + " (+/- " + (e.window_minutes == null ? DEFAULT_WINDOW : e.window_minutes) + " min)"
    : "at any time";
  return server + ", " + metric + ", " + when;
}

/* One write: own fetch rather than apiSend, because a 400's body carries the refusal text apiSend's error shape drops. */
async function send(method, path, body) {
  let resp;
  try {
    resp = await fetch(path, {
      method,
      headers: storeHeaders({ Accept: "application/json", "Content-Type": "application/json" }),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    return { kind: "error", message: networkErrorText(e) };
  }
  let parsed = null;
  try {
    parsed = JSON.parse(await resp.text());
  } catch {
    parsed = null;
  }
  let result = interpretWrite(resp.status, parsed);
  if (result.kind === "ok" && (parsed === null || typeof parsed !== "object")) {
    /* A 2xx that is not a JSON body is a sign-in page in front of the API, not a saved entry. */
    result = { kind: "expired", message: "Your session has expired. Sign in again." };
  }
  if (result.kind === "expired") reportSessionExpired(result.message, "/");
  return result;
}

export const createEntry = (body) => send("POST", "/api/known-patterns", body);
export const replaceEntry = (id, body) => send("PUT", "/api/known-patterns/" + encodeURIComponent(id), body);
export const deleteEntry = (id) => send("DELETE", "/api/known-patterns/" + encodeURIComponent(id));

/* ─────────────────────────── page ─────────────────────────── */

/* Module scope: survives the 60 s re-render. `live` is the mounted page; `form` is the open form's state. */
let live = null;
let form = null;
/* The registered servers, {id, label}, for the picker and the Scope column. */
let servers = [];

function serverLabel(id) {
  const s = servers.find((x) => x.id === id);
  return s ? s.label : "server " + id;
}

async function loadServers() {
  /* list_servers carries no store id, so the picker reads the Admin grid's registry route (a GET any seat may make). */
  const res = await apiGet("/api/admin/servers");
  if (res.kind === "error" || res.kind === "auth" || res.kind === "aborted" || res.kind === "empty") return;
  const rows = Array.isArray(res.data) ? res.data : res.data && Array.isArray(res.data.servers) ? res.data.servers : [];
  servers = rows
    .filter((r) => Number.isInteger(r.server_id))
    .map((r) => ({ id: r.server_id, label: r.display_name || r.server_name || "server " + r.server_id }));
}

export async function renderKnownPatterns(main) {
  if (live && live.main === main && live.headEl.isConnected) {
    await refreshList(live);
    return;
  }
  const session = await api.getSession();
  const canEdit = !!session.can_edit;
  const newBtn = canEdit ? el("button", { class: "btn primary", type: "button", text: "New known pattern" }) : null;
  const headEl = el("div", { class: "page-head" }, [
    el("h2", { text: "Known Patterns" }),
    el("div", { class: "meta", text: "what an operator already knows about an alert: shown beside matching alerts, nothing is suppressed" }),
    el("div", { class: "spacer" }),
    newBtn,
  ]);
  const formBox = el("div", {});
  const listBox = el("div", {});
  mount(main, [headEl, formBox, listBox]);
  live = { main, headEl, formBox, listBox, canEdit, entries: [] };
  if (newBtn) newBtn.addEventListener("click", () => openForm({ mode: "create", values: { time_zone: browserZone() } }));
  if (form) drawForm();
  mount(listBox, loadingStrip("Loading known patterns…"));
  await loadServers();
  await refreshList(live);
}

async function refreshList(state) {
  const res = await readTool("get_known_patterns", { include_expired: true });
  if (res.kind === "error") return mount(state.listBox, errorStrip(res.message));
  if (res.kind === "empty") {
    state.entries = [];
    return mount(state.listBox, emptyStrip(res.message || "No known patterns are recorded."));
  }
  state.entries = (res.data && res.data.known_patterns) || [];
  drawList(state);
}

function drawList(state) {
  if (!state.entries.length) return mount(state.listBox, emptyStrip("No known patterns are recorded."));
  const head = ["Status", "Scope", "Note", "Expires", "Added by", state.canEdit ? "" : null]
    .filter((h) => h !== null)
    .map((h) => el("th", { text: h }));
  mount(state.listBox, el("table", { class: "data known-patterns" }, [
    el("thead", {}, [el("tr", {}, head)]),
    el("tbody", {}, state.entries.map((e) => entryRow(e, state.canEdit))),
  ]));
}

function entryRow(e, canEdit) {
  const expired = isExpired(e);
  const cells = [
    el("td", {}, [el("span", { class: expired ? "paused-badge" : "status-cell sev-Healthy", text: expired ? "Expired" : "Active" })]),
    el("td", { text: scopeText(e, serverLabel) }),
    /* The note is operator-written text: a text node only, never markup. */
    el("td", { class: "known-pattern-note", text: e.note || "" }),
    el("td", { text: e.expires_at_utc ? localTime(e.expires_at_utc) : "Never" }),
    el("td", { text: (e.created_by || "—") + (e.created_at_utc ? ", " + localTime(e.created_at_utc) : "")
      + (e.updated_at_utc || e.updated_by ? " (edited by " + (e.updated_by || "—") + (e.updated_at_utc ? ", " + localTime(e.updated_at_utc) : "") + ")" : "") }),
  ];
  if (canEdit) {
    cells.push(el("td", { class: "mute-actions" }, [
      el("button", { class: "btn", type: "button", text: "Edit", onClick: () => openForm({ mode: "edit", id: e.id, values: valuesOf(e, browserZone()) }) }),
      el("button", { class: "btn", type: "button", text: "Delete", onClick: () => remove(e) }),
    ]));
  }
  return el("tr", { "data-pattern-id": e.id }, cells);
}

function notify(message, isError) {
  if (live) mount(live.formBox, [form ? formNode() : null, el("div", { class: "strip " + (isError ? "error" : "notice"), text: message })]);
}

async function remove(e) {
  if (!window.confirm("Delete this known pattern permanently?\n\n" + scopeText(e, serverLabel))) return;
  const res = await deleteEntry(e.id);
  if (res.kind !== "ok" && res.kind !== "notfound") return notify(res.message, true);
  await refreshList(live);
}

/* ─────────────────────────── form ─────────────────────────── */

function openForm(f) {
  form = { banner: null, ...f };
  drawForm();
}

function drawForm() {
  if (!live) return;
  mount(live.formBox, form ? formNode() : []);
}

function closeForm() {
  form = null;
  drawForm();
}

function labelled(label, control, extra) {
  return el("label", { class: "mute-field" }, [el("span", { class: "mute-label", text: label }), control, extra || null]);
}

function textInput(key, label, type, attrs) {
  const input = el("input", { type, class: "mute-input", value: form.values[key] || "", "aria-label": label, "data-field": key, ...(attrs || {}) });
  input.addEventListener("input", () => { form.values[key] = input.value; });
  return input;
}

function serverSelect() {
  const options = [el("option", { value: "", text: "Every server" })].concat(
    servers.map((s) => el("option", { value: String(s.id), text: s.label })));
  const current = form.values.server_id || "";
  /* A server the picker no longer lists stays selectable so an edit does not silently widen the entry to every server. */
  if (current && !servers.some((s) => String(s.id) === current)) options.push(el("option", { value: current, text: "server " + current }));
  const select = el("select", { class: "mute-input", "aria-label": "Server", "data-field": "server_id" }, options);
  select.value = current;
  select.addEventListener("change", () => { form.values.server_id = select.value; });
  return select;
}

function formNode() {
  const counter = el("span", { class: "meta", "data-role": "note-counter", text: noteCounter(form.values.note).text });
  const note = el("textarea", { class: "mute-input", rows: "3", "aria-label": "Note", "data-field": "note" });
  note.value = form.values.note || "";
  note.addEventListener("input", () => {
    form.values.note = note.value;
    const c = noteCounter(note.value);
    counter.textContent = c.text;
    counter.className = c.over ? "mute-rules-field-error" : "meta";
  });
  return el("div", { class: "card mute-form" }, [
    el("h3", { text: form.mode === "create" ? "New known pattern" : "Edit known pattern" }),
    el("div", { class: "meta", text: "An alert that matches ALL filled fields shows this note. Blank fields match anything." }),
    form.banner ? el("div", { class: "strip error", role: "alert", "data-role": "refusal", text: form.banner }) : null,
    labelled("Server", serverSelect()),
    labelled("Metric (blank = any alert)", textInput("metric_name", "Metric", "text")),
    labelled("Time of day (24-hour, blank = any time)", textInput("time_of_day", "Time of day", "time")),
    labelled("Time zone (IANA id)", textInput("time_zone", "Time zone", "text", { placeholder: browserZone() })),
    labelled("Window, minutes either side (0 to " + MAX_WINDOW + ", blank = " + DEFAULT_WINDOW + ")", textInput("window_minutes", "Window minutes", "number", { min: "0", max: String(MAX_WINDOW) })),
    labelled("Note (required, " + MAX_NOTE + " characters at most)", note, counter),
    labelled("Expires (local time; blank = never)", textInput("expires_at_utc", "Expires", "datetime-local")),
    el("div", { class: "form-actions" }, [
      el("button", { class: "btn primary", type: "button", text: "Save", onClick: submit }),
      el("button", { class: "btn", type: "button", text: "Cancel", onClick: closeForm }),
    ]),
  ]);
}

async function submit() {
  form.banner = null;
  const body = formToBody(form.values, browserZone());
  const res = form.mode === "create" ? await createEntry(body) : await replaceEntry(form.id, body);
  if (res.kind === "ok") {
    closeForm();
    return refreshList(live);
  }
  form.banner = res.message;
  drawForm();
}
