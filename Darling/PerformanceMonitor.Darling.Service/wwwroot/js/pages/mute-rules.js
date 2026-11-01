/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Mute Rules page (#4843) — the web twin of the desktop viewer's Manage Mute Rules window. Lists every rule
 * from get_mute_rules (enabled_only=false, so paused and expired rules show) and, for a seat the session reports
 * can_edit, writes through the four /api/mute-rules routes: POST (create), PATCH (partial edit), PUT .../enabled
 * (pause / resume) and DELETE. The server is the authority for every write; this page is only the affordance.
 *
 * The read carries no updated-at or last-matched field, so the page shows created and expiry only.
 *
 * A rule can also repeat on a schedule (#7040): days of the week, a daily start and end time, and an IANA time zone.
 * The four fields travel together (scheduleOf, scheduleBody, schedulePatch); all four empty is a one-off rule.
 *
 * Polling: app.js re-calls renderMuteRules on its 60 s tick. The form, its typed values and its focus live in
 * module scope (`live`, `form`): a tick re-reads and redraws the LIST only and never touches an open form.
 *
 * The logic that decides what is sent (formToCreateBody, buildPatch, isEmptyCreate, interpretWrite) is plain
 * functions with no DOM, run under Node by MuteRulesBehaviourTests. All user text reaches the DOM through
 * el()/textContent.
 */

import { storeHash, storeHeaders, el, mount, readTool, loadingStrip, errorStrip, emptyStrip, localTime, reportSessionExpired } from "../util.js";
import * as api from "../alerts-api.js";

/** The editable text scope/pattern fields, in form order, with their labels. `reason` and the expiry follow. */
export const TEXT_FIELDS = [
  { key: "server_name", label: "Server name (exact)" },
  { key: "metric_name", label: "Metric" },
  /* #5654: limits the rule to alerts one external source raised; empty (the default) leaves it unlimited. */
  { key: "external_source", label: "External source (lower case, e.g. nagios)" },
  { key: "database_pattern", label: "Database (substring)" },
  { key: "wait_type_pattern", label: "Wait type (substring)" },
  { key: "query_text_pattern", label: "Query text (substring)" },
  { key: "job_name_pattern", label: "Job name (substring)" },
  { key: "client_app_pattern", label: "Client application (substring)" },
  { key: "reason", label: "Reason" },
];
const EXPIRY = "expires_at_utc";
/* #6231: the optional start; until it passes the rule is stored and listed but mutes nothing (a quiet window set ahead of planned work). */
const STARTS = "starts_at_utc";
/* server_id is not a form field: it arrives from an Alert History pre-fill and rides along in the create body, so
   the rule is keyed on the server's store id (the name is only its label). */
const SERVER_ID = "server_id";
const EDITABLE_KEYS = TEXT_FIELDS.map((f) => f.key).concat([STARTS, EXPIRY, SERVER_ID]);
/* #7040: the schedule's four form fields, named as the write routes name them. The read names the times recur_start_time and
   recur_end_time; the write names them recur_start and recur_end. */
const RECUR_DAYS = "recur_days";
const RECUR_START = "recur_start";
const RECUR_END = "recur_end";
const RECUR_ZONE = "recur_time_zone";
export const SCHEDULE_KEYS = [RECUR_DAYS, RECUR_START, RECUR_END, RECUR_ZONE];
/* #7083: what the rule does to a matching alert. "mute" is the default and is not sent on a create; "digest" holds the alert back from
   paging and records it for the daily digest instead of muting it. */
const ACTION = "action";
export const ACTION_MUTE = "mute";
export const ACTION_DIGEST = "digest";
/** A rule's or form's action as one of the two words; anything else is the default, a mute. */
export function actionOf(x) {
  return x === ACTION_DIGEST ? ACTION_DIGEST : ACTION_MUTE;
}
/** The day names the form offers, Monday first, as the write routes accept them. */
export const DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
/* The only fields a link may fill in: the ones the app's own "Mute this alert" and "Mute similar" links send
   (mute-context.js's mutePrefillParams, alerts.js's muteCell). The reason, the external source, the start and the
   expiry are never taken from a link: they are typed in the form (#6286). */
const PREFILL_KEYS = [
  SERVER_ID, "server_name", "metric_name", "database_pattern", "wait_type_pattern",
  "query_text_pattern", "job_name_pattern", "client_app_pattern",
];

/** A datetime-local value ("2026-08-01T10:30", local clock) to an ISO UTC string, or "" when blank/unparseable. */
export function localInputToIso(v) {
  if (!v) return "";
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? "" : d.toISOString();
}

/** An ISO timestamp to a datetime-local value (local clock), or "". */
export function isoToLocalInput(iso) {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  const p = (n) => String(n).padStart(2, "0");
  return d.getFullYear() + "-" + p(d.getMonth() + 1) + "-" + p(d.getDate()) + "T" + p(d.getHours()) + ":" + p(d.getMinutes());
}

/** The value a form field sends: trimmed text, or null when blank. `values` holds raw strings (expiry as a
 *  datetime-local value). */
function fieldValue(values, key) {
  const raw = values[key] == null ? "" : String(values[key]).trim();
  if (key === EXPIRY || key === STARTS) return localInputToIso(raw) || null;
  if (key === SERVER_ID) return /^-?\d+$/.test(raw) && Number(raw) !== 0 ? Number(raw) : null;
  return raw === "" ? null : raw;
}

/** The browser's own IANA time zone, the default for a new schedule. "" when the browser does not say. */
export function browserZone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || "";
  } catch {
    return "";
  }
}

/** A day name from the form or the read to its three-letter lower-case key ("Monday" and "mon" both give "mon"). */
function dayKey(name) {
  return String(name == null ? "" : name).trim().slice(0, 3).toLowerCase();
}

/** The schedule a form holds, as the write routes take it, or null when the form has none. A schedule needs the times: days with
 *  no time are kept so the server can refuse them with its own message. The zone rides only with times, since the form starts
 *  with the browser's zone filled in and that must not turn a one-off rule into a scheduled one. */
export function scheduleOf(values) {
  const start = values[RECUR_START] == null ? "" : String(values[RECUR_START]).trim();
  const end = values[RECUR_END] == null ? "" : String(values[RECUR_END]).trim();
  const days = Array.isArray(values[RECUR_DAYS]) ? values[RECUR_DAYS].filter((d) => DAY_NAMES.includes(d)) : [];
  if (!start && !end && days.length === 0) return null;
  const zone = values[RECUR_ZONE] == null ? "" : String(values[RECUR_ZONE]).trim();
  return {
    [RECUR_DAYS]: days.length ? days : null,
    [RECUR_START]: start || null,
    [RECUR_END]: end || null,
    [RECUR_ZONE]: start || end ? zone || null : null,
  };
}

/** The stored rule's schedule in the same shape as scheduleOf, or null for a one-off rule. */
function storedSchedule(rule) {
  if (!rule || (rule.recur_start_time == null && rule.recur_end_time == null)) return null;
  return {
    [RECUR_DAYS]: Array.isArray(rule.recur_days) && rule.recur_days.length ? rule.recur_days : null,
    [RECUR_START]: rule.recur_start_time || null,
    [RECUR_END]: rule.recur_end_time || null,
    [RECUR_ZONE]: rule.recur_time_zone || null,
  };
}

function sameSchedule(a, b) {
  if (a === null || b === null) return a === b;
  const days = (x) => (x[RECUR_DAYS] ? x[RECUR_DAYS].map(dayKey).sort().join(",") : "");
  /* A stored zone the person did not touch is the same schedule; an empty zone on the form means "the default", not a change. */
  return days(a) === days(b) && a[RECUR_START] === b[RECUR_START] && a[RECUR_END] === b[RECUR_END]
    && (a[RECUR_ZONE] || "") === (b[RECUR_ZONE] || "");
}

/** The POST body: only the fields that hold a value. A blank form gives {} — see isEmptyCreate. */
export function formToCreateBody(values) {
  const body = {};
  for (const key of EDITABLE_KEYS) {
    const v = fieldValue(values, key);
    if (v !== null) body[key] = v;
  }
  const schedule = scheduleOf(values);
  if (schedule) {
    for (const key of SCHEDULE_KEYS) if (schedule[key] !== null) body[key] = schedule[key];
  }
  if (actionOf(values[ACTION]) === ACTION_DIGEST) body[ACTION] = ACTION_DIGEST;
  return body;
}

/** True for a create body that names no scope at all: such a rule mutes EVERY alert, so the page confirms first. */
export function isEmptyCreate(body) {
  return !body || Object.keys(body).filter((k) => k !== "reason" && k !== EXPIRY && k !== STARTS && k !== ACTION && !SCHEDULE_KEYS.includes(k)).length === 0;
}

export const EMPTY_CREATE_WARNING =
  "No server, metric or pattern is set. This rule will mute EVERY alert on EVERY server until it is paused, " +
  "deleted or expires. Create it anyway?";

/** The PATCH body: ONLY the fields whose value differs from the stored rule. A field the user blanked is sent
 *  as an explicit null (the server clears it); an untouched field is not sent. `enabled` is never part of it —
 *  that goes through the PUT .../enabled route. Returns {} when nothing changed. */
export function buildPatch(original, values) {
  const patch = {};
  for (const key of EDITABLE_KEYS) {
    if (key === SERVER_ID) continue; /* never edited on the form, so an edit must not clear it */
    const next = fieldValue(values, key);
    const old = original[key] == null || original[key] === "" ? null : original[key];
    const same = (key === EXPIRY || key === STARTS) && next !== null && old !== null
      ? new Date(next).getTime() === new Date(old).getTime()
      : next === old;
    if (!same) patch[key] = next;
  }
  /* #7040: the schedule is one unit. When any part changed, all four are sent, so the times always go together; clearing it
     sends all four as null. */
  const nextSchedule = scheduleOf(values);
  if (!sameSchedule(nextSchedule, storedSchedule(original))) {
    for (const key of SCHEDULE_KEYS) patch[key] = nextSchedule ? nextSchedule[key] : null;
  }
  if (actionOf(values[ACTION]) !== actionOf(original[ACTION])) patch[ACTION] = actionOf(values[ACTION]);
  return patch;
}

/** Turns a write response ({status, body}) into what the page acts on. */
export function interpretWrite(status, body) {
  const b = body && typeof body === "object" ? body : {};
  const message = typeof b.message === "string" ? b.message : typeof b.error === "string" ? b.error : "Request failed (HTTP " + status + ")";
  if (status === 401) return { kind: "expired", message: "Your session has expired. Sign in again." };
  if (status === 403) return { kind: "readonly", message: "This session is read-only, so the change was not made." };
  if (status === 404) return { kind: "notfound", message };
  if (status === 409 && b.status === "already_exists") return { kind: "exists", existingId: b.rule_id || null, message };
  if (status === 400) {
    const m = /'([a-z_]+)'/.exec(message);
    const field = m && (EDITABLE_KEYS.includes(m[1]) || SCHEDULE_KEYS.includes(m[1])) ? m[1] : null;
    return { kind: "invalid", field, message };
  }
  if (status >= 200 && status < 300) return { kind: "ok", rule: b.mute_rule || null, status: b.status || null };
  return { kind: "error", message };
}

/** Whether a rule's start is still ahead (#6231): the rule exists but does not apply yet. */
export function isUpcoming(rule, now) {
  return !!rule.starts_at_utc && new Date(rule.starts_at_utc).getTime() > (now || Date.now());
}

/** Whether a rule's expiry has passed. */
export function isExpired(rule, now) {
  return !!rule.expires_at_utc && new Date(rule.expires_at_utc).getTime() <= (now || Date.now());
}

/* One write: own fetch rather than apiSend, because a 409 carries the existing rule's id in its body and the
   field name a 400 complains about, which apiSend's error shape drops. */
async function send(method, path, body) {
  let resp;
  try {
    resp = await fetch(path, {
      method,
      headers: storeHeaders({ Accept: "application/json", "Content-Type": "application/json" }),
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    return { kind: "error", message: "Network error: " + (e && e.message ? e.message : String(e)) };
  }
  let parsed = null;
  try {
    parsed = JSON.parse(await resp.text());
  } catch {
    parsed = null;
  }
  let result = interpretWrite(resp.status, parsed);
  if (result.kind === "ok" && (parsed === null || typeof parsed !== "object")) {
    /* A 2xx that is not a JSON body is a sign-in page in front of the API, not a saved rule. */
    result = { kind: "expired", message: "Your session has expired. Sign in again." };
  }
  if (result.kind === "expired") reportSessionExpired(result.message, "/");
  return result;
}

export const createRule = (body) => send("POST", "/api/mute-rules", body);
const patchRule = (id, body) => send("PATCH", "/api/mute-rules/" + encodeURIComponent(id), body);
const setEnabled = (id, enabled) => send("PUT", "/api/mute-rules/" + encodeURIComponent(id) + "/enabled", { enabled });
export const deleteRule = (id) => send("DELETE", "/api/mute-rules/" + encodeURIComponent(id));

/* ─────────────────────────── page ─────────────────────────── */

/* Module scope: survives the 60 s re-render. `live` is the mounted page; `form` is the open form's state. */
let live = null;
let form = null;
let lastPrefillQuery = null;
/* Shown on a create form that was filled in from a link, so the person reads every field before saving. */
export const LINK_FILLED_NOTICE = "This form was filled in from a link. Check every field before saving.";

function parsePrefill(query) {
  const p = new URLSearchParams(query || "");
  const v = {};
  for (const k of PREFILL_KEYS) if (p.get(k)) v[k] = p.get(k);
  return v;
}

export async function renderMuteRules(main, query) {
  if (live && live.main === main && live.headEl.isConnected) {
    await refreshList(live);
    return;
  }
  const session = await api.getSession();
  const canEdit = !!session.can_edit;
  const newBtn = canEdit ? el("button", { class: "btn primary", type: "button", text: "New rule" }) : null;
  const headEl = el("div", { class: "page-head" }, [
    el("h2", { text: "Mute Rules" }),
    el("div", { class: "meta", text: "suppressed alerts are still logged" }),
    el("div", { class: "spacer" }),
    newBtn,
  ]);
  const formBox = el("div", {});
  const listBox = el("div", {});
  mount(main, [headEl, formBox, listBox]);
  live = { main, headEl, formBox, listBox, canEdit, rules: [] };
  if (newBtn) newBtn.addEventListener("click", () => openForm({ mode: "create", values: {} }));

  /* A deep link from Alert History opens the create form pre-filled, once per distinct query. */
  if (canEdit && query && query !== lastPrefillQuery) {
    lastPrefillQuery = query;
    form = null;
    const values = parsePrefill(query);
    openForm({ mode: "create", values, fromLink: true, idName: values[SERVER_ID] ? values.server_name || "" : null });
  } else if (form) {
    drawForm();
  }
  mount(listBox, loadingStrip("Loading mute rules…"));
  await refreshList(live);
}

async function refreshList(state) {
  const res = await readTool("get_mute_rules", { enabled_only: false });
  if (res.kind === "error") return mount(state.listBox, errorStrip(res.message));
  if (res.kind === "empty") {
    state.rules = [];
    return mount(state.listBox, emptyStrip(res.message || "No mute rules are configured."));
  }
  state.rules = (res.data && res.data.mute_rules) || [];
  drawList(state);
}

function drawList(state) {
  if (!state.rules.length) return mount(state.listBox, emptyStrip("No mute rules are configured."));
  const head = ["Status", "Scope", "External source", "Reason", "Schedule", "Starts", "Expires", "Created", state.canEdit ? "" : null]
    .filter((h) => h !== null)
    .map((h) => el("th", { text: h }));
  const rows = state.rules.map((r) => ruleRow(r, state.canEdit));
  mount(state.listBox, el("table", { class: "data mute-rules" }, [
    el("thead", {}, [el("tr", {}, head)]),
    el("tbody", {}, rows),
  ]));
}

function statusOf(r) {
  if (isExpired(r)) return "Expired";
  if (!r.enabled) return "Paused";
  return isUpcoming(r) ? "Upcoming" : "Enabled";
}

/** The Schedule cell: the backend's schedule text and whether a window is open now, or a dash for a one-off rule. */
export function scheduleCell(r) {
  if (!r.schedule) return "—";
  if (r.in_window_now === true) return r.schedule + " (in window now)";
  if (r.in_window_now === false) return r.schedule + " (outside window)";
  return r.schedule;
}

function ruleRow(r, canEdit) {
  const st = statusOf(r);
  const scope = (r.match_summary || "Every alert") + (actionOf(r.action) === ACTION_DIGEST ? " (to the daily digest)" : "");
  const cells = [
    el("td", {}, [el("span", { class: st === "Enabled" ? "status-cell sev-Healthy" : "paused-badge", text: st })]),
    el("td", { text: scope, title: r.server_id != null ? "server_id " + r.server_id : null }),
    el("td", { text: r.external_source || "—" }),
    el("td", { text: r.reason || "—" }),
    el("td", { text: scheduleCell(r), title: r.next_window_start_utc ? "Next window opens " + localTime(r.next_window_start_utc) : null }),
    el("td", { text: r.starts_at_utc ? localTime(r.starts_at_utc) + (isUpcoming(r) ? " (not started)" : "") : "At once" }),
    el("td", { text: r.expires_at_utc ? localTime(r.expires_at_utc) + (isExpired(r) ? " (expired)" : "") : "Never" }),
    el("td", { text: r.created_at_utc ? localTime(r.created_at_utc) : "—" }),
  ];
  if (canEdit) {
    cells.push(el("td", { class: "mute-actions" }, [
      el("button", { class: "btn", type: "button", text: "Edit", onClick: () => openForm({ mode: "edit", id: r.id, original: r, values: valuesOf(r) }) }),
      el("button", { class: "btn", type: "button", text: r.enabled ? "Pause" : "Enable", onClick: () => toggle(r) }),
      el("button", { class: "btn", type: "button", text: "Delete", onClick: () => remove(r) }),
    ]));
  }
  return el("tr", { "data-rule-id": r.id }, cells);
}

function valuesOf(r) {
  const v = {};
  for (const f of TEXT_FIELDS) v[f.key] = r[f.key] || "";
  v[STARTS] = isoToLocalInput(r.starts_at_utc);
  v[EXPIRY] = isoToLocalInput(r.expires_at_utc);
  /* A null recur_days is every day, which the form says as no day ticked. */
  v[RECUR_DAYS] = Array.isArray(r.recur_days) ? r.recur_days.map((d) => DAY_NAMES.find((n) => dayKey(n) === dayKey(d))).filter(Boolean) : [];
  v[RECUR_START] = r.recur_start_time || "";
  v[RECUR_END] = r.recur_end_time || "";
  v[RECUR_ZONE] = r.recur_time_zone || browserZone();
  v[ACTION] = actionOf(r.action);
  return v;
}

function notify(message, isError) {
  if (live) mount(live.formBox, form ? [formNode(), el("div", { class: "strip " + (isError ? "error" : "notice"), text: message })] : [el("div", { class: "strip " + (isError ? "error" : "notice"), text: message })]);
}

async function toggle(r) {
  const res = await setEnabled(r.id, !r.enabled);
  if (res.kind !== "ok") return notify(res.message, true);
  await refreshList(live);
}

async function remove(r) {
  if (!window.confirm("Delete this mute rule permanently?\n\n" + (r.match_summary || "Every alert") + "\n\nPausing keeps the rule; deleting cannot be undone.")) return;
  const res = await deleteRule(r.id);
  if (res.kind !== "ok" && res.kind !== "notfound") return notify(res.message, true);
  await refreshList(live);
}

/* ─────────────────────────── form ─────────────────────────── */

function openForm(f) {
  form = { errors: {}, banner: null, exists: null, confirmEmpty: false, ...f };
  /* A new rule's schedule zone starts as the browser's; it rides along only when times are filled in (see scheduleOf). */
  if (form.values[RECUR_ZONE] == null) form.values[RECUR_ZONE] = browserZone();
  drawForm();
}

function drawForm() {
  if (!live) return;
  mount(live.formBox, form ? formNode() : []);
}

function closeForm() {
  form = null;
  /* Forget the deep link too, so clicking the same alert's Mute link again reopens the form. */
  lastPrefillQuery = null;
  /* The deep link stays in the address bar; drop it so the same Mute link is a real navigation next time. */
  if (typeof window !== "undefined" && window.history && window.history.replaceState && String(window.location && window.location.hash).includes("?")) {
    window.history.replaceState(null, "", storeHash("#/mute-rules"));
  }
  drawForm();
}

function formNode() {
  const fields = TEXT_FIELDS.map((f) => fieldRow(f.key, f.label, "text"));
  fields.push(fieldRow(STARTS, "Starts (local time; blank = at once)", "datetime-local"));
  fields.push(fieldRow(EXPIRY, "Expires (local time; blank = never)", "datetime-local"));
  fields.push(scheduleNode());
  fields.push(actionNode());
  const empty = form.mode === "create" && isEmptyCreate(formToCreateBody(form.values));
  return el("div", { class: "card mute-form" }, [
    el("h3", { text: form.mode === "create" ? "New mute rule" : "Edit mute rule" }),
    el("div", { class: "meta", text: "Alerts matching ALL filled fields are handled by the action below. Blank fields match anything." }),
    form.fromLink ? el("div", { class: "strip notice", role: "note", text: LINK_FILLED_NOTICE }) : null,
    form.banner ? el("div", { class: "strip error", text: form.banner }) : null,
    form.exists ? existsNode() : null,
    ...fields,
    form.confirmEmpty && empty ? el("div", { class: "strip error", text: EMPTY_CREATE_WARNING }) : null,
    el("div", { class: "form-actions" }, [
      el("button", { class: "btn primary", type: "button", text: form.confirmEmpty && empty ? "Create anyway" : "Save", onClick: submit }),
      el("button", { class: "btn", type: "button", text: "Cancel", onClick: closeForm }),
    ]),
  ]);
}

function existsNode() {
  return el("div", { class: "strip notice" }, [
    el("span", { text: form.exists.message + " " }),
    form.exists.id ? el("button", { class: "btn", type: "button", text: "Go to that rule", onClick: goToExisting }) : null,
  ]);
}

function goToExisting() {
  const id = form.exists.id;
  closeForm();
  const row = live && live.listBox.querySelector('[data-rule-id="' + id + '"]');
  if (row) {
    row.classList.add("mute-rules-highlight");
    if (row.scrollIntoView) row.scrollIntoView({ block: "center" });
  }
}

/** #7083: what the rule does with a matching alert: mute it, or hold it back from paging and send it to the daily digest. */
function actionNode() {
  const select = el("select", { class: "mute-input", "aria-label": "Action", "data-field": ACTION }, [
    el("option", { value: ACTION_MUTE, text: "Mute" }),
    el("option", { value: ACTION_DIGEST, text: "Send to the daily digest" }),
  ]);
  select.value = actionOf(form.values[ACTION]);
  select.addEventListener("change", () => {
    form.values[ACTION] = select.value;
    form.confirmEmpty = false;
  });
  return el("label", { class: "mute-field" }, [
    el("span", { class: "mute-label", text: "Action" }),
    select,
    el("span", { class: "meta", text: "Mute records a matching alert as muted and sends nothing. The daily digest holds it back from paging, records it, and lists it in the daily digest. Pair the digest action with a repeating window." }),
  ]);
}

/** The optional repeating schedule: day boxes, start and end time, and the zone. All four empty is a one-off rule. */
function scheduleNode() {
  const days = Array.isArray(form.values[RECUR_DAYS]) ? form.values[RECUR_DAYS] : (form.values[RECUR_DAYS] = []);
  const boxes = DAY_NAMES.map((d) => {
    const box = el("input", { type: "checkbox", "data-day": d, "aria-label": d });
    box.checked = days.includes(d);
    box.addEventListener("change", () => {
      const now = form.values[RECUR_DAYS].filter((x) => x !== d);
      if (box.checked) now.push(d);
      form.values[RECUR_DAYS] = DAY_NAMES.filter((x) => now.includes(x));
      form.confirmEmpty = false;
    });
    return el("label", { class: "mute-day" }, [box, el("span", { text: " " + d })]);
  });
  const zoneRow = fieldRow(RECUR_ZONE, "Time zone (IANA id, e.g. America/New_York)", "text");
  const zoneInput = zoneRow.querySelector("input");
  if (typeof Intl !== "undefined" && typeof Intl.supportedValuesOf === "function") {
    try {
      const list = el("datalist", { id: "mute-zones" }, Intl.supportedValuesOf("timeZone").map((z) => el("option", { value: z })));
      zoneInput.setAttribute("list", "mute-zones");
      zoneRow.appendChild(list);
    } catch {
      /* The browser does not list its zones; the box still takes a typed id. */
    }
  }
  return el("fieldset", { class: "mute-schedule" }, [
    el("legend", { text: "Repeats (optional)" }),
    el("div", { class: "meta", text: "Mute only inside a daily window. Leave the times blank for a one-off rule. With no day ticked, every day counts. A window that ends before it starts crosses midnight and belongs to the day it starts." }),
    el("div", { class: "mute-days" }, boxes),
    form.errors[RECUR_DAYS] ? el("span", { class: "mute-rules-field-error", text: form.errors[RECUR_DAYS] }) : null,
    fieldRow(RECUR_START, "From (daily start, HH:mm)", "time"),
    fieldRow(RECUR_END, "To (daily end, HH:mm)", "time"),
    zoneRow,
  ]);
}

function fieldRow(key, label, type) {
  const input = el("input", { type, class: "mute-input", value: form.values[key] || "", "aria-label": label, "data-field": key });
  input.addEventListener("input", () => {
    form.values[key] = input.value;
    form.confirmEmpty = false;
  });
  return el("label", { class: "mute-field" }, [
    el("span", { class: "mute-label", text: label }),
    input,
    form.errors[key] ? el("span", { class: "mute-rules-field-error", text: form.errors[key] }) : null,
  ]);
}

async function submit() {
  form.errors = {};
  form.banner = null;
  form.exists = null;
  let res;
  if (form.mode === "create") {
    const body = formToCreateBody(form.values);
    /* The id keys the rule only while the name is the one the row carried; an edited name is a deliberate by-name rule. */
    if (body[SERVER_ID] !== undefined && (form.values.server_name || "").trim() !== (form.idName || "")) delete body[SERVER_ID];
    if (isEmptyCreate(body) && !form.confirmEmpty) {
      form.confirmEmpty = true;
      return drawForm();
    }
    res = await createRule(body);
  } else {
    const patch = buildPatch(form.original, form.values);
    if (Object.keys(patch).length === 0) return closeForm();
    res = await patchRule(form.id, patch);
  }
  if (res.kind === "ok") {
    closeForm();
    return refreshList(live);
  }
  if (res.kind === "invalid" && res.field) form.errors[res.field] = res.message;
  else if (res.kind === "exists") form.exists = { id: res.existingId, message: res.message };
  else form.banner = res.message;
  drawForm();
}
