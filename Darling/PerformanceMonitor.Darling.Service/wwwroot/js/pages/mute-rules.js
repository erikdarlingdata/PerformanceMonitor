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
 * Polling: app.js re-calls renderMuteRules on its 60 s tick. The form, its typed values and its focus live in
 * module scope (`live`, `form`): a tick re-reads and redraws the LIST only and never touches an open form.
 *
 * The logic that decides what is sent (formToCreateBody, buildPatch, isEmptyCreate, interpretWrite) is plain
 * functions with no DOM, run under Node by MuteRulesBehaviourTests. All user text reaches the DOM through
 * el()/textContent.
 */

import { el, mount, readTool, loadingStrip, errorStrip, emptyStrip, localTime, reportSessionExpired } from "../util.js";
import * as api from "../alerts-api.js";

/** The editable text scope/pattern fields, in form order, with their labels. `reason` and the expiry follow. */
export const TEXT_FIELDS = [
  { key: "server_name", label: "Server name (exact)" },
  { key: "metric_name", label: "Metric" },
  { key: "database_pattern", label: "Database (substring)" },
  { key: "wait_type_pattern", label: "Wait type (substring)" },
  { key: "query_text_pattern", label: "Query text (substring)" },
  { key: "job_name_pattern", label: "Job name (substring)" },
  { key: "reason", label: "Reason" },
];
const EXPIRY = "expires_at_utc";
/* server_id is not a form field: it arrives from an Alert History pre-fill and rides along in the create body, so
   the rule is keyed on the server's store id (the name is only its label). */
const SERVER_ID = "server_id";
const EDITABLE_KEYS = TEXT_FIELDS.map((f) => f.key).concat([EXPIRY, SERVER_ID]);

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
  if (key === EXPIRY) return localInputToIso(raw) || null;
  if (key === SERVER_ID) return /^-?\d+$/.test(raw) && Number(raw) !== 0 ? Number(raw) : null;
  return raw === "" ? null : raw;
}

/** The POST body: only the fields that hold a value. A blank form gives {} — see isEmptyCreate. */
export function formToCreateBody(values) {
  const body = {};
  for (const key of EDITABLE_KEYS) {
    const v = fieldValue(values, key);
    if (v !== null) body[key] = v;
  }
  return body;
}

/** True for a create body that names no scope at all: such a rule mutes EVERY alert, so the page confirms first. */
export function isEmptyCreate(body) {
  return !body || Object.keys(body).filter((k) => k !== "reason" && k !== EXPIRY).length === 0;
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
    const same = key === EXPIRY && next !== null && old !== null
      ? new Date(next).getTime() === new Date(old).getTime()
      : next === old;
    if (!same) patch[key] = next;
  }
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
    const field = m && EDITABLE_KEYS.includes(m[1]) ? m[1] : null;
    return { kind: "invalid", field, message };
  }
  if (status >= 200 && status < 300) return { kind: "ok", rule: b.mute_rule || null, status: b.status || null };
  return { kind: "error", message };
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
      headers: { Accept: "application/json", "Content-Type": "application/json" },
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

const createRule = (body) => send("POST", "/api/mute-rules", body);
const patchRule = (id, body) => send("PATCH", "/api/mute-rules/" + encodeURIComponent(id), body);
const setEnabled = (id, enabled) => send("PUT", "/api/mute-rules/" + encodeURIComponent(id) + "/enabled", { enabled });
const deleteRule = (id) => send("DELETE", "/api/mute-rules/" + encodeURIComponent(id));

/* ─────────────────────────── page ─────────────────────────── */

/* Module scope: survives the 60 s re-render. `live` is the mounted page; `form` is the open form's state. */
let live = null;
let form = null;
let lastPrefillQuery = null;

function parsePrefill(query) {
  const p = new URLSearchParams(query || "");
  const v = {};
  for (const k of EDITABLE_KEYS) if (p.get(k)) v[k] = p.get(k);
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
    openForm({ mode: "create", values, idName: values[SERVER_ID] ? values.server_name || "" : null });
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
  const head = ["Status", "Scope", "Reason", "Expires", "Created", state.canEdit ? "" : null]
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
  return r.enabled ? "Enabled" : "Paused";
}

function ruleRow(r, canEdit) {
  const st = statusOf(r);
  const scope = r.summary || "Every alert";
  const cells = [
    el("td", {}, [el("span", { class: st === "Enabled" ? "status-cell sev-Healthy" : "paused-badge", text: st })]),
    el("td", { text: scope, title: r.server_id != null ? "server_id " + r.server_id : null }),
    el("td", { text: r.reason || "—" }),
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
  v[EXPIRY] = isoToLocalInput(r.expires_at_utc);
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
  if (!window.confirm("Delete this mute rule permanently?\n\n" + (r.summary || "Every alert") + "\n\nPausing keeps the rule; deleting cannot be undone.")) return;
  const res = await deleteRule(r.id);
  if (res.kind !== "ok" && res.kind !== "notfound") return notify(res.message, true);
  await refreshList(live);
}

/* ─────────────────────────── form ─────────────────────────── */

function openForm(f) {
  form = { errors: {}, banner: null, exists: null, confirmEmpty: false, ...f };
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
    window.history.replaceState(null, "", "#/mute-rules");
  }
  drawForm();
}

function formNode() {
  const fields = TEXT_FIELDS.map((f) => fieldRow(f.key, f.label, "text"));
  fields.push(fieldRow(EXPIRY, "Expires (local time; blank = never)", "datetime-local"));
  const empty = form.mode === "create" && isEmptyCreate(formToCreateBody(form.values));
  return el("div", { class: "card mute-form" }, [
    el("h3", { text: form.mode === "create" ? "New mute rule" : "Edit mute rule" }),
    el("div", { class: "meta", text: "Alerts matching ALL filled fields are suppressed. Blank fields match anything." }),
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
