/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Manage Query Tags page (#5986) — the web twin of the query-tag MCP tools, modelled on manage-tags.js. It reads the tag
 * tree with each tag's hand assignments from list_query_tags (/api/read) and the rules, in the order they apply, from
 * GET /api/query-tag-rules. For a sign-in the session reports can_edit it writes through /api/query-tags (POST create,
 * PATCH edit, DELETE ?confirm=true), /api/query-tags/{id}/assignments (POST assign, DELETE unassign) and
 * /api/query-tag-rules (POST, PATCH, DELETE). The Test button runs GET /api/query-tag-rules/test on the draft and shows the
 * counts and samples before anything is saved. The server is the authority for every write and every figure; this page
 * only builds requests and shows the answers.
 *
 * A delete that answers 409 confirm_required shows the counts the server named and sends again with confirm=true only when
 * the user asks again. A hand assignment beats any rule, and a write reaches the tag map on the collector's next pass: the
 * page shows the map's `current` flag and rules_version / built_version as the server sent them.
 *
 * Polling: app.js re-calls renderManageQueryTags on its 60 s tick. The selected tag, the open form and what was typed in it
 * live in module scope (`selectedId`, `form`), so a tick re-reads and repaints the tree, the assignments and the rule list
 * without rebuilding the open form's inputs: focus, caret and unsaved edits stay. Every write handler is guarded by `busy`,
 * so a double click sends one request. All server text reaches the DOM through el()/textContent.
 *
 * Servers (#5971): a rule or an assignment names its server by id, and list_servers rows carry no id, so the names come from
 * the fleet read's cards (server_id, display_name, server_name), the read the sidebar already makes each tick. A server-scoped
 * rule shows its server's display name, then its server name, then "server N" for an id the fleet no longer lists (a removed
 * server). The rule list has a server filter: "Every server" asks GET /api/query-tag-rules with no server_id, one server sends
 * ?server_id=N and shows what the route answered (that server's rules plus the every-server rules). The filter is module scope,
 * so the 60 s repaint keeps it. The rule and assignment forms pick a server from the same list; when the fleet read failed
 * they keep the typed id, so a write is never blocked by a failed name lookup.
 */

import { el, mount, apiGet, apiGetFleet, apiWrite, loadingStrip, errorStrip, emptyStrip } from "../util.js";
import { orderServers } from "../server-order.js";
import { getSession } from "../views-api.js";
import { PALETTE, safeColour, treeRows, subtreeIds, formToCreateBody, buildPatch } from "./manage-tags.js";

export { treeRows, subtreeIds, formToCreateBody, buildPatch };

/** The rule fields the server accepts. */
export const RULE_FIELDS = ["application", "login", "host", "database", "object", "text", "text_tag"];

/** The query string of a rule test: only the fields that hold a value. A text_tag rule sends no pattern and no tag. */
export function testQuery(values, serverId, windowHours) {
  const q = new URLSearchParams();
  q.set("field", values.field);
  if (values.field !== "text_tag") {
    q.set("pattern", String(values.pattern || ""));
    if (values.tag_id !== "" && values.tag_id != null) q.set("tag_id", String(values.tag_id));
  }
  if (values.priority !== "" && values.priority != null) q.set("priority", String(values.priority));
  if (serverId !== "" && serverId != null) q.set("server_id", String(serverId));
  if (windowHours !== "" && windowHours != null) q.set("window_hours", String(windowHours));
  return q.toString();
}

/** The POST body for a rule create: only the fields that hold a value. */
export function ruleToCreateBody(values) {
  const body = { field: values.field };
  if (values.field !== "text_tag") {
    body.pattern = String(values.pattern || "");
    if (values.tag_id !== "" && values.tag_id != null) body.tag_id = Number(values.tag_id);
  }
  if (values.priority !== "" && values.priority != null) body.priority = Number(values.priority);
  body.enabled = !!values.enabled;
  if (values.server_id !== "" && values.server_id != null) body.server_id = Number(values.server_id);
  return body;
}

/** The PATCH body for a rule: ONLY the fields that differ from the stored rule. A blank server is an explicit null. */
export function ruleToPatch(original, values) {
  const patch = {};
  if (original.field !== "text_tag") {
    if (String(values.pattern || "") !== original.pattern) patch.pattern = String(values.pattern || "");
    const tag = values.tag_id === "" || values.tag_id == null ? null : Number(values.tag_id);
    if (tag !== (original.tag_id == null ? null : original.tag_id)) patch.tag_id = tag;
  }
  if (values.priority !== "" && Number(values.priority) !== original.priority) patch.priority = Number(values.priority);
  if (!!values.enabled !== !!original.enabled) patch.enabled = !!values.enabled;
  const server = values.server_id === "" || values.server_id == null ? null : Number(values.server_id);
  if (server !== (original.server_id == null ? null : original.server_id)) patch.server_id = server;
  return patch;
}

/** The body of an assign or unassign from the assignment form values: only the key parts its kind takes. */
export function assignmentBody(values, withQueryId) {
  const body = { kind: values.kind };
  if (values.kind === "hash") body.query_hash = String(values.query_hash || "").trim();
  else {
    body.database_name = String(values.database_name || "").trim();
    body.schema_name = String(values.schema_name || "").trim();
    body.object_name = String(values.object_name || "").trim();
  }
  if (values.server_id !== "" && values.server_id != null) body.server_id = Number(values.server_id);
  if (withQueryId && values.query_id !== "" && values.query_id != null) body.query_id = Number(values.query_id);
  return body;
}

/** The unassign body that names an existing assignment as the server listed it. */
export function unassignBody(a) {
  const body = { kind: a.kind };
  if (a.kind === "hash") body.query_hash = a.query_hash;
  else {
    body.database_name = a.database_name;
    body.schema_name = a.schema_name;
    body.object_name = a.object_name;
  }
  if (a.server_id != null) body.server_id = a.server_id;
  return body;
}

/** A readable key for an assignment row. */
export function keyText(a) {
  return a.kind === "hash" ? a.query_hash : [a.database_name, a.schema_name, a.object_name].join(".");
}

/** Turns a write response ({status, body}) into what the page acts on. Branches on the HTTP status and the envelope's
 *  status word, never on message text. */
export function interpretWrite(status, body) {
  const b = body && typeof body === "object" ? body : {};
  const message = typeof b.message === "string" ? b.message : typeof b.error === "string" ? b.error : "Request failed (HTTP " + status + ")";
  if (status === 401) return { kind: "expired", message: "Your session has expired. Sign in again." };
  if (status === 403) return { kind: "readonly", message: "This session is read-only, so the change was not made." };
  if (status === 404) return { kind: "notfound", message, refusal: typeof b.refusal === "string" ? b.refusal : null };
  if (status === 409 && b.status === "confirm_required") return { kind: "confirm", message, body: b };
  if (status === 409) return { kind: "conflict", message };
  if (status === 400) return { kind: "invalid", message };
  if (status >= 200 && status < 300) return { kind: "ok", body: b, status: b.status || null };
  return { kind: "error", message };
}

async function send(method, path, body) {
  const res = await apiWrite(method, path, body);
  if (res.status === 0) return { kind: "error", message: res.message };
  if (res.expired) return { kind: "expired", message: "Your session has expired. Sign in again." };
  if (res.unexpected) return { kind: "error", message: "The service gave an unexpected answer (HTTP " + res.status + "). Check the list before trying again." };
  return interpretWrite(res.status, res.body);
}

const tagPath = (id) => "/api/query-tags/" + encodeURIComponent(id);
const rulePath = (id) => "/api/query-tag-rules/" + encodeURIComponent(id);

/* ─────────────────────────── page ─────────────────────────── */

/* Module scope: survives the 60 s re-render. */
let live = null;
let selectedId = null;
let form = null;
let notice = null;
let pendingDelete = null;
/* True while a write is in flight: a second click on any write control does nothing until it settles. */
let busy = false;
/* The rule list's server filter (#5971): "" is every server, otherwise a server id as text. Module scope, so the 60 s repaint keeps it. */
let ruleServerFilter = "";
const lockable = new Set();

function lockButton(props) {
  const b = el("button", props);
  b.disabled = busy;
  lockable.add(b);
  return b;
}

function setBusy(value) {
  busy = value;
  for (const b of [...lockable]) {
    if (b.isConnected === false) lockable.delete(b);
    else b.disabled = value;
  }
}

export async function renderManageQueryTags(main) {
  if (live && live.main === main && live.headEl.isConnected) {
    await refresh(live);
    return;
  }
  const session = await getSession();
  const canEdit = !!session.can_edit;
  const newRoot = canEdit ? lockButton({ class: "btn primary", type: "button", text: "New root tag", onClick: () => openForm(tagFormFor("create", null, "")) }) : null;
  const newRule = canEdit ? lockButton({ class: "btn", type: "button", text: "New rule", onClick: () => openForm(ruleFormFor(null)) }) : null;
  const headEl = el("div", { class: "page-head" }, [
    el("h2", { text: "Manage Query Tags" }),
    el("div", { class: "meta", text: canEdit ? "a hand assignment beats any rule" : "read-only sign-in: query tags can be viewed, not changed" }),
    el("div", { class: "spacer" }),
    newRoot,
    newRule,
  ]);
  const noticeBox = el("div", {});
  const formBox = el("div", {});
  const body = el("div", {});
  mount(main, [headEl, noticeBox, formBox, body]);
  live = { main, headEl, noticeBox, formBox, body, canEdit, tags: [], rules: [], map: null, rulesError: null, servers: [], filterSel: null };
  if (form) mount(formBox, formNode());
  mount(body, loadingStrip("Loading query tags…"));
  await refresh(live);
}

/* The list read answers {status:"ok"|"empty", ...}; an "empty" status that carries a message arrives as kind "empty" with the body in `data`. */
const bodyOf = (res) => (res.kind === "data" || res.kind === "empty" ? res.data || {} : null);

async function refresh(state) {
  const filter = ruleServerFilter;
  const [tagRes, ruleRes] = await Promise.all([
    apiGet("/api/read/list_query_tags"),
    apiGet("/api/query-tag-rules" + (filter === "" ? "" : "?server_id=" + encodeURIComponent(filter))),
    loadServers(state),
  ]);
  const t = bodyOf(tagRes);
  if (!t) return mount(state.body, errorStrip(tagRes.message || "The query tags could not be read."));
  state.tags = (Array.isArray(t.tags) ? t.tags : []).map(tagRow);
  state.map = t.map || null;
  const r = bodyOf(ruleRes);
  state.rulesError = r ? null : ruleRes.message || "The rules could not be read.";
  if (r) state.rules = Array.isArray(r.rules) ? r.rules : [];
  state.rulesFilter = filter;
  if (selectedId != null && !state.tags.some((x) => x.id === selectedId)) selectedId = null;
  draw(state);
  refreshOptions(state);
}

/* The fleet's servers, in the sidebar's order, as { id, label }. Read once per page load: a failed read is tried again on the next
   tick, and until it lands the page names every server "server N" and the forms keep their typed id. */
async function loadServers(state) {
  if (state.serversLoaded) return;
  const res = await apiGetFleet();
  if (res.kind !== "data" || !res.data) return;
  const cards = Array.isArray(res.data.cards) ? res.data.cards : [];
  state.servers = orderServers(cards.filter((c) => c && Number.isInteger(c.server_id)))
    .map((c) => ({ id: c.server_id, label: c.display_name || c.server_name || "server " + c.server_id }));
  state.serversLoaded = true;
}

/** A server's name for the page: its display name or server name, "server N" when the fleet does not list it. */
function serverLabel(state, id) {
  const known = state.servers.find((s) => s.id === Number(id));
  return known ? known.label : "server " + id;
}

/** The options of a server pick list: the blank choice first, then every server; a value the fleet no longer lists stays selectable. */
function serverOptions(state, blankLabel, current) {
  const opts = [{ value: "", label: blankLabel }, ...state.servers.map((s) => ({ value: String(s.id), label: s.label }))];
  if (current !== "" && current != null && !opts.some((o) => o.value === String(current))) opts.push({ value: String(current), label: serverLabel(state, current) });
  return opts;
}

/* list_query_tags names the id tag_id; manage-tags.js's tree helpers read id. */
function tagRow(t) {
  return { id: t.tag_id, name: t.name, parent_id: t.parent_id == null ? null : t.parent_id, colour: t.colour, path: t.path, assignments: t.assignments || [], rule_count: t.rule_count || 0, assignment_count: t.assignment_count || 0 };
}

/** Rebuilds only the open form's tag options, in place, so the typed fields keep their element and focus. */
function refreshOptions(state) {
  if (!form || !form.tagSelect) return;
  const opts = form.kind === "tag" ? parentOptions(state) : ruleTagOptions(state);
  if (!opts.some((o) => o.value === form.values[form.tagField])) form.values[form.tagField] = "";
  mount(form.tagSelect, opts.map((o) => el("option", { value: o.value, text: o.label })));
  form.tagSelect.value = form.values[form.tagField];
}

function draw(state) {
  drawNotice(state);
  const mapLine = state.map
    ? el("div", { class: "meta", text: state.map.current ? "The tag map is current." : "The tag map is behind (rules_version " + state.map.rules_version + ", built_version " + state.map.built_version + "): it catches up on the collector's next pass." })
    : null;
  const tagsPart = state.tags.length
    ? el("div", { class: "tags-layout" }, [treeNode(state), el("div", { class: "tags-detail" }, [detailNode(state)])])
    : emptyStrip("No query tags are defined." + (state.canEdit ? " Use New root tag to create one." : ""));
  mount(state.body, [mapLine, tagsPart, rulesNode(state)]);
}

function drawNotice(state) {
  const nodes = [];
  if (notice) nodes.push(el("div", { class: "strip " + (notice.isError ? "error" : "notice"), role: notice.isError ? "alert" : "status", text: notice.message }));
  if (pendingDelete) {
    const b = pendingDelete.body || {};
    nodes.push(el("div", { class: "strip error" }, [
      el("div", { text: pendingDelete.message }),
      el("div", { class: "meta", text: (b.child_tags || 0) + " child tag(s), " + (b.assignments || 0) + " hand assignment(s), " + (b.rules || 0) + " rule(s) would go with it." }),
      el("div", { class: "form-actions" }, [
        el("button", { class: "btn primary", type: "button", text: "Delete anyway", onClick: confirmDelete }),
        lockButton({ class: "btn", type: "button", text: "Cancel", onClick: cancelDelete }),
      ]),
    ]));
  }
  mount(state.noticeBox, nodes);
}

function swatch(colour) {
  const c = safeColour(colour);
  return el("span", { class: "tag-swatch", style: c ? "background:" + c : null, "aria-hidden": "true" });
}

function treeNode(state) {
  return el("ul", { class: "tag-tree" }, treeRows(state.tags).map(({ tag, depth }) =>
    el("li", { style: "padding-left:" + depth * 18 + "px" }, [
      el("button", {
        class: "tag-node" + (tag.id === selectedId ? " tag-selected" : ""),
        type: "button",
        "data-tag-id": tag.id,
        "aria-current": tag.id === selectedId ? "true" : null,
        onClick: () => select(tag.id),
      }, [swatch(tag.colour), el("span", { text: tag.name + " (" + tag.assignment_count + " hand, " + tag.rule_count + " rules)" })]),
    ])));
}

function select(id) {
  selectedId = id;
  if (live) draw(live);
}

function detailNode(state) {
  const tag = state.tags.find((t) => t.id === selectedId);
  if (!tag) return emptyStrip("Select a tag to see and change its hand assignments.");
  const actions = state.canEdit
    ? el("div", { class: "form-actions" }, [
        lockButton({ class: "btn", type: "button", text: "New child tag", onClick: () => openForm(tagFormFor("create", null, String(tag.id))) }),
        lockButton({ class: "btn", type: "button", text: "Edit", onClick: () => openForm(tagFormFor("edit", tag, tag.parent_id == null ? "" : String(tag.parent_id))) }),
        lockButton({ class: "btn", type: "button", text: "Assign a query", onClick: () => openForm(assignFormFor(tag)) }),
        el("button", { class: "btn", type: "button", text: "Delete", onClick: () => remove(tag) }),
      ])
    : null;
  const rows = tag.assignments.map((a) => el("li", { "data-assignment-id": a.assignment_id }, [
    el("span", { text: a.kind + " " + keyText(a) + " — " + (a.server_name || (a.server_id == null ? "every server" : serverLabel(state, a.server_id))) }),
    state.canEdit ? lockButton({ class: "btn", type: "button", text: "Remove", "aria-label": "Remove assignment " + keyText(a), onClick: () => unassign(tag, a) }) : null,
  ]));
  return el("div", {}, [
    el("h3", { text: tag.path || tag.name }),
    actions,
    rows.length ? el("ul", { class: "tag-assignments" }, rows) : el("div", { class: "meta", text: "No hand assignments." }),
  ]);
}

/* ─────────────────────────── rules ─────────────────────────── */

/* The filter select is built once and re-used by every repaint, so its handler and chosen value outlive a tick. */
function filterNode(state) {
  if (!state.filterSel) {
    state.filterSel = el("select", { class: "tag-input", "aria-label": "Show rules for server", "data-field": "rule_server_filter" });
    state.filterSel.addEventListener("change", () => {
      ruleServerFilter = state.filterSel.value;
      refresh(state);
    });
  }
  mount(state.filterSel, serverOptions(state, "Every server", ruleServerFilter).map((o) => el("option", { value: o.value, text: o.label })));
  state.filterSel.value = ruleServerFilter;
  return el("div", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Server" }), state.filterSel]);
}

function rulesNode(state) {
  const head = el("h3", { text: "Rules (in the order they apply: priority, then id)" });
  const filter = filterNode(state);
  if (state.rulesError) return el("div", { class: "tags-rules" }, [head, filter, errorStrip(state.rulesError)]);
  if (!state.rules.length) {
    const none = state.rulesFilter ? "No rules apply to " + serverLabel(state, state.rulesFilter) + "." : "No rules are defined." + (state.canEdit ? " Use New rule to create one." : "");
    return el("div", { class: "tags-rules" }, [head, filter, emptyStrip(none)]);
  }
  const rows = state.rules.map((r) => el("li", { "data-rule-id": r.id }, [
    el("span", { text: "#" + r.priority + " " + r.field + (r.field === "text_tag" ? "" : " ~ " + r.pattern) + " → " + (r.tag || (r.field === "text_tag" ? "tag named in the comment" : "(no tag)")) + (r.server_id == null ? "" : " (" + serverLabel(state, r.server_id) + ")") + (r.enabled ? "" : " [disabled]") }),
    state.canEdit ? lockButton({ class: "btn", type: "button", text: "Edit rule", "aria-label": "Edit rule " + r.id, onClick: () => openForm(ruleFormFor(r)) }) : null,
    state.canEdit ? lockButton({ class: "btn", type: "button", text: "Delete rule", "aria-label": "Delete rule " + r.id, onClick: () => removeRule(r) }) : null,
  ]));
  return el("div", { class: "tags-rules" }, [head, filter, el("ul", { class: "rule-list" }, rows)]);
}

/* ─────────────────────────── writes ─────────────────────────── */

function setNotice(message, isError) {
  notice = { message, isError };
  if (live) drawNotice(live);
}

async function afterWrite(message) {
  notice = { message, isError: false };
  await refresh(live);
}

async function remove(tag) {
  if (busy) return;
  if (!window.confirm("Delete tag \"" + tag.name + "\" and every tag under it?\n\nIts hand assignments and rules go with it. This cannot be undone.")) return;
  setBusy(true);
  try {
    notice = null;
    pendingDelete = null;
    await finishDelete(tag.id, await send("DELETE", tagPath(tag.id)));
  } finally {
    setBusy(false);
  }
}

async function confirmDelete() {
  if (busy || !pendingDelete) return;
  const pending = pendingDelete;
  setBusy(true);
  try {
    const res = await send("DELETE", tagPath(pending.id) + "?confirm=true");
    if (pendingDelete !== pending) return;
    await finishDelete(pending.id, res);
  } finally {
    setBusy(false);
  }
}

function cancelDelete() {
  pendingDelete = null;
  if (live) drawNotice(live);
}

async function finishDelete(id, res) {
  if (res.kind === "confirm") {
    pendingDelete = { id, message: res.message, body: res.body };
    return drawNotice(live);
  }
  pendingDelete = null;
  if (res.kind !== "ok" && res.kind !== "notfound") return setNotice(res.message, true);
  const gone = res.kind === "ok" && Array.isArray(res.body.deleted_tag_ids) ? res.body.deleted_tag_ids : [id];
  if (gone.includes(selectedId)) selectedId = null;
  await afterWrite(res.kind === "ok" ? "Tag deleted." : "That tag was already gone.");
}

async function unassign(tag, a) {
  if (busy) return;
  setBusy(true);
  try {
    const res = await send("DELETE", tagPath(tag.id) + "/assignments", unassignBody(a));
    if (res.kind !== "ok") return setNotice(res.message, true);
    await afterWrite(res.status === "unchanged" ? "That assignment was already gone." : "Assignment removed.");
  } finally {
    setBusy(false);
  }
}

async function removeRule(rule) {
  if (busy) return;
  if (!window.confirm("Delete rule " + rule.id + "?\n\nQueries it tagged lose that tag at the next collector pass. This cannot be undone.")) return;
  setBusy(true);
  try {
    const res = await send("DELETE", rulePath(rule.id));
    if (res.kind !== "ok" && res.kind !== "notfound") return setNotice(res.message, true);
    await afterWrite(res.kind === "ok" ? "Rule deleted." : "That rule was already gone.");
  } finally {
    setBusy(false);
  }
}

/* ─────────────────────────── forms ─────────────────────────── */

function tagFormFor(mode, tag, parentId) {
  return { kind: "tag", mode, id: tag ? tag.id : null, original: tag, tagField: "parent_id", values: { name: tag ? tag.name : "", colour: tag && tag.colour ? tag.colour : "", parent_id: parentId } };
}

function assignFormFor(tag) {
  return { kind: "assign", tagId: tag.id, tagName: tag.name, values: { kind: "hash", query_hash: "", database_name: "", schema_name: "", object_name: "", server_id: "", query_id: "" } };
}

function ruleFormFor(rule) {
  const values = rule
    ? { field: rule.field, pattern: rule.pattern, tag_id: rule.tag_id == null ? "" : String(rule.tag_id), priority: String(rule.priority), enabled: !!rule.enabled, server_id: rule.server_id == null ? "" : String(rule.server_id) }
    : { field: "application", pattern: "", tag_id: "", priority: "100", enabled: true, server_id: "" };
  return { kind: "rule", mode: rule ? "edit" : "create", id: rule ? rule.id : null, original: rule, tagField: "tag_id", values, testResult: null, testBusy: false };
}

function openForm(f) {
  form = { banner: null, tagSelect: null, ...f };
  if (live) mount(live.formBox, formNode());
}

function closeForm() {
  form = null;
  if (live) mount(live.formBox, []);
}

function parentOptions(state) {
  const excluded = form.mode === "edit" ? subtreeIds(state.tags, form.id) : new Set();
  const opts = [{ value: "", label: "(no parent — root tag)" }];
  for (const { tag, depth } of treeRows(state.tags)) {
    if (!excluded.has(tag.id)) opts.push({ value: String(tag.id), label: "\u00A0\u00A0".repeat(depth) + tag.name });
  }
  return opts;
}

function ruleTagOptions(state) {
  return [{ value: "", label: "(no tag)" }, ...treeRows(state.tags).map(({ tag }) => ({ value: String(tag.id), label: tag.path || tag.name }))];
}

function input(label, field, extra = {}) {
  const node = el("input", { type: "text", class: "tag-input", "aria-label": label, "data-field": field, value: form.values[field], ...extra });
  node.addEventListener("input", () => { form.values[field] = node.value; });
  return el("div", { class: "tag-field" }, [el("span", { class: "mute-label", text: label }), node]);
}

function select2(label, field, options) {
  const node = el("select", { class: "tag-input", "aria-label": label, "data-field": field }, options.map((o) => el("option", { value: o, text: o })));
  node.value = form.values[field];
  node.addEventListener("change", () => { form.values[field] = node.value; reflow(); });
  return el("div", { class: "tag-field" }, [el("span", { class: "mute-label", text: label }), node]);
}

/* A kind or field change swaps which inputs apply: rebuild the form from what has been typed. */
function reflow() {
  if (live && form) mount(live.formBox, formNode());
}

/* A server pick list (#5971) with "every server" as the blank choice. With no fleet read to build it from, the typed id stays. */
function serverField(label, field) {
  if (!live.servers.length) return input(label, field);
  const node = el("select", { class: "tag-input", "aria-label": label, "data-field": field }, serverOptions(live, "Every server", form.values[field]).map((o) => el("option", { value: o.value, text: o.label })));
  node.value = form.values[field];
  node.addEventListener("change", () => { form.values[field] = node.value; });
  return el("div", { class: "tag-field" }, [el("span", { class: "mute-label", text: label }), node]);
}

function tagSelectField(label, field, options) {
  const node = el("select", { class: "tag-input", "aria-label": label, "data-field": field }, options.map((o) => el("option", { value: o.value, text: o.label })));
  node.value = form.values[field];
  node.addEventListener("change", () => { form.values[field] = node.value; });
  form.tagSelect = node;
  return el("div", { class: "tag-field" }, [el("span", { class: "mute-label", text: label }), node]);
}

function formNode() {
  const actions = [lockButton({ class: "btn primary", type: "button", text: "Save", onClick: submit })];
  let title;
  let fields;
  if (form.kind === "tag") {
    title = form.mode === "create" ? "New query tag" : "Edit query tag";
    const colourInput = input("Colour (#RRGGBB)", "colour", { placeholder: "#RRGGBB, blank for none" });
    const swatches = PALETTE.map((c) => el("button", {
      class: "tag-swatch tag-swatch-pick", type: "button", style: "background:" + c, title: c, "aria-label": "Use colour " + c,
      onClick: () => { form.values.colour = c; reflow(); },
    }));
    fields = [input("Tag name", "name"), colourInput, el("div", {}, swatches), tagSelectField("Parent tag", "parent_id", parentOptions(live))];
  } else if (form.kind === "assign") {
    title = "Assign a query to " + form.tagName;
    const v = form.values;
    fields = [select2("Key kind", "kind", ["hash", "object"])];
    if (v.kind === "hash") fields.push(input("Query hash", "query_hash", { placeholder: "0x..." }));
    else fields.push(input("Database", "database_name"), input("Schema", "schema_name"), input("Object", "object_name"));
    fields.push(serverField("Server (blank for every server)", "server_id"), input("Query id (display only, optional)", "query_id"));
  } else {
    title = form.mode === "create" ? "New rule" : "Edit rule " + form.id;
    const v = form.values;
    const create = form.mode === "create";
    fields = [create ? select2("Match on", "field", RULE_FIELDS) : el("div", { class: "meta", text: "Matches on " + v.field + " (a rule's field cannot change)." })];
    if (v.field !== "text_tag") fields.push(input("Pattern (* is the only wildcard)", "pattern"), tagSelectField("Tag", "tag_id", ruleTagOptions(live)));
    else {
      form.tagSelect = null;
      fields.push(el("div", { class: "meta", text: "A text_tag rule reads the tag name from a /* tag:NAME */ comment in the query text." }));
    }
    fields.push(input("Priority (lower runs first)", "priority"), serverField("Server (blank for every server)", "server_id"));
    const enabled = el("input", { type: "checkbox", "data-field": "enabled", "aria-label": "Enabled" });
    enabled.checked = !!v.enabled;
    enabled.addEventListener("change", () => { form.values.enabled = enabled.checked; });
    fields.push(el("label", { class: "tag-field" }, [enabled, el("span", { text: "Enabled" })]));
    actions.push(el("button", { class: "btn", type: "button", text: "Test", onClick: runTest }));
  }
  actions.push(lockButton({ class: "btn", type: "button", text: "Cancel", onClick: closeForm }));
  if (form.kind !== "tag" && form.kind !== "rule") form.tagSelect = null;
  const resultBox = el("div", { class: "rule-test" }, form.testResult ? testNodes(form.testResult) : []);
  form.resultBox = resultBox;
  return el("div", { class: "card tag-form" }, [
    el("h3", { text: title }),
    form.banner ? el("div", { class: "strip error", role: "alert", text: form.banner }) : null,
    ...fields,
    el("div", { class: "form-actions" }, actions),
    resultBox,
  ]);
}

function testNodes(r) {
  if (r.error) return [el("div", { class: "strip error", role: "alert", text: r.error })];
  const nodes = [el("div", { class: "meta", text: r.message || "" })];
  if (r.keys_examined != null) {
    nodes.push(el("div", { text: r.keys_examined + " key(s) examined, " + r.keys_matched + " matched: " + r.would_tag + " would be tagged, " + r.held_by_hand_assignment + " held by a hand assignment, " + r.held_by_higher_rule + " held by a higher rule." }));
  }
  if (Array.isArray(r.samples) && r.samples.length) {
    nodes.push(el("ul", { class: "rule-samples" }, r.samples.map((s) => el("li", { text: s.outcome + ": " + s.key + " (" + s.matched_field + " = " + s.matched_value + ")" + (s.held_tag ? " held by " + s.held_tag : "") }))));
  }
  return nodes;
}

async function runTest() {
  if (!form || form.kind !== "rule" || form.testBusy) return;
  const f = form;
  f.testBusy = true;
  try {
    const res = await apiGet("/api/query-tag-rules/test?" + testQuery(f.values, f.values.server_id, ""));
    const b = bodyOf(res);
    f.testResult = b ? b : { error: res.message || "The test could not run." };
    if (form === f && f.resultBox) mount(f.resultBox, testNodes(f.testResult));
  } finally {
    f.testBusy = false;
  }
}

async function submit() {
  if (busy || !form) return;
  const f = form;
  setBusy(true);
  try {
    f.banner = null;
    let res;
    if (f.kind === "tag") {
      if (f.mode === "create") res = await send("POST", "/api/query-tags", formToCreateBody(f.values));
      else {
        const patch = buildPatch(f.original, f.values);
        if (Object.keys(patch).length === 0) return closeForm();
        res = await send("PATCH", tagPath(f.id), patch);
      }
    } else if (f.kind === "assign") {
      res = await send("POST", tagPath(f.tagId) + "/assignments", assignmentBody(f.values, true));
    } else if (f.mode === "create") {
      res = await send("POST", "/api/query-tag-rules", ruleToCreateBody(f.values));
    } else {
      const patch = ruleToPatch(f.original, f.values);
      if (Object.keys(patch).length === 0) return closeForm();
      res = await send("PATCH", rulePath(f.id), patch);
    }
    if (form !== f) return;
    if (res.kind !== "ok") {
      f.banner = res.message;
      return mount(live.formBox, formNode());
    }
    if (f.kind === "tag" && f.mode === "create" && res.body.tag && res.body.tag.tag_id != null) selectedId = res.body.tag.tag_id;
    const verb = res.status === "unchanged" ? "No change." : res.status === "moved" ? "Assignment moved to this tag." : f.kind === "tag" ? (f.mode === "create" ? "Tag created." : "Tag updated.") : f.kind === "assign" ? "Assignment saved." : f.mode === "create" ? "Rule created." : "Rule updated.";
    closeForm();
    await afterWrite(verb);
  } finally {
    setBusy(false);
  }
}
