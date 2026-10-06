/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Manage Tags page (#5085) — the web twin of the desktop viewer's tag manager. It reads the tag forest and each
 * server's directly-assigned tags from the fleet roll-up (apiGetFleet: `tags` and `cards[].tags`) and, for a
 * sign-in the session reports can_edit, writes through the five /api/server-tags routes: POST (create), PATCH
 * (partial edit), DELETE ?confirm=true, POST .../servers (assign) and DELETE .../servers (unassign). The server is
 * the authority for every write; this page is only the affordance and computes no figure.
 *
 * Every write result names the custom alert rules whose coverage changed (`affected_rules`); the page lists them
 * after each write. A delete that would leave a rule matching no server answers 409 confirm_required with those
 * rules; the page shows them and sends the delete again with confirm=true only when the user asks again.
 *
 * Polling: app.js re-calls renderManageTags on its 60 s tick. The selected tag, an open form and the unsaved
 * checkbox intents live in module scope (`selectedId`, `form`, `edits`), so a tick re-reads and repaints without
 * losing them. A tick redraws the tree, the server list and the form's parent options only; the open form's
 * inputs are drawn by openForm, closeForm and submit and are never rebuilt by a tick, so focus and caret stay.
 * An intent is what the user changed ({add, remove} per tag), never a snapshot of the whole checked set, so a
 * server assigned or unassigned elsewhere is never undone by Apply. Every write handler is guarded by `busy`, so
 * a double click sends one request. All server text reaches the DOM through el()/textContent.
 */

import { el, mount, apiGetFleet, apiWrite, loadingStrip, errorStrip, emptyStrip } from "../util.js";
import { getSession } from "../views-api.js";

/** The swatches offered next to the #RRGGBB box. The server accepts any #RRGGBB; these are only shortcuts. */
export const PALETTE = ["#4E79A7", "#F28E2B", "#E15759", "#76B7B2", "#59A14F", "#EDC948", "#B07AA1", "#FF9DA7", "#9C755F", "#BAB0AC"];

/** A stored colour that is safe to put in a style attribute, or null. */
export function safeColour(c) {
  return /^#[0-9a-fA-F]{6}$/.test(c || "") ? c : null;
}

/** The forest as depth-first rows { tag, depth }, siblings in the order the server sent them. A dangling parent
 *  surfaces as a root and a cycle cannot repeat a tag. */
export function treeRows(forest) {
  const known = new Set(forest.map((t) => t.id));
  const kids = new Map();
  for (const t of forest) {
    const p = t.parent_id != null && known.has(t.parent_id) ? t.parent_id : 0;
    if (!kids.has(p)) kids.set(p, []);
    kids.get(p).push(t);
  }
  const rows = [];
  const seen = new Set();
  const walk = (t, depth) => {
    if (seen.has(t.id)) return;
    seen.add(t.id);
    rows.push({ tag: t, depth });
    for (const c of kids.get(t.id) || []) walk(c, depth + 1);
  };
  for (const t of kids.get(0) || []) walk(t, 0);
  for (const t of forest) walk(t, 0);
  return rows;
}

/** The ids of a tag and everything under it. */
export function subtreeIds(forest, id) {
  const out = new Set([id]);
  let grew = true;
  while (grew) {
    grew = false;
    for (const t of forest) {
      if (t.parent_id != null && out.has(t.parent_id) && !out.has(t.id)) {
        out.add(t.id);
        grew = true;
      }
    }
  }
  return out;
}

/** The POST body for a create: only the fields that hold a value. */
export function formToCreateBody(values) {
  const body = { name: String(values.name || "").trim() };
  if (values.parent_id !== "" && values.parent_id != null) body.parent_id = Number(values.parent_id);
  const colour = String(values.colour || "").trim();
  if (colour !== "") body.colour = colour;
  return body;
}

/** The PATCH body: ONLY the fields that differ from the stored tag. A blanked colour is an explicit null and
 *  "no parent" is an explicit null parent_id (a move to the root). {} when nothing changed. */
export function buildPatch(original, values) {
  const patch = {};
  const name = String(values.name || "").trim();
  if (name !== original.name) patch.name = name;
  const colour = String(values.colour || "").trim();
  const nextColour = colour === "" ? null : colour;
  const oldColour = original.colour || null;
  if ((nextColour === null ? null : nextColour.toLowerCase()) !== (oldColour === null ? null : oldColour.toLowerCase())) {
    patch.colour = nextColour;
  }
  const nextParent = values.parent_id === "" || values.parent_id == null ? null : Number(values.parent_id);
  if (nextParent !== (original.parent_id == null ? null : original.parent_id)) patch.parent_id = nextParent;
  return patch;
}

/** The two id lists an Apply sends from a tag's intent: ids to assign that the baseline lacks, ids to unassign
 *  that the baseline carries. An id the baseline already agrees with is not sent. */
export function diffAssignment(baseline, intent) {
  const add = [...intent.add].filter((id) => !baseline.has(id));
  const remove = [...intent.remove].filter((id) => baseline.has(id));
  return { add, remove };
}

/** Turns a write response ({status, body}) into what the page acts on. Branches on the HTTP status and the
 *  envelope's status word, never on message text. */
export function interpretWrite(status, body) {
  const b = body && typeof body === "object" ? body : {};
  const message = typeof b.message === "string" ? b.message : typeof b.error === "string" ? b.error : "Request failed (HTTP " + status + ")";
  const rules = Array.isArray(b.affected_rules) ? b.affected_rules : [];
  if (status === 401) return { kind: "expired", message: "Your session has expired. Sign in again." };
  if (status === 403) return { kind: "readonly", message: "This session is read-only, so the change was not made." };
  if (status === 404) return { kind: "notfound", message, refusal: typeof b.refusal === "string" ? b.refusal : null };
  if (status === 409 && b.status === "confirm_required") return { kind: "confirm", message, rules };
  if (status === 409) return { kind: "conflict", message };
  if (status === 400) return { kind: "invalid", message };
  if (status >= 200 && status < 300) return { kind: "ok", body: b, rules, status: b.status || null };
  return { kind: "error", message };
}

/* One write: apiWrite's transport (it tells the shell once when the session is gone), read through interpretWrite. A
   status of 0 is a request that got no answer; `expired` is a 401 or a 2xx that is not a JSON body, never a saved change. */
async function send(method, path, body) {
  const res = await apiWrite(method, path, body);
  if (res.status === 0) return { kind: "error", message: res.message };
  if (res.expired) return { kind: "expired", message: "Your session has expired. Sign in again." };
  if (res.unexpected) return { kind: "error", message: "The service gave an unexpected answer (HTTP " + res.status + "). Check the list before trying again." };
  return interpretWrite(res.status, res.body);
}

const tagPath = (id) => "/api/server-tags/" + encodeURIComponent(id);
const createTag = (body) => send("POST", "/api/server-tags", body);
const patchTag = (id, body) => send("PATCH", tagPath(id), body);
const deleteTag = (id, confirm) => send("DELETE", tagPath(id) + (confirm ? "?confirm=true" : ""));
const assignServers = (id, ids) => send("POST", tagPath(id) + "/servers", { server_ids: ids });
const unassignServers = (id, ids) => send("DELETE", tagPath(id) + "/servers", { server_ids: ids });

/* ─────────────────────────── page ─────────────────────────── */

/* Module scope: survives the 60 s re-render. */
let live = null;
let selectedId = null;
let form = null;
let notice = null;
let pendingDelete = null;
/* True while a write is in flight: a second click on any write control does nothing until it settles. */
let busy = false;
/* The buttons that open or replace a form, or cancel one, so they can be disabled while a write is in flight. */
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
/* Unsaved checkbox intents, keyed by tag id: { add, remove } server-id sets recorded from the change events.
   Checked = baseline ∪ add − remove. An entry exists only while it holds an id. */
const edits = new Map();

export async function renderManageTags(main) {
  if (live && live.main === main && live.headEl.isConnected) {
    await refresh(live);
    return;
  }
  const session = await getSession();
  const canEdit = !!session.can_edit;
  const newRoot = canEdit ? lockButton({ class: "btn primary", type: "button", text: "New root tag", onClick: () => openForm({ mode: "create", values: { name: "", colour: "", parent_id: "" } }) }) : null;
  const headEl = el("div", { class: "page-head" }, [
    el("h2", { text: "Manage Tags" }),
    el("div", { class: "meta", text: canEdit ? "tags scope custom alert rules" : "read-only sign-in: tags can be viewed, not changed" }),
    el("div", { class: "spacer" }),
    newRoot,
  ]);
  const noticeBox = el("div", {});
  const formBox = el("div", {});
  const body = el("div", {});
  mount(main, [headEl, noticeBox, formBox, body]);
  live = { main, headEl, noticeBox, formBox, body, canEdit, forest: [], cards: [] };
  if (form) mount(formBox, formNode());
  mount(body, loadingStrip("Loading tags…"));
  await refresh(live);
}

async function refresh(state) {
  const res = await apiGetFleet();
  if (res.kind === "error") return mount(state.body, errorStrip(res.message));
  const d = res.data || {};
  state.forest = Array.isArray(d.tags) ? d.tags : [];
  state.cards = Array.isArray(d.cards) ? d.cards : [];
  if (selectedId != null && !state.forest.some((t) => t.id === selectedId)) selectedId = null;
  pruneEdits(state);
  draw(state);
  refreshParentOptions(state);
}

/** Drops the intents of tags that no longer exist and every id the baseline now agrees with. */
function pruneEdits(state) {
  const known = new Set(state.forest.map((t) => t.id));
  for (const [id, intent] of [...edits]) {
    if (!known.has(id)) {
      edits.delete(id);
      continue;
    }
    const base = baselineOf(state, id);
    for (const s of [...intent.add]) if (base.has(s)) intent.add.delete(s);
    for (const s of [...intent.remove]) if (!base.has(s)) intent.remove.delete(s);
    if (!intent.add.size && !intent.remove.size) edits.delete(id);
  }
}

/** Rebuilds only the open form's parent options, in place, so the typed fields keep their element and focus. */
function refreshParentOptions(state) {
  if (!form || !form.parentSelect) return;
  const opts = parentOptions(state);
  /* A parent deleted elsewhere has no option any more: fall back to none rather than send it on the next Save. */
  if (!opts.some((o) => o.value === form.values.parent_id)) form.values.parent_id = "";
  mount(form.parentSelect, opts.map((o) => el("option", { value: o.value, text: o.label })));
  form.parentSelect.value = form.values.parent_id;
}

function draw(state) {
  drawNotice(state);
  if (!state.forest.length) {
    return mount(state.body, emptyStrip("No tags are defined." + (state.canEdit ? " Use New root tag to create one." : "")));
  }
  mount(state.body, el("div", { class: "tags-layout" }, [treeNode(state), el("div", { class: "tags-detail" }, [detailNode(state)])]));
}

function drawNotice(state) {
  const nodes = [];
  if (notice) {
    nodes.push(el("div", { class: "strip " + (notice.isError ? "error" : "notice"), role: notice.isError ? "alert" : "status", text: notice.message }));
    if (notice.rules && notice.rules.length) nodes.push(rulesNode(notice.rules, "Custom alert rules whose coverage changed:"));
  }
  if (pendingDelete) {
    nodes.push(el("div", { class: "strip error" }, [
      el("div", { text: pendingDelete.message }),
      rulesNode(pendingDelete.rules, "These custom alert rules would match no server:"),
      el("div", { class: "form-actions" }, [
        el("button", { class: "btn primary", type: "button", text: "Delete anyway", onClick: confirmDelete }),
        lockButton({ class: "btn", type: "button", text: "Cancel", onClick: cancelDelete }),
      ]),
    ]));
  }
  mount(state.noticeBox, nodes);
}

function rulesNode(rules, heading) {
  return el("div", { class: "tags-rules" }, [
    el("div", { class: "meta", text: heading }),
    el("ul", {}, rules.map((r) => el("li", { "data-rule-id": r.rule_id, text: ruleLine(r) }))),
  ]);
}

function ruleLine(r) {
  const bits = [r.name || "(unnamed rule)"];
  if (r.effect) bits.push(r.effect);
  if (r.servers_lost_count) bits.push(r.servers_lost_count + " server(s) lost");
  if (r.servers_gained_count) bits.push(r.servers_gained_count + " server(s) gained");
  if (r.enabled === false) bits.push("rule disabled");
  return bits.join(" — ");
}

function swatch(colour) {
  const c = safeColour(colour);
  return el("span", { class: "tag-swatch", style: c ? "background:" + c : null, "aria-hidden": "true" });
}

function treeNode(state) {
  const rows = treeRows(state.forest);
  return el("ul", { class: "tag-tree" }, rows.map(({ tag, depth }) =>
    el("li", { style: "padding-left:" + depth * 18 + "px" }, [
      el("button", {
        class: "tag-node" + (tag.id === selectedId ? " tag-selected" : ""),
        type: "button",
        "data-tag-id": tag.id,
        "aria-current": tag.id === selectedId ? "true" : null,
        onClick: () => select(tag.id),
      }, [swatch(tag.colour), el("span", { text: tag.name + (edits.has(tag.id) ? " (unsaved)" : "") })]),
    ])));
}

function select(id) {
  selectedId = id;
  if (live) draw(live);
}

function detailNode(state) {
  const tag = state.forest.find((t) => t.id === selectedId);
  if (!tag) return emptyStrip("Select a tag to see and change its servers.");
  const actions = state.canEdit
    ? el("div", { class: "form-actions" }, [
        lockButton({ class: "btn", type: "button", text: "New child tag", onClick: () => openForm({ mode: "create", values: { name: "", colour: "", parent_id: String(tag.id) } }) }),
        lockButton({ class: "btn", type: "button", text: "Edit", onClick: () => openForm({ mode: "edit", id: tag.id, original: tag, values: { name: tag.name, colour: tag.colour || "", parent_id: tag.parent_id == null ? "" : String(tag.parent_id) } }) }),
        el("button", { class: "btn", type: "button", text: "Delete", onClick: () => remove(tag) }),
      ])
    : null;
  return el("div", {}, [el("h3", { text: tag.name }), actions, assignmentNode(state, tag)]);
}

/* ─────────────────────────── assignment ─────────────────────────── */

function baselineOf(state, tagId) {
  const s = new Set();
  for (const c of state.cards) if ((c.tags || []).some((t) => t.id === tagId)) s.add(c.server_id);
  return s;
}

function checkedOf(state, tagId) {
  const checked = baselineOf(state, tagId);
  const intent = edits.get(tagId);
  if (intent) {
    for (const id of intent.add) checked.add(id);
    for (const id of intent.remove) checked.delete(id);
  }
  return checked;
}

/** Records one checkbox change as an intent against the current baseline. */
function recordIntent(state, tagId, serverId, isChecked) {
  const intent = edits.get(tagId) || { add: new Set(), remove: new Set() };
  const inBase = baselineOf(state, tagId).has(serverId);
  intent.add.delete(serverId);
  intent.remove.delete(serverId);
  if (isChecked && !inBase) intent.add.add(serverId);
  if (!isChecked && inBase) intent.remove.add(serverId);
  if (intent.add.size || intent.remove.size) edits.set(tagId, intent);
  else edits.delete(tagId);
}

function assignmentNode(state, tag) {
  if (!state.cards.length) return emptyStrip("No servers are monitored.");
  const checked = checkedOf(state, tag.id);
  const rows = state.cards.map((c) => {
    const box = el("input", { type: "checkbox", "data-server-id": c.server_id, "aria-label": c.display_name });
    box.checked = checked.has(c.server_id);
    box.disabled = !state.canEdit;
    box.addEventListener("change", () => {
      recordIntent(state, tag.id, c.server_id, box.checked);
    });
    return el("label", { class: "tags-server" }, [box, el("span", { text: c.display_name })]);
  });
  const nodes = [el("div", { class: "meta", text: "Checked servers carry this tag." }), ...rows];
  if (state.canEdit) {
    nodes.push(el("div", { class: "form-actions" }, [
      el("button", { class: "btn primary", type: "button", text: "Apply", onClick: () => applyAssignment(tag.id) }),
      el("button", { class: "btn", type: "button", text: "Reset", onClick: () => { edits.delete(tag.id); draw(live); } }),
    ]));
  }
  return el("div", { class: "tags-servers" }, nodes);
}

async function applyAssignment(tagId) {
  if (busy) return;
  setBusy(true);
  try {
    const intent = edits.get(tagId);
    if (!intent) return setNotice("No assignment changes to apply.", false);
    const { add, remove } = diffAssignment(baselineOf(live, tagId), intent);
    const rules = [];
    if (add.length) {
      const res = await assignServers(tagId, add);
      if (res.kind !== "ok") return failNotice(res);
      rules.push(...res.rules);
    }
    if (remove.length) {
      const res = await unassignServers(tagId, remove);
      if (res.kind !== "ok") {
        /* The assign half landed: show its rules with the error and re-read, so the baseline is current. */
        if (add.length) {
          notice = { message: res.message, isError: true, rules };
          return await refresh(live);
        }
        return failNotice(res);
      }
      rules.push(...res.rules);
    }
    edits.delete(tagId);
    await afterWrite("Assignment applied.", rules);
  } finally {
    setBusy(false);
  }
}

/* ─────────────────────────── writes ─────────────────────────── */

function setNotice(message, isError, rules) {
  notice = { message, isError, rules: rules || [] };
  if (live) drawNotice(live);
}

function failNotice(res) {
  setNotice(res.message, true);
}

async function afterWrite(message, rules) {
  notice = { message, isError: false, rules };
  await refresh(live);
}

async function remove(tag) {
  if (busy) return;
  if (!window.confirm("Delete tag \"" + tag.name + "\" and every tag under it?\n\nServers and their data are untouched. This cannot be undone.")) return;
  setBusy(true);
  try {
    notice = null;
    pendingDelete = null;
    const res = await deleteTag(tag.id, false);
    await finishDelete(tag.id, res);
  } finally {
    setBusy(false);
  }
}

async function confirmDelete() {
  if (busy || !pendingDelete) return;
  const pending = pendingDelete;
  setBusy(true);
  try {
    const res = await deleteTag(pending.id, true);
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
    pendingDelete = { id, message: res.message, rules: res.rules };
    return drawNotice(live);
  }
  pendingDelete = null;
  if (res.kind !== "ok" && res.kind !== "notfound") return failNotice(res);
  const gone = res.kind === "ok" && Array.isArray(res.body.deleted_tag_ids) ? res.body.deleted_tag_ids : [id];
  for (const g of gone) edits.delete(g);
  if (gone.includes(selectedId)) selectedId = null;
  await afterWrite(res.kind === "ok" ? "Tag deleted." : "That tag was already gone.", res.rules || []);
}

/* ─────────────────────────── form ─────────────────────────── */

function openForm(f) {
  form = { banner: null, parentSelect: null, ...f };
  if (live) mount(live.formBox, formNode());
}

function closeForm() {
  form = null;
  if (live) mount(live.formBox, []);
}

function parentOptions(state) {
  const excluded = form.mode === "edit" ? subtreeIds(state.forest, form.id) : new Set();
  const opts = [{ value: "", label: "(no parent — root tag)" }];
  for (const { tag, depth } of treeRows(state.forest)) {
    if (!excluded.has(tag.id)) opts.push({ value: String(tag.id), label: "\u00A0\u00A0".repeat(depth) + tag.name });
  }
  return opts;
}

function formNode() {
  const nameInput = el("input", { type: "text", class: "tag-input", "aria-label": "Tag name", "data-field": "name", value: form.values.name });
  nameInput.addEventListener("input", () => { form.values.name = nameInput.value; });
  const colourInput = el("input", { type: "text", class: "tag-input", "aria-label": "Colour (#RRGGBB)", "data-field": "colour", value: form.values.colour, placeholder: "#RRGGBB, blank for none" });
  colourInput.addEventListener("input", () => { form.values.colour = colourInput.value; });
  const swatches = PALETTE.map((c) => el("button", {
    class: "tag-swatch tag-swatch-pick", type: "button", style: "background:" + c, title: c, "aria-label": "Use colour " + c,
    onClick: () => { form.values.colour = c; colourInput.value = c; },
  }));
  const parentSelect = el("select", { class: "tag-input", "aria-label": "Parent tag", "data-field": "parent_id" },
    parentOptions(live).map((o) => el("option", { value: o.value, text: o.label })));
  parentSelect.value = form.values.parent_id;
  parentSelect.addEventListener("change", () => { form.values.parent_id = parentSelect.value; });
  form.parentSelect = parentSelect;
  return el("div", { class: "card tag-form" }, [
    el("h3", { text: form.mode === "create" ? "New tag" : "Edit tag" }),
    form.banner ? el("div", { class: "strip error", role: "alert", text: form.banner }) : null,
    el("div", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Name" }), nameInput]),
    el("div", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Colour" }), colourInput, el("span", {}, swatches)]),
    el("div", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Parent" }), parentSelect]),
    el("div", { class: "form-actions" }, [
      el("button", { class: "btn primary", type: "button", text: "Save", onClick: submit }),
      lockButton({ class: "btn", type: "button", text: "Cancel", onClick: closeForm }),
    ]),
  ]);
}

async function submit() {
  if (busy || !form) return;
  const f = form;
  setBusy(true);
  try {
    f.banner = null;
    let res;
    if (f.mode === "create") {
      res = await createTag(formToCreateBody(f.values));
    } else {
      const patch = buildPatch(f.original, f.values);
      if (Object.keys(patch).length === 0) return closeForm();
      res = await patchTag(f.id, patch);
    }
    if (form !== f) return;
    if (res.kind === "notfound" && res.refusal === "unknown_parent") {
      /* The chosen parent is gone, not the tag: keep the form and what was typed, reset the parent, re-read. */
      f.values.parent_id = "";
      f.banner = res.message;
      notice = null;
      await refresh(live);
      return mount(live.formBox, formNode());
    }
    if (res.kind === "notfound") {
      /* The tag was deleted elsewhere: close the form and re-read, as a delete does. */
      closeForm();
      if (f.mode === "edit") {
        edits.delete(f.id);
        if (selectedId === f.id) selectedId = null;
      }
      return await afterWrite(res.message, []);
    }
    if (res.kind !== "ok") {
      f.banner = res.message;
      return mount(live.formBox, formNode());
    }
    const made = res.body.tag && res.body.tag.tag_id;
    if (f.mode === "create" && made != null) selectedId = made;
    const verb = f.mode === "create" ? "Tag created." : res.status === "unchanged" ? "No change." : "Tag updated.";
    closeForm();
    await afterWrite(verb, res.rules);
  } finally {
    setBusy(false);
  }
}
