/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Admin > People (#7107): the people the service knows, the sign-ins that map to each one, and the grants each one holds.
     GET /api/people             {can_decide, signed_in_admin, sign_in_key, people?: [{person_id, display_name, enabled, identities: [...], grants: [...]}]}
                                 people comes only to an administrator. Any other session gets its own key and no list.
     PUT /api/people/{command}   the command's args  ->  200 {status: added|renamed|removed|unchanged, message}; 400; 403; 404; 409; 503
   The commands: add_person, rename_person, set_person_enabled, add_person_identity, remove_person_identity, add_person_grant, remove_person_grant.
   Every signed-in person sees their own sign-in key here, with a copy button, so they can hand it to an administrator, who pastes it into the
   person's web sign-in. The key is a hash of the sign-in, and knowing it signs nobody in. The shared web token has no key.
   Changes are made only by a signed-in admin under a store login that can decide. Under any other session the list still shows and the page says so.
   Self-contained: admin.js hands this file its helpers, so a change here never touches that file's tab code. */

import { VIZ } from "../panels.js";
import { el, mount, errorStrip, noticeStrip, listOnlyNote, apiGet, apiWrite } from "../util.js";

/** The capabilities a grant gives, in the order the picker lists them, with their words. */
export const CAPABILITIES = [
  { key: "ask", label: "Ask the agent (reads only)" },
  { key: "request_level_2", label: "Request Level 2 changes" },
  { key: "request_level_3", label: "Request server actions" },
  { key: "approve", label: "Approve server actions" },
  { key: "undo", label: "Undo agent changes" },
  { key: "decide_note", label: "Approve or reject agent notes" },
  { key: "feedback", label: "Give report feedback" },
];

/** The server action kinds a Level 3 request or an approval names. */
export const ACTION_KINDS = [
  { key: "force_plan", label: "Force or unforce plans" },
  { key: "evict_plan", label: "Evict plans" },
  { key: "kill_session", label: "Kill sessions" },
  { key: "recompile_module", label: "Recompile modules" },
  { key: "create_plan_guide", label: "Create plan guides" },
  { key: "drop_plan_guide", label: "Drop plan guides" },
];

/** Whether a capability takes a list of kinds (at least one). The store holds the same rule. */
export function capabilityTakesKinds(capability) {
  return capability === "request_level_3" || capability === "approve";
}

const label = (list, key) => (list.find((x) => x.key === key) || {}).label || key;

export const PEOPLE_COLUMNS = [
  { key: "name", label: "Name" },
  { key: "state", label: "State" },
  { key: "signins", label: "Sign-ins", wrap: true },
  { key: "grants", label: "Grants", wrap: true },
  { key: "changed_by", label: "Changed by" },
  { key: "changed", label: "Changed", format: "time" },
];

const who = (by) => (typeof by === "string" && by.startsWith("person:") && by.length > 7 ? by.slice(7) : by || null);

/** A sign-in in words: "Slack T1:U1" or "Web sign-in abcdefgh...". */
export function identityText(i) {
  if (i.kind === "slack") return "Slack " + i.external_id;
  if (i.kind === "web") return "Web sign-in " + String(i.external_id || "").slice(0, 8) + "...";
  return i.kind + " " + i.external_id;
}

/** A grant's scope in words: All servers, or the server's or tag's name. */
export function grantScopeText(g) {
  if (g.scope === "server") return "Server: " + (g.scope_server_name || "id " + g.scope_server_id);
  if (g.scope === "tag") return "Tag: " + (g.scope_tag_name || "id " + g.scope_tag_id);
  return "All servers";
}

/** A grant in words: the capability, its kinds when it takes them, and its scope. */
export function grantText(g) {
  const kinds = Array.isArray(g.kinds) && g.kinds.length ? " (" + g.kinds.map((k) => label(ACTION_KINDS, k)).join(", ") + ")" : "";
  return label(CAPABILITIES, g.capability) + kinds + ", " + grantScopeText(g);
}

/** One table row per person. */
export function peopleRows(data) {
  const people = data && Array.isArray(data.people) ? data.people : [];
  return people.map((p) => ({
    person_id: p.person_id,
    name: p.display_name,
    enabled: p.enabled === true,
    state: p.enabled === true ? "Enabled" : "Disabled",
    signins: (p.identities || []).map(identityText).join("; ") || "None",
    grants: (p.grants || []).map(grantText).join("; ") || "None",
    changed_by: who(p.updated_by),
    changed: p.updated_at_utc || null,
  }));
}

/** The args of an add_person_identity write from the form's values, or { error } when a value is missing. Slack ids are upper case. */
export function identityArgs(personId, v) {
  if (v.kind === "slack") {
    const team = String(v.team || "").trim().toUpperCase();
    const user = String(v.user || "").trim().toUpperCase();
    if (!team || !user) return { error: "Enter the Slack team id and the user id." };
    return { person_id: personId, kind: "slack", external_id: team + ":" + user };
  }
  const key = String(v.key || "").trim();
  if (!key) return { error: "Paste the person's sign-in key." };
  return { person_id: personId, kind: "web", external_id: key };
}

/** The args of an add_person_grant write from the form's values, or { error }. The scope value is "" (all servers), "s<id>" or "t<id>". */
export function grantArgs(personId, v) {
  const capability = String(v.capability || "");
  if (!capability) return { error: "Pick what the grant lets the person do." };
  const scope = String(v.scope || "");
  const args = { person_id: personId, capability };
  if (scope.startsWith("s")) args.scope_server_id = Number(scope.slice(1));
  if (scope.startsWith("t")) args.scope_tag_id = Number(scope.slice(1));
  if (capabilityTakesKinds(capability)) {
    const kinds = Array.isArray(v.kinds) ? v.kinds.filter((k) => ACTION_KINDS.some((a) => a.key === k)) : [];
    if (!kinds.length) return { error: "Pick at least one server action for this grant." };
    args.kinds = kinds;
  }
  return args;
}

/** A write's answer in plain words: { message, isError }. A refusal keeps the service's own sentence. */
export function peopleOutcome(res) {
  if (!res || res.expired) return { message: null, isError: true };
  if (res.status === 0) return { message: res.message || "Network error.", isError: true };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && typeof body.status === "string" && body.status !== "refused") return { message: body.message || "Done.", isError: false };
  const said = body && typeof body.message === "string" ? body.message : body && typeof body.error === "string" ? body.error : "";
  if ([400, 403, 404, 409, 503].includes(res.status)) return { message: said || "Nothing was changed.", isError: true };
  return { message: "Nothing was changed. The service answered " + res.status + "." + (said ? " " + said : ""), isError: true };
}

const listOnly = (managed, adminWritesOff) => listOnlyNote(managed, "this page lists people only", "To add or change a person", adminWritesOff);
const CAN_DECIDE =
  "The people the service knows, the sign-ins that map to each one, and what each one may do. A grant names what the person may do and where: " +
  "one server, one tag, or all servers. Nothing reads a grant yet, and administrators keep every power they have. Every change is recorded with your name.";
const KEY_NOTE =
  "Your sign-in key. Give it to an administrator to link this sign-in to your name. It is a hash of your sign-in, and knowing it signs nobody in.";
const NO_KEY =
  "This session has no sign-in key, so it cannot be linked to a person. The shared web token has none. A person signed in through the identity " +
  "provider has one: if you are, open /auth/logout and sign in again.";

/** The saving line a person reads while a change is in flight. */
export const SAVING_PEOPLE = "Saving the change. The buttons are locked until it finishes.";

/* Page state in module scope, so a repaint keeps it: the last answer, the person whose details are open, and what the forms hold. */
let status = null;
let busy = false;
let openId = null;
let form = blankForm();
const buttons = new Set();

function blankForm() {
  return { newName: "", rename: null, kind: "web", team: "", user: "", key: "", capability: "", kinds: [], scope: "" };
}

function setBusy(value) {
  busy = value;
  for (const b of buttons) b.disabled = value;
}

/** A button of this tab: disabled while a change is in flight, and tracked so the next change disables it at once. */
function button(props) {
  const b = el("button", { class: "btn", type: "button", ...props });
  b.disabled = busy;
  buttons.add(b);
  return b;
}

function input(field, value, props = {}) {
  const box = el("input", { class: "tag-input", type: "text", "data-field": field, ...props });
  box.value = value;
  box.addEventListener("input", () => { form[field] = box.value; });
  return box;
}

function select(field, choices, value, onChange) {
  const box = el("select", { class: "tag-input", "data-field": field }, choices.map((o) => el("option", { value: o.value, text: o.label })));
  box.value = value;
  box.addEventListener("change", () => { form[field] = box.value; if (onChange) onChange(); });
  return box;
}

const field = (text, control) => el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text }), control]);

/** The sign-in key panel every session sees: its own key with a copy button, or why it has none. A session that is not an administrator sees only this. */
function keyPanel(key) {
  if (!key) return noticeStrip(NO_KEY);
  const copy = el("button", { class: "btn", type: "button", text: "Copy", "aria-label": "Copy your sign-in key" });
  copy.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(key);
      copy.textContent = "Copied";
    } catch {
      copy.textContent = "Copy failed: select the key and copy it";
    }
  });
  return el("div", { class: "notice", "data-role": "sign-in-key" }, [
    el("p", { text: KEY_NOTE }),
    el("div", { class: "row-actions" }, [el("code", { "data-field": "sign-in-key", text: key }), copy]),
  ]);
}

/** Build the tab. `ctx` carries admin.js's own helpers: { getSession, failure, readScopes, isCurrent(generation), repaint() }. */
export async function buildPeople(body, generation, ctx) {
  const [res, scopes] = await Promise.all([apiGet("/api/people"), ctx.readScopes ? ctx.readScopes() : Promise.resolve({ servers: [], tags: [] })]);
  if (!ctx.isCurrent(generation)) return;
  buttons.clear();
  const nodes = [];
  if (status) nodes.push(status.isError ? errorStrip(status.message) : noticeStrip(status.message));
  const bad = ctx.failure(res);
  if (bad) return mount(body, nodes.concat([bad]));
  const data = res.data || {};
  const key = typeof data.sign_in_key === "string" && data.sign_in_key ? data.sign_in_key : null;
  /* Only an administrator gets the list (#7107). Any other session sees its own sign-in key and its Copy button, and nothing else. */
  if (!Array.isArray(data.people)) return mount(body, nodes.concat([keyPanel(key)]));
  const canDecide = data.can_decide === true;
  let notice = CAN_DECIDE;
  if (!canDecide) {
    const session = await ctx.getSession();
    notice = listOnly(session.managed_store === true, session.admin_writes_off === true);
  }
  nodes.push(noticeStrip(notice));
  nodes.push(keyPanel(key));
  const people = data.people;
  if (canDecide) {
    nodes.push(el("div", { class: "row-actions", "data-role": "add-person" }, [
      field("New person:", input("newName", form.newName, { maxlength: "128", placeholder: "Display name" })),
      button({ text: "Add person", onClick: () => { const name = form.newName.trim(); if (name) write("add_person", { display_name: name }, ctx, () => { form.newName = ""; }); } }),
    ]));
  }
  const manageColumn = {
    key: "manage", label: "Details", csv: false, copy: false,
    render: (row) => (row ? button({
      text: openId === row.person_id ? "Close" : "Open", "aria-label": (openId === row.person_id ? "Close " : "Open ") + row.name,
      onClick: () => { openId = openId === row.person_id ? null : row.person_id; form = blankForm(); status = null; ctx.repaint(); },
    }) : null),
  };
  nodes.push(VIZ.table({ people: peopleRows(data) }, { rowsKey: "people", columns: PEOPLE_COLUMNS.concat([manageColumn]), emptyText: "No person is set up." }));
  const open = people.find((p) => p.person_id === openId);
  if (open) nodes.push(detailPanel(open, canDecide, key, scopes, ctx));
  mount(body, nodes);
}

/** One person's details: rename, enable or disable, sign-ins and grants, with the controls only when the session can decide. */
function detailPanel(p, canDecide, key, scopes, ctx) {
  const id = p.person_id;
  const parts = [el("h3", { text: p.display_name })];
  if (canDecide) {
    if (form.rename === null) form.rename = p.display_name;
    parts.push(el("div", { class: "row-actions" }, [
      field("Name:", input("rename", form.rename, { maxlength: "128" })),
      button({ text: "Rename", onClick: () => write("rename_person", { person_id: id, display_name: form.rename.trim() }, ctx) }),
      button({ text: p.enabled ? "Disable" : "Enable", "aria-label": (p.enabled ? "Disable " : "Enable ") + p.display_name,
        onClick: () => write("set_person_enabled", { person_id: id, enabled: !p.enabled }, ctx) }),
    ]));
  }

  parts.push(el("p", { class: "mute-label", text: "Sign-ins" }));
  const identities = p.identities || [];
  parts.push(identities.length
    ? el("ul", {}, identities.map((i) => el("li", {}, [
      el("span", { text: identityText(i) + " " }),
      canDecide ? button({ text: "Remove", "aria-label": "Remove " + identityText(i), onClick: () => write("remove_person_identity", { identity_id: i.identity_id }, ctx) }) : null,
    ])))
    : el("p", { text: "No sign-in is linked." }));
  if (canDecide) {
    const slack = form.kind === "slack";
    parts.push(el("div", { class: "row-actions", "data-role": "add-identity" }, [
      field("Kind:", select("kind", [{ value: "web", label: "Web sign-in" }, { value: "slack", label: "Slack" }], form.kind, () => ctx.repaint())),
      ...(slack
        ? [field("Team id:", input("team", form.team, { placeholder: "T0123ABCD" })), field("User id:", input("user", form.user, { placeholder: "U0123ABCD" }))]
        : [field("Sign-in key:", input("key", form.key, { maxlength: "43", placeholder: "Paste the person's sign-in key" }))]),
      button({ text: "Add sign-in", onClick: () => {
        const args = identityArgs(id, form);
        if (args.error) { status = { message: args.error, isError: true }; ctx.repaint(); return; }
        write("add_person_identity", args, ctx, () => { form.team = ""; form.user = ""; form.key = ""; });
      } }),
      key ? button({ text: "Add my own sign-in", onClick: () => write("add_person_identity", { person_id: id, kind: "web", external_id: key }, ctx) }) : null,
    ]));
  }

  parts.push(el("p", { class: "mute-label", text: "Grants" }));
  const grants = p.grants || [];
  parts.push(grants.length
    ? el("ul", {}, grants.map((g) => el("li", {}, [
      el("span", { text: grantText(g) + " " }),
      canDecide ? button({ text: "Remove", "aria-label": "Remove grant " + grantText(g), onClick: () => write("remove_person_grant", { grant_id: g.grant_id }, ctx) }) : null,
    ])))
    : el("p", { text: "No grant." }));
  if (canDecide) {
    const scopeChoices = [{ value: "", label: "All servers" }]
      .concat((scopes.servers || []).map((s) => ({ value: "s" + s.id, label: "Server: " + s.name })))
      .concat((scopes.tags || []).map((t) => ({ value: "t" + t.id, label: "Tag: " + t.name })));
    const kindBoxes = capabilityTakesKinds(form.capability)
      ? ACTION_KINDS.map((k) => {
        const box = el("input", { type: "checkbox", "data-kind": k.key });
        box.checked = form.kinds.includes(k.key);
        box.addEventListener("change", () => { form.kinds = box.checked ? form.kinds.concat([k.key]) : form.kinds.filter((x) => x !== k.key); });
        return el("label", { class: "tag-field" }, [box, el("span", { text: " " + k.label })]);
      })
      : [];
    parts.push(el("div", { class: "row-actions", "data-role": "add-grant" }, [
      field("Lets them:", select("capability", [{ value: "", label: "Pick one" }].concat(CAPABILITIES.map((c) => ({ value: c.key, label: c.label }))), form.capability, () => ctx.repaint())),
      ...kindBoxes,
      field("Where:", select("scope", scopeChoices, form.scope)),
      button({ text: "Add grant", onClick: () => {
        const args = grantArgs(id, form);
        if (args.error) { status = { message: args.error, isError: true }; ctx.repaint(); return; }
        write("add_person_grant", args, ctx, () => { form.capability = ""; form.kinds = []; form.scope = ""; });
      } }),
    ]));
  }
  return el("div", { class: "notice", "data-role": "person-detail", "aria-label": "Details of " + p.display_name }, parts);
}

async function write(command, args, ctx, onDone) {
  if (busy) return;
  setBusy(true);
  status = { message: SAVING_PEOPLE, isError: false };
  ctx.repaint();
  try {
    const out = peopleOutcome(await apiWrite("PUT", "/api/people/" + encodeURIComponent(command), args));
    if (out.message) status = { message: out.message, isError: out.isError };
    if (!out.isError) {
      if (onDone) onDone();
      if (command === "rename_person") form.rename = null;
    }
  } catch (e) {
    status = { message: "Could not save the change: " + (e && e.message ? e.message : String(e)), isError: true };
  } finally {
    setBusy(false);
  }
  ctx.repaint();
}

/** Clears the last answer and the open person; admin.js calls it when the person leaves the tab. */
export function resetPeopleStatus() {
  status = null;
  openId = null;
  form = blankForm();
}
