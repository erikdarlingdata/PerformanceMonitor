/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Admin > Notification Routes (agent channel) and Environment Profiles (#6264): the web stands level with the desktop Viewer.
     POST   /api/notification-routes         create a route
     PUT    /api/notification-routes/{id}    edit a route (a field left out keeps its saved value)
     DELETE /api/notification-routes/{id}    delete a route
     GET    /api/environment-profiles        the profiles, with max_length and can_edit
     PUT    /api/environment-profiles        {scope_server_id?, scope_tag_id?, profile_text, expected_modified_at?}; blank text removes the profile
     GET    /api/agent-setup/access          {can_edit_routes, can_edit_profiles}: whether the store login holds the write grants
   Editing shows only when the session can make changes AND the store login holds the grants; the viewer role (managed and compose
   stores) lists only, and a write the store refuses anyway answers 403 with its own sentence, which is shown as it came. A route's own
   destinations are set here as the MCP route tools set them: a webhook URL or routing key is sealed by the service and never shown (the
   form says only whether one is saved, from configured_channels); a box left blank keeps what the route has, "Remove" sends "-". The email
   recipients are shown, as the routes read shows them.
   Self-contained: admin.js hands this file its helpers. */

import { VIZ } from "../panels.js";
import { el, mount, errorStrip, emptyStrip, noticeStrip, listOnlyNote, apiGet, apiWrite } from "../util.js";

export const AGENT_LEVEL_CHOICES = [
  { value: "", label: "Inherit" },
  { value: "0", label: "Off" },
  { value: "1", label: "Investigate" },
  { value: "2", label: "Act (Level 2)" },
  { value: "3", label: "Act on servers (Level 3)" },
];

/** #6979: the report notice setting of a route, in the order the editor offers it. */
export const REPORT_NOTICE_CHOICES = [
  { value: "off", label: "Off" },
  { value: "needs_person_or_change", label: "Needs a person or made a change" },
  { value: "all", label: "All reports" },
];

export const AGENT_LIMIT_DEFAULTS = { perHour: "20", perDay: "100", lease: "15", expiry: "240" };

export const routesListOnly = (managed, adminWritesOff) => listOnlyNote(managed, "routes are listed only", "To add, edit or delete a route", adminWritesOff);
export const ROUTES_EDITING_NOTE =
  "Add, edit and delete a route's agent channel, match, scope and destinations here. A webhook URL or key is sealed when saved and never " +
  "shown again: a box left blank keeps the saved one, and Remove deletes it. Email recipients are shown and edited as plain text.";

/** The webhook destinations a route can carry: the body field, the label, the box key and the name configured_channels uses. */
export const WEBHOOK_DESTINATIONS = [
  { field: "teams_webhook_url", label: "Teams webhook URL", key: "teams", channel: "Teams" },
  { field: "slack_webhook_url", label: "Slack webhook URL", key: "slack", channel: "Slack" },
  { field: "generic_webhook_url", label: "Generic webhook URL", key: "generic", channel: "Generic" },
  { field: "pagerduty_routing_key", label: "PagerDuty routing key", key: "pagerduty", channel: "PagerDuty" },
];

/** The Agent use column: how much of an agent route's limits its alerts have used, or null where the route sends no requests. */
export function routeUseText(r) {
  if (!r || (r.agent_level !== 1 && r.agent_level !== 2 && r.agent_level !== 3)) return null;
  if (typeof r.agent_requests_last_hour !== "number" || typeof r.agent_requests_last_day !== "number") return "use unknown";
  return `${r.agent_requests_last_hour} of ${r.agent_max_per_hour} this hour, ${r.agent_requests_last_day} of ${r.agent_max_per_day} today`;
}

/** #7109: one strip for each route where nobody can approve a proposal (GET /api/notification-routes/nobody-can-approve answers
    {route_ids, message}). A failed read shows nothing: the routes below are the page. */
export function nobodyCanApproveNodes(res) {
  if (!res || res.kind !== "data" || !res.data || !Array.isArray(res.data.route_ids)) return [];
  return res.data.route_ids.map((id) => noticeStrip("Route " + id + ": " + res.data.message));
}

/** The editor's entries for a route, from a routes read row (or the defaults for a new one). */
export function entriesFromRoute(r) {
  if (!r) {
    return {
      id: null, match: "", scope: "", enabled: true, level: "", perHour: AGENT_LIMIT_DEFAULTS.perHour, perDay: AGENT_LIMIT_DEFAULTS.perDay,
      lease: AGENT_LIMIT_DEFAULTS.lease, expiry: AGENT_LIMIT_DEFAULTS.expiry, resolutions: "", fallback: "", severities: "both", reportNotice: "off",
      approval: "required", singlePerson: "false",
      saved: [], hooks: {}, removes: {}, recipients: "", recipientsWas: "", modifiedAt: null,
    };
  }
  return {
    id: r.route_id,
    /* #6274: the version this form read; the save sends it back so a route changed elsewhere since is refused, not overwritten. */
    modifiedAt: typeof r.modified_at_utc === "string" ? r.modified_at_utc : null,
    match: r.metric_match || "",
    scope: r.scope_server_id != null ? "s" + r.scope_server_id : r.scope_tag_id != null ? "t" + r.scope_tag_id : "",
    enabled: r.enabled !== false,
    level: r.agent_level === null || r.agent_level === undefined ? "" : String(r.agent_level),
    perHour: String(r.agent_max_per_hour ?? AGENT_LIMIT_DEFAULTS.perHour),
    perDay: String(r.agent_max_per_day ?? AGENT_LIMIT_DEFAULTS.perDay),
    lease: String(r.agent_lease_minutes ?? AGENT_LIMIT_DEFAULTS.lease),
    expiry: String(r.agent_expiry_minutes ?? AGENT_LIMIT_DEFAULTS.expiry),
    resolutions: r.send_resolutions === true ? "true" : r.send_resolutions === false ? "false" : "",
    fallback: r.fallback_route_id != null ? String(r.fallback_route_id) : "",
    /* #6928: a route read without the field, or with a word this page does not know, edits as Both. */
    severities: r.severities === "critical" || r.severities === "warning" ? r.severities : "both",
    /* #6979: a route read without the field, or with a word this page does not know, edits as Off. */
    reportNotice: r.report_notice === "needs_person_or_change" || r.report_notice === "all" ? r.report_notice : "off",
    /* #7109: a route read without the field, or with a word this page does not know, edits as Required, the safe side. */
    approval: r.action_approval === "off" ? "off" : "required",
    singlePerson: r.approval_single_person === true ? "true" : "false",
    saved: Array.isArray(r.configured_channels) ? r.configured_channels : [],
    hooks: {},
    removes: {},
    recipients: typeof r.smtp_recipients === "string" ? r.smtp_recipients : "",
    recipientsWas: typeof r.smtp_recipients === "string" ? r.smtp_recipients : "",
  };
}

/** The destination fields to send: a typed webhook value, "-" for one marked Remove, a changed recipient list ("-" when it was emptied).
    A box left alone sends nothing, so the service keeps what the route has. */
export function destinationPayload(entries) {
  const out = {};
  const hooks = entries.hooks || {};
  const removes = entries.removes || {};
  for (const d of WEBHOOK_DESTINATIONS) {
    const typed = String(hooks[d.key] || "").trim();
    if (typed !== "") out[d.field] = typed;
    else if (removes[d.key] === true) out[d.field] = "-";
  }
  const now = String(entries.recipients || "").trim();
  const was = String(entries.recipientsWas || "").trim();
  if (now !== was) out.email_recipients = now === "" ? "-" : now;
  return out;
}

/** The request body for the entries. The service applies the rules (the same ones as the Viewer); this only turns the boxes into
    values, and says which box is not a number. */
export function routePayload(entries) {
  /* A blank match is refused by the service with this sentence (a 400); said here it costs no request. */
  if (String(entries.match).trim() === "") return { error: "Choose a family or enter an exact metric name." };
  const whole = (label, text) => {
    const t = String(text).trim();
    return /^\d+$/.test(t) ? { value: Number(t) } : { error: label + " must be a whole number." };
  };
  const fields = [["Requests per hour", entries.perHour], ["Requests per day", entries.perDay], ["Lease minutes", entries.lease], ["Expiry minutes", entries.expiry]];
  const nums = [];
  for (const [label, text] of fields) {
    const n = whole(label, text);
    if (n.error) return { error: n.error };
    nums.push(n.value);
  }
  let fallback = null;
  if (String(entries.fallback).trim() !== "") {
    const f = whole("Fallback route", entries.fallback);
    if (f.error) return { error: f.error };
    fallback = f.value;
  }
  const scope = String(entries.scope || "");
  return {
    payload: {
      ...destinationPayload(entries),
      metric_match: String(entries.match).trim(),
      enabled: entries.enabled === true,
      scope_server_id: scope.startsWith("s") ? Number(scope.slice(1)) : null,
      scope_tag_id: scope.startsWith("t") ? Number(scope.slice(1)) : null,
      agent_level: entries.level === "" ? null : Number(entries.level),
      agent_max_per_hour: nums[0],
      agent_max_per_day: nums[1],
      agent_lease_minutes: nums[2],
      agent_expiry_minutes: nums[3],
      send_resolutions: entries.resolutions === "" ? null : entries.resolutions === "true",
      fallback_route_id: fallback,
      severities: entries.severities === "critical" || entries.severities === "warning" ? entries.severities : "both",
      report_notice: entries.reportNotice === "needs_person_or_change" || entries.reportNotice === "all" ? entries.reportNotice : "off",
      action_approval: entries.approval === "off" ? "off" : "required",
      approval_single_person: entries.singlePerson === "true",
      ...(entries.id != null ? { expected_modified_at: entries.modifiedAt } : {}),
    },
  };
}

/** What a route write answer says, in plain words: { message, isError }. An expired session says nothing here. */
export function routeOutcome(verb, res) {
  if (!res || res.expired) return { message: null, isError: true };
  if (res.status === 0) return { message: res.message || "Network error.", isError: true };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && (body.status === "created" || body.status === "updated" || body.status === "deleted")) {
    return { message: "Route " + body.route_id + " " + body.status + ".", isError: false };
  }
  const said = body && typeof body.error === "string" && body.error ? body.error : "";
  if (res.status === 403) return { message: said || "This sign-in cannot change routes.", isError: true };
  return { message: "The route was not " + (verb === "delete" ? "deleted" : "saved") + "." + (said ? " " + said : " The service answered " + res.status + "."), isError: true };
}

let routeEntries = null;
let routeStatus = null;
let routeBusy = false;

const statusNode = (status) => (status ? (status.isError ? errorStrip(status.message) : noticeStrip(status.message)) : null);

async function sendRoute(verb, entries, repaint) {
  if (routeBusy) return;
  routeBusy = true;
  let out;
  try {
    if (verb === "delete") {
      out = routeOutcome(verb, await apiWrite("DELETE", "/api/notification-routes/" + entries.id));
    } else {
      const built = routePayload(entries);
      if (built.error) {
        out = { message: built.error, isError: true };
      } else {
        const path = entries.id == null ? "/api/notification-routes" : "/api/notification-routes/" + entries.id;
        out = routeOutcome(verb, await apiWrite(entries.id == null ? "POST" : "PUT", path, built.payload));
      }
    }
  } catch (e) {
    out = { message: "Could not change the route: " + (e && e.message ? e.message : String(e)), isError: true };
  }
  routeBusy = false;
  if (out.message) routeStatus = { message: out.message, isError: out.isError };
  if (out.isError === false) routeEntries = null;
  await repaint();
}

function routeForm(options) {
  const entries = routeEntries || entriesFromRoute(null);
  const field = (label, control) => el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: label }), control]);
  const text = (name, value, extra) => {
    const c = el("input", Object.assign({ type: "text", class: "tag-input", "data-field": name }, extra || {}));
    c.value = value;
    return c;
  };
  const select = (name, choices, value) => {
    const c = el("select", { class: "tag-input", "data-field": name }, choices.map((o) => el("option", { value: o.value, text: o.label })));
    c.value = value;
    return c;
  };
  const scopeChoices = [{ value: "", label: "All servers" }]
    .concat(options.servers.map((s) => ({ value: "s" + s.id, label: "Server: " + s.name })))
    .concat(options.tags.map((t) => ({ value: "t" + t.id, label: "Tag: " + t.name })));
  const controls = {
    match: text("match", entries.match, { placeholder: "family, 'Analysis: <category>', exact metric or *" }),
    scope: select("scope", scopeChoices, entries.scope),
    severities: select("severities", [{ value: "both", label: "Both (Warning and Critical)" }, { value: "critical", label: "Critical only" }, { value: "warning", label: "Warning only" }], entries.severities || "both"),
    reportNotice: select("reportNotice", REPORT_NOTICE_CHOICES, entries.reportNotice || "off"),
    /* #7109: whether a Level 3 action waits for a person's approval, and whether the person who asked may approve it. An admin seat sets any value. */
    approval: select("approval", [{ value: "required", label: "Required (a person approves each Level 3 action)" }, { value: "off", label: "Off (Level 3 actions run without approval)" }], entries.approval || "required"),
    singlePerson: select("singlePerson", [{ value: "false", label: "No (a second person approves)" }, { value: "true", label: "Yes (the person who asked may approve)" }], entries.singlePerson || "false"),
    level: select("level", AGENT_LEVEL_CHOICES, entries.level),
    perHour: text("perHour", entries.perHour, { inputmode: "numeric" }),
    perDay: text("perDay", entries.perDay, { inputmode: "numeric" }),
    lease: text("lease", entries.lease, { inputmode: "numeric" }),
    expiry: text("expiry", entries.expiry, { inputmode: "numeric" }),
    resolutions: select("resolutions", [{ value: "", label: "Follow each channel" }, { value: "true", label: "Always send" }, { value: "false", label: "Never send" }], entries.resolutions),
    fallback: text("fallback", entries.fallback, { placeholder: "route number, blank = none" }),
  };
  const enabled = el("input", { type: "checkbox", "data-field": "enabled" });
  enabled.checked = entries.enabled;
  /* #6979: a report notice fires for a request of this route, and only a route with its own agent level (Investigate, Act or Act on servers)
     owns requests. While the level is Inherit or Off the setting is disabled, with a hint; the stored value is kept and still saved. */
  const noticeHint = el("span", { class: "mute-label", text: " Applies once the Agent level is Investigate, Act or Act on servers." });
  const syncNoticeEnabled = () => {
    const owns = controls.level.value === "1" || controls.level.value === "2" || controls.level.value === "3";
    controls.reportNotice.disabled = !owns;
    /* The author rule `.mute-label { display: block }` outranks the browser's [hidden] rule, so the attribute alone leaves the hint on screen
       (as setShown in admin.js notes); the inline display is what hides it. */
    noticeHint.hidden = owns;
    noticeHint.style.display = owns ? "none" : "";
  };
  syncNoticeEnabled();
  controls.level.addEventListener("input", syncNoticeEnabled);
  /* The webhook boxes never hold a saved value (the page is never given one): a box is for a NEW value, and Remove is for a saved one. */
  const hookBoxes = {};
  const removeBoxes = {};
  const hookFields = WEBHOOK_DESTINATIONS.map((d) => {
    const isSaved = entries.saved.includes(d.channel);
    const box = el("input", {
      type: "password", class: "tag-input", autocomplete: "off", "data-field": d.key + "Hook",
      placeholder: isSaved ? "saved - leave blank to keep" : "none",
    });
    box.value = (entries.hooks && entries.hooks[d.key]) || "";
    hookBoxes[d.key] = box;
    const parts = [el("span", { class: "mute-label", text: d.label + (isSaved ? " (saved):" : " (none):") }), box];
    if (isSaved) {
      const remove = el("input", { type: "checkbox", "data-field": d.key + "Remove" });
      remove.checked = !!(entries.removes && entries.removes[d.key]);
      removeBoxes[d.key] = remove;
      parts.push(remove, el("span", { text: " Remove" }));
    }
    return el("label", { class: "tag-field" }, parts);
  });
  const recipients = text("recipients", entries.recipients, { placeholder: "name@example.com, other@example.com" });
  const read = () => {
    const e = {
      /* #6936: modifiedAt rides along, or an edit goes out without expected_modified_at and the service refuses it with a 400. */
      id: entries.id, modifiedAt: entries.modifiedAt, enabled: enabled.checked, saved: entries.saved, recipientsWas: entries.recipientsWas, hooks: {}, removes: {},
    };
    for (const k of Object.keys(controls)) e[k] = controls[k].value;
    for (const d of WEBHOOK_DESTINATIONS) {
      e.hooks[d.key] = hookBoxes[d.key].value;
      e.removes[d.key] = removeBoxes[d.key] ? removeBoxes[d.key].checked : false;
    }
    e.recipients = recipients.value;
    return e;
  };
  const sync = () => { routeEntries = read(); };
  for (const c of [...Object.values(controls), enabled, recipients, ...Object.values(hookBoxes), ...Object.values(removeBoxes)]) c.addEventListener("input", sync);
  const save = el("button", {
    class: "btn primary", type: "button", "data-action": "save-route", text: entries.id == null ? "Add route" : "Save route " + entries.id,
    onClick: () => sendRoute("save", read(), options.repaint),
  });
  const parts = [
    field("Match:", controls.match), field("Applies to:", controls.scope), field("Severity:", controls.severities), field("Agent:", controls.level), el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Report notice:" }), controls.reportNotice, noticeHint]),
    field("Action approval:", controls.approval), field("Single-person approval:", controls.singlePerson),
    field("Requests per hour:", controls.perHour), field("Requests per day:", controls.perDay),
    field("Lease minutes:", controls.lease), field("Expiry minutes:", controls.expiry),
    field("Send resolutions:", controls.resolutions), field("Fallback route:", controls.fallback),
    ...hookFields, field("Email recipients:", recipients),
    el("label", { class: "tag-field" }, [enabled, el("span", { text: " Enabled" })]), save,
  ];
  if (entries.id != null) {
    parts.push(el("button", {
      class: "btn", type: "button", text: "Cancel", onClick: () => { routeEntries = null; return options.repaint(); },
    }));
  }
  return el("div", { class: "tag-form", "data-route-form": "1" }, parts);
}

/** The nodes the Routes tab shows above its grid for an editing seat: the status of the last write and the form. */
export function routeEditorNodes(options) {
  const nodes = [];
  const status = statusNode(routeStatus);
  if (status) nodes.push(status);
  nodes.push(noticeStrip(ROUTES_EDITING_NOTE));
  nodes.push(routeForm(options));
  return nodes;
}

/** The nodes for a seat that lists only. */
/* #6274: a seat that is not a signed-in admin lists only, and the service says why in its own sentence (sign in as an admin, or use the Viewer). */
export function routeListOnlyNodes(message, managed, adminWritesOff) {
  const status = statusNode(routeStatus);
  return (status ? [status] : []).concat([noticeStrip(message || routesListOnly(managed, adminWritesOff))]);
}

/** The Routes grid's extra columns: Agent use for everyone, Change (Edit and Delete) for an editing seat. */
export function routeColumns(base, options) {
  const columns = base.concat([{ key: "agent_use", label: "Agent use", wrap: true }]);
  if (!options.canEdit) return columns;
  const actions = (row) => {
    const source = options.routes.find((r) => r.route_id === row.route_id);
    const edit = el("button", {
      class: "btn", type: "button", text: "Edit", "aria-label": "Edit route " + row.route_id, "data-action": "edit-route",
      onClick: () => { routeEntries = entriesFromRoute(source); return options.repaint(); },
    });
    const remove = el("button", {
      class: "btn", type: "button", text: "Delete", "aria-label": "Delete route " + row.route_id, "data-action": "delete-route",
      onClick: () => {
        if (!window.confirm("Delete route " + row.route_id + "? Routes that name it as their fallback lose that fallback. This cannot be undone.")) return undefined;
        return sendRoute("delete", { id: row.route_id }, options.repaint);
      },
    });
    return el("span", { class: "row-actions" }, [edit, remove]);
  };
  return columns.concat([{ key: "actions", label: "Change", csv: false, copy: false, render: actions }]);
}

/** Whether the seat may edit: the session says it can make changes AND the store login holds the grants. */
export async function readAgentSetupAccess(session) {
  const managed = !!session && session.managed_store === true;
  const adminWritesOff = !!session && session.admin_writes_off === true;
  if (!session || session.can_edit !== true) return { routes: false, profiles: false, managed, adminWritesOff };
  const res = await apiGet("/api/agent-setup/access");
  const d = res && res.kind === "data" ? res.data : null;
  return { routes: !!d && d.can_edit_routes === true, profiles: !!d && d.can_edit_profiles === true, managed, adminWritesOff, message: d && typeof d.sign_in_message === "string" ? d.sign_in_message : null };
}

/* ───────────────────────────── Environment Profiles ───────────────────────────── */

export const profilesListOnly = (managed, adminWritesOff) => listOnlyNote(managed, "profiles are listed only", "To change one", adminWritesOff);
export const PROFILES_NOTE =
  "An environment profile is free text every agent brief carries for its scope: the fleet, one server or one tag. Write the server's role, " +
  "business hours, owners, read limits and patterns known to be noisy. Saving blank text removes the scope's profile. Every change is journaled.";

export const PROFILE_COLUMNS = [
  { key: "scope", label: "Scope" },
  { key: "profile_text", label: "Profile", wrap: true },
  { key: "modified_at_utc", label: "Saved", format: "time" },
  { key: "modified_by", label: "By" },
];

/** The scope's words: Fleet, or the server's or tag's name (its id when the name is not known). */
export function profileScopeText(p, servers, tags) {
  if (p.scope_server_id != null) return "Server: " + ((servers.find((s) => s.id === p.scope_server_id) || {}).name || "id " + p.scope_server_id);
  if (p.scope_tag_id != null) return "Tag: " + ((tags.find((t) => t.id === p.scope_tag_id) || {}).name || "id " + p.scope_tag_id);
  return "Fleet";
}

/** What a profile write answer says: { message, isError }. */
export function profileOutcome(res) {
  if (!res || res.expired) return { message: null, isError: true };
  if (res.status === 0) return { message: res.message || "Network error.", isError: true };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && typeof body.status === "string") {
    return {
      message: body.status === "saved" ? "Profile saved." : body.status === "removed" ? "Profile removed." : "Nothing to remove: that scope has no profile.",
      isError: false,
    };
  }
  const said = body && typeof body.error === "string" && body.error ? body.error : "";
  return { message: "The profile was not saved." + (said ? " " + said : " The service answered " + res.status + "."), isError: true };
}

let profileScope = "";
let profileText = null;
let profileStatus = null;
let profileBusy = false;

/** The scope picker's value as the write's two ids. */
export function profileScopeIds(value) {
  return {
    scope_server_id: String(value).startsWith("s") ? Number(String(value).slice(1)) : null,
    scope_tag_id: String(value).startsWith("t") ? Number(String(value).slice(1)) : null,
  };
}

export async function buildProfiles(body, generation, ctx) {
  const [session, res, fleet] = await Promise.all([ctx.getSession(), apiGet("/api/environment-profiles"), ctx.readScopes()]);
  if (!ctx.isCurrent(generation)) return;
  const bad = ctx.failure(res);
  if (bad) return mount(body, bad);
  const d = res.data || {};
  const canEdit = session.can_edit === true && d.can_edit === true;
  const profiles = Array.isArray(d.profiles) ? d.profiles : [];
  const nodes = [];
  const status = statusNode(profileStatus);
  if (status) nodes.push(status);
  nodes.push(noticeStrip(canEdit ? PROFILES_NOTE : (typeof d.sign_in_message === "string" && d.sign_in_message) || profilesListOnly(session.managed_store === true, session.admin_writes_off === true)));
  if (canEdit) nodes.push(profileForm(profiles, fleet, d.max_length || 4000, ctx));
  const rows = profiles.map((p) => ({ ...p, scope: profileScopeText(p, fleet.servers, fleet.tags) }));
  nodes.push(rows.length
    ? VIZ.table({ profiles: rows }, { rowsKey: "profiles", columns: PROFILE_COLUMNS })
    : emptyStrip("No environment profile is set. Agents get no description of the environment."));
  mount(body, nodes);
}

function profileForm(profiles, fleet, maxLength, ctx) {
  const choices = [{ value: "", label: "Fleet" }]
    .concat(fleet.servers.map((s) => ({ value: "s" + s.id, label: "Server: " + s.name })))
    .concat(fleet.tags.map((t) => ({ value: "t" + t.id, label: "Tag: " + t.name })));
  const scope = el("select", { class: "tag-input", "data-field": "profile-scope" }, choices.map((o) => el("option", { value: o.value, text: o.label })));
  scope.value = profileScope;
  const textBox = el("textarea", { class: "tag-input", rows: "8", maxlength: String(maxLength), "data-field": "profile-text" });
  const profileOf = (value) => {
    const ids = profileScopeIds(value);
    return profiles.find((x) => (x.scope_server_id ?? null) === ids.scope_server_id && (x.scope_tag_id ?? null) === ids.scope_tag_id);
  };
  const savedOf = (value) => profileOf(value)?.profile_text ?? "";
  textBox.value = profileText !== null ? profileText : savedOf(profileScope);
  scope.addEventListener("change", () => {
    profileScope = scope.value;
    profileText = null;
    textBox.value = savedOf(scope.value);
  });
  textBox.addEventListener("input", () => { profileText = textBox.value; });
  const save = el("button", {
    class: "btn primary", type: "button", text: "Save profile", "data-action": "save-profile",
    onClick: async () => {
      if (profileBusy) return;
      profileBusy = true;
      let out;
      try {
        /* The version the page read for this scope (#6274): a profile saved by someone else since is refused with 409, not overwritten.
           A scope with no profile sends none, and the store refuses it if a profile has appeared. */
        const read = profileOf(scope.value);
        const res = await apiWrite("PUT", "/api/environment-profiles", {
          ...profileScopeIds(scope.value), profile_text: textBox.value,
          ...(read ? { expected_modified_at: read.modified_at_utc } : {}),
        });
        out = profileOutcome(res);
        /* After a refusal the draft is dropped, so the repaint shows what is saved now and the next save carries its version. */
        if (res && res.status === 409) profileText = null;
      } catch (e) {
        out = { message: "Could not save the profile: " + (e && e.message ? e.message : String(e)), isError: true };
      }
      profileBusy = false;
      if (out.message) profileStatus = { message: out.message, isError: out.isError };
      if (out.isError === false) profileText = null;
      await ctx.repaint();
    },
  });
  return el("div", { class: "tag-form", "data-profile-form": "1" }, [
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Scope:" }), scope]),
    el("label", { class: "tag-field" }, [el("span", { class: "mute-label", text: "Profile:" }), textBox]),
    save,
  ]);
}
