/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Admin > Server actions (#6269): each server's agent action switches, who changed each and when, and an on/off control.
     GET /api/server-actions                          {can_decide, signed_in_admin, servers: [{server_id, server_name, engine, engine_kind, actions: [{action, valid, enabled, set_by, updated_at_utc}]}]}
     PUT /api/servers/{id}/actions/{action}           {"enabled": true|false}  ->  200 {status: set|unchanged, message}; 403 viewer role; 404; 409 wrong engine
   Every switch is off until a person turns it on. Turning one ON shows a plain warning first and writes only after the person confirms it.
   The tab decides only under the admin store login. Under the viewer role (managed and compose stores) the list still shows and the page says so;
   a change the store refuses answers 403 with its own sentence, which is shown as it came.
   Self-contained: admin.js hands this file its helpers, so a change here never touches that file's tab code. */

import { VIZ } from "../panels.js";
import { el, mount, errorStrip, noticeStrip, apiGet, apiWrite } from "../util.js";

export const SERVER_ACTION_COLUMNS = [
  { key: "server", label: "Server" },
  { key: "engine", label: "Engine" },
  { key: "action", label: "Action" },
  { key: "state", label: "Switch" },
  { key: "set_by", label: "Changed by" },
  { key: "changed", label: "Changed", format: "time" },
  { key: "note", label: "Note" },
];

const ENGINE_CANNOT_RUN =
  "This server's engine can't run this action. Turn it off to clear it.";

const LIST_ONLY =
  "This dashboard signs in to the store as the viewer role (managed and compose stores), so this page lists the switches only. " +
  "To turn one on or off, use the Viewer app, or run the web under the admin login.";
const NOT_SIGNED_IN =
  "Sign in to the dashboard as an admin to turn a switch on or off; this page lists the switches only. A dashboard with no sign-in configured " +
  "never changes them. Or use the Viewer app.";
const CAN_DECIDE =
  "Each server's agent action switches. Every switch is off until a person turns it on. A switch that is on lets agents change that server, " +
  "and it also needs the remediation login and its grants (see the Darling README). Every agent change is journaled and can be undone where that is possible. " +
  "Turning a switch on or off is recorded with your name.";

/** An action in plain words. */
export function serverActionLabel(action) {
  if (action === "force_plan") return "Force or unforce plans";
  if (action === "evict_plan") return "Evict plans from the plan cache";
  if (action === "kill_session") return "Kill sessions";
  return action;
}

function serverActionLets(action) {
  if (action === "force_plan") return "force or unforce a Query Store plan";
  if (action === "evict_plan") return "evict a plan from the plan cache";
  if (action === "kill_session") return "kill a session";
  return action;
}

/** What the person reads before a switch is turned ON: what an agent may then do, what it needs, and that it is journaled. */
export function serverActionWarning(serverName, action) {
  return "Turning this on lets agents change " + serverName + ": they will be able to " + serverActionLets(action) + " on it. " +
    "It also needs the remediation login for this server, with the grants the Darling README describes. " +
    "Every agent change is journaled, and can be undone where that is possible.";
}

/** One row per server and per action its engine can run, plus a switch that is on but that the engine cannot run (shown with a note; it can only be turned off). */
export function serverActionRows(data) {
  const servers = data && Array.isArray(data.servers) ? data.servers : [];
  const rows = [];
  for (const s of servers) {
    /* The service parses the registry's spelling (pg, aurora, mixed case) with the shared parser and answers engine_kind; this page never parses it. */
    const postgres = s.engine_kind === "postgresql";
    for (const a of Array.isArray(s.actions) ? s.actions : []) {
      if (a.valid !== true && a.enabled !== true) continue;
      rows.push({
        server_id: s.server_id,
        server_name: s.server_name,
        server: s.server_name,
        engine: postgres ? "PostgreSQL" : "SQL Server",
        action: serverActionLabel(a.action),
        action_key: a.action,
        can_run: a.valid === true,
        note: a.valid === true ? "" : ENGINE_CANNOT_RUN,
        enabled: a.enabled === true,
        state: a.enabled === true ? "ON" : "off",
        set_by: a.set_by || null,
        changed: a.updated_at_utc || null,
      });
    }
  }
  return rows;
}

/** A change's answer in plain words: { message, isError }. A refusal keeps the store's own sentence. */
export function serverActionOutcome(res) {
  if (!res || res.expired) return { message: null, isError: true };
  if (res.status === 0) return { message: res.message || "Network error.", isError: true };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && (body.status === "set" || body.status === "unchanged")) return { message: body.message || "Done.", isError: false };
  const said = body && typeof body.message === "string" ? body.message : body && typeof body.error === "string" ? body.error : "";
  if ([400, 403, 404, 409, 503].includes(res.status)) return { message: said || "The switch was not changed.", isError: true };
  return { message: "The switch was not changed. The service answered " + res.status + "." + (said ? " " + said : ""), isError: true };
}

let status = null;
let busy = false;
let pending = null;

/** Build the tab. `ctx` carries admin.js's own helpers: { getSession, failure, isCurrent(generation), repaint() }. */
export async function buildServerActions(body, generation, ctx) {
  const res = await apiGet("/api/server-actions");
  if (!ctx.isCurrent(generation)) return;
  const nodes = [];
  if (status) nodes.push(status.isError ? errorStrip(status.message) : noticeStrip(status.message));
  if (pending) nodes.push(warningPanel(pending, ctx));
  const bad = ctx.failure(res);
  if (bad) return mount(body, nodes.concat([bad]));
  const data = res.data || {};
  const canDecide = data.can_decide === true;
  nodes.push(noticeStrip(canDecide ? CAN_DECIDE : data.signed_in_admin === false ? NOT_SIGNED_IN : LIST_ONLY));
  const columns = SERVER_ACTION_COLUMNS.concat(canDecide ? [{
    key: "change", label: "Change", csv: false, copy: false,
    render: (row) => (row ? el("button", {
      class: "btn", type: "button", text: row.enabled ? "Turn off" : "Turn on...",
      "aria-label": (row.enabled ? "Turn off " : "Turn on ") + row.action + " on " + row.server_name,
      onClick: () => (row.enabled ? change(row, false, ctx) : ask(row, ctx)),
    }) : null),
  }] : []);
  nodes.push(VIZ.table({ switches: serverActionRows(data) }, { rowsKey: "switches", columns, emptyText: "No server is registered." }));
  mount(body, nodes);
}

/** Turning a switch ON asks first: what it lets an agent do, what it needs, and that it is journaled. */
function ask(row, ctx) {
  if (busy) return;
  pending = { row };
  status = null;
  ctx.repaint();
}

function warningPanel(p, ctx) {
  const row = p.row;
  return el("div", { class: "notice", role: "alertdialog", "aria-label": "Turn on " + row.action + " on " + row.server_name }, [
    el("p", { class: "mute-label", text: "Turn on: " + row.action + " on " + row.server_name }),
    el("p", { text: serverActionWarning(row.server_name, row.action_key) }),
    el("div", { class: "row-actions" }, [
      el("button", { class: "btn primary", type: "button", text: "Turn it on", onClick: () => change(row, true, ctx) }),
      el("button", { class: "btn", type: "button", text: "Cancel", onClick: () => { pending = null; ctx.repaint(); } }),
    ]),
  ]);
}

async function change(row, enabled, ctx) {
  if (busy) return;
  busy = true;
  pending = null;
  try {
    const out = serverActionOutcome(await apiWrite("PUT", "/api/servers/" + encodeURIComponent(row.server_id) + "/actions/" + encodeURIComponent(row.action_key), { enabled }));
    if (out.message) status = { message: out.message, isError: out.isError };
  } catch (e) {
    status = { message: "Could not change the switch: " + (e && e.message ? e.message : String(e)), isError: true };
  } finally {
    busy = false;
  }
  ctx.repaint();
}

/** Clears the last answer; admin.js calls it when the person leaves the tab. */
export function resetServerActionsStatus() {
  status = null;
  pending = null;
}
