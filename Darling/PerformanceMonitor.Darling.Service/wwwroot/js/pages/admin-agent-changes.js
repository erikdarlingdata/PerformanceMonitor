/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Admin > Agent changes (#5917): what agents changed, what they were refused, and a one-click Undo, with the switch that turns
   every agent change off.
     GET /api/agent-changes                       the calls, the refusals, the switch and can_decide
     PUT /api/agent-changes/{id}/undo             restore the before image, only when nobody changed it since. A plan force or unforce
                                                  answers {status:"confirm", statement, state} first and writes only on {confirm:true, statement}
     PUT /api/agent-changes/switch                {"enabled": true|false}
   The tab decides only under the admin store login. Under the viewer role (managed and compose stores) the list still shows and
   the page says so; a decision the store refuses answers 403 with its own sentence, which is shown as it came.
   Self-contained: admin.js hands this file its helpers, so a change here never touches that file's tab code. */

import { VIZ } from "../panels.js";
import { el, mount, errorStrip, emptyStrip, noticeStrip, listOnlyNote, apiGet, apiWrite } from "../util.js";

export const AGENT_CHANGE_COLUMNS = [
  { key: "when", label: "When", format: "time" },
  { key: "server", label: "Server" },
  { key: "kind", label: "Kind" },
  { key: "tool", label: "Tool" },
  { key: "actor", label: "Agent" },
  { key: "reason", label: "Why", wrap: true },
  { key: "result", label: "Result" },
  { key: "muted", label: "Muted since" },
  { key: "review", label: "Review" },
];

const listOnly = (managed, adminWritesOff) => listOnlyNote(managed, "this page lists agent changes only", "To undo a change or switch agent changes off", adminWritesOff);
const CAN_DECIDE =
  "Agent changes, newest first. Undo restores what the setting was before the agent's change, but only if nobody has changed it since; " +
  "otherwise it says so and leaves the setting alone. Undo works for 365 days. An evicted plan has no undo. The next run compiles a new plan. A killed session has no undo.";

/** The Kind column's words. The Level 2 kinds read as the journal names them; a plan force on a monitored server (#5632) says so in plain words. */
export function agentKindText(kind) {
  if (kind === "plan_force") return "force plan";
  if (kind === "plan_unforce") return "unforce plan";
  if (kind === "plan_evict") return "evict plan";
  if (kind === "session_kill") return "kill session"; // #6310: the Darling Viewer's word for it
  if (kind === "module_recompile") return "recompile module"; // #6569
  if (kind === "plan_guide_create") return "create plan guide"; // #6569
  if (kind === "plan_guide_drop") return "drop plan guide"; // #6569
  return kind || null;
}

/** What the Result column says. A call with no done or failed row is "result unknown": the change may or may not have happened. */
export function agentResultText(change) {
  if (change.undone_at_utc) return "undone by " + (change.undone_by || "a person");
  if (change.result === "done") return "done";
  if (change.result === "failed") {
    const detail = change.result_detail && typeof change.result_detail === "object" ? change.result_detail : null;
    return "failed" + (detail && typeof detail.error === "string" && detail.error ? ": " + detail.error : "");
  }
  return "result unknown";
}

/** The Server column (#6310): the registry's name the read sends, else the id marked removed, else empty. */
export function agentServerText(row) {
  if (row && typeof row.server_name === "string" && row.server_name) return row.server_name;
  return row && Number.isInteger(row.server_id) ? "Server " + row.server_id + " (removed)" : "";
}

/** The rows of the changes grid from the list read's body. */
export function agentChangeRows(data) {
  const changes = data && Array.isArray(data.changes) ? data.changes : [];
  return changes.map((c) => ({
    called_id: c.called_id,
    when: c.called_at_utc,
    server: agentServerText(c),
    kind: agentKindText(c.kind),
    tool: c.tool || null,
    actor: c.actor,
    reason: c.reason || null,
    result: agentResultText(c),
    muted: Number.isInteger(c.muted_alerts) ? String(c.muted_alerts) + " alerts" : null,
    review: typeof c.force_review === "string" && c.force_review ? c.force_review : null,
    can_undo: c.can_undo === true,
    is_plan: c.kind === "plan_force" || c.kind === "plan_unforce",
  }));
}

/** The refusals as rows: what an agent asked for and the code it was told. */
export function agentRefusalRows(data) {
  const refusals = data && Array.isArray(data.refusals) ? data.refusals : [];
  return refusals.map((r) => ({ when: r.at_utc, server: agentServerText(r), tool: r.tool || null, actor: r.actor, code: r.code || null }));
}

/** An undo's answer in plain words: { message, isError, reread }. A refusal (409) keeps the store's own sentence. */
export function agentUndoOutcome(res) {
  if (!res || res.expired) return { message: null, isError: true, reread: false };
  if (res.status === 0) return { message: res.message || "Network error.", isError: true, reread: false };
  const body = res.body && typeof res.body === "object" ? res.body : null;
  if (res.status === 200 && body && body.status === "undone") return { message: body.message || "Undone.", isError: false, reread: true };
  /* #5632 step 9: a plan force or unforce is shown first. The page asks the person to confirm the statement before it sends the second request. */
  if (res.status === 200 && body && body.status === "confirm" && typeof body.statement === "string") {
    return { message: null, isError: false, reread: false, confirm: { statement: body.statement, message: body.message || "", state: body.state || null } };
  }
  const said = body && typeof body.error === "string" ? body.error : "";
  if (res.status === 403 || res.status === 409 || res.status === 404) return { message: said || "The change was not undone.", isError: true, reread: res.status !== 403 };
  return { message: "The change was not undone. The service answered " + res.status + "." + (said ? " " + said : ""), isError: true, reread: false };
}

let status = null;
let busy = false;
let pending = null;

/** The state the server reported, in a line a person can read: the plan forced now, or that none is. */
export function agentStateText(state) {
  const forced = state && state.forced_plan && Number.isInteger(state.forced_plan.plan_id) ? state.forced_plan : null;
  const where = state && typeof state.database === "string" ? " (query " + state.query_id + " in " + state.database + ")" : "";
  return (forced ? "Plan " + forced.plan_id + " is forced now" + (forced.forcing_type && forced.forcing_type !== "unknown" ? " (" + forced.forcing_type.toLowerCase() + ")" : "") : "No plan is forced now") + where + ".";
}

/** Build the tab. `ctx` carries admin.js's own helpers: { getSession, failure, isCurrent(generation), repaint() }. */
export async function buildAgentChanges(body, generation, ctx) {
  const res = await apiGet("/api/agent-changes");
  if (!ctx.isCurrent(generation)) return;
  const nodes = [];
  if (status) nodes.push(status.isError ? errorStrip(status.message) : noticeStrip(status.message));
  if (pending) nodes.push(confirmPanel(pending, ctx));
  const bad = ctx.failure(res);
  if (bad) return mount(body, nodes.concat([bad]));
  const data = res.data || {};
  const canDecide = data.can_decide === true;
  let notice = canDecide ? CAN_DECIDE : null;
  if (notice === null) {
    const session = await ctx.getSession();
    notice = listOnly(session.managed_store === true, session.admin_writes_off === true);
  }
  nodes.push(noticeStrip(notice));
  nodes.push(switchBar(data.enabled !== false, canDecide, ctx));
  const rows = agentChangeRows(data);
  const columns = AGENT_CHANGE_COLUMNS.concat(canDecide ? [{
    key: "undo", label: "Undo", csv: false, copy: false,
    render: (row) => (row && row.can_undo ? el("button", {
      class: "btn", type: "button", text: "Undo", "aria-label": "Undo agent change " + row.called_id,
      onClick: () => undo(row.called_id, ctx),
    }) : null),
  }] : []);
  nodes.push(VIZ.table({ changes: rows }, { rowsKey: "changes", columns, emptyText: "No agent has changed anything." }));
  const refusals = agentRefusalRows(data);
  nodes.push(el("h3", { text: "Refusals" }));
  nodes.push(refusals.length === 0 ? emptyStrip("No agent change was refused.") : VIZ.table({ refusals }, {
    rowsKey: "refusals",
    columns: [
      { key: "when", label: "When", format: "time" }, { key: "server", label: "Server" }, { key: "tool", label: "Tool" },
      { key: "actor", label: "Agent" }, { key: "code", label: "Refused because" },
    ],
    emptyText: "No agent change was refused.",
  }));
  mount(body, nodes);
}

function switchBar(enabled, canDecide, ctx) {
  const label = enabled ? "Agent changes are ON." : "Agent changes are OFF: every agent change is refused (actions_off).";
  const children = [el("span", { text: label })];
  if (canDecide) {
    children.push(el("button", {
      class: "btn" + (enabled ? "" : " primary"), type: "button", text: enabled ? "Turn off agent changes" : "Turn on agent changes",
      onClick: () => flip(!enabled, ctx),
    }));
  }
  return el("div", { class: "row-actions" }, children);
}

async function flip(enabled, ctx) {
  if (busy) return;
  busy = true;
  try {
    const res = await apiWrite("PUT", "/api/agent-changes/switch", { enabled });
    if (res && res.status === 200) status = { message: enabled ? "Agent changes are on." : "Agent changes are off.", isError: false };
    else if (res && !res.expired) status = { message: (res.body && res.body.error) || "The switch was not changed.", isError: true };
  } finally {
    busy = false;
  }
  ctx.repaint();
}

/** The confirm step of a plan undo: the exact statement, the state read on the server, and a button that sends the second request. */
function confirmPanel(p, ctx) {
  return el("div", { class: "notice" }, [
    el("p", { text: p.message }),
    el("p", { text: agentStateText(p.state) }),
    el("pre", { text: p.statement }),
    el("div", { class: "row-actions" }, [
      el("button", { class: "btn primary", type: "button", text: "Confirm undo", onClick: () => undo(p.calledId, ctx, { confirm: true, statement: p.statement }) }),
      el("button", { class: "btn", type: "button", text: "Cancel", onClick: () => { pending = null; ctx.repaint(); } }),
    ]),
  ]);
}

async function undo(calledId, ctx, sent) {
  if (busy) return;
  busy = true;
  let out;
  try {
    out = agentUndoOutcome(await apiWrite("PUT", "/api/agent-changes/" + encodeURIComponent(calledId) + "/undo", sent || {}));
    pending = out.confirm ? { calledId, ...out.confirm } : null;
  } catch (e) {
    pending = null;
    out = { message: "Could not undo: " + (e && e.message ? e.message : String(e)), isError: true, reread: false };
  }
  busy = false;
  if (out.message) status = { message: out.message, isError: out.isError };
  ctx.repaint();
}

/** Clears the last answer; admin.js calls it when the person leaves the tab. */
export function resetAgentChangesStatus() {
  status = null;
  pending = null;
}
