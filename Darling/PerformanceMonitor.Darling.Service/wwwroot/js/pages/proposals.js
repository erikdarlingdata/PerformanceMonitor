/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* Proposals (#7109): the server actions an agent proposed on a route that needs approval, and the two presses that approve one.
     GET  /api/proposals                       {can_press, presses_as, person_name, press_note, proposals: [...]}, newest first
     GET  /api/proposals/{id}                  {can_press, presses_as, person_name, press_note, proposal: {...}}
     POST /api/proposals/{id}/first-press      {}  ->  {status, code, message, facts, changed}
     POST /api/proposals/{id}/confirm          {}  ->  the same
   #/proposals is the list and #/proposals?proposal=<id> one proposal (the link a proposal notice carries).
   The first press reads the facts again from the server and shows them. The confirm reads them again, compares, and runs the action.
   The same person makes both presses. The server works out who is pressing from the sign-in, so the body sends nothing.
   Every value here is drawn as text. The agent's reason is labelled as the agent's, and a program or host name a client set is plain text. */

import { el, mount, loadingStrip, emptyStrip, noticeStrip, errorStrip, apiGet, apiWrite, localTime } from "../util.js";

/** The words for each server action kind. */
export const KIND_LABELS = {
  force_plan: "Force or unforce a plan",
  evict_plan: "Evict a cached plan",
  kill_session: "Kill a session",
  recompile_module: "Recompile a module",
  create_plan_guide: "Create a plan guide",
  drop_plan_guide: "Drop a plan guide",
};

/** The words for each proposal state. */
export const STATE_LABELS = {
  pending: "Waiting for a person",
  approved: "Approved, running",
  run: "Run",
  failed: "Failed",
  unknown: "Outcome unknown",
  refused: "Refused",
  expired: "Expired",
  superseded: "Superseded: the facts changed",
  voided: "Voided",
};

/** The proposal id in a #/proposals?proposal=<id> query, or "". */
export function proposalIdFromQuery(query) {
  const id = new URLSearchParams(query || "").get("proposal");
  return id ? id.trim() : "";
}

/** One fact as a line of text: objects and arrays as JSON, null as a dash. */
function factValue(v) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/** The lines of a facts object (a target, a binding, the facts a press read), in key order. */
export function factLines(facts) {
  if (!facts || typeof facts !== "object") return [];
  return Object.keys(facts).map((k) => k + ": " + factValue(facts[k]));
}

/** A short target for the list. The service words it for each tool (p.target_text) from the masked target. A proposal without that line falls back to the target's values joined. */
export function targetLine(p) {
  return p.target_text || targetText(p.target);
}

/** The target's values joined, as key and value pairs. */
export function targetText(target) {
  if (!target || typeof target !== "object") return "—";
  const parts = Object.keys(target).map((k) => k + " " + factValue(target[k]));
  return parts.length ? parts.join(", ") : "—";
}

/** Who pressed: the person's display name, or the administrator path in words. The stored key (person:13) is only the last resort. */
export function pressName(name, key) {
  return name || key || "";
}

/** Who made the first press, in words. */
export function pressedByText(p) {
  if (!p.first_press_by) return "No press yet";
  return pressName(p.first_press_name, p.first_press_by) + " at " + localTime(p.first_press_at_utc);
}

const factList = (facts, role) => el("ul", { class: "fact-list", "data-role": role }, factLines(facts).map((t) => el("li", { text: t })));

const field = (label, value, role) => el("div", { class: "kv", "data-role": role }, [el("strong", { text: label + ": " }), el("span", { text: value })]);

let pageSeq = 0;

/** The last press answer on the page, by proposal id, so it stays in view after the page reads the proposal again. */
const lastPress = new Map();

/** The page: the list, or one proposal. */
export function renderProposals(main, proposalId) {
  const mine = ++pageSeq;
  const body = el("div", {}, [loadingStrip(proposalId ? "Loading the proposal…" : "Loading proposals…")]);
  mount(main, [
    el("div", { class: "page-head" }, [el("h2", { text: proposalId ? "Proposal" : "Proposals" })]),
    el("div", { class: "muted", text: "Server actions an agent proposed on a route that needs approval. A person approves one in two presses: the first reads the facts again, the second confirms and runs it." }),
    body,
  ]);
  const isCurrent = () => mine === pageSeq;
  return proposalId ? buildProposal(body, proposalId, isCurrent) : buildList(body, isCurrent);
}

/** The list, newest first. */
export async function buildList(body, isCurrent = () => true) {
  const res = await apiGet("/api/proposals");
  if (!isCurrent()) return;
  if (res.kind !== "data") return mount(body, errorStrip(res.message || "The proposals could not be read."));
  const data = res.data || {};
  const rows = Array.isArray(data.proposals) ? data.proposals : [];
  const nodes = [];
  if (!data.can_press && data.press_note) nodes.push(noticeStrip(data.press_note));
  if (!rows.length) return mount(body, nodes.concat([emptyStrip("No proposals.")]));
  const head = el("tr", {}, ["Kind", "Server", "Target", "State", "Proposed", "Expires", "First press"].map((t) => el("th", { text: t })));
  const lines = rows.map((p) => el("tr", { "data-role": "proposal-row" }, [
    el("td", {}, [el("a", { href: "#/proposals?proposal=" + encodeURIComponent(p.proposal_id), text: KIND_LABELS[p.kind] || p.kind })]),
    el("td", { text: p.server_name || "Server " + p.server_id }),
    el("td", { text: targetLine(p) }),
    el("td", { text: STATE_LABELS[p.state] || p.state }),
    el("td", { text: localTime(p.proposed_at_utc) }),
    el("td", { text: localTime(p.expires_at_utc) }),
    el("td", { text: pressedByText(p) }),
  ]));
  return mount(body, nodes.concat([el("div", { class: "table-wrap" }, [el("table", { class: "data proposals" }, [el("thead", {}, [head]), el("tbody", {}, lines)])])]));
}

/** What a press answer shows: its message, what changed for a superseded proposal, and the facts the press read. */
export function pressResultNodes(result) {
  if (!result) return [];
  const answer = result.body || {};
  const message = answer.message || result.message || "The press did not answer.";
  const nodes = [];
  if (answer.status === "first_pressed" || answer.status === "run") nodes.push(noticeStrip(message));
  else nodes.push(errorStrip(message));
  if (answer.status === "superseded" && Array.isArray(answer.changed) && answer.changed.length) {
    nodes.push(field("What changed", answer.changed.join(", "), "changed"));
  }
  if (answer.facts) nodes.push(el("div", { class: "kv" }, [el("strong", { text: "The facts this press read:" })]), factList(answer.facts, "press-facts"));
  return nodes;
}

/** One proposal, with the presses for a session that can press. */
export async function buildProposal(body, id, isCurrent = () => true) {
  const res = await apiGet("/api/proposals/" + encodeURIComponent(id));
  if (!isCurrent()) return;
  if (res.kind !== "data") return mount(body, errorStrip(res.message || "The proposal could not be read."));
  const data = res.data || {};
  const p = data.proposal || {};
  const nodes = [
    el("div", {}, [el("a", { href: "#/proposals", text: "All proposals" })]),
    field("Kind", KIND_LABELS[p.kind] || p.kind, "kind"),
    field("Server", p.server_name || "Server " + p.server_id, "server"),
    field("State", STATE_LABELS[p.state] || p.state, "state"),
    el("div", { class: "kv" }, [el("strong", { text: "Target:" })]),
    factList(p.target, "target"),
    field("The agent's reason (written by the agent " + (p.agent || "") + ")", p.agent_reason || "—", "agent-reason"),
    el("div", { class: "kv", "data-role": "request" }, [
      el("strong", { text: "Request: " }),
      el("a", { href: "#/investigations?request=" + encodeURIComponent(p.request_id), text: p.request_id }),
    ]),
    field("Proposed", localTime(p.proposed_at_utc), "proposed"),
    field("Expires", localTime(p.expires_at_utc), "expires"),
    field("First press", pressedByText(p), "first-press"),
    el("div", { class: "kv" }, [el("strong", { text: "The facts the agent saw (the binding):" })]),
    factList(p.binding, "binding"),
  ];
  if (p.first_press_facts) nodes.push(el("div", { class: "kv" }, [el("strong", { text: "The facts the first press read:" })]), factList(p.first_press_facts, "first-press-facts"));
  if (p.decided_by) nodes.push(field("Decided by", pressName(p.decided_by_name, p.decided_by) + " at " + localTime(p.decided_at_utc), "decided"));
  if (p.outcome) nodes.push(el("div", { class: "kv" }, [el("strong", { text: "Outcome:" })]), factList(p.outcome, "outcome"));
  nodes.push(...pressResultNodes(lastPress.get(p.proposal_id)));

  if (!data.can_press) {
    if (data.press_note) nodes.push(noticeStrip(data.press_note));
  } else if (p.state === "pending") {
    if (data.press_note) nodes.push(noticeStrip(data.press_note));
    else if (data.person_name) nodes.push(noticeStrip("You press as " + data.person_name + "."));
    const press = (verb) => async () => {
      const result = await apiWrite("POST", "/api/proposals/" + encodeURIComponent(p.proposal_id) + "/" + verb, {});
      lastPress.set(p.proposal_id, result);
      return buildProposal(body, id, isCurrent);
    };
    const buttons = [el("button", { class: "btn", type: "button", "data-action": "first-press", text: p.first_press_by ? "Read the facts again" : "First press: read the facts", onClick: press("first-press") })];
    if (p.first_press_by) buttons.push(el("button", { class: "btn", type: "button", "data-action": "confirm", text: "Confirm and run", onClick: press("confirm") }));
    nodes.push(el("div", { class: "row-actions proposal-actions" }, buttons));
  }
  return mount(body, nodes);
}
