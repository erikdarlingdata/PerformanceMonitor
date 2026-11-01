/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The Investigations page (#5904): the agent investigation requests the service opened, newest first, with the state
 * each one is in (open, claimed, reported, closed, expired). Two reads over /api/read, the answers of the MCP tools:
 * list_investigation_requests for the list (filtered by state and family by the read itself), and
 * get_investigation_reports for ONE request when a row is picked: its stored brief, its reports and its journal.
 *
 * Read only, except that a sign-in that can make changes may close an open, claimed or reported request with a reason (#6265, the
 * desktop Viewer's Close). Nothing here claims, releases or reports anything for an agent. The page draws exactly what
 * the read answered: the operator sees the full report and brief, as in the desktop viewer (mcp.dataDetail masks
 * agents over MCP only). A report is an agent's own claim, unreviewed and self-attributed, and the page says so.
 *
 * The list keeps the order the read gave (opened_at DESC, request_id) and the journal keeps its own (event_time
 * DESC, journal_id DESC). The filters and the picked request live at MODULE scope, so the poll's rebuild of the page shows the
 * same choices and the same open request.
 *
 * R4 (XSS): every value, the brief and the report text included, reaches the DOM as text through el()'s text path
 * (a <pre> or a cell), never as HTML.
 */

import { VIZ } from "../panels.js";
import { READ_FIELDS } from "../read-fields.js";
import { getSession } from "../views-api.js";
import { canCloseState, closeRequest, writeAccess, SIGN_IN_SENTENCE, CLOSE_REASON_MAX_LENGTH } from "../investigation-writes.js";
import { el, mount, onPageLeave, loadingStrip, emptyStrip, noticeStrip, errorStrip, readErrorStrip, readTool, apiGet, localTime, makeActivatable, rowsUnder } from "../util.js";

/** The states the list read takes; an empty value is every state. */
export const STATES = [
  { value: "", label: "All states" },
  { value: "open", label: "Open" },
  { value: "claimed", label: "Claimed" },
  { value: "reported", label: "Reported" },
  { value: "closed", label: "Closed" },
  { value: "expired", label: "Expired" },
];

/** The alert families the list read takes; an empty value is every family. */
export const FAMILIES = [
  { value: "", label: "All families" },
  { value: "blocking", label: "Blocking" },
  { value: "query_performance", label: "Query performance" },
  { value: "resources", label: "Resources" },
  { value: "availability", label: "Availability" },
  { value: "jobs", label: "Jobs" },
  { value: "self_monitor", label: "Self monitor" },
  { value: "other", label: "Other" },
];

/** The most requests one list read returns (the read takes 1 to 100). */
export const LIST_LIMIT = 100;

/** The list's columns, from READ_FIELDS (once, for the page and for a notebook cell). claimed_by is the agent id; lease_until is when its live lease ends (the claim time itself is in the journal). */
export const REQUEST_COLUMNS = READ_FIELDS.list_investigation_requests.table.columns;

/** The journal's columns, in the order the read gave them (newest first): the read's own, then the detail the page draws as text. */
export const JOURNAL_COLUMNS = [...READ_FIELDS.get_investigation_reports.table.columns, { key: "detail_text", label: "Detail", wrap: true }];

/** The text of one value: a string as it is, nothing as "", anything else as indented JSON (a masked field is an object). */
export function valueText(v) {
  if (v == null) return "";
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v, null, 2);
  } catch {
    return String(v);
  }
}

/** When the request was claimed: the newest "claimed" event of its journal, or null when it was never claimed. */
export function claimedAt(journal, requestId) {
  let best = null;
  for (const j of journal || []) {
    if (!j || j.event !== "claimed") continue;
    if (j.request_id != null && String(j.request_id) !== String(requestId)) continue;
    if (best == null || String(j.event_time || "") > String(best)) best = j.event_time;
  }
  return best;
}

/** The journal rows of one request, each with its detail as text, in the order the read gave them. */
export function journalRows(data, requestId) {
  return rowsUnder(data, "journal")
    .filter((j) => j && (j.request_id == null || String(j.request_id) === String(requestId)))
    .map((j) => ({ ...j, detail_text: j.detail == null ? "" : valueText(j.detail) }));
}

/** The reports of one request, as the read gave them. */
export function reportRows(data, requestId) {
  return rowsUnder(data, "reports").filter((r) => r && String(r.request_id) === String(requestId));
}

/** The list read's parameters for the chosen filters. A blank choice is not sent. */
export function listParams(choices) {
  const p = { limit: LIST_LIMIT };
  if (choices.state) p.state = choices.state;
  if (choices.family) p.family = choices.family;
  return p;
}

const choices = { state: "", family: "", requestId: null };
let pageSeq = 0;
let detailSeq = 0;
let pageAbort = null;
let detailAbort = null;
/* #6793: leaving the page (route() calls beginPageScope) aborts its reads. */
onPageLeave(() => {
  if (pageAbort) pageAbort.abort();
  if (detailAbort) detailAbort.abort();
});

function select(label, options, value, onPick) {
  const sel = el("select", { "aria-label": label, class: "range-select-inline" }, options.map((o) => el("option", { value: String(o.value), text: o.label })));
  sel.value = value;
  sel.addEventListener("change", () => onPick(sel.value));
  return el("label", { class: "range-control" }, [el("span", { text: label }), sel]);
}

/* #6265: the Close form of one request, drawn only for a seat that can make changes and a request that can still be closed. The
   service checks the reason and the state again; a refusal's sentence is shown as the service wrote it. */
function closeForm(row, access) {
  if (access === "no" || !canCloseState(row.state)) return null;
  if (access === "sign-in") return section("Close this request", [el("div", { class: "muted", "data-close-sign-in": "1", text: SIGN_IN_SENTENCE })]);
  const reason = el("input", { type: "text", maxlength: String(CLOSE_REASON_MAX_LENGTH), "aria-label": "Reason for closing this request", "data-close-reason": "1", placeholder: "Why is this request being closed?" });
  const status = el("div", { class: "muted", role: "status", "data-close-status": "1" });
  const button = el("button", { type: "button", class: "btn", "data-close-request": row.request_id, text: "Close request" });
  button.addEventListener("click", async () => {
    button.disabled = true;
    const out = await closeRequest(row.request_id, reason.value);
    button.disabled = false;
    if (out.message === null) return;
    status.textContent = out.message;
    if (out.ok && reloadList) await reloadList();
  });
  return section("Close this request", [
    el("div", { class: "muted", text: "Closing ends the request for good and is recorded in its journal under your name. Give a reason." }),
    el("div", {}, [reason, button]),
    status,
  ]);
}

function section(title, children) {
  return el("div", { class: "inv-section" }, [el("strong", { text: title }), ...children]);
}

function reportBlock(rep) {
  const found = rep.root_cause_found === true ? "Root cause found" : rep.root_cause_found === false ? "Root cause not found" : null;
  const head = [rep.agent_id ? "Agent " + rep.agent_id : null, rep.submitted_at ? "reported " + localTime(rep.submitted_at) : null, found]
    .filter(Boolean).join(" · ");
  const parts = [el("div", { class: "muted", text: head })];
  if (rep.verdict != null) parts.push(el("div", {}, [el("strong", { text: "Verdict: " }), el("span", { text: valueText(rep.verdict) })]));
  if (rep.summary != null) parts.push(el("div", {}, [el("strong", { text: "Summary: " }), el("pre", { class: "jh-detail-message", text: valueText(rep.summary) })]));
  if (rep.root_cause != null) parts.push(el("div", {}, [el("strong", { text: "Root cause: " }), el("pre", { class: "jh-detail-message", text: valueText(rep.root_cause) })]));
  /* #6004: the evidence arrives as verified and inferred lists (items stored before the basis was required are unlabelled);
     a store that sends one list keeps the old block. */
  const evidenceLists = [
    ["Evidence, verified", rep.evidence_verified],
    ["Evidence, inferred", rep.evidence_inferred],
    ["Evidence, unlabelled", rep.evidence_unlabelled],
    ["Evidence", rep.evidence],
  ];
  for (const [title, list] of evidenceLists) {
    const items = Array.isArray(list) ? list : [];
    if (items.length) {
      parts.push(el("div", {}, [el("strong", { text: title + " (" + items.length + "): " }), ...items.map((e) => el("pre", { class: "jh-detail-message", text: valueText(e) }))]));
    }
  }
  const queries = Array.isArray(rep.queries_run) ? rep.queries_run : [];
  if (queries.length) {
    parts.push(el("div", {}, [el("strong", { text: "Queries run (" + queries.length + "): " }), ...queries.map((q) => el("pre", { class: "jh-detail-message", text: valueText(q) }))]));
  }
  if (rep.family_of != null) parts.push(el("div", {}, [el("strong", { text: "Family of: " }), el("span", { text: valueText(rep.family_of) })]));
  return el("div", { class: "inv-report" }, parts);
}

/* The pane under the list for one request, from its get_investigation_reports answer. */
function detailPane(row, data, access, single) {
  const reports = reportRows(data, row.request_id);
  const journal = journalRows(data, row.request_id);
  const claimed = claimedAt(journal, row.request_id);
  const facts = [
    "State " + row.state,
    row.claimed_by ? "agent " + row.claimed_by : "no agent",
    claimed ? "claimed " + localTime(claimed) : "never claimed",
    row.reported_at ? "reported " + localTime(row.reported_at) : "not reported",
  ].join(" · ");
  const parts = [
    el("div", { class: "jh-detail-head" }, [
      el("strong", { text: "Request on " + (row.server_name || "?") + " (" + (row.family || "?") + ")" }),
      single ? null : el("button", { type: "button", class: "jh-detail-close", text: "Close", onClick: () => { choices.requestId = null; mount(slot, []); } }),
    ].filter(Boolean)),
    el("div", { class: "muted", text: facts }),
  ];

  const closer = closeForm(row, access);
  if (closer) parts.push(closer);

  if (reports.length) {
    parts.push(noticeStrip("A report is an agent's own claim: unreviewed and self-attributed, not a finding."));
    parts.push(section("Report" + (reports.length > 1 ? "s" : ""), reports.map(reportBlock)));
  } else {
    parts.push(section("Report", [emptyStrip("No agent has reported on this request.")]));
  }

  if (data && data.brief != null) {
    const level = data.brief && data.brief.data_detail ? " (data detail: " + data.brief.data_detail + ")" : "";
    parts.push(section("Stored brief" + level, [el("pre", { class: "jh-detail-message", text: valueText(data.brief) })]));
  }

  const jparts = [];
  if (data && data.journal_truncated && journal.length) jparts.push(noticeStrip("Showing the newest " + journal.length + " journal rows; older ones are not listed."));
  jparts.push(VIZ.table({ journal }, { id: "investigation-journal", rowsKey: "journal", columns: JOURNAL_COLUMNS, emptyText: "No journal rows for this request." }));
  parts.push(section("Journal", jparts));
  return parts;
}

let slot = null;
let reloadList = null;

async function loadDetail(row) {
  const ticket = ++detailSeq;
  if (detailAbort) detailAbort.abort();
  const signal = (detailAbort = new AbortController()).signal;
  mount(slot, [loadingStrip("Loading this request…")]);
  const [res, session] = await Promise.all([readTool("get_investigation_reports", { request_id: row.request_id }, signal), getSession()]);
  if (ticket !== detailSeq) return;
  if (res.kind === "aborted" || res.kind === "auth") return;
  if (res.kind === "error") return mount(slot, [readErrorStrip(res.message)]);
  if (res.kind === "empty") return mount(slot, [emptyStrip(res.message || "This request has no stored report or journal.")]);
  try {
    mount(slot, detailPane(row, res.data || {}, writeAccess(session)));
  } catch (e) {
    mount(slot, [errorStrip("Could not draw this request: " + (e && e.message ? e.message : String(e)))]);
  }
}

/** #6979: the request id in the query of #/investigations?request=<id>, trimmed, or "" when there is none (the page is then the list). */
export function requestIdFromQuery(query) {
  const id = new URLSearchParams(query || "").get("request");
  return id ? id.trim() : "";
}

/** The agent changes of one request, #6979: the rows of GET /api/agent-changes (the newest calls) whose request_id is this one. */
export function changesOfRequest(data, requestId) {
  const wanted = String(requestId).toLowerCase();
  const rows = data && Array.isArray(data.changes) ? data.changes : [];
  return rows.filter((c) => c && c.request_id != null && String(c.request_id).toLowerCase() === wanted);
}

/** The agent changes columns of the one-request page. */
export const REQUEST_CHANGE_COLUMNS = [
  { key: "called_at_utc", label: "Called" },
  { key: "tool", label: "Tool" },
  { key: "kind", label: "Kind" },
  { key: "result", label: "Result" },
  { key: "undone_by", label: "Undone by" },
];

function changesSection(res, requestId) {
  const link = el("a", { href: "#/admin/agent-changes", text: "Open the Agent changes tab" });
  if (res.kind === "error") return section("Agent changes", [errorStrip("Could not read the agent changes: " + res.message), link]);
  if (res.kind !== "data") return section("Agent changes", [emptyStrip(res.message || "The agent changes are not available."), link]);
  const changes = changesOfRequest(res.data, requestId);
  if (!changes.length) return section("Agent changes", [emptyStrip("No agent change on this request in the newest calls the service lists."), link]);
  return section("Agent changes (" + changes.length + ")", [
    VIZ.table({ changes }, { id: "request-agent-changes", rowsKey: "changes", columns: REQUEST_CHANGE_COLUMNS, emptyText: "No agent changes." }),
    el("div", { class: "muted", text: "Undo is a person's action, on the Agent changes tab." }),
    link,
  ]);
}

/* #6979: #/investigations?request=<id>, the link a report notice carries. One request on its own: its report, brief, journal and
   its agent changes, or one plain line when the id is not a request. It reads only; a change happens after a click on a button. */
function renderOneRequest(main, requestId) {
  if (pageAbort) pageAbort.abort();
  const signal = (pageAbort = new AbortController()).signal;
  ++pageSeq;
  slot = el("div", { class: "jh-detail" });
  mount(main, [
    el("div", { class: "page-head" }, [
      el("h2", { text: "Investigations" }),
      el("div", { class: "spacer" }),
      el("a", { href: "#/investigations", text: "All requests" }),
    ]),
    slot,
  ]);

  async function load() {
    const mine = ++pageSeq;
    mount(slot, [loadingStrip("Loading this request…")]);
    const [res, session, changes] = await Promise.all([
      readTool("get_investigation_reports", { request_id: requestId }, signal),
      getSession(),
      apiGet("/api/agent-changes", signal),
    ]);
    if (mine !== pageSeq || res.kind === "aborted" || res.kind === "auth") return;
    if (res.kind === "error") return mount(slot, [readErrorStrip(res.message)]);
    /* An id that is not a request: the read answers not_found with a sentence, which is shown as it is. */
    if (res.kind === "empty") return mount(slot, [emptyStrip(res.message || "No investigation request has this id.")]);
    const data = res.data || {};
    const row = rowsUnder(data, "requests").find((r) => r && String(r.request_id).toLowerCase() === requestId.toLowerCase());
    if (!row) return mount(slot, [emptyStrip("No investigation request has the id " + requestId + ".")]);
    if (changes.kind === "aborted" || changes.kind === "auth") return;
    try {
      mount(slot, [...detailPane(row, data, writeAccess(session), true), changesSection(changes, requestId)]);
    } catch (e) {
      mount(slot, [errorStrip("Could not draw this request: " + (e && e.message ? e.message : String(e)))]);
    }
  }

  reloadList = load;
  load();
}

export function renderInvestigations(main, requestId) {
  if (requestId) return renderOneRequest(main, requestId);
  if (pageAbort) pageAbort.abort();
  const signal = (pageAbort = new AbortController()).signal;
  ++pageSeq;
  const body = el("div", {}, [loadingStrip("Loading investigation requests…")]);
  slot = el("div", { class: "jh-detail" });

  const reload = () => {
    choices.requestId = null;
    mount(slot, []);
    load();
  };
  const state = select("State", STATES, choices.state, (v) => { choices.state = v; reload(); });
  const family = select("Family", FAMILIES, choices.family, (v) => { choices.family = v; reload(); });
  mount(main, [
    el("div", { class: "page-head" }, [el("h2", { text: "Investigations" }), el("div", { class: "spacer" }), state, family]),
    el("div", { class: "muted", text: "Requests the service opened for agents to look into, newest first. Pick a request to read its report and journal; a sign-in that can make changes can also close it." }),
    body,
    slot,
  ]);

  async function load() {
    const mine = ++pageSeq;
    mount(body, loadingStrip("Loading investigation requests…"));
    const res = await readTool("list_investigation_requests", listParams(choices), signal);
    if (mine !== pageSeq || res.kind === "aborted" || res.kind === "auth") return;
    if (res.kind === "error") return mount(body, readErrorStrip(res.message));
    if (res.kind === "empty") return mount(body, emptyStrip(res.message || "No investigation requests."));
    const data = res.data || {};
    const rows = rowsUnder(data, "requests");
    if (!rows.length) return mount(body, emptyStrip("No investigation requests."));
    const onRow = (r, tr) => {
      if (!r) return;
      tr.style.cursor = "pointer";
      tr.setAttribute("title", "Show this request's report and journal");
      makeActivatable(tr, () => {
        choices.requestId = r.request_id;
        loadDetail(r);
      });
    };
    const parts = [];
    if (data.truncated) parts.push(noticeStrip("Showing the newest " + rows.length + " requests; older ones are not listed. Narrow the state or family."));
    parts.push(el("div", { class: "muted", text: rows.length + " requests shown" }));
    parts.push(VIZ.table(data, { id: "investigations", rowsKey: "requests", columns: REQUEST_COLUMNS, onRow, emptyText: "No investigation requests." }));
    mount(body, parts);
    const keep = choices.requestId ? rows.find((r) => String(r.request_id) === String(choices.requestId)) : null;
    if (keep) loadDetail(keep);
    else choices.requestId = null;
  }

  reloadList = load;
  load();
}
