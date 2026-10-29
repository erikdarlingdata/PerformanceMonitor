/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The Recommendations server tab (#4843) — the web twin of the desktop viewer's analysis Recommendations view
 * (not the FinOps Recommendations tab). One read, get_analysis_findings, for the server the page is on: the
 * persisted findings grouped by incident (incident_id; a finding with none is its own group), each group headed
 * by its highest-severity finding, each card showing a severity badge, the advice text, the fix script in a
 * <pre> with a Copy button, and, for an incident finding, a link to that server's Queries tab.
 *
 * Advise-only: nothing here runs a fix or mutes. Two actions reach the service (#6389), both the desktop's: "Generate now" queues an
 * analyze_now command (POST /api/analyze-now) and re-reads the findings when the service finishes it, and "Ask AI" copies the
 * investigation prompt the Viewer copies (GET /api/analysis/ask-ai-prompt: the service builds the text, so the page and the Viewer
 * cannot drift). Neither holds a key or calls a model.
 *
 * Which findings are "incidents" follows the desktop: a finding that carries a structured_remediation is a
 * standing config fix and gets no Active Queries link; every other finding with a time_range does. The link
 * opens the server's Queries tab, which has no from/to deep link on the web, so it carries the server only.
 *
 * State: which groups the reader has opened or closed lives in module scope (`openState`, keyed by server then
 * by group key), because the server page rebuilds this tab on every 60 s poll and on every Range change.
 *
 * R4 (XSS): every value, the fix script included, reaches the DOM through el()'s text path.
 */

import { getSession } from "../views-api.js";
import { el, apiGet, apiWrite, listOnlyNote, readToolWithinKeptHistory, keptWindowStrip, noticeStrip, readErrorStrip, emptyStrip, loadingStrip, localTime, bandClass, dbScopeChip, storeHash } from "../util.js";

/** The desktop's severity cutoffs: >= 1.5 Critical, >= 0.75 Warning, else Info. */
export function severityLabel(severity) {
  const s = Number(severity);
  if (s >= 1.5) return "Critical";
  if (s >= 0.75) return "Warning";
  return "Info";
}

/** Info has no band of its own in the colour vocabulary, so it takes the neutral one. */
function badgeClass(label) {
  return "badge " + bandClass(label === "Info" ? "Unknown" : label);
}

/** An incident finding: not a standing config fix (the read's is_config_fix, the desktop's rule; force-plan stays an incident), with a time window to point at. */
export function showsActiveQueriesLink(f) {
  return !f.is_config_fix && !!(f.time_range && f.time_range.start && f.time_range.end);
}

/**
 * The findings as incident groups, most severe group first. A finding with no incident_id is a group of its own.
 * Within a group the order the read gave (still-firing first, then severity) is kept after a severity sort.
 */
export function groupFindings(findings) {
  const order = [];
  const buckets = new Map();
  let solo = 0;
  for (const f of findings || []) {
    const key = f.incident_id ? "i:" + f.incident_id : "s:" + solo++;
    if (!buckets.has(key)) {
      buckets.set(key, []);
      order.push(key);
    }
    buckets.get(key).push(f);
  }
  const groups = order.map((key) => {
    const items = buckets.get(key).slice().sort((a, b) => (b.severity || 0) - (a.severity || 0));
    const primary = items[0];
    return { key, items, primary, label: severityLabel(primary.severity) };
  });
  return groups.map((g, i) => ({ g, i })).sort((a, b) => (b.g.primary.severity || 0) - (a.g.primary.severity || 0) || a.i - b.i).map((x) => x.g);
}

/** The group header: primary title, finding count when more than one, severity label. */
export function groupHeader(group) {
  const title = findingTitle(group.primary);
  const n = group.items.length;
  return title + " · " + (n > 1 ? n + " findings · " : "") + group.label.toUpperCase();
}

/** A story key such as CONFIG_PG_TRACK_IO_TIMING as words: "Config PostgreSQL track io timing". Only used when a finding carries no headline. */
export function readableStory(key) {
  const words = String(key || "").trim().replace(/[_\s]+/g, " ").toLowerCase().replace(/\bpg\b/g, "PostgreSQL");
  return words ? words[0].toUpperCase() + words.slice(1) : "";
}

/** The finding's own headline; a story key in words when it has none. Never the raw key. */
export function findingTitle(f) {
  return (f.advice && f.advice.headline) || readableStory(f.story_path) || f.category || "Finding";
}

/* ─────────────────────────── state ─────────────────────────── */

/** server -> Map(group key -> open?). A group never touched takes its default: open unless Info-only. */
const openState = new Map();

function isOpen(server, group) {
  const m = openState.get(server);
  return m && m.has(group.key) ? m.get(group.key) : group.label !== "Info";
}

function setOpen(server, key, open) {
  if (!openState.has(server)) openState.set(server, new Map());
  openState.get(server).set(key, open);
}

/* ─────────────────────────── copy ─────────────────────────── */

/** Copies text with the async clipboard API, falling back to a hidden textarea. Resolves true when it copied. */
export async function copyText(text) {
  try {
    if (typeof navigator !== "undefined" && navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* fall through to the textarea path */
  }
  try {
    const ta = el("textarea", { style: "position:fixed;left:-1000px;top:0", readonly: "readonly" });
    ta.value = text;
    document.body.appendChild(ta);
    ta.select();
    const ok = typeof document.execCommand === "function" ? document.execCommand("copy") : false;
    document.body.removeChild(ta);
    return !!ok;
  } catch {
    return false;
  }
}

function copyButton(text) {
  const btn = el("button", { class: "btn small", type: "button", text: "Copy fix" });
  btn.addEventListener("click", async () => {
    const ok = await copyText(text);
    btn.textContent = ok ? "Copied" : "Copy failed";
  });
  return btn;
}

/** The path of the Ask AI prompt for a finding: the server, its headline and, when it has one, its UTC window. The service converts the window to the server's own clock. */
export function askAiPath(server, f) {
  const params = ["server=" + encodeURIComponent(server), "title=" + encodeURIComponent(findingTitle(f))];
  if (f.time_range && f.time_range.start && f.time_range.end) {
    params.push("start=" + encodeURIComponent(f.time_range.start), "end=" + encodeURIComponent(f.time_range.end));
  }
  return "/api/analysis/ask-ai-prompt?" + params.join("&");
}

function askAiButton(server, f) {
  const btn = el("button", { class: "btn small", type: "button", text: "Ask AI", title: "Copies a prompt for your own MCP client to investigate this finding. Nothing is sent anywhere." });
  btn.addEventListener("click", async () => {
    const res = await apiGet(askAiPath(server, f));
    const prompt = res.kind === "data" && res.data && typeof res.data.prompt === "string" ? res.data.prompt : null;
    btn.textContent = prompt && (await copyText(prompt)) ? "AI prompt copied" : "Couldn't copy";
  });
  return btn;
}

/* ─────────────────────────── generate now ─────────────────────────── */

const POLL_MS = 2000;
/* The desktop viewer waits three minutes for the pass; the service caps it at two. */
const GIVE_UP_MS = 3 * 60 * 1000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function refusalText(res) {
  if (res.status === 0) return res.message || "The request did not reach the service.";
  if (res.status === 403 && !(res.body && res.body.error)) return "This account has read-only access, so it cannot start an analysis.";
  const body = res.body && typeof res.body === "object" ? res.body : {};
  return body.error || "The service refused the request (HTTP " + res.status + ").";
}

/** Queue an analysis pass and wait for it. Returns { ok, message }. */
export async function runAnalyzeNow(server) {
  const queued = await apiWrite("POST", "/api/analyze-now", { server });
  if (queued.expired) return { ok: false, message: "Your session has expired. Sign in again." };
  if (queued.status !== 202 || !queued.body || queued.body.command_id == null) return { ok: false, message: refusalText(queued) };
  const id = queued.body.command_id;
  const started = Date.now();
  while (Date.now() - started < GIVE_UP_MS) {
    await sleep(POLL_MS);
    const r = await apiGet("/api/analyze-now/" + id);
    if (r.kind === "data" && r.data && r.data.finished) {
      if (!r.data.succeeded) return { ok: false, message: "Analysis did not complete: " + (r.data.result_status || r.data.status) };
      /* A pass that finds too little collected history finishes as a success with no findings; say so, or the tab only reads "No findings". */
      return { ok: true, message: r.data.result_status === "insufficient data" ? "Not enough collected data yet to analyze this server." : "Analysis complete." };
    }
  }
  return { ok: false, message: "Analysis is still running. Refresh in a moment." };
}

/** The note for a store login that cannot queue the run. A managed store always can; a store brought along may need its roles script run again. */
export function generateUnavailableNote(managedStore) {
  return managedStore === true
    ? listOnlyNote(true, "an analysis cannot be started from this page", "To generate recommendations now")
    : "Generate now is not available. This seat can edit, but the store login this dashboard connects with (postgres.webConnectionString) is not allowed to queue an analysis: the store's roles script has not been run since the service was upgraded. " +
      "To fix it, run provision-roles.sql (from the Darling/tools folder of the project's GitHub repository) against the store as its owner, then reload this page. Or use the Viewer's Generate now.";
}

/* The server page rebuilds this tab on every poll, and a pass can outlast a poll, so the lock, the status line and the reload hook live
   here and not in the elements (the same reason active-live.js keeps its state at module scope). */
const running = new Set();
const statusText = new Map();
const buttons = new Map();
const statusNodes = new Map();
/* server -> re-read and redraw the findings of the tab that is on the page now. */
const reloaders = new Map();

function paintGenerate(server) {
  const node = statusNodes.get(server);
  if (node && node.isConnected) node.textContent = statusText.get(server) || "";
  const btn = buttons.get(server);
  if (btn && btn.isConnected) btn.disabled = running.has(server);
}

function say(server, text) {
  statusText.set(server, text);
  paintGenerate(server);
}

/** The "Generate now" block: nothing for a read-only seat, a note for a store login that cannot queue, else the button and its status line. The probe takes this render's abort signal, so a render the next tick replaced does not leave it running. */
function generateBar(server, signal) {
  const slot = el("div", { class: "reco-generate" }, []);
  (async () => {
    const session = await getSession();
    if (!session || !session.can_edit) return;
    const avail = await apiGet("/api/analyze-now/available", signal);
    if (avail.kind !== "data" || !avail.data) return;
    if (avail.data.can_queue !== true) {
      slot.appendChild(noticeStrip(generateUnavailableNote(avail.data.managed_store === true)));
      slot.dataset.status = "unavailable";
      return;
    }
    const status = el("span", { class: "muted", role: "status" }, []);
    const button = el("button", { type: "button", class: "btn", title: "Asks the service to run an analysis pass for this server now, then shows the new findings." }, ["Generate now"]);
    button.addEventListener("click", async () => {
      if (running.has(server)) return;
      running.add(server);
      say(server, "Analyzing " + server + "...");
      try {
        const outcome = await runAnalyzeNow(server);
        say(server, outcome.message);
        const reload = reloaders.get(server);
        if (reload) await reload();
      } finally {
        running.delete(server);
        paintGenerate(server);
      }
    });
    buttons.set(server, button);
    statusNodes.set(server, status);
    slot.appendChild(el("div", {}, [button, " ", status]));
    slot.dataset.status = "ready";
    paintGenerate(server);
  })();
  return slot;
}

/* ─────────────────────────── rendering ─────────────────────────── */

/** The score after the label, or nothing when the row carries no numeric severity. It is worded as a score and the category follows a dot, so "0.80 waits" cannot read as a count of waits. */
function severityText(severity) {
  const n = severity == null || severity === "" ? NaN : Number(severity);
  return Number.isFinite(n) ? " (score " + n.toFixed(2) + ")" : "";
}

/* #6159: a lone finding's title is already on its group's summary line directly above, so its card does not repeat it as a heading. */
function card(server, f, showTitle) {
  const label = severityLabel(f.severity);
  const advice = f.advice || {};
  const kids = [
    el("div", { class: "reco-head" }, [
      el("span", { class: badgeClass(label), text: label.toUpperCase() + severityText(f.severity) }),
      f.category ? el("span", { class: "muted", text: " · " + f.category }) : null,
    ]),
    showTitle ? el("h4", { text: findingTitle(f) }) : null,
    advice.investigation ? el("p", { class: "reco-advice", text: advice.investigation }) : null,
    advice.remediation ? el("p", { class: "reco-advice", text: advice.remediation }) : null,
    el("div", { class: "muted", text: "Seen " + f.occurrences + "× · last " + localTime(f.last_seen) + (f.recurring_at_this_hour ? " · recurs at this hour" : "") }),
  ];
  if (f.remediation_command) {
    kids.push(el("pre", { class: "reco-fix", text: f.remediation_command }));
  }
  const actions = [];
  if (f.remediation_command) actions.push(copyButton(f.remediation_command));
  actions.push(askAiButton(server, f));
  if (showsActiveQueriesLink(f)) {
    actions.push(el("a", { class: "btn small", href: storeHash("#/server/") + encodeURIComponent(server) + "/queries", text: "Open in Active Queries" }));
  }
  if (actions.length) kids.push(el("div", { class: "reco-actions" }, actions));
  return el("div", { class: "card reco-card" }, kids);
}

function groupNode(server, group) {
  const details = el("details", { class: "reco-group" }, [
    el("summary", { class: "disc-summary" }, [groupHeader(group)]),
    el("div", { class: "disc-body" }, group.items.map((f) => card(server, f, group.items.length > 1))),
  ]);
  if (isOpen(server, group)) details.setAttribute("open", "");
  details.addEventListener("toggle", () => setOpen(server, group.key, !!details.open));
  return details;
}

/** The nodes for one read result (kept apart from the fetch so a test can drive it). */
export function renderFindings(server, res) {
  if (res.kind === "error") return [readErrorStrip(res.message)];
  if (res.kind === "empty") return [keptWindowStrip(res), emptyStrip(res.message)];
  if (res.kind !== "data") return [];
  const data = res.data || {};
  const groups = groupFindings(data.findings);
  /* #5558: databases left out because this node holds only a secondary copy of them in an availability group. */
  const replicaNote = typeof data.secondary_replica_note === "string" && data.secondary_replica_note ? noticeStrip(data.secondary_replica_note) : null;
  if (!groups.length) return [keptWindowStrip(res), replicaNote, emptyStrip("No findings in this window.")];
  return [
    keptWindowStrip(res),
    replicaNote,
    typeof data.truncation_note === "string" && data.truncation_note ? noticeStrip(data.truncation_note) : null,
    typeof data.findings_truncated_note === "string" && data.findings_truncated_note ? noticeStrip(data.findings_truncated_note) : null,
    ...groups.map((g) => groupNode(server, g)),
  ];
}

/** The Recommendations tab, in the shape server-tabs.js registers. */
export const analysisFindingsTab = {
  id: "recommendations",
  label: "Recommendations",
  /* The windowed reads this tab makes, so the server page reads its reach from the catalog (#5562 review r1 M4). */
  reachReads: ["get_analysis_findings"],
  build: (server, ctx) => {
    const body = el("div", { class: "panel-body" }, [loadingStrip()]);
    const load = async () => {
      const res = await readToolWithinKeptHistory("get_analysis_findings", { server, hours: ctx.hours, limit: 50, full_text: true }, ctx && ctx.signal);
      if (res.kind === "aborted") return;
      while (body.firstChild) body.removeChild(body.firstChild);
      for (const n of renderFindings(server, res)) if (n) body.appendChild(n);
    };
    reloaders.set(server, load);
    load();
    return [el("div", { class: "panel card span-2" }, [el("h3", {}, ["Recommendations", el("span", { class: "panel-sub", text: " " + ctx.label }), dbScopeChip("get_analysis_findings")]), generateBar(server, ctx && ctx.signal), body])];
  },
};
