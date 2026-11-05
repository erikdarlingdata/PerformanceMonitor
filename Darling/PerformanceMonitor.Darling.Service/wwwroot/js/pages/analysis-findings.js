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
 * <pre> with a Copy button, and, for an incident finding whose fact family has a tab, a link to that tab (#7677).
 *
 * Advise-only: nothing here runs a fix or mutes. Two actions reach the service (#6389), both the desktop's: "Generate now" queues an
 * analyze_now command (POST /api/analyze-now) and re-reads the findings when the service finishes it, and "Ask AI" copies the
 * investigation prompt the Viewer copies (GET /api/analysis/ask-ai-prompt: the service builds the text, so the page and the Viewer
 * cannot drift). Neither holds a key or calls a model.
 *
 * Which findings are "incidents" follows the desktop: a finding that carries a structured_remediation is a
 * standing config fix and gets no tab link, and neither does a standing state (a setting, a volume's free space: standing_state
 * on the read, #7416). Every other finding with a time_range and a tab of its own gets one (#7677: Wait Stats for a wait card, Blocking
 * for blocking and deadlocks, and so on, from the one FAMILY_TABS map below). The server tabs have no from/to deep link on the web, so the
 * link carries the server only and names the tab ("Open Wait Stats tab"), not a sub-tab it cannot reach.
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

/**
 * A finding a server tab can explain (#7416): not a standing config fix (the read's is_config_fix, the desktop's rule;
 * force-plan stays an incident), not a standing state (the read's standing_state: a setting or the free space of a volume, true at
 * every hour whatever is running), and with a time window to point at.
 */
export function isIncidentFinding(f) {
  return !f.is_config_fix && !f.standing_state && !!(f.time_range && f.time_range.start && f.time_range.end);
}

/* #7677: the one map from a card's fact family (the finding's category) to the server tab that holds its evidence, [tab id, tab label].
   The ids and labels are the ones SERVER_TABS and POSTGRES_TABS register in server-tabs.js. A family with no tab of its own is left out,
   and its card gets no button: tempdb, disk, jobs, bad actors and the PostgreSQL families whose evidence sits on no PostgreSQL tab. */
const FAMILY_TABS = {
  waits: ["waits", "Wait Stats"],
  blocking: ["blocking", "Blocking"], // deadlocks are facts of the blocking family
  cpu: ["cpu", "CPU"],
  memory: ["memory", "Memory"],
  io: ["io", "File I/O"],
  queries: ["queries", "Queries"],
  pg_waits: ["waits", "Waits"],
  pg_io: ["io", "I/O"],
  pg_blocking: ["activity", "Activity"],
  pg_queries: ["activity", "Activity"],
  pg_replication: ["replication", "Replication"],
  pg_bloat: ["storage", "Storage"],
  pg_growth: ["storage", "Storage"],
  pg_config: ["config", "Configuration"],
};

/* An anomaly finding's category is just "anomaly"; its root fact key (the read's root_fact.key, else the first step of story_path)
   says which family it belongs to. First match wins, so the PostgreSQL prefixes are tried before the SQL Server ones. */
const ANOMALY_FAMILIES = [
  ["ANOMALY_PG_DEADLOCK", "pg_blocking"], ["ANOMALY_PG_BLOCKING", "pg_blocking"],
  ["ANOMALY_PG_WAIT_PROFILE", "pg_waits"], ["ANOMALY_PG_SAMPLED_WAIT_PROFILE", "pg_waits"],
  ["ANOMALY_PG_IO_LATENCY", "pg_io"], ["ANOMALY_PG_REPLICATION_LAG", "pg_replication"],
  ["ANOMALY_PG_DATABASE_GROWTH", "pg_growth"],
  ["ANOMALY_BLOCKING", "blocking"], ["ANOMALY_DEADLOCK", "blocking"],
  ["ANOMALY_WAIT", "waits"],
  ["ANOMALY_CPU", "cpu"],
  ["ANOMALY_MEMORY", "memory"],
  ["ANOMALY_READ_LATENCY", "io"], ["ANOMALY_WRITE_LATENCY", "io"],
  ["ANOMALY_QUERY_DURATION", "queries"],
];

function familyOf(f) {
  const category = String(f.category || "").toLowerCase();
  if (category !== "anomaly") return category;
  const root = (rootKeyOf(f) || String(f.story_path || "").split("→")[0]).trim().toUpperCase();
  const match = ANOMALY_FAMILIES.find(([prefix]) => root.startsWith(prefix));
  return match ? match[1] : "";
}

/** The tab an incident card opens, as { id, label }, or null when the card has no tab of its own (and so no button). */
export function findingTab(f) {
  if (!isIncidentFinding(f)) return null;
  const family = familyOf(f);
  const tab = Object.prototype.hasOwnProperty.call(FAMILY_TABS, family) ? FAMILY_TABS[family] : null;
  return tab ? { id: tab[0], label: tab[1] } : null;
}

/** The root fact a finding hangs from: two chains that start at the same root are one cause seen two ways. */
function rootKeyOf(f) {
  return (f.root_fact && f.root_fact.key) || "";
}

/** A finite number, or null. A missing score is not 0: a row with none keeps showing none. */
function finiteOrNull(v) {
  const n = v == null || v === "" ? NaN : Number(v);
  return Number.isFinite(n) ? n : null;
}

function instantOf(v) {
  const t = Date.parse(v);
  return Number.isFinite(t) ? t : null;
}

/** The worst score a row ever reached: its newest occurrence's severity or its peak, whichever is higher. */
function worstScore(f) {
  const a = finiteOrNull(f.severity);
  const b = finiteOrNull(f.peak_severity);
  return a == null ? b : b == null ? a : Math.max(a, b);
}

/**
 * A fact key: upper case with underscores, starting with a letter (LATCH_EX, PG_WAIT_LOCK, BAD_ACTOR_0x1A2B). A story path can also carry
 * a segment that is not a fact: a same-statement pileup's path is "SAME_STATEMENT_PILEUP → <statement hash>" (#7582), and the hash starts
 * with a digit (a surrogate identity starts with "text:"). The stored row has no typed keys (#3859), so the shape is the test.
 */
const FACT_KEY = /^[A-Z][A-Za-z0-9_]*$/;

function pathSegments(f) {
  return String(f.story_path || "").split("→").map((s) => s.trim()).filter(Boolean);
}

/** The path segments that name something other than a fact (a statement's hash), joined: "" for an ordinary chain. */
function identityOf(f) {
  return pathSegments(f).filter((s) => !FACT_KEY.test(s)).join("|");
}

/** A chain's facts in the order they happened, in words: "Latch ex, then Cxpacket". Never the raw arrows or the hash. */
export function plainChain(f) {
  const steps = pathSegments(f).filter((s) => FACT_KEY.test(s)).map((s) => readableStory(s)).filter(Boolean);
  return steps.length ? steps.join(", then ") : readableStory(rootKeyOf(f)) || findingTitle(f);
}

/** One line for a chain of a merged row: its facts in words, its own severity and its own occurrences. */
export function chainLine(c) {
  const n = Number(c.occurrences) || 0;
  return c.path + " · " + c.label + " · " + n + (n === 1 ? " occurrence" : " occurrences");
}

/**
 * Root facts whose headline is the only place that names what they are about: ANOMALY_OBJECT_GROWTH and ANOMALY_OBJECT_CONTENTION print the
 * table or index ("dbo.Orders grew 900 MB day-over-day") and their key is the bare fact key. These keep the headline in the merge key, so
 * two tables stay two rows (#7696).
 */
const SUBJECT_IN_TITLE_ROOTS = new Set(["ANOMALY_OBJECT_GROWTH", "ANOMALY_OBJECT_CONTENTION"]);

/**
 * What makes two rows one (#7696): the root fact and the statement identity, not the headline. An anomaly headline prints the observed
 * figure ("Write latency spiked to 2300 ms"), and the figure is the window's peak at the time of each analysis pass, so one condition
 * read on two incidents prints two sentences. A row with no root key, or a root whose headline names its subject, still keys on the headline.
 */
function mergeKeyOf(f) {
  const root = rootKeyOf(f);
  const byRoot = root && !SUBJECT_IN_TITLE_ROOTS.has(root);
  return (byRoot ? "root:" + root : "title:" + findingTitle(f) + "\u0000" + root) + "\u0000" + identityOf(f);
}

/**
 * #7582, #7696: one row per root fact. The read keeps one entry per chain and incident, and chains that start at the same fact print the same
 * headline, so the page showed one sentence on two or three rows with nothing to tell them apart. Rows with the same root fact (#7696: not the same headline, which prints the observed figure) become
 * one: the worst severity any of them reached (counting each row's peak, so a row whose newest occurrence is Info but whose peak was
 * Warning reads Warning), the earliest first seen, the latest last seen and the occurrences added up. The newest row speaks for the
 * rest (its advice, its incident, its figures). `chains` lists what the row covers, one entry per distinct chain, worst first, for the
 * detail. The grid and the Recommendations tab both call this, so they cannot disagree. Nothing in the input is changed.
 * One exception: a path that names a statement (a same-statement pileup's hash) keeps the statement in the merge key, so two pileups
 * on different statements with the same headline stay two rows, each with its own advice and remediation. The same statement on two
 * incidents still merges. This is the one place rows fold: groupFindings takes the merged rows as they are, so a row never hides
 * another row's remediation, and two pileups or two tables in one incident are two cards.
 */
export function mergeBySentence(findings) {
  const order = [];
  const buckets = new Map();
  for (const f of findings || []) {
    const key = mergeKeyOf(f);
    if (!buckets.has(key)) {
      buckets.set(key, { rows: [] });
      order.push(key);
    }
    buckets.get(key).rows.push(f);
  }
  return order.map((key) => {
    const { rows } = buckets.get(key);
    let newest = rows[0];
    let first = null;
    let last = null;
    let start = null;
    let end = null;
    let worst = null;
    let occurrences = 0;
    const byPath = new Map();
    for (const f of rows) {
      if ((instantOf(f.last_seen) ?? -Infinity) > (instantOf(newest.last_seen) ?? -Infinity)) newest = f;
      const fs = instantOf(f.first_seen);
      const ls = instantOf(f.last_seen);
      if (fs != null && (first == null || fs < instantOf(first))) first = f.first_seen;
      if (ls != null && (last == null || ls > instantOf(last))) last = f.last_seen;
      const range = f.time_range || {};
      if (instantOf(range.start) != null && (start == null || instantOf(range.start) < instantOf(start))) start = range.start;
      if (instantOf(range.end) != null && (end == null || instantOf(range.end) > instantOf(end))) end = range.end;
      const score = worstScore(f);
      if (score != null && (worst == null || score > worst)) worst = score;
      const n = Number(f.occurrences) || 0;
      occurrences += n;
      const path = plainChain(f);
      const chain = byPath.get(path) || { path, score: null, occurrences: 0 };
      chain.occurrences += n;
      if (score != null && (chain.score == null || score > chain.score)) chain.score = score;
      byPath.set(path, chain);
    }
    const chains = [...byPath.values()]
      .sort((a, b) => (b.score ?? -1) - (a.score ?? -1) || b.occurrences - a.occurrences)
      .map((c) => ({ path: c.path, severity: c.score, label: severityLabel(c.score), occurrences: c.occurrences }));
    return {
      ...newest,
      finding_title: findingTitle(newest),
      severity: worst,
      peak_severity: worst,
      occurrences,
      first_seen: first ?? newest.first_seen,
      last_seen: last ?? newest.last_seen,
      time_range: newest.time_range ? { ...newest.time_range, start: start ?? newest.time_range.start, end: end ?? newest.time_range.end } : newest.time_range,
      chains,
    };
  });
}

/**
 * The findings as incident groups, most severe group first. A finding with no incident_id is a group of its own.
 * Within a group the order the read gave (still-firing first, then severity) is kept after a severity sort.
 */
export function groupFindings(findings) {
  const order = [];
  const buckets = new Map();
  let solo = 0;
  for (const f of mergeBySentence(findings)) {
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
  return (f.advice && f.advice.headline) || readableStory(f.story_path) || f.category_name || f.category || "Finding";
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
      f.category ? el("span", { class: "muted", text: " · " + (f.category_name || f.category) }) : null,
    ]),
    showTitle ? el("h4", { text: findingTitle(f) }) : null,
    advice.investigation ? el("p", { class: "reco-advice", text: advice.investigation }) : null,
    advice.remediation ? el("p", { class: "reco-advice", text: advice.remediation }) : null,
    el("div", { class: "muted", text: "Seen " + f.occurrences + "× · last " + localTime(f.last_seen) + (f.recurring_at_this_hour ? " · recurs at this hour" : "") }),
  ];
  if (f.chains && f.chains.length > 1) {
    kids.push(el("div", { class: "muted", text: "Seen as " + f.chains.length + " chains: " + f.chains.map(chainLine).join("; ") }));
  }
  if (f.remediation_command) {
    kids.push(el("pre", { class: "reco-fix", text: f.remediation_command }));
  }
  const actions = [];
  if (f.remediation_command) actions.push(copyButton(f.remediation_command));
  actions.push(askAiButton(server, f));
  const tab = findingTab(f);
  if (tab) {
    actions.push(el("a", { class: "btn small", href: storeHash("#/server/") + encodeURIComponent(server) + "/" + tab.id, text: "Open " + tab.label + " tab" }));
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
