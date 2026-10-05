/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Job History page (#4843): the web twin of the desktop viewer's Job History tab. SQL Agent job runs (steps and job
 * outcomes) across the fleet, newest first, from get_job_history, with the viewer's filters: server, window,
 * job, status and category, plus a row limit. Every filter is applied by the read itself, before its row limit.
 *
 * The filters live at MODULE scope, so the 60 s poll's rebuild of the page shows the same choices, the text being
 * typed and the focus; the grid's sort is kept by the shared grid renderer. Every value from the read is drawn as
 * text. The page says so when the window reaches past the history the store keeps, and when the row limit cut the list.
 */

import { VIZ } from "../panels.js";
import { el, mount, clear, loadingStrip, emptyStrip, noticeStrip, errorStrip, readErrorStrip, readTool, readToolWithinKeptHistory, keptWindowStrip, localTime } from "../util.js";

/** The window choices: the house presets, in hours. */
export const WINDOWS = [
  { value: 1, label: "Last 1 hour" },
  { value: 4, label: "Last 4 hours" },
  { value: 12, label: "Last 12 hours" },
  { value: 24, label: "Last 24 hours" },
  { value: 168, label: "Last 7 days" },
];

/** The run statuses the read accepts; an empty value is every status. */
export const STATUSES = [
  { value: "", label: "All statuses" },
  { value: "Failed", label: "Failed" },
  { value: "Succeeded", label: "Succeeded" },
  { value: "Retry", label: "Retry" },
  { value: "Canceled", label: "Canceled" },
];

/** The row limits on offer (the read takes at most 1000). */
export const LIMITS = [100, 250, 500, 1000];

/** The desktop grid's columns, in its order: Run Time, Server, Job, Category, Step, Status, Duration, Retries, Last Success, Message. */
export const JOB_HISTORY_COLUMNS = [
  { key: "run_time", label: "Run Time", format: "time" },
  { key: "server", label: "Server" },
  { key: "job_name", label: "Job" },
  { key: "category", label: "Category" },
  { key: "step", label: "Step" },
  { key: "status", label: "Status" },
  { key: "duration_formatted", label: "Duration", sortValue: (r) => r.duration_seconds },
  { key: "retries", label: "Retries", format: "int" },
  { key: "last_success", label: "Last Success", format: "time" },
  { key: "message", label: "Message", wrap: true },
];

/* The filter state, kept across the 60 s rebuild. `jobDraft` is the job text as typed, applied on Enter or when
   the box loses focus; `job` is the text the last read used. */
const state = { server: "", hours: 24, status: "", category: "", limit: 100, job: "", jobDraft: "", jobFocused: false, jobCaret: null };
/* The categories seen in any answer, so the Category choices survive a filter that narrows the next answer. */
const seenCategories = new Set();
/* The server choices from the last list_servers read, so the rebuild paints its select at once. */
let knownServers = [];
let pageSeq = 0;
let loadSeq = 0;
let pageAbort = null;
let loadAbort = null;

function serverRows(data) {
  if (Array.isArray(data)) return data;
  if (data && Array.isArray(data.servers)) return data.servers;
  return [];
}

/* Agent history exists only on SQL Server targets; a server whose engine is not stamped yet stays on offer. */
function isSqlServerTarget(r) {
  return !/postgres/i.test(String(r && r.engine_kind ? r.engine_kind : ""));
}

/** The row class for the desktop grid's colour coding: failed and long-running red/amber, retry amber, canceled grey. */
export function runRowClass(r) {
  if (!r) return "";
  if (r.status === "Failed") return "sev-Critical";
  if (r.is_long_running === true || r.status === "Retry") return "sev-Warning";
  if (r.status === "Canceled") return "band-Offline";
  return "";
}

function select(label, options, value, onPick) {
  const sel = el(
    "select",
    { class: "range-select-inline", "aria-label": label },
    options.map((o) => el("option", { value: String(o.value), text: o.label }))
  );
  sel.value = String(value);
  sel.addEventListener("change", () => onPick(sel.value));
  return { sel, label: el("label", { class: "range-control" }, [el("span", { text: label }), sel]) };
}

function serverOptions() {
  const opts = [{ value: "", label: "All servers" }, ...knownServers.map((r) => ({ value: r.server_name, label: r.display_name || r.server_name }))];
  if (state.server && !opts.some((o) => o.value === state.server)) opts.push({ value: state.server, label: state.server });
  return opts;
}

function categoryOptions() {
  const names = [...seenCategories];
  if (state.category && !seenCategories.has(state.category)) names.push(state.category);
  names.sort((a, b) => a.localeCompare(b));
  return [{ value: "", label: "All categories" }, ...names.map((n) => ({ value: n, label: n }))];
}

function fill(sel, options, value) {
  clear(sel);
  for (const o of options) sel.appendChild(el("option", { value: String(o.value), text: o.label }));
  sel.value = String(value);
}

/** The read's parameters for the current filters; an empty filter is left off so the read applies none. */
export function readParams() {
  const p = { hours: state.hours, limit: state.limit };
  if (state.server) p.server = state.server;
  /* The text in the box is what the next read uses, so a rebuild or another filter's change never leaves typed text unapplied. */
  state.job = state.jobDraft.trim();
  if (state.job) p.job_name = state.job;
  if (state.status) p.status = state.status;
  if (state.category) p.category = state.category;
  return p;
}

/** The notice for a window that reaches past the retained history, or null. */
export function retainedNote(data) {
  if (!data || data.window_truncated !== true) return null;
  const from = typeof data.effective_start === "string" && data.effective_start ? localTime(data.effective_start) : null;
  return from
    ? "partial window: the store keeps job history from " + from + ", after the window's start, so earlier runs are not shown."
    : "partial window: the store keeps less job history than the window asks for, so earlier runs are not shown.";
}

/** The notice for a list the row limit cut, or null. */
export function limitNote(data) {
  if (!data || data.truncated !== true) return null;
  const shown = Array.isArray(data.runs) ? data.runs.length : data.shown;
  return "Showing the newest " + shown + " runs; more matched. Raise the row limit or narrow the filters to see the rest.";
}

export function renderJobHistory(main) {
  if (pageAbort) pageAbort.abort();
  if (loadAbort) loadAbort.abort();
  const controller = (pageAbort = new AbortController());
  const mine = ++pageSeq;
  const body = el("div", {}, [loadingStrip("Loading job history…")]);

  const reload = () => load();
  const win = select("Window", WINDOWS, state.hours, (v) => {
    state.hours = Number(v);
    reload();
  });
  const server = select("Server", serverOptions(), state.server, (v) => {
    state.server = v;
    reload();
  });
  const status = select("Status", STATUSES, state.status, (v) => {
    state.status = v;
    reload();
  });
  const category = select("Category", categoryOptions(), state.category, (v) => {
    state.category = v;
    reload();
  });
  const limit = select("Rows", LIMITS.map((n) => ({ value: n, label: String(n) })), state.limit, (v) => {
    state.limit = Number(v);
    reload();
  });
  const job = el("input", { type: "text", class: "range-select-inline", "aria-label": "Job name", placeholder: "exact job name", value: state.jobDraft });
  job.value = state.jobDraft;
  job.addEventListener("input", () => {
    state.jobDraft = job.value;
    state.jobCaret = [job.selectionStart, job.selectionEnd];
  });
  job.addEventListener("focus", () => {
    state.jobFocused = true;
  });
  const apply = () => {
    state.jobDraft = job.value;
    if (state.jobDraft.trim() !== state.job) reload();
  };
  /* The rebuild removes this box, and the browser fires blur DURING the removal while the box is still connected,
     so the check waits a turn: a removed box neither clears the focus nor reads again. */
  job.addEventListener("blur", () => {
    setTimeout(() => {
      if (!job.isConnected) return;
      state.jobFocused = false;
      apply();
    }, 0);
  });
  job.addEventListener("keydown", (e) => {
    if (e.key === "Enter") apply();
  });
  const jobLabel = el("label", { class: "range-control" }, [el("span", { text: "Job" }), job]);

  mount(main, [
    el("div", { class: "page-head" }, [
      el("h2", { text: "Job History" }),
      el("div", { class: "spacer" }),
      server.label,
      win.label,
      jobLabel,
      status.label,
      category.label,
      limit.label,
    ]),
    body,
  ]);
  if (state.jobFocused && typeof job.focus === "function") {
    job.focus();
    if (state.jobCaret && typeof job.setSelectionRange === "function") job.setSelectionRange(state.jobCaret[0], state.jobCaret[1]);
  }

  async function load() {
    const ticket = ++loadSeq;
    if (loadAbort) loadAbort.abort();
    const signal = (loadAbort = new AbortController()).signal;
    mount(body, loadingStrip("Loading job history…"));
    try {
      const res = await readToolWithinKeptHistory("get_job_history", readParams(), signal);
      if (ticket !== loadSeq) return;
      if (res.kind === "aborted" || res.kind === "auth") return;
      if (res.kind === "error") return mount(body, readErrorStrip(res.message));
      const data = res.data || {};
      const kept = keptWindowStrip(res);
      if (res.kind === "empty") {
        return mount(body, [kept, noticeFor(retainedNote(res.hints)), emptyStrip(res.message)]);
      }
      for (const r of data.runs || []) if (r.category) seenCategories.add(r.category);
      fill(category.sel, categoryOptions(), state.category);
      mount(body, [
        kept,
        noticeFor(retainedNote(data)),
        noticeFor(limitNote(data)),
        el("div", { class: "muted", text: (data.runs || []).length + " runs shown" }),
        VIZ.table(data, { id: "job-history", rowsKey: "runs", columns: JOB_HISTORY_COLUMNS, rowClass: runRowClass, emptyText: "No job runs matched in the requested time range." }),
      ]);
    } catch (e) {
      if (ticket === loadSeq && e?.name !== "AbortError") mount(body, errorStrip("Could not render job history: " + (e && e.message ? e.message : String(e))));
    }
  }

  /* The server choices are read beside the first load; a failed read leaves the cached list (or just All servers). */
  (async () => {
    const res = await readTool("list_servers", {}, controller.signal);
    if (mine !== pageSeq || res.kind !== "data") return;
    knownServers = serverRows(res.data).filter(isSqlServerTarget);
    fill(server.sel, serverOptions(), state.server);
  })();
  load();
}

function noticeFor(text) {
  return text ? noticeStrip(text) : null;
}
