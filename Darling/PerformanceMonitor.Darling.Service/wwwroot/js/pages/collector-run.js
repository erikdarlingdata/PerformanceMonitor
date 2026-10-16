/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #5941: "Run all collectors now" and a per-collector "Run now" on the Collection Health tab. Both queue a one-time
 * collector pass for the server (POST /api/collectors/run), then poll the command (GET /api/collectors/run/{id}) until
 * the service has finished, and show the newest collection log row of each collector that ran. A read-only seat sees
 * neither button: the server answers 403 to it anyway, so this is the affordance layer, never the enforcement.
 */

import { el, mount, apiGet, apiWrite, readTool } from "../util.js";
import { getSession } from "../views-api.js";

const POLL_MS = 2000;
const GIVE_UP_MS = 15 * 60 * 1000;

/* One run per server at a time on this page, so every button on it can lock while it goes. */
const running = new Map();
const listeners = new Map();
const bars = new Map();

function notify(server) {
  for (const fn of listeners.get(server) || []) fn();
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Plain-words sentence for a refused or failed request. */
function refusalText(res) {
  if (res.status === 0) return res.message || "The request did not reach the service.";
  if (res.status === 403) return "This account has read-only access, so it cannot run collectors.";
  const body = res.body && typeof res.body === "object" ? res.body : {};
  return body.error || "The service refused the request (HTTP " + res.status + ").";
}

/**
 * Queue a run and wait for it. `collectors` is a list of names for "run now", or null for "run all".
 * Returns { ok, message, result }; `result` is the service's own answer (collectorsRun, rows, skipped).
 */
export async function runCollectors(server, collectors) {
  const body = { server };
  if (collectors && collectors.length) body.collectors = collectors;
  const queued = await apiWrite("POST", "/api/collectors/run", body);
  if (queued.expired) return { ok: false, message: "Your session has expired. Sign in again." };
  if (queued.status !== 202 || !queued.body || queued.body.command_id == null) {
    return { ok: false, message: refusalText(queued) };
  }
  const id = queued.body.command_id;
  const started = Date.now();
  while (Date.now() - started < GIVE_UP_MS) {
    await sleep(POLL_MS);
    const r = await apiGet("/api/collectors/run/" + id);
    if (r.kind === "data" && r.data && r.data.finished) {
      const result = r.data.result || {};
      if (r.data.status === "failed") {
        return { ok: false, message: result.error || r.data.result_status || "The run failed.", result };
      }
      return { ok: true, message: "Finished.", result };
    }
  }
  return { ok: false, message: "The run is still going. Open the Collection Log below in a minute to see its result." };
}

function describeResult(result) {
  if (!result) return "";
  const parts = [];
  if (typeof result.collectorsRun === "number") {
    parts.push(result.collectorsRun + " collector" + (result.collectorsRun === 1 ? "" : "s") + " run, " + (result.rows || 0) + " rows stored.");
  }
  for (const s of result.skipped || []) {
    parts.push(s.collector + " skipped: " + s.reason + ".");
  }
  return parts.join(" ");
}

/* The newest collection log row of each named collector (or of every collector seen when no names are given). */
async function newestRows(server, names) {
  const r = await readTool("get_collection_log", { server, hours: 1, limit: 200 });
  const runs = r && r.kind === "data" && r.data && Array.isArray(r.data.runs) ? r.data.runs : [];
  const newest = new Map();
  for (const run of runs) {
    const name = run.collector;
    if (!name || (names && names.length && !names.includes(name))) continue;
    const seen = newest.get(name);
    if (!seen || String(run.collection_time) > String(seen.collection_time)) newest.set(name, run);
  }
  return [...newest.values()].sort((a, b) => String(a.collector).localeCompare(String(b.collector)));
}

function resultTable(rows) {
  const head = el("tr", {}, ["Collector", "When", "Status", "Rows", "Error"].map((h) => el("th", {}, [h])));
  const body = rows.map((run) =>
    el("tr", {}, [
      el("td", {}, [String(run.collector)]),
      el("td", {}, [run.collection_time ? new Date(run.collection_time).toLocaleString() : ""]),
      el("td", {}, [String(run.status || "")]),
      el("td", {}, [run.rows_collected == null ? "" : String(run.rows_collected)]),
      el("td", {}, [run.error_message ? String(run.error_message) : ""]),
    ])
  );
  return el("table", { class: "data-table" }, [el("thead", {}, [head]), el("tbody", {}, body)]);
}

/** Start a run, keep the status line current, and show the newest log row of each collector that ran. */
async function startRun(server, collectors, status, output) {
  if (running.get(server)) return;
  running.set(server, true);
  notify(server);
  status.textContent = collectors && collectors.length ? "Running " + collectors.join(", ") + " now..." : "Running every enabled collector now...";
  mount(output, []);
  try {
    const outcome = await runCollectors(server, collectors);
    status.textContent = (outcome.ok ? "" : "Not run: ") + outcome.message + (outcome.ok ? " " + describeResult(outcome.result) : "");
    if (outcome.result) {
      const rows = await newestRows(server, collectors);
      if (rows.length) mount(output, [resultTable(rows)]);
    }
  } finally {
    running.set(server, false);
    notify(server);
  }
}

/**
 * The "Run all collectors now" block for the top of the Collection Health tab. Empty for a read-only seat.
 */
export function collectorRunBar(server) {
  const slot = el("div", { class: "collector-run" }, []);
  bars.set(server, slot);
  slot.style.gridColumn = "1 / -1";
  getSession().then((session) => {
    if (!session || !session.can_edit) return;
    const status = el("div", { class: "muted", role: "status" }, []);
    const output = el("div", {}, []);
    const button = el(
      "button",
      {
        type: "button",
        class: "btn",
        title: "Runs every enabled collector for this server once, now. Disabled collectors are skipped. Schedules do not change.",
        onClick: () => startRun(server, null, status, output),
      },
      ["Run all collectors now"]
    );
    const note = el("span", { class: "muted" }, [
      " Runs every enabled collector once. This puts extra load on the monitored server; pick one collector in the table below to run just that one.",
    ]);
    const sync = () => {
      button.disabled = !!running.get(server);
    };
    const set = listeners.get(server) || new Set();
    set.add(sync);
    listeners.set(server, set);
    sync();
    mount(slot, [el("div", {}, [button, note]), status, output]);
    slot.dataset.status = "ready";
    slot.__status = status;
    slot.__output = output;
  });
  return slot;
}

/**
 * Adds a "Run now" button to one row of the Collectors table (called from the table's onRow). Does nothing for a
 * read-only seat. The click does not also pick the row.
 */
export function addRunNowButton(server, row, tr) {
  if (!row || !row.collector) return;
  getSession().then((session) => {
    if (!session || !session.can_edit) return;
    const button = el(
      "button",
      {
        type: "button",
        class: "btn btn-small",
        title: "Runs " + row.collector + " once, now, even if it is disabled. Its schedule and enabled state do not change.",
        onClick: (e) => {
          e.stopPropagation();
          const bar = bars.get(server);
          const status = bar && bar.__status;
          const output = bar && bar.__output;
          if (status && output) startRun(server, [row.collector], status, output);
        },
      },
      ["Run now"]
    );
    const sync = () => {
      button.disabled = !!running.get(server);
    };
    const set = listeners.get(server) || new Set();
    set.add(sync);
    listeners.set(server, set);
    sync();
    const td = el("td", {}, [button]);
    td.addEventListener("click", (e) => e.stopPropagation());
    tr.appendChild(td);
  });
}
