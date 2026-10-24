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

import { el, mount, apiGet, apiGetFleet, apiWrite, readTool } from "../util.js";
import { getSession, peekSession } from "../views-api.js";
import { plainTextCounts } from "../plain-text.js";

const POLL_MS = 2000;
const GIVE_UP_MS = 15 * 60 * 1000;

/* One run per server at a time on this page, so every button on it can lock while it goes. */
const running = new Map();
/* The buttons on the page for each server. The tab is drawn again on every auto-refresh and every window change, so
   a button that left the page is dropped here the next time something changes. */
const buttons = new Map();
const bars = new Map();
/* What the status line and the results table say for each server. Kept here, not in the elements, because the tab
   is rebuilt while a run goes (auto-refresh) and the run's last words must land on the page that is showing. */
const shown = new Map();
/* #6155: servers the fleet card calls Offline. The service has no connection to such a target, so a run it is asked for
   is skipped ("server not connected"); the buttons say so and stay off instead of queueing a run that cannot happen. */
const offline = new Set();
const OFFLINE_TIP = "This server is offline: the service has no connection to it, so a collector run would be skipped. Collection resumes on its own when the server is reachable again.";
function locked(server) {
  return !!running.get(server) || offline.has(server);
}

function notify(server) {
  const set = buttons.get(server);
  if (!set) return;
  for (const button of [...set]) {
    if (!button.isConnected) set.delete(button);
    else button.disabled = locked(server);
  }
}

function track(server, button) {
  const set = buttons.get(server) || new Set();
  set.add(button);
  buttons.set(server, set);
  button.disabled = locked(server);
  if (offline.has(server)) button.title = OFFLINE_TIP;
}

/* Ask the fleet read whether the server's card says Offline (the read the page header already makes, shared when in
   flight), and turn the buttons off with the reason when it does. A failed read leaves the buttons as they were. */
async function markOffline(server, button, note) {
  const res = await apiGetFleet();
  if (res.kind !== "data") return;
  const card = (res.data.cards || []).find((c) => c.server_name === server || c.display_name === server);
  if (!card || card.band !== "Offline") {
    offline.delete(server);
    return;
  }
  offline.add(server);
  button.title = OFFLINE_TIP;
  note.textContent = " This server is offline, so a collector run would be skipped. Collection resumes on its own when it is reachable again.";
  notify(server);
  for (const other of buttons.get(server) || []) other.title = OFFLINE_TIP;
}

/* Draw what is known for the server into the bar that is on the page now (nothing for a read-only seat's empty bar). */
function paint(server) {
  const slot = bars.get(server);
  if (!slot || !slot.__status) return;
  const state = shown.get(server) || { text: "", rows: [] };
  slot.__status.textContent = state.text;
  mount(slot.__output, state.rows.length ? [resultTable(state.rows)] : []);
}

function say(server, text, rows) {
  shown.set(server, { text, rows: rows || [] });
  paint(server);
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
      el("td", {}, [run.error_message ? plainTextCounts(String(run.error_message)) : ""]),
    ])
  );
  return el("table", { class: "data-table" }, [el("thead", {}, [head]), el("tbody", {}, body)]);
}

/** Start a run, keep the status line current, and show the newest log row of each collector that ran. */
async function startRun(server, collectors) {
  if (running.get(server)) return;
  running.set(server, true);
  notify(server);
  say(server, collectors && collectors.length ? "Running " + collectors.join(", ") + " now..." : "Running every enabled collector now...");
  try {
    const outcome = await runCollectors(server, collectors);
    const text = (outcome.ok ? "" : "Not run: ") + outcome.message + (outcome.ok ? " " + describeResult(outcome.result) : "");
    say(server, text);
    /* Only a finished run has new rows to show; after a failure the newest rows would be old ones under a "Not run". */
    if (outcome.ok && outcome.result) {
      const rows = await newestRows(server, collectors);
      say(server, text, rows);
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
        onClick: () => startRun(server, null),
      },
      ["Run all collectors now"]
    );
    const note = el("span", { class: "muted" }, [
      " Runs every enabled collector once. This puts extra load on the monitored server; pick one collector in the table below to run just that one.",
    ]);
    track(server, button);
    mount(slot, [el("div", {}, [button, note]), status, output]);
    markOffline(server, button, note);
    slot.dataset.status = "ready";
    slot.__status = status;
    slot.__output = output;
    paint(server);
  });
  return slot;
}

/**
 * The extra "Run now" column of the Collectors table: one button per collector row, with its own header. An empty
 * list for a read-only seat, so that seat sees no column and no header. A click runs that collector only (even when
 * it is disabled) and does not also pick the row.
 */
export function runNowColumns(server) {
  /* The session app.js fetched at start, read without asking again: importing this module is not a data read. */
  const seat = peekSession();
  if (!seat || !seat.can_edit) return [];
  return [
    {
      key: "collector",
      label: "Run now",
      sortable: false,
      csv: false,
      filter: false,
      copy: false,
      render: (row) => {
        const button = el(
          "button",
          {
            type: "button",
            class: "btn btn-small",
            title: "Runs " + row.collector + " once, now, even if it is disabled. Its schedule and enabled state do not change.",
            onClick: (e) => {
              e.stopPropagation();
              startRun(server, [row.collector]);
            },
          },
          ["Run now"]
        );
        button.addEventListener("keydown", (e) => e.stopPropagation());
        track(server, button);
        return button;
      },
    },
  ];
}
