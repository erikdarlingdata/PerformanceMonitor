/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #6386: "Run live now" on the Active Queries panel. It queues the same fetch_active_queries command the desktop viewer's Current Active
 * Queries queues (POST /api/active-queries/live), polls the command (GET /api/active-queries/live/{id}) until the service has read the
 * running requests from the monitored server, and draws them in the stored snapshot's own columns. A read-only seat sees the list-only note and no control (the
 * server answers 403 to it anyway, so this is the affordance layer, never the enforcement); a store login that cannot queue the command sees
 * the note that says where to read it instead.
 */

import { el, mount, apiGet, apiWrite, listOnlyNote, noticeStrip } from "../util.js";
import { getSession } from "../views-api.js";
import { VIZ, getPanelSignal } from "../panels.js";

const POLL_MS = 1000;
/* The desktop viewer waits 60 s for the service (which caps the SQL read at about 30 s); a little more here for the first poll. */
const GIVE_UP_MS = 75 * 1000;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** The service serializes the collector's row as PascalCase; the grid's columns are the stored read's snake_case. */
export function snakeKey(name) {
  return String(name).replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
}

/** The grid rows for a finished read: each running request with snake_case keys and the read's own time as its collection_time. */
export function liveRows(result) {
  const rows = result && Array.isArray(result.rows) ? result.rows : [];
  return rows.map((row) => {
    const out = { collection_time: result.fetchedAtUtc };
    for (const [key, value] of Object.entries(row || {})) out[snakeKey(key)] = value;
    return out;
  });
}

function refusalText(res) {
  if (res.status === 0) return res.message || "The request did not reach the service.";
  if (res.status === 403 && !(res.body && res.body.error)) return "This account has read-only access, so it cannot run a live read.";
  const body = res.body && typeof res.body === "object" ? res.body : {};
  return body.error || "The service refused the request (HTTP " + res.status + ").";
}

/**
 * Queue a live read and wait for it. Returns { ok, message, result } where `result` is the service's own answer (rows, fetchedAtUtc).
 */
export async function runLiveActiveQueries(server) {
  const queued = await apiWrite("POST", "/api/active-queries/live", { server });
  if (queued.expired) return { ok: false, message: "Your session has expired. Sign in again." };
  if (queued.status !== 202 || !queued.body || queued.body.command_id == null) {
    return { ok: false, message: refusalText(queued) };
  }
  const id = queued.body.command_id;
  const started = Date.now();
  while (Date.now() - started < GIVE_UP_MS) {
    await sleep(POLL_MS);
    const r = await apiGet("/api/active-queries/live/" + id);
    if (r.kind === "data" && r.data && r.data.finished) {
      const result = r.data.result || {};
      if (r.data.status === "failed") {
        return { ok: false, message: result.error || r.data.result_status || "The service could not read the live active queries." };
      }
      return { ok: true, message: "", result };
    }
  }
  return { ok: false, message: "The service did not return the live active queries in time. It may still be reading: try again." };
}

/** The note for a store login that cannot queue the read. A managed store always can; a store brought along may need its roles script run again. */
export function liveUnavailableNote(managedStore) {
  return managedStore === true
    ? listOnlyNote(true, "the live read cannot be queued from this page", "To read the active queries live")
    : "Run live now is not available. This seat can edit, but the store login this dashboard connects with (postgres.webConnectionString) is not allowed to queue a live read: the store's roles script has not been run since the service was upgraded. " +
      "To fix it, run provision-roles.sql (from the Darling/tools folder of the project's GitHub repository) against the store as its owner, then reload this page. Or use the Viewer's Current Active Queries.";
}

/** The note a read-only seat sees where the Run live now button would be: the same sentence every list-only page gives that seat
 *  (util.js listOnlyNote), so the advice to use the Viewer's Current Active Queries is on the page, not only in the banner's hover text. */
export function liveReadOnlyNote(session) {
  return listOnlyNote(session.managed_store === true, "the live read cannot be queued from this page", "To read the active queries live", session.admin_writes_off === true);
}

/* #6386: what the page says for each server, kept here and not in the elements. The server page replaces its whole panel grid on every
   auto-refresh tick (every 60 s by default, 30 s if the operator picks it), which is shorter than a read may take (GIVE_UP_MS), so the
   read's last words and its lock must outlive the bar that started it: collector-run.js keeps the same state for the same reason. */
/* Servers with a read in flight, so a rebuilt bar comes up with its button off instead of queueing a second read (the store refuses it with DL409). */
const running = new Set();
/* The Run-live-now buttons on the page for each server; a button the rebuild dropped is forgotten the next time something changes. */
const buttons = new Map();
/* The status line, result slot and grid columns of the bar that is on the page now, for each server. */
const bars = new Map();
/* The status text and the finished rows for each server. */
const shown = new Map();

function notify(server) {
  const set = buttons.get(server);
  if (!set) return;
  for (const button of [...set]) {
    if (!button.isConnected) set.delete(button);
    else button.disabled = running.has(server);
  }
}

function track(server, button) {
  const set = buttons.get(server) || new Set();
  set.add(button);
  buttons.set(server, set);
  button.disabled = running.has(server);
}

/* Draw what is known for the server into the bar that is on the page now. */
function paint(server) {
  const bar = bars.get(server);
  if (!bar) return;
  const state = shown.get(server) || { text: "", rows: [] };
  bar.status.textContent = state.text;
  mount(bar.output, state.rows.length ? [VIZ.table({ rows: state.rows }, { rowsKey: "rows", columns: bar.columns })] : []);
}

function say(server, text, rows) {
  shown.set(server, { text, rows: rows || [] });
  paint(server);
}

/**
 * The "Run live now" block for the Active Queries panel. `columns` are the stored grid's columns. A read-only seat gets the list-only note and no button.
 */
export function activeQueriesLiveBar(server, columns) {
  const slot = el("div", { class: "active-live" }, []);
  slot.style.gridColumn = "1 / -1";
  /* The probe takes this render's abort signal (taken now, the panel signal moves on), so a render the next tick replaced does not leave it running. */
  const signal = getPanelSignal();
  (async () => {
    const session = await getSession();
    if (!session) return;
    /* A seat that cannot edit gets the note and never a button or the availability probe (the server answers 403 to a queue from it); a failed probe also arrives as can_edit false and draws nothing. */
    if (!session.can_edit) {
      if (session.probe_failed === true) return;
      mount(slot, [noticeStrip(liveReadOnlyNote(session))]);
      slot.dataset.status = "read-only";
      return;
    }
    const avail = await apiGet("/api/active-queries/live/available", signal);
    if (avail.kind !== "data" || !avail.data) return;
    if (avail.data.can_queue !== true) {
      mount(slot, [noticeStrip(liveUnavailableNote(avail.data.managed_store === true))]);
      slot.dataset.status = "unavailable";
      return;
    }
    const status = el("div", { class: "muted", role: "status" }, []);
    const output = el("div", {}, []);
    const button = el(
      "button",
      {
        type: "button",
        class: "btn",
        title: "Reads the requests running on this server right now, through the service. The stored snapshots below are not changed.",
      },
      ["Run live now"]
    );
    button.addEventListener("click", async () => {
      if (running.has(server)) return;
      running.add(server);
      notify(server);
      say(server, "Reading the running requests now...", []);
      try {
        const outcome = await runLiveActiveQueries(server);
        if (!outcome.ok) {
          say(server, "Not read: " + outcome.message, []);
          return;
        }
        const rows = liveRows(outcome.result);
        say(
          server,
          rows.length === 0
            ? "Nothing is running on this server right now."
            : rows.length + (rows.length === 1 ? " running request" : " running requests") + " read live, kept on the store for about an hour.",
          rows
        );
      } finally {
        running.delete(server);
        notify(server);
      }
    });
    const note = el("span", { class: "muted" }, [
      " Reads the running requests from the monitored server once, now. The result is shown here and also kept on the store: a newer request more than an hour later removes it, otherwise retention does.",
    ]);
    mount(slot, [el("div", {}, [button, note]), status, output]);
    slot.dataset.status = "ready";
    track(server, button);
    bars.set(server, { status, output, columns });
    paint(server);
  })();
  return slot;
}
