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
 * running requests from the monitored server, and draws them in the stored snapshot's own columns. A read-only seat sees no control (the
 * server answers 403 to it anyway, so this is the affordance layer, never the enforcement); a store login that cannot queue the command sees
 * the note that says where to read it instead.
 */

import { el, mount, apiGet, apiWrite, listOnlyNote, noticeStrip } from "../util.js";
import { getSession } from "../views-api.js";
import { VIZ } from "../panels.js";

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
    : "This dashboard's store login cannot queue a live read. Re-run Darling/tools/provision-roles.sql on the store, or use the Viewer's Current Active Queries to read the active queries live.";
}

/**
 * The "Run live now" block for the Active Queries panel. `columns` are the stored grid's columns. Empty for a read-only seat.
 */
export function activeQueriesLiveBar(server, columns) {
  const slot = el("div", { class: "active-live" }, []);
  slot.style.gridColumn = "1 / -1";
  (async () => {
    const session = await getSession();
    if (!session || !session.can_edit) return;
    const avail = await apiGet("/api/active-queries/live/available");
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
      if (button.disabled) return;
      button.disabled = true;
      status.textContent = "Reading the running requests now...";
      mount(output, []);
      try {
        const outcome = await runLiveActiveQueries(server);
        if (!outcome.ok) {
          status.textContent = "Not read: " + outcome.message;
          return;
        }
        const rows = liveRows(outcome.result);
        status.textContent = rows.length === 0
          ? "Nothing is running on this server right now."
          : rows.length + (rows.length === 1 ? " running request" : " running requests") + " read live, kept on the store for about an hour.";
        if (rows.length) mount(output, [VIZ.table({ rows }, { rowsKey: "rows", columns })]);
      } finally {
        button.disabled = false;
      }
    });
    const note = el("span", { class: "muted" }, [
      " Reads the running requests from the monitored server once, now. The result is shown here and also kept on the store: a newer request more than an hour later removes it, otherwise retention does.",
    ]);
    mount(slot, [el("div", {}, [button, note]), status, output]);
    slot.dataset.status = "ready";
  })();
  return slot;
}
