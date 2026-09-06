/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * Availability Groups page (#991) — the AG topology from GET /api/ag. One card per (reporting server, AG): the
 * replicas as chips, then the per-database secondary state as a grid. The API is PRE-BANDED — every severity is
 * computed server-side by DarlingAgReader; this page only maps a severity NAME to a CSS class and never
 * re-derives a threshold (R1).
 *
 * An AG whose replicas are all monitored yields one card PER monitored replica, each naming the server whose view
 * it is — deliberately not merged, because several DMV columns are populated only for the local replica. The card
 * subtitle says whose perspective it is so the duplication reads as intent, not a bug.
 *
 * Every value is untrusted (AG names, replica names, database names, suspend reasons all come from a monitored
 * server) and reaches the DOM through el()/textContent (R4 — never innerHTML).
 */

import {
  el,
  mount,
  apiGet,
  loadingStrip,
  errorStrip,
  emptyStrip,
  localTime,
  relTime,
  fmtInt,
  fmtText,
  bandClass,
  sevClass,
  rollupTextId,
} from "../util.js";
import { VIZ } from "../panels.js";

/* The empty state is the NORMAL case for most fleets — Always On is opt-in, and the collectors write no rows for
   an instance that hosts no AGs (and never run against Azure SQL Database). Say so plainly rather than implying
   something failed. */
const NO_AGS_MESSAGE =
  "No Availability Groups observed on any monitored server. The AG collectors write no rows for an instance that " +
  "hosts none, and they do not run against Azure SQL Database.";

/* A short non-color severity cue so severity is never conveyed by color alone (M2, as on the fleet cards). */
const SEV_BADGE = { Critical: "CRIT", Warning: "WARN" };

const DATABASE_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "replica_server_name", label: "Replica" },
  { key: "synchronization_state", label: "Sync state", sevKey: "synchronization_state_severity" },
  /* Units go in the header: the DMV reports queues in KB and rates in KB/s, and a bare "6,000" in a cell is
     unreadable without them. The drain estimates are minutes. */
  { key: "log_send_queue_kb", label: "Send queue (KB)", format: "int", align: "right" },
  { key: "redo_queue_kb", label: "Redo queue (KB)", format: "int", align: "right" },
  { key: "log_send_rate_kb_sec", label: "Send rate (KB/s)", format: "int", align: "right" },
  { key: "redo_rate_kb_sec", label: "Redo rate (KB/s)", format: "int", align: "right" },
  /* No align:"right" on the two custom-render columns — the shared cell() applies the numeric class only on the
     format path, so asking for it here would right-align the header and leave the cell left. */
  { key: "secondary_lag_seconds", label: "Lag", render: lagCell },
  { key: "est_send_drain_minutes", label: "Est drain (min)", format: "num1", align: "right" },
  { key: "est_redo_completion_minutes", label: "Est redo (min)", format: "num1", align: "right" },
  { key: "suspend_reason", label: "Data movement", render: suspendCell },
];

export async function renderAg(main) {
  mount(main, [pageHead(null), loadingStrip("Loading availability groups…")]);

  const res = await apiGet("/api/ag");
  if (res.kind === "error") {
    mount(main, [pageHead(null), errorStrip(res.message)]);
    return;
  }

  const d = res.data;
  const groups = (d && d.availability_groups) || [];
  if (!groups.length) {
    mount(main, [pageHead(d), emptyStrip(NO_AGS_MESSAGE)]);
    return;
  }

  mount(main, [pageHead(d), rollup(d), ...groups.map(agCard)]);
}

function pageHead(d) {
  return el("div", { class: "page-head" }, [
    el("h2", { text: "Availability Groups" }),
    el("div", { class: "spacer" }),
    d ? el("div", { class: "meta", text: "Updated " + localTime(d.generated_at) }) : null,
  ]);
}

/* The counts are deliberately three separate numbers: cards and distinct AGs differ exactly when an AG has more
   than one monitored replica, and that difference is the thing a reader needs to understand before wondering why
   the same AG name appears twice. */
function rollup(d) {
  /* #3045: every tile's number is programmatically tied to the text that says what it counts — the wiring
     #3038 put on the fleet rollup, from the shared helper, because the defect was identical and the two
     pages have no code path in common to fix it once. The label carries a stable id and the number describes
     itself with it, so the figure and its meaning travel together for a consumer that reaches the number's
     node on its own rather than browsing the tile top to bottom. Reading order alone leaves that
     relationship inferable but not determinable. Wired here in the helper and at no call site, so a fourth
     tile cannot be added half-wired.

     The label rides in aria-describedby rather than aria-label / aria-labelledby because .num is a plain
     div: ARIA prohibits NAMING role=generic, so a name set there is invalid and may be dropped on the floor,
     while a description is a supported property on it. Description is the carrier that works without
     inventing a widget role for a static figure.

     No sub-line and no title here, unlike the fleet tile: that rollup qualifies one of its numbers with a
     coverage line and carries a long coverage note. These three counts qualify nothing and hover over
     nothing, so there is no second text row for an id to point at. */
  const usedIds = new Set();
  const tile = (num, lbl, cls) => {
    const lblId = rollupTextId(lbl, "lbl", usedIds);
    return el("div", { class: "tile " + (cls || "") }, [
      el("div", { class: "num", text: fmtInt(num), "aria-describedby": lblId }),
      el("div", { class: "lbl", id: lblId, text: lbl }),
    ]);
  };

  return el("div", { class: "rollup" }, [
    el("div", { class: "rollup-group" }, [
      tile(d.distinct_ag_count, "Groups"),
      tile(d.reporting_server_count, "Reporting servers"),
      tile(d.availability_group_count, "Views"),
    ]),
  ]);
}

function agCard(g) {
  const cls = bandClass(g.severity);

  return el("div", { class: "card ag-card " + cls }, [
    el("div", { class: "head" }, [
      el("span", { class: "dot " + cls }),
      el("span", { class: "title", text: fmtText(g.ag_name) }),
      el("span", { class: "badge " + cls, text: g.severity_label || g.severity }),
      el("div", { class: "spacer" }),
      g.primary_replica
        ? el("span", { class: "ag-primary-note", text: "Primary: " + g.primary_replica })
        : el("span", { class: "ag-primary-note muted", text: "No primary reported" }),
    ]),
    el("div", { class: "status-line", text: viewSubtitle(g) }),
    el("div", { class: "ag-replicas" }, (g.replicas || []).map(replicaChip)),
    databaseGrid(g),
  ]);
}

/* Whose view this is, and how fresh. The two collector sweeps land independently, so the database grain gets its
   own "as of" whenever it differs from the replica grain. */
function viewSubtitle(g) {
  let text = "As reported by " + g.server_name + " · collected " + relTime(g.collection_time);
  if (g.database_collection_time && g.database_collection_time !== g.collection_time) {
    text += " · databases " + relTime(g.database_collection_time);
  }
  return text;
}

/* One chip per replica. The chip's border carries the replica's server-computed worst severity; the PRIMARY
   replica additionally gets the accent treatment so the current primary is findable at a glance. */
function replicaChip(r) {
  const badge = SEV_BADGE[r.severity];
  const role = r.role || "UNKNOWN ROLE";

  /* The states worth reading at chip size: where the replica stands and whether it is reachable. A column the DMV
     leaves null (it reports operational state and recovery health for the LOCAL replica only) is simply omitted
     rather than rendered as an em dash the reader has to interpret.

     recovery_health only ever reads ONLINE or ONLINE_IN_PROGRESS, so a healthy one would just repeat the bare
     "ONLINE" that operational_state already contributes. It joins the line only when the server-computed band says
     it is noteworthy, and carries a prefix so it cannot be mistaken for the operational state. */
  const states = [r.synchronization_health, r.connected_state, r.operational_state].filter(Boolean);
  if (r.recovery_health && r.recovery_health_severity !== "Healthy") {
    states.push("recovery " + r.recovery_health);
  }
  const modes = [r.availability_mode, r.failover_mode].filter(Boolean);

  return el("div", { class: "metric-chip ag-replica " + sevClass(r.severity) + (r.is_primary ? " is-primary" : "") }, [
    el("div", { class: "label" }, [
      el("span", { text: role }),
      /* "local" marks the replica that IS the reporting server — the label that makes the per-perspective model
         legible, separating what a server knows about ITSELF from what it believes about a remote node. Shown
         only when explicitly true: null means unknown (rows predating the column), never "remote". */
      r.is_local === true ? el("span", { class: "ag-local", text: "local" }) : null,
      badge ? el("span", { class: "sev-badge", text: badge }) : null,
    ]),
    el("div", { class: "value", text: fmtText(r.replica_server_name), title: r.endpoint_url || null }),
    states.length ? el("div", { class: "detail", text: states.join(" · ") }) : null,
    modes.length ? el("div", { class: "detail", text: modes.join(" · ") }) : null,
  ]);
}

function databaseGrid(g) {
  const rows = g.databases || [];
  if (!rows.length) {
    return el("div", { class: "ag-databases" }, [
      emptyStrip("No database-level state collected for this group yet."),
    ]);
  }

  return el("div", { class: "ag-databases" }, [
    VIZ.table({ rows }, { rowsKey: "rows", columns: DATABASE_COLUMNS }),
  ]);
}

/* Lag needs its suspension context inline: the DMV reports secondary_lag_seconds as 0 — not null — while data
   movement is suspended, so a suspended replica reads as perfectly caught up on the number alone. */
function lagCell(d) {
  if (d.secondary_lag_seconds == null) return el("span", { class: "muted", text: "—" });
  const text = fmtInt(d.secondary_lag_seconds) + " s";
  return d.is_suspended
    ? el("span", { class: sevClass("Critical"), text: text + " (suspended)" })
    : el("span", { text: text });
}

function suspendCell(d) {
  if (!d.is_suspended) {
    return el("span", { class: "muted", text: d.is_suspended === false ? "Moving" : "—" });
  }
  return el("span", { class: sevClass("Critical"), text: "Suspended" + (d.suspend_reason ? ": " + d.suspend_reason : "") });
}
