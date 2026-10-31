/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { routeHash, el, mount, disclosure, noticeStrip, getActiveDatabaseFilter } from "../../util.js";
import { VIZ } from "../../panels.js";
import { windowParams } from "../../time-slicer.js";
import { downloadText } from "../../grid-tools.js";
import { deadlockGraphCell, processRowsInDatabases } from "../deadlock-graph.js";
import { codeDisclosure, slicedSlot, tablePanel } from "./shared.js";

/** XML cell: the same disclosure over a captured payload (blocked-process report, deadlock graph). The desktop
 *  viewer renders these as a graph; the browser has no graph viewer, so it hands over the payload verbatim
 *  rather than pretending. The tab's note says where the graph lives. */
function xmlDisclosure(text) {
  if (text == null || text === "") return document.createTextNode("—");
  return disclosure("XML capture (" + String(text).length.toLocaleString() + " chars)", el("pre", { class: "code" }, [text]), {
    max: 60,
  });
}

/** "20260105_143007" from an ISO stamp (its UTC digits), for a download's file name; "unknown" when there is none. */
function fileStamp(iso) {
  const m = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/.exec(String(iso ?? ""));
  return m ? m[1] + m[2] + m[3] + "_" + m[4] + m[5] + m[6] : "unknown";
}

/**
 * A Save XML button: downloads `text` as `fileName`. The rows already carry the XML, so there is no fetch and nothing in
 * flight. Disabled, with the reason on hover, when there is no XML or the read cut it short (a saved cut graph would not
 * open as a graph).
 */
function saveXmlButton(text, fileName, mime, truncated) {
  const none = text == null || text === "";
  const btn = el("button", { class: "btn small", type: "button", text: "Save XML" });
  if (none || truncated) {
    btn.disabled = true;
    btn.setAttribute("disabled", "");
    btn.setAttribute("title", none ? "No XML was captured for this row." : "This read sent only a preview of the XML, so it is not saved. Reload with the full graph to save it.");
    return btn;
  }
  btn.addEventListener("click", () => {
    try {
      downloadText(fileName, [text], mime);
    } catch (e) {
      btn.setAttribute("title", "Save failed: " + (e && e.message ? e.message : "the browser refused the download."));
    }
  });
  return btn;
}

/**
 * A plain grid under a time slicer, or following one (#6301 slice 2b): the Blocking page's Blocking and Deadlocks tables. `kind` picks the strip
 * ("blocking" or "deadlocks"); `strip` false draws the grid with no strip of its own, narrowed all the same by a selection made on another
 * strip of the page for this server. `build(w)` returns the panel for `w`, the page range or the selection.
 */
export function slicedPanel(kind, cmp, build, strip = true) {
  return slicedSlot(kind, cmp, strip, (w) => build(w));
}

/* W16 (#5644): the Blocking tab's Deadlock trend chart and Deadlocks table say the same thing when empty. */
export const DEADLOCKS_EMPTY_TEXT = "No deadlocks in this window.";

/* The two trend reads that return {time, count}. Every panel that charts them declares `format: "int"`: counts
   put their gridlines on whole numbers (charts.js integerTicks) instead of repeating one label down a 0-1 axis. */
export const COUNT_SERIES = [{ key: "count", label: "Events" }];

/* #5644 W16: get_blocking_trend counts one point per blocked-process REPORT (a single wait can produce several), so the
   chart says "reports" where the tile beside it counts one event per wait. Same read and same count as COUNT_SERIES. */
export const BLOCKING_REPORT_SERIES = [{ key: "count", label: "Reports" }];
/* When get_blocking_trend has no blocked process reports for the window and falls back to the DMV blocking snapshot (AWS RDS and any
   server without the blocked process report event), a point counts snapshot rows, not reports: title, legend and axis follow the
   answer's `source` (panels.js applies it), so the chart never calls snapshot rows reports. */
export const BLOCKING_BY_SOURCE = { "DMV snapshot": { title: "Blocking snapshot trend", label: "Snapshot rows", unit: "snapshot rows" } };

/* #2484: the aggregate lock-wait rate. One numeric key, per the same reasoning as the Current Waits series
   below — the LCK wait type is the grouping the read applied, not a second axis. */
/* Stays local: this page labels the series "Lock wait (ms/sec)", not the catalog's label. */
export const LOCK_WAIT_SERIES = [{ key: "wait_time_ms_per_second", label: "Lock wait (ms/sec)" }];

/* #2484: the two Current Waits series. Each charts ONE numeric key; the wait type and database name are
   the grouping the read already applied, not extra axes. */
export const WAITING_TASK_SERIES = [{ key: "total_wait_ms", label: "Total Wait (ms)" }];
export const BLOCKED_SESSION_SERIES = [{ key: "blocked_count", label: "Blocked Sessions" }];

/* #2484 severity series. Total and max share one millisecond axis honestly; the event/victim COUNTS are
   deliberately not charted beside them, because a count and a duration on one y-domain is the two-units
   mistake the CPU chart's dropped idle series exists to avoid. Counts live in the count trends above. */
export const BLOCKING_SEVERITY_SERIES = [
  { key: "total_duration_ms", label: "Total Wait (ms)" },
  { key: "max_duration_ms", label: "Max Wait (ms)" },
];
export const DEADLOCK_SEVERITY_SERIES = [
  { key: "total_wait_ms", label: "Total Wait (ms)" },
  { key: "max_wait_ms", label: "Max Wait (ms)" },
];

/* The desktop Blocked Process Reports grid's columns, order and headers, for every field get_blocking returns. Object is
   the web's own extra. The Blocked plan and Blocking plan buttons are added where the grid is built (blockingPlanColumns,
   #5236), so this list stays the fields the read returns. */
/* Blocking column groups: the report, the pair and the wait are always shown; the sessions' status and isolation, the blocked
   transaction and the logins, hosts and apps follow the desktop order in three toggles, with the first two on at first. */
export const BLOCKING_GROUPS = { groups: ["Status and isolation", "Transaction", "Sessions"], defaultGroups: ["Status and isolation", "Transaction"] };

export const BLOCKING_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  { key: "blocked_sql_text", label: "Blocked SQL", valueList: false, render: (r) => codeDisclosure(r.blocked_sql_text) },
  { key: "blocking_sql_text", label: "Blocking SQL", valueList: false, render: (r) => codeDisclosure(r.blocking_sql_text) },
  { key: "source", label: "Source" },
  { key: "database_name", label: "Database" },
  { key: "blocked_spid", label: "Blocked SPID", format: "int" },
  { key: "blocking_spid", label: "Blocking SPID", format: "int" },
  { key: "wait_time_ms", label: "Wait Time", format: "ms" },
  { key: "wait_resource", label: "Wait Resource", wrap: true },
  { key: "lock_mode", label: "Lock Mode" },
  { key: "blocked_status", label: "Blocked Status", group: "Status and isolation" },
  { key: "blocking_status", label: "Blocking Status", group: "Status and isolation" },
  { key: "blocked_isolation_level", label: "Blocked Isolation", group: "Status and isolation" },
  { key: "blocking_isolation_level", label: "Blocking Isolation", group: "Status and isolation" },
  { key: "blocked_transaction_name", label: "Blocked Tran", group: "Transaction" },
  { key: "blocked_priority", label: "Blocked Priority", format: "int", group: "Transaction" },
  { key: "blocked_transaction_count", label: "Blocked Tran Count", format: "int", group: "Transaction" },
  { key: "blocked_log_used", label: "Blocked Log Used", format: "int", group: "Transaction" },
  { key: "blocked_login_name", label: "Blocked Login", group: "Sessions" },
  { key: "blocked_host_name", label: "Blocked Host", group: "Sessions" },
  { key: "blocked_client_app", label: "Blocked App", wrap: true, group: "Sessions" },
  { key: "blocking_login_name", label: "Blocking Login", group: "Sessions" },
  { key: "blocking_host_name", label: "Blocking Host", group: "Sessions" },
  { key: "blocking_client_app", label: "Blocking App", wrap: true, group: "Sessions" },
  /* #5632 part 4: the head blocker's session, from the DMV snapshot rows only (get_blocking sends the two times as UTC). */
  { key: "blocking_login_time", label: "Head login time", format: "time", group: "Sessions", hint: "The head blocker's session at capture. Empty when the row was captured before this was collected, or came from a blocked process report." },
  { key: "blocking_last_request_end_time", label: "Head last request ended", format: "time", group: "Sessions", hint: "The head blocker's session at capture. Empty when the row was captured before this was collected, or came from a blocked process report." },
  { key: "blocking_open_transaction_count", label: "Head open transactions", format: "int", group: "Sessions", hint: "The head blocker's session at capture. Empty when the row was captured before this was collected, or came from a blocked process report." },
  { key: "blocking_tran_log_bytes_used", label: "Head log used", format: "bytes", group: "Sessions", hint: "The head blocker's session at capture. Empty when the row was captured before this was collected, or came from a blocked process report." },
  { key: "contentious_object", label: "Object" },
];

/* Stays local: this page renders the deadlock text through a codeDisclosure and orders the columns differently from the catalog.
   The Victim plan button is added where the grid is built (deadlockPlanColumn, #5236), so the five columns here stay the summary. */
export const DEADLOCK_COLUMNS = [
  { key: "deadlock_time", label: "Deadlock Time", format: "time" },
  { key: "victim_sql_text", label: "Victim SQL", valueList: false, render: (r) => codeDisclosure(r.victim_sql_text) },
  { key: "victim_process_id", label: "Victim" },
  { key: "process_summary", label: "Processes", wrap: true },
  { key: "has_deadlock_xml", label: "Graph", format: "bool" },
];

/* The per-process rows of one deadlock, the desktop Deadlocks grid's columns and header text, plus Log Used and Status, which the web adds. The service parses the graph and sends
   processes[] (absent values left off), so the browser never reads the XML. */
const DEADLOCK_PROCESS_COLUMNS = [
  { key: "deadlock_type", label: "Type" },
  { key: "victim", label: "Victim", render: (r) => document.createTextNode(r.victim ? "Victim" : "") },
  { key: "spid", label: "SPID", format: "int" },
  { key: "database_name", label: "Database" },
  { key: "object_names", label: "Object(s)", wrap: true },
  { key: "proc_name", label: "Procedure" },
  { key: "lock_mode", label: "Lock Mode" },
  { key: "owner_mode", label: "Owner Mode" },
  { key: "waiter_mode", label: "Waiter Mode" },
  { key: "wait_resource", label: "Wait Resource", wrap: true },
  { key: "wait_time_ms", label: "Wait (ms)", format: "ms" },
  { key: "isolation_level", label: "Isolation" },
  { key: "transaction_name", label: "Tran Name" },
  { key: "transaction_count", label: "Tran Count", format: "int" },
  { key: "priority", label: "Priority", format: "int" },
  { key: "log_used", label: "Log Used", format: "int" },
  { key: "login_name", label: "Login" },
  { key: "host_name", label: "Host" },
  { key: "client_app", label: "App", wrap: true },
  { key: "status", label: "Status" },
  { key: "sql_text", label: "Statement", valueList: false, render: (r) => codeDisclosure(r.sql_text) },
];

/* Which deadlocks have their process sub-grid open, at MODULE scope so the 60 s rebuild of the tab keeps it open. Keyed by
   server and deadlock id (the dedup key, else the deadlock's own timestamps); one entry per deadlock the session opens. */
const deadlockProcessesOpen = new Set();

function deadlockProcessKey(server, row) {
  return server + "\u0001" + (row.dedup_key || (row.collection_time || "") + "|" + (row.deadlock_time || ""));
}

/* The database filter as it applies to this panel, or null for none: the page's filter when it is the active server's and the
   page is a server page (dbScopeChip's own rule, so the chip and the rows always agree). */
function deadlockRowFilter(server) {
  const filter = getActiveDatabaseFilter();
  const hash = routeHash();
  return filter && filter.server === server && hash.startsWith("#/server/") ? filter : null;
}

/* One Deadlock Graphs panel's count of the process rows its database filter hid (#5244), per deadlock, so a grid that redraws
   its cells (a sort) does not count one twice, and the notice line under the panel. */
function deadlockRowScope() {
  const hidden = new Map();
  const note = el("div", { class: "deadlock-rows-note" });
  const update = () => {
    let total = 0;
    for (const n of hidden.values()) total += n;
    mount(
      note,
      total > 0
        ? noticeStrip(
            "The database filter hides " + total + (total === 1 ? " process row" : " process rows") + " outside the chosen databases. Each graph is whole."
          )
        : null
    );
  };
  return {
    note,
    set(key, count) {
      hidden.set(key, count);
      update();
    },
  };
}

/** The expandable per-process sub-grid for one deadlock row of get_deadlock_detail. While the page's database filter is active
 *  only the rows of the chosen databases are listed (#5244); `scope` takes the count of those it hid. */
function deadlockProcessesCell(server, row, scope = null) {
  const all = Array.isArray(row.processes) ? row.processes : [];
  if (!all.length) {
    /* The shared page row budget can cut every row of a deadlock; say so and how to get them, rather than a bare dash. */
    const cut = Number(row.processes_truncated) || 0;
    if (cut <= 0) return document.createTextNode("—");
    return document.createTextNode(cut + (cut === 1 ? " process" : " processes") + " not sent (page row limit); pick Custom… in the time range and narrow it to this deadlock to see them");
  }
  const key = deadlockProcessKey(server, row);
  const filter = deadlockRowFilter(server);
  const rows = filter ? processRowsInDatabases(all, filter.databases) : all;
  if (scope) scope.set(key, all.length - rows.length);
  /* The page row budget runs before this filter, so the chosen database's rows may be the ones cut: the suffix shows in the
     all-hidden cell too, and a cut row (its database unknown) is not counted as hidden. */
  const more = row.processes_truncated > 0 ? " (+" + row.processes_truncated + " more in the graph)" : "";
  if (!rows.length) return document.createTextNode(all.length + (all.length === 1 ? " process" : " processes") + " hidden by the database filter" + more);
  const node = disclosure(rows.length + (rows.length === 1 ? " process" : " processes") + more, [
    VIZ.table({ processes: rows }, { id: "deadlock-processes", rowsKey: "processes", columns: DEADLOCK_PROCESS_COLUMNS }),
  ]);
  if (deadlockProcessesOpen.has(key)) node.setAttribute("open", "");
  node.addEventListener("toggle", () => {
    if (node.open) deadlockProcessesOpen.add(key);
    else deadlockProcessesOpen.delete(key);
  });
  return node;
}

/* The Deadlock Graphs panel: each graph is whole, its process rows follow the database filter, and one notice line under the
   panel says so when the filter hid any (#5244). Its chip is "process-rows". */
export function deadlockGraphsPanel(server, ctx) {
  const scope = deadlockRowScope();
  const panel = tablePanel(
    "Deadlock Graphs",
    "get_deadlock_detail",
    windowParams({ server, hours: ctx.hours, limit: 5 }, ctx, true),
    "deadlocks",
    deadlockXmlColumns(server, scope),
    ctx.label,
    "No deadlock graph XML captured in this window.",
    2,
    null,
    null,
    null,
    null,
    "process-rows"
  );
  panel.appendChild(scope.note);
  return panel;
}

function deadlockXmlColumns(server, scope = null) {
  return [
    { key: "deadlock_time", label: "Deadlock Time", format: "time" },
    { key: "victim_process_id", label: "Victim" },
    {
      key: "deadlock_graph_xml_save",
      label: "XML",
      sortable: false,
      csv: false,
      render: (r) =>
        saveXmlButton(r.deadlock_graph_xml, "deadlock_" + fileStamp(r.deadlock_time) + ".xdl", "application/xml;charset=utf-8", r.deadlock_graph_xml_truncated === true),
    },
    { key: "graph", label: "Graph", sortable: false, csv: false, filter: false, copy: false, render: (r) => deadlockGraphCell(server, r) },
    { key: "processes", label: "Processes", sortable: false, render: (r) => deadlockProcessesCell(server, r, scope) },
    { key: "deadlock_graph_xml", label: "Deadlock graph", valueList: false, render: (r) => xmlDisclosure(r.deadlock_graph_xml) },
  ];
}

/* The desktop Blocked Process Reports columns get_blocked_process_xml returns, under the desktop's headers. */
export const BPR_COLUMNS = [
  { key: "event_time", label: "Event Time", format: "time" },
  {
    key: "blocked_process_report_xml_save",
    label: "XML",
    sortable: false,
    csv: false,
    render: (r) => saveXmlButton(r.blocked_process_report_xml, "blocked_process_" + fileStamp(r.event_time) + ".xml", "application/xml;charset=utf-8", false),
  },
  { key: "database_name", label: "Database" },
  { key: "blocked_spid", label: "Blocked SPID", format: "int" },
  { key: "blocking_spid", label: "Blocking SPID", format: "int" },
  { key: "wait_time_ms", label: "Wait Time", format: "ms" },
  { key: "blocked_process_report_xml", label: "Report", valueList: false, render: (r) => xmlDisclosure(r.blocked_process_report_xml) },
];

export const OBJECT_LOCK_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "row_lock_wait_ms", label: "Row lock wait", format: "ms" },
  { key: "page_lock_wait_ms", label: "Page lock wait", format: "ms" },
  { key: "lock_escalations", label: "Escalations", format: "int" },
  { key: "page_latch_wait_ms", label: "Page latch", format: "ms" },
  { key: "page_io_latch_wait_ms", label: "Page IO latch", format: "ms" },
  { key: "total_rows", label: "Rows", format: "int" },
];

