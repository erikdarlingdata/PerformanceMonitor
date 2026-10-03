/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Server Inventory" tab: one row per monitored server from get_finops_inventory (view server_inventory).
   The view is fleet-wide: the page's server choice is shown but not used. Uptime is not shown: the start time is
   each server's own clock, and the browser's clock cannot be subtracted from it. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool } from "../../util.js";

const LIMIT = 200;
const BAND_SEV = { good: "Healthy", fair: "Warning", poor: "Critical" };
const PROVISIONING = {
  RIGHT_SIZED: ["RIGHT SIZED", "Healthy"],
  OVER_PROVISIONED: ["OVER PROVISIONED", "Warning"],
  UNDER_PROVISIONED: ["UNDER PROVISIONED", "Critical"],
  NOT_APPLICABLE: ["N/A", null],
};

const COLUMNS = [
  { key: "server", label: "Server" },
  { key: "edition", label: "Edition" },
  { key: "product_version", label: "Version" },
  { key: "host_os_version", label: "Host OS" },
  { key: "cpu_count", label: "Logical CPUs", format: "int" },
  { key: "physical_memory_mb", label: "Memory MB", format: "int" },
  { key: "socket_count", label: "Sockets", format: "int" },
  { key: "cores_per_socket", label: "Cores/Socket", format: "int" },
  { key: "hardware_note", label: "Hardware note", wrap: true },
  { key: "provisioning_status", label: "Status", sevKey: "provisioning_sev" },
  { key: "avg_cpu_pct", label: "Avg CPU %", format: "num1" },
  { key: "storage_total_gb", label: "Storage GB", format: "num1" },
  { key: "idle_db_count", label: "Idle DBs", format: "int" },
  { key: "sqlserver_start_time_local", label: "Start time (server clock)" },
  { key: "inventory_as_of", label: "Inventory as of", format: "time" },
  { key: "last_collected", label: "Last collected", format: "time" },
  { key: "monitoring", label: "Monitoring" },
  { key: "monthly_cost_usd", label: "Monthly ($)", format: "int" },
  { key: "annual_cost_usd", label: "Annual ($)", format: "int" },
  { key: "health_score", label: "Health", format: "int", sevKey: "health_sev" },
  { key: "health_band", label: "Band", sevKey: "health_sev" },
  { key: "license_warning", label: "License warning", wrap: true, sevKey: "license_sev" },
  { key: "is_hadr_enabled", label: "HADR", format: "bool" },
  { key: "ag_replica_role", label: "AG role" },
  { key: "is_clustered", label: "Clustered", format: "bool" },
];

// The row as shown: a hardware-note code becomes the legend's text, the verdict token becomes its label, and the
// severity names come from the payload's own band and verdict. A key the read omitted stays missing and shows "—".
function displayRow(r, legend) {
  return {
    ...r,
    hardware_note: r.hardware_note == null ? null : (legend[r.hardware_note] ?? r.hardware_note),
    provisioning_status: r.provisioning_status == null ? null : (PROVISIONING[r.provisioning_status]?.[0] ?? r.provisioning_status.replace(/_/g, " ")),
    provisioning_sev: PROVISIONING[r.provisioning_status]?.[1] ?? null,
    health_sev: BAND_SEV[r.health_band] ?? null,
    license_sev: r.license_warning ? "Critical" : null,
    sqlserver_start_time_local: r.sqlserver_start_time_local ? r.sqlserver_start_time_local.slice(0, 16).replace("T", " ") : null,
    monitoring: r.monitoring === "active" ? "Active" : r.monitoring === "stopped" ? "Stopped" : r.monitoring ?? null,
    ag_replica_role: String(r.ag_replica_role ?? "").toLowerCase() === "standalone" ? null : r.ag_replica_role ?? null,
  };
}

function noticeText(data, n) {
  return n + (n === 1 ? " server" : " servers")
    + (data.truncated ? " (the first " + n + " of " + data.total_servers + ")" : "")
    + ". Fleet-wide: the server picked above does not filter this list. CPU over the last " + (data.cpu_window_hours ?? 24)
    + " hours, idle databases over the last " + (data.idle_window_days ?? 7)
    + " days. Times are local except Start time, which is each server's own clock.";
}

export const tab = {
  id: "server-inventory",
  label: "Server Inventory",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    (async () => {
      try {
        const res = await readTool("get_finops_inventory", { view: "server_inventory", limit: LIMIT }, ctx && ctx.signal);
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        const legend = data.hardware_note_legend || {};
        const rows = (data.servers || []).map((r) => displayRow(r, legend));
        mount(body, [
          noticeStrip(noticeText(data, rows.length)),
          VIZ.table({ servers: rows }, { rowsKey: "servers", columns: COLUMNS, emptyText: "No server property data collected yet." }),
        ]);
      } catch (e) {
        if (e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    })();
    return body;
  },
};
