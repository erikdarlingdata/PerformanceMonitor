/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Database Resources" tab: per-database CPU, reads, writes, executions and file I/O from get_finops
   (view database_resources) over a fixed 24-hour window, with each database's share of the server's CPU and I/O. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool } from "../../util.js";

const HOURS = 24;

const COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "cpu_time_ms", label: "CPU", format: "ms" },
  { key: "cpu_share_pct", label: "CPU %", format: "num2" },
  { key: "logical_reads", label: "Logical reads", format: "int" },
  { key: "physical_reads", label: "Physical reads", format: "int" },
  { key: "logical_writes", label: "Logical writes", format: "int" },
  { key: "execution_count", label: "Executions", format: "int" },
  { key: "io_read_mb", label: "I/O read MB", format: "num2" },
  { key: "io_write_mb", label: "I/O write MB", format: "num2" },
  { key: "io_share_pct", label: "I/O %", format: "num2" },
  { key: "io_stall_ms", label: "I/O stall", format: "ms" },
];

// The count, the window and the truncation all come from the answer.
function noticeText(data) {
  const n = (data.rows || []).length;
  let text = (n === 1 ? "1 database" : n + " databases") + ", last " + (data.hours_back ?? HOURS) + " hours";
  text += data.truncated ? "; the top " + n + " of " + data.database_count + " databases by CPU, then I/O." : ".";
  return text;
}

export const tab = {
  id: "database-resources",
  label: "Database Resources",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    (async () => {
      try {
        const res = await readTool("get_finops", { server, view: "database_resources", hours: HOURS }, ctx && ctx.signal);
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        mount(body, [
          noticeStrip(noticeText(data)),
          VIZ.table(data, { rowsKey: "rows", columns: COLUMNS, emptyText: "No database resource usage was recorded for this server." }),
        ]);
      } catch (e) {
        if (e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    })();
    return body;
  },
};
