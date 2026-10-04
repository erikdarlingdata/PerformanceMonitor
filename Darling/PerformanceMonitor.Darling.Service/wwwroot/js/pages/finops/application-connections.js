/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Application Connections" tab: per-application connection counts (average and peak; running, sleeping, dormant),
   CPU, reads and writes from get_finops (view application_connections) over a fixed 24-hour window. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool } from "../../util.js";

const HOURS = 24;

const COLUMNS = [
  { key: "application_name", label: "Application" },
  { key: "avg_connections", label: "Avg connections", format: "int" },
  { key: "max_connections", label: "Max connections", format: "int" },
  { key: "avg_running", label: "Avg running", format: "int" },
  { key: "max_running", label: "Max running", format: "int" },
  { key: "avg_sleeping", label: "Avg sleeping", format: "int" },
  { key: "max_sleeping", label: "Max sleeping", format: "int" },
  { key: "avg_dormant", label: "Avg dormant", format: "int" },
  { key: "max_dormant", label: "Max dormant", format: "int" },
  { key: "avg_cpu_time_ms", label: "Avg CPU", format: "ms" },
  { key: "max_cpu_time_ms", label: "Max CPU", format: "ms" },
  { key: "avg_reads", label: "Avg reads", format: "int" },
  { key: "max_reads", label: "Max reads", format: "int" },
  { key: "avg_writes", label: "Avg writes", format: "int" },
  { key: "max_writes", label: "Max writes", format: "int" },
  { key: "avg_logical_reads", label: "Avg logical reads", format: "int" },
  { key: "max_logical_reads", label: "Max logical reads", format: "int" },
  { key: "sample_count", label: "Samples", format: "int" },
  { key: "first_seen_utc", label: "First seen", format: "time" },
  { key: "last_seen_utc", label: "Last seen", format: "time" },
];

// The count, the window and the truncation all come from the answer.
function noticeText(data) {
  const n = (data.rows || []).length;
  let text = (n === 1 ? "1 application" : n + " applications") + ", last " + (data.hours_back ?? HOURS) + " hours";
  text += data.truncated ? "; the top " + n + " of " + (data.application_count ?? "more") + " applications by peak connections." : ".";
  return text;
}

export const tab = {
  id: "application-connections",
  label: "Application Connections",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    (async () => {
      try {
        const res = await readTool("get_finops", { server, view: "application_connections", hours: HOURS }, ctx && ctx.signal);
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        mount(body, [
          el("h3", { text: "Application Connections (24h)" }),
          noticeStrip(noticeText(data)),
          VIZ.table(data, { rowsKey: "rows", columns: COLUMNS, emptyText: "No application connection data was recorded for this server." }),
        ]);
      } catch (e) {
        if (e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    })();
    return body;
  },
};
