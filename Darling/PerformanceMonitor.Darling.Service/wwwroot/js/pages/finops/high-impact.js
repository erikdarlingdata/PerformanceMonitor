/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "High Impact" tab: the top queries by impact score from get_finops (view high_impact) over a fixed
   24-hour window, each with its share of CPU, duration, reads, writes, memory and executions, the band the read
   returns as text, and a sample of its statement. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool } from "../../util.js";

const HOURS = 24;
const LIMIT = 10;

const COLUMNS = [
  { key: "impact_score", label: "Score", format: "int" },
  { key: "impact_band", label: "Band" },
  { key: "database_name", label: "Database" },
  { key: "query_hash", label: "Query hash", mono: true },
  { key: "sample_query_text", label: "Query preview", wrap: true },
  { key: "total_executions", label: "Executions", format: "int" },
  { key: "total_cpu_ms", label: "CPU", format: "ms" },
  { key: "cpu_share_pct", label: "CPU %", format: "num1" },
  { key: "total_duration_ms", label: "Duration", format: "ms" },
  { key: "duration_share_pct", label: "Duration %", format: "num1" },
  { key: "total_reads", label: "Reads", format: "int" },
  { key: "reads_share_pct", label: "Reads %", format: "num1" },
  { key: "total_writes", label: "Writes", format: "int" },
  { key: "writes_share_pct", label: "Writes %", format: "num1" },
  { key: "total_memory_mb", label: "Memory MB", format: "num1" },
  { key: "memory_share_pct", label: "Memory %", format: "num1" },
  { key: "executions_share_pct", label: "Executions %", format: "num1" },
  { key: "has_plan", label: "Plan", format: "bool" },
];

// The read returns the union of the top LIMIT hashes on each of six measures, so the count and the window come from the answer.
function noticeText(data) {
  const n = (data.rows || []).length;
  const lead = n === 1 ? "1 query in the top " : n + " queries in the top ";
  return lead + LIMIT + " on CPU, duration, reads, writes, memory or executions, ranked by impact score, last " + (data.hours_back ?? HOURS) + " hours.";
}

export const tab = {
  id: "high-impact",
  label: "High Impact",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    (async () => {
      try {
        const res = await readTool("get_finops", { server, view: "high_impact", hours: HOURS, limit: LIMIT }, ctx && ctx.signal);
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        mount(body, [
          noticeStrip(noticeText(data)),
          VIZ.table(data, { rowsKey: "rows", columns: COLUMNS, emptyText: "No queries were ranked for this server." }),
        ]);
      } catch (e) {
        if (e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    })();
    return body;
  },
};
