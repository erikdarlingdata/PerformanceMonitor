/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Index Analysis" tab: get_finops view index_analysis, shown as the caveat notes, the reclaimable-space
   roll-up (the overall row first, then one row per database) and the cleanup recommendations.
   Rows stay in payload order: recommendations largest index first, databases by total maximum savings. At most LIMIT
   recommendations and the first databases come back, and the notice says so. Differences from the desktop grid:
   the overall row has no workload figures, so those cells show "—"; an average wait of none shows 0.00; script and
   definition text is cut by the read; analyzer notes sit in a collapsed block; there are no filters or tooltips. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool, fmtInt, fmtNum } from "../../util.js";

const LIMIT = 50;
const DERIVED_KEYS = ["reads_breakdown", "index_size_gb_text"];

const ROLLUP_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "tables_analyzed", label: "Tables", format: "int" },
  { key: "index_count", label: "Indexes", format: "int" },
  { key: "total_size_gb", label: "Size GB", format: "num2" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "indexes_to_disable", label: "To Disable", format: "int" },
  { key: "indexes_to_merge", label: "To Merge", format: "int" },
  { key: "compressable_indexes", label: "Compressable", format: "int" },
  { key: "unused_indexes", label: "Unused", format: "int" },
  { key: "unused_size_gb", label: "Disable GB", format: "num2" },
  { key: "compression_min_savings_gb", label: "Compress Min GB", format: "num2" },
  { key: "compression_max_savings_gb", label: "Compress Max GB", format: "num2" },
  { key: "total_min_savings_gb", label: "Total Min GB", format: "num2" },
  { key: "total_max_savings_gb", label: "Total Max GB", format: "num2" },
  { key: "reads_breakdown", label: "Reads" },
  { key: "writes", label: "Writes", format: "int" },
  { key: "lock_wait_count", label: "Lock Waits", format: "int" },
  { key: "avg_lock_wait_ms", label: "Avg Lock Wait ms", format: "num2" },
  { key: "latch_wait_count", label: "Latch Waits", format: "int" },
  { key: "avg_latch_wait_ms", label: "Avg Latch Wait ms", format: "num2" },
];

const RECOMMENDATION_COLUMNS = [
  { key: "action", label: "Action" },
  { key: "result_kind", label: "Result Kind" },
  { key: "consolidation_rule", label: "Rule" },
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_size_gb_text", label: "Size GB", align: "right" },
  { key: "index_rows", label: "Rows", format: "int" },
  { key: "index_reads", label: "Reads", format: "int" },
  { key: "index_writes", label: "Writes", format: "int" },
  { key: "target_index_name", label: "Target Index" },
  { key: "superseded_by", label: "Superseded / Related", wrap: true },
  { key: "additional_info", label: "Info", wrap: true },
  { key: "original_index_definition", label: "Original Definition", wrap: true, mono: true },
  { key: "script", label: "Script", wrap: true, mono: true },
];

// The reads cell as the desktop writes it. The overall row carries no counters, so its cell stays missing.
function rollupRow(r) {
  return { ...r, reads_breakdown: r.total_reads == null ? null : fmtInt(r.total_reads) + " (" + fmtInt(r.user_seeks) + " seeks, " + fmtInt(r.user_scans) + " scans, " + fmtInt(r.user_lookups) + " lookups)" };
}

function recommendationRow(r) {
  return { ...r, index_size_gb_text: fmtNum(r.index_size_gb, 3) };
}

function noticeText(data, n) {
  const count = n === 0
    ? "No cleanup recommendations: indexes look clean."
    : (data.truncated ? "The largest " + n + " of " + data.recommendation_count + " recommendations." : n + (n === 1 ? " recommendation." : " recommendations."));
  const cut = data.databases_truncated ? " Showing the largest databases of " + data.database_count + "." : "";
  const workload = data.overall_workload_reason ? " Overall row: " + data.overall_workload_reason + "." : "";
  return count + cut + " Analyzed from the latest collected snapshot." + workload;
}

export const tab = {
  id: "index-analysis",
  label: "Index Analysis",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    (async () => {
      try {
        const res = await readTool("get_finops", { server, view: "index_analysis", limit: LIMIT }, ctx && ctx.signal);
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        const recs = (data.recommendations || []).map(recommendationRow);
        const rollups = (data.overall ? [rollupRow({ ...data.overall, database_name: "ALL DATABASES" })] : [])
          .concat((data.databases || []).map(rollupRow));
        const lead = (data.uptime_warning ? 1 : 0) + (data.dedupe_only_applied ? 1 : 0);
        const notes = data.notes || [];
        const parts = [noticeStrip(noticeText(data, recs.length))];
        notes.slice(0, lead).forEach((t) => parts.push(noticeStrip(t)));
        const more = notes.slice(lead);
        if (more.length) {
          parts.push(el("details", {}, [
            el("summary", { text: "Analyzer notes (" + more.length + ")" }),
            ...more.map((t) => el("p", { class: "finops-note", text: t })),
          ]));
        }
        parts.push(el("h3", { text: "Reclaimable Space (overall + per database)" }));
        parts.push(VIZ.table({ rows: rollups }, { rowsKey: "rows", columns: ROLLUP_COLUMNS, emptyText: "No index statistics collected yet." }));
        parts.push(el("h3", { text: "Recommendations" }));
        parts.push(VIZ.table({ rows: recs }, { rowsKey: "rows", columns: RECOMMENDATION_COLUMNS, emptyText: "No cleanup recommendations: indexes look clean." }));
        mount(body, parts);
      } catch (e) {
        if (e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    })();
    return body;
  },
};
