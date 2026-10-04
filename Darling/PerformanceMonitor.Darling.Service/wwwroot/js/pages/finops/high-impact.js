/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "High Impact" tab: the top queries by impact score from get_finops (view high_impact) over a
   window picked from 1 hour to 7 days, each with its share of CPU, duration, reads, writes, memory and executions, the band the read
   returns as text, and a sample of its statement. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool } from "../../util.js";

// The default window.
const HOURS = 24;

// The desktop's window choices (FinOpsTab.xaml ~:874), as hours.
const WINDOWS = [
  { value: 1, label: "Last 1 hour" },
  { value: 4, label: "Last 4 hours" },
  { value: 12, label: "Last 12 hours" },
  { value: 24, label: "Last 24 hours" },
  { value: 168, label: "Last 7 days" },
];

// The chosen window per server, kept here so the 60 s rebuild of the tab does not put it back to 24 hours.
const chosenHours = new Map();

// A labelled <select> (the server-tabs.js pickerControl pattern); every value goes through el()'s text and attribute paths.
function pickerControl(label, options, selected, onPick) {
  const sel = el(
    "select",
    { class: "range-select-inline", "aria-label": label },
    options.map((o) => el("option", { value: o.value, text: o.label }))
  );
  sel.value = String(selected);
  sel.addEventListener("change", () => onPick(Number(sel.value)));
  return el("label", { class: "range-control" }, [el("span", { text: label }), sel]);
}
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
    let seq = 0;
    const load = async (hours) => {
      const mine = ++seq;
      chosenHours.set(server, hours);
      mount(body, loadingStrip());
      try {
        const res = await readTool("get_finops", { server, view: "high_impact", hours, limit: LIMIT }, ctx && ctx.signal);
        if (mine !== seq) return;
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        mount(body, [
          noticeStrip(noticeText(data)),
          VIZ.table(data, { rowsKey: "rows", columns: COLUMNS, emptyText: "No queries were ranked for this server." }),
        ]);
      } catch (e) {
        if (mine === seq && e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    };
    const start = chosenHours.get(server) ?? HOURS;
    const root = el("div", {}, [pickerControl("Window", WINDOWS, start, (hours) => load(hours)), body]);
    load(start);
    return root;
  },
};
