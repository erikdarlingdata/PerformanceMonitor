/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Application Connections" tab: per-application connection counts (average and peak; running, sleeping, dormant),
   CPU, reads and writes from get_finops (view application_connections) over a window picked from 1 hour to 7 days. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool } from "../../util.js";

// The default window.
const HOURS = 24;

// The desktop's window picker (FinOpsTab.xaml ~:497-499), as hours.
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
    let seq = 0;
    const load = async (hours) => {
      const mine = ++seq;
      chosenHours.set(server, hours);
      mount(body, loadingStrip());
      try {
        const res = await readTool("get_finops", { server, view: "application_connections", hours }, ctx && ctx.signal);
        if (mine !== seq) return;
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        mount(body, [
          noticeStrip(noticeText(data)),
          VIZ.table(data, { rowsKey: "rows", columns: COLUMNS, emptyText: "No application connection data was recorded for this server." }),
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
