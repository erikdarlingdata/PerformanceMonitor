/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Database Resources" tab: per-database CPU, reads, writes, executions and file I/O from get_finops
   (view database_resources) over a window picked from 1 hour to 7 days, with each database's share of the server's CPU and I/O. */

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
    let seq = 0;
    const load = async (hours) => {
      const mine = ++seq;
      mount(body, loadingStrip());
      try {
        const res = await readTool("get_finops", { server, view: "database_resources", hours }, ctx && ctx.signal);
        if (mine !== seq) return;
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        mount(body, [
          noticeStrip(noticeText(data)),
          VIZ.table(data, { rowsKey: "rows", columns: COLUMNS, emptyText: "No database resource usage was recorded for this server." }),
        ]);
      } catch (e) {
        if (mine === seq && e?.name !== "AbortError") mount(body, errorStrip("Could not render this tab: " + (e && e.message ? e.message : String(e))));
      }
    };
    const root = el("div", {}, [pickerControl("Window", WINDOWS, HOURS, (hours) => load(hours)), body]);
    load(HOURS);
    return root;
  },
};
