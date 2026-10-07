/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Database Sizes" tab: one row per database file from get_finops (view database_sizes): size, used and
   free space, auto growth, recovery model, VLF count, volume and the file's share of the server's monthly cost, as
   the read returns them. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, errorStrip, readTool, fmtMb, applyFormat, relTime, localTime } from "../../util.js";

// The desktop's Auto Growth text: a percentage step, a MB step, "Disabled" when growth is off, "-" when unknown.
function growthText(row) {
  if (row.is_percent_growth == null) return "-";
  if (row.is_percent_growth) return row.growth_pct == null ? "-" : row.growth_pct + "%";
  return !row.auto_growth_mb ? "Disabled" : applyFormat("int", row.auto_growth_mb) + " MB";
}

// The desktop's sort order for the column: unknown first, then the step size.
function growthSort(row) {
  if (row.is_percent_growth == null) return -1;
  if (row.is_percent_growth) return row.growth_pct ?? -1;
  return row.auto_growth_mb ?? 0;
}

const COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "file_name", label: "File" },
  { key: "file_type", label: "Type" },
  { key: "total_size_mb", label: "Total", format: "mb" },
  { key: "used_size_mb", label: "Used", format: "mb" },
  { key: "free_space_mb", label: "Free MB", format: "mb" },
  { key: "used_pct", label: "Used %", format: "num1" },
  {
    key: "max_size_mb",
    label: "Max size",
    align: "right",
    sortValue: (row) => (row.max_size_mb === -1 ? Infinity : row.max_size_mb),
    render: (row) => el("span", { text: row.max_size_mb === -1 ? "Unlimited" : row.max_size_mb == null ? "—" : fmtMb(row.max_size_mb) }),
  },
  {
    key: "auto_growth_mb",
    label: "Auto growth",
    align: "right",
    sortValue: growthSort,
    render: (row) => el("span", { text: growthText(row) }),
  },
  { key: "recovery_model", label: "Recovery model" },
  { key: "vlf_count", label: "VLF count", format: "int" },
  { key: "volume_mount_point", label: "Volume" },
  { key: "volume_total_mb", label: "Volume total", format: "mb" },
  { key: "volume_free_mb", label: "Volume free", format: "mb" },
  { key: "monthly_cost_usd", label: "Monthly cost ($)", format: "num2", hideWhenEmpty: true },
  { key: "size_note", label: "Note", wrap: true, hideWhenEmpty: true },
];

/* The most files the view lists; the grid shows every file, so the tab asks for the ceiling. */
const MAX_FILES = 500;

export const tab = {
  id: "database-sizes",
  label: "Database Sizes",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    (async () => {
      try {
        const res = await readTool("get_finops", { server, view: "database_sizes", limit: MAX_FILES }, ctx && ctx.signal);
        if (res.kind === "aborted" || res.kind === "auth") return;
        if (res.kind === "error") return mount(body, readErrorStrip(res.message));
        if (res.kind === "empty") return mount(body, emptyStrip(res.message));
        const data = res.data || {};
        const files = Array.isArray(data.rows) ? data.rows : [];
        const notes = [];
        if (typeof data.captured_at === "string" && data.captured_at) notes.push(noticeStrip("Captured " + relTime(data.captured_at) + " (" + localTime(data.captured_at) + ")"));
        if (data.truncated) notes.push(noticeStrip("Showing the largest " + files.length + " of " + data.file_count + " files."));
        if (typeof data.note === "string" && data.note.trim()) notes.push(noticeStrip(data.note));
        mount(body, [
          ...notes,
          VIZ.table({ files }, { rowsKey: "files", columns: COLUMNS, emptyText: "No database files were reported for this server." }),
        ]);
      } catch (e) {
        if (e?.name !== "AbortError") mount(body, errorStrip(String(e)));
      }
    })();
    return body;
  },
};
