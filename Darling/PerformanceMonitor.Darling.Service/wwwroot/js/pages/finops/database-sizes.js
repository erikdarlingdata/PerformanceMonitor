/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Database Sizes" tab: one row per database file from get_database_sizes (size, used space, growth
   settings and the volume it sits on), as the read returns them. */

import { VIZ } from "../../panels.js";
import { el, mount, loadingStrip, emptyStrip, noticeStrip, readErrorStrip, readTool } from "../../util.js";

const COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "file_name", label: "File" },
  { key: "file_type", label: "Type" },
  { key: "total_size_mb", label: "Total", format: "mb" },
  { key: "used_size_mb", label: "Used", format: "mb" },
  { key: "auto_growth_mb", label: "Auto-growth", format: "mb" },
  { key: "max_size_mb", label: "Max size", format: "mb" },
  { key: "volume_mount_point", label: "Volume" },
  { key: "volume_total_mb", label: "Volume total", format: "mb" },
  { key: "volume_free_mb", label: "Volume free", format: "mb" },
];

export const tab = {
  id: "database-sizes",
  label: "Database Sizes",
  build(server, ctx) {
    const body = el("div", {}, [loadingStrip()]);
    (async () => {
      const res = await readTool("get_database_sizes", { server }, ctx && ctx.signal);
      if (res.kind === "aborted" || res.kind === "auth") return;
      if (res.kind === "error") return mount(body, readErrorStrip(res.message));
      if (res.kind === "empty") return mount(body, emptyStrip(res.message));
      const data = res.data || {};
      const files = [];
      for (const db of data.databases || []) {
        for (const f of db.files || []) files.push({ ...f, database_name: db.database_name });
      }
      const notes = [];
      if (typeof data.captured_at === "string" && data.captured_at) notes.push(noticeStrip("Captured at " + data.captured_at));
      if (typeof data.note === "string" && data.note.trim()) notes.push(noticeStrip(data.note));
      mount(body, [
        ...notes,
        VIZ.table({ files }, { rowsKey: "files", columns: COLUMNS, emptyText: "No database files were reported for this server." }),
      ]);
    })();
    return body;
  },
};
