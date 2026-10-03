/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Locking & Contention" tab: the per-index lock and latch waits from get_object_locking (latest daily
   snapshot, most contended first, up to 200 rows), with the lock-wait counts and reserved size the server
   page's Object Contention table leaves out. The read has no database filter, so there is no picker. */

import { renderPanel } from "../../panels.js";

const LOCKING_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_type", label: "Type" },
  { key: "reserved_mb", label: "Reserved (MB)", format: "num1" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "row_lock_wait_count", label: "Row lock waits", format: "int" },
  { key: "row_lock_wait_ms", label: "Row lock wait", format: "ms" },
  { key: "page_lock_wait_count", label: "Page lock waits", format: "int" },
  { key: "page_lock_wait_ms", label: "Page lock wait", format: "ms" },
  { key: "lock_escalations", label: "Escalations", format: "int" },
  { key: "page_latch_wait_ms", label: "Page latch", format: "ms" },
  { key: "page_io_latch_wait_ms", label: "Page IO latch", format: "ms" },
];

export const tab = {
  id: "locking",
  label: "Locking & Contention",
  build(server, ctx) {
    return renderPanel({
      title: "Locking & Contention",
      subtitle: "daily collection",
      read: "get_object_locking",
      params: { server, limit: 200 },
      viz: "table",
      rowsKey: "objects",
      columns: LOCKING_COLUMNS,
      emptyText: "No lock-wait rows recorded. Index and object stats are collected daily.",
      noteKey: "optimized_locking_note",
      moreNoteKeys: ["separately_monitored_note"],
      span: 2,
    });
  },
};
