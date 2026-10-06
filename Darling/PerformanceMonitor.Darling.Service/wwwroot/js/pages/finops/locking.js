/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/* FinOps "Locking & Contention" tab: the per-index lock and latch waits from get_object_locking (latest daily
   snapshot, most contended first, up to 200 rows), with the lock-wait counts and reserved size the server
   page's Object Contention table leaves out. A Database box (database-box.js) above the table narrows the read to one database:
   it takes one name, sent exactly as typed except that a name matching a suggestion ignoring case goes in its stored spelling, and
   empty means All databases. Its suggestions come from /api/server-databases, which stops at 5,000 names; when it cuts the list the
   box asks the route again with the typed text, so a name past the cut can still be found. The choice is kept per server in module
   state, so it survives the page's poll rebuilds, and a new choice remounts only the table below the box. The read's snapshot
   time (captured_at) is not shown: a table panel has no slot for a top-level field. */

import { el, mount } from "../../util.js";
import { renderPanel } from "../../panels.js";
import { databaseBox, newBoxChoice } from "./database-box.js";

/* #5311: the four wait columns are shaded the desktop's way. The service bands each column over the rows it returns and
   sends `heat: [row lock, page lock, page latch, page I/O latch]` (a band 0 to 7, or null for no shade); the browser only
   turns the band it was given into the existing .heat-band-N class and computes nothing. */
function heatClass(i) {
  return (row) => (Array.isArray(row.heat) && Number.isInteger(row.heat[i]) ? "heat-band-" + row.heat[i] : null);
}

const LOCKING_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "schema_name", label: "Schema" },
  { key: "table_name", label: "Table" },
  { key: "index_name", label: "Index" },
  { key: "index_type", label: "Type" },
  { key: "reserved_mb", label: "Reserved (MB)", format: "num1" },
  { key: "total_rows", label: "Rows", format: "int" },
  { key: "row_lock_wait_count", label: "Row lock waits", format: "int" },
  { key: "row_lock_wait_ms", label: "Row lock wait", format: "ms", cellClass: heatClass(0) },
  { key: "page_lock_wait_count", label: "Page lock waits", format: "int" },
  { key: "page_lock_wait_ms", label: "Page lock wait", format: "ms", cellClass: heatClass(1) },
  { key: "lock_escalations", label: "Escalations", format: "int" },
  { key: "page_latch_wait_ms", label: "Page latch", format: "ms", cellClass: heatClass(2) },
  { key: "page_io_latch_wait_ms", label: "Page IO latch", format: "ms", cellClass: heatClass(3) },
];

// The latest choice per server: the Database box's fields (db, draft, caret, focused, names, ...).
const choices = new Map();

function choiceFor(server) {
  let c = choices.get(server);
  if (!c) {
    c = newBoxChoice();
    choices.set(server, c);
  }
  return c;
}

export const tab = {
  id: "locking",
  label: "Locking & Contention",
  build(server, ctx) {
    const choice = choiceFor(server);
    const content = el("div", {});
    const box = databaseBox(choice, { onCommit: () => show(), server });
    const controls = el("div", { class: "sort-control" }, box.nodes);

    function show() {
      const params = { server, limit: 200 };
      if (choice.db) params.database_name = choice.db;
      mount(content, [
        renderPanel({
          title: "Locking & Contention",
          subtitle: choice.db ? "daily collection, database " + choice.db : "daily collection",
          read: "get_object_locking",
          params,
          viz: "table",
          rowsKey: "objects",
          columns: LOCKING_COLUMNS,
          emptyText: choice.db
            ? "No lock-wait rows recorded for this database. Index and object stats are collected daily."
            : "No lock-wait rows recorded. Index and object stats are collected daily.",
          noteKey: "optimized_locking_note",
          /* `note` is the read's truncation sentence ("TRUNCATED: more than N indexes ...") when the 200-row cap
             cuts the list, and a "Complete: ..." sentence otherwise, so a capped page never looks like the whole. */
          moreNoteKeys: ["note", "separately_monitored_note"],
          span: 2,
        }),
      ]);
    }

    show();
    return el("div", {}, [controls, content]);
  },
};
