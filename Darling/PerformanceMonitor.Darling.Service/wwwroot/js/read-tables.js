/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * The table definitions for reads, once: read name -> { rowsKey, columns, emptyText }. An alert notebook's read
 * cell names a read and `viz: "table"` and nothing else, so the page looks the table up here. The starter
 * dashboards spread the same entries into their table panels.
 */
export const READ_TABLES = {};

/**
 * The cell with its table definition filled in from READ_TABLES. A cell that already carries its own columns, a
 * cell that is not a table, and a read with no entry come back as they were.
 */
export function resolveReadTable(cell) {
  if (!cell || cell.viz !== "table" || (Array.isArray(cell.columns) && cell.columns.length)) return cell;
  const entry = READ_TABLES[cell.read];
  return entry ? { ...cell, ...entry } : cell;
}
