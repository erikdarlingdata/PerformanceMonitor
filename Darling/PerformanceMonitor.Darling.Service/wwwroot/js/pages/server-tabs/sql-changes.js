/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { configValueCell } from "./config-values.js";
import { localTime } from "../../util.js";

/* #7662: change_time is the first capture that held the new value, and the config snapshots run on connect and once a day,
   so it can trail the change by a day. The grid says so: the column is "First seen", and a row that has the capture before
   it gives the span the change happened in. A state_desc row dated from database_states has no earlier capture and shows
   its one time. Both times are shown in the browser's zone. */
export function firstSeenCell(r) {
  const seen = localTime(r.change_time);
  return r.previous_capture_time ? "between " + localTime(r.previous_capture_time) + " and " + seen : seen;
}


export const SERVER_CHANGE_COLUMNS = [
  { key: "change_time", label: "First seen", display: firstSeenCell },
  { key: "configuration_name", label: "Setting" },
  { key: "old_value_configured", label: "Old (configured)" },
  { key: "new_value_configured", label: "New (configured)" },
  { key: "old_value_in_use", label: "Old (in use)" },
  { key: "new_value_in_use", label: "New (in use)" },
];

export const DB_CHANGE_COLUMNS = [
  { key: "change_time", label: "First seen", display: firstSeenCell },
  { key: "database_name", label: "Database", name: true },
  { key: "setting_name", label: "Setting" },
  /* #7416 item 3: a value that only gained or lost whitespace shows it, so Old and New never read the same. */
  { key: "old_value", label: "Old", display: (r) => configValueCell(r, "old_value", "new_value") },
  { key: "new_value", label: "New", display: (r) => configValueCell(r, "new_value", "old_value") },
];

export const TRACE_FLAG_CHANGE_COLUMNS = [
  { key: "change_time", label: "Changed", format: "time" },
  { key: "trace_flag", label: "Flag", format: "int" },
  { key: "change_type", label: "Change" },
  { key: "previous_status", label: "Previous" },
  { key: "new_status", label: "New" },
  { key: "scope", label: "Scope" },
];

