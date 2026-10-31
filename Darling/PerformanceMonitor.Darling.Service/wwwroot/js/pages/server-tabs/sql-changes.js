/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


export const SERVER_CHANGE_COLUMNS = [
  { key: "change_time", label: "Changed", format: "time" },
  { key: "configuration_name", label: "Setting" },
  { key: "old_value_configured", label: "Old (configured)" },
  { key: "new_value_configured", label: "New (configured)" },
  { key: "old_value_in_use", label: "Old (in use)" },
  { key: "new_value_in_use", label: "New (in use)" },
];

export const DB_CHANGE_COLUMNS = [
  { key: "change_time", label: "Changed", format: "time" },
  { key: "database_name", label: "Database" },
  { key: "setting_name", label: "Setting" },
  { key: "old_value", label: "Old" },
  { key: "new_value", label: "New" },
];

export const TRACE_FLAG_CHANGE_COLUMNS = [
  { key: "change_time", label: "Changed", format: "time" },
  { key: "trace_flag", label: "Flag", format: "int" },
  { key: "change_type", label: "Change" },
  { key: "previous_status", label: "Previous" },
  { key: "new_status", label: "New" },
  { key: "scope", label: "Scope" },
];

