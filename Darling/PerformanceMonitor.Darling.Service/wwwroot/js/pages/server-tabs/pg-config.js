/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


export const PG_SERVER_CONFIG_COLUMNS = [
  { key: "name", label: "Setting" },
  { key: "setting", label: "Value" },
  { key: "unit", label: "Unit", small: true },
  { key: "default_value", label: "Default", small: true },
  { key: "source", label: "Source", small: true },
  { key: "context", label: "Change needs", small: true },
  { key: "pending_restart", label: "Pending restart", format: "bool", small: true },
  { key: "category", label: "Category", small: true },
];

export const PG_CONFIG_CHANGE_COLUMNS = [
  { key: "changed_at", label: "Seen at", format: "time" },
  { key: "name", label: "Setting" },
  { key: "old_value", label: "From" },
  { key: "new_value", label: "To" },
  { key: "unit", label: "Unit", small: true },
  { key: "source", label: "Source", small: true },
];

