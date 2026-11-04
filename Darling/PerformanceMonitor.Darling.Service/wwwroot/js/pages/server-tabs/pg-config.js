/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { configValueCell } from "./config-values.js";


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
  /* #7416 item 3: a value that only gained or lost whitespace shows it (log_line_prefix), so From and To never read the same. */
  { key: "old_value", label: "From", display: (r) => configValueCell(r, "old_value", "new_value") },
  { key: "new_value", label: "To", display: (r) => configValueCell(r, "new_value", "old_value") },
  { key: "unit", label: "Unit", small: true },
  { key: "source", label: "Source", small: true },
];

