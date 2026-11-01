/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


/* worst_* beside latest on every lag measure. Lag is spiky, a sample catches one instant, and a grid showing
   only the latest value reports a replica that fell an hour behind and caught up as perfectly healthy. */
export const PG_REPLICATION_STATS_COLUMNS = [
  { key: "application_name", label: "Replica" },
  { key: "state", label: "State" },
  { key: "sync_state", label: "Sync" },
  { key: "replay_lag_ms", label: "Replay Lag", format: "ms" },
  { key: "worst_replay_lag_ms", label: "Worst Lag", format: "ms" },
  { key: "replay_bytes_behind", label: "Bytes Behind", format: "int" },
  { key: "worst_replay_bytes_behind", label: "Worst Behind", format: "int" },
  { key: "samples", label: "Samples", format: "int" },
];

export const PG_SLOT_COLUMNS = [
  { key: "slot_name", label: "Slot" },
  { key: "severity", label: "Severity" },
  { key: "slot_type", label: "Type" },
  { key: "plugin", label: "Plugin" },
  { key: "database_name", label: "Database" },
  { key: "is_active", label: "Active", format: "bool" },
  { key: "wal_status", label: "WAL Status" },
  { key: "retained_wal_gb", label: "Retained WAL (GB)", format: "num2" },
  { key: "retained_wal_growth_gb_per_hour", label: "Growth (GB/h)", format: "num2" },
  { key: "safe_wal_size_bytes", label: "Safe WAL (bytes)", format: "int" },
  { key: "xmin_age", label: "xmin Age", format: "int" },
  { key: "catalog_xmin_age", label: "Catalog xmin Age", format: "int" },
  { key: "inactive_since", label: "Inactive Since", format: "time" },
  { key: "invalidation_reason", label: "Invalidated", wrap: true },
  { key: "conflicting", label: "Conflicting", format: "bool" },
];
