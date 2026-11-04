/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

/*
 * #7486: the plain name a person reads for an analysis finding's category. The category is an internal word (the source of the
 * finding's root fact), and `bad_actor` beside "One expensive query dominated the server's work this window" reads like a security
 * event. The raw value stays in the MCP reply, stored rows, mute and alert-route matching and CSV export; only the text on screen
 * goes through this. The same map lives in PerformanceMonitor.Analysis/FindingCategoryNames.cs, and a test holds the two equal.
 */

export const FINDING_CATEGORY_NAMES = {
  absolution: "No problem found",
  anomaly: "Unusual activity",
  bad_actor: "Expensive query",
  blocking: "Blocking",
  config: "Server settings",
  coverage: "Data coverage",
  cpu: "CPU",
  database_config: "Database settings",
  disk: "Disk space",
  io: "Disk I/O",
  jobs: "Agent jobs",
  memory: "Memory",
  queries: "Queries",
  sessions: "Sessions",
  tempdb: "TempDB",
  waits: "Waits",
  pg_bloat: "PostgreSQL table bloat",
  pg_blocking: "PostgreSQL blocking",
  pg_buffer: "PostgreSQL buffer cache",
  pg_config: "PostgreSQL settings",
  pg_cpu: "PostgreSQL CPU",
  pg_database: "PostgreSQL databases",
  pg_growth: "PostgreSQL growth",
  pg_io: "PostgreSQL disk I/O",
  pg_kernel: "PostgreSQL host",
  pg_memory: "PostgreSQL memory",
  pg_plans: "PostgreSQL plans",
  pg_posture: "PostgreSQL durability settings",
  pg_queries: "PostgreSQL queries",
  pg_replication: "PostgreSQL replication",
  pg_sessions: "PostgreSQL sessions",
  pg_temp: "PostgreSQL temp files",
  pg_vacuum: "PostgreSQL vacuum",
  pg_waits: "PostgreSQL waits",
  pg_write: "PostgreSQL writes",
};

/** The display name for a category. One the map does not know shows as it is, so a new source is never a blank cell. */
export function findingCategoryName(category) {
  if (category == null || category === "") return "";
  const raw = String(category);
  return Object.prototype.hasOwnProperty.call(FINDING_CATEGORY_NAMES, raw) ? FINDING_CATEGORY_NAMES[raw] : raw;
}
