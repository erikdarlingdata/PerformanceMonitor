/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

import { fmtMs } from "../../util.js";

/**
 * A cell over a column whose store writes -1 for "there was none of this".
 *
 * Every numeric formatter in util.js would print that sentinel as a negative number — `fmtMs(-1)` is "-1 ms",
 * `fmtInt(-1)` is "-1" — and a negative duration or a negative age beside real ones reads as a small value
 * rather than as an absence. The dash is the same answer the WPF grids give the same sentinel.
 */
function sentinelCell(value, formatter) {
  return document.createTextNode(value == null || Number(value) < 0 ? "—" : formatter(value));
}

/* PostgreSQL's own thresholds, shipped by the read as constants. They are here because a percentage with no
   scale is not a measurement: 74.5% sounds comfortable and is where the failsafe engages. */
export const PG_WRAPAROUND_THRESHOLD_STATS = [
  { key: "thresholds.failsafe_engages_around_pct", label: "Failsafe engages (%)", format: "num1" },
  { key: "thresholds.server_warnings_begin_around_pct", label: "Server warns (%)", format: "num1" },
  { key: "thresholds.writes_stop_at_pct", label: "Writes stop (%)", format: "num2" },
  {
    key: "thresholds.anti_wraparound_vacuum_forced_at_pct_of_freeze_max_age",
    label: "Forced vacuum (% of freeze_max_age)",
    format: "num1",
  },
];

/* The capture tiles lead, and they are not decoration on this read: it is an EXCEPTION surface sampled once
   per collection cycle, so an empty grid is either "every transaction was short" or "nobody looked", and
   only the denominator separates them.

   `idle_in_transaction_pinning_nothing` is a first-class tile rather than a per-row detail because it is the
   correction this whole read exists to make: those sessions look exactly like the harmful ones and cost
   VACUUM nothing, and a reader who meets that count before the grid does not spend the grid looking for
   something to kill. `redacted_row_count` is here for the same reason `suppressed_estimate_count` is on the
   bloat tile - the usual cause is one missing GRANT on the whole instance. */
export const PG_SESSION_STATE_STATS = [
  { key: "captures_in_window", label: "Captures", format: "int" },
  { key: "captures_with_sessions", label: "With reportable sessions", format: "int" },
  { key: "sessions_returned", label: "Sessions returned", format: "int" },
  { key: "horizon_holder_count", label: "Pinned the xmin horizon", format: "int" },
  { key: "idle_in_transaction_pinning_nothing", label: "Idle in transaction, pinning nothing", format: "int" },
  { key: "redacted_row_count", label: "Redacted rows", format: "int" },
  { key: "truncated", label: "Truncated", format: "bool" },
];

export const PG_AUTOVACUUM_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "table_name", label: "Table" },
  { key: "severity", label: "Severity" },
  { key: "dead_tuples", label: "Dead Tuples", format: "int" },
  { key: "vacuum_threshold", label: "Vacuum At", format: "int" },
  /* The headline number: 1.0 means autovacuum should be triggering right now. Null where the table has never
     been analyzed or the collector had no threshold to report — a ratio invented there would rank a table
     nothing is known about above tables that have been measured. */
  { key: "threshold_ratio", label: "× Vacuum Threshold", format: "num2" },
  { key: "dead_tuples_growing", label: "Growing", format: "bool" },
  { key: "dead_tuple_change", label: "Change", format: "int" },
  { key: "mods_since_analyze", label: "Mods Since Analyze", format: "int" },
  { key: "analyze_threshold_ratio", label: "× Analyze Threshold", format: "num2" },
  { key: "inserts_since_vacuum", label: "Inserts Since Vacuum", format: "int" },
  { key: "autovacuum_disabled", label: "AV Disabled", format: "bool" },
  { key: "never_autovacuumed", label: "Never AV'd", format: "bool" },
  { key: "total_gb", label: "Size (GB)", format: "num2" },
  { key: "last_autovacuum", label: "Last Autovacuum", format: "time" },
  { key: "last_autoanalyze", label: "Last Autoanalyze", format: "time" },
];

/* Both counters, side by side and never merged into a worst-of: XIDs and MultiXacts wrap independently, a
   database can be comfortable on one and in trouble on the other, and the remedies differ. */
export const PG_WRAPAROUND_COLUMNS = [
  { key: "database_name", label: "Database" },
  { key: "severity", label: "Severity" },
  { key: "frozen_xid_age", label: "Frozen XID Age", format: "int" },
  { key: "xids_remaining", label: "XIDs Left", format: "int" },
  { key: "pct_toward_wraparound", label: "% to Wraparound", format: "num2" },
  { key: "pct_toward_emergency_vacuum", label: "% to Emergency Vacuum", format: "num1" },
  { key: "window_peak_frozen_xid_age", label: "Peak Age (window)", format: "int" },
  { key: "freezing_is_keeping_up", label: "Keeping Up", format: "bool" },
  { key: "min_multixid_age", label: "MultiXact Age", format: "int" },
  { key: "multixids_remaining", label: "MultiXacts Left", format: "int" },
  { key: "pct_toward_multixact_wraparound", label: "% to MultiXact Wraparound", format: "num2" },
  { key: "pct_toward_multixact_emergency", label: "% to MultiXact Emergency", format: "num1" },
  { key: "allows_connections", label: "Connectable", format: "bool" },
  { key: "measured_at", label: "Measured", format: "time" },
];

/* Every cause, not only the current winner. Four unrelated things produce "autovacuum runs, reports success,
   reclaims nothing", each with a different fix, so `source` and `remedy` are the columns that matter and
   `xmin_age` on its own is the one that leaves the reader where they started. */
export const PG_XMIN_COLUMNS = [
  { key: "source", label: "Source" },
  { key: "is_currently_winning", label: "Winning", format: "bool" },
  { key: "xmin_age", label: "xmin Age", format: "int" },
  { key: "peak_xmin_age", label: "Peak Age", format: "int" },
  { key: "holder", label: "Holder", wrap: true },
  { key: "detail", label: "Detail", wrap: true },
  { key: "samples_as_winner", label: "Samples Winning", format: "int" },
  { key: "samples", label: "Samples", format: "int" },
  { key: "pct_of_window_winning", label: "% of Window", format: "num1" },
  { key: "measured_at", label: "Measured", format: "time" },
  { key: "remedy", label: "Remedy", clamp: true },
];

/* peak_horizon_age is rendered rather than formatted, and that is the one non-negotiable cell on this grid:
   -1 means the session pinned NOTHING - no snapshot and no transaction id in any sample - and it is a
   MEASURED finding rather than a missing value. `format: "int"` would print "-1", which reads as a small age
   next to a long duration and is precisely the misreading that gets a harmless backend killed. The WPF grid
   renders the same sentinel as the same words for the same reason. */
export const PG_SESSION_STATE_COLUMNS = [
  { key: "pid", label: "PID", format: "int" },
  /* The collector's synthetic (backend_start, pid) identity. Shown because a pid alone is not one - pids are
     reused, and this is the id that matches the same backend on the blocking grid. */
  { key: "backend_id", label: "Backend", mono: true },
  /* sevKey, not statusSev, and the same shape the bloat and index grids use: the band is SERVER-computed
     (R1 - the browser never re-derives one) and it names one of the four house words the shared sev-*
     CSS defines. The read returned a private lower-case vocabulary in its first draft, which would have
     attached sev-critical / sev-info / sev-none - classes that match no rule, so the badge would have
     rendered UNSTYLED rather than failing. That is the kind of defect that ships, so the fix went into
     the read rather than being worked around here. */
  { key: "severity", label: "Severity", sevKey: "severity" },
  { key: "database", label: "Database" },
  { key: "username", label: "User" },
  { key: "application_name", label: "App" },
  { key: "client_addr", label: "Client" },
  { key: "backend_type", label: "Backend Type" },
  { key: "last_state", label: "State" },
  {
    key: "peak_horizon_age",
    label: "Pinned Horizon (peak)",
    render: (row) =>
      document.createTextNode(
        row.peak_horizon_age == null
          ? "—"
          : Number(row.peak_horizon_age) < 0
            ? "pins nothing"
            : Number(row.peak_horizon_age).toLocaleString() + " transactions",
      ),
  },
  { key: "pinned_the_horizon", label: "Pinned Anything", format: "bool" },
  /* The sample counts, not a flag: a session that was the oldest holder once in a hundred captures was
     momentarily at the front of a queue every write transaction passes through, and one that held it in
     ninety-eight is why vacuum reclaims nothing. Those are opposite findings. */
  { key: "horizon_holder_samples", label: "Holder Samples", format: "int" },
  { key: "idle_in_transaction_samples", label: "Idle-In-Xact Samples", format: "int" },
  { key: "sample_count", label: "Samples", format: "int" },
  /* PEAKS, not averages - averaging a hundred samples of a transaction that grew monotonically reports
     about half of what happened.

     The read's own pre-formatted strings are used for the two it ships, rather than `format: "ms"` over the
     raw milliseconds: those columns carry the -1 sentinel, and the read already renders it as "not
     measured". The third has no string twin, so it goes through sentinelCell for the same reason. */
  { key: "peak_xact_duration", label: "Peak Xact" },
  { key: "peak_state_duration", label: "Peak State" },
  /* Backend age against transaction age: a connection created ten minutes ago that has been idle in
     transaction for all ten is a pool handing out a session nobody finished with, while a three-day-old
     worker holding one for ten minutes is a code path that forgot to commit. Same duration, different bug. */
  { key: "backend_age", label: "Backend Age" },
  {
    key: "peak_query_duration_ms",
    label: "Peak Query",
    render: (row) => sentinelCell(row.peak_query_duration_ms, fmtMs),
  },
  /* -1 means the session held no snapshot / no transaction id, which is an absence rather than a small age. */
  {
    key: "peak_xmin_age",
    label: "Peak xmin Age",
    render: (row) => sentinelCell(row.peak_xmin_age, (v) => Number(v).toLocaleString()),
  },
  {
    key: "peak_xid_age",
    label: "Peak XID Age",
    render: (row) => sentinelCell(row.peak_xid_age, (v) => Number(v).toLocaleString()),
  },
  { key: "last_wait_event_type", label: "Wait Type" },
  { key: "last_wait_event", label: "Wait Event" },
  /* The leading SQL keyword, whitelisted at collection - NOT a truncation of the statement. No raw query
     text is stored anywhere: pg_stat_activity.query carries literal parameter values. */
  { key: "last_command_tag", label: "Command" },
  { key: "last_query_id", label: "Query ID", mono: true },
  { key: "query_id_note", label: "Query ID Note", clamp: true },
  { key: "finding", label: "Finding", clamp: true },
  /* Instance context from this backend's most recent sample: two idle-in-transaction sessions out of six
     connections is a different server from two out of four thousand. */
  { key: "idle_in_transaction_on_instance", label: "Idle In Xact (instance)", format: "int" },
  { key: "active_sessions_on_instance", label: "Active (instance)", format: "int" },
  { key: "sessions_on_instance", label: "Sessions (instance)", format: "int" },
  { key: "reportable_sessions_on_instance", label: "Reportable (instance)", format: "int" },
  { key: "state_was_redacted", label: "Redacted", format: "bool" },
  { key: "capture_was_truncated", label: "Capture Truncated", format: "bool" },
  { key: "first_seen_at", label: "First Seen", format: "time" },
  { key: "last_seen_at", label: "Last Seen", format: "time" },
];

