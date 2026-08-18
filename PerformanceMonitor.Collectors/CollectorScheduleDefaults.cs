/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The default per-collector cadence and retention — the shared source both SKUs schedule by,
/// so portable Lite and the Darling service collect on identical rhythms out of the box. Lite's
/// ScheduleManager carries the same table (user-editable per install) and an identity-pin test
/// asserts the two cannot drift; Darling consumes this directly (no schedule knobs until someone
/// needs them — defaults over speculative config). FrequencyMinutes 0 = collect once on server
/// load only (config snapshots).
/// </summary>
public static class CollectorScheduleDefaults
{
    /// <summary>
    /// Per-collector cadence + retention, plus the collector's default enabled state. Nearly every
    /// collector ships ENABLED (<see cref="DefaultEnabled"/> = true); a collector that must ship OFF
    /// and be opted into (long_query_completions — a completion trace is not free on a busy server,
    /// #1496) sets it false. Both SKUs consult this: Lite's ScheduleManager seeds its per-install
    /// enabled flag from it, and Darling's <c>StoreConfigProvider.ResolveSchedule</c> falls back to it
    /// when no <c>config_collector_schedules</c> override row exists — so "reset to defaults" (which
    /// deletes override rows) returns a default-off collector to OFF, not ON.
    /// </summary>
    public sealed record Entry(int FrequencyMinutes, int RetentionDays, bool DefaultEnabled = true);

    public static IReadOnlyDictionary<string, Entry> All { get; } = new Dictionary<string, Entry>(StringComparer.OrdinalIgnoreCase)
    {
        ["wait_stats"] = new(1, 30),
        ["latch_stats"] = new(1, 30),
        ["spinlock_stats"] = new(1, 30),
        ["cpu_scheduler_stats"] = new(1, 30),
        ["plan_cache_stats"] = new(5, 30),
        ["query_stats"] = new(1, 30),
        ["procedure_stats"] = new(1, 30),
        ["query_store"] = new(5, 30),
        ["query_snapshots"] = new(1, 7),
        ["cpu_utilization"] = new(1, 30),
        ["file_io_stats"] = new(1, 30),
        ["memory_stats"] = new(1, 30),
        ["memory_clerks"] = new(5, 30),
        ["memory_pressure_events"] = new(5, 30),
        ["tempdb_stats"] = new(1, 30),
        ["perfmon_stats"] = new(1, 30),
        /* #1963: the deadlocks read costs ~273ms of FIXED dm_xe_session_targets serialization no matter
           how empty the ring buffer is (field-measured at 284 bytes of content), so cadence is the only
           lever on its overhead. The buffer retains events between polls and the watermark catches up, so
           a slower cadence keeps the same data - the trade is detection latency, and a deadlock is
           forensic by the time anyone reads it (the victim already rolled back). Erik ruled 2026-08-01:
           default to the 5-minute tier beside the other event-buffer readers (system_health_events,
           default_trace_events). Loss bound: a storm big enough to cycle the buffer between polls drops
           events a faster poll would have caught - the buffer's capacity bounds that either way. */
        ["deadlocks"] = new(5, 30),
        ["server_config"] = new(0, 30),
        ["database_config"] = new(0, 30),
        /* Per-database state as a time series (#db-offline-alert): unlike the load-time
           database_config snapshot (frequency 0), this feeds the "database offline / unhealthy
           state" alert, so it must run on a cadence to catch a database going OFFLINE / SUSPECT /
           RESTORING after load. A few-row read against in-memory catalog metadata, so per-minute is
           cheap — matching the other health time series (wait_stats, cpu_utilization). */
        ["database_states"] = new(1, 30),
        ["memory_grant_stats"] = new(1, 30),
        ["waiting_tasks"] = new(1, 7),
        ["dmv_blocking_snapshot"] = new(1, 30),
        ["blocked_process_report"] = new(1, 30),
        /* #1496 long-running query completion trace: seeded DISABLED (DefaultEnabled: false). A
           completion trace (rpc_completed/sql_batch_completed) fires per statement/batch even though
           the duration predicate discards most of them, so it is opt-in per fleet; enabling it creates
           the XE session on the monitored servers and disabling it DROPS the session there. Cadence +
           retention mirror the sibling XE collectors. */
        ["long_query_completions"] = new(1, 30, DefaultEnabled: false),
        ["database_scoped_config"] = new(0, 30),
        /* #2319: hourly, NOT the config family's on-load cadence — actual_state, readonly_reason and
           current_storage_size_mb change BY THEMSELVES (the cap-hit transition to READ_ONLY is the point
           of collecting this), and an on-load snapshot would miss the transition until the next
           reconnect. One cheap row per database per hour; 30 days to match its config siblings. */
        ["query_store_health"] = new(60, 30),
        ["trace_flags"] = new(0, 30),
        ["running_jobs"] = new(5, 7),
        ["database_size_stats"] = new(60, 90),
        ["index_object_stats"] = new(1440, 90),
        ["server_properties"] = new(0, 365),
        ["session_stats"] = new(5, 30),
        ["session_summary_stats"] = new(5, 30),
        ["system_health_events"] = new(5, 30),
        ["default_trace_events"] = new(5, 30),
        ["job_history"] = new(5, 365),
        ["agent_status"] = new(5, 7),
        /* #991 Availability Group health. Per-minute like the other health time series (wait_stats,
           cpu_utilization): send/redo queue depth and secondary lag are exactly the signals whose
           spikes are lost at a coarser grain, and both queries are a handful of rows against
           in-memory AG metadata. On an AG-less server every cycle is a zero-row read. */
        ["ag_replica_states"] = new(1, 30),
        ["ag_database_replica_states"] = new(1, 30),
        /* #1952 automatic plan correction. Deliberately NOT the FrequencyMinutes 0 (on-load) tier the
           other per-database config snapshots use, even though half of what it reads is enablement
           state: sys.dm_db_tuning_recommendations is documented as living only until the instance
           restarts, and a restart mid-verification silently unforces the plan the engine had forced.
           An on-load-only collector would capture that database once and then miss every Active and
           Verifying recommendation the engine worked through afterwards - and those rows exist
           nowhere else once the engine drops them. The 5-minute tier is where the other enumerating
           per-database reader already sits (query_store); the read itself is a handful of rows per
           database against in-memory recommendation state, not a Query Store scan. */
        ["plan_correction"] = new(5, 30),
        /* #1951 ADR persistent version store. Cadence and retention deliberately MATCH
           database_size_stats (60/90) rather than the per-minute health tier: PVS size is the same
           kind of slow-moving disk-pressure telemetry, it is read beside database sizes, and pairing
           the two cadences is what lets a chart put "the database grew" and "its version store grew"
           on one time axis without resampling. The fast-moving leading indicators an operator would
           want per-minute — the long-running transaction itself, the snapshot scan holding cleanup
           back — are already collected per-minute by dmv_blocking_snapshot and waiting_tasks; what
           this collector adds is the slow CONSEQUENCE, and 90 days is the window that shows a PVS
           trend against the database-growth trend it explains. */
        ["pvs_stats"] = new(60, 90),

        /* PostgreSQL. Same cadence and horizon as wait_stats, deliberately: it is the same kind of
           signal (cumulative counters, delta-on-write) read at the same resolution, and matching the
           two means a mixed-engine store shows one time axis without resampling.

           Enabled by default despite being Aurora-only, because the cost of it being wrong is
           nothing: on a SQL Server target the engine gate drops it before dispatch with no log row,
           and on non-Aurora PostgreSQL its own AppliesTo returns false. It only ever runs where
           there is something to read. */
        ["pg_wait_stats"] = new(1, 30),

        /* Same cadence and horizon as query_stats, its SQL Server counterpart. */
        ["pg_statement_stats"] = new(1, 30),

        /* Freeze headroom moves slowly — autovacuum shifts it in steps, not continuously — so a
           5-minute read is ample, and 90 days is the horizon that shows an age trend against the
           table-growth trend that usually explains it. Cheap: a handful of rows from a shared
           catalog. */
        ["pg_wraparound_stats"] = new(5, 90),

        /* Per-minute, unlike its wraparound sibling: an xmin holder is the FAST-moving leading
           indicator, and the thing an operator wants is the session or slot that appeared minutes
           ago, before it has cost anything. At most five rows a cycle. 30 days matches the other
           per-minute health series. */
        ["pg_xmin_horizon"] = new(1, 30),

        /* Per-minute: retained WAL on an abandoned slot grows at whatever rate the server generates
           WAL, which on a busy writer fills a volume in hours, not days. 90 days of retention because
           the question after an incident is "how long was that slot orphaned", and that answer has to
           outlive the incident. A handful of rows a cycle. */
        ["pg_replication_slots"] = new(1, 90),

        /* Hourly, unlike every other PostgreSQL collector, because this one is a per-database
           fan-out and on PostgreSQL that means one CONNECTION per database per cycle — a database
           count that is fine hourly would be a connection storm per minute. Autovacuum also works on
           the scale of minutes to hours, so a per-minute read would mostly re-record the same state.
           90 days matches database_size_stats: the useful reading is a bloat trend, not a spot check. */
        ["pg_autovacuum_stats"] = new(60, 90),

        /* Back to per-minute: pg_stat_io is cluster-wide (one connection, no fan-out) and returned
           25-37 rows per snapshot on the fleet, so the cost is the same order as pg_wait_stats. 30 days
           matches the other rate collectors — the value here is correlating an I/O shift against a
           deployment, which is a days-to-weeks question, not a quarterly one. */
        ["pg_io_stats"] = new(1, 30),

        /* Per-minute, and the cadence IS the limitation. Unlike SQL Server's blocked-process report —
           where the engine itself records an event when blocking crosses a threshold, so evidence exists
           whether or not anyone looked — PostgreSQL records nothing. This is a sample, so blocking shorter
           than one minute is simply not seen. A minute is the floor worth paying for:
           pg_blocking_pids() takes ShareLock on the lock manager partitions per call, and the whole
           point of a blocking monitor is to not become the contention it reports. 30 days matches the
           other per-minute series, and is the horizon that answers "is this the same chain every
           Monday at open" — the question that turns a one-off into a pattern. */
        ["pg_blocking"] = new(1, 30),
    };
}
