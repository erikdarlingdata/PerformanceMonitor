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

        /* Per-minute, and cheap enough to be uncontroversial: pg_stat_database is cluster-wide, so this is
           one query on the connection the collector already has, returning one row per database — single
           digits to low tens on every target in the fleet. No fan-out, unlike pg_autovacuum_stats.
           The cadence IS the value, not a cost to justify: a temp-file spill is what you correlate against
           a deployment or a nightly job, and at an hourly grain "the reporting database started spilling"
           loses the minute that would have named the cause. 30 days matches the other per-minute rate
           collectors — the question this answers is "did this start on Tuesday", not a quarterly trend. */
        ["pg_database_stats"] = new(1, 30),

        /* DAILY, and the cadence is inherited from index_object_stats — the SQL Server collector that
           answers the same question — rather than from the PostgreSQL fan-out sibling. "Has anything
           scanned this index" is a structural question about a schema, not a rate: an hourly sample would
           record the same catalog facts 24 times a day and cost 24x the fan-out connections to do it. The
           counters are cumulative, so a daily sample loses no total; it only coarsens WHEN a scan happened,
           and nobody drops an index on the strength of which hour it was last used.

           90 days of retention, and this is the number that actually matters. The retention window IS the
           evidence: an index can only be called unused for as long as we have been watching it, so the
           window has to outlast the slowest query that might legitimately need it. 30 days cannot clear a
           monthly report; 90 covers monthly and quarterly jobs. It still cannot clear an ANNUAL one, which
           is why the read reports the observed window rather than asserting an index is unused. */
        ["pg_index_usage_stats"] = new(1440, 90),

        /* Hourly, matching pg_autovacuum_stats deliberately rather than by copying: this collector measures
           the DAMAGE whose CAUSE that one measures, and correlating the two requires a common grain — at
           different cadences "vacuum fell behind at 14:00 and bloat grew" stops being a sentence the data
           can support. It is also the second per-database fan-out, so sharing the cadence means one
           connection-budget decision instead of two.

           90 days matches pg_autovacuum_stats and database_size_stats for the same reason: the useful
           reading of bloat is a trend — is this table's waste growing, holding, or being reclaimed — and a
           spot percentage on its own is what gets someone to run VACUUM FULL on a Tuesday. */
        ["pg_table_bloat_stats"] = new(60, 90),

        /* One minute, matching pg_blocking, and for the same reason rather than by copying it: both read
           pg_stat_activity, both are SAMPLES of a view that records nothing on its own, and the cadence IS
           the resolution — a transaction shorter than the interval is invisible no matter what else is
           tuned. This one is the cheaper of the two: it makes no pg_blocking_pids() call, takes no
           lock-manager ShareLock, and fans out to no databases, so a minute costs a single indexed scan of
           an in-memory view per cycle.

           A minute cannot see a ten-second idle-in-transaction reliably even though ten seconds is the
           storage floor, and that is accepted rather than papered over. What this collector is for is the
           CHRONIC holder — the session that has been parked for minutes or hours — which is also the only
           kind that can pin the xmin horizon long enough to starve vacuum. Sampling faster would buy
           recall on episodes that by definition cannot cause the harm.

           30 days matches pg_blocking. The question is "how often does this application park a transaction,
           and is it getting worse" — a month covers a release cycle, which is the unit at which someone can
           actually act on the answer. Longer would mean keeping per-session rows, which are the widest and
           most numerous thing here, well past the point anyone would correlate them to a deploy. */
        ["pg_session_states"] = new(1, 30),
        /* #2564 plan-capture readiness. HOURLY, not per-minute: every facet is a parameter-group setting,
           and on Aurora/RDS changing one needs a reboot - so the value cannot move between cycles the way a
           counter does. The history exists so somebody can see WHEN it changed, which an hourly grain
           answers, and a 1-minute grain would pay 60x for the same answer. Retained a year because "when
           did plan capture get turned on" is a question asked months later. */
        ["pg_plan_capture_readiness"] = new(60, 365),
        /* #2544 write side - checkpoints, background writer, WAL. PER-MINUTE and 30 days, matching
           pg_io_stats rather than the hourly readiness collector above, because these are cumulative
           COUNTERS: the value a reader wants is a rate, and a rate is only as fine-grained as the sampling
           interval that produced it. An hourly grain would smear a five-minute burst of requested
           checkpoints into nothing, which is the exact event this collects to catch. Affordable at that
           cadence for the same reason pg_io_stats is - all three source views are cluster-wide singletons,
           so a snapshot is ONE row, not one per relation. */
        ["pg_write_stats"] = new(1, 30),
        /* #2545 extension availability. DAILY, and retained a year. An extension appearing or being upgraded
           is a rare, deliberate act - nobody installs one twice an hour - so a per-minute cadence would pay
           1440x for the same answer. The year of retention is the point of keeping history at all: "when did
           pg_stat_statements get installed" and "when did this extension get upgraded" are asked months
           later, usually right after a plan changed shape and nobody can explain why. */
        ["pg_extension_availability"] = new(1440, 365),
        /* #2544 lock state. PER-MINUTE and 30 days, matching pg_blocking - this is a SAMPLE of instantaneous
           state, not a counter, so the cadence IS the resolution. A lock queue that forms and clears inside
           five minutes is the interesting one, and an hourly grain would miss it entirely while reporting
           the server as quiet. Row count is bounded by CONTENTION rather than by concurrency, because the
           snapshot aggregates by (database, locktype, mode, granted, relation) - an idle server produces a
           handful of rows. */
        ["pg_lock_stats"] = new(1, 30),
        /* #2543 column statistics. DAILY and a year, because these change only when ANALYZE runs - which is
           autovacuum's cadence, not a minute's - and the question they answer is asked retrospectively:
           "n_distinct on this column moved on the day the plan changed" needs a year of history and gains
           nothing from a finer grain. This is also the widest per-row fan-out here (columns x tables x
           databases), which the 128-page floor in the query bounds. */
        ["pg_column_stats"] = new(1440, 365),
        /* #2544 replication connections. PER-MINUTE and 30 days, matching the slot collector it sits beside.
           This is instantaneous state, so the cadence IS the resolution - a standby that drifts away and
           catches up inside five minutes is a different and more worrying animal than one steadily behind,
           and only a fine grain tells them apart. Row count is the number of connected standbys, so single
           digits on any real topology. */
        ["pg_replication_stats"] = new(1, 30),
        /* #2544 buffer pool contents. HOURLY, for two reasons that point the same way. The full
           pg_buffercache view is a scan of every buffer - measured at 6.1 ms for a 512 MB pool, which scales
           linearly to roughly 780 ms at 64 GB of shared_buffers - so it is affordable hourly and not per
           minute. And a server WITHOUT the pg_buffercache extension records an ObjectMissing outcome every
           cycle, which at a minute grain would be thousands of rows a day of noise on a fleet that mostly
           lacks it. What is resident in the pool is slow-moving enough that an hour is the right resolution
           anyway. */
        ["pg_buffer_usage"] = new(60, 30),
    };
}
