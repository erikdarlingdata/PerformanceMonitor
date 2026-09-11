/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One starter custom-alert-rule template (#3285, plan Component 7 / #3282 Q2): a curated
/// {metric + predicate + hysteresis} an operator can browse (<c>list_custom_alert_templates</c>) and create a
/// real rule from (<c>create_custom_alert_rule</c>). <see cref="DefinitionJson"/> is a genuine
/// <see cref="CustomAlertRuleDefinition"/> body: CODE, so the drift-guard re-validates it against the LIVE
/// catalog on every build; a template naming a measure that later drifts out of the catalog fails the build
/// rather than shipping a rule that can never fire.</summary>
public sealed record CustomAlertTemplate(string Key, string Name, string Description, string DefinitionJson);

/// <summary>
/// The starter custom-alert templates. #3282's answer to "the collected-but-unalerted signals": templates over
/// new hardcoded evaluators, so the set generalizes to signals nobody has named yet. Each maps to a REAL
/// <see cref="MeasureCatalog"/> measure (source + measure|ratio + aggregate + unit), with a deliberately
/// conservative starter threshold + hysteresis the operator tunes; scope defaults to all servers.
///
/// <para>Thresholds are STARTERS, not tuned bars: they are set where a value is unambiguous trouble on most
/// fleets (a 30s+ block, a 40%+ signal wait), never at a value only a specific fleet's baseline could justify,
/// and are documented per template. Every one fires only after 3 consecutive breaching evaluations (2 for the
/// more urgent blocking signal) so a single spike does not page.</para>
///
/// <para><b>Deliberately not shipped: a PostgreSQL "storage growth" template.</b> #3282 names it, but the
/// catalog has no database-size GAUGE for PostgreSQL (<c>pg_database_stats</c> is transaction/block/temp
/// COUNTERS, not size), so there is nothing honest to threshold on. The related disk-fill risk IS covered by
/// the replication-slot-WAL-retention template (retained WAL is the PostgreSQL storage a stuck slot actually
/// grows without bound). SQL Server storage is already covered by the built-in low-disk / file-growth alerts.</para>
/// </summary>
public static class CustomAlertTemplates
{
    /// <summary>The curated set. Ordered PostgreSQL-first (the signals #3282 flagged as unalerted), then the
    /// obvious SQL Server ones.</summary>
    public static readonly IReadOnlyList<CustomAlertTemplate> All = new[]
    {
        /* ── PostgreSQL (the #3282 collected-but-unalerted signals) ── */

        new CustomAlertTemplate(
            "pg-autovacuum-dead-tuples",
            "PostgreSQL: dead tuples piling up",
            "Fires when a table's estimated dead-tuple count stays high, the tell that autovacuum is falling "
            + "behind the write rate (bloat and plan drift follow). Starter: warn at 1,000,000 dead tuples, "
            + "critical at 10,000,000: table-grain, so it is the worst table in the window. Tune to your "
            + "largest hot table; a small database should lower both.",
            "{\"metric\":{\"source\":\"pg_autovacuum_stats\",\"measure\":\"pg_av_dead_tuples\",\"aggregate\":\"max\",\"unit\":\"count\",\"hours\":1}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000000,\"criticalThreshold\":10000000}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}}"),

        new CustomAlertTemplate(
            "pg-replication-lag",
            "PostgreSQL: replica replay lag high",
            "Fires when a standby's replay lag stays high: the replica is falling behind the primary, so a "
            + "failover would lose more, and read replicas serve staler data. Starter: warn at 30s of replay "
            + "lag, critical at 5 minutes. Lower it if your RPO is tight.",
            "{\"metric\":{\"source\":\"pg_replication_stats\",\"measure\":\"pg_repl_replay_lag_ms\",\"aggregate\":\"max\",\"unit\":\"ms\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":30000,\"criticalThreshold\":300000}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}}"),

        new CustomAlertTemplate(
            "pg-connection-saturation",
            "PostgreSQL: connection count high",
            "Fires when the total session count stays high: approaching max_connections means new connections "
            + "start failing. Starter: warn at 200 sessions, critical at 500 (absolute, since the catalog does "
            + "not carry max_connections); set both to a fraction of your configured max_connections.",
            "{\"metric\":{\"source\":\"pg_session_states\",\"measure\":\"pg_sess_total_sessions\",\"aggregate\":\"max\",\"unit\":\"count\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":200,\"criticalThreshold\":500}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}}"),

        new CustomAlertTemplate(
            "pg-table-bloat",
            "PostgreSQL: table bloat high",
            "Fires when a table's estimated bloat percentage stays high: wasted space and slower scans that a "
            + "VACUUM FULL / pg_repack would reclaim. Starter: warn at 40% bloat, critical at 70%. Bloat is "
            + "slow-moving, so this evaluates over an hour.",
            "{\"metric\":{\"source\":\"pg_table_bloat_stats\",\"measure\":\"pg_tbl_bloat_pct\",\"aggregate\":\"max\",\"unit\":\"percent\",\"hours\":1}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":40,\"criticalThreshold\":70}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}}"),

        new CustomAlertTemplate(
            "pg-replication-slot-wal",
            "PostgreSQL: replication slot retaining WAL",
            "Fires when a replication slot's retained WAL stays large: a disconnected or slow consumer holds "
            + "WAL that cannot be recycled, and the disk fills for the WHOLE instance. This is the storage risk "
            + "#3282 flags (PostgreSQL has no database-size gauge to threshold directly). Starter: warn at ~10 GB "
            + "retained, critical at ~50 GB. Set below your free disk headroom.",
            "{\"metric\":{\"source\":\"pg_replication_slot_stats\",\"measure\":\"pg_slot_retained_wal_bytes\",\"aggregate\":\"max\",\"unit\":\"mb\",\"hours\":1}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":10240,\"criticalThreshold\":51200}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}}"),

        /* ── SQL Server (the obvious ones) ── */

        new CustomAlertTemplate(
            "sqlserver-signal-wait-pct",
            "SQL Server: high signal wait %",
            "Fires when signal wait % stays high: threads are ready but waiting for a scheduler, the classic "
            + "sign of CPU pressure / scheduler contention. Starter: warn at 25%, critical at 40% (the "
            + "widely-used rule-of-thumb bars).",
            "{\"metric\":{\"source\":\"wait_stats\",\"ratio\":\"signal_wait_pct\",\"unit\":\"percent\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":25,\"criticalThreshold\":40}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}}"),

        new CustomAlertTemplate(
            "sqlserver-sustained-blocking",
            "SQL Server: sustained blocking",
            "Fires when the longest blocked-process report in the window crosses a duration bar: a session was "
            + "blocked that long, not just momentary contention. Starter: warn at 30s blocked, critical at 2 "
            + "minutes. Fires after 2 breaching evaluations (blocking is more urgent than the slow-moving "
            + "signals). Requires the blocked-process report threshold to be configured on the instance.",
            "{\"metric\":{\"source\":\"blocked_process_reports\",\"measure\":\"bpr_wait_time_ms\",\"aggregate\":\"max\",\"unit\":\"ms\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":30000,\"criticalThreshold\":120000}," +
            "\"hysteresis\":{\"breachSamples\":2,\"clearSamples\":2}}"),

        new CustomAlertTemplate(
            "sqlserver-long-running-query",
            "SQL Server: long-running query",
            "Fires when the average query duration per execution stays high: a query (or plan regression) that "
            + "is consistently slow, not one that ran long once. Starter: warn at a 10s average, critical at 60s. "
            + "Tune to your workload's normal query time.",
            "{\"metric\":{\"source\":\"query_stats\",\"ratio\":\"query_avg_elapsed_us\",\"unit\":\"ms\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":10000,\"criticalThreshold\":60000}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}}"),

        new CustomAlertTemplate(
            "sqlserver-tempdb-pressure",
            "SQL Server: tempdb space growth",
            "Fires when total tempdb reserved space stays high: spills, version store, or a runaway object "
            + "eating tempdb, which can stall the whole instance if it runs out. Starter: warn at ~50 GB "
            + "reserved, critical at ~100 GB (absolute); set both relative to your tempdb file sizes.",
            "{\"metric\":{\"source\":\"tempdb_stats\",\"measure\":\"tempdb_total_reserved_mb\",\"aggregate\":\"max\",\"unit\":\"mb\",\"hours\":0.25}," +
            "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":51200,\"criticalThreshold\":102400}," +
            "\"hysteresis\":{\"breachSamples\":3,\"clearSamples\":2}}"),
    };
}
