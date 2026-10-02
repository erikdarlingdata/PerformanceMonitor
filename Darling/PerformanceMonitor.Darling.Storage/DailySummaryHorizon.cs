/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The retention facts the daily-summary readers and the purge share — the two log horizons, the
/// baseline-serving floor set, and the fleet-retention rule (#3653). One home for the numbers, in Storage,
/// because the service's purge (<c>DarlingRetention</c>), the MCP health reader (<c>DarlingHealthReader</c>)
/// and the desktop viewer's Performance Calendar (<c>ViewerDataService.DailySummary</c>) all need them, and
/// the viewer cannot see the service. The service keeps its original names as aliases of these, so the purge's
/// horizon and the horizon a calendar cell is judged against are the same constant read twice, not two
/// literals that happen to agree.
/// </summary>
public static class DarlingRetentionHorizons
{
    /// <summary>
    /// The base metric-data retention window, in days — the horizon every collector's
    /// <see cref="CollectorScheduleDefaults"/> retention defaults to on an untouched store, and the unit the
    /// collection log's horizon is a multiple of. Thirty days: enough for a month-over-month comparison, short
    /// enough that a 42-server fleet's raw hypertables stay in the tens of GB.
    /// </summary>
    public const int DataRetentionBaseDays = 30;

    /// <summary>
    /// <c>collection_log</c> is not a collector, so it has no <see cref="CollectorScheduleDefaults"/> entry to
    /// carry its horizon. It is kept at 2x the base window (mirrors the Dashboard's <c>retention_date x2</c>
    /// rule) so a collector run-record survives long enough to diagnose WHY a collector failed AFTER its metric
    /// rows have aged out — a 30-day metric row and its failure log would otherwise expire together, erasing the
    /// evidence. Effectively 60 days. This is also, with the alert log, exactly what lets a daily-summary spine
    /// row outlive its signals (#3541 A9): the day is still named by its run record after every count the band
    /// reads has been purged.
    /// </summary>
    public const int CollectionLogRetentionDays = DataRetentionBaseDays * 2;

    /// <summary>
    /// The earliest instant a read of <c>collection_log</c> with no window can still see at
    /// <paramref name="nowUtc"/>: <see cref="CollectionLogRetentionDays"/> back, as naive UTC (#3967). The purge
    /// never removes a row newer than this. It drops whole chunks whose every row is older, or deletes rows older
    /// than its own run's cutoff, which is earlier still, so some older rows can survive and none newer are
    /// lost. It is the <c>searchedFromUtc</c> every surface that reads a server's newest collection with no
    /// window hands <c>ServerHealthClassifier.ClassifyFreshness</c>, so a server whose whole history retention
    /// has dropped reads Offline rather than "Awaiting first collection".
    /// </summary>
    public static DateTime CollectionLogHorizon(DateTime nowUtc) =>
        DateTime.SpecifyKind(nowUtc.AddDays(-CollectionLogRetentionDays), DateTimeKind.Unspecified);

    /// <summary>
    /// <c>config_alert_log</c> (the fired-alert history) is a plain registry table — not a collector, so no
    /// schedule horizon; not a hypertable, so a batched DELETE purge. Kept 90 days (a quarter): alert history is
    /// low-volume and a valuable audit trail, so the horizon is generous, but it is BOUNDED. No operator setting
    /// governs this today, so this constant is the single source of truth.
    /// </summary>
    public const int AlertHistoryRetentionDays = 90;

    /// <summary>
    /// #1743 follow-up: the raw collectors whose hypertables serve baselines DIRECTLY (their retired sum/sumsq
    /// rollups could not produce a median). Their effective purge horizon is floored at the baseline window
    /// regardless of the user-editable schedule — the product-controlled insulation the rollups' fixed retention
    /// used to provide. BaselineSupplyTests pins membership against the provider's raw-reading arms.
    ///
    /// <para>#3691 (the #1757 shape, PostgreSQL edition): the four <c>pg_*</c> raw hypertables
    /// <c>PgTargetBaselineProvider</c> reads DIRECTLY for the PostgreSQL-target baselines — <c>pg_database_stats</c>
    /// (tps, deadlock rate), <c>pg_session_states</c> (session count), <c>pg_wait_stats</c> (wait ms/s) and
    /// <c>pg_cpu_utilization</c> (percent of the capacity ceiling) — join the set. They have no rollup at all, so
    /// their 30-day schedule default was the ONLY thing covering the 30-day baseline window, and that default is
    /// user-editable: an operator shortening PostgreSQL retention to 7 d would have starved every PostgreSQL
    /// anomaly detector (<c>PgTargetAnomalyDetector.HasBaselineDataSql</c> asks about the 30 days before the
    /// window; the provider binds <c>analysisTime − BaselineWindowDays</c>) without a word said. Same mechanism,
    /// same constant: the purge floors these at <c>BaselineMath.BaselineWindowDays</c>, the detector's own
    /// minimum-history gate. The set is engine-agnostic on purpose — a store purges its shared tables once for
    /// the whole fleet, so the floor cannot depend on which engine a given server is. A v2 lane's table joins the
    /// set the day its baseline arm reads it directly: <c>pg_replication_stats</c> (lane 12's replay-lag point
    /// series, added by that lane), and — the #3691 between-waves batch, because lanes 11 and 15 reported theirs
    /// as out of their files — <c>pg_io_stats</c> (lane 11's <c>pg_io_read_latency</c>) and <c>pg_write_stats</c>
    /// (lane 15's <c>pg_wal_bytes_per_sec</c>). Until then a 7-day PostgreSQL retention would have starved
    /// <c>ANOMALY_PG_IO_LATENCY</c> and <c>ANOMALY_PG_WAL_VOLUME</c> the same silent way. <c>BaselineSupplyTests</c>
    /// now DERIVES the expected membership from the provider's own query text (every <c>FROM pg_*</c> the arms
    /// name), so the next arm without a floor fails there rather than starving quietly. Lane 17 (wave 3) added
    /// <c>pg_blocking</c> — the COLLECTOR name, which is what the purge resolves by, for the table
    /// <c>pg_blocking_edges</c> its <c>pg_blocked_sessions</c> arm reads; the one member whose schedule name and
    /// table name differ, so the test maps the derived table back to its collector through the catalog. Lane 24
    /// added <c>pg_wait_sampling</c>, read directly by the stock <c>pg_sampled_wait_ms_per_sec</c> arm (its
    /// <c>sampled_ms</c> denominator, V133) — the same silent-starvation shape for <c>ANOMALY_PG_SAMPLED_WAIT_PROFILE</c>
    /// without it. Lane 27 (v3) added <c>pg_statement_stats</c>, read directly by the server-wide
    /// <c>pg_statement_mean_ms</c> arm (Σ stored exec-time deltas over Σ stored call deltas per collection) — the
    /// family's heaviest read and the one whose 30-day default retention an operator is likeliest to shorten, which
    /// would have starved <c>ANOMALY_PG_PLAN_REGRESSION</c> the same silent way. Lane 28 (v3) added <c>pg_kernel_stats</c>, read directly by the
    /// <c>pg_cpu_burn_cores</c> arm (the per-collection cores-busy series behind <c>ANOMALY_PG_CPU_BURN</c>); its 30-day
    /// schedule default was, again, the only thing covering the baseline window. Lane 38 added
    /// <c>pg_database_size_stats</c>, read directly by the <c>pg_database_growth_bytes_per_day</c> arm (the instance
    /// total's per-collection growth behind <c>ANOMALY_PG_DATABASE_GROWTH</c>); its schedule default is a year, so the
    /// floor is a no-op today and exists so a shortened retention cannot starve the detector tomorrow.</para>
    /// </summary>
    public static readonly IReadOnlySet<string> BaselineServingRawCollectors =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "cpu_utilization", "file_io_stats",
            "pg_database_stats", "pg_session_states", "pg_wait_stats", "pg_cpu_utilization",
            "pg_replication_stats", "pg_io_stats", "pg_write_stats",
            "pg_blocking",
            "pg_wait_sampling",
            "pg_statement_stats",
            "pg_kernel_stats",
            "pg_database_size_stats",
        };

    /// <summary>
    /// The effective FLEET-WIDE retention for a collector: the fleet override's <c>retention_days</c> when it
    /// is set and valid (at least one day — a retention of 0 would invert the purge cutoff and wipe the table,
    /// so it is treated as "no override", the same defense-in-depth the V17 CHECK constraint and the viewer's
    /// schedule editor apply), else the collector's <see cref="CollectorScheduleDefaults"/> default. A
    /// per-server override cannot apply to a shared-table purge, which is why only the fleet row's value is an
    /// input. Pure. The service's <c>StoreConfigProvider.ResolveFleetRetentionDays</c> delegates here after
    /// locating the fleet row, so the purge and the daily-summary horizon apply ONE rule.
    /// </summary>
    public static int ResolveFleetRetentionDays(string collectorName, int? fleetOverrideDays)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(collectorName);
        return fleetOverrideDays is int days && days >= 1
            ? days
            : CollectorScheduleDefaults.All[collectorName].RetentionDays;
    }
}

/// <summary>
/// The retention horizon the daily-summary aggregate's rows are judged against (#3541 A9), computed ONCE here
/// for the MCP health reader and the viewer's Performance Calendar (#3653).
///
/// <para><b>The defect this closes on the viewer.</b> The aggregate's day spine is a UNION over nine sources
/// that age out at different horizons: the seven per-signal collector tables at their 30-day default, the
/// collection log at twice that, the alert log at ninety. For every day between the shortest horizon and the
/// longest the spine still has the day while every signal the band reads has been purged, and the aggregate's
/// COALESCEs render each as a measured zero. #3641 taught the MCP reader to judge such a day
/// (<c>DailySummaryRetention.StateFor</c>: collected / purged / past_horizon / no_run_record) against the
/// store's horizon; the viewer's calendar read the same SQL and kept banding those days Healthy, because it
/// never computed the horizon — the computation lived in the service assembly the viewer does not reference.
/// This file is that computation, moved down so both consumers call it.</para>
///
/// <para>The horizon's DATE arithmetic (<c>DailySummaryRetention.HorizonFor</c>) stays in Common with the state
/// machine that consumes it; this file supplies the days it counts back, which is the part that needs the
/// store (fleet overrides) and the retention constants.</para>
/// </summary>
public static class DailySummaryHorizon
{
    /// <summary>
    /// The collectors whose tables the daily aggregate reads as SIGNALS, by their schedule names — the sources
    /// whose retention decides the horizon. The collection log and the alert log are the spine's other two
    /// members; they are constants on <see cref="DarlingRetentionHorizons"/> and are folded in by
    /// <see cref="ShortestSignalRetentionDays(Func{string, int}, int)"/>. <c>query_stats</c> is included even
    /// though old windows route its CTE to a rollup with its own longer retention: the horizon is a floor over
    /// EVERY signal, and the rollup keeps only the query count, not the band's inputs.
    /// </summary>
    public static readonly string[] SignalCollectors =
    {
        "wait_stats", "query_stats", "deadlocks", "blocked_process_report", "dmv_blocking_snapshot",
        "cpu_utilization", "memory_pressure_events",
    };

    /// <summary>
    /// The FLEET-WIDE retention overrides (<c>server_id</c> NULL) for the signal collectors — the same rows the
    /// purge's resolver layers over <see cref="CollectorScheduleDefaults"/>, so the horizon the readers publish
    /// is the horizon the purge actually enforces rather than the shipped default. A per-server override cannot
    /// apply to a shared-table purge, which is why only fleet rows are read. $1 the collector names.
    /// </summary>
    public const string FleetRetentionOverridesSql = """
        SELECT collector_name, retention_days
        FROM config_collector_schedules
        WHERE server_id IS NULL
        AND   retention_days IS NOT NULL
        AND   collector_name = ANY($1)
        """;

    /// <summary>
    /// Runs <see cref="FleetRetentionOverridesSql"/> for <see cref="SignalCollectors"/>: collector name → the
    /// fleet row's <c>retention_days</c>, for the collectors that have one (empty on an untouched store). Values
    /// come back RAW; <see cref="DarlingRetentionHorizons.ResolveFleetRetentionDays"/> validates on resolve.
    /// </summary>
    public static async Task<IReadOnlyDictionary<string, int>> ReadFleetRetentionOverrideDaysAsync(
        NpgsqlDataSource dataSource, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(dataSource);

        var overrides = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        await using var command = dataSource.CreateCommand(FleetRetentionOverridesSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = SignalCollectors });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            overrides[reader.GetString(0)] = reader.GetInt32(1);
        }

        return overrides;
    }

    /// <summary>
    /// The shortest effective retention among the daily aggregate's sources, in days — the number the horizon is
    /// measured back from. Pure: <paramref name="fleetRetentionDaysFor"/> resolves one collector's effective
    /// fleet retention (the service passes its purge resolver; the viewer passes the dictionary overload), and
    /// <paramref name="baselineFloorDays"/> is the baseline window the purge floors the
    /// <see cref="DarlingRetentionHorizons.BaselineServingRawCollectors"/> at (<c>BaselineMath.BaselineWindowDays</c>,
    /// passed in because that constant lives in the Analysis assembly this one does not reference).
    ///
    /// <para>The collection log and the alert log are folded in at their constants; on a default store they are
    /// the LONGER horizons (60 and 90 days), which is exactly why a spine row can outlive its signals and why the
    /// shortest one is the horizon. Never below one day: <c>HorizonFor</c> rejects less.</para>
    /// </summary>
    public static int ShortestSignalRetentionDays(Func<string, int> fleetRetentionDaysFor, int baselineFloorDays)
    {
        ArgumentNullException.ThrowIfNull(fleetRetentionDaysFor);

        var shortest = Math.Min(DarlingRetentionHorizons.CollectionLogRetentionDays, DarlingRetentionHorizons.AlertHistoryRetentionDays);
        foreach (var collector in SignalCollectors)
        {
            var days = fleetRetentionDaysFor(collector);
            if (DarlingRetentionHorizons.BaselineServingRawCollectors.Contains(collector))
            {
                days = Math.Max(days, baselineFloorDays);
            }

            shortest = Math.Min(shortest, days);
        }

        return Math.Max(1, shortest);
    }

    /// <summary>
    /// <see cref="ShortestSignalRetentionDays(Func{string, int}, int)"/> over the dictionary
    /// <see cref="ReadFleetRetentionOverrideDaysAsync"/> returns — the viewer's form, resolving each collector
    /// through <see cref="DarlingRetentionHorizons.ResolveFleetRetentionDays"/>, the rule the purge applies.
    /// </summary>
    public static int ShortestSignalRetentionDays(IReadOnlyDictionary<string, int> fleetOverrideDays, int baselineFloorDays)
    {
        ArgumentNullException.ThrowIfNull(fleetOverrideDays);
        return ShortestSignalRetentionDays(
            collector => DarlingRetentionHorizons.ResolveFleetRetentionDays(
                collector, fleetOverrideDays.TryGetValue(collector, out var days) ? days : null),
            baselineFloorDays);
    }
}
