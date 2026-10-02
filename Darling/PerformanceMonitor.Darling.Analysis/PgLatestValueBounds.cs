/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// #3896: stamps <see cref="AnalysisContext.LatestValueStarts"/> for a SQL Server-target pass — the lower
/// bound of every latest-value read, from the EFFECTIVE cadence of the collector feeding it. A day, or twice
/// the interval when an operator has scheduled the collector slower than twice a day
/// (<see cref="AnalysisContext.LatestValueLookbackFor"/>); the newest capture at or before the window's end
/// when it runs on load only.
///
/// <para>The cadence comes from the rows the scheduler reads (<c>config_collector_schedules</c>, per-server
/// over fleet-wide) through <see cref="CollectorScheduleDefaults.ResolveFrequencyMinutes"/>, the rule
/// <c>StoreConfigProvider.ResolveSchedule</c> runs the collector by — so the bound is the cadence the
/// collector is actually on, not the shipped default. The bounds are resolved here, before any read, and
/// bound as plain timestamps: an interval computed inside the statement would cost TimescaleDB its plan-time
/// chunk exclusion.</para>
///
/// <para>Shared by <see cref="PgFactCollector"/>, which stamps right after the coverage witness, and
/// <see cref="PgDrillDownCollector"/>, which lists the same rows and stamps only when handed a context the
/// fact collector never saw — the same context carries one set of bounds to both, so a fact and its
/// drill-down cannot disagree on which rows are current.</para>
/// </summary>
internal static class PgLatestValueBounds
{
    /// <summary>The collectors whose newest samples the latest-value reads take, by the key the reads use.</summary>
    internal static readonly IReadOnlyList<ICollectorSchemaInfo> Collectors = new ICollectorSchemaInfo[]
    {
        FileIoStatsCollector.Instance,
        DatabaseSizeStatsCollector.Instance,
        MemoryClerksCollector.Instance,
        PlanCacheStatsCollector.Instance,
        MemoryStatsCollector.Instance,
    };

    /// <summary>This server's schedule overrides: its own rows and the fleet-wide ones, the two levels the
    /// scheduler layers. The table is sparse — an absent row is the default — so this is usually empty.</summary>
    internal const string ScheduleOverridesSql = @"
SELECT server_id, collector_name, frequency_minutes
FROM config_collector_schedules
WHERE server_id = $1
OR    server_id IS NULL";

    /// <summary>An on-load collector's newest capture at or before the window's end ($1 server_id, $2 window
    /// end). The table name is a catalog constant, never input.</summary>
    internal static string NewestCaptureSql(string table) => $@"
SELECT MAX(collection_time)
FROM {table}
WHERE server_id = $1
AND   collection_time <= $2";

    /// <summary>The per-command deadline for these reads — the fact collector's, for its reason.</summary>
    private const int CommandTimeoutSeconds = PgFactCollector.FactCommandTimeoutSeconds;

    /// <summary>
    /// Stamps the bounds once per context. A failure to read the overrides costs the pass nothing but the
    /// overrides: every collector falls back to its shipped default, which is the bound every read took
    /// before cadences were consulted. An abandonment is NOT swallowed (#2443).
    /// </summary>
    internal static async Task EnsureAsync(NpgsqlDataSource postgres, AnalysisContext context, ILogger? logger)
    {
        if (context.LatestValueStarts is not null) return;

        var perServer = new Dictionary<string, int?>(StringComparer.OrdinalIgnoreCase);
        var fleet = new Dictionary<string, int?>(StringComparer.OrdinalIgnoreCase);
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = new NpgsqlCommand(ScheduleOverridesSql, connection) { CommandTimeout = CommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                int? frequency = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                (reader.IsDBNull(0) ? fleet : perServer)[reader.GetString(1)] = frequency;
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            perServer.Clear();
            fleet.Clear();
            logger?.LogWarning(
                "[PgLatestValueBounds] Could not read the collector schedule overrides for server {ServerId} ({ServerName}); this pass bounds its latest-value reads by the default cadences: {Message}",
                context.ServerId, context.ServerName, ex.Message);
        }

        var starts = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        foreach (var collector in Collectors)
        {
            var frequency = CollectorScheduleDefaults.ResolveFrequencyMinutes(
                collector.Name,
                perServer.GetValueOrDefault(collector.Name),
                fleet.GetValueOrDefault(collector.Name));

            starts[collector.Name] = AnalysisContext.LatestValueLookbackFor(frequency) is TimeSpan lookback
                ? context.TimeRangeEnd - lookback
                : await NewestCaptureAsync(postgres, collector.TargetTable, context, logger)
                  ?? context.TimeRangeEnd - AnalysisContext.LatestValueLookback;
        }

        /* #3929: trace_flags is on-load by default (frequency 0) but can't take the newest-capture anchor the
           OTHER on-load config facts use (server_config, database_config, server_properties, none of which are
           in Collectors above either — they read their own newest capture ad hoc). A capture that finds every
           flag off writes ZERO rows, so MAX(capture_time) would silently fall back to an older capture that
           still had one on — exactly the case that matters. Bounding the read to the on-load-aware lookback
           instead means a flag missing from the whole window (about two OnLoadRecaptureMinutes cycles once
           #3930's daily recapture is in effect) has no row there at all, on-load or not, so this entry always
           takes the interval branch (EffectiveRecurringIntervalMinutes is never 0) rather than the anchor one. */
        var traceFlagsFrequency = CollectorScheduleDefaults.ResolveFrequencyMinutes(
            "trace_flags", perServer.GetValueOrDefault("trace_flags"), fleet.GetValueOrDefault("trace_flags"));
        starts["trace_flags"] = context.TimeRangeEnd - AnalysisContext.LatestValueLookbackFor(
            CollectorScheduleDefaults.EffectiveRecurringIntervalMinutes(traceFlagsFrequency))!.Value;

        context.LatestValueStarts = starts;
    }

    /// <summary>
    /// An on-load collector's newest capture, or null when it has none at or before the window's end — the
    /// read then finds nothing whatever its bound, so the caller's fallback is only a well-formed value.
    /// </summary>
    private static async Task<DateTime?> NewestCaptureAsync(
        NpgsqlDataSource postgres, string table, AnalysisContext context, ILogger? logger)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(context.CancellationToken);
            using var cmd = new NpgsqlCommand(NewestCaptureSql(table), connection) { CommandTimeout = CommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(DateTime.SpecifyKind(context.TimeRangeEnd, DateTimeKind.Unspecified));

            var newest = await cmd.ExecuteScalarAsync(context.CancellationToken);
            return newest is DateTime at ? at : null;
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            logger?.LogWarning(
                "[PgLatestValueBounds] Could not read the newest {Table} capture for server {ServerId} ({ServerName}); its latest-value reads fall back to the default lookback this pass: {Message}",
                table, context.ServerId, context.ServerName, ex.Message);
            return null;
        }
    }
}
