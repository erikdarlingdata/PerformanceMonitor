/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// #3896: stamps <see cref="AnalysisContext.LatestValueStarts"/> for a Lite pass — the lower bound of every
/// latest-value read, from the EFFECTIVE cadence of the collector feeding it: a day, or twice the interval
/// when the collector is scheduled slower than twice a day (<see cref="AnalysisContext.LatestValueLookbackFor"/>);
/// the newest capture at or before the window's end when it runs on load only. Darling's
/// <c>PgLatestValueBounds</c>, over the schedule Lite keeps (<see cref="ScheduleManager"/>, reached through the
/// resolver <see cref="AnalysisService"/> is built with) instead of a store table.
///
/// <para>Shared by <see cref="DuckDbFactCollector"/>, which stamps right after the coverage witness, and
/// <see cref="DrillDownCollector"/>, which lists the same rows and stamps only when handed a context the fact
/// collector never saw. No resolver — a test, or a host that has no schedule to offer — is every collector
/// at its shipped default, which is the bound every read took before cadences were consulted.</para>
/// </summary>
internal static class LatestValueBounds
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

    /// <summary>An on-load collector's newest capture at or before the window's end, hot or archived ($1
    /// server_id, $2 window end). The table name is a catalog constant, never input.</summary>
    internal static string NewestCaptureSql(string table) => $@"
SELECT MAX(collection_time)
FROM v_{table}
WHERE server_id = $1
AND   collection_time <= $2";

    /// <summary>
    /// Stamps the bounds once per context. <paramref name="frequencyMinutes"/> answers (server id, collector)
    /// with the cadence the collector runs at on that server, or null when it does not know; a resolver that
    /// throws costs the pass nothing but the override. An abandonment is NOT swallowed (#2443).
    /// </summary>
    internal static async Task EnsureAsync(
        DuckDbInitializer duckDb, Func<int, string, int?>? frequencyMinutes, AnalysisContext context)
    {
        if (context.LatestValueStarts is not null) return;

        var starts = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        foreach (var collector in Collectors)
        {
            int? configured = null;
            try
            {
                configured = frequencyMinutes?.Invoke(context.ServerId, collector.Name);
            }
            catch (Exception ex)
            {
                AppLogger.Warn("LatestValueBounds",
                    $"Could not resolve the {collector.Name} schedule for {context.ServerName}; its latest-value reads use the default cadence this pass: {ex.Message}");
            }

            /* Lite's schedule is already one effective list per server, so it enters as the per-server level;
               the shared rule still refuses a cadence the scheduler could not honour. */
            var frequency = CollectorScheduleDefaults.ResolveFrequencyMinutes(collector.Name, configured, fleetOverride: null);

            starts[collector.Name] = AnalysisContext.LatestValueLookbackFor(frequency) is TimeSpan lookback
                ? context.TimeRangeEnd - lookback
                : await NewestCaptureAsync(duckDb, collector.TargetTable, context)
                  ?? context.TimeRangeEnd - AnalysisContext.LatestValueLookback;
        }

        context.LatestValueStarts = starts;
    }

    /// <summary>
    /// An on-load collector's newest capture, or null when it has none at or before the window's end — the read
    /// then finds nothing whatever its bound, so the caller's fallback is only a well-formed value.
    /// </summary>
    private static async Task<DateTime?> NewestCaptureAsync(DuckDbInitializer duckDb, string table, AnalysisContext context)
    {
        try
        {
            using var readLock = duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = NewestCaptureSql(table);
            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            var newest = await cmd.ExecuteScalarAsync(context.CancellationToken);
            return newest is DateTime at ? at : null;
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            AppLogger.Warn("LatestValueBounds",
                $"Could not read the newest {table} capture for {context.ServerName}; its latest-value reads fall back to the default lookback this pass: {ex.Message}");
            return null;
        }
    }
}
