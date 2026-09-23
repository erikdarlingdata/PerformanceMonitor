using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitorDashboard.Helpers;

namespace PerformanceMonitorDashboard.Analysis;

/// <summary>
/// #3896, the frozen Dashboard's mirror of Darling's <c>PgLatestValueBounds</c>: stamps
/// <see cref="AnalysisContext.LatestValueStarts"/> from <c>config.collection_schedule</c> — a day, or twice
/// the collector's interval when an operator scheduled it slower than twice a day
/// (<see cref="AnalysisContext.LatestValueLookbackFor"/>). The Dashboard has no on-load collectors: frequency 0
/// there is "every master-collector tick", so it takes the floor. Shared by <see cref="SqlServerFactCollector"/>
/// and <see cref="SqlServerDrillDownCollector"/>, which lists the rows the autogrowth fact counts.
/// </summary>
internal static class SqlServerLatestValueBounds
{
    /// <summary>The keys the latest-value reads bind by — the same collector names Darling and Lite use.</summary>
    internal const string FileIoStats = "file_io_stats";
    internal const string DatabaseSizeStats = "database_size_stats";
    internal const string MemoryClerks = "memory_clerks";
    internal const string MemoryStats = "memory_stats";

    /// <summary>Each key's row in <c>config.collection_schedule</c>, which names its collectors by procedure.</summary>
    private static readonly IReadOnlyDictionary<string, string> s_scheduleNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        [FileIoStats] = "file_io_stats_collector",
        [DatabaseSizeStats] = "database_size_stats_collector",
        [MemoryClerks] = "memory_clerks_stats_collector",
        [MemoryStats] = "memory_stats_collector",
    };

    internal const string ScheduleSql = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    cs.collector_name,
    cs.frequency_minutes
FROM config.collection_schedule AS cs
WHERE cs.collector_name IN (N'file_io_stats_collector', N'database_size_stats_collector', N'memory_clerks_stats_collector', N'memory_stats_collector');";

    /// <summary>Stamps the bounds once per context; a schedule that cannot be read leaves every read at the floor.</summary>
    internal static async Task EnsureAsync(string connectionString, AnalysisContext context)
    {
        if (context.LatestValueStarts is not null) return;

        var frequencies = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        try
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = ScheduleSql;

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                frequencies[reader.GetString(0)] = reader.GetInt32(1);
            }
        }
        catch (Exception ex)
        {
            Logger.Error("SqlServerLatestValueBounds.EnsureAsync failed; latest-value reads use the one-day floor this pass", ex);
        }

        context.LatestValueStarts = Starts(context.TimeRangeEnd, frequencies);
    }

    /// <summary>
    /// The bounds for a window ending at <paramref name="end"/>, from <c>config.collection_schedule</c>'s
    /// <paramref name="frequencies"/> (keyed by schedule row name). A collector with no row takes the floor.
    /// </summary>
    internal static IReadOnlyDictionary<string, DateTime> Starts(DateTime end, IReadOnlyDictionary<string, int> frequencies)
    {
        var starts = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        foreach (var (key, scheduleName) in s_scheduleNames)
        {
            var frequency = frequencies.TryGetValue(scheduleName, out var minutes) ? Math.Max(minutes, 1) : 1;
            starts[key] = end - (AnalysisContext.LatestValueLookbackFor(frequency) ?? AnalysisContext.LatestValueLookback);
        }

        return starts;
    }
}
