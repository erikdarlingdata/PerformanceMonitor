using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Mcp;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitorDashboard.Analysis;

public partial class SqlServerDrillDownCollector
{
    private async Task CollectTopDeadlocks(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 3
    collection_time,
    event_date,
    spid,
    LEFT(CAST(query AS NVARCHAR(MAX)), 500) AS victim_sql,
    CAST(deadlock_graph AS NVARCHAR(MAX)) AS deadlock_graph
FROM collect.deadlocks
WHERE collection_time >= @startTime AND collection_time <= @endTime
ORDER BY collection_time DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* #1140: parse the involved objects from the graph for the dedup fingerprint + a readable
               Objects field. The raw graph XML is NOT surfaced (it would bloat the alert detail). */
            var objects = DeadlockObjectExtractor.FromGraphXml(reader.IsDBNull(4) ? null : reader.GetString(4));
            items.Add(new
            {
                time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                deadlock_time = reader.IsDBNull(1) ? "" : reader.GetDateTime(1).ToString("o"),
                victim = reader.IsDBNull(2) ? "" : reader.GetValue(2).ToString(),
                victim_sql = reader.IsDBNull(3) ? "" : reader.GetString(3),
                objects = string.Join(", ", objects)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["top_deadlocks"] = items;
    }

    private async Task CollectTopBlockingChains(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 5
    collection_time,
    database_name,
    spid AS blocked_spid,
    0 AS blocking_spid,
    wait_time_ms,
    lock_mode,
    LEFT(CAST(query_text AS NVARCHAR(MAX)), 500) AS blocked_sql,
    LEFT(blocking_tree, 500) AS blocking_sql,
    contentious_object
FROM collect.blocking_BlockedProcessReport
WHERE collection_time >= @startTime AND collection_time <= @endTime
ORDER BY wait_time_ms DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                time = reader.IsDBNull(0) ? "" : reader.GetDateTime(0).ToString("o"),
                database = reader.IsDBNull(1) ? "" : reader.GetString(1),
                blocked_spid = reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                blocking_spid = reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                wait_time_ms = reader.IsDBNull(4) ? 0L : Convert.ToInt64(reader.GetValue(4)),
                lock_mode = reader.IsDBNull(5) ? "" : reader.GetString(5),
                blocked_sql = reader.IsDBNull(6) ? "" : reader.GetString(6),
                blocking_sql = reader.IsDBNull(7) ? "" : reader.GetString(7),
                contentious_object = reader.IsDBNull(8) ? "" : reader.GetString(8)
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["top_blocking_chains"] = items;
    }

    /// <summary>
    /// Reconstructs blocking chains (same logic as the collector) and surfaces the top 3
    /// by magnitude — apex, depth, victim count, and the level-by-level structure that
    /// the flat top_blocking_chains list cannot show.
    /// </summary>
    private async Task CollectReconstructedBlockingChains(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        // Shared query/filter — see BlockingPairRowQuery (keeps the drill-down, the BLOCKING_CHAIN fact
        // collector, and the viewer fetch in lockstep on the apex-determining blocking_spid filter).
        cmd.CommandText = BlockingPairRowQuery.Sql;
        BlockingPairRowQuery.AddParameters(cmd, context.TimeRangeStart, context.TimeRangeEnd);

        var rows = new List<BlockingPairRow>();
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                rows.Add(BlockingPairRowQuery.Read(reader));
        }

        if (rows.Count == 0) return;

        var reconstruction = BlockingChainReconstructor.Reconstruct(
            rows, maxDepth: 50, maxPairs: 5000, stepBudget: 100_000, scopeByMonitorLoop: false);

        var items = new List<object>();
        foreach (var chain in reconstruction.Chains.Take(3))
        {
            items.Add(new
            {
                apex_spid = chain.ApexSpid,
                apex_sleeping = chain.ApexSleeping,
                depth = chain.Depth,
                // Distinct sessions blocked under this apex over the window — cumulative, not peak-concurrent.
                victim_count = chain.VictimCount,
                max_wait_ms = chain.MaxWaitMs,
                levels = chain.Levels.Select(l => new
                {
                    level = l.Level,
                    blocking_spid = l.BlockingSpid,
                    blocked_spid = l.BlockedSpid,
                    lock_mode = l.LockMode,
                    wait_time_ms = l.WaitTimeMs,
                    blocking_sql = l.BlockingSqlText,
                    blocked_sql = l.BlockedSqlText
                }).ToList()
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["reconstructed_blocking_chains"] = items;
    }

    private async Task CollectLockModeBreakdown(AnalysisFinding finding, AnalysisContext context)
    {
        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT TOP 10
    wait_type,
    CAST(SUM(wait_time_ms_delta) AS BIGINT) AS total_wait_ms,
    CAST(SUM(waiting_tasks_count_delta) AS BIGINT) AS total_count
FROM collect.wait_stats
WHERE collection_time >= @startTime AND collection_time <= @endTime
AND   wait_type LIKE 'LCK%'
AND   wait_time_ms_delta > 0
GROUP BY wait_type
ORDER BY CAST(SUM(wait_time_ms_delta) AS BIGINT) DESC;";

        cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
        cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

        var items = new List<object>();
        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new
            {
                lock_type = reader.IsDBNull(0) ? "" : reader.GetString(0),
                total_wait_ms = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                waiting_tasks = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2))
            });
        }

        if (items.Count > 0)
            finding.DrillDown!["lock_mode_breakdown"] = items;
    }
}
