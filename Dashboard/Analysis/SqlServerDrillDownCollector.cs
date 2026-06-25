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

/// <summary>
/// Enriches findings with drill-down data from SQL Server.
/// Runs after graph traversal, only for findings above severity threshold.
/// Each drill-down query is limited to top N results with truncated text.
///
/// This makes analyze_server self-sufficient -- instead of returning a list
/// of "next tools to call," findings include the actual supporting data.
///
/// Port of Lite's DrillDownCollector -- uses SQL Server collect.* tables instead of DuckDB views.
/// No server_id filtering -- Dashboard monitors one server per database.
/// </summary>
public partial class SqlServerDrillDownCollector
{
    private readonly string _connectionString;
    private readonly IPlanFetcher? _planFetcher;
    private const int TextLimit = 500;

    public SqlServerDrillDownCollector(string connectionString, IPlanFetcher? planFetcher = null)
    {
        _connectionString = connectionString;
        _planFetcher = planFetcher;
    }

    /// <summary>
    /// Enriches each finding's DrillDown dictionary based on its story path.
    /// </summary>
    public async Task EnrichFindingsAsync(List<AnalysisFinding> findings, AnalysisContext context)
    {
        foreach (var finding in findings)
        {
            try
            {
                finding.DrillDown = new Dictionary<string, object>();
                var pathKeys = finding.StoryPath.Split(" → ", StringSplitOptions.RemoveEmptyEntries).ToHashSet();

                /* D7: the config drill-down is a single cheap config-table read and is
                   required to build config/RCSI/db-config Apply actions, which legitimately
                   score below 0.5 (RCSI-off base severity is 0.3). Collect it regardless of
                   the 0.5 display gate. */
                if (pathKeys.Contains("DB_CONFIG"))
                    await CollectConfigIssues(finding, context);

                /* WS3: the percent-autogrowth drill-down is a single cheap config-table read
                   and is required to render the copy-paste MODIFY FILE fix for a
                   FILE_AUTOGROWTH_PERCENT finding, which scores 0.3 (advisory). Collect it
                   regardless of the 0.5 display gate, like the config drill-down above. */
                if (pathKeys.Contains("FILE_AUTOGROWTH_PERCENT"))
                    await CollectAutogrowthPercentFiles(finding, context);

                /* WS3: the server-config drill-down is a single cheap config-table read and is
                   required to build the SERVER_CONFIG Apply/copy-paste action for a CONFIG_*
                   finding, which scores 0.4 (advisory). Collect it regardless of the 0.5 display
                   gate, like the two config drill-downs above. Only the bad setting that rooted
                   THIS finding is emitted (the engine roots one CONFIG_* fact per finding). */
                if (pathKeys.Contains("CONFIG_MAXDOP") || pathKeys.Contains("CONFIG_CTFP")
                    || pathKeys.Contains("CONFIG_MAX_MEMORY_MB") || pathKeys.Contains("CONFIG_MIN_MAX_MEMORY_NARROW"))
                    await CollectServerConfig(finding, context, pathKeys);

                /* WS4: re-parse the top collected query plans to render the specific missing
                   indexes / warnings for a MISSING_INDEX or PLAN_WARNING finding, which scores 0.4
                   (advisory). Run regardless of the 0.5 display gate, like the config drill-downs
                   above — the fact only carries counts (numeric metadata), so the strings live here. */
                if (pathKeys.Contains("MISSING_INDEX") || pathKeys.Contains("PLAN_WARNING"))
                    await CollectPlanAdvisoryDetail(finding, context, pathKeys);

                // Below the 0.5 display gate, only the cheap config drill-down above runs;
                // the expensive collectors (plan fetches, multi-row reads) are skipped.
                if (finding.Severity < 0.5)
                {
                    if (finding.DrillDown.Count == 0)
                        finding.DrillDown = null;
                    continue;
                }

                if (pathKeys.Contains("DEADLOCKS"))
                    await CollectTopDeadlocks(finding, context);

                if (pathKeys.Contains("BLOCKING_EVENTS"))
                    await CollectTopBlockingChains(finding, context);

                if (pathKeys.Contains("BLOCKING_CHAIN"))
                    await CollectReconstructedBlockingChains(finding, context);

                if (pathKeys.Contains("CPU_SPIKE"))
                    await CollectQueriesAtSpike(finding, context);

                if (pathKeys.Contains("CPU_SQL_PERCENT") || pathKeys.Contains("CPU_SPIKE"))
                {
                    await CollectTopCpuQueries(finding, context);
                    await CollectAbnormalCpuPlans(finding, context, pathKeys);
                }

                if (pathKeys.Contains("QUERY_SPILLS"))
                    await CollectTopSpillingQueries(finding, context);

                if (   pathKeys.Contains("IO_READ_LATENCY_MS")
                    || pathKeys.Contains("IO_WRITE_LATENCY_MS")
                    )
                    await CollectFileLatencyBreakdown(finding, context);

                if (   pathKeys.Contains("LCK")
                    || pathKeys.Contains("LCK_M_S")
                    || pathKeys.Contains("LCK_M_IS")
                    )
                    await CollectLockModeBreakdown(finding, context);

                if (pathKeys.Contains("TEMPDB_USAGE"))
                    await CollectTempDbBreakdown(finding, context);

                if (pathKeys.Contains("MEMORY_GRANT_PENDING"))
                    await CollectPendingGrants(finding, context);

                if (pathKeys.Any(k => k.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase)))
                    await CollectBadActorDetail(finding, context);

                if (pathKeys.Contains("PARAMETER_SENSITIVITY"))
                    await CollectParameterSensitiveQueries(finding, context);

                if (pathKeys.Contains("PLAN_REGRESSION"))
                    await CollectRegressedQueries(finding, context);

                // Plan analysis: for findings with top queries, analyze their cached plans
                await CollectPlanAnalysis(finding, context);

                // Remove empty drill-down dictionaries
                if (finding.DrillDown.Count == 0)
                    finding.DrillDown = null;
            }
            catch (Exception ex)
            {
                Logger.Error(
                    $"[SqlServerDrillDownCollector] Drill-down failed for {finding.StoryPath}: {ex.GetType().Name}: {ex.Message}\n{ex.StackTrace}");
                // Don't null out -- keep whatever was collected before the error
            }
        }
    }

    private sealed class ConfigIssueRow
    {
        public string Database = "";
        public string RecoveryModel = "";
        public bool AutoShrink;
        public bool AutoClose;
        public bool Rcsi;
        public string PageVerify = "";
        public bool QueryStore;
    }

    private sealed class RcsiInactionFigures
    {
        public int BlockingEvents;
        public int Deadlocks;
        public int? ReaderWriterPct;
    }

    /// <summary>
    /// Computes, per RCSI-off database, the §3.3 inaction-risk figures over the
    /// analysis window: blocking/deadlock counts from the PRE-AGGREGATED
    /// collect.blocking_deadlock_stats (O-P3-F — never a blocking_BlockedProcessReport
    /// row count), and the reader-vs-writer share from a SEPARATE pass over
    /// collect.blocking_BlockedProcessReport classified against the FULL lock_mode
    /// vocabulary (M-1). All reads are parameterized on the time window; database names
    /// are bound as a parameterized TVP-free IN list (literal-free).
    /// </summary>
    private static async Task<Dictionary<string, RcsiInactionFigures>> CollectRcsiInactionFigures(
        SqlConnection connection, List<string> databases, AnalysisContext context)
    {
        var result = new Dictionary<string, RcsiInactionFigures>(StringComparer.Ordinal);
        foreach (var d in databases)
            result[d] = new RcsiInactionFigures();

        // Pass 1 — counts from the pre-aggregated stats table (per-DB, per-window).
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

SELECT
    bds.database_name,
    blocking_events = SUM(bds.blocking_event_count),
    deadlocks = SUM(bds.deadlock_count)
FROM collect.blocking_deadlock_stats AS bds
WHERE bds.collection_time >= @startTime
AND   bds.collection_time <= @endTime
GROUP BY bds.database_name;";
            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var db = reader.IsDBNull(0) ? "" : reader.GetString(0);
                if (!result.TryGetValue(db, out var fig)) continue;
                fig.BlockingEvents = reader.IsDBNull(1) ? 0 : (int)Math.Min(int.MaxValue, Convert.ToInt64(reader.GetValue(1)));
                fig.Deadlocks = reader.IsDBNull(2) ? 0 : (int)Math.Min(int.MaxValue, Convert.ToInt64(reader.GetValue(2)));
            }
        }

        // Pass 2 — reader-vs-writer split over the blocked-process reports, classified
        // against the FULL lock_mode vocabulary (M-1). Reader-side = S/IS/RangeS-*;
        // writer-side = X/IX/U/UIX/RangeX-*/RangeI-*; Sch-S/Sch-M/BU EXCLUDED from the
        // denominator (RCSI-irrelevant). pct = 100 * reader / NULLIF(reader+writer, 0).
        using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = @"
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

WITH classified AS
(
    SELECT
        bpr.database_name,
        is_reader =
            CASE
                WHEN bpr.lock_mode IN (N'S', N'IS') THEN 1
                WHEN bpr.lock_mode LIKE N'RangeS-%' THEN 1
                ELSE 0
            END,
        is_writer =
            CASE
                WHEN bpr.lock_mode IN (N'X', N'IX', N'U', N'UIX') THEN 1
                WHEN bpr.lock_mode LIKE N'RangeX-%' THEN 1
                WHEN bpr.lock_mode LIKE N'RangeI-%' THEN 1
                ELSE 0
            END
    FROM collect.blocking_BlockedProcessReport AS bpr
    WHERE bpr.event_time >= @startTime
    AND   bpr.event_time <= @endTime
    AND   bpr.database_name IS NOT NULL
    /* EXCLUDE Sch-S / Sch-M / BU from the denominator (RCSI-irrelevant). */
    AND   bpr.lock_mode NOT IN (N'Sch-S', N'Sch-M', N'BU')
)
SELECT
    c.database_name,
    reader_count = SUM(CONVERT(bigint, c.is_reader)),
    writer_count = SUM(CONVERT(bigint, c.is_writer))
FROM classified AS c
WHERE c.is_reader = 1 OR c.is_writer = 1
GROUP BY c.database_name;";
            cmd.Parameters.Add(new SqlParameter("@startTime", context.TimeRangeStart));
            cmd.Parameters.Add(new SqlParameter("@endTime", context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var db = reader.IsDBNull(0) ? "" : reader.GetString(0);
                if (!result.TryGetValue(db, out var fig)) continue;
                var readerCount = reader.IsDBNull(1) ? 0L : Convert.ToInt64(reader.GetValue(1));
                var writerCount = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
                var denom = readerCount + writerCount;
                fig.ReaderWriterPct = denom > 0 ? (int)(100 * readerCount / denom) : (int?)null;
            }
        }

        return result;
    }

    private static void CollectPlanNodes(PlanNode node, List<PlanNode> nodes)
    {
        nodes.Add(node);
        foreach (var child in node.Children)
            CollectPlanNodes(child, nodes);
    }

}
