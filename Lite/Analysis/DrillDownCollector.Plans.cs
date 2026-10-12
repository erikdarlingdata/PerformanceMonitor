using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;

using PerformanceMonitorLite;
namespace PerformanceMonitorLite.Analysis;

public partial class DrillDownCollector
{
    /// <summary>
    /// For findings that have query hashes (bad actors), fetch the execution plan
    /// live from SQL Server via IPlanFetcher, then run PlanAnalyzer to surface
    /// warnings and missing indexes. No plan storage needed — fetch on demand
    /// only for queries that make it into high-impact findings.
    /// </summary>
    private async Task CollectPlanAnalysis(AnalysisFinding finding, AnalysisContext context)
    {
        if (finding.DrillDown == null || _planFetcher == null) return;

        // Only analyze plans for bad actor findings (1 plan each).
        // Skip top_cpu_queries (5 plans would be too heavy).
        if (!finding.RootFactKey.StartsWith("BAD_ACTOR_", StringComparison.OrdinalIgnoreCase)) return;

        var queryHash = finding.RootFactKey.Replace("BAD_ACTOR_", "");
        if (string.IsNullOrEmpty(queryHash)) return;

        // Look up plan_handle from DuckDB for this query_hash
        string? planHandle = null;
        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT plan_handle
FROM v_query_stats
WHERE server_id = $1
AND   query_hash = $2
AND   plan_handle IS NOT NULL AND plan_handle != ''
ORDER BY collection_time DESC
LIMIT 1";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = queryHash });

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (await reader.ReadAsync(context.CancellationToken) && !reader.IsDBNull(0))
                planHandle = reader.GetString(0);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* No plan_handle for this hash — the fetch below has nothing to ask for. An abandonment
               is NOT swallowed here (#2443). */
            return;
        }

        if (string.IsNullOrEmpty(planHandle)) return;

        // Fetch plan XML live from SQL Server
        var planXml = await _planFetcher.FetchPlanXmlAsync(context.ServerId, planHandle, context.CancellationToken);
        if (string.IsNullOrEmpty(planXml)) return;
        // #4348: the plan is judged BEFORE it is analyzed. A warning quotes up to 300 characters of a predicate or
        // a statement from the plan, so a statement the filter withholds must reach the analyzer already withheld,
        // with its predicates and parameter values: each quote is then judged with its statement.
        planXml = SensitiveStatements.Xml(planXml)!;

        try
        {
            var plan = ShowPlanParser.Parse(planXml, context.CancellationToken);
            // #4551: a parse-error plan still carries parser-extracted content (SQL Server's own
            // plan warnings and missing-index suggestions), so it can't be treated as empty; return
            // before that content is read. PlanAnalysisPipeline.Run separately skips analysis on it.
            if (!string.IsNullOrWhiteSpace(plan.ParseError))
                return;

            // #4530: one store read per drill-down call so rule 38 can see the server's edition/MAXDOP.
            var metadata = await ReadServerMetadataForPlanAnalysisAsync(context.ServerId, context.CancellationToken);
            PlanAnalysisPipeline.Run(plan, App.AnalyzerConfig, metadata, context.CancellationToken);

            // #4514: includes statements nested inside a stored procedure or UDF body, so a
            // finding inside an EXEC <procedure> plan's body reaches the drill-down.
            var allWarnings = PlanStatements.EnumerateAll(plan)
                .Where(s => s.RootNode != null)
                .SelectMany(s =>
                {
                    var nodeWarnings = new List<PlanNode>();
                    CollectPlanNodes(s.RootNode!, nodeWarnings);
                    return s.PlanWarnings
                        .Concat(nodeWarnings.SelectMany(n => n.Warnings));
                })
                .ToList();

            var missingIndexes = plan.AllMissingIndexes;

            if (allWarnings.Count == 0 && missingIndexes.Count == 0) return;

            finding.DrillDown["plan_analysis"] = new
            {
                query_hash = queryHash,
                warning_count = allWarnings.Count,
                critical_count = allWarnings.Count(w => w.Severity == PlanWarningSeverity.Critical),
                warnings = allWarnings
                    .OrderByDescending(w => w.Severity)
                    .Take(10)
                    .Select(w => new
                    {
                        severity = w.Severity.ToString(),
                        type = w.WarningType,
                        message = McpHelpers.Truncate(w.Message, 300)
                    }),
                missing_indexes = missingIndexes.Take(5).Select(idx => new
                {
                    table = $"{idx.Schema}.{idx.Table}",
                    impact = idx.Impact,
                    create_statement = idx.CreateStatement
                })
            };
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            // Plan parsing can fail on malformed XML — skip silently. An abandonment is NOT
            // swallowed here (#2443).
        }
    }

    /// <summary>
    /// #4530: the drill-down's own store read for rule 38 (edition/MAXDOP), against the same DuckDB
    /// connection pattern the collector's other reads here use. Mirrors
    /// <c>DarlingServerMetadataReader.ReadAsync</c>; a failure or no rows returns <c>null</c>.
    /// </summary>
    private async Task<PerformanceMonitor.PlanAnalysis.ServerMetadata?> ReadServerMetadataForPlanAnalysisAsync(int serverId, System.Threading.CancellationToken cancellationToken)
    {
        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT p.edition, p.product_version, p.product_level, p.cpu_count, p.physical_memory_mb,
       (SELECT c.value_in_use
        FROM v_server_config AS c
        WHERE c.server_id = $1
        AND   c.configuration_name = 'max degree of parallelism'
        AND   c.capture_time = (SELECT MAX(capture_time) FROM v_server_config WHERE server_id = $1)
        LIMIT 1) AS max_dop,
       p.engine_edition,
       p.vcore_count
FROM v_server_properties AS p
WHERE p.server_id = $1
ORDER BY p.collection_time DESC
LIMIT 1";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken)) return null;

            /* Same rule as LocalDataService.GetServerMetadataForPlanAnalysisAsync: an Azure SQL Database's stored
               physical_memory_mb is the HOST's, so the drill-down's server context carries no RAM figure, and its Hardware row
               names the vCores. */
            int? engineEdition = reader.IsDBNull(6) ? null : Convert.ToInt32(reader.GetValue(6));
            int? vcoreCount = reader.IsDBNull(7) ? null : Convert.ToInt32(reader.GetValue(7));
            int? storedCpuCount = reader.IsDBNull(3) ? null : reader.GetInt32(3);
            long? storedPhysicalMemoryMb = reader.IsDBNull(4) ? null : Convert.ToInt64(reader.GetValue(4));

            return new PerformanceMonitor.PlanAnalysis.ServerMetadata
            {
                Edition = reader.IsDBNull(0) ? null : reader.GetString(0),
                ProductVersion = reader.IsDBNull(1) ? null : reader.GetString(1),
                ProductLevel = reader.IsDBNull(2) ? null : reader.GetString(2),
                CpuCount = storedCpuCount ?? 0,
                PhysicalMemoryMB = ServerHardwareScope.OwnPhysicalMemoryMb(engineEdition, storedPhysicalMemoryMb) ?? 0L,
                EngineEdition = engineEdition,
                VcoreCount = vcoreCount,
                MaxDop = reader.IsDBNull(5) ? 0 : Convert.ToInt32(Convert.ToDouble(reader.GetValue(5))),
            };
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, cancellationToken))
        {
            return null;
        }
    }

    /// <summary>
    /// WS4: re-parses the top collected query plans (the same top-10-by-cost set the fact collector
    /// summarized) and attaches the specific missing indexes / plan warnings to a MISSING_INDEX or
    /// PLAN_WARNING finding's drill-down. The fact carries only counts (Fact.Metadata is numeric),
    /// so the strings — CREATE statements, warning messages — are rendered here. Mirrors the
    /// Dashboard SqlServerDrillDownCollector. Best-effort: a read/parse failure leaves no detail.
    /// </summary>
    private async Task CollectPlanAdvisoryDetail(AnalysisFinding finding, AnalysisContext context, HashSet<string> pathKeys)
    {
        try
        {
            var planXmls = new List<string>();

            using (var readLock = _duckDb.AcquireReadLock(context.CancellationToken))
            using (var connection = _duckDb.CreateConnection())
            {
                await connection.OpenAsync(context.CancellationToken);

                using var cmd = connection.CreateCommand();
                cmd.CommandText = @"
SELECT query_plan_xml
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   query_plan_xml IS NOT NULL
ORDER BY delta_worker_time DESC
LIMIT 10";
                cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
                cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (!reader.IsDBNull(0))
                        planXmls.Add(SensitiveStatements.Xml(reader.GetString(0))!); // #4348: judged before the analyzer quotes it
                }
            }

            if (planXmls.Count == 0)
                return;

            // #4530: one store read per collector call so rule 38 can see the server's edition/MAXDOP.
            var metadata = await ReadServerMetadataForPlanAnalysisAsync(context.ServerId, context.CancellationToken);
            var details = PlanAdvisoryAggregator.ExtractCancellable(planXmls, App.AnalyzerConfig, metadata, context.CancellationToken);

            if (pathKeys.Contains("MISSING_INDEX") && details.MissingIndexes.Count > 0)
            {
                finding.DrillDown!["missing_indexes"] = details.MissingIndexes
                    .OrderByDescending(i => i.Impact)
                    .Take(5)
                    .Select(i => new
                    {
                        table = $"{i.Schema}.{i.Table}",
                        impact = Math.Round(i.Impact, 1),
                        create_statement = i.CreateStatement
                    })
                    .ToList();
            }

            if (pathKeys.Contains("PLAN_WARNING") && details.Warnings.Count > 0)
            {
                finding.DrillDown!["plan_warnings"] = details.Warnings
                    .OrderByDescending(w => w.Severity)
                    .Take(5)
                    .Select(w => new
                    {
                        type = w.WarningType,
                        severity = w.Severity.ToString(),
                        message = McpHelpers.Truncate(w.Message, 300)
                    })
                    .ToList();
            }
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            // Plan read/parse can fail on malformed XML — skip, the detail is best-effort.
            // An abandonment is NOT swallowed here (#2443).
        }
    }
}
