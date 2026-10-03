/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage.FinOps;

namespace PerformanceMonitor.Darling.Service.Mcp;

public sealed partial class DarlingMcpFinOpsTools
{
    internal const string HighImpactView = "high_impact";

    internal const string HighImpactViewLine =
        "high_impact: top query_hash aggregates by CPU, duration, reads, writes, memory and executions, each with its share of the set, impact_score 0-100 and impact_band (high >= 80, medium >= 60, else low). No cost fields.";

    internal const string HighImpactViewGuide =
        "high_impact reads the query-stats window for the server, keeps the top limit query hashes on each of the six measures, and ranks the union by impact_score (the mean percent-rank across the six measures). The *_share_pct fields are each row's share of the kept set's total, rounded to 0.1. sample_query_text is the first 200 characters of the hash's busiest statement; the full text and the plan are not returned, has_plan only says whether a plan was captured.";

    private static async Task<string> ReadHighImpactAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName) resolved, int hoursBack, int limit, CancellationToken ct)
    {
        var rows = await DarlingFinOpsHighImpactReader.ReadAsync(
            postgres, resolved.ServerId, hoursBack, McpCommandDeadlines.ReadSeconds, ct, limit);
        if (rows.Count == 0)
        {
            return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, "query_stats", ct)
                ?? McpHelpers.Status("empty",
                    $"No query statistics were collected for this server in the last {hoursBack} hours, so there is nothing to rank.");
        }

        return JsonSerializer.Serialize(new
        {
            server = resolved.ServerName,
            view = HighImpactView,
            hours_back = hoursBack,
            rows = rows.Select(HighImpactRow).ToList(),
        }, McpHelpers.JsonOptions);
    }

    /// <summary>One high-impact row in the wire shape: snake_case keys, the band decided here, no query text beyond
    /// the short sample and no plan XML.</summary>
    internal static object HighImpactRow(HighImpactQuery r) => new
    {
        query_hash = r.QueryHash,
        database_name = r.DatabaseName,
        total_executions = r.TotalExecutions,
        total_cpu_ms = r.TotalCpuMs,
        total_duration_ms = r.TotalDurationMs,
        total_reads = r.TotalReads,
        total_writes = r.TotalWrites,
        total_memory_mb = r.TotalMemoryMb,
        cpu_share_pct = r.CpuShare,
        duration_share_pct = r.DurationShare,
        reads_share_pct = r.ReadsShare,
        writes_share_pct = r.WritesShare,
        memory_share_pct = r.MemoryShare,
        executions_share_pct = r.ExecutionsShare,
        impact_score = r.ImpactScore,
        impact_band = HighImpactScorer.HighImpactBand(r.ImpactScore),
        sample_query_text = r.SampleQueryText,
        has_plan = !string.IsNullOrEmpty(r.QueryPlanXml),
    };
}
