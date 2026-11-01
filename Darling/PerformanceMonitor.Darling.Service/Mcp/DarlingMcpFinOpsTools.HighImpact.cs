/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
        "high_impact: top query hashes on six measures, scored and banded.";

    internal const string HighImpactViewGuide =
        "high_impact reads the query-stats window for the server, keeps the top limit query hashes on each of the six measures (CPU, duration, reads, writes, memory and executions), and ranks the union by impact_score 0-100 (the mean percent-rank across the six measures). impact_band is high at 80 and above, medium at 60 and above, else low. The *_share_pct fields are each row's share of the kept set's total, rounded to 0.1. sample_query_text is the first 200 characters of the hash's busiest statement; the full text and the plan are not returned, has_plan only says whether a plan was captured. No cost fields.";

    private static async Task<string> ReadHighImpactAsync(
        NpgsqlDataSource postgres, (int ServerId, string ServerName, string DisplayName) resolved, int hoursBack, int limit, CancellationToken ct)
    {
        var rows = await DarlingFinOpsHighImpactReader.ReadAsync(
            postgres, resolved.ServerId, hoursBack, McpCommandDeadlines.ReadSeconds, ct, limit);
        /* #7010: the window-floor notice of query_stats, the table the ranking reads. The unmet-engine answer says its own cause and stays
           bare; a failed probe costs the notice, never the rows. */
        var now = DateTime.UtcNow;
        var start = now.AddHours(-hoursBack);
        Task<McpWindowNotice> NoticeAsync(bool emptyAnswer) => DarlingMcpWindowNotice.ReadAsync(
            () => DarlingMcpWindowNotice.Probe(postgres, "query_stats", resolved.ServerName, start, now, ct),
            start, now, "query_stats", emptyAnswer: emptyAnswer, cancellationToken: ct);

        if (rows.Count == 0)
        {
            var unmet = await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.DisplayName, "query_stats", ct);
            if (unmet is not null) return unmet;

            var noRows = await NoticeAsync(emptyAnswer: true);
            return DarlingMcpWindowNotice.FinishEmpty(McpHelpers.Status("empty",
                $"No query statistics were collected for this server in the last {hoursBack} hours, so there is nothing to rank.", noRows.AsHints()), noRows);
        }

        var notice = await NoticeAsync(emptyAnswer: false);
        return DarlingMcpWindowNotice.Finish(JsonSerializer.Serialize(new
        {
            server = resolved.DisplayName,
            view = HighImpactView,
            hours_back = hoursBack,
            effective_start = notice.EffectiveStart,
            window_truncated = notice.WindowTruncated,
            truncation_note = notice.TruncationNote,
            rows = rows.Select(HighImpactRow).ToList(),
        }, McpHelpers.JsonOptions), notice);
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
        /* #5320: judged on the whole statement, then cut to the 200 characters the SQL sample carries. */
        sample_query_text = McpHelpers.StatementPreview(r.FullQueryText, 200),
        has_plan = !string.IsNullOrEmpty(r.QueryPlanXml),
    };
}
