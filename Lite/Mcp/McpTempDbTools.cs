using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpTempDbTools
{
    [McpServerTool(Name = "get_tempdb_trend"), Description("Gets TempDB space usage over time in time buckets: user objects, internal objects, version store, total reserved and unallocated space, and the top consumer session. High version store can indicate long-running transactions under RCSI/SNAPSHOT isolation.")]
    public static async Task<string> GetTempDbTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            /* #3960: bucketed as on Darling; the desktop chart's per-collection read (GetTempDbTrendAsync) is untouched. */
            var budget = TrendBudget.Mcp(TrendBuckets.TempDbMaxPoints);
            var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
            if (bucketError != null) return bucketError;

            var points = await dataService.GetTempDbBucketsAsync(resolved.ServerId, hours_back, windowEnd, bucketMinutes);
            if (points.Count == 0)
            {
                /* #7005: the window notice (an empty answer only; not_collected and precondition say their own cause). */
                return await McpQueryTools.WithEmptyWindowNoticeAsync(
                    await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "tempdb_stats", awsRdsKind: AwsRdsKind.None)
                        ?? await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.DisplayName, "tempdb_stats")
                        ?? McpHelpers.Status("unavailable", "No TempDB data available."),
                    dataService, QueryWindowRelation.TempDbStats, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, "tempdb_stats");
            }

            return await McpQueryTools.WithDataWindowNoticeAsync(
                TrendPayloads.TempDbTrend(resolved.DisplayName, hours_back, points, bucketMinutes, bucket_minutes is not null, budget.AutoPoints),
                dataService, QueryWindowRelation.TempDbStats, resolved.ServerId, windowEnd.AddHours(-hours_back), windowEnd, "tempdb_stats");
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_tempdb_trend", ex);
        }
    }
}
