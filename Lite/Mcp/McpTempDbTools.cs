using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpTempDbTools
{
    [McpServerTool(Name = "get_tempdb_trend"), Description("Gets TempDB space usage over time: user objects, internal objects, version store, total reserved, and unallocated space. Also shows top TempDB consumer session. High version store can indicate long-running transactions under RCSI/SNAPSHOT isolation.")]
    public static async Task<string> GetTempDbTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetTempDbTrendAsync(resolved.Value.ServerId, hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No TempDB data available.");
            }

            var result = rows.Select(r => new
            {
                time = r.CollectionTime.ToString("o"),
                user_objects_mb = Math.Round(r.UserObjectReservedMb, 1),
                internal_objects_mb = Math.Round(r.InternalObjectReservedMb, 1),
                version_store_mb = Math.Round(r.VersionStoreReservedMb, 1),
                total_reserved_mb = Math.Round(r.TotalReservedMb, 1),
                unallocated_mb = Math.Round(r.UnallocatedMb, 1),
                sessions_using_tempdb = r.TotalSessionsUsingTempDb,
                top_consumer_session_id = r.TopSessionId,
                top_consumer_mb = Math.Round(r.TopSessionTempDbMb, 1)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_tempdb_trend", ex);
        }
    }
}
