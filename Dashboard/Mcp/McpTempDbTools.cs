using System;
using System.Linq;
using System.Threading.Tasks;
using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpTempDbTools
{
    [McpServerTool(Name = "get_tempdb_trend"), Description("Gets TempDB space usage over time showing user objects, internal objects, version store, and unallocated space. High version store usage can indicate long-running transactions under RCSI/snapshot isolation. Includes pressure level assessment and recommendations.")]
    public static async Task<string> GetTempDbTrend(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
        {
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";
        }

        try
        {
            var hoursError = McpHelpers.ValidateHoursBack(hours_back);
            if (hoursError != null) return hoursError;

            var rows = await resolved.Value.Service.GetTempdbStatsAsync(hours_back);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No TempDB data available.");
            }

            var result = rows.Select(r => new
            {
                time = r.CollectionTime.ToString("o"),
                user_objects_mb = r.UserObjectReservedMb,
                internal_objects_mb = r.InternalObjectReservedMb,
                version_store_mb = r.VersionStoreReservedMb,
                total_reserved_mb = r.TotalReservedMb,
                unallocated_mb = r.UnallocatedMb,
                sessions_using_tempdb = r.TotalSessionsUsingTempdb,
                top_consumer_session_id = r.TopTaskSessionId,
                top_consumer_mb = r.TopTaskTotalMb,
                version_store_pct = r.VersionStorePercent,
                version_store_high_warning = r.VersionStoreHighWarning,
                allocation_contention_warning = r.AllocationContentionWarning,
                pressure_level = r.PressureLevel,
                recommendation = r.Recommendation
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
