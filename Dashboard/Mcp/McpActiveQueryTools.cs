using System;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorDashboard.Mcp;

[McpServerToolType]
public sealed class McpActiveQueryTools
{
    [McpServerTool(Name = "get_active_queries"), Description("Gets active query snapshots captured by sp_WhoIsActive. Shows what queries were running at each collection point: session ID, query text, wait info, CPU, reads, blocking details, and memory usage. Use hours_back to look at a specific time window — critical for finding what was running during a CPU spike or blocking event.")]
    public static async Task<string> GetActiveQueries(
        ServerManager serverManager,
        DatabaseServiceRegistry registry,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to retrieve. Default 1.")] int hours_back = 1,
        [Description("Maximum number of rows to return. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, registry, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await resolved.Value.Service.GetQuerySnapshotsAsync(hours_back);
            if (rows.Count == 0)
                return McpHelpers.Status("empty", "No active query snapshots found in the requested time range.");

            var result = rows.Take(limit).Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                session_id = r.SessionId,
                database_name = r.DatabaseName,
                status = r.Status,
                duration = r.Duration,
                cpu = r.Cpu,
                reads = r.Reads,
                writes = r.Writes,
                physical_reads = r.PhysicalReads,
                used_memory_mb = r.UsedMemoryMb,
                wait_info = r.WaitInfo,
                blocking_session_id = r.BlockingSessionId,
                blocked_session_count = r.BlockedSessionCount,
                login_name = r.LoginName,
                host_name = r.HostName,
                program_name = r.ProgramName,
                sql_text = McpHelpers.Truncate(r.SqlText, 2000),
                sql_command = McpHelpers.Truncate(r.SqlCommand, 500)
            }).ToList();

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                hours_back,
                total_snapshots = rows.Count,
                shown = result.Count,
                queries = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_active_queries", ex);
        }
    }
}
