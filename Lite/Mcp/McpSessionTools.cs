using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpSessionTools
{
    [McpServerTool(Name = "get_active_queries"), Description("Gets active query snapshots captured by sp_WhoIsActive. Shows what queries were running at each collection point: session ID, query text, wait type, CPU time, elapsed time, blocking info, DOP, and memory grants. Use hours_back to look at a specific time window — critical for finding what was running during a CPU spike or blocking event.")]
    public static async Task<string> GetActiveQueries(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of data to retrieve. Default 1.")] int hours_back = 1,
        [Description("Filter to a specific database.")] string? database_name = null,
        [Description("Show only queries involved in blocking (blocking_session_id > 0 or is a head blocker).")] bool blocking_only = false,
        [Description("Maximum number of rows to return. Default 50.")] int limit = 50)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        var validation = McpHelpers.ValidateHoursBack(hours_back);
        if (validation != null) return validation;

        try
        {
            var rows = await dataService.GetLatestQuerySnapshotsAsync(resolved.Value.ServerId, hours_back);
            if (rows.Count == 0)
                return "No active query snapshots found in the requested time range.";

            IEnumerable<QuerySnapshotRow> filtered = rows;

            if (!string.IsNullOrEmpty(database_name))
                filtered = filtered.Where(r => r.DatabaseName.Equals(database_name, StringComparison.OrdinalIgnoreCase));

            if (blocking_only)
                filtered = filtered.Where(r => r.BlockingSessionId > 0
                    || rows.Any(other => other.BlockingSessionId == r.SessionId));

            var result = filtered.Take(limit).Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                session_id = r.SessionId,
                database_name = r.DatabaseName,
                status = r.Status,
                cpu_time_ms = r.CpuTimeMs,
                elapsed_time_ms = r.TotalElapsedTimeMs,
                elapsed_time_formatted = r.ElapsedTimeFormatted,
                logical_reads = r.LogicalReads,
                reads = r.Reads,
                writes = r.Writes,
                wait_type = string.IsNullOrEmpty(r.WaitType) ? null : r.WaitType,
                wait_time_ms = r.WaitTimeMs > 0 ? r.WaitTimeMs : (long?)null,
                blocking_session_id = r.BlockingSessionId > 0 ? r.BlockingSessionId : (int?)null,
                dop = r.Dop > 0 ? r.Dop : (int?)null,
                parallel_worker_count = r.ParallelWorkerCount > 0 ? r.ParallelWorkerCount : (int?)null,
                granted_query_memory_gb = r.GrantedQueryMemoryGb > 0 ? r.GrantedQueryMemoryGb : (double?)null,
                transaction_isolation_level = string.IsNullOrEmpty(r.TransactionIsolationLevel) ? null : r.TransactionIsolationLevel,
                open_transaction_count = r.OpenTransactionCount > 0 ? r.OpenTransactionCount : (int?)null,
                login_name = r.LoginName,
                host_name = r.HostName,
                program_name = r.ProgramName,
                query_text = McpHelpers.Truncate(r.QueryText, 2000)
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

    [McpServerTool(Name = "get_session_stats"), Description("Gets connection and session statistics grouped by application. Shows connection counts, running/sleeping/dormant breakdown, and aggregate resource usage per application.")]
    public static async Task<string> GetSessionStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var resolved = ServerResolver.Resolve(serverManager, server_name);
        if (resolved == null)
            return $"Could not resolve server. Available servers:\n{ServerResolver.ListAvailableServers(serverManager)}";

        try
        {
            var rows = await dataService.GetLatestSessionStatsAsync(resolved.Value.ServerId);
            if (rows.Count == 0)
                return "No session statistics available. The session collector may not have run yet.";

            var totalConnections = rows.Sum(r => r.ConnectionCount);
            var totalRunning = rows.Sum(r => r.RunningCount);
            var totalSleeping = rows.Sum(r => r.SleepingCount);
            var totalDormant = rows.Sum(r => r.DormantCount);

            return JsonSerializer.Serialize(new
            {
                server = resolved.Value.ServerName,
                collection_time = rows[0].CollectionTime.ToString("o"),
                summary = new
                {
                    total_connections = totalConnections,
                    total_running = totalRunning,
                    total_sleeping = totalSleeping,
                    total_dormant = totalDormant,
                    distinct_applications = rows.Count
                },
                applications = rows.Select(r => new
                {
                    program_name = r.ProgramName,
                    connections = r.ConnectionCount,
                    running = r.RunningCount,
                    sleeping = r.SleepingCount,
                    dormant = r.DormantCount,
                    total_cpu_time_ms = r.TotalCpuTimeMs,
                    total_logical_reads = r.TotalLogicalReads
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_session_stats", ex);
        }
    }
}
