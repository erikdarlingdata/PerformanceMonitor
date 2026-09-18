using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The latch / spinlock contention MCP tools — get_latch_stats, get_spinlock_stats — served over Lite's
/// DuckDB store. Each wraps the existing latest-snapshot reader (GetLatchStatsSnapshotAsync /
/// GetSpinlockStatsSnapshotAsync): the per-class / per-spinlock cumulative counters plus the last
/// collection interval's delta at the most recent collection in the window. STORED reads, no live hit.
/// </summary>
[McpServerToolType]
public sealed class McpLatchSpinlockTools
{
    [McpServerTool(Name = "get_latch_stats"), Description("Gets the latest latch-contention snapshot by latch class: cumulative waiting requests and wait time (with the max single wait) plus the last collection interval's delta waits. High LATCH_EX on ACCESS_METHODS_DATASET_PARENT or a page-latch class indicates allocation/page contention (often TempDB). LATEST IS A TIME: this is the newest snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant it was collected and age_seconds its distance from the window's end.")]
    public static async Task<string> GetLatchStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetLatchStatsSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "latch_stats")
                    ?? McpHelpers.Status("unavailable", "No latch statistics available in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #3541 A10: hours_back here is the span SEARCHED for the newest snapshot (its description says
                   so); the snapshot's own clock and its distance from the anchor are what make that honest. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, windowEnd),
                latch_count = rows.Count,
                latches = rows.Select(r => new
                {
                    latch_class = r.LatchClass,
                    waiting_requests_count = r.WaitingRequestsCount,
                    wait_time_ms = r.WaitTimeMs,
                    max_wait_time_ms = r.MaxWaitTimeMs,
                    delta_waiting_requests_count = r.DeltaWaitingRequestsCount,
                    delta_wait_time_ms = r.DeltaWaitTimeMs,
                    avg_wait_ms_per_request = r.DeltaWaitingRequestsCount > 0
                        ? Math.Round((double)r.DeltaWaitTimeMs / r.DeltaWaitingRequestsCount, 2)
                        : (double?)null
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_latch_stats", ex);
        }
    }

    [McpServerTool(Name = "get_spinlock_stats"), Description("Gets the latest spinlock-contention snapshot: cumulative collisions, spins, backoffs and spins-per-collision plus the last collection interval's delta collisions/spins. High spinlock contention is CPU-bound internal contention that does not appear in wait stats. LATEST IS A TIME: this is the newest snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant it was collected and age_seconds its distance from the window's end.")]
    public static async Task<string> GetSpinlockStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetSpinlockStatsSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "spinlock_stats")
                    ?? McpHelpers.Status("unavailable", "No spinlock statistics available in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, windowEnd),
                spinlock_count = rows.Count,
                spinlocks = rows.Select(r => new
                {
                    spinlock_name = r.SpinlockName,
                    collisions = r.Collisions,
                    spins = r.Spins,
                    spins_per_collision = Math.Round(r.SpinsPerCollision, 1),
                    sleep_time = r.SleepTime,
                    backoffs = r.Backoffs,
                    delta_collisions = r.DeltaCollisions,
                    delta_spins = r.DeltaSpins
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_spinlock_stats", ex);
        }
    }
}
