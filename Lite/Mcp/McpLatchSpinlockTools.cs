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
    [McpServerTool(Name = "get_latch_stats"), Description("Gets the latest latch-contention snapshot by latch class: cumulative waiting requests and wait time (with the max single wait) plus the last collection interval's delta waits. High LATCH_EX on ACCESS_METHODS_DATASET_PARENT or a page-latch class indicates allocation/page contention (often TempDB). LATEST IS A TIME: this is the newest snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant it was collected and age_seconds its distance from the window's end. THE PAGE IS BOUNDED BY limit: latches_returned is how many latch classes you got, heaviest last-interval wait first, and truncated says the snapshot held more than limit - a sum over the page is a sum over the page, not over the server. Raise limit when truncated is true. delta_waiting_requests_count, delta_wait_time_ms and avg_wait_ms_per_request are null on a restart / first-sample row (the interval they would have accrued over was unknowable) - null, never 0, so a restart cannot read as a quiet latch.")]
    public static async Task<string> GetLatchStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description("Maximum latch classes to return, heaviest last-interval wait first. Default 20 (the Latch Stats grid's cap). This is what bounds the page - read truncated to know whether the snapshot held more.")] int limit = LocalDataService.LatchSpinlockGridRowCap,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            /* #3653 (the #3541 A3 class on Lite): the caller's limit + 1 as the fetch, the extra row as the
               observed truncation signal. The reader's LIMIT 20 sat under a count this tool published as the
               snapshot's population. */
            var rows = await dataService.GetLatchStatsSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, limit: limit + 1);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "latch_stats")
                    ?? McpHelpers.Status("unavailable", "No latch statistics available in the requested time range.");

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                /* #3541 A10: hours_back here is the span SEARCHED for the newest snapshot (its description says
                   so); the snapshot's own clock and its distance from the anchor are what make that honest. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, windowEnd),
                /* #3653: the page described as a page, on the #3594 names — latch_count read as the snapshot's
                   population and was the cap. */
                latches_returned = page.Count,
                truncated,
                order = "delta_wait_time_ms_desc",
                latches = page.Select(r => new
                {
                    latch_class = r.LatchClass,
                    waiting_requests_count = r.WaitingRequestsCount,
                    wait_time_ms = r.WaitTimeMs,
                    max_wait_time_ms = r.MaxWaitTimeMs,
                    /* #3653 A7 (#3642's rule reaching this twin through the shared row): both deltas are null on
                       the restart / first-sample row — the stored interval is the calculator's 0 marker and the
                       zeros beside it were never measured — so a caller cannot read a restart as a quiet latch. */
                    delta_waiting_requests_count = r.DeltaWaitingRequestsCount,
                    delta_wait_time_ms = r.DeltaWaitTimeMs,
                    avg_wait_ms_per_request = r.DeltaWaitingRequestsCount is long requests && requests > 0 && r.DeltaWaitTimeMs is long waitMs
                        ? Math.Round((double)waitMs / requests, 2)
                        : (double?)null
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_latch_stats", ex);
        }
    }

    [McpServerTool(Name = "get_spinlock_stats"), Description("Gets the latest spinlock-contention snapshot: cumulative collisions, spins, backoffs and spins-per-collision plus the last collection interval's delta collisions/spins. High spinlock contention is CPU-bound internal contention that does not appear in wait stats. LATEST IS A TIME: this is the newest snapshot found within hours_back of as_of, not an aggregate over those hours - captured_at is the instant it was collected and age_seconds its distance from the window's end. THE PAGE IS BOUNDED BY limit: spinlocks_returned is how many spinlocks you got, most last-interval collisions first, and truncated says the snapshot held more than limit - sys.dm_os_spinlock_stats carries well over a hundred, so at the default the page is the hot tail, not the population. Raise limit when truncated is true. delta_collisions and delta_spins are null on a restart / first-sample row (the interval they would have accrued over was unknowable) - null, never 0.")]
    public static async Task<string> GetSpinlockStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to search for the latest snapshot. Default 24.")] int hours_back = 24,
        [Description("Maximum spinlocks to return, most last-interval collisions first. Default 20 (the Spinlock Stats grid's cap). This is what bounds the page - read truncated to know whether the snapshot held more.")] int limit = LocalDataService.LatchSpinlockGridRowCap,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var limitError = McpHelpers.ValidateTop(limit);
            if (limitError != null) return limitError;

            /* #3653: limit + 1 fetched, the extra row read as truncation — see get_latch_stats. */
            var rows = await dataService.GetSpinlockStatsSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, limit: limit + 1);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "spinlock_stats")
                    ?? McpHelpers.Status("unavailable", "No spinlock statistics available in the requested time range.");

            var truncated = rows.Count > limit;
            var page = truncated ? rows.Take(limit).ToList() : rows;

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, windowEnd),
                spinlocks_returned = page.Count,
                truncated,
                order = "delta_collisions_desc",
                spinlocks = page.Select(r => new
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
