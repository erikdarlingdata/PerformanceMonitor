using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpMemoryTools
{
    [McpServerTool(Name = "get_memory_stats"), Description("Gets the latest memory statistics snapshot: physical memory, buffer pool size, plan cache size, memory utilization %, and SQL Server memory model. Use this for a quick memory health check; use get_memory_clerks to see detailed breakdown by component. LATEST IS A TIME: this reads one snapshot, not a window, and captured_at is the instant that snapshot was collected - read it before treating any figure as current, because the newest row a store holds can be minutes or days old.")]
    public static async Task<string> GetMemoryStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var stats = await dataService.GetLatestMemoryStatsAsync(resolved.ServerId);
            if (stats == null)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_stats")
                    ?? McpHelpers.Status("unavailable", "No memory stats available.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3541 A10: the one stamp every latest-snapshot read publishes, under the one name. */
                captured_at = stats.CollectionTime.ToString("o"),
                total_physical_memory_mb = stats.TotalPhysicalMemoryMb,
                available_physical_memory_mb = stats.AvailablePhysicalMemoryMb,
                memory_utilization_pct = Math.Round(stats.MemoryUtilizationPercent, 1),
                system_memory_state = stats.SystemMemoryState,
                sql_memory_model = stats.SqlMemoryModel,
                target_server_memory_mb = stats.TargetServerMemoryMb,
                total_server_memory_mb = stats.TotalServerMemoryMb,
                buffer_pool_mb = stats.BufferPoolMb,
                plan_cache_mb = stats.PlanCacheMb
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_stats", ex);
        }
    }

    [McpServerTool(Name = "get_memory_trend"), Description("Gets memory usage trend over time: total server memory, target memory, buffer pool, plan cache, and granted memory joined per point from the memory-grant series. total_granted_mb is null on points the grants series does not cover — a granted_note explains any gap; use get_memory_grants for grant detail. Useful for identifying memory growth patterns or pressure periods." + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetMemoryTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var points = await dataService.GetMemoryTrendAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            if (points.Count == 0)
            {
                /*
                    A bare empty array here told an MCP client nothing at all -- and Darling's twin already
                    returned a status envelope, so the same tool name gave two different answers depending
                    on which SKU it was pointed at. Both now make the same distinction in the same words: a
                    server that collected fine and was quiet in THIS window wants the window widened, while
                    a server the collector has never touched wants somebody to go look at collection, and
                    widening will never fill it. Probed only here, against the SAME source the trend read.
                */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await dataService.HasAnyMemoryStatAsync(resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No memory samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected memory stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No memory stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the memory_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_memory_stats will be equally empty until it does.");
            }

            var grants = await dataService.GetMemoryGrantTrendAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            var granted = AlignGrantSeries(
                points.Select(p => p.CollectionTime).ToArray(),
                grants.Select(g => (g.CollectionTime, g.TotalGrantedMb)).ToArray());
            /* #3653 A5: the window's baseline discontinuities, the payload's trailing key on every trend tool of
               both SKUs — see BaselineDiscontinuities; Darling's DarlingMcpTrendTools carries the same block. */
            var discontinuities = await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            var result = points.Select((p, i) => new
            {
                time = p.CollectionTime.ToString("o"),
                total_server_memory_mb = p.TotalServerMemoryMb,
                target_server_memory_mb = p.TargetServerMemoryMb,
                buffer_pool_mb = p.BufferPoolMb,
                plan_cache_mb = p.PlanCacheMb,
                /* Joined from the memory-grant series (#3548): the nearest memory_grant_stats snapshot
                   within 30 seconds of this memory sample, SUM(granted_memory_mb) across pools — the same
                   series the Memory Overview overlay charts. null when no snapshot aligns, never a
                   fabricated 0: a literal zero read as "granted was 0 all window" and steered callers away
                   from memory grants at exactly the wrong moment (#3529). A genuine 0.0 still appears when
                   a snapshot exists with nothing granted. */
                total_granted_mb = granted[i]
            });

            /* The note exists to explain null points; a fully covered window gets no note at all rather
               than a null-valued key (JsonOptions writes nulls). */
            return granted.Any(v => v is null)
                ? JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    granted_note = GrantGapNote,
                    trend = result,
                    discontinuities = BaselineDiscontinuities.ToPayload(discontinuities)
                }, McpHelpers.JsonOptions)
                : JsonSerializer.Serialize(new
                {
                    server = resolved.ServerName,
                    hours_back,
                    trend = result,
                    discontinuities = BaselineDiscontinuities.ToPayload(discontinuities)
                }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_trend", ex);
        }
    }

    /// <summary>
    /// Half the 1-minute cadence floor both collectors share (<c>CollectorScheduleDefaults</c>). The two
    /// series each stamp their own capture clock per collector run, so same-cycle rows sit seconds
    /// apart and can never be equality-joined — while a grants series on a slower cadence must NOT smear
    /// onto every memory point. Within half the finest cadence, at most one snapshot can claim a point.
    /// </summary>
    private static readonly TimeSpan GrantJoinTolerance = TimeSpan.FromSeconds(30);

    /// <summary>Why a point is null, stated once per payload — and only when a null point exists.</summary>
    private const string GrantGapNote =
        "total_granted_mb is null where no memory-grant snapshot lies within 30 seconds of the memory sample — the memory_grant_stats series is collected on its own schedule, so a gap means no grant measurement at that moment, not zero granted. Use get_memory_grants for the full grant picture.";

    /// <summary>
    /// Nearest-match join of the memory-grant series onto the memory-trend points (#3548): for each trend
    /// point, the closest grants snapshot within <see cref="GrantJoinTolerance"/>, else null — no grant
    /// measurement at that moment, which is not the same claim as a genuine 0.0 from a snapshot with
    /// nothing granted. Both inputs are time-ascending (both reads ORDER BY collection_time), so one
    /// forward pointer finds every nearest neighbor. Twin of Darling's
    /// <c>DarlingMcpTrendTools.AlignGrantSeries</c> — the two must stay in step so both SKUs join the
    /// same way.
    /// </summary>
    private static double?[] AlignGrantSeries(
        DateTime[] trendTimes,
        (DateTime Time, double TotalGrantedMb)[] grants)
    {
        var aligned = new double?[trendTimes.Length];
        if (grants.Length == 0) return aligned;

        var g = 0;
        for (var t = 0; t < trendTimes.Length; t++)
        {
            var target = trendTimes[t];
            while (g + 1 < grants.Length && (grants[g + 1].Time - target).Duration() <= (grants[g].Time - target).Duration())
            {
                g++;
            }

            if ((grants[g].Time - target).Duration() <= GrantJoinTolerance)
            {
                aligned[t] = grants[g].TotalGrantedMb;
            }
        }

        return aligned;
    }

    [McpServerTool(Name = "get_memory_clerks"), Description("Gets the top memory consumers by memory clerk type — shows which SQL Server components are using the most memory. LATEST IS A TIME: this reads the newest clerk snapshot, not a window, and captured_at is the instant it was collected.")]
    public static async Task<string> GetMemoryClerks(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestMemoryClerksAsync(resolved.ServerId);

            if (rows.Count == 0)
                /*
                    ONE branch here, deliberately, and it is the reason this read gets no existence probe.
                    The read is "every clerk at MAX(collection_time)", so zero rows back is logically the
                    same statement as zero rows in the table — any probe against that source would agree
                    with the read by construction. What the caller needs told is that an empty clerk list is
                    NEVER a quiet period, because on a live SQL Server it cannot be. Same words as Darling's
                    twin.
                */
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_clerks")
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No memory-clerk snapshot is available for {resolved.ServerName}. This read returns the LATEST snapshot rather than a window, so an empty result is never a quiet period — a live SQL Server always has memory clerks. It means nothing the memory_clerks collector stored is still retained, either because it has not run for this server or because its rows have aged out. Check get_collection_health and get_collection_log for the memory_clerks collector.");

            var result = rows.Select(r => new
            {
                clerk_type = r.ClerkType,
                memory_mb = Math.Round(r.MemoryMb, 2)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* Every row shares this stamp by construction (the read is every clerk at MAX(collection_time)). */
                captured_at = rows[0].CollectionTime.ToString("o"),
                clerks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_clerks", ex);
        }
    }

    [McpServerTool(Name = "get_memory_pressure_events"), Description(@"Gets memory pressure notifications from the RING_BUFFER_RESOURCE_MONITOR ring buffer (same source as sp_pressuredetector). Returns RESOURCE_MEMPHYSICAL_LOW, RESOURCE_MEMVIRTUAL_LOW, RESOURCE_MEMPHYSICAL_HIGH, and RESOURCE_MEM_STEADY notifications with indicator values.

Indicator scale (applies to both memory_indicators_process and memory_indicators_system):
  0-1 = normal, no pressure
  2   = medium pressure (SQL Server's Resource Monitor starts trimming caches and reducing grants)
  3+  = severe pressure (aggressive buffer pool / plan cache eviction)

memory_indicators_process = SQL Server process itself is under memory pressure (workload-induced).
memory_indicators_system  = Windows is signaling low memory system-wide (could be other tenants on the box).

Not available on Azure SQL DB (ring buffer not exposed). For actionable interpretation and suggested follow-up tools, see the 'Interpreting Memory Pressure Events' section of the server instructions.")]
    public static async Task<string> GetMemoryPressureEvents(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetMemoryPressureEventsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_pressure_events")
                    ?? McpHelpers.Status("empty", "No memory pressure events found in the requested time range.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                events = rows.Select(r => new
                {
                    sample_time = r.SampleTime.ToString("o"),
                    memory_notification = r.MemoryNotification,
                    memory_indicators_process = r.MemoryIndicatorsProcess,
                    memory_indicators_system = r.MemoryIndicatorsSystem
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_pressure_events", ex);
        }
    }

    [McpServerTool(Name = "get_resource_semaphore"), Description("Gets resource semaphore statistics showing granted vs available workspace memory against the target/max-target ceiling, waiter counts, and timeout/forced grant pressure indicators. High waiter counts or rising timeout/forced deltas indicate memory grant pressure affecting query performance. TWO READS UNDER ONE WINDOW: grants[] is the NEWEST snapshot in the window (one row per resource semaphore and pool), stamped once as captured_at with age_seconds against the window's end - it is a moment, not the window. window[] aggregates EVERY snapshot in the window per (resource_semaphore_id, pool_id): peak_waiter_count and peak_waiters_at (the most sessions ever seen waiting for a grant and when), peak_granted_memory_mb, min_available_memory_mb, and timeout_errors_in_window / forced_grants_in_window (the SUM of the per-interval deltas across the window). A calm grants[] beside a window[] with waiters or timeouts is a grant storm that has passed; read window[] first for 'was there pressure', grants[] for 'is there pressure now'. Each grants[] row also carries sample_interval_seconds, the measured seconds its two deltas accrued over; it is null with interval_known false when the row is a restart marker (no delta was knowable, so the zero deltas beside it are not 'no timeouts') or predates the column.")]
    public static async Task<string> GetResourceSemaphore(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24. window[] aggregates every snapshot in these hours; grants[] is the newest snapshot in them.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetResourceSemaphoreSnapshotAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_grant_stats")
                    ?? McpHelpers.Status("unavailable", "No memory grant data available.");
            }

            /* #3541 A10: the window half, over the SAME window the latest read searched — its last_snapshot_at
               IS captured_at, so the two halves describe one span of the same rows. Same shape as Darling's. */
            var window = await dataService.GetResourceSemaphoreWindowAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            var result = rows.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                resource_semaphore_id = r.ResourceSemaphoreId,
                pool_id = r.PoolId,
                target_memory_mb = Math.Round(r.TargetMemoryMb, 2),
                max_target_memory_mb = Math.Round(r.MaxTargetMemoryMb, 2),
                total_memory_mb = Math.Round(r.TotalMemoryMb, 2),
                available_memory_mb = Math.Round(r.AvailableMemoryMb, 2),
                granted_memory_mb = Math.Round(r.GrantedMemoryMb, 2),
                used_memory_mb = Math.Round(r.UsedMemoryMb, 2),
                grantee_count = r.GranteeCount,
                waiter_count = r.WaiterCount,
                timeout_error_count = r.TimeoutErrorCount,
                forced_grant_count = r.ForcedGrantCount,
                timeout_error_count_delta = r.TimeoutErrorCountDelta,
                forced_grant_count_delta = r.ForcedGrantCountDelta,
                /* #3540 (v61): the interval the deltas accrued over, the way the perfmon and file-I/O tools
                   hand it over. A stored 0 is the calculator's no-delta-knowable marker (a restart, not a
                   quiet semaphore) and is reported as null rather than 0 — 0 seconds is not a measurement;
                   a pre-v61 row that never recorded one is null too. interval_known states the one thing
                   both nulls have in common: the two *_delta zeros beside them are not "none this interval". */
                sample_interval_seconds = r.SampleIntervalSeconds is > 0 ? r.SampleIntervalSeconds : null,
                interval_known = r.SampleIntervalSeconds is > 0
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                window_start = windowEnd.AddHours(-hours_back).ToString("o"),
                window_end = windowEnd.ToString("o"),
                captured_at = rows[0].CollectionTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(rows[0].CollectionTime, windowEnd),
                grants = result,
                window = window.Select(WindowShape)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_resource_semaphore", ex);
        }
    }

    [McpServerTool(Name = "get_memory_grants"), Description("Gets resource semaphore statistics showing granted vs available workspace memory per resource pool, waiter counts, and timeout/forced grant deltas. High waiter counts or rising timeout deltas indicate memory grant pressure affecting query performance. TWO READS UNDER ONE WINDOW: grants[] is the NEWEST snapshot in the window (one row per pool, summed across its semaphores), stamped once as captured_at with age_seconds against the window's end - it is a moment, not the window. window[] aggregates EVERY snapshot in the window per pool: peak_waiter_count and peak_waiters_at (the most sessions ever seen waiting on the pool at one instant and when), peak_granted_memory_mb, min_available_memory_mb, and timeout_errors_in_window / forced_grants_in_window (the SUM of the per-interval deltas across the window). A calm grants[] beside a window[] with waiters or timeouts is a grant storm that has passed; read window[] first for 'was there pressure', grants[] for 'is there pressure now'.")]
    public static async Task<string> GetMemoryGrants(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 1. window[] aggregates every snapshot in these hours; grants[] is the newest snapshot in them.")] int hours_back = 1,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            var rows = await dataService.GetMemoryGrantChartDataAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "memory_grant_stats")
                    ?? McpHelpers.Status("unavailable", "No memory grant data available.");
            }

            /* grants[] is the latest snapshot in the window; window[] is the whole window (#3541 A10). */
            var latestTime = rows.Max(r => r.CollectionTime);
            var latest = rows.Where(r => r.CollectionTime == latestTime);
            var window = await dataService.GetMemoryGrantsWindowAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            var result = latest.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                pool_id = r.PoolId,
                available_memory_mb = Math.Round(r.AvailableMemoryMb, 2),
                granted_memory_mb = Math.Round(r.GrantedMemoryMb, 2),
                used_memory_mb = Math.Round(r.UsedMemoryMb, 2),
                grantee_count = r.GranteeCount,
                waiter_count = r.WaiterCount,
                timeout_error_count_delta = r.TimeoutErrorCountDelta,
                forced_grant_count_delta = r.ForcedGrantCountDelta
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                window_start = windowEnd.AddHours(-hours_back).ToString("o"),
                window_end = windowEnd.ToString("o"),
                captured_at = latestTime.ToString("o"),
                age_seconds = McpLatestSnapshotStamp.AgeSeconds(latestTime, windowEnd),
                grants = result,
                window = window.Select(WindowShape)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_memory_grants", ex);
        }
    }

    /// <summary>
    /// The window half's payload shape, shared by both lenses so the same key set describes a semaphore's
    /// window and a pool's window (the pool lens carries a null <c>resource_semaphore_id</c>, which
    /// <see cref="McpHelpers.JsonOptions"/> writes rather than drops — the key is present on both so a caller
    /// can read one shape). Twin of Darling's <c>DarlingMcpMemoryGrantTools.WindowShape</c>.
    /// </summary>
    private static object WindowShape(MemoryGrantWindowRow w) => new
    {
        resource_semaphore_id = w.ResourceSemaphoreId,
        pool_id = w.PoolId,
        snapshots_in_window = w.SnapshotsInWindow,
        first_snapshot_at = w.FirstSnapshotAt.ToString("o"),
        last_snapshot_at = w.LastSnapshotAt.ToString("o"),
        peak_waiter_count = w.PeakWaiterCount,
        peak_waiters_at = w.PeakWaitersAt.ToString("o"),
        peak_granted_memory_mb = Math.Round(w.PeakGrantedMemoryMb, 2),
        min_available_memory_mb = Math.Round(w.MinAvailableMemoryMb, 2),
        timeout_errors_in_window = w.TimeoutErrorsInWindow,
        forced_grants_in_window = w.ForcedGrantsInWindow
    };
}
