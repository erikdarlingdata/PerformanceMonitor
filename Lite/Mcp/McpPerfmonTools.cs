using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpPerfmonTools
{
    [McpServerTool(Name = "get_perfmon_stats"), Description("Gets the latest SQL Server performance counter values: batch requests/sec, compilations/sec, deadlocks/sec, and more. Provides throughput context to distinguish a busy server from a sick one. Use counter_name or instance_name to filter results.")]
    public static async Task<string> GetPerfmonStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Filter to a specific counter name, e.g. 'Batch Requests/sec'.")] string? counter_name = null,
        [Description("Filter to a specific instance name, e.g. a database name.")] string? instance_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestPerfmonStatsAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "perfmon_stats")
                    ?? McpHelpers.Status("unavailable", "No perfmon stats available.");
            }

            IEnumerable<PerfmonRow> filtered = rows;
            if (!string.IsNullOrEmpty(counter_name))
                filtered = filtered.Where(r => r.CounterName != null && r.CounterName.Contains(counter_name, StringComparison.OrdinalIgnoreCase));
            if (!string.IsNullOrEmpty(instance_name))
                filtered = filtered.Where(r => r.InstanceName != null && r.InstanceName.Contains(instance_name, StringComparison.OrdinalIgnoreCase));

            var result = filtered.Select(r => new
            {
                counter_name = r.CounterName,
                instance_name = r.InstanceName,
                value = r.Value,
                delta_value = r.DeltaValue
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                counters = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_perfmon_stats", ex);
        }
    }

    [McpServerTool(Name = "get_perfmon_trend"), Description("Gets a time-series trend for a specific performance counter. Use get_perfmon_stats first to see available counter names.")]
    public static async Task<string> GetPerfmonTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The exact counter name, e.g. 'Batch Requests/sec'.")] string counter_name,
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

            var points = await dataService.GetPerfmonTrendAsync(resolved.ServerId, counter_name, hours_back, asOfUtc: windowEnd);
            if (points.Count == 0)
            {
                /* The engine question comes BEFORE the distinct-counter probe, not after it. Both are on
                   the miss path, so either order keeps the property that matters — but a permanently gated
                   engine takes this branch on every call, forever, and neither the probe nor the PLE branch
                   below could tell it anything. Asking first makes that case one query instead of two. */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "perfmon_stats");
                if (gated != null)
                {
                    return gated;
                }

                /* No points can mean three different things to a caller. Distinguish them so an LLM
                   doesn't read a bad counter name as "this metric looks fine." */
                var collected = await dataService.GetDistinctPerfmonCountersAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

                /* Page Life Expectancy is the counter people reach for by habit; it is intentionally
                   not collected, so an empty trend would otherwise be misread as "PLE looks fine." */
                if (IsPageLifeExpectancy(counter_name))
                    return McpHelpers.Status(
                        "not_collected",
                        $"No trend data for counter '{counter_name}'. Page Life Expectancy is a legacy metric and is intentionally not collected. " +
                        "Use get_memory_stats for buffer pool / memory pressure instead.",
                        new { collected_counters = collected });

                /* Nothing collected at all for this server in the window: the collector likely hasn't
                   produced perfmon data yet (delta counters need two cycles). Not retrievable now. */
                if (collected.Count == 0)
                    return McpHelpers.Status(
                        "unavailable",
                        $"No trend data for counter '{counter_name}'. No perfmon counters have been collected for this server in the last {hours_back}h yet " +
                        "(the collector may not have run, or delta counters need a second collection cycle).");

                /* Other counters exist but not this one: the name is almost certainly wrong. Hand back
                   the collected names so the caller can correct it. */
                return McpHelpers.Status(
                    "not_collected",
                    $"No trend data for counter '{counter_name}'. It may not be a counter this server collects — see hints.collected_counters for the {collected.Count} that are.",
                    new { collected_counters = collected });
            }

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                value = p.Value,
                delta_value = p.DeltaValue,
                /* The delta's denominator. 0 means no delta was knowable, so delta_value = 0 with an
                   interval of 0 must NOT be read as "no activity"; derive rates as
                   delta_value / sample_interval_seconds rather than assuming a fixed cadence (#2234). */
                sample_interval_seconds = p.SampleIntervalSeconds
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                counter_name,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_perfmon_trend", ex);
        }
    }

    /// <summary>
    /// True when the caller asked for Page Life Expectancy by any common spelling. Matches the full
    /// counter name (case-insensitive) or an exact "PLE" — but not "PLE" as a substring, so counters
    /// like "samples" don't false-positive.
    /// </summary>
    private static bool IsPageLifeExpectancy(string counterName) =>
        counterName.Contains("page life expectancy", StringComparison.OrdinalIgnoreCase) ||
        counterName.Trim().Equals("PLE", StringComparison.OrdinalIgnoreCase);
}
