using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpPerfmonTools
{
    [McpServerTool(Name = "get_perfmon_stats"), Description("Gets the latest SQL Server performance counter values (batch requests/sec, compilations/sec, deadlocks/sec, and more). LATEST IS A TIME: the newest snapshot, not a window; captured_at is when it was collected; use get_perfmon_trend for history. counter_kind: gauge = value IS the reading, delta_value null; rate = value is cumulative, delta_value its per-interval change; other = a non-rate per-interval change; null counter_kind predates the column, classify by name (ends in /sec = rate)." + McpToolGuide.Marker + " Gets the latest SQL Server performance counter values: batch requests/sec, compilations/sec, deadlocks/sec, and more. Provides throughput context to distinguish a busy server from a sick one. Use counter_name or instance_name to filter results. LATEST IS A TIME: this reads the newest counter snapshot, not a window, and captured_at is the instant it was collected; use get_perfmon_trend for a counter over time. Each row carries counter_kind from the stored cntr_type: 'gauge' means value IS the reading (a level such as Total Server Memory (KB); delta_value is null because a level has no delta), 'rate' means value is a cumulative count and delta_value is its change over the last collection interval (get_perfmon_trend carries the sample_interval_seconds to divide it by for a per-second figure), 'other' means an average/fraction numerator whose delta_value is a per-interval change and not a rate; null counter_kind is a row written before the type was stored — classify it by name (a counter whose name ends in /sec is a rate).")]
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

            /* counter_kind is the stored type's three-way reading (v62, #3653 A7) through the one shared
               vocabulary; delta_value is null on a gauge because the collector writes none — the reading is
               value — and null on nothing else. Twin of Darling's DarlingMcpDataTools. */
            var result = filtered.Select(r => new
            {
                counter_name = r.CounterName,
                instance_name = r.InstanceName,
                value = r.Value,
                delta_value = r.DeltaValue,
                cntr_type = r.CntrType,
                counter_kind = PerfmonCounterTypes.Word(r.CntrType)
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3541 A10: taken from the unfiltered snapshot, so a filter that matches nothing still says when. */
                captured_at = rows[0].CollectionTime.ToString("o"),
                counters = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_perfmon_stats", ex);
        }
    }

    [McpServerTool(Name = "get_perfmon_trend"), Description("Gets one performance counter over time in time buckets. Use get_perfmon_stats first to see available counter names. counter_kind (from the stored cntr_type) says what a point's number is: 'gauge' — value is the bucket's average reading and peak_value its highest, delta_value and sample_interval_seconds are null because a level has no delta; 'rate' — the per-second figure is delta_value divided by sample_interval_seconds, never delta_value alone, and never where sample_interval_seconds is 0 (no delta was knowable); 'other' — delta_value is the change of an average/fraction numerator, not a rate and not a level; null — the rows predate the stored type or the instances mix types, so classify by name (a name ending in /sec is a rate)." + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetPerfmonTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("The exact counter name, e.g. 'Batch Requests/sec'.")] string counter_name,
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

            /* #3960: bucketed as on Darling; the desktop chart's per-collection read (GetPerfmonTrendAsync) is untouched. */
            var budget = TrendBudget.Mcp(TrendBuckets.PerfmonMaxPoints);
            var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, 1, budget, out var bucketMinutes);
            if (bucketError != null) return bucketError;

            var points = await dataService.GetPerfmonBucketsAsync(resolved.ServerId, counter_name, hours_back, windowEnd, bucketMinutes);
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

            /* counter_kind is the series' stored type read three ways (v62, #3653 A7): the type of any point
               that has one, because a counter's type does not change and the read reports a type only where
               the point's instance rows agree; null when no point has one. A gauge's points publish null
               delta_value and null sample_interval_seconds — the collector writes neither for a level — and
               value is the bucket's average reading. sample_interval_seconds is the delta's denominator: 0 means
               no delta was knowable, so delta_value = 0 with an interval of 0 must NOT be read as "no activity"
               (#2234). TrendPayloads.PerfmonTrend builds both SKUs' envelope. */
            /* #3653 A5: the window's baseline discontinuities as the trailing key — see BaselineDiscontinuities. */
            var discontinuities = await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            return TrendPayloads.PerfmonTrend(
                resolved.ServerName, counter_name, hours_back, points, bucketMinutes, bucket_minutes is not null,
                budget.AutoPoints, BaselineDiscontinuities.ToPayload(discontinuities));
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
