using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpIoTools
{
    [McpServerTool(Name = "get_file_io_stats"), Description("Gets the latest per-database-file I/O stats: read/write counts, bytes, stall times, calculated latency. Reads the newest snapshot, not a window; captured_at is when it was collected, and the deltas cover the sample_interval_seconds ending there. sample_interval_seconds 0 means no delta was knowable for that file (first sighting, counter reset, a gap) and that row's latencies are null, not 0." + McpToolGuide.Marker + " High read latency (>20ms) or write latency (>10ms for data, >2ms for log) often indicates storage bottlenecks. Each row carries sample_interval_seconds, the measured seconds its deltas accrued over; a 0 means no delta was knowable for that file at this collection (first sighting, counter reset, or a gap past the delta policy — typically a restart) and its latencies are null rather than 0.")]
    public static async Task<string> GetFileIoStats(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetLatestFileIoStatsAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "file_io_stats")
                    ?? McpHelpers.Status("unavailable", "No file I/O stats available.");
            }

            var result = rows.Select(r => new
            {
                database_name = r.DatabaseName,
                file_name = r.FileName,
                file_type = r.FileType,
                physical_name = r.PhysicalName,
                size_mb = Math.Round(r.SizeMb, 1),
                delta_reads = r.DeltaReads,
                delta_writes = r.DeltaWrites,
                delta_read_bytes = r.DeltaReadBytes,
                delta_write_bytes = r.DeltaWriteBytes,
                delta_stall_read_ms = r.DeltaStallReadMs,
                delta_stall_write_ms = r.DeltaStallWriteMs,
                /* #3540: the measured seconds the deltas accrued over, handed to the caller as the perfmon
                   tools hand theirs. 0 means no delta on this row was knowable (first sighting, counter reset,
                   a gap past the policy) and the latencies are null for it rather than the "0.00 ms" a restart
                   used to read as; null on the interval itself is a pre-v60 row that never recorded one. */
                sample_interval_seconds = r.SampleIntervalSeconds,
                avg_read_latency_ms = r.AvgReadLatencyMs is double readMs ? Math.Round(readMs, 2) : (double?)null,
                avg_write_latency_ms = r.AvgWriteLatencyMs is double writeMs ? Math.Round(writeMs, 2) : (double?)null
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                /* #3541 A10: every file row shares this stamp (the read is every file at MAX(collection_time)). */
                captured_at = rows[0].CollectionTime.ToString("o"),
                files = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_file_io_stats", ex);
        }
    }

    [McpServerTool(Name = "get_file_io_trend"), Description("Gets file I/O read and write latency over time per database, data and log files pooled, heaviest I/O stall first; past the top five the rest fold into one (other) line. database_name charts one database per file. Useful for spotting degradation in storage performance." + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetFileIoTrend(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null,
        [Description(TrendBuckets.BucketMinutesDescription)] int? bucket_minutes = null,
        [Description("One database, charted per file. Omit for every database.")] string? database_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
            if (hoursError != null) return hoursError;

            /* An unusable width is refused before anything is read, as on Darling; the cap waits for the ranking,
               because it depends on how many lines there are to draw. */
            var widthError = TrendBuckets.ValidateWidth(bucket_minutes);
            if (widthError != null) return widthError;

            /* #3897: two reads, as on Darling. The ranking's length decides the bucket width — five lines and an
               "(other)" line need coarser buckets than two lines do to stay inside the same budget — and the
               bucketed read charts exactly the series the ranking counted. The desktop chart's per-collection
               read (GetFileIoLatencyTrendAsync) is untouched: a chart wants every collection. */
            var scope = string.IsNullOrWhiteSpace(database_name) ? null : database_name.Trim();
            var series = await dataService.GetFileIoSeriesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd, scope);

            if (series.Count == 0)
            {
                /* Same two states as the memory trend, same probe discipline, same words as Darling's twin.
                   The quiet-window sentence carries one extra clause the others do not need: the ranking
                   counts only series that read or wrote, so a genuinely idle file set is empty here even on a
                   server whose file_io_stats collector ran every cycle. */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "file_io_stats");
                if (gated != null)
                {
                    return gated;
                }

                var everCollected = await dataService.HasAnyFileIoStatAsync(resolved.ServerId);

                /* A scoped read that found nothing is first a question about the NAME — Darling's twin's rule. */
                if (scope is not null && everCollected)
                {
                    return McpHelpers.Status("empty", TrendPayloads.FileIoScopeEmptyMessage(resolved.ServerName, scope, hours_back));
                }

                return everCollected
                    ? McpHelpers.Status(
                        "empty",
                        $"No file I/O samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected file I/O stats before, so this window is genuinely quiet rather than broken — widen hours_back, or read it as no measurable read or write activity on any file in this window.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No file I/O stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the file_io_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_file_io_stats will be equally empty until it does.");
            }

            var budget = TrendBudget.Mcp(TrendBuckets.FileIoMaxPoints);
            var bucketError = TrendBuckets.Resolve(hours_back, bucket_minutes, TrendPayloads.LinesFor(series.Count), budget, out var bucketMinutes);
            if (bucketError != null) return bucketError;

            var points = await dataService.GetFileIoTrendAsync(
                resolved.ServerId, hours_back, asOfUtc: windowEnd, scope, TrendPayloads.ChartedFor(series.Count), bucketMinutes);
            /* #3653 A5: the window's baseline discontinuities as the trailing key — see BaselineDiscontinuities. */
            var discontinuities = await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            /* One builder for both SKUs' envelope (TrendPayloads), so the keys and sentences cannot drift. */
            return TrendPayloads.FileIoTrend(
                resolved.ServerName, hours_back, scope, series, points, bucketMinutes, bucket_minutes is not null,
                budget.AutoPoints, BaselineDiscontinuities.ToPayload(discontinuities));
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_file_io_trend", ex);
        }
    }
}
