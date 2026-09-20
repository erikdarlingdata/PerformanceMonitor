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
    [McpServerTool(Name = "get_file_io_stats"), Description("Gets the latest file I/O statistics per database file: read/write counts, bytes, stall times, and calculated latency. High read latency (>20ms) or write latency (>10ms for data, >2ms for log) often indicates storage bottlenecks. Each row carries sample_interval_seconds, the measured seconds its deltas accrued over; a 0 means no delta was knowable for that file at this collection (first sighting, counter reset, or a gap past the delta policy — typically a restart) and its latencies are null rather than 0. LATEST IS A TIME: this reads the newest file-I/O snapshot, not a window, and captured_at is the instant it was collected; the deltas cover the sample_interval_seconds ending there.")]
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

    [McpServerTool(Name = "get_file_io_trend"), Description("Gets I/O latency trend over time per database, useful for spotting degradation in storage performance." + BaselineDiscontinuities.DescriptionSentence)]
    public static async Task<string> GetFileIoTrend(
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

            var points = await dataService.GetFileIoLatencyTrendAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            if (points.Count == 0)
            {
                /* Same two states as the memory trend, same probe discipline, same words as Darling's twin.
                   The quiet-window sentence carries one extra clause the others do not need: this read's
                   top_files CTE requires delta_reads or delta_writes above zero, so a genuinely idle file
                   set is empty here even on a server whose file_io_stats collector ran every cycle. */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "file_io_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await dataService.HasAnyFileIoStatAsync(resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No file I/O samples recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected file I/O stats before, so this window is genuinely quiet rather than broken — widen hours_back, or read it as no measurable read or write activity on any file in this window.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No file I/O stats have EVER been recorded for {resolved.ServerName}. This is not an empty window — the file_io_stats collector has stored nothing at all for this server. Check that collection is running and that the server is enabled; get_file_io_stats will be equally empty until it does.");
            }

            var result = points.Select(p => new
            {
                time = p.CollectionTime.ToString("o"),
                database_name = p.DatabaseName,
                avg_read_latency_ms = Math.Round(p.AvgReadLatencyMs, 2),
                avg_write_latency_ms = Math.Round(p.AvgWriteLatencyMs, 2)
            });
            /* #3653 A5: the window's baseline discontinuities as the trailing key — see BaselineDiscontinuities. */
            var discontinuities = await dataService.GetBaselineDiscontinuitiesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                trend = result,
                discontinuities = BaselineDiscontinuities.ToPayload(discontinuities)
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_file_io_trend", ex);
        }
    }
}
