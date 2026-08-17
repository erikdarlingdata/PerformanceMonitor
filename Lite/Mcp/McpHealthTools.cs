using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpHealthTools
{
    [McpServerTool(Name = "get_server_summary"), Description("Gets a quick health overview for a SQL Server instance: current CPU %, memory usage, recent blocking count, and deadlock count. Use this for a fast health check before drilling into specific areas.")]
    public static async Task<string> GetServerSummary(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name. Optional if only one server is configured.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var summary = await dataService.GetServerSummaryAsync(resolved.ServerId, resolved.ServerName);
            if (summary == null)
            {
                return McpHelpers.Status(
                    "unavailable",
                    $"No data available for {resolved.ServerName}. The collector may not have run yet.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                cpu_percent = summary.CpuPercent,
                memory_mb = summary.MemoryMb,
                blocking_count = summary.BlockingCount,
                deadlock_count = summary.DeadlockCount,
                last_collection = summary.LastCollectionTime?.ToString("o")
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_server_summary", ex);
        }
    }

    [McpServerTool(Name = "get_daily_summary"), Description("Gets a daily health summary: overall composite health band (Healthy/Warning/Critical), total wait time, top wait type, unique query count, deadlocks, blocking events, memory pressure (and severe memory pressure), high-CPU samples, collection errors, and actionable alert count for one day. Use this for a quick overview to decide which areas need investigation.")]
    public static async Task<string> GetDailySummary(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Summary date (yyyy-MM-dd), interpreted as a UTC day. Default is today.")] string? summary_date = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        DateTime? date = null;
        if (!string.IsNullOrEmpty(summary_date))
        {
            if (!DateTime.TryParse(summary_date, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var parsed))
                return $"Invalid date format '{summary_date}'. Use yyyy-MM-dd format (e.g., 2026-07-09).";
            date = parsed;
        }

        try
        {
            var row = await dataService.GetDailySummaryAsync(resolved.ServerId, date);
            if (row == null || !row.HasData)
            {
                var missDate = row?.SummaryDate ?? date ?? DateTime.UtcNow.Date;
                return McpHelpers.Status(
                    "empty",
                    $"No data collected for {resolved.ServerName} on {missDate:yyyy-MM-dd}.",
                    new { summary_date = missDate.ToString("yyyy-MM-dd"), overall_health = row?.OverallHealth });
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                summary_date = row.SummaryDate.ToString("yyyy-MM-dd"),
                overall_health = row.OverallHealth,
                health_band = row.HealthBand.ToString(),
                total_wait_time_sec = row.TotalWaitTimeSec,
                top_wait_type = row.TopWaitType,
                unique_queries = row.UniqueQueries,
                deadlock_count = row.DeadlockCount,
                blocking_events = row.BlockingEvents,
                high_cpu_events = row.HighCpuEvents,
                memory_pressure_events = row.MemoryPressureEvents,
                memory_critical_events = row.MemoryCriticalEvents,
                collection_errors = row.CollectionErrors,
                alert_count = row.AlertCount,
                max_block_duration_ms = row.MaxBlockDurationMs
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_daily_summary", ex);
        }
    }

    [McpServerTool(Name = "get_collection_health"), Description("Shows the health status of all data collectors for a server — whether they're running successfully, failing, or stale. Check this before investigating data to ensure collectors are working properly. Each row also carries last_note/note_count: what a NON-failing run reported, e.g. an enumeration that came back with 0 items. note_count equal to total_runs means the collector has been collecting nothing all window — not a fault (the target may be legitimately empty), but the reason a HEALTHY collector can still have no data. target_has_user_databases tells those two apart: true means the target DID have user databases in the same window, so an all-window empty enumeration is worth investigating (a login that cannot enter them, an exclusion filter that matched everything); false means either no user databases or no inventory to go on. The sweep_pressure block is the server-level roll-up: it compares the collectors' combined execution demand (average duration amortized by cadence) against the minute the fastest cadence holds. SATURATED means the collection body cannot fit inside its cadence, so relaunches are skipped and the server collects at a multiple of its configured interval while every collector still reads healthy — heaviest_collectors names where that budget goes.")]
    public static async Task<string> GetCollectionHealth(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var rows = await dataService.GetCollectionHealthAsync(resolved.ServerId);
            if (rows.Count == 0)
            {
                return McpHelpers.Status("unavailable", "No collection health data available.");
            }

            var result = rows.Select(r => new
            {
                collector = r.CollectorName,
                status = r.HealthStatus,
                total_runs = r.TotalRuns,
                errors = r.ErrorCount,
                /* Deliberate 1s lock-timeout yields (#1805) — benign, distinct from errors; clustering
                   here is a lock-contention signal about the monitored server. */
                yields = r.YieldCount,
                failure_rate_pct = Math.Round(r.FailureRatePercent, 1),
                avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                last_success = r.LastSuccessTime?.ToString("o"),
                last_error = r.LastError,
                /* #1837: what a NON-failing run reported — an enumeration that came back with 0 items,
                   items whose enumeration probe failed. note_count == total_runs means every run in the
                   window came back that way, which is the "collecting nothing for weeks" case that reads
                   as HEALTHY (correctly — an empty target is not a fault) and needs saying out loud. */
                last_note = r.LastNote,
                note_count = r.NoteCount,
                /* #1852: whether the store saw user databases on this target in the same window. The
                   fact that separates "nothing to collect" from "collecting nothing" — a caller
                   diagnosing an empty collector gets it as a boolean instead of parsing it out of the
                   sentence below. False also means "no inventory to go on", never "no databases". */
                target_has_user_databases = r.TargetHasUserDatabases,
                /* The same string both WPF grids render, composed on this side so the web dashboard and
                   any other consumer cannot re-derive it differently. */
                note_summary = CollectorHealthClassifier.FormatCollectionNote(
                    r.LastNote, r.NoteCount, r.TotalRuns, r.CollectorName, r.TargetHasUserDatabases)
            });

            /* #2296: the roll-up that makes half-rate collection visible. Every collector on a saturated
               server reads HEALTHY — from each one's own seat nothing is wrong — so the condition only
               existed as a service-log warning ("collection body has not completed … skipping relaunch").
               The verdict compares the collectors' combined execution demand (average duration amortized
               by cadence) against the minute the fastest cadence holds; heaviest_collectors names where
               the budget goes, which is the actionable half of the answer. */
            var pressure = SweepPressureClassifier.Compute(
                rows.Select(r => (r.CollectorName, r.AvgDurationMs, r.FrequencyMinutes)));
            var heaviest = rows
                .Where(r => r.FrequencyMinutes > 0 && r.AvgDurationMs > 0)
                .OrderByDescending(r => r.AvgDurationMs / r.FrequencyMinutes)
                .Take(3)
                .Select(r => new
                {
                    collector = r.CollectorName,
                    avg_duration_ms = Math.Round(r.AvgDurationMs, 0),
                    frequency_minutes = r.FrequencyMinutes
                });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                sweep_pressure = new
                {
                    busy_ms_per_minute = Math.Round(pressure.BusyMsPerMinute, 0),
                    busy_percent = Math.Round(pressure.BusyPercent, 1),
                    verdict = pressure.Verdict,
                    heaviest_collectors = heaviest,
                    note = pressure.Verdict switch
                    {
                        SweepPressureClassifier.Saturated =>
                            "The collection body cannot finish inside its cadence: relaunches are skipped every cycle and this server collects at a multiple of its configured interval, while each collector above correctly reads healthy from its own seat. The lever is capacity or placement (lighter or fewer scheduled collectors, a longer cadence, or a collector closer to the target), not collector repair.",
                        SweepPressureClassifier.AtRisk =>
                            "The collection body's average demand is close to its cadence; variance will intermittently push it over, skipping relaunches and stretching the delivered interval.",
                        _ => null
                    }
                },
                collectors = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_collection_health", ex);
        }
    }
}
