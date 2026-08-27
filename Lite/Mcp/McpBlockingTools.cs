using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

[McpServerToolType]
public sealed class McpBlockingTools
{
    [McpServerTool(Name = "get_deadlocks"), Description("Gets recent deadlock events with victim process info. Deadlocks occur when two or more sessions permanently block each other. Use get_deadlock_detail for the full deadlock graph XML.")]
    public static async Task<string> GetDeadlocks(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 20.")] int limit = 20,
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

            var rows = await dataService.GetRecentDeadlocksAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "deadlocks")
                    /* #2546: capability first (permanent), then the runtime precondition (fixable), then the
                       read's own miss. A deadlock capture the collector cannot read returns zero rows
                       forever, which is byte-identical to a server that simply did not deadlock — the one
                       answer nobody should be given without being told. */
                    ?? await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.ServerName, "deadlocks")
                    ?? McpHelpers.Status("empty", "No deadlocks found in the specified time range.");
            }

            var result = rows.Take(limit).Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                deadlock_time = r.DeadlockTime?.ToString("o"),
                victim_process_id = r.VictimProcessId,
                victim_sql_text = McpHelpers.Truncate(r.VictimSqlText, 2000),
                process_summary = r.ProcessSummary,
                has_deadlock_xml = r.HasDeadlockXml
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                total_deadlocks = rows.Count,
                deadlocks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlocks", ex);
        }
    }

    [McpServerTool(Name = "get_deadlock_detail"), Description("Gets the full deadlock graph XML for a specific time range. Returns the raw XML that can be analyzed for lock resources, process details, and deadlock chains.")]
    public static async Task<string> GetDeadlockDetail(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum deadlocks to return. Default 5.")] int limit = 5,
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

            var rows = await dataService.GetRecentDeadlocksAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            var withXml = rows.Where(r => r.HasDeadlockXml).Take(limit).ToList();
            if (withXml.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "deadlocks")
                    ?? McpHelpers.Status("empty", "No deadlock XML available in the specified time range.");
            }

            var result = withXml.Select(r => new
            {
                collection_time = r.CollectionTime.ToString("o"),
                deadlock_time = r.DeadlockTime?.ToString("o"),
                victim_process_id = r.VictimProcessId,
                deadlock_graph_xml = r.DeadlockGraphXml
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                deadlocks = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlock_detail", ex);
        }
    }

    [McpServerTool(Name = "get_blocked_process_reports"), Description("Gets detailed blocked process reports from extended events (parsed via sp_HumanEventsBlockViewer). Provides detailed blocked/blocking session info: isolation levels, transaction names, full query text for both sessions. Use for deep analysis of prolonged blocking.")]
    public static async Task<string> GetBlockedProcessReports(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum rows. Default 30.")] int limit = 30,
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

            var rows = await dataService.GetRecentBlockedProcessReportsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "blocked_process_report")
                    ?? McpHelpers.Status("empty", "No blocked process reports found.");
            }

            var result = rows.Take(limit).Select(r => new
            {
                event_time = r.EventTime?.ToString("o"),
                database_name = r.DatabaseName,
                blocked_spid = r.BlockedSpid,
                blocked_ecid = r.BlockedEcid,
                blocking_spid = r.BlockingSpid,
                blocking_ecid = r.BlockingEcid,
                wait_time_ms = r.WaitTimeMs,
                wait_resource = r.WaitResource,
                lock_mode = r.LockMode,
                blocked_status = r.BlockedStatus,
                blocked_isolation_level = r.BlockedIsolationLevel,
                blocked_log_used = r.BlockedLogUsed,
                blocked_transaction_count = r.BlockedTransactionCount,
                blocked_client_app = r.BlockedClientApp,
                blocked_host_name = r.BlockedHostName,
                blocked_login_name = r.BlockedLoginName,
                blocked_sql_text = McpHelpers.Truncate(r.BlockedSqlText, 2000),
                blocking_status = r.BlockingStatus,
                blocking_isolation_level = r.BlockingIsolationLevel,
                blocking_client_app = r.BlockingClientApp,
                blocking_host_name = r.BlockingHostName,
                blocking_login_name = r.BlockingLoginName,
                blocking_sql_text = McpHelpers.Truncate(r.BlockingSqlText, 2000),
                blocked_transaction_name = r.BlockedTransactionName,
                blocking_transaction_name = r.BlockingTransactionName,
                blocked_last_tran_started = r.BlockedLastTranStarted?.ToString("o"),
                blocking_last_tran_started = r.BlockingLastTranStarted?.ToString("o"),
                blocked_last_batch_started = r.BlockedLastBatchStarted?.ToString("o"),
                blocking_last_batch_started = r.BlockingLastBatchStarted?.ToString("o"),
                blocked_last_batch_completed = r.BlockedLastBatchCompleted?.ToString("o"),
                blocking_last_batch_completed = r.BlockingLastBatchCompleted?.ToString("o"),
                blocked_priority = r.BlockedPriority,
                blocking_priority = r.BlockingPriority
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                reports = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocked_process_reports", ex);
        }
    }

    [McpServerTool(Name = "get_blocked_process_xml"), Description("Gets the raw blocked process report XML from extended events. Contains full detail about both the blocked and blocking sessions for deep analysis.")]
    public static async Task<string> GetBlockedProcessXml(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history. Default 24.")] int hours_back = 24,
        [Description("Maximum reports to return. Default 5.")] int limit = 5,
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

            var rows = await dataService.GetRecentBlockedProcessReportsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            var withXml = rows.Where(r => r.HasReportXml).Take(limit).ToList();
            if (withXml.Count == 0)
            {
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "blocked_process_report")
                    /* #2546: same order and same reason as get_deadlocks — a blocked-process capture the
                       collector cannot read is indistinguishable here from a server that never blocked. */
                    ?? await McpRuntimePrecondition.StatusAsync(dataService, resolved.ServerId, resolved.ServerName, "blocked_process_report")
                    ?? McpHelpers.Status("empty", "No blocked process report XML available in the specified time range.");
            }

            var result = withXml.Select(r => new
            {
                event_time = r.EventTime?.ToString("o"),
                database_name = r.DatabaseName,
                blocked_spid = r.BlockedSpid,
                blocking_spid = r.BlockingSpid,
                wait_time_ms = r.WaitTimeMs,
                blocked_process_report_xml = r.BlockedProcessReportXml
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                reports = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocked_process_xml", ex);
        }
    }

    [McpServerTool(Name = "get_blocking_trend"), Description("Gets a time-series of blocking event counts over time. Useful for identifying patterns (e.g., blocking spikes during batch jobs) or confirming whether blocking is a new, worsening, or resolved issue.")]
    public static async Task<string> GetBlockingTrend(
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
            var hoursError = McpHelpers.ValidateWindow(hours_back, as_of, out var anchorEnd);
            if (hoursError != null) return hoursError;

            /* One instant for BOTH reads. Resolving now separately in the trend and the capture count
               lets a row arrive between them, and the two answers exist to be compared -- Darling's
               twin pins a single now for exactly this reason. That instant is the as_of anchor when one
               was sent, so both reads move together onto the past window rather than one of them.

               Threaded as asOfUtc rather than as fromDate/toDate: those two are SERVER-LOCAL and are
               converted back to UTC inside GetTimeRange, so handing them an instant that is already UTC
               shifts the window by the monitored server's offset -- silently, and in the unanchored case
               too (review catch). asOfUtc is the UTC-safe branch, and one value still means one instant. */
            var points = await dataService.GetBlockingTrendAsync(
                resolved.ServerId, hours_back, asOfUtc: anchorEnd);

            if (points.Count == 0)
            {
                /*
                    An empty trend is two facts and the WRONG one is the reassuring one. "No blocking"
                    reads as an all-clear and a caller who believes it stops looking; "nothing collected"
                    means nothing at all is known about the window. The stored tables cannot tell them
                    apart -- both are an absence of rows in an EDGE table -- so the denominator comes from
                    collection_log, which records a SUCCESS with zero rows for a collector that ran and saw
                    nothing. Darling's twin makes the same distinction with the same words.
                */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "blocked_process_report");
                if (gated != null)
                {
                    return gated;
                }

                var captures = await dataService.GetBlockingCaptureCountsAsync(
                    resolved.ServerId, hours_back, asOfUtc: anchorEnd);
                return await EmptyTrend(
                    "blocking", resolved.ServerName, hours_back, captures,
                    () => dataService.HasAnyBlockingCollectorRunAsync(resolved.ServerId));
            }

            var result = points.Select(p => new { time = p.Time.ToString("o"), count = p.Count });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_blocking_trend", ex);
        }
    }

    [McpServerTool(Name = "get_deadlock_trend"), Description("Gets a time-series of deadlock event counts over time. Useful for identifying patterns or confirming whether deadlock issues are new, worsening, or resolved.")]
    public static async Task<string> GetDeadlockTrend(
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

            /* Same single-instant discipline as the blocking trend above, threaded the same UTC-safe way. */
            var points = await dataService.GetDeadlockTrendAsync(
                resolved.ServerId, hours_back, asOfUtc: windowEnd);

            if (points.Count == 0)
            {
                /* Same two facts as the blocking trend above, same denominator, same reason. */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "deadlocks");
                if (gated != null)
                {
                    return gated;
                }

                var captures = await dataService.GetDeadlockCaptureCountsAsync(
                    resolved.ServerId, hours_back, asOfUtc: windowEnd);
                return await EmptyTrend(
                    /* SINGULAR: the subject lands in "No {subject} was recorded", and "no deadlocks
                       was recorded" is not a sentence. It also reads correctly in the other two,
                       where it modifies the collector rather than the event. */
                    "deadlock", resolved.ServerName, hours_back, captures,
                    () => dataService.HasAnyDeadlockCollectorRunAsync(resolved.ServerId));
            }

            var result = points.Select(p => new { time = p.Time.ToString("o"), count = p.Count });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_deadlock_trend", ex);
        }
    }

    [McpServerTool(Name = "get_lock_wait_trend"), Description("Gets the AGGREGATE lock-wait rate over time for a server: every LCK% wait type's wait milliseconds per second at each collection, the viewer's Blocking Trends lock-wait chart. get_wait_trend charts ONE named wait type and get_blocking_trend counts incidents; this is the whole lock family at once, as a rate rather than a count. Use it to see whether a server's lock pressure is rising when no single wait type dominates, to tell a few long blocks from constant low-grade contention, and to pick which LCK type to hand to get_wait_trend next. The rate is delta wait time divided by the seconds since the previous collection, so it is comparable across servers collecting on different cadences.")]
    public static async Task<string> GetLockWaitTrend(
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

            var points = await dataService.GetLockWaitTrendAsync(
                resolved.ServerId, hours_back, asOfUtc: windowEnd);

            if (points.Count == 0)
            {
                /*
                    The denominator here is the DATA, not collection_log — the opposite of the two trends
                    above, and deliberately so. Those read EDGE tables, where a row exists only because
                    something went wrong, so a healthy server has none and a data probe would report it as
                    uncollected. wait_stats is PERIODIC: the collector writes a row every cycle for every
                    wait type it observes, whatever the server is doing, so the presence of ANY wait sample
                    is proof somebody looked.

                    The LCK% filter is deliberately NOT applied to the probe. A server that has collected
                    wait stats for months and never taken a lock wait is the all-clear this branch is for,
                    and filtering the probe the same way would call that server uncollected. Darling's twin
                    makes the same distinction with the same words.
                */
                var gated = await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, "wait_stats");
                if (gated != null)
                {
                    return gated;
                }

                return await dataService.HasAnyWaitStatAsync(resolved.ServerId)
                    ? McpHelpers.Status(
                        "empty",
                        $"No lock waits recorded for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS collected wait stats before, so this window is genuinely quiet rather than broken — widen hours_back to find the most recent samples.")
                    : McpHelpers.Status(
                        "unavailable",
                        $"No wait stats have EVER been recorded for {resolved.ServerName}, so this is NOT a report of a server without lock contention — nothing has been stored for it at all. Delta wait stats need a SECOND collection cycle before the first row exists, so on a newly added server this clears itself; otherwise check that collection is running and that the server is enabled.");
            }

            /* Rows, not a pre-pivoted series per wait type. The caller decides whether to sum the family or
               chart the members, and a pivot here would have to pick a top-N and silently drop the rest —
               on a read whose premise is that no single LCK type dominates. */
            var result = points.Select(p => new
            {
                collection_time = p.CollectionTime.ToString("o"),
                wait_type = p.WaitType,
                wait_time_ms_per_second = Math.Round(p.WaitTimeMsPerSecond, 3),
            });

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                trend = result
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex)
        {
            return McpHelpers.FormatError("get_lock_wait_trend", ex);
        }
    }

    /// <summary>
    /// The empty answer both trends give, and the whole point of #2485: <c>trend: []</c> is the same bytes
    /// on a server that had no blocking and on one that collected nothing, and an agent holding only the
    /// JSON cannot tell a clean bill of health from a hole in coverage.
    ///
    /// <para>Two statuses, picked by the denominator rather than by the edge rows. <c>empty</c> means
    /// captures ran in this window and none of them saw the event — a real all-clear, bounded by the
    /// sampling interval. <c>unavailable</c> means no capture ran, so the window says nothing either way;
    /// the existence probe then separates a server that has never collected this at all from one with a
    /// GAP, because "check that collection is running" and "widen the window" are different next
    /// moves.</para>
    ///
    /// <para>The hints carry the per-collector run counts: three events mean something different in a
    /// window of 60 captures than in a window of 4, and the caller cannot supply that number itself.
    /// Darling's <c>DarlingMcpBlockingTools.EmptyTrend</c> returns the SAME sentences word for word.</para>
    /// </summary>
    private static async Task<string> EmptyTrend(
        /* SINGULAR ("blocking", "deadlock"): it is the subject of "No {subject} was recorded". */
        string subject,
        string serverName,
        int hoursBack,
        List<CollectorCaptureCount> captures,
        Func<Task<bool>> hasEverCapturedAsync)
    {
        var captureCount = captures.Sum(c => c.Runs);
        var hints = new
        {
            server = serverName,
            hours_back = hoursBack,
            capture_count = captureCount,
            captures = captures.Select(c => new
            {
                collector = c.CollectorName,
                runs = c.Runs,
                first_run_at = c.FirstRunAt?.ToString("o"),
                last_run_at = c.LastRunAt?.ToString("o"),
            }),
        };

        if (captureCount > 0)
            return McpHelpers.Status(
                "empty",
                $"No {subject} was recorded for {serverName} in the last {hoursBack} hour(s). {captureCount} collector run(s) DID execute over this window, so this is a genuine all-clear rather than missing data — see hints.captures for which collectors ran and when.",
                hints);

        var everCaptured = await hasEverCapturedAsync();
        return McpHelpers.Status(
            "unavailable",
            everCaptured
                ? $"No {subject} collector runs are recorded for {serverName} in the last {hoursBack} hour(s), so this is NOT an all-clear — nothing was captured and the window says nothing either way. Collection HAS run for this server outside the window, so this is a gap rather than a dead collector: widen hours_back, or use get_collection_health to find where it stopped."
                : $"No {subject} collector runs have EVER been recorded for {serverName}, so this is NOT an all-clear — there is nothing to read. Check that collection is running for this server before concluding it was quiet.",
            hints);
    }
}
