using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using PerformanceMonitorLite.Services;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// The system_health parse-on-read MCP tools — get_health_parser_system_health / _severe_errors /
/// _io_issues / _scheduler_issues / _memory_conditions / _cpu_tasks / _memory_broker / _memory_node_oom /
/// _significant_waits — served over Lite's DuckDB store. Each wraps the existing System Events reader,
/// which shreds the raw
/// system_health event_xml with the shared PerformanceMonitor.Common.SystemHealthParser and gates it through
/// SystemHealthSignificance (the SAME significant set the viewer's System Events tab shows). System Health
/// is the one UNGATED category (its corruption/contention counter series returns every snapshot). STORED
/// reads, no live monitored-server hit; windowed on the XE event_time. Each tool caps output at limit.
/// </summary>
[McpServerToolType]
public sealed class McpHealthParserTools
{
    /// <summary>
    /// The collector every one of these nine reads is served by. Named once so the #2511 capability probe
    /// asks about the same collector on every read and on both SKUs; a test scans both MCP trees for the
    /// names passed to the probe and holds them to <c>CollectorCatalog</c>, because an unknown name would
    /// answer "supported" and silently restore the old wrong message.
    /// </summary>
    private const string SystemHealthCollectorName = "system_health_events";

    [McpServerTool(Name = "get_health_parser_system_health"), Description("Gets parsed system_health extended event data: overall health indicators (spinlock backoffs, sick spinlocks, latch warnings, dump requests, non-yielding tasks, SQL vs system CPU, bad pages) captured by sp_server_diagnostics.")]
    public static async Task<string> GetSystemHealth(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetSystemHealthAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No system health data found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                total_entries = rows.Count,
                shown = Math.Min(rows.Count, limit),
                entries = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    state = r.State,
                    spinlock_backoffs = r.SpinlockBackoffs,
                    sick_spinlock_type = r.SickSpinlockType,
                    sick_spinlock_type_after_av = r.SickSpinlockTypeAfterAv,
                    latch_warnings = r.LatchWarnings,
                    is_access_violation_occurred = r.IsAccessViolationOccurred,
                    write_access_violation_count = r.WriteAccessViolationCount,
                    total_dump_requests = r.TotalDumpRequests,
                    interval_dump_requests = r.IntervalDumpRequests,
                    non_yielding_tasks_reported = r.NonYieldingTasksReported,
                    page_faults = r.PageFaults,
                    system_cpu_utilization = r.SystemCpuUtilization,
                    sql_cpu_utilization = r.SqlCpuUtilization,
                    bad_pages_detected = r.BadPagesDetected,
                    bad_pages_fixed = r.BadPagesFixed
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_system_health", ex); }
    }

    [McpServerTool(Name = "get_health_parser_severe_errors"), Description("Gets severe errors from system_health (severity >= 19, benign connection-reset numbers excluded): error number, severity, state, database, and message. These are critical SQL Server events (stack dumps, fatal errors).")]
    public static async Task<string> GetSevereErrors(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetSevereErrorsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No severe errors found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                error_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                errors = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    error_number = r.ErrorNumber,
                    severity = r.Severity,
                    state = r.State,
                    database_id = r.DatabaseId,
                    database_name = r.DatabaseName,
                    message = r.Message
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_severe_errors", ex); }
    }

    [McpServerTool(Name = "get_health_parser_io_issues"), Description("Gets I/O-related issues from system_health (IO_SUBSYSTEM component): 15-second I/O warnings, long I/O request counts, and the longest pending request duration with its file path.")]
    public static async Task<string> GetIOIssues(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetIoIssuesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No I/O issues found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                issue_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                issues = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    state = r.State,
                    io_latch_timeouts = r.IoLatchTimeouts,
                    interval_long_ios = r.IntervalLongIos,
                    total_long_ios = r.TotalLongIos,
                    longest_pending_requests_duration_ms = r.LongestPendingRequestsDurationMs,
                    longest_pending_requests_file_path = r.LongestPendingRequestsFilePath
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_io_issues", ex); }
    }

    [McpServerTool(Name = "get_health_parser_scheduler_issues"), Description("Gets scheduler issues from system_health: non-yielding schedulers and scheduler-monitor warnings, with the scheduler/cpu ids, online/runnable/running state, and non-yielding time.")]
    public static async Task<string> GetSchedulerIssues(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetSchedulerIssuesAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No scheduler issues found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                issue_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                issues = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    scheduler_id = r.SchedulerId,
                    cpu_id = r.CpuId,
                    status = r.Status,
                    is_online = r.IsOnline,
                    is_runnable = r.IsRunnable,
                    is_running = r.IsRunning,
                    non_yielding_time_ms = r.NonYieldingTimeMs,
                    thread_quantum_ms = r.ThreadQuantumMs
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_scheduler_issues", ex); }
    }

    [McpServerTool(Name = "get_health_parser_memory_conditions"), Description("Gets memory condition snapshots from system_health (RESOURCE_MEMPHYSICAL_LOW): low-memory notifications, out-of-memory exceptions, and the memory-manager report (available physical/virtual/paging memory, working set, VM reserved/committed, pages, and the physical/virtual memory-low flags).")]
    public static async Task<string> GetMemoryConditions(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetMemoryConditionsAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No memory condition events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                event_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    last_notification = r.LastNotification,
                    out_of_memory_exceptions = r.OutOfMemoryExceptions,
                    is_any_pool_out_of_memory = r.IsAnyPoolOutOfMemory,
                    process_out_of_memory_period = r.ProcessOutOfMemoryPeriod,
                    name = r.Name,
                    available_physical_memory_gb = r.AvailablePhysicalMemoryGb,
                    available_virtual_memory_gb = r.AvailableVirtualMemoryGb,
                    available_paging_file_gb = r.AvailablePagingFileGb,
                    working_set_gb = r.WorkingSetGb,
                    percent_of_committed_memory_in_ws = r.PercentOfCommittedMemoryInWs,
                    page_faults = r.PageFaults,
                    system_physical_memory_high = r.SystemPhysicalMemoryHigh,
                    system_physical_memory_low = r.SystemPhysicalMemoryLow,
                    process_physical_memory_low = r.ProcessPhysicalMemoryLow,
                    process_virtual_memory_low = r.ProcessVirtualMemoryLow,
                    vm_reserved_gb = r.VmReservedGb,
                    vm_committed_gb = r.VmCommittedGb,
                    locked_pages_allocated = r.LockedPagesAllocated,
                    large_pages_allocated = r.LargePagesAllocated,
                    emergency_memory_gb = r.EmergencyMemoryGb,
                    emergency_memory_in_use_gb = r.EmergencyMemoryInUseGb,
                    target_committed_gb = r.TargetCommittedGb,
                    current_committed_gb = r.CurrentCommittedGb,
                    pages_allocated = r.PagesAllocated,
                    pages_reserved = r.PagesReserved,
                    pages_free = r.PagesFree,
                    pages_in_use = r.PagesInUse,
                    page_alloc_potential = r.PageAllocPotential,
                    numa_growth_phase = r.NumaGrowthPhase,
                    last_oom_factor = r.LastOomFactor,
                    last_os_error = r.LastOsError
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_memory_conditions", ex); }
    }

    [McpServerTool(Name = "get_health_parser_cpu_tasks"), Description("Gets CPU task events from system_health (QUERY_PROCESSING component): worker thread counts (max/created/idle), tasks completed within the interval, pending tasks and oldest pending task wait time, plus deadlock/blocking flags.")]
    public static async Task<string> GetCPUTasks(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetCpuTasksAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No CPU task events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                event_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    state = r.State,
                    max_workers = r.MaxWorkers,
                    workers_created = r.WorkersCreated,
                    workers_idle = r.WorkersIdle,
                    tasks_completed_within_interval = r.TasksCompletedWithinInterval,
                    pending_tasks = r.PendingTasks,
                    oldest_pending_task_waiting_time = r.OldestPendingTaskWaitingTime,
                    has_unresolvable_deadlock_occurred = r.HasUnresolvableDeadlockOccurred,
                    has_deadlocked_schedulers_occurred = r.HasDeadlockedSchedulersOccurred,
                    did_blocking_occur = r.DidBlockingOccur
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_cpu_tasks", ex); }
    }

    [McpServerTool(Name = "get_health_parser_memory_broker"), Description("Gets memory broker events from system_health: broker ratio changes and target adjustments (currently predicated / allocated / previously allocated), the broker name, and the notification (RESOURCE_MEMPHYSICAL_HIGH/LOW).")]
    public static async Task<string> GetMemoryBroker(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetMemoryBrokerAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No memory broker events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                event_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    broker_id = r.BrokerId,
                    pool_metadata_id = r.PoolMetadataId,
                    delta_time = r.DeltaTime,
                    memory_ratio = r.MemoryRatio,
                    new_target = r.NewTarget,
                    overall = r.Overall,
                    rate = r.Rate,
                    currently_predicated = r.CurrentlyPredicated,
                    currently_allocated = r.CurrentlyAllocated,
                    previously_allocated = r.PreviouslyAllocated,
                    broker = r.Broker,
                    notification = r.Notification
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_memory_broker", ex); }
    }

    [McpServerTool(Name = "get_health_parser_memory_node_oom"), Description("Gets memory node OOM events from system_health: out-of-memory conditions on specific NUMA nodes, with the node's physical/virtual/page-file memory, target/reserved/committed KB, the failure type, and the memory-low flags. Never gated — every recorded OOM is returned.")]
    public static async Task<string> GetMemoryNodeOOM(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var rows = await dataService.GetMemoryNodeOomAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);
            if (rows.Count == 0)
                return await McpEngineCapability.NotCollectedStatusAsync(dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No memory node OOM events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                event_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                events = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    node_id = r.NodeId,
                    memory_node_id = r.MemoryNodeId,
                    memory_utilization_pct = r.MemoryUtilizationPct,
                    total_physical_memory_kb = r.TotalPhysicalMemoryKb,
                    available_physical_memory_kb = r.AvailablePhysicalMemoryKb,
                    total_page_file_kb = r.TotalPageFileKb,
                    available_page_file_kb = r.AvailablePageFileKb,
                    total_virtual_address_space_kb = r.TotalVirtualAddressSpaceKb,
                    available_virtual_address_space_kb = r.AvailableVirtualAddressSpaceKb,
                    target_kb = r.TargetKb,
                    reserved_kb = r.ReservedKb,
                    committed_kb = r.CommittedKb,
                    shared_committed_kb = r.SharedCommittedKb,
                    awe_kb = r.AweKb,
                    pages_kb = r.PagesKb,
                    failure_type = r.FailureType,
                    failure_value = r.FailureValue,
                    resources = r.Resources,
                    factor_text = r.FactorText,
                    factor_value = r.FactorValue,
                    last_error = r.LastError,
                    pool_metadata_id = r.PoolMetadataId,
                    is_process_in_job = r.IsProcessInJob,
                    is_system_physical_memory_high = r.IsSystemPhysicalMemoryHigh,
                    is_system_physical_memory_low = r.IsSystemPhysicalMemoryLow,
                    is_process_physical_memory_low = r.IsProcessPhysicalMemoryLow,
                    is_process_virtual_memory_low = r.IsProcessVirtualMemoryLow
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_memory_node_oom", ex); }
    }

    [McpServerTool(Name = "get_health_parser_significant_waits"), Description("Gets significant individual waits from system_health: one row per wait_info event where a real session's non-BACKUP statement waited at least 500 ms on a wait type that is not idle/background - the wait type, total and signal duration, the wait resource, the session id and the waiting statement. get_wait_stats gives the instance-wide totals and can never name the statement that paid them; this is the individual waits, with their SQL text.")]
    public static async Task<string> GetSignificantWaits(
        LocalDataService dataService,
        ServerManager serverManager,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = ServerResolver.ResolveOrError(serverManager, server_name);
        if (error != null) return error;

        try
        {
            var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
            if (validation != null) return validation;

            var (rows, captured) = await dataService.GetSignificantWaitsWithCaptureAsync(resolved.ServerId, hours_back, asOfUtc: windowEnd);

            if (rows.Count == 0)
            {
                /*
                    Three different nothings, and only one of them is good news. Events captured but none
                    significant is the healthy state and costs no extra query - the reader already counted
                    them. Nothing captured at all needs the probe to tell a quiet window from a server whose
                    wait_info has never been collected, because "no significant waits" is exactly what an
                    operator wants to hear and a caller who believes it stops looking. Darling's twin makes
                    the same three distinctions in the same words.
                */
                if (captured > 0)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"{captured} wait_info event(s) were captured for {resolved.ServerName} in the last {hours_back} hour(s) and none was significant (needs a real session, a non-BACKUP statement, at least {SystemHealthSignificance.SignificantWaitMinDurationMs} ms, and a wait type off the idle list). Events ARE being captured, so this is the healthy answer for this read rather than missing data.");
                }

                var everCaptured = await dataService.HasAnySystemHealthEventOfTypeAsync(
                    resolved.ServerId, SystemHealthParser.WaitInfoEvent);
                if (everCaptured)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"No wait_info events were captured for {resolved.ServerName} in the last {hours_back} hour(s). This server HAS captured them before, so the window is genuinely quiet rather than blind — widen hours_back to reach the most recent events.");
                }

                /*
                    #2511 adds a FOURTH nothing, and it is the one that was being mis-explained. On an engine
                    whose system_health collector is gated off there is no session to start and no collection
                    to check, so the advice below is advice about something that cannot exist. The engine
                    answer goes first because it is the stronger claim; the text after it stays exactly right
                    for every engine that DOES collect this.
                */
                return await McpEngineCapability.NotCollectedStatusAsync(
                        dataService, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status(
                        "unavailable",
                        $"No wait_info events have EVER been captured for {resolved.ServerName}, so this is NOT an all-clear — there is nothing here to be clear about. This read is served from the collected system_health ring buffer: check that collection is running for this server and that its system_health session is started before concluding nothing was waiting.");
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                wait_count = rows.Count,
                shown = Math.Min(rows.Count, limit),
                waits = rows.Take(limit).Select(r => new
                {
                    event_time = r.EventTime?.ToString("o"),
                    wait_type = r.WaitType,
                    duration_ms = r.DurationMs,
                    /* Signal duration is the part spent runnable AFTER the resource was granted, so a
                       signal close to the total is CPU pressure wearing a wait type's name. */
                    signal_duration_ms = r.SignalDurationMs,
                    wait_resource = r.WaitResource,
                    session_id = r.SessionId,
                    query_text = r.QueryText
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_significant_waits", ex); }
    }
}
