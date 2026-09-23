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
///
/// <para>
/// Every one of the nine publishes its SOURCE WITNESS (#3541 A12): <c>source_observed</c> — whether the
/// collector has ever stored a system_health event of any type for this server, i.e. whether the ring buffer
/// has ever been read into the store — and <c>last_captured_at</c>, the collector's newest capture. A zero-row
/// window is then one of four nothings (<see cref="EmptyAsync"/>) and says which; a server whose session has
/// never been read answers <c>unavailable</c>, never <c>empty</c>. Before this, eight of the nine answered a
/// dead session with the same word a healthy quiet hour earns. Darling's twin does the same.
/// </para>
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

    [McpServerTool(Name = "get_health_parser_system_health"), Description("Gets parsed system_health health indicators (the sp_server_diagnostics component results) over an event_time window ending at as_of, newest first. Ungated: every parsed event is returned. An empty answer with status empty is a real result: nothing of this kind was recorded in this window. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets parsed system_health extended event data: overall health indicators (spinlock backoffs, sick spinlocks, latch warnings, dump requests, non-yielding tasks, SQL vs system CPU, bad pages) captured by sp_server_diagnostics. " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.SpServerDiagnosticsEvent,
                    "none carried a SYSTEM component result with a timestamp (the other four sp_server_diagnostics components feed the sibling reads)", capturedInWindow: null, lastCapturedAt);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    [McpServerTool(Name = "get_health_parser_severe_errors"), Description("Gets severe errors from system_health over an event_time window ending at as_of, newest first. Gated: severity 19 or higher only, benign connection-reset error numbers excluded, so a lower-severity error is never listed. An empty answer with status empty is a real result: nothing in this window passed the gate, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets severe errors from system_health (severity >= 19, benign connection-reset numbers excluded): error number, severity, state, database, and message. These are critical SQL Server events (stack dumps, fatal errors). " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.ErrorReportedEvent,
                    $"none was a significant severe error (severity {SystemHealthSignificance.SevereErrorMinSeverity}+ and off the benign connection-reset list)", capturedInWindow: null, lastCapturedAt);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    [McpServerTool(Name = "get_health_parser_io_issues"), Description("Gets I/O issues from system_health (IO_SUBSYSTEM component results) over an event_time window ending at as_of, newest first. Gated: WARNING-state results only. An empty answer with status empty is a real result: nothing in this window passed the gate, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets I/O-related issues from system_health (IO_SUBSYSTEM component): 15-second I/O warnings, long I/O request counts, and the longest pending request duration with its file path. " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.SpServerDiagnosticsEvent,
                    "none was an IO_SUBSYSTEM component result in the WARNING state", capturedInWindow: null, lastCapturedAt);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    [McpServerTool(Name = "get_health_parser_scheduler_issues"), Description("Gets scheduler issues from system_health (non-yielding schedulers, scheduler-monitor warnings) over an event_time window ending at as_of, newest first. Gated: WARNING-state results only. An empty answer with status empty is a real result: nothing in this window passed the gate, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets scheduler issues from system_health: non-yielding schedulers and scheduler-monitor warnings, with the scheduler/cpu ids, online/runnable/running state, and non-yielding time. " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.SchedulerMonitorEvent,
                    "none was a scheduler-monitor record in the WARNING state", capturedInWindow: null, lastCapturedAt);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    [McpServerTool(Name = "get_health_parser_memory_conditions"), Description("Gets memory-condition snapshots from system_health over an event_time window ending at as_of, newest first. Gated: only snapshots whose last notification is RESOURCE_MEMPHYSICAL_LOW. An empty answer with status empty is a real result: nothing in this window passed the gate, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets memory condition snapshots from system_health (RESOURCE_MEMPHYSICAL_LOW): low-memory notifications, out-of-memory exceptions, and the memory-manager report (available physical/virtual/paging memory, working set, VM reserved/committed, pages, and the physical/virtual memory-low flags). " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.SpServerDiagnosticsEvent,
                    "none was a RESOURCE component result carrying a low-memory (RESOURCE_MEMPHYSICAL_LOW) notification", capturedInWindow: null, lastCapturedAt);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    [McpServerTool(Name = "get_health_parser_cpu_tasks"), Description("Gets CPU task results from system_health (QUERY_PROCESSING component) over an event_time window ending at as_of, newest first. Gated: WARNING-state results with at least 10 pending tasks only. An empty answer with status empty is a real result: nothing in this window passed the gate, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets CPU task events from system_health (QUERY_PROCESSING component): worker thread counts (max/created/idle), tasks completed within the interval, pending tasks and oldest pending task wait time, plus deadlock/blocking flags. " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.SpServerDiagnosticsEvent,
                    $"none was a QUERY_PROCESSING component result in the WARNING state with at least {SystemHealthSignificance.CpuTaskMinPendingTasks} pending tasks", capturedInWindow: null, lastCapturedAt);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    [McpServerTool(Name = "get_health_parser_memory_broker"), Description("Gets memory broker notifications from system_health over an event_time window ending at as_of, newest first. Gated: RESOURCE_MEMPHYSICAL_LOW notifications only. An empty answer with status empty is a real result: nothing in this window passed the gate, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets memory broker events from system_health: broker ratio changes and target adjustments (currently predicated / allocated / previously allocated), the broker name, and the notification (RESOURCE_MEMPHYSICAL_LOW; the gate returns no other). " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.MemoryBrokerEvent,
                    "none carried a low-memory notification (broker adjustments that are not a shrink under pressure are routine)", capturedInWindow: null, lastCapturedAt);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    [McpServerTool(Name = "get_health_parser_memory_node_oom"), Description("Gets memory-node out-of-memory events from system_health over an event_time window ending at as_of, newest first. Ungated: every recorded OOM is returned. An empty answer with status empty is a real result: nothing of this kind was recorded in this window. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets memory node OOM events from system_health: out-of-memory conditions on specific NUMA nodes, with the node's physical/virtual/page-file memory, target/reserved/committed KB, the failure type, and the memory-low flags. Never gated — every recorded OOM is returned. " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);
            if (rows.Count == 0)
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.MemoryNodeOomEvent,
                    "none shredded to a memory-node OOM record (this category is ungated, so a captured OOM event that parsed would be here)", capturedInWindow: null, lastCapturedAt);

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    [McpServerTool(Name = "get_health_parser_significant_waits"), Description("Gets individual waits from system_health, one row per wait_info event with the waiting statement's SQL text, over an event_time window ending at as_of, newest first. Floors: a real session, a non-BACKUP statement, at least 500 ms, and a wait type off the idle/background list; shorter waits are never listed. get_wait_stats has the instance-wide totals. An empty answer with status empty is a real result: nothing in this window passed the floors, and events_in_window counts what was captured and filtered out. status unavailable with source_observed false is no evidence either way. <<GUIDE>> Gets significant individual waits from system_health: one row per wait_info event where a real session's non-BACKUP statement waited at least 500 ms on a wait type that is not idle/background - the wait type, total and signal duration, the wait resource, the session id and the waiting statement. get_wait_stats gives the instance-wide totals and can never name the statement that paid them; this is the individual waits, with their SQL text. " + McpToolGuideTopics.SystemHealthEmptyWindows)]
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
            var lastCapturedAt = await dataService.GetLastSystemHealthCaptureAsync(resolved.ServerId);

            if (rows.Count == 0)
            {
                /*
                    The read this family's empty ladder was modelled on (#2484): events captured but none
                    significant is the healthy state and costs no extra query - the reader already counted
                    them; nothing captured in the window needs the probe to tell a quiet window from a server
                    whose wait_info has never been collected, because "no significant waits" is exactly what
                    an operator wants to hear and a caller who believes it stops looking. Since #3541 A12 the
                    ladder lives in EmptyAsync and all nine reads climb it; only the gate's own description
                    (the four conditions) is this tool's to word. Darling's twin climbs the same ladder in the
                    same words.
                */
                return await EmptyAsync(dataService, resolved.ServerId, resolved.ServerName, hours_back, windowEnd, SystemHealthParser.WaitInfoEvent,
                    $"none was significant (needs a real session, a non-BACKUP statement, at least {SystemHealthSignificance.SignificantWaitMinDurationMs} ms, and a wait type off the idle list)",
                    capturedInWindow: captured, lastCapturedAt);
            }

            return JsonSerializer.Serialize(new
            {
                server = resolved.ServerName,
                hours_back,
                source_observed = true,
                last_captured_at = Stamp(lastCapturedAt),
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

    /* ─────────────────────────── the four nothings (#3541 A12) ─────────────────────────── */

    /// <summary>
    /// What zero rows means for one system_health category, which is four different things — and only the
    /// first two are good news. Modelled on get_health_parser_significant_waits' three-way ladder (#2484),
    /// which was the ONE read of the nine that refused to call a never-read session a clean bill; the other
    /// eight answered <c>empty</c> to everything, so a dead <c>system_health</c> session, a collector that
    /// never ran, and a healthy quiet hour all read as "no severe errors". Contract rule 5: zero is a
    /// measurement, and an absence must say what it is an absence OF.
    ///
    /// <para><b>Rung 1 — captured and gated out.</b> Events of the type WERE stored in the window; the
    /// shred + significance gate kept none. Healthy. The waits reader returns the count with its rows; the
    /// other eight return survivors only, so the count is a bounded second read over the same window,
    /// taken here only once the rows came back empty. <b>Rung 2 — captured before, not in this window.</b>
    /// Quiet window; widening reaches the most recent events, and the message says when the last one was
    /// stored so the caller knows how far. <b>Rung 3 — this type never, but the session IS being read.</b>
    /// Other categories have been stored, so the ring buffer is reachable and the engine has simply never
    /// recorded one of these — for a memory-node OOM or a severe error that is the healthy measurement, not a
    /// blind spot, and it must not be called unavailable. <b>Rung 4 — nothing of any type, ever.</b> A dead
    /// session or a collector that never ran: <c>unavailable</c>, the #3524 shape, never <c>empty</c>. The
    /// #2511 engine-capability probe goes first on this rung because it is the stronger claim (an Azure SQL
    /// Database has no session to start), and its text stays exactly right for every engine that does
    /// collect this.</para>
    ///
    /// <para>Every rung carries the same two witness keys the data envelope carries
    /// (<c>source_observed</c>, <c>last_captured_at</c>) plus the rung's own evidence, at the top level
    /// beside <c>status</c> — the trend family's precedent (#3541 A2): a caller reads the witness without
    /// first checking which branch answered. Darling's <c>DarlingMcpHealthParserTools.EmptyAsync</c> is the
    /// twin, sentence for sentence; <c>McpMissMessageParityPinTests</c> holds the shared ones.</para>
    /// </summary>
    private static async Task<string> EmptyAsync(
        LocalDataService dataService, int serverId, string serverName, int hoursBack, DateTime windowEnd, string eventType,
        string noneQualifiedBecause, int? capturedInWindow, DateTime? lastCapturedAt)
    {
        var captured = capturedInWindow
            ?? await dataService.CountSystemHealthEventsAsync(serverId, eventType, hoursBack, asOfUtc: windowEnd);
        /* The type-scoped probe runs on every rung: on rung 1 the type exists in the window, and the stamp it
           returns is THIS type's newest capture rather than the server-level witness standing in for it. */
        var lastOfType = await dataService.GetLastSystemHealthCaptureOfTypeAsync(serverId, eventType);
        if (captured > 0)
        {
            return WitnessStatus(
                "empty",
                $"{captured} {eventType} event(s) were captured for {serverName} in the last {hoursBack} hour(s) and {noneQualifiedBecause}. Events ARE being captured, so this is the healthy answer for this read rather than missing data.",
                sourceObserved: true, lastCapturedAt, lastCapturedOfTypeAt: lastOfType, eventsInWindow: captured);
        }

        if (lastOfType is DateTime seen)
        {
            return WitnessStatus(
                "empty",
                $"No {eventType} events were captured for {serverName} in the last {hoursBack} hour(s). This server HAS captured them before (the newest was stored at {Stamp(seen)}), so the window is genuinely quiet rather than blind — widen hours_back to reach the most recent events.",
                sourceObserved: true, lastCapturedAt, lastCapturedOfTypeAt: seen, eventsInWindow: 0);
        }

        if (lastCapturedAt is DateTime alive)
        {
            return WitnessStatus(
                "empty",
                $"No {eventType} events have been captured for {serverName} at any time, but its system_health session IS being read — the collector last stored an event of another type at {Stamp(alive)} — so for this category the absence is a measurement: the engine has not recorded one. Not a blind spot, and a wider window would not change it.",
                sourceObserved: true, alive, lastCapturedOfTypeAt: null, eventsInWindow: 0);
        }

        return await McpEngineCapability.NotCollectedStatusAsync(dataService, serverId, serverName, SystemHealthCollectorName)
            ?? WitnessStatus(
                "unavailable",
                $"No system_health events of ANY type have EVER been captured for {serverName}, so this is NOT an all-clear — there is nothing here to be clear about. This read is served from the collected system_health ring buffer: check that collection is running for this server and that its system_health session is started before concluding nothing happened.",
                sourceObserved: false, lastCapturedAt: null, lastCapturedOfTypeAt: null, eventsInWindow: 0);
    }

    /// <summary>
    /// <see cref="McpHelpers.Status"/> with the source witness beside <c>status</c> and <c>message</c>: the
    /// same <c>source_observed</c> / <c>last_captured_at</c> pair the data envelope carries, plus what this
    /// rung measured (<c>last_captured_of_type_at</c>, <c>events_in_window</c>). Top-level rather than under
    /// <c>hints</c> so the keys sit in one place whichever branch answered.
    /// </summary>
    private static string WitnessStatus(
        string status, string message, bool sourceObserved, DateTime? lastCapturedAt, DateTime? lastCapturedOfTypeAt, int eventsInWindow)
        => JsonSerializer.Serialize(new
        {
            status,
            message,
            source_observed = sourceObserved,
            last_captured_at = Stamp(lastCapturedAt),
            last_captured_of_type_at = Stamp(lastCapturedOfTypeAt),
            events_in_window = eventsInWindow,
        }, McpHelpers.JsonOptions);

    /// <summary>The store's naive-UTC stamp in the same ISO shape the rows' <c>event_time</c> uses; null stays null.</summary>
    private static string? Stamp(DateTime? stamp) => stamp?.ToString("o");
}
