/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;

#pragma warning disable CA1707 // MCP tools use snake_case naming convention

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The system_health parse-on-read MCP tools — get_health_parser_cpu_tasks / _io_issues / _memory_broker /
/// _memory_conditions / _memory_node_oom / _scheduler_issues / _severe_errors / _significant_waits /
/// _system_health — served over Darling's Postgres store, the SAME nine tools the Dashboard's
/// <c>McpHealthParserTools</c> exposes. Where the Dashboard reads its server-side-parsed
/// <c>collect.HealthParser_*</c> tables, these PARSE ON READ: the raw <c>system_health_events</c> the
/// collector captured are shredded by the shared
/// <see cref="SystemHealthParser"/> (PerformanceMonitor.Common — reused, NOT re-implemented) and gated by
/// <see cref="SystemHealthSignificance"/>, exactly as the viewer's System Events tab does. The
/// result is the same SIGNIFICANT warning set the Dashboard surfaces (its collector runs sp_HealthParser at
/// <c>@warnings_only = 1</c>); System Health is the one UNGATED category (its corruption/contention counter
/// series plots every snapshot, matching the viewer's <c>GetSystemHealthAsync</c>).
///
/// <para>
/// Reads flow through <see cref="DarlingSystemHealthReader"/> — a STORED read (no live monitored-server hit)
/// windowed on the XE <c>event_time</c> (the event's real time, the viewer's choice). The result rows carry
/// every column sp_HealthParser logs for the category (the Common record's full field set — a superset of
/// the Dashboard tool's projection), each surfaced as the event's <c>event_time</c> plus the category
/// columns; the store's synthetic row id and collection_time (a persistence artifact of the Dashboard's
/// parsed-table architecture) have no analog in the parse-on-read record and are omitted. Severe-error
/// <c>database_name</c> is resolved from the collected size-stats mapping (the DB-free shred left it null).
/// </para>
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpHealthParserTools
{
    /// <summary>
    /// The collector every one of these nine reads is served by. Named once so the #2511 capability probe
    /// asks about the same collector on every read and on both SKUs; a test scans both MCP trees for the
    /// names passed to the probe and holds them to <c>CollectorCatalog</c>, because an unknown name would
    /// answer "supported" and silently restore the old wrong message.
    /// </summary>
    private const string SystemHealthCollectorName = "system_health_events";

    [McpServerTool(Name = "get_health_parser_system_health"), Description("Gets parsed system_health extended event data: overall health indicators captured by sp_HealthParser.")]
    public static async Task<string> GetSystemHealth(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        try
        {
            /* The corruption + contention counter series is UNGATED (no warnings-only filter) — the viewer's
               GetSystemHealthAsync keeps every SYSTEM snapshot that has a timestamp. */
            var c = await CollectAsync(postgres, server_name, hours_back, limit, as_of,
                SystemHealthParser.SpServerDiagnosticsEvent,
                xml => One(SystemHealthParser.ParseSystemHealth(xml)),
                r => r.EventTime.HasValue);
            if (c.EarlyReturn != null) return c.EarlyReturn;
            if (c.Rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, c.ServerId, c.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No system health data found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = c.ServerName,
                hours_back,
                total_entries = c.Rows.Count,
                shown = Math.Min(c.Rows.Count, limit),
                entries = c.Rows.Take(limit).Select(r => new
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

    [McpServerTool(Name = "get_health_parser_severe_errors"), Description("Gets severe errors from system_health: stack dumps, non-yielding schedulers, and other critical SQL Server events.")]
    public static async Task<string> GetSevereErrors(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            /* database_id → name resolution needs the collected size-stats mapping (the shred left it null). */
            var mapTask = DarlingSystemHealthReader.GetDatabaseNameMapAsync(postgres, resolved.ServerId);
            var xmls = await DarlingSystemHealthReader.ReadEventXmlAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, SystemHealthParser.ErrorReportedEvent);
            var map = await mapTask;

            var rows = xmls
                .Select(SystemHealthParser.ParseSevereError)
                .Where(r => r != null && SystemHealthSignificance.IsSignificant(r))
                .Select(r => r!)
                .ToList();
            if (rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
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
                    database_name = DarlingSystemHealthReader.ResolveDatabaseName(r.DatabaseId, map),
                    message = r.Message
                })
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) { return McpHelpers.FormatError("get_health_parser_severe_errors", ex); }
    }

    [McpServerTool(Name = "get_health_parser_io_issues"), Description("Gets I/O-related issues from system_health: 15-second I/O warnings, long I/O requests, and stalled I/O subsystems.")]
    public static async Task<string> GetIOIssues(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        try
        {
            /* IO_SUBSYSTEM fans one event out to one row per pending-request file — a many-per-event shred. */
            var c = await CollectAsync(postgres, server_name, hours_back, limit, as_of,
                SystemHealthParser.SpServerDiagnosticsEvent,
                SystemHealthParser.ParseIoIssues,
                SystemHealthSignificance.IsSignificant);
            if (c.EarlyReturn != null) return c.EarlyReturn;
            if (c.Rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, c.ServerId, c.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No I/O issues found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = c.ServerName,
                hours_back,
                issue_count = c.Rows.Count,
                shown = Math.Min(c.Rows.Count, limit),
                issues = c.Rows.Take(limit).Select(r => new
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

    [McpServerTool(Name = "get_health_parser_scheduler_issues"), Description("Gets scheduler issues from system_health: non-yielding schedulers, deadlocked schedulers, and scheduler monitor events.")]
    public static async Task<string> GetSchedulerIssues(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        try
        {
            var c = await CollectAsync(postgres, server_name, hours_back, limit, as_of,
                SystemHealthParser.SchedulerMonitorEvent,
                xml => One(SystemHealthParser.ParseSchedulerIssue(xml)),
                SystemHealthSignificance.IsSignificant);
            if (c.EarlyReturn != null) return c.EarlyReturn;
            if (c.Rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, c.ServerId, c.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No scheduler issues found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = c.ServerName,
                hours_back,
                issue_count = c.Rows.Count,
                shown = Math.Min(c.Rows.Count, limit),
                issues = c.Rows.Take(limit).Select(r => new
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

    [McpServerTool(Name = "get_health_parser_memory_conditions"), Description("Gets memory condition events from system_health: low memory notifications, memory broker adjustments, and memory pressure indicators.")]
    public static async Task<string> GetMemoryConditions(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        try
        {
            var c = await CollectAsync(postgres, server_name, hours_back, limit, as_of,
                SystemHealthParser.SpServerDiagnosticsEvent,
                xml => One(SystemHealthParser.ParseMemoryConditions(xml)),
                SystemHealthSignificance.IsSignificant);
            if (c.EarlyReturn != null) return c.EarlyReturn;
            if (c.Rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, c.ServerId, c.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No memory condition events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = c.ServerName,
                hours_back,
                event_count = c.Rows.Count,
                shown = Math.Min(c.Rows.Count, limit),
                events = c.Rows.Take(limit).Select(r => new
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

    [McpServerTool(Name = "get_health_parser_cpu_tasks"), Description("Gets CPU task events from system_health: long-running CPU-bound tasks, high CPU worker threads, and process utilization snapshots.")]
    public static async Task<string> GetCPUTasks(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        try
        {
            var c = await CollectAsync(postgres, server_name, hours_back, limit, as_of,
                SystemHealthParser.SpServerDiagnosticsEvent,
                xml => One(SystemHealthParser.ParseCpuTasks(xml)),
                SystemHealthSignificance.IsSignificant);
            if (c.EarlyReturn != null) return c.EarlyReturn;
            if (c.Rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, c.ServerId, c.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No CPU task events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = c.ServerName,
                hours_back,
                event_count = c.Rows.Count,
                shown = Math.Min(c.Rows.Count, limit),
                events = c.Rows.Take(limit).Select(r => new
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

    [McpServerTool(Name = "get_health_parser_memory_broker"), Description("Gets memory broker events from system_health: cache shrink/grow notifications, memory clerk adjustments, and broker-mediated memory redistribution.")]
    public static async Task<string> GetMemoryBroker(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        try
        {
            var c = await CollectAsync(postgres, server_name, hours_back, limit, as_of,
                SystemHealthParser.MemoryBrokerEvent,
                xml => One(SystemHealthParser.ParseMemoryBroker(xml)),
                SystemHealthSignificance.IsSignificant);
            if (c.EarlyReturn != null) return c.EarlyReturn;
            if (c.Rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, c.ServerId, c.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No memory broker events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = c.ServerName,
                hours_back,
                event_count = c.Rows.Count,
                shown = Math.Min(c.Rows.Count, limit),
                events = c.Rows.Take(limit).Select(r => new
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

    [McpServerTool(Name = "get_health_parser_memory_node_oom"), Description("Gets memory node OOM events from system_health: out-of-memory conditions on specific NUMA nodes.")]
    public static async Task<string> GetMemoryNodeOOM(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        try
        {
            /* Memory-node OOM is never gated — every recorded OOM is significant (sp_HealthParser applies
               no WHERE filter to this category). */
            var c = await CollectAsync(postgres, server_name, hours_back, limit, as_of,
                SystemHealthParser.MemoryNodeOomEvent,
                xml => One(SystemHealthParser.ParseMemoryNodeOom(xml)),
                SystemHealthSignificance.IsSignificant);
            if (c.EarlyReturn != null) return c.EarlyReturn;
            if (c.Rows.Count == 0)
                return await DarlingEngineCapability.NotCollectedStatusAsync(postgres, c.ServerId, c.ServerName, SystemHealthCollectorName)
                    ?? McpHelpers.Status("empty", "No memory node OOM events found in the requested time range.");

            return JsonSerializer.Serialize(new
            {
                server = c.ServerName,
                hours_back,
                event_count = c.Rows.Count,
                shown = Math.Min(c.Rows.Count, limit),
                events = c.Rows.Take(limit).Select(r => new
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

    [McpServerTool(Name = "get_health_parser_significant_waits"), Description("Gets significant individual waits from system_health: one row per wait_info event where a real session's non-BACKUP statement waited at least 500 ms on a wait type that is not idle/background — the wait type, total and signal duration, the wait resource, the session id and the waiting statement. get_wait_stats gives the instance-wide totals and can never name the statement that paid them; this is the individual waits, with their SQL text.")]
    public static async Task<string> GetSignificantWaits(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to retrieve. Default 24.")] int hours_back = 24,
        [Description("Maximum number of entries. Default 50.")] int limit = 50,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            /*
                Written out rather than routed through CollectAsync because the raw event count is
                load-bearing here: zero significant waits out of a hundred captured events is a healthy
                server, and zero out of zero is a blind one. CollectAsync returns only the surviving rows,
                so the two would arrive indistinguishable.
            */
            var now = windowEnd;
            var xmls = await DarlingSystemHealthReader.ReadEventXmlAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, SystemHealthParser.WaitInfoEvent);

            var rows = xmls
                .Select(SystemHealthParser.ParseSignificantWait)
                .Where(r => r != null && SystemHealthSignificance.IsSignificant(r))
                .Select(r => r!)
                .ToList();

            if (rows.Count == 0)
            {
                /*
                    Three different nothings, and only one of them is good news. Events captured but none
                    significant is the healthy state and costs no extra query -- we already counted them.
                    Nothing captured in the window needs the probe to tell a quiet window from a server
                    whose wait_info has never been collected, because "no significant waits" is exactly
                    what an operator wants to hear and a caller who believes it stops looking.
                */
                if (xmls.Count > 0)
                {
                    return McpHelpers.Status(
                        "empty",
                        $"{xmls.Count} wait_info event(s) were captured for {resolved.ServerName} in the last {hours_back} hour(s) and none was significant (needs a real session, a non-BACKUP statement, at least {SystemHealthSignificance.SignificantWaitMinDurationMs} ms, and a wait type off the idle list). Events ARE being captured, so this is the healthy answer for this read rather than missing data.");
                }

                var everCaptured = await DarlingSystemHealthReader.HasAnyEventOfTypeAsync(
                    postgres, resolved.ServerId, SystemHealthParser.WaitInfoEvent);
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
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, SystemHealthCollectorName)
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

    /* ─────────────────────────── shared read + parse + filter ─────────────────────────── */

    /// <summary>The outcome of resolve + validate + read + shred + significance-filter: either an
    /// <see cref="EarlyReturn"/> string (a resolution error or #1224 validation) the tool returns verbatim,
    /// or the resolved <see cref="ServerId"/>/<see cref="ServerName"/> + the SIGNIFICANT parsed
    /// <see cref="Rows"/>. The id rides along for the #2511 engine-capability probe on the zero-row path —
    /// re-resolving the name there would be a second chance to match a DIFFERENT server, since resolution is
    /// first-wins over a partial.</summary>
    private readonly record struct Collected<T>(string? EarlyReturn, int ServerId, string ServerName, List<T> Rows);

    /// <summary>
    /// Resolves the server, validates hours_back + as_of + limit, reads the raw event_xml for
    /// <paramref name="eventType"/> over the window, shreds each blob with <paramref name="shred"/> (the
    /// reused <see cref="SystemHealthParser"/> — 0..n records per event), and keeps only the rows
    /// <paramref name="significant"/> accepts. The seven gated categories pass their
    /// <see cref="SystemHealthSignificance"/> predicate; System Health passes an EventTime-present
    /// predicate (ungated, matching the viewer's chart read).
    /// </summary>
    private static async Task<Collected<T>> CollectAsync<T>(
        NpgsqlDataSource postgres, string? serverName, int hoursBack, int limit, string? asOf, string eventType,
        Func<string, IEnumerable<T>> shred, Func<T, bool> significant) where T : class
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverName);
        if (error != null) return new Collected<T>(error, 0, "", new List<T>());

        var validation = McpHelpers.ValidateWindow(hoursBack, asOf, out var windowEnd) ?? McpHelpers.ValidateTop(limit);
        if (validation != null) return new Collected<T>(validation, 0, "", new List<T>());

        var now = windowEnd;
        var xmls = await DarlingSystemHealthReader.ReadEventXmlAsync(
            postgres, resolved.ServerId, now.AddHours(-hoursBack), now, eventType);

        var rows = new List<T>();
        foreach (var xml in xmls)
        {
            foreach (var record in shred(xml))
            {
                if (record != null && significant(record))
                    rows.Add(record);
            }
        }

        return new Collected<T>(null, resolved.ServerId, resolved.ServerName, rows);
    }

    /// <summary>Wraps a single-record shred (0-or-1) as the 0..n sequence <see cref="CollectAsync"/> expects.</summary>
    private static IEnumerable<T> One<T>(T? record) where T : class =>
        record is null ? Enumerable.Empty<T>() : new[] { record };
}
