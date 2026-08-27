/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

/*
 * The System Events tab reads (system_health parity) — the Lite (DuckDB) port of the Darling viewer's
 * ViewerDataService.SystemEvents reads. Each warning category is a parse-on-read shred: the reader fetches
 * the raw event_xml for one XE event type over the tab window from v_system_health_events (the archive view
 * UNIONing hot + parquet, so the window includes archived rows), hands each blob to the Common
 * SystemHealthParser to shred into the category record, then keeps only the SIGNIFICANT rows via the shared
 * SystemHealthSignificance (sp_HealthParser's per-category WHERE predicates). Three categories (Memory
 * Conditions, CPU Tasks, I/O Issues) share the single sp_server_diagnostics feed and split on the event's
 * component. No persisted parsed tables — the raw table + the shared parser are the whole pipeline.
 *
 * The XML shred is CPU-bound; the code-behind calls these methods via Task.Run (mirroring the deadlock-graph
 * parse ParseDeadlocksOffUiThreadAsync), so the shred runs off the UI thread. The predicates and Default
 * Trace classifier live ONCE in PerformanceMonitor.Common (SystemHealthSignificance /
 * DefaultTraceEventSignificance) and are called directly here — Lite and Darling share the one source, so
 * the significant set can never drift between them.
 *
 * Time frames (load-bearing): the system_health event_time is the XE @timestamp, which is naive-UTC — those
 * reads window on the UTC bounds from GetTimeRange, and the row VMs render event_time through
 * ServerTimeHelper.FormatServerTime (naive-UTC -> display) exactly like the deadlock / blocked-process grids.
 * The Default Trace event_time is the monitored server's LOCAL StartTime (ft.StartTime, stored raw) — that
 * read windows on the server-LOCAL bounds from GetTimeRangeServerLocal and de-skews each row's server-local
 * event_time to naive-UTC (subtract the connected server's UtcOffsetMinutes) BEFORE the row VM, so a Default
 * Trace row and a system_health row from the same instant render the same wall-clock time and a merged
 * System Events timeline sorts consistently.
 */

/// <summary>Shared render of a naive-UTC event timestamp for the System Events grids — the same
/// ServerTimeHelper.FormatServerTime the deadlock / blocked-process grids use (empty for a null time).</summary>
internal static class SystemEventRowFormat
{
    public static string Local(DateTime? utc) => ServerTimeHelper.FormatServerTime(utc, "yyyy-MM-dd HH:mm:ss");
}

/// <summary>One scheduler-monitor WARNING row (Scheduler Issues sub-tab). Mirrors sp_HealthParser's <c>*_SchedulerIssues</c>.</summary>
public sealed class SchedulerIssueRow(SchedulerIssueRecord record)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
    /// <summary>Raw naive-UTC event time (the XE @timestamp) for the MCP layer's ISO output.</summary>
    public DateTime? EventTime => record.EventTime;
    public int? SchedulerId => record.SchedulerId;
    public int? CpuId => record.CpuId;
    public string? Status => record.Status;
    public bool? IsOnline => record.IsOnline;
    public bool? IsRunnable => record.IsRunnable;
    public bool? IsRunning => record.IsRunning;
    public long? NonYieldingTimeMs => record.NonYieldingTimeMs;
    public long? ThreadQuantumMs => record.ThreadQuantumMs;
}

/// <summary>
/// One severe-error row (Severe Errors sub-tab). Mirrors sp_HealthParser's <c>*_SevereErrors</c>.
/// <see cref="DatabaseName"/> is resolved from the store's collected database_id -> database_name mapping
/// (see <see cref="LocalDataService.ResolveDatabaseName"/>) — the pure shred left it null.
/// </summary>
public sealed class SevereErrorRow(SevereErrorRecord record, string databaseName)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
    /// <summary>Raw naive-UTC event time (the XE @timestamp) for the MCP layer's ISO output.</summary>
    public DateTime? EventTime => record.EventTime;
    public int? ErrorNumber => record.ErrorNumber;
    public int? Severity => record.Severity;
    public int? State => record.State;
    public int? DatabaseId => record.DatabaseId;
    public string DatabaseName => databaseName;
    public string? Message => record.Message;
}

/// <summary>One RESOURCE_MEMPHYSICAL_LOW memory-conditions snapshot (Memory Conditions sub-tab). Mirrors <c>*_MemoryConditions</c>.</summary>
public sealed class MemoryConditionsRow(MemoryConditionsRecord record)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
    /// <summary>Raw naive-UTC event time (the XE @timestamp) for the MCP layer's ISO output.</summary>
    public DateTime? EventTime => record.EventTime;
    public string? LastNotification => record.LastNotification;
    public long? OutOfMemoryExceptions => record.OutOfMemoryExceptions;
    public bool? IsAnyPoolOutOfMemory => record.IsAnyPoolOutOfMemory;
    public long? ProcessOutOfMemoryPeriod => record.ProcessOutOfMemoryPeriod;
    public string? Name => record.Name;
    public long? AvailablePhysicalMemoryGb => record.AvailablePhysicalMemoryGb;
    public long? AvailableVirtualMemoryGb => record.AvailableVirtualMemoryGb;
    public long? AvailablePagingFileGb => record.AvailablePagingFileGb;
    public long? WorkingSetGb => record.WorkingSetGb;
    public long? PercentOfCommittedMemoryInWs => record.PercentOfCommittedMemoryInWs;
    public long? PageFaults => record.PageFaults;
    public long? SystemPhysicalMemoryHigh => record.SystemPhysicalMemoryHigh;
    public long? SystemPhysicalMemoryLow => record.SystemPhysicalMemoryLow;
    public long? ProcessPhysicalMemoryLow => record.ProcessPhysicalMemoryLow;
    public long? ProcessVirtualMemoryLow => record.ProcessVirtualMemoryLow;
    public long? VmReservedGb => record.VmReservedGb;
    public long? VmCommittedGb => record.VmCommittedGb;
    public long? LockedPagesAllocated => record.LockedPagesAllocated;
    public long? LargePagesAllocated => record.LargePagesAllocated;
    public long? EmergencyMemoryGb => record.EmergencyMemoryGb;
    public long? EmergencyMemoryInUseGb => record.EmergencyMemoryInUseGb;
    public long? TargetCommittedGb => record.TargetCommittedGb;
    public long? CurrentCommittedGb => record.CurrentCommittedGb;
    public long? PagesAllocated => record.PagesAllocated;
    public long? PagesReserved => record.PagesReserved;
    public long? PagesFree => record.PagesFree;
    public long? PagesInUse => record.PagesInUse;
    public long? PageAllocPotential => record.PageAllocPotential;
    public long? NumaGrowthPhase => record.NumaGrowthPhase;
    public long? LastOomFactor => record.LastOomFactor;
    public long? LastOsError => record.LastOsError;
}

/// <summary>One RESOURCE_MEMPHYSICAL_LOW memory-broker ratio change (Memory Broker sub-tab). Mirrors <c>*_MemoryBroker</c>.</summary>
public sealed class MemoryBrokerRow(MemoryBrokerRecord record)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
    /// <summary>Raw naive-UTC event time (the XE @timestamp) for the MCP layer's ISO output.</summary>
    public DateTime? EventTime => record.EventTime;
    public long? BrokerId => record.BrokerId;
    public long? PoolMetadataId => record.PoolMetadataId;
    public long? DeltaTime => record.DeltaTime;
    public long? MemoryRatio => record.MemoryRatio;
    public long? NewTarget => record.NewTarget;
    public long? Overall => record.Overall;
    public long? Rate => record.Rate;
    public long? CurrentlyPredicated => record.CurrentlyPredicated;
    public long? CurrentlyAllocated => record.CurrentlyAllocated;
    public long? PreviouslyAllocated => record.PreviouslyAllocated;
    public string? Broker => record.Broker;
    public string? Notification => record.Notification;
}

/// <summary>One memory-node OOM row (Memory Node OOM sub-tab). Mirrors <c>*_MemoryNodeOOM</c> (never gated — every OOM shows).</summary>
public sealed class MemoryNodeOomRow(MemoryNodeOomRecord record)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
    /// <summary>Raw naive-UTC event time (the XE @timestamp) for the MCP layer's ISO output.</summary>
    public DateTime? EventTime => record.EventTime;
    public long? NodeId => record.NodeId;
    public long? MemoryNodeId => record.MemoryNodeId;
    public long? MemoryUtilizationPct => record.MemoryUtilizationPct;
    public long? TotalPhysicalMemoryKb => record.TotalPhysicalMemoryKb;
    public long? AvailablePhysicalMemoryKb => record.AvailablePhysicalMemoryKb;
    public long? TotalPageFileKb => record.TotalPageFileKb;
    public long? AvailablePageFileKb => record.AvailablePageFileKb;
    public long? TotalVirtualAddressSpaceKb => record.TotalVirtualAddressSpaceKb;
    public long? AvailableVirtualAddressSpaceKb => record.AvailableVirtualAddressSpaceKb;
    public long? TargetKb => record.TargetKb;
    public long? ReservedKb => record.ReservedKb;
    public long? CommittedKb => record.CommittedKb;
    public decimal? SharedCommittedKb => record.SharedCommittedKb;
    public long? AweKb => record.AweKb;
    public long? PagesKb => record.PagesKb;
    public string? FailureType => record.FailureType;
    public long? FailureValue => record.FailureValue;
    public long? Resources => record.Resources;
    public string? FactorText => record.FactorText;
    public long? FactorValue => record.FactorValue;
    public long? LastError => record.LastError;
    public long? PoolMetadataId => record.PoolMetadataId;
    public string? IsProcessInJob => record.IsProcessInJob;
    public string? IsSystemPhysicalMemoryHigh => record.IsSystemPhysicalMemoryHigh;
    public string? IsSystemPhysicalMemoryLow => record.IsSystemPhysicalMemoryLow;
    public string? IsProcessPhysicalMemoryLow => record.IsProcessPhysicalMemoryLow;
    public string? IsProcessVirtualMemoryLow => record.IsProcessVirtualMemoryLow;
}

/// <summary>One significant-wait row (Significant Waits sub-tab). Mirrors <c>*_SignificantWaits</c>.</summary>
public sealed class SignificantWaitRow(SignificantWaitRecord record)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
    /// <summary>Raw naive-UTC event time (the XE @timestamp) for the MCP layer's ISO output.</summary>
    public DateTime? EventTime => record.EventTime;
    public string? WaitType => record.WaitType;
    public long? DurationMs => record.DurationMs;
    public long? SignalDurationMs => record.SignalDurationMs;
    public string? WaitResource => record.WaitResource;
    public int? SessionId => record.SessionId;
    public string? QueryText => record.QueryText;
}

/// <summary>One CPU-task-details row (CPU Tasks sub-tab). Mirrors <c>*_CPUTasks</c> (QUERY_PROCESSING component).</summary>
public sealed class CpuTasksRow(CpuTasksRecord record)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
    /// <summary>Raw naive-UTC event time (the XE @timestamp) for the MCP layer's ISO output.</summary>
    public DateTime? EventTime => record.EventTime;
    public string? State => record.State;
    public long? MaxWorkers => record.MaxWorkers;
    public long? WorkersCreated => record.WorkersCreated;
    public long? WorkersIdle => record.WorkersIdle;
    public long? TasksCompletedWithinInterval => record.TasksCompletedWithinInterval;
    public long? PendingTasks => record.PendingTasks;
    public long? OldestPendingTaskWaitingTime => record.OldestPendingTaskWaitingTime;
    public bool? HasUnresolvableDeadlockOccurred => record.HasUnresolvableDeadlockOccurred;
    public bool? HasDeadlockedSchedulersOccurred => record.HasDeadlockedSchedulersOccurred;
    public bool? DidBlockingOccur => record.DidBlockingOccur;
}

/// <summary>One potential-IO-issue row (I/O Issues sub-tab). Mirrors <c>*_IOIssues</c> (IO_SUBSYSTEM component).</summary>
public sealed class IoIssuesRow(IoIssuesRecord record)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
    /// <summary>Raw naive-UTC event time (the XE @timestamp) for the MCP layer's ISO output.</summary>
    public DateTime? EventTime => record.EventTime;
    public string? State => record.State;
    public long? IoLatchTimeouts => record.IoLatchTimeouts;
    public long? IntervalLongIos => record.IntervalLongIos;
    public long? TotalLongIos => record.TotalLongIos;
    public long? LongestPendingRequestsDurationMs => record.LongestPendingRequestsDurationMs;
    public string? LongestPendingRequestsFilePath => record.LongestPendingRequestsFilePath;
}

/// <summary>
/// One significant Default Trace event (the Default Trace sub-tab) — the always-on server events no DMV
/// captures (file auto-grow/shrink stalls, severe ErrorLog writes, schema DDL, security audits, Server
/// Memory Change), classified via the shared <see cref="DefaultTraceEventSignificance.Classify"/>. Unlike
/// the system_health rows (whose event_time is the UTC XE @timestamp), the Default Trace StartTime is the
/// monitored server's LOCAL wall clock, so <see cref="LocalDataService.GetDefaultTraceEventsAsync"/> de-skews
/// it to naive-UTC BEFORE this row, so <see cref="EventTimeLocal"/> renders through the same
/// <see cref="SystemEventRowFormat.Local"/> as every other System Events grid and sorts consistently.
/// </summary>
public sealed class DefaultTraceEventRow
{
    public DefaultTraceEventRow(
        DateTime? eventTimeUtc,
        DefaultTraceEventCategory category,
        string? eventName,
        string? databaseName,
        string? objectName,
        string? loginName,
        string? hostName,
        string? applicationName,
        int? spid,
        long? durationUs,
        long? integerData,
        int? severity,
        int? errorNumber,
        string? textData)
    {
        EventTimeUtc = eventTimeUtc;
        EventTimeLocal = SystemEventRowFormat.Local(eventTimeUtc);
        Category = category.ToString();
        EventName = eventName;
        DatabaseName = databaseName;
        ObjectName = objectName;
        LoginName = loginName;
        HostName = hostName;
        ApplicationName = applicationName;
        Spid = spid;
        /* Duration (ms) is meaningful for the auto-grow/shrink stalls; growth (MB) is the 8-KB page count on
           those (IntegerData) converted, and only meaningful for that category. */
        DurationMs = durationUs.HasValue ? durationUs.Value / 1000.0m : (decimal?)null;
        GrowthMb = category == DefaultTraceEventCategory.AutoGrowShrink && integerData.HasValue
            ? integerData.Value * 8.0m / 1024.0m
            : (decimal?)null;
        Severity = severity;
        ErrorNumber = errorNumber;
        TextData = textData;
    }

    /// <summary>Raw naive-UTC event time (de-skewed server-local StartTime) for the MCP layer's ISO output.</summary>
    public DateTime? EventTimeUtc { get; }
    public string EventTimeLocal { get; }
    public string Category { get; }
    public string? EventName { get; }
    public string? DatabaseName { get; }
    public string? ObjectName { get; }
    public string? LoginName { get; }
    public string? HostName { get; }
    public string? ApplicationName { get; }
    public int? Spid { get; }
    public decimal? DurationMs { get; }
    public decimal? GrowthMb { get; }
    public int? Severity { get; }
    public int? ErrorNumber { get; }
    public string? TextData { get; }
}

public partial class LocalDataService
{
    /// <summary>
    /// Raw event_xml for one XE event type over the tab window, newest first, from the v_system_health_events
    /// archive view (hot UNION parquet). The System Events reads window on <c>event_time</c> (the XE
    /// <c>@timestamp</c> — the event's real time, which for the ring-buffer categories can lag when it was
    /// collected), not <c>collection_time</c>, so "last 24 hours" means events that happened in the last 24
    /// hours. The XE @timestamp is naive-UTC, so the UTC bounds from GetTimeRange line up.
    /// </summary>
    private async Task<List<string>> ReadSystemHealthEventXmlAsync(
        int serverId, DateTime startUtc, DateTime endUtc, string eventType)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    event_xml
FROM v_system_health_events
WHERE server_id = $1
AND   event_time >= $2
AND   event_time <= $3
AND   event_type = $4
AND   event_xml IS NOT NULL
ORDER BY event_time DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });
        command.Parameters.Add(new DuckDBParameter { Value = eventType });

        var xmls = new List<string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            xmls.Add(reader.GetString(0));

        return xmls;
    }

    // ── Scheduler Issues ──

    /// <summary>Significant scheduler-monitor warnings (WARNING status) for the window, newest first.</summary>
    public async Task<List<SchedulerIssueRow>> GetSchedulerIssuesAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetSchedulerIssuesAsync", "v_system_health_events scheduler_monitor shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.SchedulerMonitorEvent);

        var rows = new List<SchedulerIssueRow>();
        foreach (var xml in xmls)
        {
            var record = SystemHealthParser.ParseSchedulerIssue(xml);
            if (record != null && SystemHealthSignificance.IsSignificant(record))
                rows.Add(new SchedulerIssueRow(record));
        }
        return rows;
    }

    // ── Severe Errors ──

    /// <summary>
    /// Significant severe errors (severity &gt;= 19, excluding the benign connection-reset numbers) for the
    /// window, newest first, with database_id resolved to a name from the collected mapping. #1319: severe
    /// errors have no database_name column (the DB is resolved in C# from the event's database_id via the
    /// collected id -> name map), so the global database filter is applied client-side on the resolved name.
    /// </summary>
    public async Task<List<SevereErrorRow>> GetSevereErrorsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetSevereErrorsAsync", "v_system_health_events error_reported shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var map = await GetDatabaseNameMapAsync(serverId);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.ErrorReportedEvent);

        var filter = databaseNames is { Count: > 0 } ? new HashSet<string>(databaseNames, StringComparer.OrdinalIgnoreCase) : null;

        var rows = new List<SevereErrorRow>();
        foreach (var xml in xmls)
        {
            var record = SystemHealthParser.ParseSevereError(xml);
            if (record != null && SystemHealthSignificance.IsSignificant(record))
            {
                var databaseName = ResolveDatabaseName(record.DatabaseId, map);
                if (filter == null || filter.Contains(databaseName))
                    rows.Add(new SevereErrorRow(record, databaseName));
            }
        }
        return rows;
    }

    // ── Memory Conditions ──

    /// <summary>Significant memory-conditions snapshots (RESOURCE_MEMPHYSICAL_LOW) for the window, newest first.</summary>
    public async Task<List<MemoryConditionsRow>> GetMemoryConditionsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetMemoryConditionsAsync", "v_system_health_events sp_server_diagnostics RESOURCE shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.SpServerDiagnosticsEvent);

        var rows = new List<MemoryConditionsRow>();
        foreach (var xml in xmls)
        {
            // Only the RESOURCE component yields a record; other sp_server_diagnostics components parse to null.
            var record = SystemHealthParser.ParseMemoryConditions(xml);
            if (record != null && SystemHealthSignificance.IsSignificant(record))
                rows.Add(new MemoryConditionsRow(record));
        }
        return rows;
    }

    // ── Memory Broker ──

    /// <summary>Significant memory-broker ratio changes (RESOURCE_MEMPHYSICAL_LOW) for the window, newest first.</summary>
    public async Task<List<MemoryBrokerRow>> GetMemoryBrokerAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetMemoryBrokerAsync", "v_system_health_events memory_broker shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.MemoryBrokerEvent);

        var rows = new List<MemoryBrokerRow>();
        foreach (var xml in xmls)
        {
            var record = SystemHealthParser.ParseMemoryBroker(xml);
            if (record != null && SystemHealthSignificance.IsSignificant(record))
                rows.Add(new MemoryBrokerRow(record));
        }
        return rows;
    }

    // ── Memory Node OOM ──

    /// <summary>Every memory-node OOM for the window, newest first (this category is never filtered).</summary>
    public async Task<List<MemoryNodeOomRow>> GetMemoryNodeOomAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetMemoryNodeOomAsync", "v_system_health_events memory_node_oom shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.MemoryNodeOomEvent);

        var rows = new List<MemoryNodeOomRow>();
        foreach (var xml in xmls)
        {
            var record = SystemHealthParser.ParseMemoryNodeOom(xml);
            if (record != null && SystemHealthSignificance.IsSignificant(record))
                rows.Add(new MemoryNodeOomRow(record));
        }
        return rows;
    }

    // ── Significant Waits ──

    /// <summary>
    /// Significant individual waits (real session, non-BACKUP statement, duration &gt;= 500 ms, wait type not
    /// on the idle-wait ignore list) for the window, newest first.
    /// </summary>
    public async Task<List<SignificantWaitRow>> GetSignificantWaitsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null) =>
        (await GetSignificantWaitsWithCaptureAsync(serverId, hoursBack, fromDate, toDate)).Rows;

    /// <summary>
    /// The same shred as <see cref="GetSignificantWaitsAsync"/>, plus the number of wait_info events that
    /// were CAPTURED in the window before the significance gate ran.
    /// <para>The count is what lets an empty answer say which nothing it is. Zero significant waits out of a
    /// hundred captured events is a healthy server; zero out of zero is a blind one, and the surviving rows
    /// alone cannot tell them apart. Returned from the one read rather than recovered by a second COUNT, so
    /// the two numbers can never describe different windows. Darling gets the same count for free from its
    /// own reader.</para>
    /// </summary>
    public async Task<(List<SignificantWaitRow> Rows, int CapturedCount)> GetSignificantWaitsWithCaptureAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetSignificantWaitsAsync", "v_system_health_events wait_info shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.WaitInfoEvent);

        var rows = new List<SignificantWaitRow>();
        foreach (var xml in xmls)
        {
            var record = SystemHealthParser.ParseSignificantWait(xml);
            if (record != null && SystemHealthSignificance.IsSignificant(record))
                rows.Add(new SignificantWaitRow(record));
        }
        return (rows, xmls.Count);
    }

    /// <summary>
    /// Whether this server has EVER captured a system_health event of one type, ignoring any window.
    /// <para>Separates a quiet window from a blind one on the empty path. Reads
    /// <c>v_system_health_events</c> - the SAME source <see cref="ReadSystemHealthEventXmlAsync"/> uses, so
    /// it cannot report a server as captured for rows the read itself can never see - and is scoped to the
    /// event_type, because a server capturing sp_server_diagnostics but no wait_info has not been sampled
    /// for waits whatever its other categories hold. Darling twin:
    /// <c>DarlingSystemHealthReader.HasAnyEventOfTypeAsync</c>; the two must stay in step so a user moving
    /// between the SKUs is not told a different story about the same state.</para>
    /// </summary>
    public async Task<bool> HasAnySystemHealthEventOfTypeAsync(int serverId, string eventType)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_system_health_events
WHERE server_id = $1
AND   event_type = $2
AND   event_xml IS NOT NULL
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = eventType });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    // ── CPU Tasks ──

    /// <summary>
    /// Significant CPU-task warnings (QUERY_PROCESSING state WARNING with pendingTasks &gt;= 10) for the
    /// window, newest first. Reads the same sp_server_diagnostics feed as Memory Conditions / I/O Issues;
    /// only the QUERY_PROCESSING component yields a record.
    /// </summary>
    public async Task<List<CpuTasksRow>> GetCpuTasksAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetCpuTasksAsync", "v_system_health_events sp_server_diagnostics QUERY_PROCESSING shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.SpServerDiagnosticsEvent);

        var rows = new List<CpuTasksRow>();
        foreach (var xml in xmls)
        {
            // Only the QUERY_PROCESSING component yields a record; other sp_server_diagnostics components parse to null.
            var record = SystemHealthParser.ParseCpuTasks(xml);
            if (record != null && SystemHealthSignificance.IsSignificant(record))
                rows.Add(new CpuTasksRow(record));
        }
        return rows;
    }

    // ── I/O Issues ──

    /// <summary>
    /// Significant potential-IO issues (IO_SUBSYSTEM state WARNING) for the window, newest first. Each event
    /// can carry several pending-request files, so the shred fans out to one row per file (durations summed);
    /// only the IO_SUBSYSTEM component yields records.
    /// </summary>
    public async Task<List<IoIssuesRow>> GetIoIssuesAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetIoIssuesAsync", "v_system_health_events sp_server_diagnostics IO_SUBSYSTEM shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.SpServerDiagnosticsEvent);

        var rows = new List<IoIssuesRow>();
        foreach (var xml in xmls)
        {
            // A non-IO_SUBSYSTEM component (or an event with no pending request) shreds to no records.
            foreach (var record in SystemHealthParser.ParseIoIssues(xml))
            {
                if (SystemHealthSignificance.IsSignificant(record))
                    rows.Add(new IoIssuesRow(record));
            }
        }
        return rows;
    }

    // ── System Health (Corruption + Contention counter charts) ──

    /// <summary>
    /// Every SYSTEM-component health snapshot for the window, oldest first (time-series order for the charts).
    /// Reads the same sp_server_diagnostics feed as Memory Conditions / CPU Tasks / I/O Issues; only the
    /// SYSTEM component yields a record. Unlike the grid categories this applies NO significance filter — the
    /// Corruption Events + Contention Events chart sub-tabs plot every collected snapshot's counters over
    /// time, so this returns them all (records with no event time can't sit on a time axis and are dropped).
    /// Both chart sub-tabs read this one list and select their own columns.
    /// </summary>
    public async Task<List<SystemHealthRecord>> GetSystemHealthAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetSystemHealthAsync", "v_system_health_events sp_server_diagnostics SYSTEM shred");
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var xmls = await ReadSystemHealthEventXmlAsync(serverId, startTime, endTime, SystemHealthParser.SpServerDiagnosticsEvent);

        var records = new List<SystemHealthRecord>();
        foreach (var xml in xmls)
        {
            // Only the SYSTEM component yields a record; other sp_server_diagnostics components parse to null.
            var record = SystemHealthParser.ParseSystemHealth(xml);
            if (record != null && record.EventTime.HasValue)
                records.Add(record);
        }
        records.Sort((a, b) => Nullable.Compare(a.EventTime, b.EventTime));
        return records;
    }

    // ── Severe-Errors database name resolution ──

    /// <summary>
    /// The server's latest database_id -> database_name mapping from the collected size-stats (the newest
    /// name per id — handles a dropped-and-recreated id). DuckDB has no <c>DISTINCT ON</c>, so this uses
    /// <c>QUALIFY ROW_NUMBER() OVER (PARTITION BY database_id ORDER BY collection_time DESC) = 1</c> — the
    /// same idiom the archive views' dedup uses. database_size_stats is the mapping source because it is the
    /// only collected table carrying BOTH database_id and database_name for every online DB.
    /// </summary>
    public async Task<Dictionary<int, string>> GetDatabaseNameMapAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    database_id,
    database_name
FROM v_database_size_stats
WHERE server_id = $1
QUALIFY ROW_NUMBER() OVER (PARTITION BY database_id ORDER BY collection_time DESC) = 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        var map = new Dictionary<int, string>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (reader.IsDBNull(0) || reader.IsDBNull(1))
                continue;
            map[reader.GetInt32(0)] = reader.GetString(1);
        }
        return map;
    }

    /// <summary>
    /// Resolves a severe-error <c>database_id</c> to a display name using the collected mapping. A null or 0
    /// id means "no database context" (error_reported often carries database_id 0; <c>DB_NAME(0)</c> is NULL
    /// server-side too) -> empty. A real id absent from the map (a database dropped before the latest
    /// size-stats snapshot, or one never captured) is surfaced as its raw id rather than silently blanked.
    /// </summary>
    public static string ResolveDatabaseName(int? databaseId, IReadOnlyDictionary<int, string> databaseNameMap)
    {
        if (databaseId is not { } id || id == 0)
            return string.Empty;
        if (databaseNameMap.TryGetValue(id, out var name))
            return name;
        return $"database_id {id}";
    }

    // ── Default Trace (always-on server events; the Default Trace sub-tab) ──

    /// <summary>
    /// Significant Default Trace events for the window, newest first, from the v_default_trace_events archive
    /// view. The Default Trace StartTime is the monitored server's LOCAL wall clock (ft.StartTime, stored
    /// raw), so this windows on the server-LOCAL bounds from <see cref="GetTimeRangeServerLocal"/> and
    /// de-skews each row's server-local event_time to naive-UTC (subtracting the connected server's
    /// <see cref="ServerTimeHelper.UtcOffsetMinutes"/>) BEFORE the row VM, so the returned timestamps share
    /// the same UTC frame as the system_health rows and render/sort consistently on the tab. #1319: the
    /// global database filter is pushed into SQL on <c>database_name</c>. The ErrorLog severity gate is
    /// applied on read via the shared <see cref="DefaultTraceEventSignificance"/>.
    /// </summary>
    public async Task<List<DefaultTraceEventRow>> GetDefaultTraceEventsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetDefaultTraceEventsAsync", "v_default_trace_events significant-set read");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* Default Trace event_time is server-LOCAL, so window on server-local bounds (the same helper the
           CPU/sample_time reads use). Bind db filter params immediately after the 3 fixed params ($4+). */
        var (startTime, endTime) = GetTimeRangeServerLocal(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
    event_time,
    event_name,
    database_name,
    object_name,
    login_name,
    host_name,
    application_name,
    spid,
    duration_us,
    integer_data,
    severity,
    error_number,
    text_data
FROM v_default_trace_events
WHERE server_id = $1
AND   event_time >= $2
AND   event_time <= $3" + dbClause + @"
ORDER BY event_time DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var offset = ServerTimeHelper.UtcOffsetMinutes;

        var rows = new List<DefaultTraceEventRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            /* De-skew server-local StartTime -> naive-UTC so the row shares the system_health rows' UTC frame. */
            var eventTimeUtc = reader.IsDBNull(0) ? (DateTime?)null : reader.GetDateTime(0).AddMinutes(-offset);
            var eventName = reader.IsDBNull(1) ? null : reader.GetString(1);
            var severity = reader.IsDBNull(10) ? (int?)null : reader.GetInt32(10);

            if (!DefaultTraceEventSignificance.IsSignificant(eventName, severity))
                continue;

            rows.Add(new DefaultTraceEventRow(
                eventTimeUtc,
                DefaultTraceEventSignificance.Classify(eventName),
                eventName,
                reader.IsDBNull(2) ? null : reader.GetString(2),   /* database_name */
                reader.IsDBNull(3) ? null : reader.GetString(3),   /* object_name */
                reader.IsDBNull(4) ? null : reader.GetString(4),   /* login_name */
                reader.IsDBNull(5) ? null : reader.GetString(5),   /* host_name */
                reader.IsDBNull(6) ? null : reader.GetString(6),   /* application_name */
                reader.IsDBNull(7) ? (int?)null : reader.GetInt32(7),   /* spid */
                reader.IsDBNull(8) ? (long?)null : reader.GetInt64(8),  /* duration_us */
                reader.IsDBNull(9) ? (long?)null : reader.GetInt64(9),  /* integer_data */
                severity,
                reader.IsDBNull(11) ? (int?)null : reader.GetInt32(11), /* error_number */
                reader.IsDBNull(12) ? null : reader.GetString(12)));    /* text_data */
        }

        return rows;
    }
}
