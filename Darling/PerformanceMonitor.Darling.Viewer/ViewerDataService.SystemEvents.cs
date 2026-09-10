/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/*
 * System Events tab reads (system_health parity, Stage 2b). Each warning category is a parse-on-read
 * shred: the data service fetches the raw event_xml for one XE event type over the tab window from
 * v_system_health_events, hands each blob to the Common SystemHealthParser (Stage 2a) to shred into the
 * category record, then keeps only the SIGNIFICANT rows via SystemEventSignificance (Stage 2b,
 * sp_HealthParser's per-category WHERE predicates). Three categories (Memory Conditions, CPU Tasks, I/O
 * Issues) share the single sp_server_diagnostics_component_result feed and split on the event's component.
 * No persisted parsed tables — the raw table + the parser are the whole pipeline. The XML shred is
 * CPU-bound, so it runs off the UI thread (Task.Run), mirroring the deadlock-graph parse.
 *
 * The row view-models below are flat projections of the Common records (all shredded columns preserved,
 * so the grids faithfully match sp_HealthParser's per-category table shape) plus an EventTimeLocal
 * display string — the machine-local render of the event's naive-UTC XE @timestamp, via the same
 * ViewerTimeHelper.ForDisplay the deadlock / blocked-process grids use. Only SevereError adds a
 * resolved DatabaseName (see ResolveDatabaseName).
 */

/// <summary>Shared machine-local render of a naive-UTC event timestamp for the System Events grids.</summary>
internal static class SystemEventRowFormat
{
    public static string Local(DateTime? utc) =>
        utc is { } t ? ViewerTimeHelper.ForDisplay(t).ToString("yyyy-MM-dd HH:mm:ss") : "";
}

/// <summary>One scheduler-monitor WARNING row (Scheduler Issues sub-tab). Mirrors <c>*_SchedulerIssues</c>.</summary>
public sealed class SchedulerIssueRow(SchedulerIssueRecord record)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
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
/// One severe-error row (Severe Errors sub-tab). Mirrors <c>*_SevereErrors</c>. <see cref="DatabaseName"/>
/// is resolved from the store's collected database_id↔database_name mapping (see
/// <see cref="ViewerDataService.ResolveDatabaseName"/>) — Stage 2a's DB-free shred left it null.
/// </summary>
public sealed class SevereErrorRow(SevereErrorRecord record, string databaseName)
{
    public string EventTimeLocal => SystemEventRowFormat.Local(record.EventTime);
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
/// Memory Change), classified via <see cref="SystemEventSignificance.ClassifyDefaultTraceEvent"/>. Unlike the
/// system_health rows, whose <c>event_time</c> is the UTC XE @timestamp, the Default Trace StartTime is the
/// monitored server's LOCAL wall clock — <see cref="ViewerDataService.GetDefaultTraceEventsAsync"/> de-skews
/// it to naive-UTC in SQL (via <c>server_properties.utc_offset_minutes</c>) BEFORE it reaches this row, so
/// <see cref="EventTimeLocal"/> renders through the same <see cref="SystemEventRowFormat.Local"/> as every
/// other System Events grid and sorts consistently with them (the inverse of the MCP reader's bounds
/// conversion; mirrors CorrelatedTimelineLanesControl's CPU-lane de-skew).
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

public sealed partial class ViewerDataService
{
    /// <summary>
    /// Raw event_xml for one XE event type over the tab window, newest first. The System Events reads
    /// window on <c>event_time</c> (the XE <c>@timestamp</c> — the event's real time, which for the ring-
    /// buffer categories can lag when it was collected), not <c>collection_time</c>, so "last 24 hours"
    /// means events that happened in the last 24 hours. Naive-UTC bounds (the store's timestamps are
    /// <c>timestamp without time zone</c>). $1 server_id, $2/$3 window (naive UTC), $4 event_type.
    /// </summary>
    public const string SystemHealthEventsByTypeSql = """
        SELECT
            event_xml
        FROM v_system_health_events
        WHERE server_id = $1
        AND   event_time >= $2
        AND   event_time <= $3
        AND   event_type = $4
        AND   event_xml IS NOT NULL
        ORDER BY event_time DESC
        """;

    /// <summary>
    /// The server's latest database_id↔database_name mapping from the collected size-stats
    /// (<c>DISTINCT ON (database_id)</c> keeps the most-recently-collected name for each id — handles a
    /// dropped-and-recreated id). Feeds <see cref="ResolveDatabaseName"/> for the Severe Errors tab.
    /// <c>database_size_stats</c> is the mapping source because it is the only collected table carrying
    /// BOTH database_id and database_name for every online DB (system + user); <c>database_config</c>
    /// carries the name but no id. $1 server_id.
    /// </summary>
    public const string DatabaseNameMapSql = """
        SELECT DISTINCT ON (database_id)
            database_id,
            database_name
        FROM v_database_size_stats
        WHERE server_id = $1
        ORDER BY database_id, collection_time DESC
        """;

    /// <summary>
    /// Resolves a severe-error <c>database_id</c> to a display name using the collected mapping. A null or
    /// 0 id means "no database context" (error_reported often carries database_id 0; <c>DB_NAME(0)</c> is
    /// NULL server-side too) → empty. A real id absent from the map (a database dropped before the latest
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

    /// <summary>Loads the server's latest database_id → database_name map for Severe Errors DB resolution.</summary>
    public async Task<Dictionary<int, string>> GetDatabaseNameMapAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var map = new Dictionary<int, string>();

        await using var command = _dataSource.CreateCommand(DatabaseNameMapSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.IsDBNull(0) || reader.IsDBNull(1))
                continue;
            map[reader.GetInt32(0)] = reader.GetString(1);
        }

        return map;
    }

    /// <summary>The four System Events parameters ($1 server_id, $2/$3 naive-UTC window, $4 XE event_type).</summary>
    private static void AddSystemEventParameters(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc, string eventType)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = eventType });
    }

    /// <summary>Reads the raw event_xml blobs for one XE event type over the window.</summary>
    private async Task<List<string>> ReadSystemHealthEventXmlAsync(
        int serverId, DateTime startUtc, DateTime endUtc, string eventType, CancellationToken cancellationToken)
    {
        var xmls = new List<string>();

        await using var command = _dataSource.CreateCommand(SystemHealthEventsByTypeSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddSystemEventParameters(command, serverId, startUtc, endUtc, eventType);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
            xmls.Add(reader.GetString(0));

        return xmls;
    }

    // ── Scheduler Issues ──

    /// <summary>Significant scheduler-monitor warnings (WARNING status) for the window, newest first.</summary>
    public async Task<List<SchedulerIssueRow>> GetSchedulerIssuesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.SchedulerMonitorEvent, cancellationToken);

        return await Task.Run(() =>
        {
            var rows = new List<SchedulerIssueRow>();
            foreach (var xml in xmls)
            {
                var record = SystemHealthParser.ParseSchedulerIssue(xml);
                if (record != null && SystemEventSignificance.IsSignificant(record))
                    rows.Add(new SchedulerIssueRow(record));
            }
            return rows;
        }, cancellationToken);
    }

    // ── Severe Errors ──

    /// <summary>
    /// Significant severe errors (severity &gt;= 19, excluding the benign connection-reset numbers) for
    /// the window, newest first, with database_id resolved to a name from the collected mapping.
    /// </summary>
    public async Task<List<SevereErrorRow>> GetSevereErrorsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var mapTask = GetDatabaseNameMapAsync(serverId, cancellationToken);
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.ErrorReportedEvent, cancellationToken);
        var map = await mapTask;

        /* #1319 database filter: severe_errors has no database_name column (the DB is resolved in C# from the
           event's database_id via the collected id↔name map), so the filter is applied client-side on the
           resolved name. A null/empty selection leaves every row (today's behavior). */
        var filter = databaseNames is { Count: > 0 } ? new HashSet<string>(databaseNames, StringComparer.OrdinalIgnoreCase) : null;

        return await Task.Run(() =>
        {
            var rows = new List<SevereErrorRow>();
            foreach (var xml in xmls)
            {
                var record = SystemHealthParser.ParseSevereError(xml);
                if (record != null && SystemEventSignificance.IsSignificant(record))
                {
                    var databaseName = ResolveDatabaseName(record.DatabaseId, map);
                    if (filter == null || filter.Contains(databaseName))
                        rows.Add(new SevereErrorRow(record, databaseName));
                }
            }
            return rows;
        }, cancellationToken);
    }

    // ── Memory Conditions ──

    /// <summary>Significant memory-conditions snapshots (RESOURCE_MEMPHYSICAL_LOW) for the window, newest first.</summary>
    public async Task<List<MemoryConditionsRow>> GetMemoryConditionsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.SpServerDiagnosticsEvent, cancellationToken);

        return await Task.Run(() =>
        {
            var rows = new List<MemoryConditionsRow>();
            foreach (var xml in xmls)
            {
                // Only the RESOURCE component yields a record; other sp_server_diagnostics components parse to null.
                var record = SystemHealthParser.ParseMemoryConditions(xml);
                if (record != null && SystemEventSignificance.IsSignificant(record))
                    rows.Add(new MemoryConditionsRow(record));
            }
            return rows;
        }, cancellationToken);
    }

    // ── Memory Broker ──

    /// <summary>Significant memory-broker ratio changes (RESOURCE_MEMPHYSICAL_LOW) for the window, newest first.</summary>
    public async Task<List<MemoryBrokerRow>> GetMemoryBrokerAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.MemoryBrokerEvent, cancellationToken);

        return await Task.Run(() =>
        {
            var rows = new List<MemoryBrokerRow>();
            foreach (var xml in xmls)
            {
                var record = SystemHealthParser.ParseMemoryBroker(xml);
                if (record != null && SystemEventSignificance.IsSignificant(record))
                    rows.Add(new MemoryBrokerRow(record));
            }
            return rows;
        }, cancellationToken);
    }

    // ── Memory Node OOM ──

    /// <summary>Every memory-node OOM for the window, newest first (this category is never filtered).</summary>
    public async Task<List<MemoryNodeOomRow>> GetMemoryNodeOomAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.MemoryNodeOomEvent, cancellationToken);

        return await Task.Run(() =>
        {
            var rows = new List<MemoryNodeOomRow>();
            foreach (var xml in xmls)
            {
                var record = SystemHealthParser.ParseMemoryNodeOom(xml);
                if (record != null && SystemEventSignificance.IsSignificant(record))
                    rows.Add(new MemoryNodeOomRow(record));
            }
            return rows;
        }, cancellationToken);
    }

    // ── Significant Waits ──

    /// <summary>
    /// Significant individual waits (real session, non-BACKUP statement, duration &gt;= 500 ms, wait type
    /// not on the idle-wait ignore list) for the window, newest first.
    /// </summary>
    public async Task<List<SignificantWaitRow>> GetSignificantWaitsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.WaitInfoEvent, cancellationToken);

        return await Task.Run(() =>
        {
            var rows = new List<SignificantWaitRow>();
            foreach (var xml in xmls)
            {
                var record = SystemHealthParser.ParseSignificantWait(xml);
                if (record != null && SystemEventSignificance.IsSignificant(record))
                    rows.Add(new SignificantWaitRow(record));
            }
            return rows;
        }, cancellationToken);
    }

    // ── CPU Tasks ──

    /// <summary>
    /// Significant CPU-task warnings (QUERY_PROCESSING state WARNING with pendingTasks &gt;= 10) for the
    /// window, newest first. Reads the same sp_server_diagnostics feed as Memory Conditions / I/O Issues;
    /// only the QUERY_PROCESSING component yields a record.
    /// </summary>
    public async Task<List<CpuTasksRow>> GetCpuTasksAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.SpServerDiagnosticsEvent, cancellationToken);

        return await Task.Run(() =>
        {
            var rows = new List<CpuTasksRow>();
            foreach (var xml in xmls)
            {
                // Only the QUERY_PROCESSING component yields a record; other sp_server_diagnostics components parse to null.
                var record = SystemHealthParser.ParseCpuTasks(xml);
                if (record != null && SystemEventSignificance.IsSignificant(record))
                    rows.Add(new CpuTasksRow(record));
            }
            return rows;
        }, cancellationToken);
    }

    // ── I/O Issues ──

    /// <summary>
    /// Significant potential-IO issues (IO_SUBSYSTEM state WARNING) for the window, newest first. Each event
    /// can carry several pending-request files, so the shred fans out to one row per file (durations summed);
    /// only the IO_SUBSYSTEM component yields records.
    /// </summary>
    public async Task<List<IoIssuesRow>> GetIoIssuesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.SpServerDiagnosticsEvent, cancellationToken);

        return await Task.Run(() =>
        {
            var rows = new List<IoIssuesRow>();
            foreach (var xml in xmls)
            {
                // A non-IO_SUBSYSTEM component (or an event with no pending request) shreds to no records.
                foreach (var record in SystemHealthParser.ParseIoIssues(xml))
                {
                    if (SystemEventSignificance.IsSignificant(record))
                        rows.Add(new IoIssuesRow(record));
                }
            }
            return rows;
        }, cancellationToken);
    }

    // ── System Health (Corruption + Contention counter charts) ──

    /// <summary>
    /// Every SYSTEM-component health snapshot for the window, oldest first (time-series order for the
    /// charts). Reads the same sp_server_diagnostics feed as Memory Conditions / CPU Tasks / I/O Issues;
    /// only the SYSTEM component yields a record. Unlike the grid categories this applies NO significance
    /// filter — the Dashboard's Corruption Events + Contention Events tabs plot every collected snapshot's
    /// counters over time, so the viewer returns them all (records with no event time can't sit on a time
    /// axis and are dropped). Both chart sub-tabs read this one list and select their own columns.
    /// </summary>
    public async Task<List<SystemHealthRecord>> GetSystemHealthAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var xmls = await ReadSystemHealthEventXmlAsync(
            serverId, startUtc, endUtc, SystemHealthParser.SpServerDiagnosticsEvent, cancellationToken);

        return await Task.Run(() =>
        {
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
        }, cancellationToken);
    }

    // ── Default Trace (always-on server events; the Default Trace sub-tab) ──

    /// <summary>
    /// Significant Default Trace events for the window, newest first. Reads the BASE <c>default_trace_events</c>
    /// table (no <c>v_*</c> view, like server_properties). The Default Trace StartTime is the monitored
    /// server's LOCAL wall clock, so this DE-SKEWS <c>event_time</c> to naive-UTC in SQL — subtracting the
    /// collected <c>server_properties.utc_offset_minutes</c> (single-row COALESCE CTE, 0 when none yet) — and
    /// windows on that de-skewed UTC value against the naive-UTC bounds ($2/$3), so the returned timestamps
    /// share the same UTC frame as the system_health rows and render/sort consistently on the tab. $1
    /// server_id, $2/$3 window (naive UTC). The ErrorLog severity gate is applied on read
    /// (<see cref="SystemEventSignificance.IsSignificantDefaultTraceEvent"/>), the same significance surface
    /// the tab uses everywhere else.
    /// </summary>
    public const string DefaultTraceEventsByWindowSql = """
        WITH svr AS (
            SELECT COALESCE((
                SELECT sp.utc_offset_minutes
                FROM server_properties AS sp
                WHERE sp.server_id = $1
                AND   sp.utc_offset_minutes IS NOT NULL
                ORDER BY sp.collection_time DESC
                LIMIT 1), 0) AS offset_minutes
        )
        SELECT
            dte.event_time - make_interval(mins => svr.offset_minutes) AS event_time_utc,
            dte.event_name,
            dte.database_name,
            dte.object_name,
            dte.login_name,
            dte.host_name,
            dte.application_name,
            dte.spid,
            dte.duration_us,
            dte.integer_data,
            dte.severity,
            dte.error_number,
            dte.text_data
        FROM default_trace_events AS dte, svr
        WHERE dte.server_id = $1
        AND   dte.event_time - make_interval(mins => svr.offset_minutes) >= $2
        AND   dte.event_time - make_interval(mins => svr.offset_minutes) <= $3
        AND   ($4::text[] IS NULL OR dte.database_name = ANY($4))
        ORDER BY event_time_utc DESC
        """;

    /// <summary>
    /// Significant Default Trace events for the window (server-local StartTime de-skewed to UTC in SQL),
    /// gated by <see cref="SystemEventSignificance.IsSignificantDefaultTraceEvent"/> and tagged with their
    /// category. The gate is cheap (a category + severity check), so it runs inline on the read rather than
    /// off-thread like the XML-shredding system_health categories.
    /// </summary>
    public async Task<List<DefaultTraceEventRow>> GetDefaultTraceEventsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var rows = new List<DefaultTraceEventRow>();

        await using var command = _dataSource.CreateCommand(DefaultTraceEventsByWindowSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var eventTimeUtc = reader.IsDBNull(0) ? (DateTime?)null : reader.GetDateTime(0);
            var eventName = reader.IsDBNull(1) ? null : reader.GetString(1);
            var severity = reader.IsDBNull(10) ? (int?)null : reader.GetInt32(10);

            if (!SystemEventSignificance.IsSignificantDefaultTraceEvent(eventName, severity))
                continue;

            rows.Add(new DefaultTraceEventRow(
                eventTimeUtc,
                SystemEventSignificance.ClassifyDefaultTraceEvent(eventName),
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
