/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the System Events tab (system_health parity, Stage 2b) against the Darling store contract, no live
/// Postgres. The Common <see cref="SystemHealthParser"/> shred is already tested; these cover the two things
/// Stage 2b adds: (1) the data-service reads — the load-bearing SQL clauses and column parity against the
/// generated collector DDL — and (2) <see cref="SystemEventSignificance"/>, the per-category
/// "significant / warning" filter that re-applies sp_HealthParser's WHERE predicates at the product's
/// canonical parameters (@warnings_only = 1, @wait_duration_ms = 500), plus the Severe-Errors database_id
/// resolution and the machine-local event-time projection.
/// </summary>
public sealed class ViewerSystemEventsTests
{
    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    // ── Reads: SQL clause pins + column parity ──

    [Fact]
    public void SystemHealthEventsByTypeSql_ReadsViewByServerTypeAndEventTimeWindow()
    {
        var sql = ViewerDataService.SystemHealthEventsByTypeSql;

        Assert.Contains("FROM v_system_health_events", sql, StringComparison.Ordinal);
        Assert.Contains("event_xml", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        /* Windows on event_time (the XE @timestamp — the event's real time), NOT collection_time, and on the
           category's own event_type ($4), newest first. */
        Assert.Contains("event_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("event_time <= $3", sql, StringComparison.Ordinal);
        Assert.Contains("event_type = $4", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY event_time DESC", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(ViewerDataService.SystemHealthEventsByTypeSql))]
    public void SystemEventsReads_PgDialect_PositionalParams_NoTSqlNamedParams(string sqlName)
    {
        var sql = (string)typeof(ViewerDataService).GetField(sqlName)!.GetValue(null)!;

        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("now(", sql.ToLowerInvariant());
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void SystemHealthEventsRead_ReferencesColumnsThatExistInTheGeneratedTable()
    {
        var ddl = PgSchemaGenerator.CreateTable(SystemHealthEventsCollector.Instance);
        Assert.Equal("system_health_events", SystemHealthEventsCollector.Instance.TargetTable);

        foreach (var col in new[] { "event_xml", "event_type", "event_time", "collection_time", "server_id" })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DatabaseNameMapRead_ReferencesColumnsThatExistInTheGeneratedTable()
    {
        var ddl = PgSchemaGenerator.CreateTable(DatabaseSizeStatsCollector.Instance);
        Assert.Equal("database_size_stats", DatabaseSizeStatsCollector.Instance.TargetTable);

        /* database_size_stats is the mapping source precisely because it carries BOTH id and name. */
        foreach (var col in new[] { "database_id", "database_name", "collection_time", "server_id" })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    // ── Significance: Scheduler Issues (SQL CPU >= 90, other-process CPU >= 50, or memory <= 50) ──

    [Theory]
    [InlineData(90, 0, 100, true)]
    [InlineData(89, 0, 100, false)]
    [InlineData(0, 50, 100, true)]
    [InlineData(0, 49, 100, false)]
    [InlineData(0, 0, 50, true)]
    [InlineData(0, 0, 51, false)]
    [InlineData(null, null, null, false)]
    public void SchedulerIssue_SignificantOnThresholds(int? sqlCpu, int? other, int? mem, bool expected) =>
        Assert.Equal(expected, SystemEventSignificance.IsSignificant(new SchedulerIssueRecord { SqlCpuUtilization = sqlCpu, OtherProcessCpu = other, MemoryUtilization = mem }));

    [Fact]
    public void SchedulerIssue_Fixture_IsSignificant()
    {
        var record = SystemHealthParser.ParseSchedulerIssue(LoadFixture("scheduler_monitor_high_sql_cpu.xml"))!;
        Assert.Equal(94, record.SqlCpuUtilization);
        Assert.True(SystemEventSignificance.IsSignificant(record));
    }

    // ── Significance: Severe Errors (severity >= 19, excluding 17830 / 18056) ──

    [Theory]
    [InlineData(24, 823, true)]     // fixture case
    [InlineData(19, 50000, true)]   // boundary — 19 is kept
    [InlineData(18, 50000, false)]  // below the warnings_only cutoff
    [InlineData(16, 50000, false)]  // the always-on floor, but not "warning"
    [InlineData(25, 17830, false)]  // severe but on the ignore list
    [InlineData(20, 18056, false)]  // severe but on the ignore list
    public void SevereError_SignificantByCutoffAndIgnoreList(int severity, int errorNumber, bool expected) =>
        Assert.Equal(expected, SystemEventSignificance.IsSignificant(
            new SevereErrorRecord { Severity = severity, ErrorNumber = errorNumber }));

    [Fact]
    public void SevereError_NullSeverity_IsNotSignificant() =>
        Assert.False(SystemEventSignificance.IsSignificant(new SevereErrorRecord { Severity = null, ErrorNumber = 823 }));

    [Fact]
    public void SevereError_HighSeverity_NullErrorNumber_IsSignificant() =>
        Assert.True(SystemEventSignificance.IsSignificant(new SevereErrorRecord { Severity = 20, ErrorNumber = null }));

    [Fact]
    public void SevereError_Fixture_IsSignificant()
    {
        var record = SystemHealthParser.ParseSevereError(LoadFixture("error_reported.xml"))!;
        Assert.True(SystemEventSignificance.IsSignificant(record));
    }

    // ── Significance: Memory Conditions / Memory Broker (RESOURCE_MEMPHYSICAL_LOW) ──

    [Theory]
    [InlineData("RESOURCE_MEMPHYSICAL_LOW", true)]
    [InlineData("RESOURCE_MEMPHYSICAL_HIGH", false)]
    [InlineData(null, false)]
    public void MemoryConditions_SignificantOnlyWhenLow(string? lastNotification, bool expected) =>
        Assert.Equal(expected, SystemEventSignificance.IsSignificant(
            new MemoryConditionsRecord { LastNotification = lastNotification }));

    [Theory]
    [InlineData("RESOURCE_MEMPHYSICAL_LOW", true)]
    [InlineData("RESOURCE_MEMPHYSICAL_HIGH", false)]
    [InlineData(null, false)]
    public void MemoryBroker_SignificantOnlyWhenLow(string? notification, bool expected) =>
        Assert.Equal(expected, SystemEventSignificance.IsSignificant(
            new MemoryBrokerRecord { Notification = notification }));

    [Fact]
    public void MemoryConditions_HighFixture_IsNotSignificant()
    {
        // The representative fixture is RESOURCE_MEMPHYSICAL_HIGH, so the warnings_only filter drops it.
        var record = SystemHealthParser.ParseMemoryConditions(LoadFixture("sp_server_diagnostics_resource.xml"))!;
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", record.LastNotification);
        Assert.False(SystemEventSignificance.IsSignificant(record));
    }

    [Fact]
    public void MemoryBroker_HighFixture_IsNotSignificant()
    {
        var record = SystemHealthParser.ParseMemoryBroker(LoadFixture("memory_broker.xml"))!;
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", record.Notification);
        Assert.False(SystemEventSignificance.IsSignificant(record));
    }

    // ── Significance: Memory Node OOM (never filtered) ──

    [Fact]
    public void MemoryNodeOom_AlwaysSignificant_EvenEmpty() =>
        Assert.True(SystemEventSignificance.IsSignificant(new MemoryNodeOomRecord()));

    [Fact]
    public void MemoryNodeOom_Fixture_IsSignificant()
    {
        var record = SystemHealthParser.ParseMemoryNodeOom(LoadFixture("memory_node_oom.xml"))!;
        Assert.True(SystemEventSignificance.IsSignificant(record));
    }

    // ── Significance: Significant Waits (session, non-BACKUP, >= 500 ms, not on ignore list) ──

    [Fact]
    public void SignificantWait_Fixture_IsSignificant()
    {
        var record = SystemHealthParser.ParseSignificantWait(LoadFixture("wait_info.xml"))!;
        Assert.True(SystemEventSignificance.IsSignificant(record));
    }

    private static SignificantWaitRecord Wait(
        string? waitType = "PAGEIOLATCH_SH", long? duration = 1500, int? session = 57, string? sql = "SELECT 1") =>
        new() { WaitType = waitType, DurationMs = duration, SessionId = session, QueryText = sql };

    [Fact]
    public void SignificantWait_DurationBoundary_500Kept_499Dropped()
    {
        Assert.True(SystemEventSignificance.IsSignificant(Wait(duration: 500)));
        Assert.False(SystemEventSignificance.IsSignificant(Wait(duration: 499)));
        Assert.False(SystemEventSignificance.IsSignificant(Wait(duration: null)));
    }

    [Fact]
    public void SignificantWait_SystemSession0_IsDropped() =>
        Assert.False(SystemEventSignificance.IsSignificant(Wait(session: 0)));

    [Fact]
    public void SignificantWait_NoStatementOrBackup_IsDropped()
    {
        Assert.False(SystemEventSignificance.IsSignificant(Wait(sql: null)));
        Assert.False(SystemEventSignificance.IsSignificant(Wait(sql: "")));
        Assert.False(SystemEventSignificance.IsSignificant(Wait(sql: "BACKUP DATABASE foo TO DISK = 'x'")));
        Assert.False(SystemEventSignificance.IsSignificant(Wait(sql: "backup log bar")));  // case-insensitive
    }

    [Fact]
    public void SignificantWait_IgnoredWaitType_IsDropped()
    {
        // A long wait on an idle/background type on the ignore list is not significant, even at 1000 ms.
        Assert.False(SystemEventSignificance.IsSignificant(Wait(waitType: "CHECKPOINT_QUEUE", duration: 1000)));
        Assert.False(SystemEventSignificance.IsSignificant(Wait(waitType: "checkpoint_queue", duration: 1000))); // case-insensitive
    }

    [Fact]
    public void IgnoredWaitTypes_ContainsKnownIdleWaits_NotRealWaits()
    {
        Assert.Contains("CHECKPOINT_QUEUE", SystemEventSignificance.IgnoredWaitTypes);
        Assert.Contains("LAZYWRITER_SLEEP", SystemEventSignificance.IgnoredWaitTypes);
        Assert.Contains("XE_TIMER_EVENT", SystemEventSignificance.IgnoredWaitTypes);
        /* Real, actionable waits must NOT be on the ignore list. */
        Assert.DoesNotContain("PAGEIOLATCH_SH", SystemEventSignificance.IgnoredWaitTypes);
        Assert.DoesNotContain("LCK_M_X", SystemEventSignificance.IgnoredWaitTypes);
        Assert.DoesNotContain("CXPACKET", SystemEventSignificance.IgnoredWaitTypes);
    }

    // ── Significance: CPU Tasks (state = WARNING and pendingTasks >= 10) ──

    [Theory]
    [InlineData("WARNING", 10, true)]     // boundary — 10 is kept
    [InlineData("WARNING", 15, true)]     // fixture case
    [InlineData("WARNING", 9, false)]     // below the pending-task threshold
    [InlineData("WARNING", null, false)]  // no pending tasks -> dropped (exist-predicate fails)
    [InlineData("CLEAN", 100, false)]     // not a warning, even with many pending tasks
    [InlineData("HAS_ISSUES", 100, false)]
    [InlineData(null, 100, false)]
    public void CpuTasks_SignificantOnlyWhenWarningAndPendingAtThreshold(string? state, int? pendingTasks, bool expected) =>
        Assert.Equal(expected, SystemEventSignificance.IsSignificant(
            new CpuTasksRecord { State = state, PendingTasks = pendingTasks }));

    [Fact]
    public void CpuTasks_WarningFixture_IsSignificant()
    {
        var record = SystemHealthParser.ParseCpuTasks(LoadFixture("sp_server_diagnostics_query_processing_warning.xml"))!;
        Assert.True(SystemEventSignificance.IsSignificant(record));
    }

    [Fact]
    public void CpuTasks_CleanFixture_IsNotSignificant()
    {
        // The shared clean QUERY_PROCESSING fixture is CLEAN with zero pending tasks — the warnings filter drops it.
        var record = SystemHealthParser.ParseCpuTasks(LoadFixture("sp_server_diagnostics_query_processing.xml"))!;
        Assert.Equal("CLEAN", record.State);
        Assert.False(SystemEventSignificance.IsSignificant(record));
    }

    // ── Significance: I/O Issues (state = WARNING) ──

    [Theory]
    [InlineData("WARNING", true)]
    [InlineData("CLEAN", false)]
    [InlineData("HAS_ISSUES", false)]
    [InlineData(null, false)]
    public void IoIssues_SignificantOnlyWhenWarning(string? state, bool expected) =>
        Assert.Equal(expected, SystemEventSignificance.IsSignificant(new IoIssuesRecord { State = state }));

    [Fact]
    public void IoIssues_WarningFixture_AllRowsSignificant()
    {
        var rows = SystemHealthParser.ParseIoIssues(LoadFixture("sp_server_diagnostics_io_subsystem.xml"));
        Assert.NotEmpty(rows);
        Assert.All(rows, r => Assert.True(SystemEventSignificance.IsSignificant(r)));
    }

    // ── System Health (Corruption + Contention counter charts — NO significance filter) ──

    [Fact]
    public void SystemHealth_Fixture_ShredsBothCorruptionAndContentionCounters()
    {
        // The ONE SYSTEM-component record feeds BOTH chart sub-tabs: the Corruption Events charts read the
        // bad-page / dump / access-violation columns; the Contention Events charts read the non-yielding /
        // latch / spinlock / CPU columns. Pin that every column both tabs plot is present off one shred.
        var r = SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_system.xml"))!;

        // Corruption Events columns.
        Assert.Equal(6L, r.BadPagesDetected);
        Assert.Equal(2L, r.IntervalDumpRequests);
        Assert.Equal(1L, r.IsAccessViolationOccurred);
        Assert.Equal(3L, r.WriteAccessViolationCount);
        // Contention Events columns.
        Assert.Equal(4L, r.NonYieldingTasksReported);
        Assert.Equal(7L, r.LatchWarnings);
        Assert.Equal("SOS_CACHESTORE", r.SickSpinlockType);
        Assert.Equal(12345L, r.SpinlockBackoffs);
        Assert.Equal(65L, r.SystemCpuUtilization);
        Assert.Equal(40L, r.SqlCpuUtilization);
    }

    [Fact]
    public void SystemHealth_CleanSnapshot_IsStillKept_NoWarningsOnlyGate()
    {
        // Unlike the grid categories, the Corruption / Contention charts plot EVERY collected snapshot over
        // time — there is no significance / warnings-only filter (GetSystemHealthAsync keeps every SYSTEM
        // record with a timestamp). The representative fixture is a CLEAN snapshot, and it is a real record
        // (not dropped), so the counter charts render it. This is the design contrast with the filtered grids.
        var r = SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_system.xml"));

        Assert.NotNull(r);
        Assert.Equal("CLEAN", r!.State);
        Assert.True(r.EventTime.HasValue);   // GetSystemHealthAsync requires a timestamp to sit on the time axis
    }

    [Fact]
    public void SystemHealth_NonSystemComponent_YieldsNothing()
    {
        // The SYSTEM read shares the sp_server_diagnostics feed with Memory Conditions / CPU Tasks / I/O
        // Issues; only the SYSTEM component contributes a record (the others parse to null, so the charts
        // never plot a RESOURCE / QUERY_PROCESSING / IO_SUBSYSTEM snapshot).
        Assert.Null(SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_resource.xml")));
        Assert.Null(SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_query_processing.xml")));
        Assert.Null(SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_io_subsystem.xml")));
    }

    // ── End-to-end: shred the fixtures then apply the filters (the data-service path's logic) ──

    [Fact]
    public void ShredThenFilter_KeepsOnlySignificantRows_AcrossAllCategories()
    {
        var events = new (string?, string?)[]
        {
            (SystemHealthParser.SchedulerMonitorEvent, LoadFixture("scheduler_monitor_high_sql_cpu.xml")),
            (SystemHealthParser.ErrorReportedEvent, LoadFixture("error_reported.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_system.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_resource.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_query_processing.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_query_processing_warning.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_io_subsystem.xml")),
            (SystemHealthParser.MemoryBrokerEvent, LoadFixture("memory_broker.xml")),
            (SystemHealthParser.MemoryNodeOomEvent, LoadFixture("memory_node_oom.xml")),
            (SystemHealthParser.WaitInfoEvent, LoadFixture("wait_info.xml")),
        };

        var result = SystemHealthParser.ParseEvents(events);

        // Scheduler (WARNING), Severe Error (sev 24), Memory Node OOM (ungated), and the wait all pass.
        Assert.Single(result.SchedulerIssues, SystemEventSignificance.IsSignificant);
        Assert.Single(result.SevereErrors, SystemEventSignificance.IsSignificant);
        Assert.Single(result.MemoryNodeOom, SystemEventSignificance.IsSignificant);
        Assert.Single(result.SignificantWaits, SystemEventSignificance.IsSignificant);
        // The memory-conditions / broker fixtures are RESOURCE_MEMPHYSICAL_HIGH — the warnings filter drops them.
        Assert.DoesNotContain(result.MemoryConditions, SystemEventSignificance.IsSignificant);
        Assert.DoesNotContain(result.MemoryBroker, SystemEventSignificance.IsSignificant);
        // CPU: the WARNING+15-pending event passes; the CLEAN one is dropped. I/O: both WARNING file rows pass.
        Assert.Single(result.CpuTasks, SystemEventSignificance.IsSignificant);
        Assert.Equal(2, result.IoIssues.Count(SystemEventSignificance.IsSignificant));
        // System Health (Corruption + Contention charts) has NO significance filter — the CLEAN SYSTEM
        // snapshot is kept, so the counter charts plot it (contrast with the dropped HIGH memory rows above).
        Assert.Single(result.SystemHealth);
    }

    // Severe-Errors database_id resolution (the name the id carried at the error's time) is pinned in DatabaseNameHistoryTests (#5373).

    // ── Row projection: machine-local event time + resolved DB passthrough ──

    [Fact]
    public void Rows_EventTimeLocal_RendersTheUtcTimestampAsMachineLocal()
    {
        var utc = new DateTime(2026, 7, 5, 12, 0, 5, DateTimeKind.Utc);
        var expected = ViewerTimeHelper.ForDisplay(utc).ToString("yyyy-MM-dd HH:mm:ss");

        Assert.Equal(expected, new SchedulerIssueRow(new SchedulerIssueRecord { EventTime = utc }).EventTimeLocal);
        Assert.Equal(expected, new SignificantWaitRow(new SignificantWaitRecord { EventTime = utc }).EventTimeLocal);
    }

    [Fact]
    public void Rows_NullEventTime_RendersEmpty() =>
        Assert.Equal(string.Empty, new MemoryBrokerRow(new MemoryBrokerRecord { EventTime = null }).EventTimeLocal);

    [Fact]
    public void SevereErrorRow_CarriesResolvedDatabaseName()
    {
        var record = new SevereErrorRecord { ErrorNumber = 823, Severity = 24, DatabaseId = 5 };
        var row = new SevereErrorRow(record, "AdventureWorks");

        Assert.Equal("AdventureWorks", row.DatabaseName);
        Assert.Equal(5, row.DatabaseId);
        Assert.Equal(823, row.ErrorNumber);
    }

    [Fact]
    public void CpuTasksRow_And_IoIssuesRow_ProjectRecordFields()
    {
        var utc = new DateTime(2026, 7, 5, 12, 6, 0, DateTimeKind.Utc);
        var expected = ViewerTimeHelper.ForDisplay(utc).ToString("yyyy-MM-dd HH:mm:ss");

        var cpu = new CpuTasksRow(new CpuTasksRecord
            { EventTime = utc, State = "WARNING", PendingTasks = 15, DidBlockingOccur = true });
        Assert.Equal(expected, cpu.EventTimeLocal);
        Assert.Equal("WARNING", cpu.State);
        Assert.Equal(15L, cpu.PendingTasks);
        Assert.True(cpu.DidBlockingOccur);

        var io = new IoIssuesRow(new IoIssuesRecord
            { EventTime = utc, State = "WARNING", LongestPendingRequestsDurationMs = 1500, LongestPendingRequestsFilePath = @"D:\data\prod.mdf" });
        Assert.Equal(expected, io.EventTimeLocal);
        Assert.Equal("WARNING", io.State);
        Assert.Equal(1500L, io.LongestPendingRequestsDurationMs);
        Assert.Equal(@"D:\data\prod.mdf", io.LongestPendingRequestsFilePath);
    }

    // ── Default Trace (always-on server events; the Default Trace sub-tab) ──

    [Fact]
    public void DefaultTraceEventsByWindowSql_ReadsBaseTable_ReturnsTheLocalEventTimeRaw_AndPreFiltersAnHourWide()
    {
        var sql = ViewerDataService.DefaultTraceEventsByWindowSql;

        /* Base default_trace_events (no v_ view, like server_properties). */
        Assert.Contains("FROM default_trace_events", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("v_default_trace_events", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE dte.server_id = $1", sql, StringComparison.Ordinal);

        /* The Default Trace StartTime is server-LOCAL. #4766: the SQL returns it RAW and the C# side converts
           it to naive-UTC with the server's ServerClock (its time zone, so the far side of a daylight-saving
           change is not an hour off) — so the row shares the system_health rows' UTC frame (the cross-frame
           caveat: system_health.event_time is UTC, default_trace.event_time is local). The latest offset
           stays in the SQL only as a pre-filter an hour wider on each side, which the C# window then trims. */
        Assert.Contains("server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.Contains("dte.event_time AS event_time_local", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AS event_time_utc", sql, StringComparison.Ordinal);
        Assert.Contains("event_time - make_interval(mins => svr.offset_minutes) >= $2 - interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("event_time - make_interval(mins => svr.offset_minutes) <= $3 + interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY event_time_local DESC", sql, StringComparison.Ordinal);

        /* Postgres dialect, positional params. */
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("getdate", sql.ToLowerInvariant());
        Assert.Contains("$1", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DefaultTraceEventsRead_ReferencesColumnsThatExistInTheGeneratedTable()
    {
        var ddl = PgSchemaGenerator.CreateTable(DefaultTraceEventsCollector.Instance);
        Assert.Equal("default_trace_events", DefaultTraceEventsCollector.Instance.TargetTable);

        foreach (var col in new[]
        {
            "event_time", "event_name", "database_name", "object_name", "login_name", "host_name",
            "application_name", "spid", "duration_us", "integer_data", "severity", "error_number",
            "text_data", "server_id",
        })
        {
            Assert.Contains(col, ddl, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DefaultTraceEventRow_EventTimeLocal_SharesTheSystemHealthUtcFrame()
    {
        /* A default-trace row and a system_health row built from the SAME UTC instant must render the SAME
           machine-local time — the loader de-skews the local StartTime to UTC, so both are in the UTC frame
           and a merged System Events timeline sorts them correctly (the coordinator's cross-frame caveat). */
        var utc = new DateTime(2026, 7, 9, 12, 0, 5, DateTimeKind.Utc);
        var expected = ViewerTimeHelper.ForDisplay(utc).ToString("yyyy-MM-dd HH:mm:ss");

        var dt = new DefaultTraceEventRow(
            utc, DefaultTraceEventCategory.AutoGrowShrink, "Data File Auto Grow", "SalesDB", null,
            "app", "APPHOST", "SqlClient", 55, 1_500_000, 256, null, null, null);
        var sh = new SchedulerIssueRow(new SchedulerIssueRecord { EventTime = utc });

        Assert.Equal(expected, dt.EventTimeLocal);
        Assert.Equal(sh.EventTimeLocal, dt.EventTimeLocal);
    }

    [Fact]
    public void DefaultTraceEventRow_NullEventTime_RendersEmpty() =>
        Assert.Equal(string.Empty, new DefaultTraceEventRow(
            null, DefaultTraceEventCategory.ErrorLog, "ErrorLog", null, null, null, null, null, null, null, null, 20, 823, "x").EventTimeLocal);

    [Fact]
    public void DefaultTraceEventRow_ProjectsCategory_DurationMs_AndGrowthMbForAutogrowOnly()
    {
        var grow = new DefaultTraceEventRow(
            null, DefaultTraceEventCategory.AutoGrowShrink, "Data File Auto Grow", "db", null, null, null, null,
            null, 1_500_000, 256, null, null, null);
        Assert.Equal("AutoGrowShrink", grow.Category);
        Assert.Equal(1500m, grow.DurationMs);   /* 1_500_000 us / 1000 */
        Assert.Equal(2.0m, grow.GrowthMb);       /* 256 pages * 8 KB / 1024 */

        /* Growth is null for non-autogrow categories even when integer_data is present. */
        var err = new DefaultTraceEventRow(
            null, DefaultTraceEventCategory.ErrorLog, "ErrorLog", null, null, null, null, null,
            null, null, 256, 20, 823, "boom");
        Assert.Equal("ErrorLog", err.Category);
        Assert.Null(err.GrowthMb);
    }

    [Fact]
    public void DefaultTrace_SignificantSet_GatesErrorLogBySeverity_KeepsEveryOtherCategory()
    {
        /* The significant set the loader keeps (the default-trace feed for the tab): every curated category
           is significant as collected, except ErrorLog which must clear the severity floor. */
        var events = new (string EventName, int? Severity)[]
        {
            ("Data File Auto Grow", null),   // stall           -> significant
            ("Object:Altered", null),        // schema DDL      -> significant
            ("Audit DBCC Event", null),      // security audit  -> significant
            ("Server Memory Change", null),  // memory change   -> significant
            ("ErrorLog", 20),                // severe ErrorLog -> significant
            ("ErrorLog", 10),                // routine ErrorLog -> DROPPED
        };

        var significant = events
            .Where(e => SystemEventSignificance.IsSignificantDefaultTraceEvent(e.EventName, e.Severity))
            .ToList();

        Assert.Equal(5, significant.Count);
        Assert.DoesNotContain(significant, e => e.EventName == "ErrorLog" && e.Severity == 10);
    }
}
