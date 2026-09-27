/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Tests for the shared <see cref="SystemHealthParser"/> — the monitor-side C# port of Erik's
/// <c>sp_HealthParser</c> per-category xpath shred. Each of the eight warning categories is asserted
/// column-by-column against a representative captured event (Fixtures/SystemHealth/*.xml, shaped to
/// sp_HealthParser's xpath + the MS event schema), plus dispatch, robustness, the per-component
/// sp_server_diagnostics routing (RESOURCE / QUERY_PROCESSING / IO_SUBSYSTEM) and no-op on the others, and
/// the sql_text control-character cleanup. Tested once in Common (not duplicated per app). No database.
/// </summary>
public class SystemHealthParserTests
{
    private static string LoadFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", name));

    private static readonly DateTime SchedulerHighCpuTime = new(2026, 9, 26, 22, 8, 48, 939, DateTimeKind.Utc);
    private static readonly DateTime ErrorTime = new(2026, 7, 5, 12, 0, 5, 500, DateTimeKind.Utc);
    private static readonly DateTime ResourceTime = new(2026, 7, 5, 12, 1, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime BrokerTime = new(2026, 7, 5, 12, 2, 10, 250, DateTimeKind.Utc);
    private static readonly DateTime OomTime = new(2026, 7, 5, 12, 3, 20, 750, DateTimeKind.Utc);
    private static readonly DateTime WaitTime = new(2026, 7, 5, 12, 4, 30, 900, DateTimeKind.Utc);
    private static readonly DateTime IoTime = new(2026, 7, 5, 12, 5, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime CpuWarningTime = new(2026, 7, 5, 12, 6, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime CpuCleanTime = new(2026, 7, 5, 12, 1, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime SystemTime = new(2026, 7, 5, 12, 7, 0, 0, DateTimeKind.Utc);

    // ── SchedulerIssues (scheduler_monitor_system_health_ring_buffer_recorded) ──

    [Fact]
    public void SchedulerIssue_ShredsEveryColumn_HighSqlCpu()
    {
        // Real field capture: process_utilization=94, system_idle=2 -> other = 100-94-2=4;
        // working_set_delta 4096 bytes -> 4096/1048576 = 0.00390625 MB, rounded to 0.00.
        var r = SystemHealthParser.ParseSchedulerIssue(LoadFixture("scheduler_monitor_high_sql_cpu.xml"));

        Assert.NotNull(r);
        Assert.Equal(SchedulerHighCpuTime, r!.EventTime);
        Assert.Equal(94, r.SqlCpuUtilization);
        Assert.Equal(4, r.OtherProcessCpu);
        Assert.Equal(2, r.SystemIdle);
        Assert.Equal(100, r.MemoryUtilization);
        Assert.Equal(505L, r.PageFaults);
        Assert.Equal(0.00m, r.WorkingSetDeltaMb);
        Assert.True(SystemHealthSignificance.IsSignificant(r));   // SQL CPU 94 >= 90
    }

    [Fact]
    public void SchedulerIssue_ShredsEveryColumn_Normal()
    {
        // Real field capture: process_utilization=3, system_idle=95 -> other = 100-3-95=2;
        // working_set_delta 77824 bytes -> 77824/1048576 = 0.074218... MB, rounded to 0.07.
        var r = SystemHealthParser.ParseSchedulerIssue(LoadFixture("scheduler_monitor_normal.xml"));

        Assert.NotNull(r);
        Assert.Equal(3, r!.SqlCpuUtilization);
        Assert.Equal(2, r.OtherProcessCpu);
        Assert.Equal(95, r.SystemIdle);
        Assert.Equal(100, r.MemoryUtilization);
        Assert.Equal(99L, r.PageFaults);
        Assert.Equal(0.07m, r.WorkingSetDeltaMb);
        Assert.False(SystemHealthSignificance.IsSignificant(r));
    }

    [Fact]
    public void SchedulerIssue_OtherProcessCpu_Significant()
    {
        // Derived from the real "normal" capture (system_idle changed 95 -> 40) to exercise the
        // other-process-CPU arm: other = 100-3-40=57 >= 50.
        var r = SystemHealthParser.ParseSchedulerIssue(LoadFixture("scheduler_monitor_other_process_cpu.xml"));
        Assert.NotNull(r);
        Assert.Equal(57, r!.OtherProcessCpu);
        Assert.True(SystemHealthSignificance.IsSignificant(r));
    }

    [Fact]
    public void SchedulerIssue_LowMemory_Significant()
    {
        // Derived from the real "normal" capture (memory_utilization changed 100 -> 50) to exercise
        // the low-memory arm: 50 <= 50.
        var r = SystemHealthParser.ParseSchedulerIssue(LoadFixture("scheduler_monitor_low_memory.xml"));
        Assert.NotNull(r);
        Assert.Equal(50, r!.MemoryUtilization);
        Assert.True(SystemHealthSignificance.IsSignificant(r));
    }

    [Theory]
    [InlineData(90, 0, 100, true)]        // sql cpu boundary: 90 is significant
    [InlineData(89, 0, 100, false)]       // one under, not significant
    [InlineData(0, 50, 100, true)]        // other-process boundary: 50 is significant (sql=0,idle=50->other=50)
    [InlineData(0, 51, 100, false)]       // other-process 49, not significant (idle=51->other=49)
    [InlineData(0, 100, 50, true)]        // memory boundary: 50 is significant
    [InlineData(0, 100, 51, false)]       // memory 51, not significant
    [InlineData(null, null, null, false)] // all null -> not significant (SQL null comparison is false)
    public void SchedulerIssue_SignificanceBoundaries(int? sqlCpu, int? idle, int? mem, bool expected)
    {
        var other = sqlCpu is { } sc && idle is { } id ? 100 - sc - id : (int?)null;
        var r = new SchedulerIssueRecord { SqlCpuUtilization = sqlCpu, OtherProcessCpu = other, SystemIdle = idle, MemoryUtilization = mem };
        Assert.Equal(expected, SystemHealthSignificance.IsSignificant(r));
    }

    // ── SevereErrors (error_reported) ──

    [Fact]
    public void SevereError_ShredsEveryColumn()
    {
        var r = SystemHealthParser.ParseSevereError(LoadFixture("error_reported.xml"));

        Assert.NotNull(r);
        Assert.Equal(ErrorTime, r!.EventTime);
        Assert.Equal(823, r.ErrorNumber);
        Assert.Equal(24, r.Severity);
        Assert.Equal(2, r.State);
        Assert.Equal(
            "The operating system returned error 21 to SQL Server during a read at offset 0x00000c60000 in file 'D:\\data\\prod.mdf'.",
            r.Message);
        Assert.Equal(6, r.DatabaseId);
        // DB_NAME(database_id) is a server-side resolution a DB-free shred cannot do — always null here.
        Assert.Null(r.DatabaseName);
    }

    // ── MemoryConditions (sp_server_diagnostics_component_result / RESOURCE) ──

    [Fact]
    public void MemoryConditions_ShredsResourceAttributesAndReportName()
    {
        var r = SystemHealthParser.ParseMemoryConditions(LoadFixture("sp_server_diagnostics_resource.xml"));

        Assert.NotNull(r);
        Assert.Equal(ResourceTime, r!.EventTime);
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", r.LastNotification);
        Assert.Equal(0L, r.OutOfMemoryExceptions);
        Assert.False(r.IsAnyPoolOutOfMemory);
        Assert.Equal(0L, r.ProcessOutOfMemoryPeriod);
        Assert.Equal("Process/System Counts", r.Name);   // first memoryReport/@name
    }

    [Fact]
    public void MemoryConditions_ByteEntries_DivideBy1024Cubed()
    {
        var r = SystemHealthParser.ParseMemoryConditions(LoadFixture("sp_server_diagnostics_resource.xml"))!;

        Assert.Equal(8L, r.AvailablePhysicalMemoryGb);   // 8589934592 B / 1024^3
        Assert.Equal(16L, r.AvailableVirtualMemoryGb);   // 17179869184 B / 1024^3
        Assert.Equal(4L, r.AvailablePagingFileGb);       // 4294967296 B / 1024^3
        Assert.Equal(2L, r.WorkingSetGb);                // 2147483648 B / 1024^3
    }

    [Fact]
    public void MemoryConditions_KbEntries_DivideBy1024Squared_Truncating()
    {
        var r = SystemHealthParser.ParseMemoryConditions(LoadFixture("sp_server_diagnostics_resource.xml"))!;

        Assert.Equal(8L, r.VmReservedGb);                // 8388608 KB / 1024^2
        Assert.Equal(4L, r.VmCommittedGb);               // 4194304 KB / 1024^2
        Assert.Equal(6L, r.TargetCommittedGb);           // 6291456 KB / 1024^2
        Assert.Equal(4L, r.CurrentCommittedGb);          // 4194304 KB / 1024^2
        // Sub-GB KB values truncate to 0, exactly like sp_HealthParser's bigint integer division.
        Assert.Equal(0L, r.EmergencyMemoryGb);           // 1024 KB / 1024^2 -> 0
        Assert.Equal(0L, r.EmergencyMemoryInUseGb);      // 16 KB / 1024^2 -> 0
    }

    [Fact]
    public void MemoryConditions_RawEntries_NotConverted()
    {
        var r = SystemHealthParser.ParseMemoryConditions(LoadFixture("sp_server_diagnostics_resource.xml"))!;

        Assert.Equal(99L, r.PercentOfCommittedMemoryInWs);
        Assert.Equal(123456L, r.PageFaults);
        Assert.Equal(1L, r.SystemPhysicalMemoryHigh);
        Assert.Equal(0L, r.SystemPhysicalMemoryLow);
        Assert.Equal(0L, r.ProcessPhysicalMemoryLow);
        Assert.Equal(0L, r.ProcessVirtualMemoryLow);
        Assert.Equal(0L, r.LockedPagesAllocated);
        Assert.Equal(0L, r.LargePagesAllocated);
        Assert.Equal(524288L, r.PagesAllocated);
        Assert.Equal(0L, r.PagesReserved);
        Assert.Equal(1024L, r.PagesFree);
        Assert.Equal(200000L, r.PagesInUse);
        Assert.Equal(700000L, r.PageAllocPotential);
        Assert.Equal(0L, r.NumaGrowthPhase);
        Assert.Equal(0L, r.LastOomFactor);
        Assert.Equal(0L, r.LastOsError);
    }

    [Fact]
    public void MemoryConditions_NonResourceComponent_YieldsNoRecord()
    {
        // A QUERY_PROCESSING sp_server_diagnostics component carries no <resource> node — no memory
        // conditions to report (sp_HealthParser's CROSS APPLY /event/data/value/resource returns nothing).
        var r = SystemHealthParser.ParseMemoryConditions(LoadFixture("sp_server_diagnostics_query_processing.xml"));

        Assert.Null(r);
    }

    // ── MemoryBroker (memory_broker_ring_buffer_recorded) ──

    [Fact]
    public void MemoryBroker_ShredsEveryColumn()
    {
        var r = SystemHealthParser.ParseMemoryBroker(LoadFixture("memory_broker.xml"));

        Assert.NotNull(r);
        Assert.Equal(BrokerTime, r!.EventTime);
        Assert.Equal(1L, r.BrokerId);                    // data[@name="id"]
        Assert.Equal(0L, r.PoolMetadataId);
        Assert.Equal(30000L, r.DeltaTime);
        Assert.Equal(85L, r.MemoryRatio);
        Assert.Equal(1048576L, r.NewTarget);
        Assert.Equal(2097152L, r.Overall);
        Assert.Equal(512L, r.Rate);
        Assert.Equal(900000L, r.CurrentlyPredicated);
        Assert.Equal(1000000L, r.CurrentlyAllocated);
        Assert.Equal(1100000L, r.PreviouslyAllocated);
        Assert.Equal("MEMORYBROKER_FOR_CACHE", r.Broker);
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", r.Notification);
    }

    // ── MemoryNodeOOM (memory_node_oom_ring_buffer_recorded) ──

    [Fact]
    public void MemoryNodeOom_ShredsEveryColumn()
    {
        var r = SystemHealthParser.ParseMemoryNodeOom(LoadFixture("memory_node_oom.xml"));

        Assert.NotNull(r);
        Assert.Equal(OomTime, r!.EventTime);
        Assert.Equal(0L, r.NodeId);                      // data[@name="id"]
        Assert.Equal(0L, r.MemoryNodeId);
        Assert.Equal(72L, r.MemoryUtilizationPct);
        Assert.Equal(16777216L, r.TotalPhysicalMemoryKb);
        Assert.Equal(4194304L, r.AvailablePhysicalMemoryKb);
        Assert.Equal(25165824L, r.TotalPageFileKb);
        Assert.Equal(8388608L, r.AvailablePageFileKb);
        Assert.Equal(137438953472L, r.TotalVirtualAddressSpaceKb);   // > int32 — proves long handling
        Assert.Equal(137000000000L, r.AvailableVirtualAddressSpaceKb);
        Assert.Equal(12582912L, r.TargetKb);
        Assert.Equal(1048576L, r.ReservedKb);
        Assert.Equal(10485760L, r.CommittedKb);
        Assert.Equal(2048m, r.SharedCommittedKb);        // numeric(38,0) -> decimal
        Assert.Equal(0L, r.AweKb);
        Assert.Equal(9437184L, r.PagesKb);
        Assert.Equal("NUMA_NODE_UNAVAILABLE", r.FailureType);   // data[@name="failure"]/text
        Assert.Equal(2L, r.FailureValue);                        // data[@name="failure"]/value
        Assert.Equal(1L, r.Resources);
        Assert.Equal("FACTORY_PROCESS", r.FactorText);           // data[@name="factor"]/text
        Assert.Equal(3L, r.FactorValue);                         // data[@name="factor"]/value
        Assert.Equal(0L, r.LastError);
        Assert.Equal(0L, r.PoolMetadataId);
        // is_* flags kept as raw text (nvarchar(10)), exactly as sp_HealthParser stores them.
        Assert.Equal("true", r.IsProcessInJob);
        Assert.Equal("false", r.IsSystemPhysicalMemoryHigh);
        Assert.Equal("true", r.IsSystemPhysicalMemoryLow);
        Assert.Equal("false", r.IsProcessPhysicalMemoryLow);
        Assert.Equal("false", r.IsProcessVirtualMemoryLow);
    }

    // ── SignificantWaits (wait_info) ──

    [Fact]
    public void SignificantWait_ShredsEveryColumn()
    {
        var r = SystemHealthParser.ParseSignificantWait(LoadFixture("wait_info.xml"));

        Assert.NotNull(r);
        Assert.Equal(WaitTime, r!.EventTime);
        Assert.Equal("PAGEIOLATCH_SH", r.WaitType);      // data[@name="wait_type"]/text
        Assert.Equal(1500L, r.DurationMs);
        Assert.Equal(12L, r.SignalDurationMs);
        Assert.Equal("7:1:12345", r.WaitResource);
        Assert.Equal(57, r.SessionId);                   // action[@name="session_id"]
        Assert.Equal("SELECT * FROM dbo.big_table WHERE id = @p1;", r.QueryText);  // action[@name="sql_text"]
    }

    [Fact]
    public void SignificantWait_CleansControlCharactersButKeepsTabCrLf()
    {
        // Real sql_text carries control chars (SQL Server serializes them as &#xNN; references). The parser
        // tolerates them (CheckCharacters=false) and CleanSqlText replaces every control char EXCEPT tab
        // (9), LF (10) and CR (13) with '?', exactly as sp_HealthParser's query_text REPLACE chain does.
        const string xml =
            "<event name=\"wait_info\" package=\"sqlserver\" timestamp=\"2026-07-05T12:04:30.900Z\">" +
            "<data name=\"duration\"><value>800</value></data>" +
            "<action name=\"sql_text\" package=\"sqlserver\"><value>SELECT&#x01;1&#x1f;FROM&#x09;t&#x0d;&#x0a;GO</value></action>" +
            "</event>";

        var r = SystemHealthParser.ParseSignificantWait(xml);

        Assert.NotNull(r);
        Assert.Equal("SELECT?1?FROM\tt\r\nGO", r!.QueryText);
    }

    // ── CpuTasks (sp_server_diagnostics_component_result / QUERY_PROCESSING) ──

    [Fact]
    public void CpuTasks_WarningEvent_ShredsEveryColumn()
    {
        var r = SystemHealthParser.ParseCpuTasks(LoadFixture("sp_server_diagnostics_query_processing_warning.xml"));

        Assert.NotNull(r);
        Assert.Equal(CpuWarningTime, r!.EventTime);
        Assert.Equal("WARNING", r.State);                     // data[@name="state"]/text
        Assert.Equal(960L, r.MaxWorkers);                     // <queryProcessing> attributes
        Assert.Equal(500L, r.WorkersCreated);
        Assert.Equal(20L, r.WorkersIdle);
        Assert.Equal(1234L, r.TasksCompletedWithinInterval);
        Assert.Equal(15L, r.PendingTasks);
        Assert.Equal(8000L, r.OldestPendingTaskWaitingTime);
        Assert.True(r.HasUnresolvableDeadlockOccurred);
        Assert.True(r.HasDeadlockedSchedulersOccurred);
        Assert.True(r.DidBlockingOccur);                      // blockingTasks/blocked-process-report present
    }

    [Fact]
    public void CpuTasks_CleanEvent_ShredsColumns_NoBlocking()
    {
        // The shared clean QUERY_PROCESSING fixture: CLEAN state, zero pending tasks, no blocking node.
        var r = SystemHealthParser.ParseCpuTasks(LoadFixture("sp_server_diagnostics_query_processing.xml"));

        Assert.NotNull(r);
        Assert.Equal(CpuCleanTime, r!.EventTime);
        Assert.Equal("CLEAN", r.State);
        Assert.Equal(512L, r.MaxWorkers);
        Assert.Equal(40L, r.WorkersCreated);
        Assert.Equal(12L, r.WorkersIdle);
        Assert.Equal(100L, r.TasksCompletedWithinInterval);
        Assert.Equal(0L, r.PendingTasks);
        Assert.Equal(0L, r.OldestPendingTaskWaitingTime);
        Assert.False(r.HasUnresolvableDeadlockOccurred);
        Assert.False(r.HasDeadlockedSchedulersOccurred);
        Assert.False(r.DidBlockingOccur);                     // .exist() over absent blocked-process-report -> false
    }

    [Fact]
    public void CpuTasks_NonQueryProcessingComponent_YieldsNoRecord()
    {
        // A RESOURCE sp_server_diagnostics component carries no <queryProcessing> node.
        Assert.Null(SystemHealthParser.ParseCpuTasks(LoadFixture("sp_server_diagnostics_resource.xml")));
    }

    // ── IoIssues (sp_server_diagnostics_component_result / IO_SUBSYSTEM) ──

    [Fact]
    public void IoIssues_ShredsEventAttributes_AndGroupsPendingRequestsByFileSummingDuration()
    {
        var rows = SystemHealthParser.ParseIoIssues(LoadFixture("sp_server_diagnostics_io_subsystem.xml"));

        // Three pendingRequests over two files -> two rows (the same-file pair is summed, per #io -> #i).
        Assert.Equal(2, rows.Count);
        Assert.All(rows, r =>
        {
            Assert.Equal(IoTime, r.EventTime);
            Assert.Equal("WARNING", r.State);                 // data[@name="state"]/text
            Assert.Equal(2L, r.IoLatchTimeouts);              // <ioSubsystem> attributes, repeated per file row
            Assert.Equal(5L, r.IntervalLongIos);
            Assert.Equal(42L, r.TotalLongIos);
        });

        var mdf = rows.Single(r => r.LongestPendingRequestsFilePath == @"D:\data\prod.mdf");
        Assert.Equal(1500L, mdf.LongestPendingRequestsDurationMs);   // 1000 + 500 summed
        var ldf = rows.Single(r => r.LongestPendingRequestsFilePath == @"E:\log\prod.ldf");
        Assert.Equal(750L, ldf.LongestPendingRequestsDurationMs);
    }

    [Fact]
    public void IoIssues_NonIoSubsystemComponent_YieldsNoRows()
    {
        // RESOURCE / QUERY_PROCESSING components carry no <ioSubsystem> node.
        Assert.Empty(SystemHealthParser.ParseIoIssues(LoadFixture("sp_server_diagnostics_resource.xml")));
        Assert.Empty(SystemHealthParser.ParseIoIssues(LoadFixture("sp_server_diagnostics_query_processing.xml")));
    }

    [Fact]
    public void IoIssues_IoSubsystemWithNoPendingRequest_YieldsNoRows()
    {
        // sp_HealthParser's WHERE duration IS NOT NULL: an IO_SUBSYSTEM event with no pending request is dropped.
        const string xml =
            "<event name=\"sp_server_diagnostics_component_result\" timestamp=\"2026-07-05T12:05:00.000Z\">" +
            "<data name=\"state\"><value>1</value><text>CLEAN</text></data>" +
            "<data name=\"data\"><value><ioSubsystem ioLatchTimeouts=\"0\" intervalLongIos=\"0\" totalLongIos=\"0\">" +
            "<longestPendingRequests /></ioSubsystem></value></data>" +
            "</event>";

        Assert.Empty(SystemHealthParser.ParseIoIssues(xml));
    }

    // ── SystemHealth (sp_server_diagnostics_component_result / SYSTEM) ──

    [Fact]
    public void SystemHealth_ShredsEveryColumn()
    {
        var r = SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_system.xml"));

        Assert.NotNull(r);
        Assert.Equal(SystemTime, r!.EventTime);
        Assert.Equal("CLEAN", r.State);                       // data[@name="state"]/text
        Assert.Equal(12345L, r.SpinlockBackoffs);             // <system> attributes
        Assert.Equal("SOS_CACHESTORE", r.SickSpinlockType);
        Assert.Equal("SOS_SUSPEND_QUEUE", r.SickSpinlockTypeAfterAv);
        Assert.Equal(7L, r.LatchWarnings);
        Assert.Equal(1L, r.IsAccessViolationOccurred);        // bigint, not bit — matches the *_SystemHealth column
        Assert.Equal(3L, r.WriteAccessViolationCount);
        Assert.Equal(9L, r.TotalDumpRequests);
        Assert.Equal(2L, r.IntervalDumpRequests);
        Assert.Equal(4L, r.NonYieldingTasksReported);
        Assert.Equal(654321L, r.PageFaults);
        Assert.Equal(65L, r.SystemCpuUtilization);
        Assert.Equal(40L, r.SqlCpuUtilization);
        Assert.Equal(6L, r.BadPagesDetected);
        Assert.Equal(5L, r.BadPagesFixed);
    }

    [Fact]
    public void SystemHealth_NonSystemComponent_YieldsNoRecord()
    {
        // RESOURCE / QUERY_PROCESSING / IO_SUBSYSTEM components carry no <system> node — no system-health
        // counters to report (parity with the CROSS APPLY over /event/data/value/system returning nothing).
        Assert.Null(SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_resource.xml")));
        Assert.Null(SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_query_processing.xml")));
        Assert.Null(SystemHealthParser.ParseSystemHealth(LoadFixture("sp_server_diagnostics_io_subsystem.xml")));
    }

    // ── dispatch (ParseEvents / ParseInto) ──

    [Fact]
    public void ParseEvents_DispatchesEachEventToItsCategory()
    {
        var events = new (string?, string?)[]
        {
            (SystemHealthParser.SchedulerMonitorEvent, LoadFixture("scheduler_monitor_high_sql_cpu.xml")),
            (SystemHealthParser.ErrorReportedEvent, LoadFixture("error_reported.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_system.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_resource.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_query_processing.xml")),
            (SystemHealthParser.SpServerDiagnosticsEvent, LoadFixture("sp_server_diagnostics_io_subsystem.xml")),
            (SystemHealthParser.MemoryBrokerEvent, LoadFixture("memory_broker.xml")),
            (SystemHealthParser.MemoryNodeOomEvent, LoadFixture("memory_node_oom.xml")),
            (SystemHealthParser.WaitInfoEvent, LoadFixture("wait_info.xml")),
        };

        var result = SystemHealthParser.ParseEvents(events);

        Assert.Single(result.SchedulerIssues);
        Assert.Single(result.SevereErrors);
        Assert.Single(result.MemoryBroker);
        Assert.Single(result.MemoryNodeOom);
        Assert.Single(result.SignificantWaits);
        // Four sp_server_diagnostics events route by component: SYSTEM -> system health,
        // RESOURCE -> memory conditions, QUERY_PROCESSING -> CPU tasks, IO_SUBSYSTEM -> I/O issues
        // (two files -> two rows). Each component parser is a no-op on the other three components.
        Assert.Single(result.SystemHealth);
        Assert.Single(result.MemoryConditions);
        Assert.Single(result.CpuTasks);
        Assert.Equal(2, result.IoIssues.Count);
        Assert.Equal(94, result.SchedulerIssues[0].SqlCpuUtilization);
        Assert.Equal(823, result.SevereErrors[0].ErrorNumber);
        Assert.Equal(6, result.SystemHealth[0].BadPagesDetected);
    }

    [Fact]
    public void ParseEvents_IgnoresUnknownEventTypes()
    {
        var events = new (string?, string?)[]
        {
            ("xml_deadlock_report", "<event name=\"xml_deadlock_report\"><data name=\"x\"><value>1</value></data></event>"),
            ("connectivity_ring_buffer_recorded", "<event name=\"connectivity_ring_buffer_recorded\" />"),
        };

        var result = SystemHealthParser.ParseEvents(events);

        Assert.Empty(result.SchedulerIssues);
        Assert.Empty(result.SevereErrors);
        Assert.Empty(result.MemoryConditions);
        Assert.Empty(result.MemoryBroker);
        Assert.Empty(result.MemoryNodeOom);
        Assert.Empty(result.SignificantWaits);
    }

    [Fact]
    public void ParseInto_FallsBackToEventNameWhenEventTypeMissing()
    {
        // event_type null -> dispatch on the XML's own @name attribute.
        var result = new SystemHealthParseResult();

        SystemHealthParser.ParseInto(null, LoadFixture("wait_info.xml"), result);

        Assert.Single(result.SignificantWaits);
        Assert.Equal("PAGEIOLATCH_SH", result.SignificantWaits[0].WaitType);
    }

    // ── robustness (mirrors DeadlockGraphParser's never-throw contract) ──

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("<event name=\"wait_info\"")]   // truncated / not well-formed
    [InlineData("not xml at all")]
    public void Parse_MalformedOrEmpty_ReturnsNullNeverThrows(string? xml)
    {
        Assert.Null(SystemHealthParser.ParseSchedulerIssue(xml));
        Assert.Null(SystemHealthParser.ParseSevereError(xml));
        Assert.Null(SystemHealthParser.ParseMemoryConditions(xml));
        Assert.Null(SystemHealthParser.ParseMemoryBroker(xml));
        Assert.Null(SystemHealthParser.ParseMemoryNodeOom(xml));
        Assert.Null(SystemHealthParser.ParseSignificantWait(xml));
        Assert.Null(SystemHealthParser.ParseCpuTasks(xml));
        Assert.Null(SystemHealthParser.ParseSystemHealth(xml));
        Assert.Empty(SystemHealthParser.ParseIoIssues(xml));   // list-returning shred: empty, never throws
    }

    [Fact]
    public void Parse_WellFormedButMissingDataNodes_YieldsRecordWithNullFields()
    {
        // A valid <event> with none of the expected data nodes still shreds (pure port of the CROSS APPLY
        // //event, which emits one row per event before any WHERE filter) — every field simply comes back null.
        var r = SystemHealthParser.ParseMemoryBroker("<event name=\"memory_broker_ring_buffer_recorded\" />");

        Assert.NotNull(r);
        Assert.Null(r!.EventTime);
        Assert.Null(r.BrokerId);
        Assert.Null(r.Broker);
        Assert.Null(r.Notification);
    }

    [Fact]
    public void ParseEvents_NullSequence_ReturnsEmptyResult()
    {
        var result = SystemHealthParser.ParseEvents(null!);

        Assert.Empty(result.SchedulerIssues);
        Assert.Empty(result.SignificantWaits);
    }
}
