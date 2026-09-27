/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Parity pins for <see cref="SystemHealthParser"/> against REAL captured <c>system_health</c> events
/// (Fixtures/SystemHealth/real/*.xml — scrubbed of any host, database, login or client identifier, and with
/// <c>call_stack</c>/<c>callstack_rva</c> stripped, but otherwise byte-for-byte the engine's own XE shape).
/// Every assertion is a value HAND-COMPUTED from the fixture's XML with sp_HealthParser's own xpath, not
/// copied from the parser's output, so a fixture that drifts from the proc's xpath (as the pre-fix
/// <c>database_id</c> read on the data axis did) shows up here.
/// </summary>
public class SystemHealthParserRealEventTests
{
    private static string LoadRealFixture(string name) =>
        File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "SystemHealth", "real", name));

    // ── error_reported: real event has NO data[@name="database_id"], only action[@name="database_id"] ──

    [Fact]
    public void SevereError_RealEvent_DatabaseIdReadsActionAxis()
    {
        // XML: <action name="database_id" ...><value>0</value></action>, and NO data[@name="database_id"]
        // anywhere in the event. severity 20, error_number 17810 (not on the ignore list: only
        // 17830/18056 are), so the row survives the base filter.
        var r = SystemHealthParser.ParseSevereError(LoadRealFixture("error_reported.xml"));

        Assert.NotNull(r);
        Assert.Equal(17810, r!.ErrorNumber);
        Assert.Equal(20, r.Severity);
        Assert.Equal(2, r.State);
        Assert.Equal(0, r.DatabaseId);   // action[@name="database_id"]/value — 0 on this real event
        Assert.Null(r.DatabaseName);
        Assert.Equal(new DateTime(2026, 9, 26, 23, 50, 54, 328, DateTimeKind.Utc), r.EventTime);
    }

    // ── memory_broker_ring_buffer_recorded: match, field-for-field ──

    [Fact]
    public void MemoryBroker_RealEvent_ShredsEveryField()
    {
        // xpath data[@name=X]/value for each X below, read straight off the fixture.
        var r = SystemHealthParser.ParseMemoryBroker(LoadRealFixture("memory_broker_ring_buffer_recorded.xml"));

        Assert.NotNull(r);
        Assert.Equal(0L, r!.BrokerId);                 // data[@name="id"]
        Assert.Equal(1L, r.PoolMetadataId);
        Assert.Equal(149L, r.DeltaTime);
        Assert.Equal(100L, r.MemoryRatio);
        Assert.Equal(245879L, r.NewTarget);
        Assert.Equal(1276657L, r.Overall);
        Assert.Equal(-60L, r.Rate);
        Assert.Equal(2786L, r.CurrentlyPredicated);
        Assert.Equal(2786L, r.CurrentlyAllocated);
        Assert.Equal(2786L, r.PreviouslyAllocated);
        Assert.Equal("MEMORYBROKER_FOR_XTP", r.Broker);
        Assert.Equal("GROW", r.Notification);          // not RESOURCE_MEMPHYSICAL_LOW -> not significant
        Assert.False(SystemHealthSignificance.IsSignificant(r));
    }

    // ── wait_info: real event's sql_text carries a BACKUP statement, so it's captured but not significant ──

    [Fact]
    public void SignificantWait_RealEvent_ShredsEveryField()
    {
        // duration/signal_duration from data[@name]/value; wait_type's friendly text from data/text;
        // sql_text/session_id from the action axis, matching sp_HealthParser's own action reads.
        var r = SystemHealthParser.ParseSignificantWait(LoadRealFixture("wait_info.xml"));

        Assert.NotNull(r);
        Assert.Equal("ASYNC_IO_COMPLETION", r!.WaitType);
        Assert.Equal(94818L, r.DurationMs);
        Assert.Equal(0L, r.SignalDurationMs);
        Assert.Equal("0x0000000000000001", r.WaitResource);
        Assert.Equal(166, r.SessionId);
        Assert.Contains("BACKUP DATABASE", r.QueryText, StringComparison.Ordinal);
        // sp_HealthParser's own WHERE excludes any wait whose statement IS a BACKUP — real, not fabricated.
        Assert.False(SystemHealthSignificance.IsSignificant(r));
    }

    // ── sp_server_diagnostics_component_result / SYSTEM: match, field-for-field ──

    [Fact]
    public void SystemHealth_RealEvent_ShredsSystemAttributes()
    {
        var r = SystemHealthParser.ParseSystemHealth(LoadRealFixture("sp_server_diagnostics_system.xml"));

        Assert.NotNull(r);
        Assert.Equal("CLEAN", r!.State);
        Assert.Equal(3897L, r.SpinlockBackoffs);
        Assert.Equal("none", r.SickSpinlockType);
        Assert.Equal("none", r.SickSpinlockTypeAfterAv);
        Assert.Equal(0L, r.LatchWarnings);
        Assert.Equal(0L, r.IsAccessViolationOccurred);
        Assert.Equal(0L, r.WriteAccessViolationCount);
        Assert.Equal(0L, r.TotalDumpRequests);
        Assert.Equal(0L, r.IntervalDumpRequests);
        Assert.Equal(0L, r.NonYieldingTasksReported);
        Assert.Equal(285438L, r.PageFaults);
        Assert.Equal(13L, r.SystemCpuUtilization);
        Assert.Equal(12L, r.SqlCpuUtilization);
        Assert.Equal(0L, r.BadPagesDetected);
        Assert.Equal(0L, r.BadPagesFixed);
    }

    // ── sp_server_diagnostics_component_result / RESOURCE: match, field-for-field ──

    [Fact]
    public void MemoryConditions_RealEvent_ShredsResourceAndMemoryReportEntries()
    {
        // *_gb entries integer-divide the raw byte/KB attribute exactly as sp_HealthParser's bigint
        // arithmetic does: 25444978688 / 1024^3 = 23, 450451664 KB / 1024^2 = 429, etc.
        var r = SystemHealthParser.ParseMemoryConditions(LoadRealFixture("sp_server_diagnostics_resource.xml"));

        Assert.NotNull(r);
        Assert.Equal("RESOURCE_MEMPHYSICAL_HIGH", r!.LastNotification);   // not LOW -> not significant
        Assert.False(SystemHealthSignificance.IsSignificant(r));
        Assert.Equal(0L, r.OutOfMemoryExceptions);
        Assert.False(r.IsAnyPoolOutOfMemory);
        Assert.Equal(0L, r.ProcessOutOfMemoryPeriod);
        Assert.Equal("Process/System Counts", r.Name);
        Assert.Equal(23L, r.AvailablePhysicalMemoryGb);     // 25444978688 / 1024^3
        Assert.Equal(130637L, r.AvailableVirtualMemoryGb);  // 140271047065600 / 1024^3
        Assert.Equal(28L, r.AvailablePagingFileGb);         // 30975954944 / 1024^3
        Assert.Equal(1L, r.WorkingSetGb);                   // 1233104896 / 1024^3
        Assert.Equal(100L, r.PercentOfCommittedMemoryInWs);
        Assert.Equal(455480798L, r.PageFaults);
        Assert.Equal(1L, r.SystemPhysicalMemoryHigh);
        Assert.Equal(0L, r.SystemPhysicalMemoryLow);
        Assert.Equal(0L, r.ProcessPhysicalMemoryLow);
        Assert.Equal(0L, r.ProcessVirtualMemoryLow);
        Assert.Equal(429L, r.VmReservedGb);                 // 450451664 KB / 1024^2
        Assert.Equal(2L, r.VmCommittedGb);                  // 2630184 KB / 1024^2
        Assert.Equal(229901500L, r.LockedPagesAllocated);
        Assert.Equal(1617920L, r.LargePagesAllocated);
        Assert.Equal(224327792L, r.PagesAllocated);
        Assert.Equal(0L, r.LastOomFactor);
        Assert.Equal(0L, r.LastOsError);
    }

    // ── sp_server_diagnostics_component_result / QUERY_PROCESSING: match, field-for-field ──

    [Fact]
    public void CpuTasks_RealEvent_ShredsQueryProcessingAttributes()
    {
        var r = SystemHealthParser.ParseCpuTasks(LoadRealFixture("sp_server_diagnostics_query_processing.xml"));

        Assert.NotNull(r);
        Assert.Equal("WARNING", r!.State);
        Assert.Equal(576L, r.MaxWorkers);
        Assert.Equal(163L, r.WorkersCreated);
        Assert.Equal(84L, r.WorkersIdle);
        Assert.Equal(179463L, r.TasksCompletedWithinInterval);
        Assert.Equal(1L, r.PendingTasks);
        Assert.Equal(0L, r.OldestPendingTaskWaitingTime);
        Assert.False(r.HasUnresolvableDeadlockOccurred);
        Assert.False(r.HasDeadlockedSchedulersOccurred);
        Assert.False(r.DidBlockingOccur);   // <blockingTasks/> is empty on this real event
        // state WARNING and pendingTasks (1) >= 1 but < CpuTaskMinPendingTasks (10) -> not significant.
        Assert.False(SystemHealthSignificance.IsSignificant(r));
    }

    // ── sp_server_diagnostics_component_result / IO_SUBSYSTEM: match; no pending request -> empty list ──

    [Fact]
    public void IoIssues_RealEvent_NoPendingRequest_YieldsEmptyList()
    {
        // <longestPendingRequests/> is empty on this real event, so sp_HealthParser's per-file GROUP BY
        // (and this parser's mirror of it) produces zero rows — a real, not fabricated, empty result.
        var r = SystemHealthParser.ParseIoIssues(LoadRealFixture("sp_server_diagnostics_io_subsystem.xml"));

        Assert.Empty(r);
    }
}
