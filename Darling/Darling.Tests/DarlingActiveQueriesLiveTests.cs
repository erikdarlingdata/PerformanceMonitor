/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The <c>fetch_active_queries</c> live-snapshot command's WIRE CONTRACT — pinned end-to-end with no database or
/// SQL Server: the SERVICE serializer (<see cref="ActiveQueriesLivePayload.Serialize"/>, which shreds the shared
/// <see cref="QuerySnapshotsCollector.Row"/>) must round-trip through the VIEWER parser
/// (<see cref="ViewerDataService.ParseActiveQueriesLiveResult"/>, which reconstructs the SAME
/// <see cref="ViewerQuerySnapshotRow"/> the stored Active Queries grid binds). Because the two ends live in
/// different assemblies and agree only by matching property names, this round-trip is what keeps them from
/// drifting. Also pins the poll-timeout / failure mappings and the viewer↔service timeout ordering.
/// </summary>
public sealed class DarlingActiveQueriesLiveTests
{
    private static QuerySnapshotsCollector.Row SampleRow() => new()
    {
        SessionId = 57,
        DatabaseName = "AdventureWorks",
        ElapsedTimeFormatted = "00 00:00:12.345",
        QueryText = "SELECT * FROM dbo.Orders WHERE CustomerId = @c",
        QueryPlan = null,                                  /* no estimated plan captured */
        LiveQueryPlan = "<ShowPlanXML>live</ShowPlanXML>", /* but a live plan was */
        Status = "running",
        BlockingSessionId = 99,
        WaitType = "CXPACKET",
        WaitTimeMs = 4321,
        WaitResource = "PAGE: 5:1:12345",
        CpuTimeMs = 6789,
        TotalElapsedTimeMs = 12345,
        Reads = 111,
        Writes = 222,
        LogicalReads = 333333,
        GrantedQueryMemoryGb = 1.25m,
        TransactionIsolationLevel = "Read Committed",
        Dop = 8,
        ParallelWorkerCount = 16,
        LoginName = "app_login",
        HostName = "APPHOST01",
        ProgramName = ".Net SqlClient Data Provider",
        OpenTransactionCount = 1,
        PercentComplete = 42.5m,
        IsCdcCapture = false,
        QueryHash = "0x1122334455667788",
        RequestedMemoryMb = 512.5,
        UsedMemoryMb = 256.25,
        MaxUsedMemoryMb = 300.75,
        TempdbCurrentMb = 12.5,
        TempdbAllocationsMb = 40.0,
        TranLogUsedMb = 7.5,
        TranStartTime = new DateTime(2026, 7, 13, 11, 59, 0, DateTimeKind.Unspecified),
        RequestId = 3,
    };

    /* #7573: the live read runs the collector's own query, which no longer drops the monitor's other sessions, so the Viewer's list follows the
       Active Queries page's switch: the monitor's out by default, a user's similar name and a missing program name kept. */
    [Fact]
    public void TheViewersLiveList_LeavesTheMonitorsSessionsOut_UnlessTheSwitchAsks()
    {
        var rows = new List<QuerySnapshotsCollector.Row>();
        var programs = new string?[] { "PerformanceMonitorLite", "Orders.Api", null, "MyPerformanceMonitorTool", "PerformanceMonitorDarling-Remediation" };
        for (var i = 0; i < programs.Length; i++)
        {
            var row = SampleRow();
            row.SessionId = 60 + i;
            row.ProgramName = programs[i];
            rows.Add(row);
        }

        var json = ActiveQueriesLivePayload.Serialize(rows, new DateTime(2026, 11, 4, 12, 0, 0, DateTimeKind.Utc));
        var parsed = ViewerDataService.ParseActiveQueriesLiveResult(new CommandResult("succeeded", "active queries fetched", json)).Rows;

        Assert.Equal(new[] { 61, 62, 63 }, ViewerDataService.LeaveOutMonitorRows(parsed, includeMonitor: false).ConvertAll(r => r.SessionId));
        Assert.Equal(5, ViewerDataService.LeaveOutMonitorRows(parsed, includeMonitor: true).Count);
    }

    [Fact]
    public void ServiceSerialize_RoundTrips_ThroughViewerParse_PreservingEveryBoundColumn()
    {
        var fetchedAt = new DateTime(2026, 7, 13, 12, 0, 0, DateTimeKind.Utc);
        var json = ActiveQueriesLivePayload.Serialize(new List<QuerySnapshotsCollector.Row> { SampleRow() }, fetchedAt);

        /* Wrap it exactly as the service reports it on the command row, then parse as the viewer does. */
        var result = ViewerDataService.ParseActiveQueriesLiveResult(
            new CommandResult("succeeded", "active queries fetched", json));

        Assert.Equal(ActiveQueriesLiveStatus.Fetched, result.Status);
        var row = Assert.Single(result.Rows);

        Assert.Equal(57, row.SessionId);
        Assert.Equal("AdventureWorks", row.DatabaseName);
        Assert.Equal("00 00:00:12.345", row.ElapsedTimeFormatted);
        Assert.Equal("SELECT * FROM dbo.Orders WHERE CustomerId = @c", row.QueryText);
        Assert.Null(row.QueryPlan);
        Assert.False(row.HasQueryPlan);
        Assert.Equal("<ShowPlanXML>live</ShowPlanXML>", row.LiveQueryPlan);
        Assert.True(row.HasLiveQueryPlan);
        Assert.Equal("running", row.Status);
        Assert.Equal(99, row.BlockingSessionId);
        Assert.Equal("CXPACKET", row.WaitType);
        Assert.Equal(4321, row.WaitTimeMs);
        Assert.Equal("PAGE: 5:1:12345", row.WaitResource);
        Assert.Equal(6789, row.CpuTimeMs);
        Assert.Equal(12345, row.TotalElapsedTimeMs);
        Assert.Equal(111, row.Reads);
        Assert.Equal(222, row.Writes);
        Assert.Equal(333333, row.LogicalReads);
        Assert.Equal(1.25, row.GrantedQueryMemoryGb, 3);
        Assert.Equal("Read Committed", row.TransactionIsolationLevel);
        Assert.Equal(8, row.Dop);
        Assert.Equal(16, row.ParallelWorkerCount);
        Assert.Equal("app_login", row.LoginName);
        Assert.Equal("APPHOST01", row.HostName);
        Assert.Equal(".Net SqlClient Data Provider", row.ProgramName);
        Assert.Equal(1, row.OpenTransactionCount);
        Assert.Equal(42.5m, row.PercentComplete);
        Assert.Equal("0x1122334455667788", row.QueryHash);
        Assert.Equal(512.5, row.RequestedMemoryMb, 3);
        Assert.Equal(256.25, row.UsedMemoryMb, 3);
        Assert.Equal(300.75, row.MaxUsedMemoryMb, 3);
        Assert.Equal(12.5, row.TempdbCurrentMb, 3);
        Assert.Equal(40.0, row.TempdbAllocationsMb, 3);
        Assert.Equal(7.5, row.TranLogUsedMb, 3);
        Assert.NotNull(row.TranStartTime);
        Assert.Equal(3, row.RequestId);

        /* The DMV row carries no capture timestamp — the fetch stamps it, so CollectionTime is the fetch time. */
        Assert.NotEqual(DateTime.MinValue, row.CollectionTime);
        Assert.Equal(2026, row.CollectionTime.ToUniversalTime().Year);
    }

    [Fact]
    public void ViewerParse_EmptySnapshot_IsFetchedWithNoRows()
    {
        var json = ActiveQueriesLivePayload.Serialize(new List<QuerySnapshotsCollector.Row>(), DateTime.UtcNow);
        var result = ViewerDataService.ParseActiveQueriesLiveResult(new CommandResult("succeeded", "active queries fetched", json));

        Assert.Equal(ActiveQueriesLiveStatus.Fetched, result.Status);
        Assert.Empty(result.Rows);
    }

    [Fact]
    public void ViewerParse_NullPoll_IsTimedOut_WithMessage()
    {
        var result = ViewerDataService.ParseActiveQueriesLiveResult(null);

        Assert.Equal(ActiveQueriesLiveStatus.TimedOut, result.Status);
        Assert.Empty(result.Rows);
        Assert.False(string.IsNullOrEmpty(result.Message));
    }

    [Fact]
    public void ViewerParse_FailedRow_SurfacesTheServiceError()
    {
        var result = ViewerDataService.ParseActiveQueriesLiveResult(
            new CommandResult("failed", "server not connected",
                "{\"success\":false,\"error\":\"server 'SQL01' is not currently connected\"}"));

        Assert.Equal(ActiveQueriesLiveStatus.Failed, result.Status);
        Assert.Empty(result.Rows);
        Assert.Contains("not currently connected", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ViewerParse_FailedRow_FallsBackToResultStatus_WhenNoErrorField()
    {
        var result = ViewerDataService.ParseActiveQueriesLiveResult(
            new CommandResult("failed", "timed out", null));

        Assert.Equal(ActiveQueriesLiveStatus.Failed, result.Status);
        Assert.Equal("timed out", result.Message);
    }

    /// <summary>
    /// The viewer's poll budget must stay wider than the service's SQL read cap, so the viewer sees the service's
    /// real "timed out" outcome rather than a poll miss (a poll miss would strand the large command row too). Pins
    /// the two ends' timeouts against each other so a future change to either cannot silently invert them.
    /// </summary>
    [Fact]
    public void ViewerPollBudget_IsWiderThanServiceSqlReadCap()
    {
        Assert.True(
            ViewerDataService.ActiveQueriesLiveTimeout.TotalSeconds > DarlingWorker.ActiveQueriesFetchTimeoutSeconds,
            "the viewer poll budget must exceed the service's SQL read cap so a slow read reports a real outcome");
    }
}
