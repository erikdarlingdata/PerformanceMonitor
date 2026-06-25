/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Regression tests for #1086: a missing/uncreatable XE session must surface in
/// collector health (so the UI stops showing OK while blocking/deadlock capture
/// is dead), and must clear again once a collection cycle succeeds (self-heal).
///
/// RecordCollectorResult and GetHealthSummary only touch the in-memory health
/// dictionary, so the service is constructed with null dependencies.
/// </summary>
public class XeSessionHealthTests
{
    private const int ServerId = 42;
    private const string Collector = "blocked_process_report";

    private static RemoteCollectorService CreateService() =>
        new(duckDb: null!, serverManager: null!, scheduleManager: null!);

    [Fact]
    public void XeFailure_Classified_As_Error_Surfaces_In_Both_Lists()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, Collector, "ERROR",
            "Failed to ensure blocked process XE session: boom", xeSessionUnavailable: true);

        var summary = service.GetHealthSummary(ServerId);

        var failure = Assert.Single(summary.XeSessionFailures);
        Assert.True(failure.XeSessionUnavailable);
        Assert.Equal(Collector, failure.CollectorName);
        Assert.Contains("boom", failure.XeSessionMessage);

        /* ERROR increments ConsecutiveErrors, so it also shows as an erroring collector */
        Assert.Equal(1, summary.ErroringCollectors);
    }

    [Fact]
    public void XeFailure_Classified_As_Permissions_Still_Surfaces()
    {
        var service = CreateService();

        /* PERMISSIONS deliberately does not increment ConsecutiveErrors, which is
           exactly why XeSessionFailures must be tracked separately — without it
           this failure would be invisible and the status bar would show OK */
        service.RecordCollectorResult(ServerId, Collector, "PERMISSIONS",
            "ALTER ANY EVENT SESSION permission denied", xeSessionUnavailable: true);

        var summary = service.GetHealthSummary(ServerId);

        Assert.Equal(0, summary.ErroringCollectors);
        var failure = Assert.Single(summary.XeSessionFailures);
        Assert.True(failure.XeSessionUnavailable);
        Assert.True(failure.IsPermissionRestricted);
    }

    [Fact]
    public void Subsequent_Success_Clears_The_Flag()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, Collector, "ERROR",
            "Failed to ensure deadlock XE session: boom", xeSessionUnavailable: true);
        service.RecordCollectorResult(ServerId, Collector, "SUCCESS");

        var summary = service.GetHealthSummary(ServerId);

        Assert.Empty(summary.XeSessionFailures);
        Assert.Equal(0, summary.ErroringCollectors);

        var entry = summary.Errors.SingleOrDefault(e => e.CollectorName == Collector);
        Assert.Null(entry);
    }

    [Fact]
    public void Success_Clears_Message_Too()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, Collector, "ERROR", "boom", xeSessionUnavailable: true);
        service.RecordCollectorResult(ServerId, Collector, "SUCCESS");
        service.RecordCollectorResult(ServerId, Collector, "ERROR", "unrelated query failure");

        var summary = service.GetHealthSummary(ServerId);

        /* A later non-XE failure must not resurrect the stale XE message */
        Assert.Empty(summary.XeSessionFailures);
        var erroring = Assert.Single(summary.Errors);
        Assert.False(erroring.XeSessionUnavailable);
        Assert.Null(erroring.XeSessionMessage);
    }

    [Fact]
    public void Failures_Are_Scoped_Per_Server()
    {
        var service = CreateService();

        service.RecordCollectorResult(ServerId, Collector, "ERROR", "boom", xeSessionUnavailable: true);
        service.RecordCollectorResult(ServerId + 1, Collector, "SUCCESS");

        Assert.Single(service.GetHealthSummary(ServerId).XeSessionFailures);
        Assert.Empty(service.GetHealthSummary(ServerId + 1).XeSessionFailures);
    }
}
