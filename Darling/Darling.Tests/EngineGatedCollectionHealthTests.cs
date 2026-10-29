/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;
using ViewerHealthRow = PerformanceMonitor.Darling.Viewer.CollectorHealthRow;

namespace Darling.Tests;

/// <summary>
/// #6912: a collector that does not run on the server's engine reads <c>not_collected</c> in collection health whatever older runs it has
/// logged, and is not counted as failing. On an Amazon RDS Web or Express target the Extended Events collectors stopped with #6895, and the
/// SESSION_MISSING runs they logged before that stayed inside the seven-day window and banded them FAILING.
/// </summary>
public sealed class EngineGatedCollectionHealthTests
{
    private const string Name = "RDS-WEB-1";

    /// <summary>A collector whose every run in the window failed, the shape the stale pre-gate SESSION_MISSING runs band as.</summary>
    private static CollectorHealth Failing(string collector)
    {
        var last = DateTime.UtcNow.AddMinutes(-1);
        return new CollectorHealth
        {
            CollectorName = collector,
            TotalRuns = 315,
            SuccessCount = 0,
            ErrorCount = 315,
            SessionMissingCount = 315,
            LastRunTime = last,
            LastError = "SESSION_MISSING",
        };
    }

    private static CollectorHealth Healthy(string collector)
    {
        var last = DateTime.UtcNow.AddMinutes(-1);
        return new CollectorHealth { CollectorName = collector, TotalRuns = 10, SuccessCount = 10, LastSuccessTime = last, LastRunTime = last };
    }

    private static DarlingGatedCollectorRows.EngineGate RdsWebOrExpressGate() =>
        new(Name, 2, null, AwsRdsKind.RdsWebOrExpress, FleetEngineGatedCollectors.For(2, null, AwsRdsKind.RdsWebOrExpress));

    [Fact]
    public void RdsWebOrExpress_GatesTheXeCollectors_AndAPlainServerGatesNone()
    {
        var gated = FleetEngineGatedCollectors.For(2, null, AwsRdsKind.RdsWebOrExpress);

        Assert.Contains("attention", gated);
        Assert.Contains("long_query_completions", gated);
        Assert.DoesNotContain("wait_stats", gated);
        Assert.Empty(FleetEngineGatedCollectors.For(2, null, AwsRdsKind.None));
        Assert.Empty(FleetEngineGatedCollectors.For(CollectorEngineCapability.UnknownEngineEdition, null, AwsRdsKind.None));
    }

    [Fact]
    public void AGatedCollectorWithOldSessionMissingRuns_ReadsNotCollected_AndTheOthersStay()
    {
        var gate = RdsWebOrExpressGate();
        var attention = Failing("attention");
        var waits = Healthy("wait_stats");
        Assert.Equal("FAILING", attention.HealthStatus);

        var running = gate.RowsThatRun(new[] { attention, waits });
        var notCollected = JsonSerializer.Serialize(gate.NotCollectedRows(new[] { attention, waits }));

        Assert.Equal(new[] { "wait_stats" }, running.Select(r => r.CollectorName));
        Assert.Contains("\"collector\":\"attention\"", notCollected, StringComparison.Ordinal);
        Assert.Contains("\"status\":\"not_collected\"", notCollected, StringComparison.Ordinal);
        Assert.Contains("Amazon RDS", notCollected, StringComparison.Ordinal);
        Assert.DoesNotContain("wait_stats", notCollected, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCountSurfaces_CountAGatedCollectorAsNotCollected_NotFailing()
    {
        var gate = RdsWebOrExpressGate();
        var rows = new[] { Failing("attention"), Failing("long_query_completions"), Healthy("wait_stats") };

        var tally = new CollectorHealthClassifier.BandTally();
        foreach (var row in rows)
        {
            tally = tally.Add(row.CollectorName, row.HealthStatus, alwaysOnOff: false, engineGated: gate.IsGated(row.CollectorName));
        }

        Assert.Equal(new CollectorHealthClassifier.BandTally(Healthy: 1, Warning: 0, Failing: 0, NotCollected: 2, Total: 3), tally);

        /* Without the gate the same rows are the two failing collectors the viewer's status bar reported. */
        var ungated = new CollectorHealthClassifier.BandTally();
        foreach (var row in rows)
        {
            ungated = ungated.Add(row.CollectorName, row.HealthStatus, alwaysOnOff: false);
        }

        Assert.Equal(2, ungated.Failing);
    }

    [Fact]
    public void AGatedDeadlocksCollector_ReadsAsNoDeadlockCapture_OnTheFleetCard()
    {
        var gated = FleetEngineGatedCollectors.For(2, null, AwsRdsKind.RdsWebOrExpress);
        Assert.Contains("deadlocks", gated);
        var deadlocks = Failing("deadlocks");
        Assert.Equal("FAILING", deadlocks.HealthStatus);

        var counts = DarlingFleetReader.AddCollector(default, deadlocks, alwaysOnOff: false, engineGated: true);

        /* No band at all, the same null a server with no deadlocks row in the window has, which the card classifies as a silent collector.
           The stale FAILING band would have classified as a deadlock capture that was read (a Read source, counted as covered). */
        Assert.Null(counts.DeadlockBand);
        Assert.Equal(FleetDeadlockSource.CollectorSilent, FleetDeadlockCoverage.ClassifyDeadlockSource(isPostgres: false, counts.DeadlockBand));
        Assert.Equal(0, counts.Failing);
        Assert.Equal(1, counts.NotCollected);

        var ungated = DarlingFleetReader.AddCollector(default, deadlocks, alwaysOnOff: false, engineGated: false);
        Assert.Equal("FAILING", ungated.DeadlockBand);
        Assert.Equal(1, ungated.Failing);
    }

    private static ViewerHealthRow ViewerRow(string collector, bool failing)
    {
        var last = DateTime.UtcNow.AddMinutes(-1);
        return new ViewerHealthRow
        {
            CollectorName = collector,
            TotalRuns = 315,
            SuccessCount = failing ? 0 : 315,
            ErrorCount = failing ? 315 : 0,
            LastSuccessTime = failing ? null : last,
            LastRunTime = last,
        };
    }

    [Fact]
    public void TheViewersCollectionHealthTab_ShowsAGatedCollectorAsNotCollected_WithTheEnginesSentence()
    {
        var attention = ViewerRow("attention", failing: true);
        var waits = ViewerRow("wait_stats", failing: false);
        var broken = ViewerRow("query_stats", failing: true);
        Assert.Equal("FAILING", attention.DisplayStatus);

        ViewerServerTab.ApplyEngineGate(new() { attention, waits, broken }, Name, 2, null, AwsRdsKind.RdsWebOrExpress);

        Assert.Equal("not_collected", attention.DisplayStatus);
        Assert.Contains("Amazon RDS", attention.NoteFormatted, StringComparison.Ordinal);
        Assert.Equal("HEALTHY", waits.DisplayStatus);
        /* A collector the engine runs keeps its band, failing or not. */
        Assert.Equal("FAILING", broken.DisplayStatus);
    }

    [Fact]
    public void TheViewersCollectionHealthTab_MakesNoClaimForAServerWhoseEngineIsPlainOrUnread()
    {
        var attention = ViewerRow("attention", failing: true);

        ViewerServerTab.ApplyEngineGate(new() { attention }, "SQL-1", 2, null, AwsRdsKind.None);
        Assert.Equal("FAILING", attention.DisplayStatus);

        ViewerServerTab.ApplyEngineGate(new() { attention }, "SQL-2", CollectorEngineCapability.UnknownEngineEdition, null, AwsRdsKind.None);
        Assert.Equal("FAILING", attention.DisplayStatus);
    }
}
