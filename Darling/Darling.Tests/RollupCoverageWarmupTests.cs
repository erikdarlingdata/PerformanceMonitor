/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4957: where the rollup-floor warm is launched. The warm itself runs against a store (see
/// <c>RollupFloorCacheLiveTests</c>); these pin that the two processes that pay the cold sort inline actually start it —
/// the service after its migrations, the Viewer once its store has answered — and that neither waits for it.
/// </summary>
public sealed class RollupCoverageWarmupTests
{
    [Fact]
    public void TheWorker_LaunchesTheCoverageWarmAfterMigrations_WithoutAwaitingItOnTheStartupPath()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs").Replace("\r\n", "\n");

        var migrate = source.IndexOf("PgMigrations.MigrateAsync(migrateConnection", System.StringComparison.Ordinal);
        var launch = source.IndexOf("var rollupCoverageWarm = RollupCoverageWarmup.RunDelayedAsync(", System.StringComparison.Ordinal);
        var drain = source.IndexOf("await rollupCoverageWarm;", System.StringComparison.Ordinal);
        var loopStop = source.IndexOf("PerformanceMonitor Darling collection loop stopped", System.StringComparison.Ordinal);

        Assert.True(migrate > 0 && launch > migrate, "the warm must launch after migrations");
        Assert.True(drain > launch && drain < loopStop, "the warm is drained only at shutdown, after the collection loop");
        Assert.Single(Regex.Matches(source, @"await rollupCoverageWarm;"));
        Assert.Contains("RollupCoverageWarmup.ServiceStartDelay", source);
    }

    [Fact]
    public void TheViewer_WarmsTheCoverageOnceItsStoreHasAnswered_AndDoesNotWaitForIt()
    {
        var main = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs").Replace("\r\n", "\n");
        var readOnlyProbe = main.IndexOf("await _dataService.DetectReadOnlyAsync();", System.StringComparison.Ordinal);
        var warm = main.IndexOf("_ = _dataService.WarmRollupCoverageAsync(", System.StringComparison.Ordinal);

        Assert.True(readOnlyProbe > 0 && warm > readOnlyProbe, "the Viewer warms only after the store answered its reachability, schema and seat probes");

        var dataService = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.RollupAvailability.cs").Replace("\r\n", "\n");
        Assert.Contains("RollupCoverageWarmup.RunDelayedAsync(_dataSource", dataService);
        Assert.Contains("RollupCoverageWarmup.ViewerStartDelay", dataService);
    }
}
