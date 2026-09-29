/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using Darling.Tests;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Three things start collections: the scheduled sweep, the tab-open sweep and the tab's Refresh button. The
/// first two register with <c>CollectionResetGate</c>, so the size-triggered reset waits for them. The Refresh
/// handler did not: DuckDB let the reset delete <c>monitor.duckdb</c> while the run held a connection to it,
/// connections opened afterwards attached to the deleted instance, the reset's re-initialization bound to it
/// too, and everything collected until the next restart was lost.
///
/// <para>The handler is a lambda inside a WPF window, which this host cannot instantiate, so its wiring is
/// pinned from the source. The gate's own behaviour (a reset waits for a registered run, a run arriving
/// mid-reset waits for the reset) is what <c>CollectionResetGateTests</c> covers.</para>
/// </summary>
public sealed class RefreshCollectionGateTests
{
    [Fact]
    public void TheRefreshHandler_RegistersWithTheResetGate_BeforeRunningAnyCollector()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(ReadSource(Path.Combine("Lite", "MainWindow.xaml.cs")));

        var handlerStart = source.IndexOf("Func<Task> refreshHandler = async () =>", StringComparison.Ordinal);
        Assert.True(handlerStart >= 0, "MainWindow.xaml.cs no longer declares the refresh handler this pins");

        var handler = CSharpSourceWalker.BraceBalanced(source, source.IndexOf('{', handlerStart));

        var registration = handler.IndexOf("CollectionResetGate.BeginCollectionAsync", StringComparison.Ordinal);
        var firstRun = handler.IndexOf("RunCollectorAsync(", StringComparison.Ordinal);

        Assert.True(firstRun >= 0, "the refresh handler no longer runs collectors through RunCollectorAsync");
        Assert.True(registration >= 0, "the refresh handler runs collectors without registering with CollectionResetGate");
        Assert.True(registration < firstRun, "the refresh handler registers with CollectionResetGate only after a collector has already run");
        Assert.Contains("using var", handler[..registration], StringComparison.Ordinal);
    }

    /// <summary>
    /// The registration belongs in the callers, not in <c>RunCollectorAsync</c>: that method also runs inside
    /// the registered sweeps, and a nested registration there would wait out the reset's drain timeout every
    /// time a reset was pending.
    /// </summary>
    [Fact]
    public void RunCollectorAsync_DoesNotRegisterWithTheResetGateItself()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(ReadSource(Path.Combine("Lite", "Services", "RemoteCollectorService.cs")));

        var methodStart = source.IndexOf("public async Task RunCollectorAsync(ServerConnection server, string collectorName, DateTime? scheduledAtUtc, CancellationToken cancellationToken)", StringComparison.Ordinal);
        Assert.True(methodStart >= 0, "RemoteCollectorService.cs no longer declares the RunCollectorAsync overload this pins");

        var body = CSharpSourceWalker.BraceBalanced(source, source.IndexOf('{', methodStart));

        Assert.DoesNotContain("CollectionResetGate", body, StringComparison.Ordinal);
    }

    private static string ReadSource(string relative)
    {
        var path = Path.Combine(RepoRoot(), relative);
        Assert.True(File.Exists(path), $"source not found: {path}");
        return File.ReadAllText(path);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.True(dir is not null, "PerformanceMonitor.sln not found above the test source");
        return dir!;
    }
}
