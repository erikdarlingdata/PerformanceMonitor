/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9769: the Scheduler's "N offline, skipped" count says how many servers a sweep left out and not which or why, so each skipped
/// server is named with its reason once per state change: not again every cycle while the reason holds, again when the reason
/// changes, and again for a later outage after the server was online in between.
///
/// <para>Collection <c>app-logger-statics</c>: the assertions drain the process-wide log buffer the other logger tests share.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class SkippedServerAnnouncementTests
{
    private static RemoteCollectorService CreateService() =>
        new(duckDb: null!, serverManager: null!, scheduleManager: null!);

    private static ServerConnection Server(string name) =>
        new() { ServerName = name + "-" + Guid.NewGuid().ToString("N"), DisplayName = name + "-" + Guid.NewGuid().ToString("N") };

    private static string[] SkipLinesFor(ServerConnection server) =>
        AppLogger.DrainBufferedLines().Where(l => l.Contains(server.DisplayName, StringComparison.Ordinal)).ToArray();

    [Fact]
    public void ASkippedServer_IsNamedWithItsReason_OnceWhileTheReasonHolds()
    {
        var service = CreateService();
        var server = Server("skipped");
        AppLogger.DrainBufferedLines();

        for (var cycle = 0; cycle < 5; cycle++)
        {
            service.AnnounceSkippedServer(server, "REASON-ONE");
        }

        var line = Assert.Single(SkipLinesFor(server));
        Assert.Contains("REASON-ONE", line, StringComparison.Ordinal);
        Assert.Contains("[Scheduler]", line, StringComparison.Ordinal);
    }

    [Fact]
    public void AChangedReason_IsAnnouncedAgain()
    {
        var service = CreateService();
        var server = Server("changed");
        AppLogger.DrainBufferedLines();

        service.AnnounceSkippedServer(server, "REASON-ONE");
        service.AnnounceSkippedServer(server, "REASON-TWO");
        service.AnnounceSkippedServer(server, "REASON-TWO");

        var lines = SkipLinesFor(server);
        Assert.Equal(2, lines.Length);
        Assert.Contains("REASON-ONE", lines[0], StringComparison.Ordinal);
        Assert.Contains("REASON-TWO", lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public void AServerThatCameBackOnline_IsAnnouncedAgainWhenItIsSkippedLater()
    {
        var service = CreateService();
        var server = Server("returned");
        AppLogger.DrainBufferedLines();

        service.AnnounceSkippedServer(server, "REASON-ONE");
        service.NoteServerOnline(server);
        service.AnnounceSkippedServer(server, "REASON-ONE");

        Assert.Equal(2, SkipLinesFor(server).Count(l => l.Contains("REASON-ONE", StringComparison.Ordinal)));
    }

    [Fact]
    public void EachServer_IsAnnouncedOnItsOwn()
    {
        var service = CreateService();
        var first = Server("first");
        var second = Server("second");
        AppLogger.DrainBufferedLines();

        service.AnnounceSkippedServer(first, "SAME-REASON");
        service.AnnounceSkippedServer(second, "SAME-REASON");

        var lines = AppLogger.DrainBufferedLines();
        Assert.Single(lines, l => l.Contains(first.DisplayName, StringComparison.Ordinal));
        Assert.Single(lines, l => l.Contains(second.DisplayName, StringComparison.Ordinal));
    }

    [Fact]
    public void ASkippedServerWithNoRecordedReason_IsStillNamed_Once()
    {
        var service = CreateService();
        var server = Server("noreason");
        AppLogger.DrainBufferedLines();

        service.AnnounceSkippedServer(server, null);
        service.AnnounceSkippedServer(server, "  ");

        Assert.Single(SkipLinesFor(server));
    }
}
