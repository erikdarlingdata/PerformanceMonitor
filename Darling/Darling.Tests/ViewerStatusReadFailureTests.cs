/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The sidebar dots and the status bar's "Collection:" field are painted from one freshness read per tick. When that
/// read fails, the last good answer must not stand: a green dot and "Collection: Active" under an unreachable store
/// are claims nothing measured. The dots go back to the unclassified grey and the field says the state is unknown.
/// </summary>
public sealed class ViewerStatusReadFailureTests
{
    private static readonly DateTime Now = new(2026, 11, 5, 12, 0, 0, DateTimeKind.Utc);

    private static DarlingServer Server() =>
        new(1, "target-1", "target-1", isEnabled: true, 16, registeredAt: Now.AddDays(-30));

    [Fact]
    public void ClearFreshness_TakesAnOnlineDotBackToUnknown()
    {
        var server = Server();
        server.ApplyFreshness(Now.AddSeconds(-10), Now);
        Assert.Equal("Online", server.DotStatus);

        server.ClearFreshness();

        Assert.Equal("Unknown", server.DotStatus);
        Assert.Null(server.IsOnline);
        Assert.False(server.CollectionStale);
        Assert.False(server.AwaitingFirstCollection);
    }

    [Fact]
    public void ClearFreshness_TakesAWarningDotBackToUnknown()
    {
        var server = Server();
        server.ApplyFreshness(Now.AddMinutes(-20), Now);
        Assert.Equal("Warning", server.DotStatus);

        server.ClearFreshness();

        Assert.Equal("Unknown", server.DotStatus);
    }

    [Fact]
    public void CollectionStatusLabel_NoCollection_IsIdle() =>
        Assert.Equal("Collection: Idle", MainWindow.CollectionStatusLabel(null, Now));

    [Fact]
    public void CollectionStatusLabel_RecentCollection_IsActive() =>
        Assert.Equal("Collection: Active", MainWindow.CollectionStatusLabel(Now.AddSeconds(-30), Now));

    [Fact]
    public void CollectionStatusLabel_OldCollection_SaysHowLongAgo()
    {
        var label = MainWindow.CollectionStatusLabel(Now.AddMinutes(-5), Now);

        Assert.StartsWith("Collection: ", label, StringComparison.Ordinal);
        Assert.EndsWith(" ago", label, StringComparison.Ordinal);
        Assert.NotEqual("Collection: Active", label);
    }

    [Fact]
    public void CollectionStatusUnknownLabel_ClaimsNeitherActiveNorIdle()
    {
        Assert.StartsWith("Collection: ", MainWindow.CollectionStatusUnknownLabel, StringComparison.Ordinal);
        Assert.NotEqual("Collection: Active", MainWindow.CollectionStatusUnknownLabel);
        Assert.NotEqual("Collection: Idle", MainWindow.CollectionStatusUnknownLabel);
    }
}
