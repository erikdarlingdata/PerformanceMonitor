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
/// #6155: a Darling Viewer PostgreSQL panel with no rows does not claim a healthy answer when the server is offline.
/// </summary>
public sealed class ViewerPostgresOfflineEmptyTests
{
    private const string Healthy = "This server has no replication slots. Zero rows is the healthy answer here, not a missing read.";

    [Fact]
    public void AnEmptyPanel_OnAnOfflineServer_SaysItMayBeMissingData_NotTheHealthySentence()
    {
        var note = ViewerServerTab.EmptyNote(null, offline: true, rowCount: 0, Healthy);

        Assert.Equal(ViewerServerTab.OfflineEmptyText, note);
        Assert.DoesNotContain("healthy answer here", note, StringComparison.Ordinal);
        Assert.StartsWith("This server is offline", note, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEmptyPanel_OnAServerBeingCollected_KeepsItsHealthySentence()
    {
        Assert.Equal(Healthy, ViewerServerTab.EmptyNote(null, offline: false, rowCount: 0, Healthy));
    }

    [Fact]
    public void APanelWithRows_AndAnEngineGap_AreUnchangedByOffline()
    {
        Assert.Equal(string.Empty, ViewerServerTab.EmptyNote(null, offline: true, rowCount: 3, Healthy));
        Assert.Equal("gap", ViewerServerTab.EmptyNote("gap", offline: true, rowCount: 0, Healthy));
    }

    [Fact]
    public void APanelThatPassedNoSentence_GetsNoneWhenOffline()
    {
        Assert.Equal(string.Empty, ViewerServerTab.EmptyNote(null, offline: true, rowCount: 0, string.Empty));
    }
}
