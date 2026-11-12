/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7384: the count of NO_PERMISSIONS plus EXTENSION_MISSING collectors (#6932) was labelled "no permission", which sent a person
/// looking for a grant when the collectors only needed an extension. The viewer's status bar and Overview card now say "need setup".
/// The web viewer's status bar and fleet chip build the same words in app.js and fleet.js, which have no seam short of a browser
/// run, so no source-text test stands in for them (#5833).
/// </summary>
public sealed class CollectorNeedSetupLabelTests
{
    [Fact]
    public void StatusBar_CountsAMissingExtensionAsNeedSetup_NotNoPermission()
    {
        var tally = new CollectorHealthClassifier.BandTally()
            .Add("pg_buffer_usage", CollectorHealthClassifier.ExtensionMissing, false, false)
            .Add("pg_kernel_stats", CollectorHealthClassifier.ExtensionMissing, false, false)
            .Add("pg_predicate_stats", CollectorHealthClassifier.ExtensionMissing, false, false);

        var text = CollectorCountText.StatusBar(tally);

        Assert.Equal("Collectors: 0 OK, 3 need setup", text);
        Assert.DoesNotContain("no permission", text, System.StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void StatusBar_CountsARefusedCollectorAndAMissingExtensionTogether()
    {
        var tally = new CollectorHealthClassifier.BandTally()
            .Add("wait_stats", CollectorHealthClassifier.Healthy, false, false)
            .Add("tempdb_stats", CollectorHealthClassifier.NoPermissions, false, false)
            .Add("pg_kernel_stats", CollectorHealthClassifier.ExtensionMissing, false, false);

        Assert.Equal("Collectors: 1 OK, 2 need setup", CollectorCountText.StatusBar(tally));
    }

    [Fact]
    public void StatusBar_LeavesTheClauseOut_WhenNothingNeedsSetup()
    {
        var tally = new CollectorHealthClassifier.BandTally()
            .Add("wait_stats", CollectorHealthClassifier.Healthy, false, false);

        Assert.Equal("Collectors: 1 OK", CollectorCountText.StatusBar(tally));
    }

    [Fact]
    public void OverviewCardDetail_SaysNeedSetup()
    {
        var card = new ServerSummaryItem
        {
            ServerName = "t",
            ServerId = 1,
            HealthyCollectorCount = 25,
            NoPermissionCollectorCount = 3,
            LastCollectionTime = System.DateTime.UtcNow,
        };

        var detail = card.CollectorDetail;

        Assert.EndsWith(", Need setup: 3", detail, System.StringComparison.Ordinal);
        Assert.DoesNotContain("No permission", detail, System.StringComparison.Ordinal);
    }
}
