/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #5847: the Alert Details window's "Copy all details" text, which Lite and the Darling Viewer both build through
/// <see cref="AlertDetailCopyText.Build"/>, and the "open the full chain" link's view and window.
/// </summary>
public sealed class AlertDetailCopyTextTests
{
    private static readonly AlertDetailCopyHeader Header = new(
        "2026-10-15 09:00:00", "SQL01", "Blocking Detected", "98", "1", "Email", "Sent", Muted: false);

    private static string TwoItemContextJson() => AlertContextSerializer.Serialize(new AlertContext
    {
        Details = new List<AlertDetailItem>
        {
            new()
            {
                Heading = "Blocking Chain 1",
                Fields = new List<(string Label, string Value)> { ("Database", "Tenant1"), ("Blocked Query", "EXEC dbo.Receive") }
            },
            new()
            {
                Heading = "Blocking Chain 2",
                Fields = new List<(string Label, string Value)> { ("Database", "Tenant2"), ("Wait Range", "13-204 s") }
            }
        }
    });

    [Fact]
    public void Build_CarriesTheHeader_ThenEveryItemAndField_InOrder()
    {
        var text = AlertDetailCopyText.Build(Header, TwoItemContextJson(), "flat text the structured view replaces");

        var expected = string.Join(Environment.NewLine,
            "Time: 2026-10-15 09:00:00",
            "Server: SQL01",
            "Metric: Blocking Detected",
            "Current Value: 98",
            "Threshold: 1",
            "Notification: Email",
            "Status: Sent",
            "",
            "Blocking Chain 1",
            "Database: Tenant1",
            "Blocked Query: EXEC dbo.Receive",
            "",
            "Blocking Chain 2",
            "Database: Tenant2",
            "Wait Range: 13-204 s") + Environment.NewLine;
        Assert.Equal(expected, text);
    }

    [Fact]
    public void Build_WithNoContext_CopiesTheFlatDetailText_AndSaysWhenMuted()
    {
        var muted = Header with { Muted = true };

        var text = AlertDetailCopyText.Build(muted, contextJson: null, detailText: "Blocked for 204 s in Tenant1\n");

        Assert.Contains("Status: Sent" + Environment.NewLine + "This alert was muted by a mute rule", text);
        Assert.EndsWith(Environment.NewLine + Environment.NewLine + "Blocked for 204 s in Tenant1" + Environment.NewLine, text);
    }

    [Theory]
    [InlineData("Blocking Detected", AlertChainView.Blocking)]
    [InlineData("Deadlocks Detected", AlertChainView.Deadlocks)]
    [InlineData("High CPU", AlertChainView.None)]
    [InlineData(null, AlertChainView.None)]
    public void ChainLink_OpensAViewOnlyForBlockingAndDeadlocks(string? metric, AlertChainView view)
    {
        Assert.Equal(view, AlertChainLink.ViewFor(metric));
        Assert.Equal(view == AlertChainView.None, AlertChainLink.LinkText(view) is null);
    }

    [Fact]
    public void ChainLink_WindowSurroundsTheAlertTime()
    {
        var alert = new DateTime(2026, 10, 15, 9, 0, 0);

        var (from, to) = AlertChainLink.WindowUtc(alert);

        Assert.Equal(new DateTime(2026, 10, 15, 8, 0, 0), from);
        Assert.Equal(new DateTime(2026, 10, 15, 9, 15, 0), to);
    }
}
