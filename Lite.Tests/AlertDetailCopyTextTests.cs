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

    /// <summary>#7124: a held row's copy says why, in the header under Status; a row with no routing copies no such line.</summary>
    [Fact]
    public void Build_CarriesTheRoutingReason_InTheHeader_OnlyWhenTheRowHasOne()
    {
        const string reason = "Held for the daily digest: 1 blocking event at 12 s, below the 15 s page bar.";
        var held = AlertContextSerializer.Serialize(new AlertContext { Routing = new AlertRoutingDto("digest", reason) });

        var text = AlertDetailCopyText.Build(Header, held, "flat text");
        var lines = new List<string>(text.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None));
        Assert.Equal(lines.IndexOf("Status: Sent") + 1, lines.IndexOf("Routing reason: " + reason));

        Assert.DoesNotContain("Routing reason", AlertDetailCopyText.Build(Header, TwoItemContextJson(), null), StringComparison.Ordinal);
        Assert.DoesNotContain("Routing reason", AlertDetailCopyText.Build(Header, null, "flat text"), StringComparison.Ordinal);
    }

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

    private static readonly AlertNewestSample Sample =
        new("CPU", new DateTime(2026, 10, 15, 8, 55, 0, DateTimeKind.Utc), TimeSpan.FromSeconds(320));

    private static int Occurrences(string text, string line) => text.Split(line).Length - 1;

    [Fact]
    public void Build_WithANewestSample_PutsTheAgeLineOnce_UnderStatus_AsTheWindowDoes()
    {
        var json = AlertContextSerializer.Serialize(new AlertContext
        {
            NewestSample = Sample,
            Details = new List<AlertDetailItem>
            {
                new() { Heading = "Blocking Chain 1", Fields = new List<(string Label, string Value)> { ("Database", "Tenant1") } }
            }
        });
        var ageLine = AlertDetailText.SampleAgeLine(Sample)!;

        var text = AlertDetailCopyText.Build(Header with { Muted = true }, json, detailText: null);

        Assert.Equal(1, Occurrences(text, ageLine));
        Assert.Contains(
            "Status: Sent" + Environment.NewLine + ageLine + Environment.NewLine + "This alert was muted by a mute rule"
            + Environment.NewLine + Environment.NewLine + "Blocking Chain 1",
            text);
    }

    [Fact]
    public void Build_WhenTheFlatTextAlreadyCarriesTheAgeLine_DoesNotRepeatIt()
    {
        /* CPU's shape (#5642): a context with the sample and no Details, and a detail_text that already ends in the line. */
        var json = AlertContextSerializer.Serialize(new AlertContext { NewestSample = Sample });
        var ageLine = AlertDetailText.SampleAgeLine(Sample)!;

        var text = AlertDetailCopyText.Build(Header, json, "CPU at 98%" + Environment.NewLine + ageLine);

        Assert.Equal(1, Occurrences(text, ageLine));
        Assert.EndsWith("CPU at 98%" + Environment.NewLine + ageLine + Environment.NewLine, text);
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
