/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5847: the viewer's Alert Details window. Its field values are selectable, read-only text boxes that stay out of the
/// tab order; "Copy all details" carries the header and every item; the full-chain link shows only for a Blocking or
/// Deadlocks alert whose opener can navigate. Built on the shared STA thread and laid out without being shown, so no
/// size is stored.
/// </summary>
public sealed class AlertDetailWindowTests
{
    private static ViewerAlertRow Row(string metric) => new()
    {
        AlertTime = new DateTime(2026, 10, 15, 9, 0, 0),
        ServerName = "SQL01",
        MetricName = metric,
        CurrentValue = 98,
        ThresholdValue = 1,
        AlertSent = true,
        NotificationType = "Email",
        Muted = false,
        ContextJson = AlertContextSerializer.Serialize(new AlertContext
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
                    Fields = new List<(string Label, string Value)> { ("Database", "Tenant2") }
                }
            }
        })
    };

    private static IEnumerable<T> Descendants<T>(DependencyObject parent) where T : DependencyObject
    {
        for (var i = 0; i < VisualTreeHelper.GetChildrenCount(parent); i++)
        {
            var child = VisualTreeHelper.GetChild(parent, i);
            if (child is T match)
            {
                yield return match;
            }

            foreach (var nested in Descendants<T>(child))
            {
                yield return nested;
            }
        }
    }

    private static void LayOut(Window window)
    {
        var root = (FrameworkElement)window.Content;
        root.Measure(new Size(800, 900));
        root.Arrange(new Rect(0, 0, 800, 900));
        root.UpdateLayout();
    }

    [Fact]
    public void FieldValues_AreReadOnlyTextBoxes_OutOfTheTabOrder()
    {
        StaTestThread.Run(() =>
        {
            var window = new AlertDetailWindow(Row("Blocking Detected"));
            try
            {
                LayOut(window);

                var values = Descendants<TextBox>(window.DetailItems).Where(t => t.Name == "FieldValue").ToList();
                Assert.Equal(new[] { "Tenant1", "EXEC dbo.Receive", "Tenant2" }, values.Select(v => v.Text));
                Assert.All(values, v =>
                {
                    Assert.True(v.IsReadOnly);
                    Assert.False(v.IsTabStop);
                });

                Assert.IsType<TextBox>(window.ServerText);
                Assert.Equal("SQL01", window.ServerText.Text);
                Assert.True(window.ServerText.IsReadOnly);
                Assert.False(window.ServerText.IsTabStop);
            }
            finally
            {
                window.Close();
            }
        });
    }

    /// <summary>#5847 review: an alert that carries a sample age and no detail items shows the age as a header row, as
    /// Lite does, and leaves the Details panel closed instead of opening it just to hold that line at its bottom.</summary>
    [Fact]
    public void SampleAge_WithNoDetailItems_IsAHeaderRow_AndOpensNoDetailsPanel()
    {
        var sample = new AlertNewestSample("CPU", new DateTime(2026, 10, 15, 8, 55, 0, DateTimeKind.Utc), TimeSpan.FromSeconds(320));
        var row = new ViewerAlertRow
        {
            AlertTime = new DateTime(2026, 10, 15, 9, 0, 0),
            ServerName = "SQL01",
            MetricName = "High CPU",
            CurrentValue = 98,
            ThresholdValue = 80,
            AlertSent = true,
            NotificationType = "Email",
            Muted = false,
            ContextJson = AlertContextSerializer.Serialize(new AlertContext { NewestSample = sample })
        };

        StaTestThread.Run(() =>
        {
            var window = new AlertDetailWindow(row);
            try
            {
                LayOut(window);

                Assert.Equal(AlertDetailText.SampleAgeLine(sample), window.SampleAgeText.Text);
                Assert.Equal(Visibility.Visible, window.SampleAgeText.Visibility);
                Assert.Equal(Visibility.Visible, window.SampleAgeLabel.Visibility);
                Assert.Equal(Visibility.Collapsed, window.DetailPanel.Visibility);
            }
            finally
            {
                window.Close();
            }
        });
    }

    /* #7124: why an alert was held or paged. The reader is the one the MCP history read uses; the window, its copy text and the
       grid row's tooltip all read through it. */
    private const string HeldReason = "Held for the daily digest: 1 blocking event at 12 s, below the 15 s page bar.";

    private static string RoutedJson(string route, string reason) =>
        AlertContextSerializer.Serialize(new AlertContext { Routing = new AlertRoutingDto(route, reason) });

    private static ViewerAlertRow RowWith(string? contextJson) => new()
    {
        AlertTime = new DateTime(2026, 10, 15, 9, 0, 0),
        ServerName = "SQL01",
        MetricName = "Blocking Detected",
        CurrentValue = 1,
        ThresholdValue = 3,
        AlertSent = false,
        NotificationType = "digest",
        Muted = false,
        ContextJson = contextJson
    };

    [Fact]
    public void RoutingReason_IsReadForAHeldRow_AndAPagedFinding_AndNullWhenThereIsNone()
    {
        Assert.Equal(HeldReason, AlertDetailText.RoutingReasonFromJson(RoutedJson("digest", HeldReason)));
        Assert.Equal("Paged: two independent signals agree.", AlertDetailText.RoutingReasonFromJson(RoutedJson("page", "  Paged: two independent signals agree.  ")));

        // A row with context but no routing (an engine alert), no context, an empty reason, and foreign JSON.
        Assert.Null(AlertDetailText.RoutingReasonFromJson(AlertContextSerializer.Serialize(new AlertContext())));
        Assert.Null(AlertDetailText.RoutingReasonFromJson(null));
        Assert.Null(AlertDetailText.RoutingReasonFromJson(""));
        Assert.Null(AlertDetailText.RoutingReasonFromJson(RoutedJson("digest", "   ")));
        Assert.Null(AlertDetailText.RoutingReasonFromJson("not json"));
    }

    [Fact]
    public void RoutingReason_IsAHeaderRow_AndInTheCopyText_ForAHeldRow_AndCollapsedForOneWithout()
    {
        StaTestThread.Run(() =>
        {
            var held = new AlertDetailWindow(RowWith(RoutedJson("digest", HeldReason)));
            var plain = new AlertDetailWindow(RowWith(AlertContextSerializer.Serialize(new AlertContext())));
            try
            {
                LayOut(held);
                LayOut(plain);

                Assert.Equal(HeldReason, held.RoutingReasonText.Text);
                Assert.Equal(Visibility.Visible, held.RoutingReasonLabel.Visibility);
                Assert.Equal(Visibility.Visible, held.RoutingReasonText.Visibility);
                Assert.Contains("Routing reason: " + HeldReason, held.BuildCopyText(), StringComparison.Ordinal);

                Assert.Equal(Visibility.Collapsed, plain.RoutingReasonLabel.Visibility);
                Assert.Equal(Visibility.Collapsed, plain.RoutingReasonText.Visibility);
                Assert.DoesNotContain("Routing reason", plain.BuildCopyText(), StringComparison.Ordinal);
            }
            finally
            {
                held.Close();
                plain.Close();
            }
        });
    }

    [Fact]
    public void RoutingReason_OnTheGridRow_ReadsTheContext_AndIsNullWithoutOne()
    {
        Assert.Equal(HeldReason, RowWith(RoutedJson("digest", HeldReason)).RoutingReason);
        Assert.Null(RowWith(null).RoutingReason);
    }

    [Fact]
    public void CopyText_CarriesTheHeader_AndBothItems()
    {
        StaTestThread.Run(() =>
        {
            var window = new AlertDetailWindow(Row("Blocking Detected"));
            try
            {
                var text = window.BuildCopyText();

                Assert.StartsWith("Time: ", text);
                Assert.Contains("Server: SQL01" + Environment.NewLine + "Metric: Blocking Detected", text);
                Assert.Contains(Environment.NewLine + "Blocking Chain 1" + Environment.NewLine + "Database: Tenant1"
                    + Environment.NewLine + "Blocked Query: EXEC dbo.Receive" + Environment.NewLine, text);
                Assert.Contains(Environment.NewLine + "Blocking Chain 2" + Environment.NewLine + "Database: Tenant2", text);
            }
            finally
            {
                window.Close();
            }
        });
    }

    /* #5825 with #5847: a resolution row's three fields are copied below Status, as the window shows them; a fire row
       copies none of them. */
    [Fact]
    public void CopyText_CarriesAResolutionRowsSend_AndAFireRowCarriesNone()
    {
        StaTestThread.Run(() =>
        {
            var resolution = new ViewerAlertRow
            {
                AlertTime = new DateTime(2026, 10, 15, 9, 30, 0),
                ServerName = "SQL01",
                MetricName = "High CPU",
                AlertSent = true,
                CurrentValue = 0,
                ThresholdValue = 0,
                NotificationType = "webhook",
                Muted = false,
                ResolutionSentAt = new DateTime(2026, 10, 15, 13, 30, 5, DateTimeKind.Utc),
                ResolutionChannels = "Slack,Email",
                ResolutionOutcome = "sent",
            };
            var shown = new AlertDetailWindow(resolution);
            var fire = new AlertDetailWindow(Row("High CPU"));
            try
            {
                var text = shown.BuildCopyText();
                Assert.Contains("Status: " + resolution.StatusDisplay + Environment.NewLine
                    + "Resolution sent: 2026-10-15 13:30:05 UTC" + Environment.NewLine
                    + "Resolution channels: Slack,Email" + Environment.NewLine
                    + "Resolution outcome: sent" + Environment.NewLine, text);
                Assert.Equal("sent", shown.ResolutionOutcomeText.Text);

                Assert.DoesNotContain("Resolution", fire.BuildCopyText());
            }
            finally
            {
                shown.Close();
                fire.Close();
            }
        });
    }

    [Theory]
    [InlineData("Blocking Detected", true, true)]
    [InlineData("Deadlocks Detected", true, true)]
    [InlineData("Blocking Detected", false, false)]
    [InlineData("High CPU", true, true)]
    public void FullChainLink_ShowsOnlyWhenTheShellGivesAnOpener_WithTheTextItGave(string metric, bool hasOpener, bool shown)
    {
        StaTestThread.Run(() =>
        {
            var opened = 0;
            var window = new AlertDetailWindow(Row(metric), hasOpener ? new AlertChainOpener("Open the Activity tab at this time", () => opened++) : null);
            try
            {
                Assert.Equal(shown ? Visibility.Visible : Visibility.Collapsed, window.FullChainLinkText.Visibility);
                if (shown)
                {
                    Assert.Equal("Open the Activity tab at this time", window.FullChainLinkRun.Text);
                    window.FullChainLink.RaiseEvent(new RoutedEventArgs(System.Windows.Documents.Hyperlink.ClickEvent));
                    Assert.Equal(1, opened);
                }
            }
            finally
            {
                window.Close();
            }
        });
    }
}
