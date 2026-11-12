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
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;
using static Darling.Tests.ViewerPageLayoutProbe;

namespace Darling.Tests;

/// <summary>
/// #8533: the Collection Cycles card carries two lines read over different windows. The skipped-slots line is a 24 hour read and is
/// always shown. The connection-failure line reads the grid's rows, which hold each collector's runs for the trailing seven days,
/// and is shown only when those rows hold a failure that reached several collectors. Each fact builds the real server tab on the
/// shared STA thread with the theme loaded, fills the card through the tab's own <c>ShowConnectionFailures</c>, and reads what
/// the card draws. That the rows stop at seven days is pinned through the read in <see cref="ViewerCollectionHealthErrorWindowLiveTests"/>.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerCollectionCyclesCardWindowTests
{
    private const string PreLoginHandshake =
        "A connection was successfully established with the server, but then an error occurred during the pre-login handshake. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)";

    /// <summary>
    /// With no failure to word the card draws the skipped-slots line alone, so a subtitle that also captioned connection failures
    /// over seven days would read as an all-clear the rows cannot give: they hold each collector's newest error only.
    /// </summary>
    [Theory]
    [InlineData(1280, 800)]
    [InlineData(900, 600)]
    public void WithNoFailureToWord_TheSubtitleCaptionsTheSkippedSlotsAlone_DrawnWhole(int width, int height)
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(width), height, (tab, window) =>
        {
            var found = new List<string>();
            var skipped = Find<TextBlock>(tab, "SkippedCyclesLine");
            skipped.Text = SkippedCycleLine.Format(SkippedCycleSummary.None);
            Invoke(tab, "ShowConnectionFailures", new List<CollectorHealthRow>());
            Reveal(skipped);
            Settle(window);

            if (Find<TextBlock>(tab, "ConnectionFailureLine").IsVisible)
            {
                found.Add("a connection-failure line is shown for rows that hold no error");
            }

            var card = Find<ChartPanel>(tab, "CollectionHealthCard");
            if (card.Template?.FindName("PART_Subtitle", card) is not TextBlock subtitle || !subtitle.IsVisible)
            {
                found.Add("the Collection Cycles card drew no subtitle");
                return found;
            }

            if (!subtitle.Text.Contains("24 hours", StringComparison.Ordinal))
            {
                found.Add($"the subtitle \"{subtitle.Text}\" does not give the 24 hours the skipped slots are read over");
            }

            if (subtitle.Text.Contains("connection failure", StringComparison.OrdinalIgnoreCase))
            {
                found.Add($"the subtitle \"{subtitle.Text}\" captions connection failures over a card that shows no connection-failure line");
            }

            var natural = ClippedTextProbe.NaturalWidth(subtitle);
            if (natural > subtitle.ActualWidth + 0.5)
            {
                found.Add($"the subtitle needs {natural:F0} px and is trimmed to {subtitle.ActualWidth:F0} px");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>Rows whose newest error is one drop that reached twelve collectors: the tab words it, and the line says which days it speaks for.</summary>
    [Fact]
    public void ADropTheRowsHold_IsWordedByTheTab_AgainstItsOwnSevenDays()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, (tab, window) =>
        {
            var found = new List<string>();
            var dropped = new DateTime(2026, 11, 7, 10, 32, 5, DateTimeKind.Utc);
            var health = Enumerable.Range(0, 12)
                .Select(i => new CollectorHealthRow { CollectorName = $"collector_{i:00}", LastErrorTime = dropped.AddSeconds(i * 3), LastError = PreLoginHandshake, ErrorCount = 1 })
                .ToList();

            Invoke(tab, "ShowConnectionFailures", health);
            var failure = Find<TextBlock>(tab, "ConnectionFailureLine");
            Reveal(failure);
            Settle(window);

            if (!failure.IsVisible)
            {
                found.Add("the connection-failure line is not shown for a drop that reached 12 collectors");
            }
            else if (!failure.Text.Contains("reached 12 collectors: each missed that one run and has no other error in the last 7 days", StringComparison.Ordinal))
            {
                found.Add($"the line \"{failure.Text}\" does not word the drop against the rows' seven days");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }
}
