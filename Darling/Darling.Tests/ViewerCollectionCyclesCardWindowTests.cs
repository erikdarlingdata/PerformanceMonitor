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
/// #8533: the Collection Cycles card carries two lines read over different windows. The skipped-slots line is a 24 hour read; the
/// connection-failure line reads the grid's rows, which hold each collector's runs for the trailing seven days. The fact builds the
/// real server tab on the shared STA thread with the theme loaded, fills the card the way its load does when the 24 hour read finds
/// nothing and the rows hold a drop 43 hours old, and reads what the card draws: the subtitle, whole, and the line under it.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class ViewerCollectionCyclesCardWindowTests
{
    private const string PreLoginHandshake =
        "A connection was successfully established with the server, but then an error occurred during the pre-login handshake. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)";

    [Theory]
    [InlineData(1280, 800)]
    [InlineData(900, 600)]
    public void ADropFortyThreeHoursOld_SitsUnderASubtitleThatGivesEachLinesWindow_DrawnWhole(int width, int height)
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(width), height, (tab, window) =>
        {
            var found = new List<string>();
            var readAt = new DateTime(2026, 11, 9, 5, 32, 5, DateTimeKind.Utc);
            var dropped = readAt.AddHours(-43);
            var health = Enumerable.Range(0, 12)
                .Select(i => new CollectorHealthRow { CollectorName = $"collector_{i:00}", LastErrorTime = dropped.AddSeconds(i * 3), LastError = PreLoginHandshake, ErrorCount = 1 })
                .ToList();

            Find<TextBlock>(tab, "SkippedCyclesLine").Text = SkippedCycleLine.Format(SkippedCycleSummary.None);
            Invoke(tab, "ShowConnectionFailures", health);
            var failure = Find<TextBlock>(tab, "ConnectionFailureLine");
            Reveal(failure);
            Settle(window);

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

            if (!subtitle.Text.Contains("7 days", StringComparison.Ordinal))
            {
                found.Add($"the subtitle \"{subtitle.Text}\" does not give the 7 days the connection failures are read over");
            }

            var natural = ClippedTextProbe.NaturalWidth(subtitle);
            if (natural > subtitle.ActualWidth + 0.5)
            {
                found.Add($"the subtitle needs {natural:F0} px and is trimmed to {subtitle.ActualWidth:F0} px");
            }

            if (!failure.IsVisible)
            {
                found.Add("the connection-failure line is not shown for a drop that reached 12 collectors");
            }
            else if (!failure.Text.Contains("in the last 7 days", StringComparison.Ordinal))
            {
                found.Add($"the line \"{failure.Text}\" does not say which days it speaks for");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }
}
