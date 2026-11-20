/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Media;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9094: a server whose data collection is turned off (#9084) reads as not watched on its Overview card, not as live status.
/// Its Blocking and Deadlocks rows used to read "0" with green dots, its border went amber (the "collection is not fresh" colour,
/// which says monitoring is failing) and its Last Collect read in a stale colour. Every one of those is a claim about a reading the
/// card does not have: Lite sends a disabled server no queries.
///
/// <para>The same class holds the Last Collect line's wrap: in Server Time the stamp ends in an offset label, and a narrow card broke it
/// inside the label. It sets <see cref="ServerTimeHelper.CurrentDisplayMode"/>, a process-wide static, so it is in the collection the
/// other classes that write it use.</para>
/// </summary>
[Collection("server-time-helper")]
public sealed class DisabledOverviewCardReadsAsNotWatchedTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;

    public void Dispose()
    {
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        ServerTimeHelper.ActiveServerClock = _savedClock;
    }

    private const string Grey = "#888888";
    private const string Green = "#81C784";

    private static string Hex(SolidColorBrush brush) => $"#{brush.Color.R:X2}{brush.Color.G:X2}{brush.Color.B:X2}";

    /// <summary>A card with no blocking, no deadlocks and a collection long past every freshness bound, the shape the issue saw.</summary>
    private static ServerSummaryItem Card(bool isEnabled, bool? isOnline = true, DateTime? lastCollection = null,
        ServerFreshness? freshness = ServerFreshness.Offline) => new()
        {
            DisplayName = "srv",
            ServerName = "srv",
            IsEnabled = isEnabled,
            IsOnline = isOnline,
            CpuPercent = 10,
            OtherProcessCpuPercent = 2,
            MemoryMb = 32768,
            BlockingCount = 0,
            DeadlockCount = 0,
            LastCollectionTime = lastCollection,
            CollectionFreshness = freshness,
        };

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void ADisabledCard_ReadsDashesWithGreyDots_WhateverItsLastConnectionAnswerWas(bool lastAnswerOnline)
    {
        var card = Card(isEnabled: false, isOnline: lastAnswerOnline, lastCollection: DateTime.UtcNow.AddDays(-3));

        Assert.Equal(ServerCardStatus.Disabled, card.CardStatus);
        Assert.Equal("--", card.CpuDisplay);
        Assert.Equal("--", card.MemoryDisplay);
        Assert.Equal("--", card.BlockingDisplay);
        Assert.Equal("--", card.DeadlockDisplay);
        Assert.Equal(Grey, Hex(card.CpuBrush));
        Assert.Equal(Grey, Hex(card.BlockingBrush));
        Assert.Equal(Grey, Hex(card.DeadlockBrush));
        Assert.StartsWith("last collected 3d ago", card.CpuDetail, StringComparison.Ordinal);
    }

    [Fact]
    public void ADisabledCard_TakesTheNeutralBorder_EvenWithStoredFiguresThatWouldBandOrWarn()
    {
        var neutral = Hex(Card(isEnabled: true, freshness: ServerFreshness.Fresh, lastCollection: DateTime.UtcNow).CardBorderBrush);

        /* Never collected, so the old amber "not fresh" arm. */
        var never = Card(isEnabled: false, freshness: ServerFreshness.NeverCollected);
        /* Stored figures that band a live card red and amber, and a collector-error marker. */
        var banded = Card(isEnabled: false, lastCollection: DateTime.UtcNow.AddDays(-3));
        banded.BlockingCount = 4;
        banded.DeadlockCount = 2;
        banded.CpuPercent = 95;
        banded.HasCollectorErrors = true;

        Assert.Equal(neutral, Hex(never.CardBorderBrush));
        Assert.Equal(neutral, Hex(banded.CardBorderBrush));
        Assert.False(never.CollectionIsNotFresh);
        Assert.False(banded.CollectionIsNotFresh);
    }

    [Fact]
    public void ADisabledCards_LastCollect_IsTheStoredTimeOrNever_InTheNeutralGrey_WithNoFaultWord()
    {
        var collected = Card(isEnabled: false, lastCollection: DateTime.UtcNow.AddDays(-3), freshness: ServerFreshness.Offline);
        var never = Card(isEnabled: false, freshness: ServerFreshness.NeverCollected);
        var agedOut = Card(isEnabled: false, freshness: ServerFreshness.Offline);
        /* Registered a decade ago with nothing retained: the aged-out reading, which an enabled card words "None retained (stopped)". */
        agedOut.ApplyCollectionFreshness(DateTime.UtcNow, DateTime.UtcNow.AddYears(-10));
        Assert.True(agedOut.HistoryAgedOut);

        Assert.Equal(Grey, Hex(collected.LastCollectionBrush));
        Assert.Equal(Grey, Hex(never.LastCollectionBrush));
        Assert.DoesNotContain("stopped", collected.LastCollectionDisplay, StringComparison.Ordinal);
        Assert.DoesNotContain("stale", collected.LastCollectionDisplay, StringComparison.Ordinal);
        Assert.Equal("Never", never.LastCollectionDisplay);
        Assert.Equal("Never", agedOut.LastCollectionDisplay);
        Assert.DoesNotContain("stopped", collected.CollectionFreshnessTooltip, StringComparison.Ordinal);
    }

    [Fact]
    public void AnEnabledCard_WithTheSameCounts_StillReadsZeroGreen_AndAnOfflineOneIsUnchanged()
    {
        var online = Card(isEnabled: true, isOnline: true, lastCollection: DateTime.UtcNow, freshness: ServerFreshness.Fresh);
        Assert.Equal("0", online.BlockingDisplay);
        Assert.Equal("0", online.DeadlockDisplay);
        Assert.Equal(Green, Hex(online.BlockingBrush));
        Assert.Equal(Green, Hex(online.DeadlockBrush));
        Assert.Equal(Green, Hex(online.LastCollectionBrush));

        var offline = Card(isEnabled: true, isOnline: false, lastCollection: DateTime.UtcNow.AddDays(-3));
        Assert.Equal("--", offline.BlockingDisplay);
        Assert.Equal(Grey, Hex(offline.BlockingBrush));
        Assert.Equal("#E57373", Hex(offline.CardBorderBrush));
        Assert.EndsWith("(stopped)", offline.LastCollectionDisplay, StringComparison.Ordinal);
        Assert.Equal("#E57373", Hex(offline.LastCollectionBrush));

        /* A stale collection on an enabled, online card still earns its amber border and fault word. */
        var stale = Card(isEnabled: true, isOnline: true, lastCollection: DateTime.UtcNow.AddMinutes(-5), freshness: ServerFreshness.Stale);
        Assert.Equal("#FFD54F", Hex(stale.CardBorderBrush));
        Assert.EndsWith("(stale)", stale.LastCollectionDisplay, StringComparison.Ordinal);
    }

    /// <summary>
    /// The Last Collect line's text, laid out in a plain TextBlock that wraps the way the card's does, at every width from one
    /// that holds a character or two to one that holds the whole line. The offset label, "(UTC+00:00)", must sit on one line at
    /// every width.
    /// </summary>
    [Fact]
    public void TheLastCollectLine_NeverWrapsInsideItsServerTimeOffsetLabel()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var card = Card(isEnabled: true, lastCollection: DateTime.UtcNow.AddMinutes(-5), freshness: ServerFreshness.Stale);
        card.Clock = ServerClock.FixedOffset(0);
        var text = card.LastCollectionDisplay;
        var start = text.IndexOf("(UTC", StringComparison.Ordinal);
        Assert.True(start > 0, "Server Time mode did not add the offset label");

        var split = StaTestThread.Run(() =>
        {
            var block = new TextBlock { Text = text, TextWrapping = TextWrapping.Wrap, FontSize = 11, FontWeight = FontWeights.SemiBold };
            var window = new Window { Content = block, Width = 400, Height = 300, Left = 0, Top = 0 };
            StaTestThread.ShowOffScreen(window);
            try
            {
                var broken = new List<double>();
                var wrapped = false;
                /* Below the label's own width a browser-style break inside a word is forced, so the sweep starts where the label fits. */
                var label = new TextBlock { Text = text[start..], FontSize = 11, FontWeight = FontWeights.SemiBold };
                label.Measure(new Size(double.PositiveInfinity, double.PositiveInfinity));
                for (double width = Math.Ceiling(label.DesiredSize.Width) + 2; width <= 300; width += 2)
                {
                    block.Width = width;
                    ClippedTextProbe.Settle(window);
                    block.UpdateLayout();
                    var origin = block.ContentStart.GetInsertionPosition(LogicalDirection.Forward);
                    var labelEnd = text.IndexOf(')', start);
                    var labelTops = new HashSet<double>();
                    for (var i = start + 1; i <= labelEnd; i++)
                    {
                        var at = origin.GetPositionAtOffset(i, LogicalDirection.Forward)!.GetInsertionPosition(LogicalDirection.Forward);
                        labelTops.Add(Math.Round(at.GetCharacterRect(LogicalDirection.Forward).Top));
                    }

                    wrapped |= block.ActualHeight > block.FontSize * 2.2;
                    if (labelTops.Count > 1)
                    {
                        broken.Add(width);
                    }
                }

                Assert.True(wrapped, "the sweep never wrapped the line, so it proved nothing");
                return broken;
            }
            finally
            {
                window.Close();
            }
        });

        Assert.True(split.Count == 0, $"the offset label split across lines at widths {string.Join(", ", split)}");
    }
}
