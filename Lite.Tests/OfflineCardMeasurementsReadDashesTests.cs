/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8465: an offline card shows no measurement at all. Its stored figures are from before the server went dark, and a calm
/// memory figure or a CPU percentage under a red Offline reads as live health, so every measurement display reads "--" and
/// the CPU row's detail says when the server was last collected: the Darling card's rule. The figures stay on the item, because
/// the alert engine reads them. Built on the item alone, as the Darling twin is, so the rule holds for every card before any
/// control renders it, and the next measurement added to a card cannot miss it the way Memory did.
/// </summary>
public sealed class OfflineCardMeasurementsReadDashesTests
{
    /// <summary>A card with a reading in every measurement row and a Last Collect stamp twelve days back.</summary>
    private static ServerSummaryItem Card(bool? isOnline) => new()
    {
        DisplayName = "dark",
        ServerName = "dark",
        IsOnline = isOnline,
        CpuPercent = 20,
        OtherProcessCpuPercent = 5,
        MemoryMb = 32768,
        BlockingCount = 3,
        DeadlockCount = 2,
        LastCollectionTime = DateTime.UtcNow.AddDays(-12),
    };

    [Fact]
    public void AnOfflineCard_ReadsDashesOnEveryMeasurement_AndSaysWhenItWasLastCollected()
    {
        var card = Card(isOnline: false);

        Assert.Equal("--", card.CpuDisplay);
        Assert.Equal("--", card.MemoryDisplay);
        Assert.Equal("--", card.BlockingDisplay);
        Assert.Equal("--", card.DeadlockDisplay);
        Assert.StartsWith("last collected 1w ago", card.CpuDetail, StringComparison.Ordinal);
    }

    [Fact]
    public void AnOfflineCard_ThatWasNeverCollected_SaysSo()
    {
        var card = Card(isOnline: false);
        card.LastCollectionTime = null;

        Assert.Equal("--", card.MemoryDisplay);
        Assert.Equal("no recent collection", card.CpuDetail);
    }

    [Fact]
    public void AnOnlineCard_StillShowsEveryFigure()
    {
        var card = Card(isOnline: true);

        Assert.Equal("25% (SQL 20%)", card.CpuDisplay);
        Assert.Equal("Other: 5%", card.CpuDetail);
        Assert.Equal("32.0 GB", card.MemoryDisplay);
        Assert.Equal("3", card.BlockingDisplay);
        Assert.Equal("2", card.DeadlockDisplay);
    }
}
