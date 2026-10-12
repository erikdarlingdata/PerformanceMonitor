/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Release click-through findings on how Lite words things: the Overview card's Last Collect follows the
/// "Show timestamps in" setting on the server's own clock (as Alert History and Job History do) and shows its date
/// when it is not from today, and the sidebar rows carry an accessible name instead of a type name.
///
/// <para>US Eastern is the test zone, five hours behind UTC in January. The Overview card used to convert on the
/// ACTIVE server's clock (whatever tab was open, else this machine's) in every mode, so a Pacific server's card sat
/// three hours away from its own alert and job rows, and a collection from nine days ago read as a time of day.</para>
/// </summary>
/* Sets ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics, so it joins the
   collection every other class that writes them uses; both are restored in Dispose. */
[Collection("server-time-helper")]
public sealed class ClickThroughUiTextTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
    }

    private static DateTime Utc(int y, int mo, int d, int h, int mi, int s = 0) =>
        new(y, mo, d, h, mi, s, DateTimeKind.Unspecified);

    /* The time-of-day clock the card's own server uses: -08:00 (Pacific standard), a fixed offset. */
    private static readonly ServerClock Pacific = ServerClock.FixedOffset(-480);

    [Fact]
    public void LastCollect_ServerMode_ConvertsOnTheCardsOwnServerClock_NotTheActiveOne()
    {
        ServerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(600);
        var now = Utc(2026, 1, 15, 21, 0);

        var text = ServerSummaryItem.FormatLastCollect(Utc(2026, 1, 15, 20, 30, 15), TimeDisplayMode.ServerTime, Pacific, now);

        Assert.Equal("12:30:15", text);
    }

    [Fact]
    public void LastCollect_UtcMode_ShowsUtc()
    {
        var text = ServerSummaryItem.FormatLastCollect(Utc(2026, 1, 15, 20, 30, 15), TimeDisplayMode.UTC, Pacific, Utc(2026, 1, 15, 21, 0));

        Assert.Equal("20:30:15", text);
    }

    [Fact]
    public void LastCollect_LocalMode_ShowsThisMachinesZone()
    {
        var utc = Utc(2026, 1, 15, 20, 30, 15);
        var now = Utc(2026, 1, 15, 21, 0);
        var expected = TimeZoneInfo.ConvertTimeFromUtc(utc, TimeZoneInfo.Local);
        var sameDay = expected.Date == TimeZoneInfo.ConvertTimeFromUtc(now, TimeZoneInfo.Local).Date;

        var text = ServerSummaryItem.FormatLastCollect(utc, TimeDisplayMode.LocalTime, Pacific, now);

        Assert.StartsWith(sameDay ? expected.ToString("HH:mm:ss") : expected.ToString("yyyy-MM-dd HH:mm:ss"), text, StringComparison.Ordinal);
    }

    [Fact]
    public void LastCollect_FromAnEarlierDay_ShowsItsDate()
    {
        /* LITE/01-start.png: five cards at launch read "19:25:47 (stopped)" for a collection nine days old. */
        var text = ServerSummaryItem.FormatLastCollect(Utc(2026, 1, 6, 20, 30, 15), TimeDisplayMode.ServerTime, Pacific, Utc(2026, 1, 15, 21, 0));

        Assert.Equal("2026-01-06 12:30:15", text);
    }

    [Fact]
    public void LastCollect_TodayIsJudgedInTheDisplayZone_NotInUtc()
    {
        /* 2026-01-16 03:00 UTC is still 2026-01-15 19:00 in Pacific; a collection at 02:00 UTC is 18:00 on the same
           Pacific day, so it is today there even though UTC has already turned over. */
        var now = Utc(2026, 1, 16, 3, 0);
        var collected = Utc(2026, 1, 16, 2, 0);

        Assert.Equal("18:00:00", ServerSummaryItem.FormatLastCollect(collected, TimeDisplayMode.ServerTime, Pacific, now));
        Assert.Equal("2026-01-16 02:00:00", ServerSummaryItem.FormatLastCollect(collected, TimeDisplayMode.UTC, Pacific, Utc(2026, 1, 17, 3, 0)));
    }

    [Fact]
    public void LastCollectionDisplay_UsesTheCardsClockAndTheDateRule()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var card = new ServerSummaryItem
        {
            DisplayName = "sql-01",
            ServerId = 1,
            Clock = Pacific,
            LastCollectionTime = Utc(2026, 1, 6, 20, 30, 15),
        };

        /* A collection from long before today carries its date; the card's own clock puts it on 06 Jan 12:30. */
        Assert.StartsWith("2026-01-06 12:30:15", card.LastCollectionDisplay, StringComparison.Ordinal);
    }

    [Fact]
    public void SidebarRows_CarryTheServerOrGroupNameAsTheirAccessibleName()
    {
        var server = new FleetServerRow(new FleetServer(7, "example-sql-01", false, null), depth: 1);
        var header = new FleetHeaderRow(FleetGroupKind.Untagged, "Untagged", depth: 0, serverCount: 1, hasChildren: true, isExpanded: true);

        Assert.Equal("example-sql-01", server.AutomationName);
        Assert.Equal("Untagged", header.AutomationName);
    }

    [Fact]
    public void ListMarkup_NamesItsRowsCardsAndComboItemsForScreenReaders()
    {
        var main = File.ReadAllText(FindRepoFile(@"Lite\MainWindow.xaml"));
        var finOps = File.ReadAllText(FindRepoFile(@"Lite\Controls\FinOpsTab.xaml"));

        Assert.Contains("AutomationProperties.Name\" Value=\"{Binding AutomationName", main, StringComparison.Ordinal);
        Assert.Contains("AutomationProperties.Name=\"{Binding DisplayName}\"", main, StringComparison.Ordinal);
        Assert.Contains("AutomationProperties.Name\" Value=\"{Binding DisplayNameWithIntent", finOps, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseSizesTitle_NamesTheSelectedServer_NotAllServers()
    {
        var finOps = File.ReadAllText(FindRepoFile(@"Lite\Controls\FinOpsTab.xaml"));

        Assert.DoesNotContain("Database Sizes (All Servers)", finOps, StringComparison.Ordinal);
        Assert.Contains("Database Sizes ({0})", finOps, StringComparison.Ordinal);
    }

    [Fact]
    public void SettingsWindow_CapsItsHeightToTheWorkArea_AndHasNoDeadPoisonWaitField()
    {
        var code = File.ReadAllText(FindRepoFile(@"Lite\Windows\SettingsWindow.xaml.cs"));
        var markup = File.ReadAllText(FindRepoFile(@"Lite\Windows\SettingsWindow.xaml"));

        Assert.Contains("SourceInitialized += (_, _) => WindowWorkArea.Clamp(this);", code, StringComparison.Ordinal);
        Assert.DoesNotContain("legacy avg-ms bar", markup, StringComparison.Ordinal);
        Assert.DoesNotContain("AlertPoisonWaitThresholdBox", markup + code, StringComparison.Ordinal);
    }

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
