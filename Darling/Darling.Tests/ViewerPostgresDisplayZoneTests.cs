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
using System.Reflection;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8919: on a PostgreSQL server tab the log event grid showed UTC while the grids beside it showed the time display zone, and the
/// range header named this machine's offset as the server's. Each fact builds the real tab on the shared STA thread, sets the time
/// display setting and the server clock the tab holds, and reads the text the grids and the header actually show.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerPostgresDisplayZoneTests
{
    private const string ViewerPack = "pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/";

    /// <summary>The instant under test, naive UTC, as the store holds it.</summary>
    private static readonly DateTime Instant = new(2026, 11, 17, 19, 26, 57, DateTimeKind.Unspecified);

    /// <summary>A server clock no test machine shares by chance: nine and a half hours ahead of UTC.</summary>
    private static readonly ServerClock ServerAhead = ServerClock.FixedOffset(570);

    private static T OnTab<T>(TimeDisplayMode mode, Func<ViewerServerTab, T> body) =>
        StaTestThread.Run(() =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            var added = new List<ResourceDictionary>
            {
                new() { Source = new Uri(ViewerPack + "DarkTheme.xaml") },
                new() { Source = ThemeManager.SharedStylesUri },
            };
            foreach (var dictionary in added)
            {
                app.Resources.MergedDictionaries.Add(dictionary);
            }

            var savedMode = ViewerTimeHelper.CurrentDisplayMode;
            var savedClock = ViewerTimeHelper.ActiveServerClock;
            try
            {
                ViewerTimeHelper.CurrentDisplayMode = mode;
                var tab = new ViewerServerTab(
                    new ViewerDataService("Host=127.0.0.1;Port=1;Database=darling;Username=darling"),
                    new DarlingServer(1, "pg-zone-test", "pg-zone-test", true, null, engineKind: "postgres"));
                var window = new Window { Content = tab, Width = 1800, Height = 1000 };
                StaTestThread.ShowOffScreen(window);
                try
                {
                    typeof(ViewerServerTab).GetField("_suppressDrillDownAutoRefresh", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(tab, true);
                    ((TabControl)tab.FindName("InnerTabs")).SelectedItem = tab.FindName("PgActivityTab");
                    return body(tab);
                }
                finally
                {
                    window.Close();
                }
            }
            finally
            {
                ViewerTimeHelper.CurrentDisplayMode = savedMode;
                ViewerTimeHelper.ActiveServerClock = savedClock;
                foreach (var dictionary in added)
                {
                    app.Resources.MergedDictionaries.Remove(dictionary);
                }
            }
        });

    private static void ShowSubTab(ViewerServerTab tab, int index)
    {
        ((TabControl)tab.FindName("PgActivitySubTabs")).SelectedIndex = index;
        ClippedTextProbe.Settle(Window.GetWindow(tab)!);
    }

    private static string FirstRowCell(DataGrid grid, string header)
    {
        var row = ClippedTextProbe.Descendants(grid).OfType<DataGridRow>().First();
        var cell = ClippedTextProbe.Descendants(row).OfType<DataGridCell>().Single(c => c.Column.Header?.ToString() == header);
        return string.Join("", ClippedTextProbe.Descendants(cell).OfType<TextBlock>().Select(b => b.Text));
    }

    private static DarlingPgLogEventReader.PgLogEventRow LogEvent(DateTime occurredUtc) =>
        new(occurredUtc, "error", "ERROR", "23505", "appdb", "app", "psql", 4242, "duplicate key", null, null, 1, default);

    private static DarlingPgDatabaseReader.PgDatabaseRow Database(DateTime statsResetUtc) =>
        new("appdb", 10, 1, 5, 50, 2, 4096, 0, 0, 0, statsResetUtc, 12, statsResetUtc, statsResetUtc, 3);

    /// <summary>The log event time and the Stats Reset time beside it on the same tab, for the same instant.</summary>
    private static (string LogWhen, string StatsReset) ReadBoth(TimeDisplayMode mode) =>
        OnTab(mode, tab =>
        {
            tab.ApplyPgServerClock(ServerAhead, "Test/Ahead");
            tab.ApplyServerClockToHelper();
            tab.PgLogEventsGrid.ItemsSource = new[] { LogEvent(Instant) };
            ShowSubTab(tab, 0);
            var logWhen = FirstRowCell(tab.PgLogEventsGrid, "When");

            tab.PgDatabaseStatsGrid.ItemsSource = new[] { PgDisplay.Database(Database(Instant)) };
            ShowSubTab(tab, 2);
            return (logWhen, FirstRowCell(tab.PgDatabaseStatsGrid, "Stats Reset"));
        });

    [Fact]
    public void TheLogEventTime_AndTheStatsResetBesideIt_ReadInTheServersZone_InServerTime()
    {
        var (logWhen, statsReset) = ReadBoth(TimeDisplayMode.ServerTime);

        Assert.Equal("2026-11-18 04:56:57", logWhen);
        Assert.Equal("2026-11-18 04:56", statsReset);
    }

    [Fact]
    public void TheLogEventTime_AndTheStatsResetBesideIt_ReadInUtc_InUtcMode()
    {
        var (logWhen, statsReset) = ReadBoth(TimeDisplayMode.UTC);

        Assert.Equal("2026-11-17 19:26:57", logWhen);
        Assert.Equal("2026-11-17 19:26", statsReset);
    }

    [Fact]
    public void TheLogEventTime_AndTheStatsResetBesideIt_ReadInThisMachinesZone_InLocalMode()
    {
        var (logWhen, statsReset) = ReadBoth(TimeDisplayMode.LocalTime);

        var local = DateTime.SpecifyKind(Instant, DateTimeKind.Utc).ToLocalTime();
        Assert.Equal(local.ToString("yyyy-MM-dd HH:mm:ss"), logWhen);
        Assert.Equal(local.ToString("yyyy-MM-dd HH:mm"), statsReset);
    }

    /// <summary>The range header names the zone the server records, not this machine's offset.</summary>
    [Fact]
    public void TheRangeHeader_NamesTheServersRecordedZone_InServerTime()
    {
        var (header, tip) = OnTab(TimeDisplayMode.ServerTime, tab =>
        {
            tab.ApplyPgServerClock(ServerAhead, "Test/Ahead");
            return HeaderAndTip(tab);
        });

        Assert.EndsWith("(UTC+09:30)", header, StringComparison.Ordinal);
        Assert.Contains("Test/Ahead", tip, StringComparison.Ordinal);
    }

    /// <summary>With no recorded zone the header reads UTC and the time display box says why, rather than naming this machine's offset as the server's.</summary>
    [Fact]
    public void TheRangeHeader_ReadsUtc_AndSaysWhy_WhenTheServersZoneIsNotRecorded()
    {
        var (header, tip) = OnTab(TimeDisplayMode.ServerTime, tab =>
        {
            tab.ApplyPgServerClock(null, null);
            return HeaderAndTip(tab);
        });

        Assert.EndsWith("(UTC+00:00)", header, StringComparison.Ordinal);
        Assert.Contains("not recorded", tip, StringComparison.Ordinal);
    }

    private static (string Header, string Tip) HeaderAndTip(ViewerServerTab tab)
    {
        var picker = (TimeRangePicker)tab.FindName("RangePicker");
        picker.Refresh();
        ClippedTextProbe.Settle(Window.GetWindow(tab)!);
        var text = ClippedTextProbe.Descendants(picker).OfType<TextBlock>().Single(b => b.Name == "ResolvedText").Text;
        return (text, (string)((ComboBox)tab.FindName("TimeDisplayModeBox")).ToolTip);
    }
}
