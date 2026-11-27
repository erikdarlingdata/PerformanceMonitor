/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls.Primitives;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9603: the Store Health window's header line names the zone. The real window reads a store folder with no store file in it (the
/// read answers at once) and the line it shows is read back from the rendered control. The housekeeping history under it is in UTC,
/// so the line is the UTC time of the read and says UTC, whatever zone the machine is in.
/// </summary>
[Collection("application-theme")] // #8395: changes the application-scope theme
public sealed class StoreHealthWindowReadAtTests
{
    private const string Prefix = "Read at ";
    private const string Suffix = " UTC";

    [Fact]
    public void TheHeaderLine_IsTheUtcTimeOfTheRead_AndSaysUtc()
    {
        var dir = Path.Combine(Path.GetTempPath(), "StoreHealthReadAt_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        var text = "";
        var before = DateTime.UtcNow;
        var after = before;
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var reader = new StoreHealthReader(duckDb, Path.Combine(dir, "archive"), new StoreHousekeepingHistory(StoreHousekeepingHistory.PathFor(Path.Combine(dir, "test.duckdb"))));
            StaTestThread.Run(async () =>
            {
                var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
                ResourceDictionary? added = null;
                if (!app.Resources.Contains("NumericCell"))
                {
                    added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
                    app.Resources.MergedDictionaries.Add(added);
                }

                var window = new StoreHealthWindow(reader);
                try
                {
                    /* The window is not shown, so its load does not run: Refresh asks for the one read. */
                    before = DateTime.UtcNow;
                    window.RefreshButton.RaiseEvent(new RoutedEventArgs(ButtonBase.ClickEvent));
                    Assert.True(await PumpUntilAsync(() => window.StatusText.Text.StartsWith(Prefix, StringComparison.Ordinal)), "the window never showed the time of the read");
                    after = DateTime.UtcNow;
                    text = window.StatusText.Text;
                }
                finally
                {
                    window.Close();
                    if (added is not null)
                    {
                        app.Resources.MergedDictionaries.Remove(added);
                    }
                }
            });
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* Best-effort cleanup */ }
        }

        Assert.StartsWith(Prefix, text);
        Assert.EndsWith(Suffix, text);
        var shown = TimeSpan.ParseExact(text[Prefix.Length..^Suffix.Length], "hh\\:mm\\:ss", CultureInfo.InvariantCulture);

        /* The line is the clock time of the read, cut to the second. The read ran between the two UTC clock readings, so the line lies
           within [before cut to the second, after]. A local time on a machine that is not at UTC+0 lies hours away. */
        var day = TimeSpan.FromDays(1);
        var beforeSecond = TimeSpan.FromSeconds(Math.Floor(before.TimeOfDay.TotalSeconds));
        var ahead = TimeSpan.FromTicks(((shown - beforeSecond).Ticks % day.Ticks + day.Ticks) % day.Ticks);
        Assert.True(ahead <= after - before + TimeSpan.FromSeconds(1), $"the line reads {text}, the read ran between {before:HH:mm:ss.fff} and {after:HH:mm:ss.fff} UTC");
    }

    /// <summary>Lets the dispatcher run until <paramref name="condition"/> holds, looking at most <paramref name="polls"/> times, 20 ms apart.
    /// The window's read finishes on the dispatcher, so the wait yields to it instead of blocking the STA thread.</summary>
    private static async Task<bool> PumpUntilAsync(Func<bool> condition, int polls = 500)
    {
        for (var i = 0; i < polls && !condition(); i++)
        {
            await Task.Delay(20);
        }

        return condition();
    }
}
