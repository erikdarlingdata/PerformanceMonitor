/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// A data-start probe that throws (Active Queries, Current Waits, and the three Queries grids share one) costs its
/// "Showing since" banner and nothing else. The probe runs before the Query Stats grid bind in the Queries refresh,
/// and before the slicer loads and the tab-badge counts in the Locking refresh, so an uncaught failure skipped them.
/// </summary>
/* AppLogger.DrainBufferedLines is destructive, so a class that reads it shares the one collection that serializes the
   others that do. */
[Collection("app-logger-statics")]
public sealed class DataStartBannerProbeFailureTests
{
    private static readonly DateTime WindowStart = new(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void AProbeThatThrows_HidesTheBanner_IsLogged_AndLetsTheRefreshGoOn()
    {
        var tag = "probe-" + Guid.NewGuid().ToString("N")[..8];
        AppLogger.DrainBufferedLines();

        var (visible, text, answer) = OnStaThread(() =>
        {
            /* Seeded visible, with the last read's text, so a helper that never touched the banner cannot pass. */
            var banner = new System.Windows.Controls.TextBlock
            {
                Visibility = System.Windows.Visibility.Visible,
                Text = "Showing since 2026-09-04 00:00:00",
            };
            /* What the refresh does: the guarded probe, then the banner step on its answer. */
            var floor = ServerTab.ProbeWindowFloorOrNullAsync(
                () => Task.FromException<DateTime?>(new InvalidOperationException(tag)),
                "[DataStartServer] QuerySnapshots").GetAwaiter().GetResult();
            var result = ServerTab.ApplyWindowFloorToBanner(banner, floor, WindowStart, TimeZoneInfo.Utc);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text, result);
        });

        Assert.False(answer);
        Assert.False(visible);
        Assert.Equal(string.Empty, text);
        var logged = Assert.Single(AppLogger.DrainBufferedLines(), l => l.Contains(tag, StringComparison.Ordinal));
        Assert.Contains("[WARN ]", logged, StringComparison.Ordinal);
        Assert.Contains("[DataStartServer] QuerySnapshots", logged, StringComparison.Ordinal);
    }

    [Fact]
    public void AProbeThatAnswersAfterTheWindowStart_ShowsTheBanner_AndLogsNothing()
    {
        var tag = "probe-" + Guid.NewGuid().ToString("N")[..8];
        AppLogger.DrainBufferedLines();

        var (visible, text, answer) = OnStaThread(() =>
        {
            var banner = new System.Windows.Controls.TextBlock();
            var floor = ServerTab.ProbeWindowFloorOrNullAsync(
                () => Task.FromResult<DateTime?>(WindowStart.AddDays(3)), tag).GetAwaiter().GetResult();
            var result = ServerTab.ApplyWindowFloorToBanner(banner, floor, WindowStart, TimeZoneInfo.Utc);
            return (banner.Visibility == System.Windows.Visibility.Visible, banner.Text, result);
        });

        Assert.True(answer);
        Assert.True(visible);
        Assert.Equal("Showing since 2026-09-04 00:00:00", text);
        Assert.DoesNotContain(AppLogger.DrainBufferedLines(), l => l.Contains(tag, StringComparison.Ordinal));
    }

    /// <summary>
    /// The shared banner refresh, the one place the probe is called, goes through the guarded probe, so every surface
    /// that carries a banner (the three Queries grids, Active Queries, Current Waits) and every read path of each is
    /// covered. Text-scans SOURCE, like QueryWindowTruncationTests.
    /// </summary>
    [Fact]
    public void TheSharedBannerRefresh_GoesThroughTheGuardedProbe()
    {
        var source = File.ReadAllText(ControlsFile("ServerTab.Refresh.cs"));

        Assert.Single(Regex.Matches(source,
            @"private async System\.Threading\.Tasks\.Task RefreshWindowTruncatedBannerAsync\([^)]*\)\s*\{\s*var floor = await ProbeWindowFloorOrNullAsync\(\s*\(\) => Task\.Run\(\(\) => _dataService\.GetQueryWindowFloorAsync\(relation, _serverId, startUtc, endUtc\)\),[^;]*;\s*ApplyWindowFloorToBanner\(banner, floor, startUtc, GetPickerZone\(\)\);"));
    }

    /// <summary>WPF objects require STA, and a probe that has already completed keeps the continuation on this thread.</summary>
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }

    private static string ControlsFile(string name) =>
        Path.GetFullPath(Path.Combine(ControlsDir(), name));

    private static string ControlsDir([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", "Controls"));
}
