/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: the Collection Health grids' times (Last Success, Last Run and Last Error on the health rows, and the run
/// history's time). They were <c>ToLocalTime().ToString("g")</c>: this machine's clock in every display mode, bare in the
/// repeated autumn hour. A row now carries the clock of the server it belongs to and words its collector-written UTC instant
/// on it (<c>ServerTimeHelper.FormatInstant</c>), so it follows the display mode, reads its own server's wall time whichever
/// tab is active when it renders, and names the UTC offset of each 01:30 of the repeated hour.
///
/// <para>The surfaces are the two grids of a server's own tab (which stamp that tab's clock on every row they load) and the
/// run-history window the tab opens for one collector (which takes the same clock). The fleet-wide log the MCP tool reads
/// never uses these columns.</para>
///
/// <para>US Eastern is the row's server and India (+05:30 all year) the active tab's, so a clock that differs from this
/// machine's is always among them; the autumn change is 2026-11-01 at 06:00 UTC, so 05:30Z is the first 01:30 (-04:00)
/// and 06:30Z the second (-05:00).</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class CollectionHealthDisplayZoneTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;

    public CollectionHealthDisplayZoneTests()
    {
        /* "g" runs on the current culture, as every grid's time does; the invariant one fixes its pattern (MM/dd/yyyy HH:mm). */
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
    }

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
        CultureInfo.CurrentCulture = _savedCulture;
    }

    private static ServerClock Eastern() => ServerClock.Resolve("Eastern Standard Time", -300);

    private static ServerClock India() => ServerClock.Resolve("India Standard Time", 330);

    private static DateTime Utc(int y, int mo, int d, int h, int mi) => new(y, mo, d, h, mi, 0, DateTimeKind.Unspecified);

    private static CollectorHealthRow Health(DateTime utc, ServerClock? clock) => new()
    {
        CollectorName = "wait_stats",
        LastSuccessTime = utc,
        LastRunTime = utc,
        LastErrorTime = utc,
        Clock = clock,
    };

    private static CollectionLogRow Log(DateTime utc, ServerClock? clock) => new()
    {
        CollectorName = "wait_stats",
        CollectionTime = utc,
        Status = "SUCCESS",
        Clock = clock,
    };

    /// <summary>
    /// A row reads its own server's wall time in Server mode, whichever clock is active: 14:30Z is 10:30 in US Eastern
    /// (the row's server) and 20:00 in India (the active tab's), and neither is this machine's.
    /// </summary>
    [Fact]
    public void ServerMode_ARowReadsItsOwnServersWallTime_NotTheActiveTabsOrThisMachines()
    {
        ServerTimeHelper.ActiveServerClock = India();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var instant = Utc(2026, 7, 1, 14, 30);

        var health = Health(instant, Eastern());
        Assert.Equal("07/01/2026 10:30", health.LastSuccessFormatted);
        Assert.Equal("07/01/2026 10:30", health.LastRunFormatted);
        Assert.Equal("07/01/2026 10:30", health.LastErrorFormatted);
        Assert.Equal("07/01/2026 10:30", Log(instant, Eastern()).CollectionTimeFormatted);

        ServerTimeHelper.ActiveServerClock = Eastern();
        Assert.Equal("07/01/2026 20:00", Health(instant, India()).LastSuccessFormatted);
        Assert.Equal("07/01/2026 20:00", Log(instant, India()).CollectionTimeFormatted);
    }

    /// <summary>
    /// The two occurrences of the repeated hour take their UTC offsets (-04:00 for 05:30Z, -05:00 for 06:30Z), the hour
    /// after is bare, and UTC mode is the stored instant with no offset.
    /// </summary>
    [Fact]
    public void InTheRepeatedHour_EachOccurrenceCarriesItsOffsetInServerMode_AndUtcModeIsTheStoredInstant()
    {
        ServerTimeHelper.ActiveServerClock = India();
        var first = Health(Utc(2026, 11, 1, 5, 30), Eastern());
        var second = Health(Utc(2026, 11, 1, 6, 30), Eastern());
        var after = Health(Utc(2026, 11, 1, 7, 30), Eastern());

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("11/01/2026 01:30 -04:00", first.LastSuccessFormatted);
        Assert.Equal("11/01/2026 01:30 -05:00", second.LastSuccessFormatted);
        Assert.Equal("11/01/2026 01:30 -05:00", second.LastRunFormatted);
        Assert.Equal("11/01/2026 01:30 -05:00", second.LastErrorFormatted);
        Assert.Equal("11/01/2026 02:30", after.LastSuccessFormatted);
        Assert.Equal("11/01/2026 01:30 -04:00", Log(Utc(2026, 11, 1, 5, 30), Eastern()).CollectionTimeFormatted);
        Assert.Equal("11/01/2026 01:30 -05:00", Log(Utc(2026, 11, 1, 6, 30), Eastern()).CollectionTimeFormatted);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("11/01/2026 05:30", first.LastSuccessFormatted);
        Assert.Equal("11/01/2026 06:30", second.LastSuccessFormatted);
        Assert.Equal("11/01/2026 06:30", Log(Utc(2026, 11, 1, 6, 30), Eastern()).CollectionTimeFormatted);
    }

    /// <summary>Local mode is this machine's zone, whatever clock the row carries (a summer instant, so no offset).</summary>
    [Fact]
    public void LocalMode_IsThisMachinesZone_OnAnyClock()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
        var instant = Utc(2026, 7, 1, 14, 30);
        var expected = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(instant, DateTimeKind.Utc), TimeZoneInfo.Local)
            .ToString("g", CultureInfo.InvariantCulture);

        foreach (var clock in new[] { Eastern(), India() })
        {
            Assert.Equal(expected, Health(instant, clock).LastRunFormatted);
            Assert.Equal(expected, Log(instant, clock).CollectionTimeFormatted);
        }
    }

    /// <summary>A row built without a clock reads on the active tab's, and a missing time keeps its placeholder.</summary>
    [Fact]
    public void ARowWithNoClockStamped_TakesTheActiveTabsClock_AndAMissingTimeKeepsItsPlaceholder()
    {
        ServerTimeHelper.ActiveServerClock = Eastern();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        Assert.Equal("11/01/2026 01:30 -05:00", Health(Utc(2026, 11, 1, 6, 30), clock: null).LastSuccessFormatted);
        Assert.Equal("11/01/2026 01:30 -05:00", Log(Utc(2026, 11, 1, 6, 30), clock: null).CollectionTimeFormatted);

        var never = new CollectorHealthRow { CollectorName = "wait_stats", Clock = Eastern() };
        Assert.Equal("Never", never.LastSuccessFormatted);
        Assert.Equal("Never", never.LastRunFormatted);
        Assert.Equal("", never.LastErrorFormatted);
    }

    /// <summary>
    /// The rows word their time through the one instant renderer and not through <c>ToLocalTime()</c>, and every place that
    /// loads them stamps the clock of the tab they belong to: the tab's two grids, and the run-history window the tab opens.
    /// </summary>
    [Fact]
    public void TheRowsAreWordedByTheInstantRenderer_AndEveryLoaderStampsItsServersClock()
    {
        var data = Code(LiteSource("Services", "LocalDataService.CollectionHealth.cs"));
        Assert.DoesNotContain("ToLocalTime", data);
        Assert.DoesNotContain("FormatServerTime(", data);
        Assert.Single(Regex.Matches(data, @"ServerTimeHelper\.FormatInstant\("));
        Assert.Equal(4, Regex.Matches(data, @"CollectionHealthTime\.Format\(").Count);

        var refresh = Code(LiteSource("Controls", "ServerTab.Refresh.cs"));
        Assert.Contains("var tabClock = _serverClock;", refresh);
        Assert.Contains("foreach (var row in collectionHealthTask.Result) row.Clock = tabClock;", refresh);
        Assert.Contains("foreach (var row in collectionLogTask.Result) row.Clock = tabClock;", refresh);

        var grids = Code(LiteSource("Controls", "ServerTab.Grids.cs"));
        Assert.Contains("new Windows.CollectionLogWindow(_dataService, _serverId, item.CollectorName, _serverClock)", grids);

        var window = Code(LiteSource("Windows", "CollectionLogWindow.xaml.cs"));
        Assert.Contains("foreach (var log in logs) log.Clock = _serverClock;", window);
    }

    private static string Code(string source) =>
        Regex.Replace(source, @"/\*.*?\*/|//[^\r\n]*", "", RegexOptions.Singleline);

    /// <summary>A Lite source file, found by walking up from this test file to the repo root.</summary>
    private static string LiteSource(string folder, string file, [CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        var relative = Path.Combine("Lite", folder, file);
        while (dir is not null && !File.Exists(Path.Combine(dir, relative)))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return File.ReadAllText(Path.Combine(dir!, relative));
    }
}
