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
/// #4766: the FinOps Application Connections rows read their First Seen and Last Seen on the SELECTED server's clock. The
/// FinOps tab lists whichever server its picker names, not the server of the tab that is active, and the rows went through
/// the active tab's clock for both the column text and the DateTime the column sorts by, so in Server mode a row could read
/// another server's hour. The tab now stamps the selected server's clock on each row when it loads them, by the rule the
/// PVS trend beside the grid uses (the server's own collected clock, else its open tab's, else the machine's), and the text
/// and the sort values both read it.
///
/// <para>US Eastern is the selected server and India (+05:30 all year) the active tab's, so a clock that differs from this
/// machine's is always among them; the autumn change is 2026-11-01 at 06:00 UTC, so 05:30Z is the first 01:30 (-04:00) and
/// 06:30Z the second (-05:00).</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class FinOpsRowDisplayZoneTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;

    public FinOpsRowDisplayZoneTests()
    {
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

    private static ApplicationConnectionRow Row(DateTime firstUtc, DateTime lastUtc, ServerClock? clock) => new()
    {
        ApplicationName = "app",
        FirstSeen = firstUtc,
        LastSeen = lastUtc,
        Clock = clock,
    };

    /// <summary>
    /// The selected server's wall time in Server mode, not the active tab's: 14:30Z is 10:30 in US Eastern (the selected
    /// server) and 20:00 in India (the active tab's). The sort values read the same clock as the text.
    /// </summary>
    [Fact]
    public void ServerMode_ARowReadsTheSelectedServersWallTime_NotTheActiveTabs_InTextAndSortValue()
    {
        ServerTimeHelper.ActiveServerClock = India();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        var row = Row(Utc(2026, 7, 1, 14, 30), Utc(2026, 7, 1, 15, 30), Eastern());
        Assert.Equal("2026-07-01 10:30", row.FirstSeenText);
        Assert.Equal("2026-07-01 11:30", row.LastSeenText);
        Assert.Equal(new DateTime(2026, 7, 1, 10, 30, 0), row.FirstSeenLocal);
        Assert.Equal(new DateTime(2026, 7, 1, 11, 30, 0), row.LastSeenLocal);

        ServerTimeHelper.ActiveServerClock = Eastern();
        var indian = Row(Utc(2026, 7, 1, 14, 30), Utc(2026, 7, 1, 15, 30), India());
        Assert.Equal("2026-07-01 20:00", indian.FirstSeenText);
        Assert.Equal(new DateTime(2026, 7, 1, 20, 0, 0), indian.FirstSeenLocal);
    }

    [Fact]
    public void InTheRepeatedHour_EachOccurrenceCarriesItsOffsetInServerMode_AndUtcModeIsTheStoredInstant()
    {
        ServerTimeHelper.ActiveServerClock = India();
        var row = Row(Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 6, 30), Eastern());

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        Assert.Equal("2026-11-01 01:30 -04:00", row.FirstSeenText);
        Assert.Equal("2026-11-01 01:30 -05:00", row.LastSeenText);
        Assert.Equal(new DateTime(2026, 11, 1, 1, 30, 0), row.FirstSeenLocal);

        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        Assert.Equal("2026-11-01 05:30", row.FirstSeenText);
        Assert.Equal("2026-11-01 06:30", row.LastSeenText);
        Assert.Equal(new DateTime(2026, 11, 1, 6, 30, 0), row.LastSeenLocal);
    }

    /// <summary>A row built without a clock reads on the active tab's, the fallback it had before.</summary>
    [Fact]
    public void ARowWithNoClockStamped_TakesTheActiveTabsClock()
    {
        ServerTimeHelper.ActiveServerClock = Eastern();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        var row = Row(Utc(2026, 11, 1, 6, 30), Utc(2026, 11, 1, 6, 30), clock: null);
        Assert.Equal("2026-11-01 01:30 -05:00", row.FirstSeenText);
        Assert.Equal(new DateTime(2026, 11, 1, 1, 30, 0), row.LastSeenLocal);
    }

    /// <summary>
    /// The tab stamps the selected server's clock the way the PVS trend picks it, before the rows reach the grid, and the row
    /// words its text through the instant renderer on its own zone and not through the active tab's shared setting.
    /// </summary>
    [Fact]
    public void TheTab_StampsTheSelectedServersClock_AndTheRowWordsItOnItsOwnZone()
    {
        var tab = Code(LiteSource("Controls", "FinOpsTab.xaml.cs"));
        var load = Member(tab, "LoadApplicationConnectionsAsync");
        Assert.Contains("_openTabClock.Invoke(serverId)", load);
        Assert.Contains("dataService.GetServerClockAsync(serverId)", load);
        Assert.Contains("ServerTimeHelper.ClockForServer(collected, openTab)", load);
        Assert.Contains("foreach (var row in data) row.Clock = clock;", load);

        var data = Code(LiteSource("Services", "LocalDataService.FinOps.cs"));
        var rowSource = data.Substring(data.IndexOf("class ApplicationConnectionRow", StringComparison.Ordinal));
        rowSource = rowSource.Substring(0, rowSource.IndexOf("class DatabaseSizeRow", StringComparison.Ordinal));
        Assert.DoesNotContain("FormatServerTime(", rowSource);
        Assert.Contains("ServerTimeHelper.FormatInstant(FirstSeen, DisplayZoneNow,", rowSource);
        Assert.Contains("ServerTimeHelper.FormatInstant(LastSeen, DisplayZoneNow,", rowSource);
        Assert.Contains("Clock ?? ServerTimeHelper.ActiveServerClock", rowSource);
    }

    /// <summary>The declaration and body of the method called <paramref name="name"/>, to its matching close brace.</summary>
    private static string Member(string source, string name)
    {
        var at = Regex.Match(source, @"Task\s+" + Regex.Escape(name) + @"\(");
        Assert.True(at.Success, $"member {name} not found");
        var open = source.IndexOf('{', at.Index);
        var depth = 0;
        for (var i = open; i < source.Length; i++)
        {
            if (source[i] == '{')
            {
                depth++;
            }
            else if (source[i] == '}' && --depth == 0)
            {
                return source.Substring(at.Index, i - at.Index + 1);
            }
        }

        throw new InvalidOperationException($"member {name} is not closed");
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
