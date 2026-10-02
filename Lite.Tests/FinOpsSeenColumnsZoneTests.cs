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
/// #4766: the FinOps Application Connections grid's First Seen and Last Seen columns.
///
/// <para>They bound a DateTime with a XAML <c>StringFormat</c>, so they could not carry the UTC offset in the repeated
/// autumn hour, and the DateTime was <c>ToLocalTime()</c> of the stored instant: this machine's clock in every display mode,
/// where every other Lite grid follows the mode (UTC, this machine, or the active server's own clock). The row now words
/// the instant through <see cref="ServerTimeHelper.FormatServerTime(DateTime, string)"/> (<c>FirstSeenText</c>), and its
/// DateTime (<c>FirstSeenLocal</c>) reads the same display zone; the column binds the text and sorts by the DateTime.</para>
///
/// <para>The tests use US Eastern (autumn change 2026-11-01 06:00Z, so 05:30Z is the first 01:30 and 06:30Z the second) and
/// India (+05:30 all year) as the server, so a clock that differs from this machine's is always among them.</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class FinOpsSeenColumnsZoneTests : IDisposable
{
    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;

    public FinOpsSeenColumnsZoneTests()
    {
        /* The text format runs on the current culture (as every grid's does); the invariant one fixes its separators. */
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

    private static ApplicationConnectionRow Row(DateTime firstSeen, DateTime lastSeen) => new()
    {
        ApplicationName = "app",
        FirstSeen = firstSeen,
        LastSeen = lastSeen,
    };

    /// <summary>
    /// In Server mode the row reads the active server's wall clock, not this machine's: 12:00Z on 2026-07-01 is 08:00 on a US
    /// Eastern server (UTC-4) and 17:30 on an India server, and a machine can share the zone of at most one of them.
    /// </summary>
    [Fact]
    public void ServerMode_ReadsTheActiveServersWallClock_NotThisMachines()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var row = Row(Utc(2026, 7, 1, 12, 0), Utc(2026, 7, 1, 13, 0));

        ServerTimeHelper.ActiveServerClock = Eastern();
        Assert.Equal(new DateTime(2026, 7, 1, 8, 0, 0), row.FirstSeenLocal);
        Assert.Equal(new DateTime(2026, 7, 1, 9, 0, 0), row.LastSeenLocal);
        Assert.Equal("2026-07-01 08:00", row.FirstSeenText);
        Assert.Equal("2026-07-01 09:00", row.LastSeenText);

        ServerTimeHelper.ActiveServerClock = India();
        Assert.Equal(new DateTime(2026, 7, 1, 17, 30, 0), row.FirstSeenLocal);
        Assert.Equal(new DateTime(2026, 7, 1, 18, 30, 0), row.LastSeenLocal);
        Assert.Equal("2026-07-01 17:30", row.FirstSeenText);
        Assert.Equal("2026-07-01 18:30", row.LastSeenText);
    }

    /// <summary>UTC mode is the stored instant itself, on any server's clock.</summary>
    [Fact]
    public void UtcMode_IsTheStoredInstant_OnAnyClock()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
        var row = Row(Utc(2026, 7, 1, 12, 0), Utc(2026, 11, 1, 6, 30));

        foreach (var clock in new[] { Eastern(), India() })
        {
            ServerTimeHelper.ActiveServerClock = clock;
            Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0), row.FirstSeenLocal);
            Assert.Equal("2026-07-01 12:00", row.FirstSeenText);
            Assert.Equal("2026-11-01 06:30", row.LastSeenText);
        }
    }

    /// <summary>Local mode is this machine's zone at the instant, whatever server is active.</summary>
    [Fact]
    public void LocalMode_IsThisMachinesZone_OnAnyClock()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
        var utc = Utc(2026, 7, 1, 12, 0);
        var row = Row(utc, utc);
        var machine = TimeZoneInfo.ConvertTimeFromUtc(DateTime.SpecifyKind(utc, DateTimeKind.Utc), TimeZoneInfo.Local);

        foreach (var clock in new[] { Eastern(), India() })
        {
            ServerTimeHelper.ActiveServerClock = clock;
            Assert.Equal(machine, row.FirstSeenLocal);
        }
    }

    /// <summary>
    /// The repeated autumn hour on a US Eastern server clock: 05:30Z reads 01:30 -04:00 and 06:30Z 01:30 -05:00, and the two
    /// DateTimes the columns sort by are the same wall time, so the text is what tells the rows apart. The hour after
    /// carries no offset.
    /// </summary>
    [Fact]
    public void AnEasternServersRepeatedHour_ReadsItsOffset_AndTheHourAfterDoesNot()
    {
        ServerTimeHelper.ActiveServerClock = Eastern();
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        var first = Row(Utc(2026, 11, 1, 5, 30), Utc(2026, 11, 1, 6, 30));
        Assert.Equal("2026-11-01 01:30 -04:00", first.FirstSeenText);
        Assert.Equal("2026-11-01 01:30 -05:00", first.LastSeenText);
        Assert.Equal(first.FirstSeenLocal, first.LastSeenLocal);

        var after = Row(Utc(2026, 11, 1, 7, 30), Utc(2026, 11, 1, 7, 30));
        Assert.Equal("2026-11-01 02:30", after.FirstSeenText);
    }

    /// <summary>
    /// The grid is a WPF grid this suite does not instantiate, so the columns are a source pin: each binds its text
    /// property (no <c>StringFormat</c>, which cannot carry the offset), sorts by the row's UTC DateTime (the display-zone
    /// one puts the second pass of the repeated hour before the first, see <see cref="GridTimeColumnSortMemberTests"/>),
    /// and keeps its filter button's tag on the display-zone DateTime property the filter manager reads.
    /// </summary>
    [Theory]
    [InlineData("FirstSeenText", "FirstSeen", "FirstSeenLocal")]
    [InlineData("LastSeenText", "LastSeen", "LastSeenLocal")]
    public void EachSeenColumn_BindsItsText_SortsByItsUtcDateTime_AndFiltersOnTheDateTime(string textProperty, string sortMember, string dateProperty)
    {
        var xaml = ReadLite("Controls", "FinOpsTab.xaml");

        var start = Regex.Match(xaml,
            @"<DataGridTextColumn(?=[^>]*\sBinding=""\{Binding " + textProperty + @"\}"")(?=[^>]*\sSortMemberPath=""" + sortMember + @""")[^>]*>");
        Assert.True(start.Success, $"the column bound to {textProperty} and sorted by {sortMember} is not in FinOpsTab.xaml.");

        var end = xaml.IndexOf("</DataGridTextColumn>", start.Index, StringComparison.Ordinal);
        Assert.True(end > start.Index, "the end of the column was not found.");
        var column = xaml[start.Index..end];

        Assert.DoesNotContain("StringFormat", column, StringComparison.Ordinal);
        Assert.Contains($"Tag=\"{dateProperty}\"", column, StringComparison.Ordinal);
    }

    /// <summary>The DateTime-bound spellings this replaced must not come back.</summary>
    [Fact]
    public void NoSeenColumn_BindsADateTime_WithAStringFormat()
    {
        var xaml = ReadLite("Controls", "FinOpsTab.xaml");

        Assert.DoesNotMatch(@"\{Binding (FirstSeenLocal|LastSeenLocal)\b", xaml);
    }

    private static string ReadLite(string folder, string file, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", folder, file)));
}
