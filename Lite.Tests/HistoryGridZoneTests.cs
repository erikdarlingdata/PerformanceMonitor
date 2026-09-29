/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a History window draws its chart, hover and summary in its own zone (the opening tab's picker zone), and its
/// grid used to word the same rows on the SELECTED tab's clock. The window stays open when the user selects another
/// server's tab, so the grid could then read in that other server's zone while the chart kept its own. Each history row
/// now carries the window's zone (<c>Zone</c>) and words its instants in it; a row nothing set a zone on (the server tab's
/// own grids, which render only while their tab is selected) keeps <see cref="ServerTimeHelper.FormatServerTime(DateTime, string)"/>.
///
/// <para>Only the columns that hold an INSTANT take the zone: the collection time, and Query Store's first and last
/// execution. The <c>sys.dm_exec_*</c> stamps (creation, cached and last execution of a query or procedure) are the SQL
/// server's own wall clock, so they stay on <see cref="ServerTimeHelper.FormatServerClock(DateTime, string)"/>; reading
/// one of them as UTC would shift it by the server's offset in Server mode.</para>
///
/// <para>The tests use US Eastern, whose 2026 autumn change is 2026-11-01 06:00Z, and the second 01:30 of that day
/// (06:30Z), which is the one a wall-clock round trip gets wrong.</para>
/// </summary>
/* Installs ServerTimeHelper.ActiveServerClock and CurrentDisplayMode, process-wide mutable statics; joins the
   collection every other class that writes them uses. */
[Collection("server-time-helper")]
public sealed class HistoryGridZoneTests : IDisposable
{
    private const string GridFormat = "yyyy-MM-dd HH:mm:ss";

    private readonly ServerClock _savedClock = ServerTimeHelper.ActiveServerClock;
    private readonly TimeDisplayMode _savedMode = ServerTimeHelper.CurrentDisplayMode;

    public void Dispose()
    {
        ServerTimeHelper.ActiveServerClock = _savedClock;
        ServerTimeHelper.CurrentDisplayMode = _savedMode;
    }

    /* The second 01:30 of the autumn change day on a US Eastern clock: 06:30Z. */
    private static readonly DateTime SecondOneThirty = DisplayZoneFixtures.At(2026, 11, 1, 6, 30);

    private static Func<TimeZoneInfo> Eastern => () => DisplayZoneFixtures.Eastern;

    private static Func<TimeZoneInfo> Utc => () => TimeZoneInfo.Utc;

    /// <summary>
    /// The text of every column of <paramref name="rowType"/> that holds an instant, each set to
    /// <paramref name="utc"/>: the collection time, and for Query Store also its first and last execution.
    /// </summary>
    private static string[] InstantTexts(string rowType, Func<TimeZoneInfo>? zone, DateTime utc)
    {
        switch (rowType)
        {
            case nameof(QueryStatsHistoryRow):
                return [new QueryStatsHistoryRow { CollectionTime = utc, Zone = zone }.CollectionTimeLocal];
            case nameof(ProcedureStatsHistoryRow):
                return [new ProcedureStatsHistoryRow { CollectionTime = utc, Zone = zone }.CollectionTimeLocal];
            case nameof(QueryStoreHistoryRow):
                var row = new QueryStoreHistoryRow
                {
                    CollectionTime = utc,
                    FirstExecutionTime = utc,
                    LastExecutionTime = utc,
                    Zone = zone
                };
                return [row.CollectionTimeLocal, row.FirstExecutionTimeLocal, row.LastExecutionTimeLocal];
            default:
                throw new ArgumentOutOfRangeException(nameof(rowType), rowType, "not a history row type");
        }
    }

    /// <summary>
    /// With a zone, the instant reads as <see cref="DisplayZone.Format"/> writes it in that zone: 06:30Z is the second
    /// 01:30 on a US Eastern clock, which carries its -05:00 suffix, and in UTC it is 06:30.
    /// </summary>
    [Theory]
    [InlineData(nameof(QueryStatsHistoryRow))]
    [InlineData(nameof(ProcedureStatsHistoryRow))]
    [InlineData(nameof(QueryStoreHistoryRow))]
    public void AZone_WordsEveryInstantColumn_InThatZone(string rowType)
    {
        var inEastern = DisplayZone.Format(SecondOneThirty, DisplayZoneFixtures.Eastern, GridFormat);
        Assert.Equal("2026-11-01 01:30:00 -05:00", inEastern);

        foreach (var text in InstantTexts(rowType, Eastern, SecondOneThirty))
        {
            Assert.Equal(inEastern, text);
        }

        foreach (var text in InstantTexts(rowType, Utc, SecondOneThirty))
        {
            Assert.Equal("2026-11-01 06:30:00", text);
        }
    }

    /// <summary>
    /// The point of the zone: with one set, the text does not depend on whichever server's clock, or display mode, is
    /// active when the row is drawn.
    /// </summary>
    [Theory]
    [InlineData(nameof(QueryStatsHistoryRow))]
    [InlineData(nameof(ProcedureStatsHistoryRow))]
    [InlineData(nameof(QueryStoreHistoryRow))]
    public void AZone_IsNotMovedByTheActiveServersClock_OrTheDisplayMode(string rowType)
    {
        var expected = InstantTexts(rowType, Eastern, SecondOneThirty);

        foreach (var clock in new[]
                 {
                     ServerClock.Utc,
                     ServerClock.FixedOffset(540),
                     ServerClock.FixedOffset(-480),
                     ServerClock.Resolve("Eastern Standard Time", -300)
                 })
        {
            foreach (var mode in Enum.GetValues<TimeDisplayMode>())
            {
                ServerTimeHelper.ActiveServerClock = clock;
                ServerTimeHelper.CurrentDisplayMode = mode;

                Assert.Equal(expected, InstantTexts(rowType, Eastern, SecondOneThirty));
            }
        }
    }

    /// <summary>
    /// A row nothing set a zone on keeps today's text: <see cref="ServerTimeHelper.FormatServerTime(DateTime, string)"/>
    /// on the active clock, in every display mode.
    /// </summary>
    [Theory]
    [InlineData(nameof(QueryStatsHistoryRow))]
    [InlineData(nameof(ProcedureStatsHistoryRow))]
    [InlineData(nameof(QueryStoreHistoryRow))]
    public void WithoutAZone_TheTextIsFormatServerTimes(string rowType)
    {
        foreach (var clock in new[] { ServerClock.FixedOffset(330), ServerClock.Resolve("Eastern Standard Time", -300) })
        {
            foreach (var mode in Enum.GetValues<TimeDisplayMode>())
            {
                ServerTimeHelper.ActiveServerClock = clock;
                ServerTimeHelper.CurrentDisplayMode = mode;

                foreach (var text in InstantTexts(rowType, null, SecondOneThirty))
                {
                    Assert.Equal(ServerTimeHelper.FormatServerTime(SecondOneThirty), text);
                }
            }
        }
    }

    /// <summary>
    /// Query Store's first and last execution are nullable and a sentinel <see cref="DateTime.MinValue"/> can reach
    /// the grid: a null is blank and the sentinel stays the sentinel, with a zone as without one.
    /// </summary>
    [Fact]
    public void QueryStoreRow_KeepsItsNullAndMinValueHandling_WithAZone()
    {
        foreach (var zone in new Func<TimeZoneInfo>?[] { null, Eastern, Utc })
        {
            var blank = new QueryStoreHistoryRow { Zone = zone };
            Assert.Equal("", blank.FirstExecutionTimeLocal);
            Assert.Equal("", blank.LastExecutionTimeLocal);

            var sentinel = new QueryStoreHistoryRow
            {
                CollectionTime = DateTime.MinValue,
                FirstExecutionTime = DateTime.MinValue,
                LastExecutionTime = DateTime.MinValue,
                Zone = zone
            };
            Assert.Equal("0001-01-01 00:00:00", sentinel.CollectionTimeLocal);
            Assert.Equal("0001-01-01 00:00:00", sentinel.FirstExecutionTimeLocal);
            Assert.Equal("0001-01-01 00:00:00", sentinel.LastExecutionTimeLocal);
        }
    }

    /// <summary>
    /// The sys.dm_exec_* stamps are the SQL server's own wall clock, not an instant: with a zone set they read exactly
    /// as they do without one. In Server mode a wall clock shows as it stands; sent through the zone as if it were UTC
    /// it would move by the server's offset.
    /// </summary>
    [Fact]
    public void TheServerClockColumns_AreAWallClockNotAnInstant_SoAZoneDoesNotMoveThem()
    {
        ServerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        var stamp = DisplayZoneFixtures.At(2026, 9, 9, 10, 0);

        var query = new QueryStatsHistoryRow
        {
            CreationTime = stamp,
            LastExecutionTime = stamp,
            Zone = () => DisplayZoneFixtures.Pacific
        };
        Assert.Equal("2026-09-09 10:00:00", query.CreationTimeLocal);
        Assert.Equal("2026-09-09 10:00:00", query.LastExecutionTimeLocal);

        var procedure = new ProcedureStatsHistoryRow
        {
            CachedTime = stamp,
            LastExecutionTime = stamp,
            Zone = () => DisplayZoneFixtures.Pacific
        };
        Assert.Equal("2026-09-09 10:00:00", procedure.CachedTimeLocal);
        Assert.Equal("2026-09-09 10:00:00", procedure.LastExecutionTimeLocal);

        Assert.Equal(ServerTimeHelper.FormatServerClock(stamp), query.CreationTimeLocal);
        Assert.Equal(ServerTimeHelper.FormatServerClock(stamp), procedure.CachedTimeLocal);
    }

    /// <summary>
    /// Each window is a WPF window this suite does not instantiate, so the wiring is a source pin: the load hands
    /// every row the window's own zone (the one its chart, hover and summary use) before the rows are bound.
    /// </summary>
    [Theory]
    [InlineData("ProcedureHistoryWindow.xaml.cs")]
    [InlineData("QueryStatsHistoryWindow.xaml.cs")]
    [InlineData("QueryStoreHistoryWindow.xaml.cs")]
    public void EachHistoryWindow_SetsItsZoneOnEveryLoadedRow_BeforeTheRowsAreBound(string file)
    {
        var source = CodeOnly(ReadLite("Windows", file));

        var start = source.IndexOf("private async System.Threading.Tasks.Task LoadHistoryAsync()", StringComparison.Ordinal);
        Assert.True(start >= 0, "LoadHistoryAsync is no longer in the source; update this pin.");
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, "the end of LoadHistoryAsync was not found.");
        var load = source[start..end];

        var loop = load.IndexOf("foreach (var row in _historyData)", StringComparison.Ordinal);
        var assign = load.IndexOf("row.Zone = _displayZone;", StringComparison.Ordinal);
        var bind = load.IndexOf("_filterManager!.UpdateData(_historyData)", StringComparison.Ordinal);

        Assert.True(loop >= 0, $"{file}: the load no longer walks the loaded rows.");
        Assert.True(assign > loop, $"{file}: the loaded rows are not given the window's zone.");
        Assert.True(bind > assign, $"{file}: the rows are bound before they are given the window's zone.");
    }

    /* Line and block comments removed, and line endings normalised, so a pin reads code only. */
    private static string CodeOnly(string source)
    {
        var lf = source.Replace("\r\n", "\n");
        lf = Regex.Replace(lf, @"/\*.*?\*/", string.Empty, RegexOptions.Singleline);
        return Regex.Replace(lf, @"//[^\n]*", string.Empty);
    }

    private static string ReadLite(string folder, string file, [CallerFilePath] string thisFile = "") =>
        File.ReadAllText(Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", "Lite", folder, file)));
}
