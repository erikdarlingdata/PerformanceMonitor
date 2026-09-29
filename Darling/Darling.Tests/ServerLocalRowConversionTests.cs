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
using System.Text.RegularExpressions;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.McpServerClockTestSupport;

namespace Darling.Tests;

/// <summary>
/// #4821: the analysis and alert reads that turn a SQL Server's LOCAL wall-clock column into UTC convert each
/// row with the offset in force at THAT row (<see cref="ServerClock"/>), not with the one newest
/// <c>server_properties.utc_offset_minutes</c>. US Eastern falls back on 2026-11-01 and springs forward on
/// 2026-03-08, so a January row is -5 h and a July row is -4 h. Every case below has a newest snapshot from the
/// summer (-240) and a zone, the state a server is in for the months after a spring change.
/// </summary>
public sealed class ServerLocalRowConversionTests
{
    /* The newest snapshot said -240 (summer), and the server reports its zone. */
    private static ServerClock SummerNewest => Eastern(-240);

    [Fact]
    public void CreatedByWindowStart_WinterPlan_ConvertsWithTheWinterOffset()
    {
        /* 2026-01-15 08:00 local is 13:00Z. The newest offset (-240) would have said 12:00Z. */
        Assert.False(ServerLocalTimes.CreatedByWindowStart(SummerNewest, Naive(2026, 1, 15, 8), Naive(2026, 1, 15, 12, 30)));
        Assert.True(ServerLocalTimes.CreatedByWindowStart(SummerNewest, Naive(2026, 1, 15, 8), Naive(2026, 1, 15, 13)));
    }

    [Fact]
    public void CreatedByWindowStart_SummerPlan_IsUnchanged()
    {
        /* 2026-07-15 08:00 local is 12:00Z, the same instant the newest offset gives. */
        Assert.True(ServerLocalTimes.CreatedByWindowStart(SummerNewest, Naive(2026, 7, 15, 8), Naive(2026, 7, 15, 12)));
        Assert.False(ServerLocalTimes.CreatedByWindowStart(SummerNewest, Naive(2026, 7, 15, 8), Naive(2026, 7, 15, 11, 59)));
    }

    [Fact]
    public void CreatedByWindowStart_ServerWithNoZone_ConvertsWithItsFixedOffsetExactlyAsBefore()
    {
        var offsetOnly = ServerClock.Resolve(null, -240);

        /* A fixed offset has no daylight saving change to follow: January and July both shift by 4 h. */
        Assert.True(ServerLocalTimes.CreatedByWindowStart(offsetOnly, Naive(2026, 1, 15, 8), Naive(2026, 1, 15, 12)));
        Assert.False(ServerLocalTimes.CreatedByWindowStart(offsetOnly, Naive(2026, 1, 15, 8), Naive(2026, 1, 15, 11, 59)));
        Assert.True(ServerLocalTimes.CreatedByWindowStart(offsetOnly, Naive(2026, 7, 15, 8), Naive(2026, 7, 15, 12)));
    }

    [Fact]
    public void CreatedByWindowStart_NoZoneAndNoOffset_IsUtc()
    {
        var utc = ServerClock.Resolve(null, null);

        Assert.True(ServerLocalTimes.CreatedByWindowStart(utc, Naive(2026, 1, 15, 8), Naive(2026, 1, 15, 8)));
        Assert.False(ServerLocalTimes.CreatedByWindowStart(utc, Naive(2026, 1, 15, 8), Naive(2026, 1, 15, 7, 59)));
    }

    [Fact]
    public void CreatedByWindowStart_NullCreationTime_IsNeverCreatedBeforeTheWindow()
    {
        Assert.False(ServerLocalTimes.CreatedByWindowStart(SummerNewest, null, Naive(2026, 12, 31, 23)));
    }

    [Fact]
    public void ClockFrom_ReadsTheZoneWhereOneIsReportedAndTheOffsetElsewhere()
    {
        Assert.Equal(-300, ServerLocalTimes.ClockFrom(EasternWindowsId, -240).OffsetMinutesAt(Naive(2026, 1, 15, 13)));
        Assert.Equal(-240, ServerLocalTimes.ClockFrom(null, -240).OffsetMinutesAt(Naive(2026, 1, 15, 13)));
        Assert.Equal(0, ServerLocalTimes.ClockFrom(null, null).OffsetMinutesAt(Naive(2026, 1, 15, 13)));
    }

    /* The window is (12:30Z, 13:30Z]. */
    private static readonly DateTime WindowStart = Naive(2026, 1, 15, 12, 30);
    private static readonly DateTime WindowEnd = Naive(2026, 1, 15, 13, 30);

    private static DateTime Seconds(int hour, int minute, int second) =>
        new(2026, 1, 15, hour, minute, second, DateTimeKind.Unspecified);

    [Fact]
    public void TraceLinesInWindow_WinterRows_KeepTheExactWindowAndDropTheRowsOutsideIt()
    {
        var rows = new List<(DateTime? EventTimeLocal, string? TextData)>
        {
            (Seconds(8, 30, 1), "just past the end"),      /* 13:30:01Z */
            (Seconds(7, 30, 1), "just after the start"),   /* 12:30:01Z */
            (Seconds(7, 30, 0), "on the start"),           /* 12:30:00Z: the start is exclusive */
            (Seconds(8, 30, 0), "on the end"),             /* 13:30:00Z: the end is inclusive */
            (Seconds(9, 15, 0), "past the end"),           /* 14:15Z: the rough filter (newest offset) reads it as 13:15Z */
            (null, "no time"),
        };

        var lines = ServerLocalTimes.TraceLinesInWindow(rows, SummerNewest, WindowStart, WindowEnd);

        Assert.Equal(new[] { "just after the start", "on the end" }, lines.Select(l => l.TextData).ToArray());
        Assert.Equal(new[] { Seconds(12, 30, 1), Seconds(13, 30, 0) }, lines.Select(l => l.EventTimeUtc).ToArray());
        Assert.All(lines, l => Assert.Equal(DateTimeKind.Unspecified, l.EventTimeUtc.Kind));
    }

    [Fact]
    public void TraceLinesInWindow_SummerRow_IsUnchanged()
    {
        var summerStart = Naive(2026, 7, 15, 12, 30);
        var summerEnd = Naive(2026, 7, 15, 13, 30);
        var rows = new List<(DateTime? EventTimeLocal, string? TextData)> { (Naive(2026, 7, 15, 9), "summer") };

        var lines = ServerLocalTimes.TraceLinesInWindow(rows, SummerNewest, summerStart, summerEnd);

        Assert.Equal(Naive(2026, 7, 15, 13), Assert.Single(lines).EventTimeUtc);
    }

    [Fact]
    public void TraceLinesInWindow_ServerWithNoZone_ConvertsWithItsFixedOffsetExactlyAsBefore()
    {
        var rows = new List<(DateTime? EventTimeLocal, string? TextData)> { (Naive(2026, 1, 15, 8), "January") };

        /* -240 for every date: 08:00 local is 12:00Z, which is before the start. */
        Assert.Empty(ServerLocalTimes.TraceLinesInWindow(rows, ServerClock.Resolve(null, -240), WindowStart, WindowEnd));
        /* and with the zone it is 13:00Z, inside. */
        Assert.Single(ServerLocalTimes.TraceLinesInWindow(rows, SummerNewest, WindowStart, WindowEnd));
    }

    [Fact]
    public void TraceLinesInWindow_NoZoneAndNoOffset_ReadsTheColumnAsUtc()
    {
        var rows = new List<(DateTime? EventTimeLocal, string? TextData)> { (Naive(2026, 1, 15, 13), "utc") };

        Assert.Equal(Naive(2026, 1, 15, 13), Assert.Single(ServerLocalTimes.TraceLinesInWindow(rows, ServerClock.Utc, WindowStart, WindowEnd)).EventTimeUtc);
    }

    [Fact]
    public void JobStartOffsetMinutes_IsTheOffsetInForceAtTheStartTime()
    {
        Assert.Equal(-300, ServerLocalTimes.JobStartOffsetMinutes(SummerNewest, Naive(2026, 1, 15, 8), -240));
        Assert.Equal(-240, ServerLocalTimes.JobStartOffsetMinutes(SummerNewest, Naive(2026, 7, 15, 8), -240));
    }

    [Fact]
    public void JobStartOffsetMinutes_ServerWithNoZone_KeepsItsOffset()
    {
        Assert.Equal(-240, ServerLocalTimes.JobStartOffsetMinutes(ServerClock.Resolve(null, -240), Naive(2026, 1, 15, 8), -240));
    }

    [Fact]
    public void JobStartOffsetMinutes_NoOffsetCollected_StaysNull()
    {
        Assert.Null(ServerLocalTimes.JobStartOffsetMinutes(ServerClock.Utc, Naive(2026, 1, 15, 8), null));
    }

    [Fact]
    public void JobStartOffsetMinutes_UnknownStartTime_KeepsTheSnapshotOffset()
    {
        /* The reader spells a NULL start_time as DateTime.MinValue: there is no instant to look an offset up at. */
        Assert.Equal(-240, ServerLocalTimes.JobStartOffsetMinutes(SummerNewest, DateTime.MinValue, -240));
    }

    [Fact]
    public void JobStartOffsetMinutes_FeedsAnomalousJobInfoAWinterStartTimeInUtc()
    {
        var start = Naive(2026, 1, 15, 8);
        var job = new AnomalousJobInfo
        {
            StartTime = start,
            UtcOffsetMinutes = ServerLocalTimes.JobStartOffsetMinutes(SummerNewest, start, -240)
        };

        Assert.Equal(-300, job.UtcOffsetMinutes);
        Assert.Equal(Naive(2026, 1, 15, 13), job.StartTimeUtc);
    }

    /* ---- the SQL: each site returns the zone and hands the local column back raw ---- */

    private static readonly Regex OffsetArithmeticOnProjection = new(
        @"make_interval\s*\(\s*mins\s*=>\s*svr\.offset_minutes\s*\)\s+AS\b", RegexOptions.IgnoreCase);

    public static IEnumerable<object[]> ConvertedSites() => new[]
    {
        new object[] { "trace lines", DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql },
        new object[] { "parameter-sensitivity drill-down", PgDrillDownCollector.ParameterSensitiveSql },
        new object[] { "regressed-queries drill-down (raw)", PgDrillDownCollector.RegressedQueriesSql },
        new object[] { "regressed-queries drill-down (table)", PgDrillDownCollector.RegressedQueriesTableSql },
        new object[] { "parameter-sensitivity fact", PgFactCollector.ParameterSensitivitySql },
        new object[] { "long-running jobs alert", DarlingAlertReadAdapter.AnomalousJobsSql },
    };

    [Theory]
    [MemberData(nameof(ConvertedSites))]
    public void Sql_ReturnsTheZoneAndDoesNotApplyTheNewestOffsetToTheProjectedColumn(string site, string sql)
    {
        Assert.True(sql.Contains("time_zone_id", StringComparison.Ordinal), site + " must return time_zone_id");
        Assert.False(OffsetArithmeticOnProjection.IsMatch(sql), site + " must not subtract the newest offset in its projection");
        Assert.False(sql.Contains("creation_time_utc", StringComparison.Ordinal), site + " must not filter on an offset-converted creation time");
        Assert.False(sql.Contains("event_time_utc", StringComparison.Ordinal), site + " must not project an offset-converted event time");
    }

    [Theory]
    [MemberData(nameof(ConvertedSites))]
    public void Sql_TakesTheZoneFromTheSameNewestRowAsTheOffset(string site, string sql)
    {
        Assert.True(sql.Contains("sp.time_zone_id", StringComparison.Ordinal), site + " must read time_zone_id from server_properties");
        Assert.True(sql.Contains("sp.utc_offset_minutes IS NOT NULL", StringComparison.Ordinal), site + " must skip snapshots without an offset");
        Assert.True(sql.Contains("ORDER BY sp.collection_time DESC", StringComparison.Ordinal), site + " must take the newest snapshot");
    }

    [Fact]
    public void TraceLinesSql_WidensBothBoundsByAnHourAndLeavesTheExactWindowToTheReader()
    {
        var sql = DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql;

        Assert.Contains("- make_interval(mins => svr.offset_minutes) > $2 - interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("- make_interval(mins => svr.offset_minutes) <= $3 + interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("dte.event_time AS event_time_local", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void PlanCreationSql_HandsBackTheLocalCreationTimeAndLeavesTheExactTestToTheReader()
    {
        foreach (var sql in new[] { PgDrillDownCollector.ParameterSensitiveSql, PgFactCollector.ParameterSensitivitySql })
        {
            Assert.Contains("<= $2 + interval '1 hour'", sql, StringComparison.Ordinal);
            Assert.Contains("creation_time AS creation_time_local", sql, StringComparison.Ordinal);
        }
    }
}
