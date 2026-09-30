/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4766: the Darling viewer's Server time mode converts with the server's time zone where SQL Server reports
/// one, so a time on either side of a daylight-saving change shows the server's wall clock and a picker range
/// across the change converts back to the right UTC bounds. US Eastern springs forward on 2026-03-08 and falls
/// back on 2026-11-01 (the zone ids <c>LocalClockBucketKeyTests</c> uses).
/// </summary>
/* Serialized with the other classes that flip the process-wide ViewerTimeHelper statics. */
[Collection("viewer-time-statics")]
public sealed class ViewerServerClockTests
{
    private const string EasternWindowsId = "Eastern Standard Time";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static ServerClock Eastern => ServerClock.Resolve(EasternWindowsId, -300);

    [Fact]
    public void ServerMode_ShowsTheServerWallClock_OnBothSidesOfTheFallBack()
    {
        /* Daylight time (-4 h) before the change, standard time (-5 h) after it. One fixed -300 offset would
           show 11:00 for the first. */
        Assert.Equal(Naive(2026, 10, 31, 12),
            ViewerTimeHelper.ConvertToDisplay(Naive(2026, 10, 31, 16), TimeDisplayMode.ServerTime, Eastern));
        Assert.Equal(Naive(2026, 11, 2, 11),
            ViewerTimeHelper.ConvertToDisplay(Naive(2026, 11, 2, 16), TimeDisplayMode.ServerTime, Eastern));
    }

    [Fact]
    public void ServerMode_ShowsTheServerWallClock_OnBothSidesOfTheSpringForward()
    {
        Assert.Equal(Naive(2026, 3, 7, 11),
            ViewerTimeHelper.ConvertToDisplay(Naive(2026, 3, 7, 16), TimeDisplayMode.ServerTime, Eastern));
        Assert.Equal(Naive(2026, 3, 9, 12),
            ViewerTimeHelper.ConvertToDisplay(Naive(2026, 3, 9, 16), TimeDisplayMode.ServerTime, Eastern));
    }

    [Fact]
    public void ServerMode_APickerRangeAcrossTheChange_ConvertsBackToTheRightUtcBounds()
    {
        /* The user types a range on the server's wall clock: 08:00 on 2026-10-31 (daylight) to 08:00 on
           2026-11-02 (standard). The two bounds are 12:00Z and 13:00Z. */
        var from = Eastern.ToUtc(Naive(2026, 10, 31, 8));
        var to = Eastern.ToUtc(Naive(2026, 11, 2, 8));

        Assert.Equal(Naive(2026, 10, 31, 12), from);
        Assert.Equal(Naive(2026, 11, 2, 13), to);
        Assert.Equal(DateTimeKind.Unspecified, from.Kind);
    }

    [Fact]
    public void ServerMode_ASkippedPickerTime_DoesNotThrow_AndAFixedOffsetStillWorks()
    {
        /* 02:30 on 2026-03-08 never happened on the server. TimeZoneInfo.ConvertTimeToUtc throws on it. */
        Assert.Equal(Naive(2026, 3, 8, 7, 30), Eastern.ToUtc(Naive(2026, 3, 8, 2, 30)));

        /* A server with no zone id keeps the old fixed-offset arithmetic. */
        Assert.Equal(Naive(2026, 3, 8, 7, 30), ServerClock.FixedOffset(-300).ToUtc(Naive(2026, 3, 8, 2, 30)));
    }

    [Fact]
    public void ServerMode_ARepeatedPickerTime_TakesTheFirstOccurrence()
    {
        Assert.Equal(Naive(2026, 11, 1, 5, 30), Eastern.ToUtc(Naive(2026, 11, 1, 1, 30)));
    }

    [Theory]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void LocalAndUtcModes_DoNotDependOnTheServerClock(TimeDisplayMode mode)
    {
        var instant = Naive(2026, 10, 31, 16);

        Assert.Equal(
            ViewerTimeHelper.ConvertToDisplay(instant, mode, ServerClock.FixedOffset(-300)),
            ViewerTimeHelper.ConvertToDisplay(instant, mode, Eastern));
        /* The zone a typed time is read in (and a chart's labels are drawn in) is the same object either way. */
        Assert.Same(
            ViewerTimeHelper.DisplayZoneFor(mode, ServerClock.FixedOffset(-300)),
            ViewerTimeHelper.DisplayZoneFor(mode, Eastern));
    }

    [Fact]
    public void ServerClockValue_InServerMode_IsShownAsStored_AndInUtcModeGoesThroughTheZone()
    {
        /* A column already in the server's frame (msdb job times, the ADR cleaner) shows as stored in Server
           mode, and converts by the offset in force AT THAT TIME in UTC mode. */
        var beforeFallBack = Naive(2026, 10, 31, 8);
        var afterFallBack = Naive(2026, 11, 2, 8);

        Assert.Equal(beforeFallBack, ViewerTimeHelper.ConvertServerClockToDisplay(beforeFallBack, TimeDisplayMode.ServerTime, Eastern));
        Assert.Equal(Naive(2026, 10, 31, 12), ViewerTimeHelper.ConvertServerClockToDisplay(beforeFallBack, TimeDisplayMode.UTC, Eastern));
        Assert.Equal(Naive(2026, 11, 2, 13), ViewerTimeHelper.ConvertServerClockToDisplay(afterFallBack, TimeDisplayMode.UTC, Eastern));
    }

    [Fact]
    public void TheProcessWideClock_DrivesForDisplayAndTheDisplayZone()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            ViewerTimeHelper.ActiveServerClock = Eastern;

            Assert.Equal(Naive(2026, 10, 31, 12), ViewerTimeHelper.ForDisplay(Naive(2026, 10, 31, 16)));
            Assert.Equal(Naive(2026, 11, 2, 11), ViewerTimeHelper.ForDisplay(Naive(2026, 11, 2, 16)));
            /* The chart labels are drawn in the same clock: the display zone shows what ForDisplay shows. */
            Assert.Equal(Naive(2026, 10, 31, 12), DisplayZone.ToDisplay(Naive(2026, 10, 31, 16), ViewerTimeHelper.CurrentDisplayZone()));
            Assert.Equal(Naive(2026, 11, 2, 11), DisplayZone.ToDisplay(Naive(2026, 11, 2, 16), ViewerTimeHelper.CurrentDisplayZone()));

            /* The offset property reads the clock, and setting it installs a fixed offset. */
            Assert.Equal((int)TimeZoneInfo.FindSystemTimeZoneById(EasternWindowsId).GetUtcOffset(DateTime.UtcNow).TotalMinutes, ViewerTimeHelper.UtcOffsetMinutes);
            ViewerTimeHelper.UtcOffsetMinutes = 90;
            Assert.Equal(Naive(2026, 10, 31, 17, 30), ViewerTimeHelper.ForDisplay(Naive(2026, 10, 31, 16)));
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
        }
    }

    [Fact]
    public void FormatForDisplay_InServerMode_GivesEachInstantOfTheRepeatedHourItsOwnOffset()
    {
        /* US Eastern fell back at 06:00Z on 2026-11-01: 05:30Z is the FIRST 01:30 (daylight time, -04:00) and 06:30Z the
           SECOND (standard time, -05:00). Both used to print as 01:30, two grid rows nobody could tell apart (#4766). */
        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            Assert.Equal("2026-11-01 01:30:00 -04:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 5, 30), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("2026-11-01 01:30:00 -05:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 6, 30), "yyyy-MM-dd HH:mm:ss"));

            /* The whole repeated hour is covered, edge to edge: 05:00Z is the first 01:00 and 06:59Z the last minute of
               the second 01:xx. The format string is the site's own, so a time-only column carries the offset too. */
            Assert.Equal("01:00 -04:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 5, 0), "HH:mm"));
            Assert.Equal("01:59 -04:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 5, 59), "HH:mm"));
            Assert.Equal("01:00 -05:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 6, 0), "HH:mm"));
            Assert.Equal("01:59 -05:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 6, 59), "HH:mm"));

            /* A value that may be missing is the same text, and nothing at all when it is. */
            Assert.Equal("2026-11-01 01:30:00 -05:00", ViewerTimeHelper.FormatForDisplay((DateTime?)Naive(2026, 11, 1, 6, 30), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("", ViewerTimeHelper.FormatForDisplay((DateTime?)null, "yyyy-MM-dd HH:mm:ss"));
        });
    }

    [Fact]
    public void FormatForDisplay_OutsideTheRepeatedHour_IsTheForDisplayTextWithNothingAppended()
    {
        /* The minute before the repeated hour (00:59 daylight time), the first minute after it (02:00 standard time),
           and an ordinary summer and winter instant read exactly as ForDisplay(x).ToString(format) did. */
        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            Assert.Equal("2026-11-01 00:59:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 4, 59), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("2026-11-01 02:00:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 7, 0), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("2026-07-06 12:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 7, 6, 16, 0), "yyyy-MM-dd HH:mm"));
            Assert.Equal("2026-01-15 12:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 1, 15, 17, 0), "yyyy-MM-dd HH:mm"));

            foreach (var instant in new[] { Naive(2026, 11, 1, 4, 59), Naive(2026, 11, 1, 7, 0), Naive(2026, 7, 6, 16, 0), Naive(2026, 1, 15, 17, 0) })
            {
                Assert.Equal(ViewerTimeHelper.ForDisplay(instant).ToString("yyyy-MM-dd HH:mm:ss"), ViewerTimeHelper.FormatForDisplay(instant, "yyyy-MM-dd HH:mm:ss"));
            }
        });
    }

    [Fact]
    public void FormatForDisplay_InUtcMode_NeverAddsAnOffset()
    {
        /* UTC has no repeated hour: 05:30Z and 06:30Z are two different times, whatever the server's own zone is. */
        WithDisplay(TimeDisplayMode.UTC, Eastern, () =>
        {
            Assert.Equal("2026-11-01 05:30:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 5, 30), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("2026-11-01 06:30:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 6, 30), "yyyy-MM-dd HH:mm:ss"));
        });
    }

    [Fact]
    public void FormatForDisplay_OnAFixedOffsetClock_NeverAddsAnOffset_AtTheInstantsWhereAUsZoneRepeatsAnHour()
    {
        /* A fixed-offset clock (a server that reported no time zone id) has no repeated hour: at 05:30Z and 06:30Z on the
           day a US zone falls back, a -05:00 clock reads 00:30 and 01:30, each once. */
        WithDisplay(TimeDisplayMode.ServerTime, ServerClock.FixedOffset(-300), () =>
        {
            Assert.Equal("2026-11-01 00:30:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 5, 30), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("2026-11-01 01:30:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 6, 30), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("01:30", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 6, 30), "HH:mm"));
        });

        /* The same two instants on a zone clock read 01:30 twice. */
        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            Assert.Equal("2026-11-01 01:30:00 -04:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 5, 30), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("2026-11-01 01:30:00 -05:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 11, 1, 6, 30), "yyyy-MM-dd HH:mm:ss"));
        });
    }

    [Fact]
    public void FormatForDisplay_OnAZoneEastOfUtc_GivesTheTwoPassesOfTheRepeatedHourTheirPositiveOffsets()
    {
        /* Berlin falls back at 01:00Z on 2026-10-25 (03:00 CEST to 02:00 CET): 02:30 happens at 00:30Z (+02:00) and
           again at 01:30Z (+01:00). */
        WithDisplay(TimeDisplayMode.ServerTime, ServerClock.Resolve("W. Europe Standard Time", 60), () =>
        {
            Assert.Equal("2026-10-25 02:30:00 +02:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 10, 25, 0, 30), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("2026-10-25 02:30:00 +01:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 10, 25, 1, 30), "yyyy-MM-dd HH:mm:ss"));

            /* The hour before and the hour after are bare. */
            Assert.Equal("2026-10-25 01:59:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 10, 24, 23, 59), "yyyy-MM-dd HH:mm:ss"));
            Assert.Equal("2026-10-25 03:00:00", ViewerTimeHelper.FormatForDisplay(Naive(2026, 10, 25, 2, 0), "yyyy-MM-dd HH:mm:ss"));
        });
    }

    [Fact]
    public void FormatForDisplay_FormatsOnTheCurrentCulture_AsTheSitesItReplacedDid()
    {
        /* "g" is the culture's own general date and short time. The renderer keeps the current culture, so the grids
           that print it read as they did before; only the offset suffix is new. */
        var german = new System.Globalization.CultureInfo("de-DE");

        WithDisplay(TimeDisplayMode.ServerTime, Eastern, () =>
        {
            var instant = Naive(2026, 11, 1, 6, 30);

            Assert.Equal(ViewerTimeHelper.ForDisplay(instant).ToString("g") + " -05:00", ViewerTimeHelper.FormatForDisplay(instant, "g"));
            Assert.NotEqual(
                ViewerTimeHelper.ForDisplay(instant).ToString("g", System.Globalization.CultureInfo.InvariantCulture) + " -05:00",
                ViewerTimeHelper.FormatForDisplay(instant, "g"));
        }, german);
    }

    /// <summary>Runs <paramref name="body"/> with the process-wide display mode and server clock set, on the invariant
    /// culture unless one is given, so a literal expected string does not depend on the machine's separators. All
    /// three are restored.</summary>
    private static void WithDisplay(TimeDisplayMode mode, ServerClock clock, Action body, System.Globalization.CultureInfo? culture = null)
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedClock = ViewerTimeHelper.ActiveServerClock;
        var savedCulture = System.Globalization.CultureInfo.CurrentCulture;
        try
        {
            ViewerTimeHelper.CurrentDisplayMode = mode;
            ViewerTimeHelper.ActiveServerClock = clock;
            System.Globalization.CultureInfo.CurrentCulture = culture ?? System.Globalization.CultureInfo.InvariantCulture;

            body();
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.ActiveServerClock = savedClock;
            System.Globalization.CultureInfo.CurrentCulture = savedCulture;
        }
    }

    [Fact]
    public void ServerClockSql_ReadsTheZoneAndTheOffsetFromTheSameNewestRow()
    {
        var sql = ViewerDataService.ServerClockSql;

        Assert.Contains("SELECT utc_offset_minutes, time_zone_id", sql, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
    }
}

/// <summary>
/// #4766: Job History no longer subtracts the latest offset in SQL. The stored server-local run time is
/// converted in C# with the server's <see cref="ServerClock"/>, so a run on either side of a daylight-saving
/// change lands at its real UTC time; the SQL pre-filters an hour wide and the window is applied exactly after.
/// </summary>
public sealed class ViewerJobHistoryServerClockTests
{
    private const string EasternWindowsId = "Eastern Standard Time";

    private static DateTime Naive(int year, int month, int day, int hour, int minute = 0) =>
        new(year, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static DataTable NewTable()
    {
        var table = new DataTable();
        table.Columns.Add("server_id", typeof(int));
        table.Columns.Add("server_name", typeof(string));
        table.Columns.Add("instance_id", typeof(long));
        table.Columns.Add("job_id", typeof(string));
        table.Columns.Add("job_name", typeof(string));
        table.Columns.Add("job_enabled", typeof(bool));
        table.Columns.Add("category_name", typeof(string));
        table.Columns.Add("step_id", typeof(int));
        table.Columns.Add("step_name", typeof(string));
        table.Columns.Add("run_status", typeof(int));
        table.Columns.Add("run_status_desc", typeof(string));
        table.Columns.Add("run_datetime_local", typeof(DateTime));
        table.Columns.Add("run_duration_seconds", typeof(long));
        table.Columns.Add("retries_attempted", typeof(int));
        table.Columns.Add("message", typeof(string));
        table.Columns.Add("last_success_run_local", typeof(DateTime));
        table.Columns.Add("is_long_running", typeof(bool));
        return table;
    }

    private static ViewerJobHistoryRow Read(int serverId, long instanceId, DateTime runLocal, DateTime? lastSuccessLocal,
        IReadOnlyDictionary<int, ServerClock> clocks)
    {
        var table = NewTable();
        table.Rows.Add(serverId, "srv", instanceId, "job", "Job", true, "cat", 0, "step", 1, "Succeeded", runLocal, 5L, 0, "ok",
            lastSuccessLocal.HasValue ? lastSuccessLocal.Value : DBNull.Value, false);

        using var reader = table.CreateDataReader();
        Assert.True(reader.Read());
        return ViewerDataService.ReadJobHistoryRow(reader, clocks);
    }

    private static ViewerJobHistoryRow RowAtUtc(long instanceId, DateTime? runUtc) =>
        new() { InstanceId = instanceId, RunDateTimeUtc = runUtc };

    [Fact]
    public void AJobRunStoredAtServerLocalTime_OnEachSideOfAChange_LandsAtTheRightUtcTime()
    {
        var clocks = new Dictionary<int, ServerClock> { [1] = ServerClock.Resolve(EasternWindowsId, -300) };

        var beforeFallBack = Read(1, 10, Naive(2026, 10, 31, 8), Naive(2026, 10, 31, 7), clocks);
        var afterFallBack = Read(1, 11, Naive(2026, 11, 2, 8), Naive(2026, 11, 2, 7), clocks);

        /* Daylight time (-4 h) then standard time (-5 h). One subtracted -300 would put the first at 13:00Z. */
        Assert.Equal(Naive(2026, 10, 31, 12), beforeFallBack.RunDateTimeUtc);
        Assert.Equal(Naive(2026, 10, 31, 11), beforeFallBack.LastSuccessfulRunUtc);
        Assert.Equal(Naive(2026, 11, 2, 13), afterFallBack.RunDateTimeUtc);
        Assert.Equal(Naive(2026, 11, 2, 12), afterFallBack.LastSuccessfulRunUtc);

        var beforeSpringForward = Read(1, 12, Naive(2026, 3, 7, 8), null, clocks);
        var afterSpringForward = Read(1, 13, Naive(2026, 3, 9, 8), null, clocks);
        Assert.Equal(Naive(2026, 3, 7, 13), beforeSpringForward.RunDateTimeUtc);
        Assert.Equal(Naive(2026, 3, 9, 12), afterSpringForward.RunDateTimeUtc);
        Assert.Null(afterSpringForward.LastSuccessfulRunUtc);
    }

    [Fact]
    public void AJobRunInASkippedOrRepeatedLocalHour_DoesNotThrow()
    {
        var clocks = new Dictionary<int, ServerClock> { [1] = ServerClock.Resolve(EasternWindowsId, -300) };

        Assert.Equal(Naive(2026, 3, 8, 7, 30), Read(1, 1, Naive(2026, 3, 8, 2, 30), null, clocks).RunDateTimeUtc);
        Assert.Equal(Naive(2026, 11, 1, 5, 30), Read(1, 2, Naive(2026, 11, 1, 1, 30), null, clocks).RunDateTimeUtc);
    }

    [Fact]
    public void AServerWithAFixedOffsetOrNoClock_ConvertsAsBefore()
    {
        var clocks = new Dictionary<int, ServerClock> { [1] = ServerClock.FixedOffset(-300) };

        Assert.Equal(Naive(2026, 10, 31, 13), Read(1, 1, Naive(2026, 10, 31, 8), null, clocks).RunDateTimeUtc);

        /* Server 2 has no server_properties row: its stored times are read as UTC (the old COALESCE(..., 0)). */
        Assert.Equal(Naive(2026, 10, 31, 8), Read(2, 2, Naive(2026, 10, 31, 8), null, clocks).RunDateTimeUtc);
    }

    [Fact]
    public void TheWindowIsApplied_ExactlyAfterTheConversion_NewestFirst_AndTrimmedToTheLimit()
    {
        var since = Naive(2026, 11, 2, 12);
        var rows = new List<ViewerJobHistoryRow>
        {
            RowAtUtc(1, since.AddMinutes(-30)), /* in the pre-filter's extra hour, but before the window */
            RowAtUtc(2, since),                 /* exactly at the start: kept */
            RowAtUtc(3, since.AddHours(1)),
            RowAtUtc(4, since.AddHours(1)),     /* same time, higher instance id: first */
            RowAtUtc(5, null),
        };

        var all = ViewerDataService.ApplyJobHistoryWindow(rows, since, limit: 100);
        Assert.Equal(new long[] { 4, 3, 2 }, all.ConvertAll(r => r.InstanceId));

        var trimmed = ViewerDataService.ApplyJobHistoryWindow(rows, since, limit: 2);
        Assert.Equal(new long[] { 4, 3 }, trimmed.ConvertAll(r => r.InstanceId));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheSql_NoLongerSubtractsTheOffsetInTheProjection_AndWidensTheWindowByAnHour(bool scoped)
    {
        var sql = ViewerDataService.BuildJobHistorySql(scoped);

        Assert.Contains("AND   jh.run_datetime >= $1 + make_interval(mins => so.offset_minutes) - interval '1 hour'", sql, StringComparison.Ordinal);
        Assert.Contains("top.run_datetime AS run_datetime_local", sql, StringComparison.Ordinal);
        Assert.Contains("MAX(jh.run_datetime) AS last_success_run_local", sql, StringComparison.Ordinal);
        Assert.Contains("base.run_datetime_local", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AS run_datetime_utc", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("AS last_success_run_utc", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void TheClockRead_TakesTheNewestOffsetRowPerServer_WithTheZoneWhereTheStoreHasIt(bool scoped, bool withZone)
    {
        var sql = ViewerDataService.BuildServerClocksSql(scoped, withZone);

        Assert.Contains("SELECT DISTINCT ON (server_id)", sql, StringComparison.Ordinal);
        Assert.Contains("utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY server_id, collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Equal(withZone, sql.Contains("time_zone_id", StringComparison.Ordinal));
        Assert.Equal(scoped, sql.Contains("AND   server_id = $1", StringComparison.Ordinal));
    }
}
