/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4766: a custom range on the Queries tab makes a round trip. The toolbar pickers hold a wall clock in the
/// display mode; <see cref="ServerTimeHelper.DisplayTimeToServerTime(DateTime, TimeDisplayMode, ServerClock)"/>
/// turns it into the server's wall clock; <see cref="LocalDataService.GetQueriesTabWindowUtc"/> turns that back
/// into the UTC instants the read compares against <c>collection_time</c>. In UTC and Local time the two
/// conversions cancel, so the window is the instant the user picked. That holds only if BOTH sides follow the
/// server's clock by the date of each bound: the picker side already did, and the window side, when it applied
/// the offset in force today to both bounds, put a winter bound an hour off when the range was viewed in summer
/// (and a summer bound an hour off when it was viewed in winter).
///
/// <para>The cases are chosen so a one-offset window fails whatever the season the test runs in: two ranges sit
/// inside one season (each is wrong for the other season's offset), and two cross a clock change (wrong at one
/// bound in any season). US Eastern, 2026: the spring-forward is 8 March (07:00 UTC) and the fall-back is
/// 1 November (06:00 UTC).</para>
/// </summary>
public sealed class QueriesTabWindowRoundTripTests
{
    private const string EasternZone = "Eastern Standard Time";

    /* The offset a snapshot taken in winter recorded. The zone must win over it, or this is the one-offset code. */
    private const int WinterOffset = -300;

    private static ServerClock Eastern() => ServerClock.Resolve(EasternZone, WinterOffset);

    private static DateTime Utc(string instant) =>
        DateTime.ParseExact(instant, "yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture, DateTimeStyles.None);

    private static string Range(DateTime start, DateTime end) =>
        start.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " to " +
        end.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);

    /* The UTC ranges the user means to pick; the last two rows cross a clock change. */
    private static readonly (string From, string To)[] Ranges =
    [
        ("2026-03-01 09:00", "2026-03-02 09:00"),
        ("2026-07-01 09:00", "2026-07-02 09:00"),
        ("2026-03-01 09:00", "2026-07-01 09:00"),
        ("2026-10-25 09:00", "2026-11-08 09:00"),
    ];

    /// <summary>
    /// The plain shape: pickers in UTC mode holding D, put through the picker's conversion and then the window
    /// method, come back as D. Viewed in September, a window that applied September's offset to a March bound
    /// returned 08:00 for a 09:00 pick.
    /// </summary>
    [Theory]
    [InlineData("2026-03-01 09:00", "2026-03-02 09:00")]
    [InlineData("2026-07-01 09:00", "2026-07-02 09:00")]
    [InlineData("2026-03-01 09:00", "2026-07-01 09:00")]
    [InlineData("2026-10-25 09:00", "2026-11-08 09:00")]
    public void UtcPickers_ComeBackAsTheSameUtcRange_ThroughTheWindow(string from, string to)
    {
        var clock = Eastern();
        var pickedFrom = Utc(from);
        var pickedTo = Utc(to);

        var fromServer = ServerTimeHelper.DisplayTimeToServerTime(pickedFrom, TimeDisplayMode.UTC, clock);
        var toServer = ServerTimeHelper.DisplayTimeToServerTime(pickedTo, TimeDisplayMode.UTC, clock);
        var (start, end) = LocalDataService.GetQueriesTabWindowUtc(24, fromServer, toServer, clock);

        Assert.Equal(Range(pickedFrom, pickedTo), Range(start, end));
    }

    /// <summary>
    /// The same round trip in every display mode: start from the instant, render it the way the toolbar does
    /// (<see cref="ServerTimeHelper.ConvertForDisplay(DateTime, TimeDisplayMode, ServerClock)"/>), take that
    /// picker value back through the picker's conversion, and read the window. Server-time mode is the identity
    /// on the picker side and the window's own conversion does the work; UTC and Local time cancel against it.
    /// All three end at the instant the user meant.
    /// </summary>
    [Theory]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    [InlineData(TimeDisplayMode.ServerTime)]
    public void EveryDisplayMode_ReadsTheInstantThePickerShows(TimeDisplayMode mode)
    {
        var clock = Eastern();

        foreach (var (from, to) in Ranges)
        {
            var fromInstant = Utc(from);
            var toInstant = Utc(to);

            var fromPicker = ServerTimeHelper.ConvertForDisplay(clock.ToServerLocal(fromInstant), mode, clock);
            var toPicker = ServerTimeHelper.ConvertForDisplay(clock.ToServerLocal(toInstant), mode, clock);
            var fromServer = ServerTimeHelper.DisplayTimeToServerTime(fromPicker, mode, clock);
            var toServer = ServerTimeHelper.DisplayTimeToServerTime(toPicker, mode, clock);
            var (start, end) = LocalDataService.GetQueriesTabWindowUtc(24, fromServer, toServer, clock);

            Assert.Equal(Range(fromInstant, toInstant), Range(start, end));
        }
    }
}
