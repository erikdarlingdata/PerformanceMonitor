/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="ViewerTimeHelper"/> — the viewer's port of Lite's ServerTimeHelper and the single
/// chokepoint every rendered timestamp routes through. The store is naive-UTC, so: UTC = raw, Local =
/// machine-local (SpecifyKind(Utc).ToLocalTime()), Server = UTC + the server's collected offset. Also pins
/// the inverse used by the custom-range pickers and the per-server offset read SQL. The conversion core is
/// exercised through the pure <c>ConvertToDisplay</c>/<c>ConvertFromDisplay</c> overloads so the process-
/// wide statics are not mutated; two focused tests confirm the static <c>ForDisplay</c>/<c>DisplayToNaiveUtc</c>
/// delegate to them (saving/restoring the statics).
/// </summary>
/* Serialized: these classes flip the process-wide ViewerTimeHelper.CurrentDisplayMode static (each
   restores in finally, but xUnit runs CLASSES in parallel — two flippers racing corrupts the mode a
   third class reads). One shared collection serializes them. */
[Collection("viewer-time-statics")]
public sealed class ViewerTimeHelperTests
{
    private static readonly DateTime NaiveUtc = new(2026, 7, 1, 12, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void ConvertToDisplay_Utc_ReturnsRawStoredValue()
    {
        Assert.Equal(NaiveUtc, ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.UTC, utcOffsetMinutes: 330));
    }

    [Fact]
    public void ConvertToDisplay_Local_MatchesMachineLocalConvention()
    {
        /* The viewer's historical convention (the former ViewerDataService.ToLocalTime): treat the
           Unspecified store value as UTC, then convert to the viewer machine's local time. Offset is ignored. */
        var expected = DateTime.SpecifyKind(NaiveUtc, DateTimeKind.Utc).ToLocalTime();
        var actual = ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.LocalTime, utcOffsetMinutes: 999);
        Assert.Equal(expected, actual);
        Assert.Equal(DateTimeKind.Local, actual.Kind);
    }

    [Theory]
    [InlineData(0, 0)]        // a UTC server: Server == UTC
    [InlineData(330, 330)]    // India +05:30
    [InlineData(-480, -480)]  // US Pacific standard -08:00
    [InlineData(-300, -300)]  // US Eastern standard -05:00
    public void ConvertToDisplay_Server_AddsCollectedOffset(int offsetMinutes, int expectedShiftMinutes)
    {
        Assert.Equal(
            NaiveUtc.AddMinutes(expectedShiftMinutes),
            ViewerTimeHelper.ConvertToDisplay(NaiveUtc, TimeDisplayMode.ServerTime, offsetMinutes));
    }

    [Theory]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    [InlineData(TimeDisplayMode.ServerTime)]
    public void ConvertFromDisplay_InvertsConvertToDisplay(TimeDisplayMode mode)
    {
        const int offset = -300;
        var display = ViewerTimeHelper.ConvertToDisplay(NaiveUtc, mode, offset);
        var backToStore = ViewerTimeHelper.ConvertFromDisplay(display, mode, offset);

        /* Round-trips to the original naive-UTC store value, re-stamped Unspecified (the kind the reads send). */
        Assert.Equal(NaiveUtc, backToStore);
        Assert.Equal(DateTimeKind.Unspecified, backToStore.Kind);
    }

    [Fact]
    public void ConvertFromDisplay_Server_SubtractsOffset()
    {
        /* A picker value the user typed in Server time maps back to the naive-UTC window bound. */
        var serverWallClock = new DateTime(2026, 7, 1, 7, 0, 0);   // 07:00 on a -05:00 server
        var expectedUtc = DateTime.SpecifyKind(new DateTime(2026, 7, 1, 12, 0, 0), DateTimeKind.Unspecified);
        Assert.Equal(expectedUtc, ViewerTimeHelper.ConvertFromDisplay(serverWallClock, TimeDisplayMode.ServerTime, -300));
    }

    [Fact]
    public void ForDisplay_ReadsProcessWideStatics()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedOffset = ViewerTimeHelper.UtcOffsetMinutes;
        try
        {
            ViewerTimeHelper.UtcOffsetMinutes = 90;

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            Assert.Equal(NaiveUtc.AddMinutes(90), ViewerTimeHelper.ForDisplay(NaiveUtc));

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            Assert.Equal(NaiveUtc, ViewerTimeHelper.ForDisplay(NaiveUtc));

            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.LocalTime;
            Assert.Equal(DateTime.SpecifyKind(NaiveUtc, DateTimeKind.Utc).ToLocalTime(), ViewerTimeHelper.ForDisplay(NaiveUtc));
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    [Fact]
    public void DisplayToNaiveUtc_ReadsProcessWideStatics_AndRoundTripsForDisplay()
    {
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedOffset = ViewerTimeHelper.UtcOffsetMinutes;
        try
        {
            ViewerTimeHelper.UtcOffsetMinutes = -300;
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

            var display = ViewerTimeHelper.ForDisplay(NaiveUtc);
            Assert.Equal(NaiveUtc, ViewerTimeHelper.DisplayToNaiveUtc(display));
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    /// <summary>
    /// <c>ViewerDataService.FormatServerClock</c> — the renderer for a column already in the monitored
    /// server's own frame — emits that clock verbatim in ALL THREE display modes, because
    /// <see cref="ViewerTimeHelper"/> has no server-local arm for it to route through: every overload
    /// above takes naive UTC.
    ///
    /// <para>Asserted rather than inferred from the absence of a mode argument, and asserted at the
    /// fleet's measured -240 so a renderer that started converting would move. Lite's same-named
    /// <c>ServerTimeHelper.FormatServerClock</c> takes the same frame and DOES honour the preference,
    /// through <c>ServerTimeHelper.ConvertForDisplay</c> — the shared name is an INPUT CONTRACT, not
    /// shared behaviour, and a reader who assumes otherwise is why #3207's sites spread. Whether the
    /// viewer SHOULD convert here is a product question; this pin only stops the answer changing by
    /// accident, in either direction.</para>
    /// </summary>
    [Fact]
    public void FormatServerClock_IgnoresTheDisplayMode_AtTheFleetOffset()
    {
        var serverClock = new DateTime(2026, 7, 1, 13, 45, 7, DateTimeKind.Unspecified);
        var savedMode = ViewerTimeHelper.CurrentDisplayMode;
        var savedOffset = ViewerTimeHelper.UtcOffsetMinutes;
        try
        {
            ViewerTimeHelper.UtcOffsetMinutes = -240;

            foreach (var mode in new[] { TimeDisplayMode.ServerTime, TimeDisplayMode.LocalTime, TimeDisplayMode.UTC })
            {
                ViewerTimeHelper.CurrentDisplayMode = mode;
                Assert.Equal("2026-07-01 13:45:07", ViewerDataService.FormatServerClock(serverClock));
                Assert.Equal("", ViewerDataService.FormatServerClock(null));
            }

            /* And the naive-UTC renderer in the same viewer DOES move with the mode, so the assertion
               above is about this renderer rather than about a display mode nothing honours. */
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
            var server = ViewerTimeHelper.ForDisplay(serverClock);
            ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;
            Assert.NotEqual(server, ViewerTimeHelper.ForDisplay(serverClock));
        }
        finally
        {
            ViewerTimeHelper.CurrentDisplayMode = savedMode;
            ViewerTimeHelper.UtcOffsetMinutes = savedOffset;
        }
    }

    [Fact]
    public void ServerUtcOffsetSql_ReadsLatestNonNullOffsetForServer()
    {
        var sql = ViewerDataService.ServerUtcOffsetSql;
        Assert.Contains("utc_offset_minutes", sql, StringComparison.Ordinal);
        Assert.Contains("FROM server_properties", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("utc_offset_minutes IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        /* Darling has no v_server_properties view — the base table, bare-name resolved via the Search Path. */
        Assert.DoesNotContain("v_server_properties", sql, StringComparison.Ordinal);
    }
}
