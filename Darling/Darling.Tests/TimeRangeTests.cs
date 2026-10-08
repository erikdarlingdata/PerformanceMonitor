/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Ui;
using Xunit;

/* Darling.Tests covers the shared picker model (Lite and the Viewer both reference PerformanceMonitor.Ui).
   Lite.Tests does not compile this file: a linked Darling file must be named in build.yml's Lite path filter. */
namespace Darling.Tests;

/// <summary>
/// #5562: the shared time range grammar. Every row of <c>Fixtures/time-range-cases.json</c> must parse to the
/// stated instants, liveness and echo text (or the stated error code); the web module runs the same file.
/// </summary>
public sealed class TimeRangeFixtureTests
{
    private static readonly string FixturePath = Path.Combine(AppContext.BaseDirectory, "Fixtures", "time-range-cases.json");

    public static IEnumerable<object[]> Cases()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(FixturePath));
        foreach (var c in doc.RootElement.GetProperty("cases").EnumerateArray())
        {
            yield return new object[] { c.GetProperty("name").GetString()! + " [" + c.GetProperty("input").GetString() + "]" , c.GetRawText() };
        }
    }

    [Fact]
    public void Fixture_UsesZoneIdsThisRuntimeResolves()
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(FixturePath));
        var zones = doc.RootElement.GetProperty("cases").EnumerateArray().Select(c => c.GetProperty("zone").GetString()!).Distinct().ToList();
        Assert.NotEmpty(zones);
        foreach (var id in zones)
        {
            Assert.NotNull(TimeZoneInfo.FindSystemTimeZoneById(id));
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void Parse_MatchesTheSharedFixture(string label, string caseJson)
    {
        Assert.False(string.IsNullOrEmpty(label));
        using var doc = JsonDocument.Parse(caseJson);
        var c = doc.RootElement;
        var input = c.GetProperty("input").GetString();
        var now = ParseZ(c.GetProperty("now").GetString()!);
        var zone = TimeZoneInfo.FindSystemTimeZoneById(c.GetProperty("zone").GetString()!);

        var result = TimeRangeParser.Parse(input, now, zone);

        var expectedError = c.GetProperty("error");
        if (expectedError.ValueKind == JsonValueKind.String)
        {
            Assert.False(result.Ok, "expected an error but got " + result.Echo);
            Assert.Equal(expectedError.GetString(), result.ErrorCode);
            Assert.False(string.IsNullOrWhiteSpace(result.Error));
            Assert.Equal(string.Empty, result.Echo);
            return;
        }

        Assert.True(result.Ok, result.ErrorCode + ": " + result.Error);
        Assert.Equal(c.GetProperty("start").GetString(), Z(result.Range!.StartUtc));
        Assert.Equal(c.GetProperty("end").GetString(), Z(result.Range!.EndUtc));
        Assert.Equal(c.GetProperty("live").GetBoolean(), result.Range.IsLive);
        Assert.Equal(c.GetProperty("echo").GetString(), result.Echo);
    }

    internal static DateTime ParseZ(string iso)
        => DateTime.SpecifyKind(DateTime.ParseExact(iso, "yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Unspecified);

    internal static string Z(DateTime naiveUtc)
        => naiveUtc.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", CultureInfo.InvariantCulture);
}

/// <summary>#5562: the preset lists, calendar periods, ids, legacy hour mapping and the notes beside the picker.</summary>
public sealed class TimeRangeModelTests
{
    private static readonly TimeZoneInfo NewYork = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
    private static readonly DateTime Now = new(2026, 10, 8, 11, 1, 0, DateTimeKind.Unspecified);

    [Fact]
    public void RollingPresets_AreTheNineShortOnesInOrder()
    {
        Assert.Equal(new[] { "5m", "15m", "30m", "1h", "4h", "1d", "2d", "1w", "1mo" }, TimeRangePresets.Rolling.Select(p => p.Id));
    }

    [Fact]
    public void CalendarPeriods_AreTheEightInOrder()
    {
        Assert.Equal(
            new[] { "Today", "Yesterday", "Week to Date", "Previous Week", "Month to Date", "Previous Month", "Year to Date", "Previous Year" },
            TimeRangePresets.CalendarPeriods.Select(p => p.Name));
    }

    [Fact]
    public void CalendarPeriods_ReportTheirCurrentLength()
    {
        // Thursday 7:01 am in New York: Week to Date is Monday 00:00 to now.
        Assert.Equal("3d", TimeRangePresets.CurrentLength(TimeRangeSpec.ForPeriod(CalendarPeriod.WeekToDate), Now, NewYork));
        Assert.Equal("7h 1m", TimeRangePresets.CurrentLength(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), Now, NewYork));
        Assert.Equal("1d", TimeRangePresets.CurrentLength(TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday), Now, NewYork));
        Assert.Equal("7d", TimeRangePresets.CurrentLength(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousWeek), Now, NewYork));
        Assert.Equal("30d", TimeRangePresets.CurrentLength(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousMonth), Now, NewYork));
        Assert.Equal("365d", TimeRangePresets.CurrentLength(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousYear), Now, NewYork));
        // Just after midnight Today is under the minimum, so it has no length to show.
        Assert.Null(TimeRangePresets.CurrentLength(TimeRangeSpec.ForPeriod(CalendarPeriod.Today), new DateTime(2026, 10, 8, 4, 2, 0), NewYork));
    }

    [Fact]
    public void ShortDaysOnTheChangeDays_ReadAs23And25Hours()
    {
        var spec = TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday);
        Assert.Equal("23h", TimeRangePresets.CurrentLength(spec, new DateTime(2026, 3, 9, 16, 0, 0), NewYork));
        Assert.Equal("1d", TimeRangePresets.CurrentLength(spec, new DateTime(2026, 11, 2, 17, 0, 0), NewYork));
        Assert.True(spec.TryResolve(new DateTime(2026, 11, 2, 17, 0, 0), NewYork, out var longDay, out _));
        Assert.Equal(TimeSpan.FromHours(25), longDay!.Span);
    }

    [Fact]
    public void ConsecutivePeriods_TileWithNoGap()
    {
        var now = new DateTime(2026, 3, 9, 16, 0, 0);
        Assert.True(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousWeek).TryResolve(now, NewYork, out var lastWeek, out _));
        Assert.True(TimeRangeSpec.ForPeriod(CalendarPeriod.WeekToDate).TryResolve(now, NewYork, out var thisWeek, out _));
        Assert.Equal(lastWeek!.EndUtc, thisWeek!.StartUtc);
    }

    [Theory]
    [InlineData(1, "1h")]
    [InlineData(4, "4h")]
    [InlineData(12, "12h")]
    [InlineData(24, "1d")]
    [InlineData(168, "1w")]
    [InlineData(48, "2d")]
    [InlineData(720, "1mo")]
    [InlineData(36, "36h")]
    [InlineData(2160, "90d")]
    public void FromLegacyHours_MapsWholeHours(int hours, string expectedId)
    {
        var spec = TimeRangePresets.FromLegacyHours(hours);
        Assert.NotNull(spec);
        Assert.Equal(expectedId, spec!.Id);
        Assert.Equal(hours, spec.WholeHours);
        Assert.True(spec.TryResolve(Now, NewYork, out var range, out _));
        Assert.Equal(TimeSpan.FromHours(hours), range!.Span);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-4)]
    public void FromLegacyHours_RefusesZeroAndNegative(int hours)
        => Assert.Null(TimeRangePresets.FromLegacyHours(hours));

    [Fact]
    public void Ids_RoundTrip()
    {
        var specs = TimeRangePresets.Rolling.Concat(TimeRangePresets.CalendarPeriods).ToList();
        specs.Add(TimeRangeSpec.Relative(TimeSpan.FromMinutes(90)));
        specs.Add(TimeRangeSpec.FixedRange(new DateTime(2026, 10, 1, 4, 0, 0), new DateTime(2026, 10, 3, 4, 0, 0)));
        specs.Add(TimeRangeSpec.SinceInstant(new DateTime(2026, 10, 1, 4, 0, 0)));
        foreach (var spec in specs)
        {
            Assert.True(TimeRangeSpec.TryFromId(spec.Id, out var back), spec.Id);
            Assert.Equal(spec, back);
        }

        Assert.False(TimeRangeSpec.TryFromId("", out _));
        Assert.False(TimeRangeSpec.TryFromId("tomorrow", out _));
        Assert.False(TimeRangeSpec.TryFromId("0h", out _));
        Assert.False(TimeRangeSpec.TryFromId("h", out _));
        Assert.Equal(TimeSpan.FromDays(60), TimeRangeSpec.TryFromId("2mo", out var twoMonths) ? twoMonths!.Span : default);
    }

    [Fact]
    public void TheMinimumIsFiveMinutes_AndNothingIsWidened()
    {
        Assert.True(TimeRangeSpec.Relative(TimeSpan.FromMinutes(5)).TryResolve(Now, NewYork, out _, out _));
        Assert.False(TimeRangeSpec.Relative(TimeSpan.FromMinutes(4)).TryResolve(Now, NewYork, out var none, out var error));
        Assert.Null(none);
        Assert.Equal("too_short", error!.Code);
        Assert.Contains("5 minutes", error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void NoUpperCap_AYearsLongRangeResolves()
    {
        var result = TimeRangeParser.Parse("1000d", Now, NewYork);
        Assert.True(result.Ok);
        Assert.Equal(TimeSpan.FromDays(1000), result.Range!.Span);
    }

    [Fact]
    public void TypedAndPresetRanges_AgreeOnTheSameInstants()
    {
        Assert.Equal(TimeRangeSpec.Relative(TimeSpan.FromDays(7)), TimeRangeParser.Parse("7d", Now, NewYork).Spec);
        Assert.Equal(TimeRangePresets.Find("1mo"), TimeRangeParser.Parse("30 days", Now, NewYork).Spec);
        Assert.Equal(TimeRangeSpec.ForPeriod(CalendarPeriod.PreviousMonth), TimeRangeParser.Parse("last month", Now, NewYork).Spec);
        Assert.Equal(TimeRangeSpec.Relative(TimeSpan.FromDays(30)), TimeRangeParser.Parse("last 30 days", Now, NewYork).Spec);
    }

    [Fact]
    public void Resolve_FollowsTheZone_ForCalendarPeriodsButNotForFixedRanges()
    {
        var today = TimeRangeSpec.ForPeriod(CalendarPeriod.Yesterday);
        Assert.True(today.TryResolve(Now, NewYork, out var ny, out _));
        Assert.True(today.TryResolve(Now, TimeZoneInfo.Utc, out var utc, out _));
        Assert.NotEqual(ny!.StartUtc, utc!.StartUtc);

        var fixedRange = TimeRangeParser.Parse("Oct 1", Now, NewYork).Spec!;
        Assert.True(fixedRange.TryResolve(Now, NewYork, out var a, out _));
        Assert.True(fixedRange.TryResolve(Now, TimeZoneInfo.Utc, out var b, out _));
        Assert.Equal(a!.StartUtc, b!.StartUtc);
        Assert.Equal(a.EndUtc, b.EndUtc);
    }

    [Fact]
    public void LiveRanges_SlideWithNow()
    {
        var spec = TimeRangeParser.Parse("since 10/1", Now, NewYork).Spec!;
        Assert.True(spec.TryResolve(Now, NewYork, out var first, out _));
        Assert.True(spec.TryResolve(Now.AddHours(3), NewYork, out var later, out _));
        Assert.Equal(first!.StartUtc, later!.StartUtc);
        Assert.Equal(first.EndUtc.AddHours(3), later.EndUtc);
        Assert.True(later.IsLive);
    }

    [Fact]
    public void Label_ForTheIssueExample()
    {
        var result = TimeRangeParser.Parse("week to date", new DateTime(2026, 10, 8, 11, 1, 0), NewYork);
        Assert.Equal("3d  Oct 5, 12:00 am - Oct 8, 7:01 am (UTC-04:00)", result.Echo);
        Assert.Equal("Oct 5, 12:00 am", result.Range!.StartText);
        Assert.Equal("UTC-04:00", result.Range.ZoneText);
    }

    [Fact]
    public void DataStartNote_AppearsOnlyWhenTheRangeStartsWellBeforeTheData()
    {
        Assert.True(TimeRangeSpec.Relative(TimeSpan.FromDays(7)).TryResolve(Now, NewYork, out var week, out _));
        // Data begins Oct 3 2:00 pm EDT = 18:00Z; the week starts Oct 1 7:01 am EDT.
        Assert.Equal("Data starts Oct 3, 2:00 pm", TimeRangeNotes.DataStartNote(week!, new DateTime(2026, 10, 3, 18, 0, 0)));
        // Starting inside the 90 minute slack of the data's start says nothing.
        Assert.Null(TimeRangeNotes.DataStartNote(week!, new DateTime(2026, 10, 1, 12, 0, 0)));
        Assert.Null(TimeRangeNotes.DataStartNote(week!, null));
        Assert.Null(TimeRangeNotes.DataStartNote(week!, new DateTime(2026, 9, 1)));
    }

    [Theory]
    [InlineData(10, 5, "Data here is collected every 5 minutes.")]
    [InlineData(14, 5, "Data here is collected every 5 minutes.")]
    [InlineData(15, 5, null)]
    [InlineData(5, 1, null)]
    [InlineData(5, 2, "Data here is collected every 2 minutes.")]
    [InlineData(120, 60, "Data here is collected every hour.")]
    [InlineData(60, 0, null)]
    public void SampleIntervalNote_FiresUnderThreeSamples(int spanMinutes, int intervalMinutes, string? expected)
    {
        TimeSpan? interval = intervalMinutes == 0 ? null : TimeSpan.FromMinutes(intervalMinutes);
        Assert.Equal(expected, TimeRangeNotes.SampleIntervalNote(TimeSpan.FromMinutes(spanMinutes), interval));
    }

    [Fact]
    public void FormatLength_FloorsAndDropsHoursPastADay()
    {
        Assert.Equal("45m", TimeRangeSpec.FormatLength(TimeSpan.FromMinutes(45)));
        Assert.Equal("1h 30m", TimeRangeSpec.FormatLength(TimeSpan.FromMinutes(90)));
        Assert.Equal("2h", TimeRangeSpec.FormatLength(TimeSpan.FromHours(2)));
        Assert.Equal("1d", TimeRangeSpec.FormatLength(TimeSpan.FromHours(47)));
        Assert.Equal("3d", TimeRangeSpec.FormatLength(new TimeSpan(3, 7, 1, 0)));
    }
}
