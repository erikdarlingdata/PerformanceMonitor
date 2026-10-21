/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The Overview tab's month calendar (#6294), run under Node (<c>web-daily-calendar-harness.mjs</c>) on a fake DOM and a scripted read:
/// the month layout (first-weekday offset, month lengths, leap years), the read each month takes, and the drawn cells, month buttons,
/// day detail, drills and table toggle. Node is skipped when it is not installed.
/// </summary>
public sealed class WebDailyCalendarBehaviourTests
{
    private static JsonElement Run(string scenario)
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-daily-calendar-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));
        psi.ArgumentList.Add(scenario);
        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped page script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the daily calendar harness did not finish in 20 s for scenario " + scenario);
            }

            Assert.True(proc.ExitCode == 0, "the daily calendar harness failed for scenario " + scenario + ": " + error.Result);
            using var doc = JsonDocument.Parse(output);
            return doc.RootElement.Clone();
        }
    }

    private static (int Offset, int Length, int Weeks) Shape(JsonElement e) =>
        (e.GetProperty("offset").GetInt32(), e.GetProperty("length").GetInt32(), e.GetProperty("weeks").GetInt32());

    /// <summary>
    /// The first weekday offset follows the week's first day, the month length follows the calendar (a leap February has 29, 2100 has 28),
    /// and only the weeks a month needs are drawn: a 28-day February that starts on the week's first day is exactly four weeks.
    /// </summary>
    [Fact]
    public void TheMonthLayout_OffsetsTheFirstWeekday_KnowsLeapYears_AndDrawsOnlyTheWeeksItNeeds()
    {
        var r = Run("layout");
        Assert.Equal((4, 29, 5), Shape(r.GetProperty("feb2024Sun")));  // 1 Feb 2024 is a Thursday
        Assert.Equal((3, 29, 5), Shape(r.GetProperty("feb2024Mon")));
        Assert.Equal((0, 28, 4), Shape(r.GetProperty("feb2026Sun")));  // 1 Feb 2026 is a Sunday
        Assert.Equal((6, 28, 5), Shape(r.GetProperty("feb2026Mon")));
        Assert.Equal((4, 31, 5), Shape(r.GetProperty("oct2026Sun")));
        Assert.Equal((1, 31, 5), Shape(r.GetProperty("mar2026Sat")));
        Assert.Equal(new[] { 29, 28, 29, 28, 30, 31 }, r.GetProperty("lengths").EnumerateArray().Select(e => e.GetInt32()).ToArray());
        Assert.True(r.GetProperty("everyWeekHasSeven").GetBoolean());
        Assert.Equal("2026-03-05", r.GetProperty("iso").GetString());

        var shifts = r.GetProperty("shifts").EnumerateArray().Select(e => (e.GetProperty("year").GetInt32(), e.GetProperty("month0").GetInt32())).ToArray();
        Assert.Equal(new[] { (2025, 11), (2027, 0), (2024, 11) }, shifts);
    }

    /// <summary>
    /// A past month is read with its own length and an anchor on its last second (a midnight anchor would give the last day a zero-length
    /// window); the current month names no anchor; a drill's range is the whole UTC day, or the day so far for today.
    /// </summary>
    [Fact]
    public void TheReadsAMonthTakes_AreItsOwnLength_AnchoredOnItsLastSecond_AndTheCurrentMonthEndsNow()
    {
        var r = Run("params");
        Assert.False(r.GetProperty("current").TryGetProperty("as_of", out _));
        Assert.Equal(31, r.GetProperty("current").GetProperty("days_back").GetInt32());
        Assert.False(r.GetProperty("future").TryGetProperty("as_of", out _));
        Assert.Equal(30, r.GetProperty("sept").GetProperty("days_back").GetInt32());
        Assert.Equal("2026-09-30T23:59:59Z", r.GetProperty("sept").GetProperty("as_of").GetString());
        Assert.Equal(29, r.GetProperty("leapFeb").GetProperty("days_back").GetInt32());
        Assert.Equal("2024-02-29T23:59:59Z", r.GetProperty("leapFeb").GetProperty("as_of").GetString());
        Assert.Equal("2025-12-31T23:59:59Z", r.GetProperty("dec").GetProperty("as_of").GetString());

        var past = r.GetProperty("dayRangePast");
        Assert.Equal(TimeSpan.FromDays(1).TotalMilliseconds, past.GetProperty("endMs").GetDouble() - past.GetProperty("startMs").GetDouble());
        var today = r.GetProperty("dayRangeToday");
        Assert.Equal(TimeSpan.FromHours(12).TotalMilliseconds, today.GetProperty("endMs").GetDouble() - today.GetProperty("startMs").GetDouble());
    }

    /// <summary>
    /// Each day is drawn in its read band, a day with no row (or a purged one) is "no data" and never Healthy, the month buttons read the
    /// month they step to, a day's detail lists the service's reasons and offers its drills, and "Show as table" holds only the shown month's days.
    /// </summary>
    [Fact]
    public void ADayAfterToday_ReadsAsNotYetHappened_NotACollectionGap_AndTodayIsTheServersUtcDay()
    {
        // #6353: the browser clock is already on the 22nd (UTC) while the read's to_date, the calendar's today, is the 21st.
        var r = Run("future");

        foreach (var name in new[] { "tomorrow", "lastDay" })
        {
            var day = r.GetProperty(name);
            Assert.Equal("Future", day.GetProperty("band").GetString());
            Assert.Contains("not yet", day.GetProperty("word").GetString());
            Assert.DoesNotContain("no data", day.GetProperty("word").GetString());
            Assert.DoesNotContain("No collection", day.GetProperty("tip").GetString());
        }
        Assert.Equal("NoData", r.GetProperty("today").GetProperty("band").GetString());
        Assert.Contains("No collection recorded", r.GetProperty("today").GetProperty("tip").GetString());
        Assert.Equal("NoData", r.GetProperty("yesterday").GetProperty("band").GetString());

        var detail = r.GetProperty("futureDetail");
        Assert.Contains("This day has not happened yet.", detail.GetProperty("text").GetString());
        Assert.DoesNotContain("No collection this day", detail.GetProperty("text").GetString());
        Assert.DoesNotContain("No Data", detail.GetProperty("text").GetString());
        Assert.False(detail.GetProperty("readAsked").GetBoolean());
        Assert.True(r.GetProperty("todayDetail").GetProperty("readAsked").GetBoolean());
        Assert.Contains("No collection this day.", r.GetProperty("todayDetail").GetProperty("text").GetString());
    }

    [Fact]
    public void TheCalendar_DrawsBands_ShowsNoRowAsNoData_StepsMonths_AndOpensADaysDetail()
    {
        var r = Run("calendar");
        Assert.Equal(31, r.GetProperty("octDays").GetInt32());
        var oct = r.GetProperty("oct");
        Assert.Equal("Critical|3/Critical", oct.GetProperty("d3").GetString());
        Assert.Equal("Healthy|4/Healthy", oct.GetProperty("d4").GetString());
        Assert.Equal("NoData|5/no data", oct.GetProperty("d5").GetString());      // a purged day carries band NoData
        Assert.Equal("NoData|1/no data", oct.GetProperty("d1").GetString());      // no row at all
        Assert.Equal("NoData|21/no data", oct.GetProperty("d21").GetString());
        Assert.Equal("Future|31/not yet", oct.GetProperty("d31").GetString());   // #6353: after today is not a gap
        Assert.False(r.GetProperty("septDayInOctoberGrid").GetBoolean());
        Assert.Equal("October 2026 (UTC)", r.GetProperty("title").GetString());
        Assert.True(r.GetProperty("nextDisabledNow").GetBoolean());
        Assert.Equal(0, r.GetProperty("readsBeforeNavigation").GetInt32());      // the current month came from the Overview's own fetch

        var detail = r.GetProperty("detail");
        Assert.Equal(new[] { "2 deadlocks", "1 alert" }, detail.GetProperty("reasons").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "View Deadlocks", "View Top Queries" }, detail.GetProperty("drills").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal("get_daily_summary", detail.GetProperty("read").GetProperty("tool").GetString());
        Assert.Equal("2026-10-03", detail.GetProperty("read").GetProperty("params").GetProperty("summary_date").GetString());
        Assert.Equal(
            new[] { "SRV1|2026-10-03|Deadlocks", "SRV1|2026-10-03|TopQueries" },
            r.GetProperty("opened").EnumerateArray().Select(e => string.Join("|", e.EnumerateArray().Select(x => x.GetString()))).ToArray());
        Assert.Equal(new[] { "refused" }, r.GetProperty("refusal").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.True(r.GetProperty("noDataDetail").GetProperty("text").GetBoolean());
        Assert.Equal(0, r.GetProperty("noDataDetail").GetProperty("healthy").GetInt32());

        var sept = r.GetProperty("sept");
        Assert.Equal(30, sept.GetProperty("days").GetInt32());
        Assert.Equal("Warning|30/Warning", sept.GetProperty("d30").GetString());
        Assert.Equal("NoData|29/no data", sept.GetProperty("d29").GetString());
        Assert.Equal("September 2026 (UTC)", sept.GetProperty("title").GetString());
        var septRead = r.GetProperty("septRead").GetProperty("params");
        Assert.Equal(30, septRead.GetProperty("days_back").GetInt32());
        Assert.Equal("2026-09-30T23:59:59Z", septRead.GetProperty("as_of").GetString());
        Assert.True(r.GetProperty("nextEnabledInSept").GetBoolean());
        Assert.True(r.GetProperty("thisMonthButton").GetBoolean());

        Assert.Equal(new[] { "2026-09-30" }, r.GetProperty("tableSept").GetProperty("days").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Empty(r.GetProperty("tableSept").GetProperty("missing").EnumerateArray());
        Assert.Equal(new[] { "2026-10-03", "2026-10-04", "2026-10-05" }, r.GetProperty("tableOct").GetProperty("days").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "2026-10-04" }, r.GetProperty("tableOct").GetProperty("missing").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(3, r.GetProperty("totalReads").GetInt32());                  // the day detail twice and September once; October was never re-read
    }
}
