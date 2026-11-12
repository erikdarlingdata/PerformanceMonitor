/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The job-history statement moved from the viewer into Darling.Storage. These pins hold the end-instant clauses to the filter
/// that adds them. The saved copies of the viewer's no-filter statement are gone: #8549 replaced the whole-table <c>svr</c> step with a
/// per-server descent, so the saved text no longer matched by design, and <c>JobHistoryJobStatsSeekLiveTests</c> and
/// <c>JobHistoryLatestOffsetLiveTests</c> hold the rows to the old statement's.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class JobHistorySqlTextPinTests
{
    private static string Normalize(string sql) => Regex.Replace(sql, @"\s+", " ").Trim();

    [Fact]
    public void AnEmptyFilter_ChangesNothing() =>
        Assert.Equal(DarlingJobHistoryReader.BuildJobHistorySql(false, null), DarlingJobHistoryReader.BuildJobHistorySql(false, new JobHistoryFilter()));

    [Theory]
    [InlineData(false, 4)]
    [InlineData(true, 5)]
    public void UntilUtc_BoundsTheNewestRunsAndTheStatsExactly(bool scoped, int firstParam)
    {
        var sql = DarlingJobHistoryReader.BuildJobHistorySql(scoped, new JobHistoryFilter(JobName: "x", UntilUtc: new System.DateTime(2026, 3, 11)));
        var untilParam = "$" + (firstParam + 1).ToString(System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(2, Regex.Matches(sql, Regex.Escape($"jh.run_datetime <= {untilParam} + make_interval(mins => so.offset_minutes)")).Count);
        Assert.DoesNotContain("+ interval '1 hour'", sql);
        Assert.Equal(2, Regex.Matches(sql, Regex.Escape(untilParam + " + make_interval")).Count);
    }
}
