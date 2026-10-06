/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4765: a Query Store interval's rate is its totals over ITS OWN length, <c>interval_end_time_utc</c>
/// minus the point's start, and only a row that stored no end (collected before the column existed) keeps
/// the seconds since the previous stored point. Query Store stores no row for an interval with no
/// executions, so that gap is the interval's length plus every quiet interval before it, and dividing by it
/// reads a busy interval low.
///
/// <para>Four SQL texts carry the rate: the viewer's raw read and its table-routed twin, the MCP reader's
/// raw read, and the raw class inside the rollup route's builder (which the viewer and the MCP reader
/// share). They are separate strings on purpose (each is read by a different app or route), so the same
/// expression is pinned in every one of them here, and one copy cannot go back to the gap, or spell the
/// fallback differently, without turning this class red. The arithmetic on a real store is
/// <see cref="QueryStoreTrendRoutingLiveTests"/>; Lite's DuckDB twin is pinned by running it, in
/// <c>Lite.Tests</c>.</para>
///
/// <para>Every copy truncates the start and the end each to a whole second before it subtracts (the
/// <c>date_trunc('second', ...)</c> in the pinned expression). Query Store intervals are whole minutes, so nothing
/// is lost; what a stored fraction of a second does to the length is pinned on a real store by
/// <c>QueryStoreTrendRoutingLiveTests.DurationTrend_AFractionalSecondLength_IsRatedOverWholeSeconds</c>.</para>
/// </summary>
public sealed class QueryStoreTrendIntervalRateTests
{
    /// <summary>The interval's own length: its (largest) stored end, less the point's start. NULL exactly
    /// when no row of the point stored an end.</summary>
    private const string OwnLength =
        "extract(epoch FROM (date_trunc('second', MAX(interval_end_time_utc)) - date_trunc('second', point_time)))";

    /// <summary>The seconds since the previous stored point, which is all a row without an end can use.</summary>
    private const string PreviousPointGap =
        "extract(epoch FROM (date_trunc('second', point_time) - date_trunc('second', LAG(point_time) OVER (ORDER BY point_time))))";

    /// <summary>Drops every whitespace character, so a pin does not care how a copy is indented or wrapped.</summary>
    private static string Squash(string sql) => Regex.Replace(sql, @"\s+", "");

    private static IEnumerable<(string Name, string Sql)> SingleStageCopies()
    {
        yield return ("ViewerDataService.QueryStoreDurationTrendSql", ViewerDataService.QueryStoreDurationTrendSql);
        yield return ("ViewerDataService.QueryStoreDurationTrendTableSql", ViewerDataService.QueryStoreDurationTrendTableSql);
        yield return ("DarlingTrendReader.QueryStoreDurationTrendFilteredSql", DarlingTrendReader.QueryStoreDurationTrendFilteredSql);
    }

    private static IEnumerable<(string Name, string Sql)> AllCopies()
    {
        foreach (var copy in SingleStageCopies())
        {
            yield return copy;
        }

        yield return ("QueryStoreTrendRouting.BuildRollupTrendSql(false)", QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: false));
        yield return ("QueryStoreTrendRouting.BuildRollupTrendSql(true)", QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: true));
    }

    /// <summary>
    /// Every copy states the interval's own length as the same expression and falls back to the previous-point
    /// gap through the same COALESCE, and that gap is the only <c>LAG</c> left in the text.
    /// </summary>
    [Fact]
    public void EveryCopy_RatesOverTheIntervalsOwnLength_AndFallsBackToThePreviousPointGap_OnlyWhereTheEndIsNull()
    {
        var ownLength = Squash(OwnLength);
        var previousPointGap = Squash(PreviousPointGap);

        foreach (var (name, rawSql) in AllCopies())
        {
            var sql = Squash(rawSql);

            Assert.True(
                CountOf(sql, ownLength) == 1,
                $"{name}: expected the interval's own length ({OwnLength}) exactly once.");

            Assert.True(
                CountOf(sql, previousPointGap) == 1,
                $"{name}: expected the previous-point gap ({PreviousPointGap}) exactly once, as the fallback.");

            Assert.True(
                CountOf(sql, "LAG(") == 1,
                $"{name}: the fallback must be the only LAG in the text.");

            /* The own length comes FIRST: COALESCE returns it whenever an end was stored, and reaches the gap
               only when it is NULL. The single-stage copies write both in one COALESCE; the routing builder
               computes the own length in its raw_points step and COALESCEs the carried column with the gap in
               its rated step, because that gap must still span the rollup/raw seam. */
            var coalesce = new Regex(
                @"COALESCE\((?:" + Regex.Escape(ownLength) + @"|own_interval_seconds)," + Regex.Escape(previousPointGap) + @"\)");
            Assert.True(coalesce.IsMatch(sql), $"{name}: the previous-point gap must be COALESCEd behind the interval's own length.");
        }
    }

    /// <summary>
    /// The three single-stage copies write the entire <c>interval_seconds</c> column identically, so one
    /// cannot be edited without the other two.
    /// </summary>
    [Fact]
    public void TheThreeSingleStageCopies_WriteTheIntervalSecondsColumnIdentically()
    {
        var expected = Squash("COALESCE(" + OwnLength + ", " + PreviousPointGap + ") AS interval_seconds");

        foreach (var (name, rawSql) in SingleStageCopies())
        {
            /* Anchored on the first argument, not on COALESCE( alone: the viewer's raw text has a comment
               that quotes COALESCE(interval_start_time_utc, collection_time). */
            var column = Regex.Match(Squash(rawSql), @"COALESCE\(extract\(epoch.*?\)ASinterval_seconds").Value;
            Assert.True(column == expected, $"{name}: the interval_seconds column drifted from the shared expression.");
        }
    }

    /// <summary>
    /// The end is read from the same arm that places the point at its start, and the legacy arm, which places
    /// a row at its collection time (there is no interval start for it), states NULL for the end. An end read
    /// off a row placed at its collection time would be measured from the wrong instant.
    /// </summary>
    [Fact]
    public void EveryCopy_CarriesTheEndOnTheArmThatPlacesAtTheStart_AndStatesNullOnTheLegacyArm()
    {
        var armOne = Squash("interval_start_time_utc AS point_time, interval_end_time_utc,");
        var legacyArm = Squash("collection_time AS point_time, CAST(NULL AS timestamp) AS interval_end_time_utc,");

        foreach (var (name, rawSql) in AllCopies())
        {
            var sql = Squash(rawSql);

            Assert.True(
                CountOf(sql, armOne) == 1,
                $"{name}: arm 1 must select the interval's end right beside the start it places the point at.");

            Assert.True(
                CountOf(sql, legacyArm) == 1,
                $"{name}: the legacy arm must state a NULL end, so its rows keep the previous-point gap.");
        }
    }

    /// <summary>
    /// The rollup route's rollup class is still rated over the fixed bucket width (#3653) and never over an
    /// own length: the end belongs to the raw class only, and the rollup arm of the union carries a NULL there.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RollupRoute_KeepsRollupPointsOnTheBucketWidth_AndCarriesTheOwnLengthOnlyOnRawPoints(bool withDatabaseFilter)
    {
        var sql = Squash(QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter));

        Assert.Contains(
            Squash("NULL AS own_interval_seconds, TRUE AS from_rollup FROM rollup_points"),
            sql, StringComparison.Ordinal);
        Assert.Contains(
            Squash("own_interval_seconds, FALSE AS from_rollup FROM raw_points"),
            sql, StringComparison.Ordinal);
        Assert.Contains(
            Squash("CASE WHEN from_rollup THEN " + DurationTrendRouting.HourlyBucketSecondsSql + " ELSE COALESCE(own_interval_seconds,"),
            sql, StringComparison.Ordinal);
        Assert.Contains(Squash(OwnLength + " AS own_interval_seconds"), sql, StringComparison.Ordinal);
    }

    private static int CountOf(string haystack, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = haystack.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }
}
