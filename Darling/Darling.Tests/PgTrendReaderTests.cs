// Copyright (c) Erik Darling Data. All rights reserved.
// Licensed under the terms in the LICENSE file in the repository root.

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The first PostgreSQL time series (#2663). The service shipped fourteen trend reads and none worked on a
/// PostgreSQL target, so every PostgreSQL answer described one window and none answered "is this getting
/// worse".
/// </summary>
public sealed class PgTrendReaderTests
{
    /// <summary>
    /// A trend differences CONSECUTIVE snapshots. That is the whole difference from the window reads next
    /// door, which want one number and take newest-minus-oldest — taking the window's ends here would
    /// produce a single point and call it a shape.
    /// </summary>
    [Fact]
    public void TheWaitTrendDifferencesConsecutiveSnapshots()
    {
        var sql = DarlingPgTrendReader.WaitTrendSql;

        Assert.Contains("LAG(samples) OVER (ORDER BY collection_time)", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE prev_samples IS NOT NULL", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A counter going backwards is a RESET, not negative work.
    /// <c>pg_wait_sampling_reset_profile()</c> and a server restart both zero the profile, and the second
    /// happens without anyone deciding to. <c>GREATEST(delta, 0)</c> would report a QUIET interval across a
    /// restart — the one reading that is definitely wrong, because the server was not idle. The interval
    /// takes the new value whole, which is everything since the reset.
    /// </summary>
    [Fact]
    public void AWaitCounterGoingBackwardsIsAResetRatherThanClampedToZero()
    {
        var sql = DarlingPgTrendReader.WaitTrendSql;

        Assert.Contains("CASE WHEN samples < prev_samples THEN samples ELSE samples - prev_samples END", sql, StringComparison.Ordinal);
        Assert.Contains("(samples < prev_samples)            AS counter_reset", sql, StringComparison.Ordinal);

        /* The clamp this read must NOT use on the sample counter. It is right for a per-interval difference
           that cannot legitimately go negative and wrong here, where the negative IS the signal. */
        Assert.DoesNotContain("GREATEST(samples", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Per SECOND, because collection intervals are not uniform: a restart or a slow cycle stretches one,
    /// and a per-interval total renders that as a spike in the data rather than in the server.
    /// </summary>
    [Fact]
    public void TheWaitTrendNormalisesByTheIntervalLength()
    {
        var sql = DarlingPgTrendReader.WaitTrendSql;

        Assert.Contains("interval_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("/ interval_seconds", sql, StringComparison.Ordinal);
        Assert.Contains("estimated_wait_ms_per_second", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The query trend reads the <c>delta_*</c> columns the collector already wrote rather than
    /// differencing the cumulative ones again.
    ///
    /// <para>They are not equivalent. The collector's delta spans the interval it actually OBSERVED, while a
    /// <c>LAG</c> here spans the gap in the STORED data — so whenever a snapshot is missing the two give
    /// different answers, and only the collector's is about the server.</para>
    /// </summary>
    [Fact]
    public void TheQueryTrendUsesTheCollectorsDeltas_NotItsOwn()
    {
        var sql = DarlingPgTrendReader.QueryDurationTrendSql;

        Assert.Contains("SUM(delta_calls)", sql, StringComparison.Ordinal);
        Assert.Contains("SUM(delta_total_exec_time_ms)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LAG(total_exec_time_ms)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// A mean over no calls is ABSENT, not fast. Returning 0 would draw the line through the floor of the
    /// chart at exactly the intervals the statement was idle, which reads as the query getting faster.
    /// </summary>
    [Fact]
    public void AnIntervalWithNoCallsHasANullMeanRatherThanZero()
    {
        var sql = DarlingPgTrendReader.QueryDurationTrendSql;

        Assert.Contains("ELSE NULL", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN coalesce(calls, 0) > 0", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The automatic choice must skip the CPU class. <c>pg_wait_sampling</c>'s <c>Running</c> means the
    /// backend was NOT waiting and dominates any healthy server's profile — measured on the rig it grew by
    /// 1,534 samples against 194 for the next event — so defaulting to it answers the opposite of the
    /// question a wait trend asks. It stays askable by name, because CPU time is a real signal; it is just
    /// never the automatic answer.
    /// </summary>
    [Fact]
    public void TheAutomaticWaitChoiceExcludesTheNotWaitingClass()
    {
        var sql = DarlingPgTrendReader.DominantWaitEventSql;

        Assert.Contains("coalesce(event_type, '') <> 'CPU'", sql, StringComparison.Ordinal);

        /* Ranked on the DIFFERENCE across the window. The profile is cumulative, so ranking it raw returns
           whichever event has accumulated longest since startup — a fact about uptime, not about now. */
        Assert.Contains("ORDER BY (hi - lo) DESC", sql, StringComparison.Ordinal);

        /* The CPU exclusion belongs to the CHOICE only: naming the event explicitly must still follow it. */
        Assert.DoesNotContain("'CPU'", DarlingPgTrendReader.WaitTrendSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The automatic query choice ranks on time actually spent in the window, and skips statements that
    /// recorded none — a queryid present in every snapshot with zero calls is not "the busiest statement".
    /// </summary>
    [Fact]
    public void TheAutomaticQueryChoiceRanksOnTimeSpentInTheWindow()
    {
        var sql = DarlingPgTrendReader.TopQueryIdSql;

        Assert.Contains("HAVING SUM(delta_total_exec_time_ms) > 0", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY SUM(delta_total_exec_time_ms) DESC", sql, StringComparison.Ordinal);
    }
}
