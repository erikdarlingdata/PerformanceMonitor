/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The interval at which the fetch probe restamps <c>collect.query_store_text.last_seen</c> (#9043) follows the
/// arithmetic of the prune that reads that column: an older build's, after a rollback. The liveness stamp itself
/// lives in <c>collect.query_store_text_seen</c>, so the text row's stamp only has to stay fresh enough that an
/// older build, which prunes on it, does not retire the text of a statement that is still in use.
///
/// <para>The derivation is pinned, not the number: the smallest fact retention is asked of the retention resolver
/// the purge runs through, the cutoff is that plus the text table's own margin, and the interval is a stated share
/// of it. Moving any of the three moves the interval or reds here.</para>
/// </summary>
public sealed class QueryStoreTextRowRefreshTests
{
    /// <summary>
    /// The smallest horizon the purge can apply to the facts that reference Query Store text is what the resolver
    /// answers when every referencing collector is set as low as an override can be asked for, and below.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(0)]
    [InlineData(-5)]
    public void TheSmallestFactRetention_IsWhatTheResolverAnswersAtTheLowestSetting(int configuredDays)
    {
        Assert.NotEmpty(QueryStoreTextStore.ReferencingCollectors);

        Assert.Equal(
            QueryStoreTextSeen.SmallestFactRetentionDays,
            DarlingRetention.WidestEffectiveRetentionDays(QueryStoreTextStore.ReferencingCollectors, _ => configuredDays));
    }

    /// <summary>
    /// The older build's cutoff at that retention is the retention plus the text table's prune margin, the same sum
    /// the retention pass computes, and the refresh interval is the stated share of it.
    /// </summary>
    [Fact]
    public void TheRefreshInterval_IsAShareOfTheOlderBuildsSmallestCutoff()
    {
        Assert.Equal(
            (QueryStoreTextSeen.SmallestFactRetentionDays + QueryStoreTextStore.PruneMarginDays) * 24,
            QueryStoreTextSeen.OlderBuildCutoffHours);

        Assert.Equal(
            QueryStoreTextSeen.OlderBuildCutoffHours,
            QueryStoreTextSeen.TextRowRefreshHours * QueryStoreTextSeen.TextRowRefreshShareDivisor);

        Assert.Equal(
            $"interval '{QueryStoreTextSeen.TextRowRefreshHours} hours'",
            QueryStoreTextSeen.TextRowRefreshInterval);

        Assert.True(
            QueryStoreTextSeen.TextRowRefreshStaysInsideOlderBuildCutoff(),
            $"the text row is restamped once it is {QueryStoreTextSeen.TextRowRefreshHours}h behind, "
            + $"1/{QueryStoreTextSeen.TextRowRefreshShareDivisor} of the {QueryStoreTextSeen.OlderBuildCutoffHours}h an older build keeps "
            + "a text at the smallest retention: the divisor must divide that exactly, and the interval must be no shorter than the "
            + "touch guard and leave a guard window on either side of it inside the cutoff.");
    }

    /// <summary>
    /// What the interval buys and what it must not cost. A live statement's text row trails by at most the interval
    /// plus one guard window (the probe asks about a statement at most once per window), and that has to sit inside
    /// the older build's cutoff with a whole guard window to spare. The interval is also longer than the guard, or
    /// the text row is back on the touch's cadence and nothing was saved.
    /// </summary>
    [Fact]
    public void ALiveTextRowsStamp_StaysInsideTheOlderBuildsCutoff_AndIsWrittenLessOftenThanTheGuard()
    {
        var worstStalenessHours = QueryStoreTextSeen.TextRowRefreshHours + QueryStoreLivenessTouchGuard.GuardHours;

        Assert.True(worstStalenessHours + QueryStoreLivenessTouchGuard.GuardHours <= QueryStoreTextSeen.OlderBuildCutoffHours);
        Assert.True(QueryStoreTextSeen.TextRowRefreshHours > QueryStoreLivenessTouchGuard.GuardHours);
    }
}
