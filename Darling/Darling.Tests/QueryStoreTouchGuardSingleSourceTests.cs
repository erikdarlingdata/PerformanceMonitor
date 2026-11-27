/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Reflection;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The width of the Query Store liveness touch guard (#3189) is a stated share of the margin the prune cutoffs
/// reserve for a trailing stamp, and it follows that margin. <see cref="QueryStoreLivenessTouchGuard.GuardInterval"/>
/// is the one width every guarded <c>last_seen</c> touch carries, in <see cref="QueryStorePlanMap.TouchAndProbeSql"/>
/// and in <see cref="QueryStoreTextStore.TouchSeenSql"/>; the text row's own restamp
/// (<see cref="QueryStoreTextStore.RefreshTextRowSql"/>) is not a guarded touch and runs at
/// <see cref="QueryStoreTextSeen.TextRowRefreshInterval"/>.
///
/// <para>The class checks relations among the guard's constants rather than a number. A pin of the form "the
/// width is six hours" passes as soon as its own expected value is updated in the same commit, so the
/// assertions tie the width to the margin it is spent out of: the guard stays inside the margin, the margin is
/// the tightest prune margin among the guarded stores, the width times the divisor is the margin, and the
/// interval is rendered from the width.</para>
/// </summary>
public sealed class QueryStoreTouchGuardSingleSourceTests
{
    /// <summary>
    /// The width is a stated share of the margin the prune cutoffs reserve for a trailing stamp: the guard
    /// stays inside the margin, the margin is the tightest prune margin among the guarded stores, the width
    /// times the divisor is the margin in hours, and the interval is rendered from the width.
    /// </summary>
    [Fact]
    public void TheWidthIsAShareOfTheStampSkewMarginAndFollowsIt()
    {
        Assert.True(
            QueryStoreLivenessTouchGuard.GuardStaysInsideStampSkewMargin(),
            $"the guard is {QueryStoreLivenessTouchGuard.GuardHours}h taking "
            + $"1/{QueryStoreLivenessTouchGuard.MarginShareDivisor} of a "
            + $"{QueryStoreLivenessTouchGuard.StampSkewMarginHours}h margin — it must be at least an hour, "
            + "and the divisor must divide the margin exactly, or the share taken is not the share stated.");

        /* Sized on the TIGHTEST margin among the guarded tables, so one shared width is safe on all of
           them. */
        Assert.Equal(
            QueryStoreLivenessTouchGuard.StampSkewMarginDays,
            Math.Min(QueryStorePlanMap.PruneMarginDays, QueryStoreTextStore.PruneMarginDays));

        Assert.Equal(
            QueryStoreLivenessTouchGuard.GuardHours * QueryStoreLivenessTouchGuard.MarginShareDivisor,
            QueryStoreLivenessTouchGuard.StampSkewMarginDays * 24);

        /* The interval is rendered from the hours rather than typed. */
        Assert.Equal(
            $"interval '{QueryStoreLivenessTouchGuard.GuardHours} hours'",
            QueryStoreLivenessTouchGuard.GuardInterval);
    }
}
