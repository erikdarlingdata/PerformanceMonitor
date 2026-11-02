/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7156: the main query now returns every statement that ran in the window (up to 2000), so the deferred plan fetch
/// is the one place that still bounds plan work, at <see cref="QueryStatsCollector.MaxPlansPerRun"/>. These drive the
/// hit-or-miss split the fetch uses, with no target.
/// </summary>
public sealed class QueryStatsDeferredPlanCapTests
{
    private static readonly DateTime Now = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime Created = new(2026, 11, 2, 11, 0, 0, DateTimeKind.Utc);

    private static QueryStatsCollector.Row RowFor(int n) => new()
    {
        PlanHandle = "0x06" + n.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
        DatabaseName = "app",
        CreationTime = Created,
        PlanGenerationNum = 1,
        StatementStartOffset = 0,
        StatementEndOffset = -1,
    };

    [Fact]
    public void MoreRowsThanTheCap_AllMissingTheCache_FetchOnlyTheFirstCapInInputOrder()
    {
        var cap = QueryStatsCollector.MaxPlansPerRun;
        Assert.Equal(200, cap);

        var rows = Enumerable.Range(0, cap + 1).Select(RowFor).ToList();
        var cache = new PlanDigestCache<QueryStatsPlanKey>();

        var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsPlans(1, rows, cache, Now);

        Assert.Equal(cap, misses.Count);
        Assert.Equal(rows.Take(cap).Select(r => r.PlanHandle), misses.Select(m => m.Row.PlanHandle));
        Assert.DoesNotContain(misses, m => ReferenceEquals(m.Row, rows[cap]));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void CachedRows_TakeTheirDigestAndDoNotSpendTheCap()
    {
        var cap = QueryStatsCollector.MaxPlansPerRun;
        var rows = Enumerable.Range(0, cap + 3).Select(RowFor).ToList();
        var cache = new PlanDigestCache<QueryStatsPlanKey>();

        /* Rows 0..2 were committed on an earlier run. They are hits, so the cap is spent on rows 3..202. */
        var cachedKeys = rows.Take(3).Select(r => QueryStatsPlanKey.TryCreate(1, r)!.Value).ToList();
        foreach (var key in cachedKeys)
        {
            cache.AddPending(key, "digest", 1234, Now);
        }

        cache.ConfirmPending(cachedKeys, Now);

        var misses = DarlingCollectorRunner.PartitionDeferredQueryStatsPlans(1, rows, cache, Now);

        Assert.Equal(cap, misses.Count);
        Assert.Equal(rows.Skip(3).Take(cap).Select(r => r.PlanHandle), misses.Select(m => m.Row.PlanHandle));
        Assert.All(rows.Take(3), r =>
        {
            Assert.Equal("digest", r.KnownPlanDigest);
            Assert.Equal(1234, r.QueryPlanXmlBytes);
        });
    }
}
