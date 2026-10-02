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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4925: the SQL shape of the daily summary's Azure master scope. Pure: no store.
/// </summary>
public sealed class DailySummaryAzureMasterScopeTests
{
    private static string Cte(string sql, string name)
    {
        var start = sql.IndexOf(name + " AS (", StringComparison.Ordinal);
        Assert.True(start >= 0, name);
        var end = sql.IndexOf("GROUP BY 1", start, StringComparison.Ordinal);
        return sql[start..end];
    }

    [Fact]
    public void Scope_ChangesOnlyTheDeadlockAndBlockedReportCtes_AndLeavesTheDmvCteByteIdentical()
    {
        var plain = DailySummarySql.RangeSql;
        var scoped = DailySummaryAzureMasterScope.Scope(plain);

        Assert.NotEqual(plain, scoped);
        Assert.Equal(Cte(plain, "dmv"), Cte(scoped, "dmv"));
        Assert.NotEqual(Cte(plain, "deadlocks"), Cte(scoped, "deadlocks"));
        Assert.NotEqual(Cte(plain, "bpr"), Cte(scoped, "bpr"));

        /* FILTER, never WHERE: the day spine and the signal-presence count must not move. */
        Assert.Contains("COUNT(*) FILTER (WHERE", Cte(scoped, "bpr"), StringComparison.Ordinal);
        Assert.Contains("MAX(wait_time_ms) FILTER (WHERE", Cte(scoped, "bpr"), StringComparison.Ordinal);
        Assert.Contains("COUNT(*) FILTER (WHERE", Cte(scoped, "deadlocks"), StringComparison.Ordinal);
    }

    [Fact]
    public void Scope_EveryTier_StillScopes_AndPlainTextIsUntouched()
    {
        var tiers = new[] { RetentionTier.Raw, RetentionTier.Hourly, RetentionTier.Daily };
        foreach (var tier in tiers)
        {
            var routed = DailySummarySql.RangeSqlFor(tier);
            var scoped = DailySummaryAzureMasterScope.Scope(routed);
            Assert.Contains("unnest($5::text[])", scoped, StringComparison.Ordinal);
            Assert.DoesNotContain("$5", routed, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Scope_ThrowsWhenTheStatementHasDrifted()
    {
        Assert.Throws<InvalidOperationException>(() => DailySummaryAzureMasterScope.Scope("SELECT 1"));
        var noDeadlocks = DailySummarySql.RangeSql.Replace(
            "SELECT date_trunc('day', deadlock_time) AS d, COUNT(*) AS c", "SELECT 1", StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() => DailySummaryAzureMasterScope.Scope(noDeadlocks));
    }

    [Fact]
    public void Predicates_AreThoseOfTheAlertCollector()
    {
        var scoped = DailySummaryAzureMasterScope.Scope(DailySummarySql.RangeSql);
        /* The alert collector's list predicates, restated with the list at $5 (it has it at $4). */
        var keep = "(database_name IS NULL OR NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($5::text[]) x)))";
        Assert.Contains(keep, PgFactCollector.BlockingSqlSkippingSeparate.Replace("$4::text[]", "$5::text[]"), StringComparison.Ordinal);
        Assert.Contains(keep, scoped, StringComparison.Ordinal);

        var collectorOutside = PgFactCollector.DeadlockOutsideCountSql.Replace("\r\n", "\n").Replace("$4::text[]", "$5::text[]");
        Assert.Contains("database_name IS NOT NULL\nAND   lower(database_name) <> 'master'\nAND   NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($5::text[]) x))", collectorOutside, StringComparison.Ordinal);
        Assert.Contains("database_name IS NOT NULL AND lower(database_name) <> 'master' AND NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($5::text[]) x))", scoped, StringComparison.Ordinal);
    }

    [Fact]
    public void CacheScopeKey_NeverCollides_AndIgnoresOrderAndCase()
    {
        Assert.Equal("", DailySummaryAzureMasterScope.CacheScopeKey(null));
        Assert.Equal("", DailySummaryAzureMasterScope.CacheScopeKey(Array.Empty<string>()));
        Assert.Equal(DailySummaryAzureMasterScope.CacheScopeKey(new[] { "b", "A" }), DailySummaryAzureMasterScope.CacheScopeKey(new[] { "a", "B" }));
        Assert.NotEqual(DailySummaryAzureMasterScope.CacheScopeKey(new[] { "a", "b" }), DailySummaryAzureMasterScope.CacheScopeKey(new[] { "ab" }));
        Assert.NotEqual(DailySummaryAzureMasterScope.CacheScopeKey(new[] { "a" }), DailySummaryAzureMasterScope.CacheScopeKey(null));
    }

    private sealed record Row(DateTime Day, int Value);

    [Fact]
    public async Task TheCache_KeepsScopesApart_AndAThrowingReadStoresNothing()
    {
        var now = new DateTime(2026, 9, 25, 12, 0, 0, DateTimeKind.Utc);
        var cache = new DailySummaryRangeCache<Row>(() => now);
        var from = new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 9, 26, 0, 0, 0, DateTimeKind.Utc);
        var calls = 0;
        Task<List<Row>> Run(int value) => Task.FromResult(new List<Row> { new(from, value) });

        var a = await cache.GetRangeAsync("s", 1, from, to, "sql", true, r => r.Day, (_, _, _) => { calls++; return Run(1); }, "", CancellationToken.None);
        var b = await cache.GetRangeAsync("s", 1, from, to, "sql", true, r => r.Day, (_, _, _) => { calls++; return Run(2); }, "x", CancellationToken.None);
        Assert.Equal(1, a[0].Value);
        Assert.Equal(2, b[0].Value);
        Assert.Equal(2, calls);

        var again = await cache.GetRangeAsync("s", 1, from, to, "sql", true, r => r.Day, (_, _, _) => { calls++; return Run(9); }, "x", CancellationToken.None);
        Assert.Equal(2, again[0].Value);   // the closed days come from the "x" block; only the open day re-ran
        Assert.Equal(3, calls);

        await Assert.ThrowsAsync<InvalidOperationException>(() => cache.GetRangeAsync(
            "s", 1, from, to, "sql", true, r => r.Day, (_, _, _) => throw new InvalidOperationException("scoped"), "y", CancellationToken.None));
        var after = await cache.GetRangeAsync("s", 1, from, to, "sql", true, r => r.Day, (_, _, _) => { calls++; return Run(5); }, "y", CancellationToken.None);
        Assert.Equal(5, after[0].Value);   // nothing was stored by the throwing read
    }
}
