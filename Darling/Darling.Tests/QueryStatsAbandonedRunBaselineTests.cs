/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7419: query_stats' read advances every candidate's delta baselines, so a run that ends before its rows are stored (the
/// item budget fires in the text or plan fetch, the store is unreachable, a write fails before it starts) must hand them
/// back. These drive the real read twice or three times on one calculator and read the deltas the next run would store.
/// </summary>
public sealed class QueryStatsAbandonedRunBaselineTests
{
    private static readonly DateTime T0 = new(2026, 11, 2, 12, 0, 0, DateTimeKind.Utc);

    private static CollectorContext Context(CollectorDeltaCalculator deltas, DateTime at) => new()
    {
        ServerId = 7,
        ServerName = "test-server",
        CollectionTime = at,
        Deltas = deltas,
        Target = new CollectorTargetInfo(),
        DeferTextFetch = true,
    };

    /// <summary>One result set the way the candidate query returns it: a row per (handle, counter) pair.</summary>
    internal static async Task<List<QueryStatsCollector.Row>> ReadAsync(
        CollectorDeltaCalculator deltas, DateTime at, params (int Handle, long Work)[] statements)
    {
        using var table = Table(at, statements);
        await using var reader = table.CreateDataReader();
        return await QueryStatsCollector.Instance.ReadAsync(reader, Context(deltas, at), TestContext.Current.CancellationToken);
    }

    /// <summary>The candidate query's result, a row per statement, as a table a data reader can be made from.</summary>
    internal static DataTable Table(DateTime at, params (int Handle, long Work)[] statements)
    {
        var table = new DataTable();
        var types = new Type[44];
        for (var i = 0; i < types.Length; i++)
        {
            types[i] = typeof(long);
        }

        foreach (var i in new[] { 0, 1, 2, 36, 37, 38, 42 })
        {
            types[i] = typeof(string);
        }

        types[3] = typeof(DateTime);
        types[4] = typeof(DateTime);
        types[40] = typeof(int);
        types[41] = typeof(int);
        types[43] = typeof(int);
        for (var i = 0; i < types.Length; i++)
        {
            table.Columns.Add("c" + i.ToString(CultureInfo.InvariantCulture), types[i]);
        }

        foreach (var (handle, work) in statements)
        {
            var values = new object?[44];
            for (var i = 0; i < values.Length; i++)
            {
                values[i] = types[i] == typeof(long) ? 0L : DBNull.Value;
            }

            values[0] = "app";
            values[1] = "0x" + handle.ToString("X8", CultureInfo.InvariantCulture);
            values[3] = T0.AddHours(-1);
            values[4] = at;
            values[5] = work;      /* executions */
            values[6] = work * 10; /* worker time */
            values[7] = work * 20; /* elapsed time */
            values[8] = work * 5;  /* logical reads */
            values[36] = "0x03" + handle.ToString("X8", CultureInfo.InvariantCulture);
            values[37] = "0x06" + handle.ToString("X8", CultureInfo.InvariantCulture);
            values[40] = 0;
            values[41] = -1;
            values[43] = 3600;
            table.Rows.Add(values);
        }

        return table;
    }

    /// <summary>
    /// The data-loss case. Run one sets the baselines. Run two is abandoned (its budget fires in the text fetch), so nothing is
    /// stored. Run three must store a delta that covers both intervals, over the real span since run one.
    /// </summary>
    [Fact]
    public async Task AnAbandonedRun_LeavesTheNextRunsDeltaSpanningBothIntervals()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(deltas, T0, (1, 100));

        var abandoned = await ReadAsync(deltas, T0.AddMinutes(1), (1, 130));
        Assert.Equal(30, abandoned.Single().Precomputed!.Exec);
        ((IBaselineRollback)abandoned).RollBackBaselines();

        var next = (await ReadAsync(deltas, T0.AddMinutes(2), (1, 160))).Single().Precomputed!;
        Assert.Equal(60, next.Exec);
        Assert.Equal(1200, next.Elapsed);
        Assert.Equal(120, next.ExecInterval);
    }

    /// <summary>The same three runs with no rollback are what the abandon arm did before: the lost interval is the loss.</summary>
    [Fact]
    public async Task WithoutTheRollback_TheAbandonedIntervalIsLost()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(deltas, T0, (1, 100));
        await ReadAsync(deltas, T0.AddMinutes(1), (1, 130));

        var next = (await ReadAsync(deltas, T0.AddMinutes(2), (1, 160))).Single().Precomputed!;
        Assert.Equal(30, next.Exec);
    }

    /// <summary>A statement first seen in the abandoned run goes back to unseen, so it is a first sighting again, not a delta.</summary>
    [Fact]
    public async Task AStatementNewInTheAbandonedRun_IsBackToUnseen()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(deltas, T0, (1, 100));
        var abandoned = await ReadAsync(deltas, T0.AddMinutes(1), (1, 130), (2, 50));
        ((IBaselineRollback)abandoned).RollBackBaselines();

        var next = await ReadAsync(deltas, T0.AddMinutes(2), (1, 160), (2, 90));
        Assert.Equal(60, next.Single(r => r.PlanHandle!.EndsWith("00000001", StringComparison.Ordinal)).Precomputed!.Exec);
        Assert.Equal(0, next.Single(r => r.PlanHandle!.EndsWith("00000002", StringComparison.Ordinal)).Precomputed!.ExecInterval);
    }

    /// <summary>A run that reaches storage keeps every baseline it advanced, the rows the ranking cap dropped included.</summary>
    [Fact]
    public async Task ARunThatReachesStorage_KeepsTheBaselinesOfRowsTheCapDropped()
    {
        var deltas = new CollectorDeltaCalculator();
        var count = QueryStatsCollector.RowsPerRankingPerRun + 50;
        var first = Enumerable.Range(1, count).Select(n => (n, 1000L + n)).ToArray();
        await ReadAsync(deltas, T0, first);

        /* Statement 1 does the least work of every counter, so the cap drops it. */
        var second = Enumerable.Range(1, count).Select(n => (n, 1000L + n + (n == 1 ? 1 : 100 + n))).ToArray();
        var kept = await ReadAsync(deltas, T0.AddMinutes(1), second);
        Assert.DoesNotContain(kept, r => r.PlanHandle!.EndsWith("00000001", StringComparison.Ordinal));

        /* In the third run statement 1 does the most work, so it is kept. */
        var third = Enumerable.Range(1, count).Select(n => (n, n == 1 ? 6002L : 1000L + n + 200 + 2 * n)).ToArray();
        var next = await ReadAsync(deltas, T0.AddMinutes(2), third);
        var one = next.Single(r => r.PlanHandle!.EndsWith("00000001", StringComparison.Ordinal));
        Assert.Equal(5000, one.Precomputed!.Exec);
        Assert.Equal(60, one.Precomputed.ExecInterval);
    }

    /// <summary>A baseline another party cleared after the capture (a server cleared on a reconnect) is not resurrected.</summary>
    [Fact]
    public async Task ARollbackNeverResurrectsABaselineThatWasClearedMeanwhile()
    {
        var deltas = new CollectorDeltaCalculator();
        await ReadAsync(deltas, T0, (1, 100));
        var abandoned = await ReadAsync(deltas, T0.AddMinutes(1), (1, 130));
        deltas.ClearServer(7);
        ((IBaselineRollback)abandoned).RollBackBaselines();

        var next = (await ReadAsync(deltas, T0.AddMinutes(2), (1, 160))).Single().Precomputed!;
        Assert.Equal(0, next.Exec);
        Assert.Equal(0, next.ExecInterval);
    }

    /// <summary>
    /// The digest the host caches for a statement text must be the one the writer stores in the dimension, or every later
    /// run writes digests that point at no dimension row. Compared through the real code on both sides, for a text with an
    /// embedded NUL, which both strip.
    /// </summary>
    [Fact]
    public void TheCachedTextDigest_IsTheDigestTheWriterStores()
    {
        const string text = "SELECT 1\0 FROM dbo.t;";
        var row = new QueryStatsCollector.Row { SqlHandle = "0x03AB", PlanHandle = "0x06AB", CreationTime = T0.AddHours(-1), QueryHash = "0x01" };
        var cache = new PlanDigestCache<QueryStatsPlanKey>();
        var key = QueryStatsPlanKey.TryCreate(7, row)!.Value;
        var chunk = new List<(QueryStatsPlanKey Key, QueryStatsCollector.Row Row)> { (key, row) };
        var pending = new List<QueryStatsPlanKey>();

        DarlingCollectorRunner.ApplyFetchedQueryStatsText(
            chunk, new Dictionary<int, string?> { [0] = text }, new HashSet<int>(), cache, T0, pending);
        cache.ConfirmPending(pending, T0);
        Assert.True(cache.TryGet(key, T0, out var entry));

        var dimensions = new PayloadDimensionBatch();
        var plan = PayloadDimensions.DiversionPlanFor(QueryStatsCollector.Instance);
        var queryTextPosition = plan.Single(p => p.Value.DimTable == PayloadDimensions.QueryTextDimTable).Key;
        var writer = new PgCollectorRowWriter();
        writer.UseDimensions(plan, dimensions);
        writer.BeginPayload();
        for (var i = 0; i < queryTextPosition; i++)
        {
            try { writer.NullValue(); } catch (InvalidOperationException) { /* no importer: the position still counts */ }
        }

        try { writer.Value(text); } catch (InvalidOperationException) { /* the dimension add comes before the importer write */ }

        var (digests, _) = dimensions.ToArrays(PayloadDimensions.QueryTextDimTable);
        Assert.Equal(entry.Digest, Convert.ToHexString(Assert.Single(digests)));
    }
}
