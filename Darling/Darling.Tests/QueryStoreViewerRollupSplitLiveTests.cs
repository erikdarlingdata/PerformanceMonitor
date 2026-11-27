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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9287: the Viewer grid's closed-day rollup statements with each interval arm split by interval start, on a real store. The split pair
/// must return the rows of the statements it replaces, in the same order, wherever the data obeys what the bounds assume: a row with an
/// interval start has its first execution inside its interval, and the span the bound uses is the longest interval of the server. The
/// tests seed rows with an interval start, rows without one, rows whose interval runs past the rollup run's first midnight and past a
/// literal end, an open and a closed end, and a database filter. They also prove the two fallbacks: no index runs the old pair, and a span
/// that could not be resolved (a day) keeps the 25 hour slack, which a row of a nine hour interval needs.
/// </summary>
/// <remarks>The edge margin and the span a read resolves are read from the store's health captures, which these direct statement tests do not
/// seed: they pass the span in, as the selector takes it.</remarks>
[Collection(ViewerLoggerStaticsCollection.Name)]
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its own database through ScratchPostgres and works entirely inside it, so it cannot race live collection. */
public sealed class QueryStoreViewerRollupSplitLiveTests
{
    private const string Wide = QueryStoreTopSplitLiveTests.Wide;
    private const int ServerId = 9287;
    private const int Top = 500;

    /// <summary>Day zero. The window starts at 06:00 on it, the run of whole built days is days 1 to 3, and the window's literal end is noon on day 5.</summary>
    private static readonly DateTime D0 = new(2026, 9, 15, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowStart = D0.AddHours(6);
    private static readonly DateTime RunStart = D0.AddDays(1);
    private static readonly DateTime RunEnd = D0.AddDays(4);
    private static readonly DateTime LiteralEnd = D0.AddDays(5).AddHours(12);

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct) =>
        QueryStoreIntervalWideBrinIndexLiveTests.ExecAsync(connection, sql, ct);

    private static string Ts(DateTime value) => QueryStoreTopSplitLiveTests.Ts(value);

    /// <summary>One seeded row: query id (also its plan id), database, interval start (null for a legacy row), collection time, first and last execution.</summary>
    private sealed record Seed(int Id, string Database, DateTime? Start, DateTime Collected, DateTime First, DateTime Last);

    private static Seed WithStart(int id, string database, DateTime start, TimeSpan firstAfterStart, TimeSpan? length = null) =>
        new(id, database, start, start + (length ?? TimeSpan.FromHours(1)) + TimeSpan.FromMinutes(5), start + firstAfterStart, start + firstAfterStart + TimeSpan.FromMinutes(20));

    private static Seed NoStart(int id, string database, DateTime collected, DateTime first) =>
        new(id, database, null, collected, first, first + TimeSpan.FromMinutes(20));

    /// <summary>
    /// The seed. Ids 1 to 19 obey the proof under a one hour interval: a first execution inside the interval.
    /// <code>
    /// margin side:  1, 2 plain rows with a start inside the window; 3 and 4 an interval that runs past the run's first midnight (the first
    ///               execution is after it); 5 a start before the window (dropped); 6, 7 rows without a start, 6 with a first execution five
    ///               hours before the window and 7 collected before the run and first executed after its first midnight.
    /// the run:      8 and 9 inside the run's days (the daily rollup answers for them, so neither statement reads them); 10 starts before the
    ///               run's end and executes after it (read by the run's day, not by the open arm).
    /// open side:    11 to 14 starts after the run (14 after the literal end), 15 and 16 run past the run's end and past the literal end, 17 and
    ///               18 have no start (17 first executed four hours before the run's end).
    /// </code>
    /// </summary>
    private static IEnumerable<Seed> Rows() => new[]
    {
        WithStart(1, "qsA", D0.AddHours(7), TimeSpan.FromMinutes(10)),
        WithStart(2, "qsB", D0.AddHours(12), TimeSpan.FromMinutes(10)),
        WithStart(3, "qsA", D0.AddHours(23).AddMinutes(30), TimeSpan.FromMinutes(50)),
        WithStart(4, "qsA", D0.AddHours(23), TimeSpan.FromHours(1)),
        WithStart(5, "qsA", D0.AddHours(5).AddMinutes(30), TimeSpan.FromMinutes(10)),
        NoStart(6, "qsA", D0.AddHours(10), WindowStart.AddHours(-5)),
        NoStart(7, "qsB", D0.AddHours(23).AddMinutes(30), RunStart.AddMinutes(20)),
        WithStart(8, "qsA", D0.AddDays(2).AddHours(10), TimeSpan.FromMinutes(10)),
        NoStart(9, "qsA", D0.AddDays(2).AddHours(11), D0.AddDays(2).AddHours(11)),
        WithStart(10, "qsA", RunEnd.AddMinutes(-30), TimeSpan.FromMinutes(50)),
        WithStart(11, "qsA", RunEnd, TimeSpan.FromMinutes(10)),
        WithStart(12, "qsB", RunEnd.AddHours(3), TimeSpan.FromMinutes(10)),
        WithStart(13, "qsA", D0.AddDays(5).AddHours(11), TimeSpan.FromMinutes(10)),
        WithStart(14, "qsA", D0.AddDays(5).AddHours(13), TimeSpan.FromMinutes(10)),
        WithStart(15, "qsA", RunEnd.AddHours(23).AddMinutes(30), TimeSpan.FromMinutes(50)),
        WithStart(16, "qsB", D0.AddDays(5).AddHours(11).AddMinutes(30), TimeSpan.FromMinutes(50)),
        NoStart(17, "qsA", RunEnd.AddHours(5), RunEnd.AddHours(-4)),
        NoStart(18, "qsB", D0.AddDays(5).AddHours(11).AddMinutes(30), LiteralEnd.AddMinutes(20)),
    };

    /// <summary>Two rows only a day-long span lets through: a nine hour interval that ends after the run's first midnight, and one that
    /// starts before the literal end and executes nine hours into it.</summary>
    private static IEnumerable<Seed> LongIntervalRows() => new[]
    {
        WithStart(20, "qsA", D0.AddHours(23), TimeSpan.FromHours(8), TimeSpan.FromHours(9)),
        WithStart(21, "qsA", D0.AddDays(5).AddHours(11), TimeSpan.FromHours(9), TimeSpan.FromHours(10)),
    };

    private static async Task SeedAsync(NpgsqlConnection connection, IEnumerable<Seed> rows, CancellationToken ct)
    {
        string StartText(DateTime? start) => start is { } s ? $"timestamp '{Ts(s)}'" : "NULL";
        var values = string.Join(",\n", rows.Select(r => string.Create(CultureInfo.InvariantCulture,
            $"(timestamp '{Ts(r.Collected)}', {ServerId}, '{r.Database}', {r.Id}, {r.Id}, 'Regular', timestamp '{Ts(r.First)}', timestamp '{Ts(r.Last)}', 'select 1', {10 + r.Id}, {1000 + r.Id}, {r.Id}, {StartText(r.Start)})")));
        await ExecAsync(connection, $@"
INSERT INTO {Wide}
    (collection_time, server_id, database_name, query_id, plan_id, execution_type_desc, first_execution_time,
     last_execution_time, query_text, execution_count, avg_duration_us, runtime_stats_interval_id, interval_start_time_utc)
VALUES
{values};
ANALYZE {Wide};", ct);
    }

    private static void AddShared(NpgsqlCommand command, DateTime? end, IReadOnlyList<string>? databases)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(QueryStoreTopSplitLiveTests.Timestamp(WindowStart));
        command.Parameters.Add(end is { } e
            ? QueryStoreTopSplitLiveTests.Timestamp(e)
            : new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
        command.Parameters.Add(ViewerDataService.DatabaseFilterParameter(databases));
    }

    private static void AddRun(NpgsqlCommand command)
    {
        command.Parameters.Add(QueryStoreTopSplitLiveTests.Timestamp(RunStart));
        command.Parameters.Add(QueryStoreTopSplitLiveTests.Timestamp(RunEnd));
    }

    private static async Task<List<string>> ReadAllAsync(NpgsqlCommand command, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            rows.Add(string.Join("|", values.Select(v => v switch
            {
                DBNull => "NULL",
                DateTime stamp => stamp.ToString("O", CultureInfo.InvariantCulture),
                _ => Convert.ToString(v, CultureInfo.InvariantCulture),
            })));
        }

        return rows;
    }

    /// <summary>The grid's two rollup statements as it runs them: the kept keys, then the rows of those keys with plain index scans off. Returns the
    /// key lines and the row lines, each in the order the statement returned them.</summary>
    private static async Task<(List<string> Keys, List<string> Rows)> RunPairAsync(
        NpgsqlConnection connection, (string KeysSql, string TopSql) pair, DateTime? end, IReadOnlyList<string>? databases, CancellationToken ct)
    {
        var keys = new ViewerDataService.TableReadKeys();
        List<string> keyLines = new();
        await using (var keysCommand = new NpgsqlCommand(pair.KeysSql, connection) { CommandTimeout = 120 })
        {
            AddShared(keysCommand, end, databases);
            keysCommand.Parameters.Add(new NpgsqlParameter<int> { TypedValue = Top });
            AddRun(keysCommand);
            await using var keyReader = await keysCommand.ExecuteReaderAsync(ct);
            while (await keyReader.ReadAsync(ct))
            {
                keys.Add(keyReader);
                keyLines.Add(string.Join("|", Enumerable.Range(0, keyReader.FieldCount).Select(i => Convert.ToString(keyReader.GetValue(i), CultureInfo.InvariantCulture))));
            }
        }

        await ExecAsync(connection, "SET enable_indexscan = off", ct);
        try
        {
            await using var byKeys = new NpgsqlCommand(pair.TopSql, connection) { CommandTimeout = 120 };
            AddShared(byKeys, end, databases);
            keys.AddParameters(byKeys);
            AddRun(byKeys);
            keys.AddFilterParameters(byKeys);
            return (keyLines, await ReadAllAsync(byKeys, ct));
        }
        finally
        {
            await ExecAsync(connection, "RESET enable_indexscan", ct);
        }
    }

    private static HashSet<long> QueryIds(List<string> keyLines) => keyLines.Select(l => long.Parse(l.Split('|')[1], CultureInfo.InvariantCulture)).ToHashSet();

    private static async Task<NpgsqlConnection> OpenWithIndexAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        var connection = await QueryStoreTopSplitLiveTests.OpenAsync(scratch, ct);
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(connection, ct);
        Assert.True(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        return connection;
    }

    /// <summary>The window's shapes: a closed end and an open end, each over every database and over one.</summary>
    private static IEnumerable<(string Label, DateTime? End, string[]? Databases)> Shapes() =>
    [
        ("closed end", LiteralEnd, null),
        ("open end", null, null),
        ("closed end, one database", LiteralEnd, new[] { "qsA" }),
        ("open end, one database", null, new[] { "qsA" }),
    ];

    /// <summary>
    /// With a one hour interval the split pair returns the old pair's keys and rows, in the same order, at every shape. The rows it must read are
    /// named, so an empty answer cannot pass: the rows without a start, the interval that runs past the run's first midnight, and the ones that
    /// run past the literal end. The statements are also run through the edge margin rewrite, which narrows only the rows without a start.
    /// </summary>
    [Fact]
    public async Task TheSplitRollupStatements_ReturnTheRowsOfTheOldOnes_InTheSameOrder_AtEveryEndAndDatabaseFilter()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenWithIndexAsync(scratch, ct);
        await SeedAsync(connection, Rows(), ct);

        var oldPair = QueryStoreTopRollupStatementsFor(splitBuilt: false, TimeSpan.FromHours(1));
        var splitPair = QueryStoreTopRollupStatementsFor(splitBuilt: true, TimeSpan.FromHours(1));
        foreach (var (label, end, databases) in Shapes())
        {
            var old = await RunPairAsync(connection, oldPair, end, databases, ct);
            var split = await RunPairAsync(connection, splitPair, end, databases, ct);
            Assert.Equal(old.Keys, split.Keys);
            Assert.Equal(old.Rows, split.Rows);

            var ids = QueryIds(split.Keys);
            Assert.True(ids.IsSupersetOf(new long[] { 1, 3, 4, 6, 11, 15, 17 }), $"{label}: the rows the arms read are missing from {string.Join(",", ids.OrderBy(i => i))}");
            Assert.DoesNotContain(5L, ids);
            Assert.DoesNotContain(8L, ids);
            Assert.DoesNotContain(9L, ids);
            Assert.DoesNotContain(10L, ids);
            Assert.Equal(end is null, ids.Contains(14));
            Assert.Equal(databases is null, ids.Contains(2));
            Assert.Equal(databases is null, ids.Contains(7));
        }

        /* The edge margin rewrite reaches the rows without a start in the split statements, as it does in the old ones: row 6 (first executed
           five hours before the window) and row 17 (four hours before the run's end) are read at the full margin and not at three hours, and the
           rows with a start do not move. */
        var narrowed = QueryStoreIntervalWide.EdgeMarginSqlFor(TimeSpan.FromHours(3));
        (string, string) Narrow((string KeysSql, string TopSql) pair) =>
            (QueryStoreIntervalWide.WithEdgeMargin(pair.KeysSql, narrowed), QueryStoreIntervalWide.WithEdgeMargin(pair.TopSql, narrowed));
        var oldNarrow = await RunPairAsync(connection, Narrow(oldPair), null, null, ct);
        var splitNarrow = await RunPairAsync(connection, Narrow(splitPair), null, null, ct);
        var full = await RunPairAsync(connection, splitPair, null, null, ct);
        Assert.Equal(oldNarrow.Keys, splitNarrow.Keys);
        Assert.Equal(oldNarrow.Rows, splitNarrow.Rows);
        Assert.Contains(6L, QueryIds(full.Keys));
        Assert.DoesNotContain(6L, QueryIds(splitNarrow.Keys));
        Assert.DoesNotContain(17L, QueryIds(splitNarrow.Keys));
        Assert.Contains(1L, QueryIds(splitNarrow.Keys));
        Assert.Contains(3L, QueryIds(splitNarrow.Keys));

        /* A wrong bound is caught: an upper slack of nothing drops the interval that runs past the run's first midnight (3, 4), and the same
           answer test above fails on it. */
        var zeroSlack = (ViewerDataService.QueryStoreTopRollupSplitKeysSqlFor(QueryStoreIntervalWide.EdgeMarginSqlFor(TimeSpan.Zero)),
            ViewerDataService.QueryStoreTopRollupSplitByKeysSqlFor(QueryStoreIntervalWide.EdgeMarginSqlFor(TimeSpan.Zero)));
        var wrong = await RunPairAsync(connection, zeroSlack, null, null, ct);
        Assert.DoesNotContain(3L, QueryIds(wrong.Keys));
    }

    private static (string KeysSql, string TopSql) QueryStoreTopRollupStatementsFor(bool splitBuilt, TimeSpan longestSpan) =>
        ViewerDataService.QueryStoreTopRollupStatements(splitBuilt, longestSpan);

    /// <summary>
    /// A span that could not be resolved comes through as a day, and the arms then keep the 25 hour slack they had: rows of a nine hour interval
    /// that a one hour span would drop are read, at a closed and an open end, and the answer is the old statements'. Without the index the
    /// selector gives the old statements.
    /// </summary>
    [Fact]
    public async Task AnUnresolvedSpan_KeepsTheOldSlack_AndWithoutTheIndexTheOldStatementsRun()
    {
        var baseCs = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = await OpenWithIndexAsync(scratch, ct);
        await SeedAsync(connection, Rows().Concat(LongIntervalRows()), ct);

        var oldPair = QueryStoreTopRollupStatementsFor(splitBuilt: false, QueryStoreIntervalWide.IntervalSpanMargin);
        var dayPair = QueryStoreTopRollupStatementsFor(splitBuilt: true, QueryStoreIntervalWide.IntervalSpanMargin);
        var hourPair = QueryStoreTopRollupStatementsFor(splitBuilt: true, TimeSpan.FromHours(1));

        /* No index: the old statements, whatever span is passed. */
        Assert.Equal(oldPair, QueryStoreTopRollupStatementsFor(splitBuilt: false, TimeSpan.FromHours(1)));
        Assert.Equal(ViewerDataService.QueryStoreTopRollupKeysSql, oldPair.KeysSql);
        Assert.Equal(ViewerDataService.QueryStoreTopRollupByKeysSql, oldPair.TopSql);

        foreach (var (label, end, databases) in Shapes())
        {
            var old = await RunPairAsync(connection, oldPair, end, databases, ct);
            var day = await RunPairAsync(connection, dayPair, end, databases, ct);
            var hour = await RunPairAsync(connection, hourPair, end, databases, ct);
            Assert.Equal(old.Keys, day.Keys);
            Assert.Equal(old.Rows, day.Rows);

            var oldIds = QueryIds(old.Keys);
            Assert.Contains(20L, oldIds);
            Assert.Contains(21L, oldIds);
            Assert.Contains(20L, QueryIds(day.Keys));
            Assert.Contains(21L, QueryIds(day.Keys));
            Assert.DoesNotContain(20L, QueryIds(hour.Keys));
            /* An open end has no upper bound on its open arm, so row 21 is read there whatever the span. */
            Assert.Equal(end is null, QueryIds(hour.Keys).Contains(21));
        }
    }

    /// <summary>
    /// The shipped grid on a store that has built days: a Past 30 days window served from the rollups gives the same rows before the partial
    /// index is built (the old rollup statements) and after (the split ones), and its slow-load line says which shape it took.
    /// </summary>
    [Fact]
    public async Task TheGridServedFromTheRollups_ReadsTheSameRows_BeforeAndAfterTheIndexIsBuilt_AndNamesItsShape()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9287 live test.");
        var ct = TestContext.Current.CancellationToken;
        await using var store = await QueryStoreClosedDayRollupLiveTests.SeedAsync(ct);
        await QueryStoreClosedDayRollupLiveTests.TickAsync(store, QueryStoreClosedDayRollupLiveTests.KeepEverything, ct);

        var end = QueryStoreClosedDayRollupLiveTests.DataEnd;
        async Task<(List<string> Lines, string Source)> ReadAsync(string[]? databases)
        {
            var read = await store.Viewer.GetQueryStoreTopQueriesWithReachAsync(
                QueryStoreClosedDayRollupLiveTests.ServerId, end.AddDays(-30).AddHours(13), end, 50, databases, literalEndUtc: null, tags: null, ct);
            Assert.True(read.Plan is { UseTable: true, Source.RollupDays.Count: > 0 }, "the read did not take rollup days, so the comparison would prove nothing");
            Assert.True(read.Rows.Count > 0);
            return (read.Rows.Select(r => string.Join("|", r.DatabaseName, r.QueryId, r.PlanId, r.ExecutionTypeDesc, r.TotalExecutions, r.AvgDurationMs, r.AvgCpuTimeMs, Convert.ToString(r.FirstExecutionTime, CultureInfo.InvariantCulture), Convert.ToString(r.LastExecutionTime, CultureInfo.InvariantCulture))).ToList(),
                read.Plan!.Value.Source!.Describe());
        }

        Assert.False(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            store.Connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        var before = await ReadAsync(null);
        var beforeOne = await ReadAsync(new[] { "rollA" });
        await QueryStoreBackgroundIndexesLiveTests.EnsureAllAsync(store.Connection, ct);
        Assert.True(await QueryStoreBackgroundIndexes.IsBuiltAsync(
            store.Connection, null, QueryStoreBackgroundIndexes.WideNullStartFirstExec, 30, ct));
        var after = await ReadAsync(null);
        var afterOne = await ReadAsync(new[] { "rollA" });

        Assert.Equal(before.Lines, after.Lines);
        Assert.Equal(beforeOne.Lines, afterOne.Lines);
        /* The store has no health captures, so the edge margin is the full 26 hours. */
        Assert.Contains("edge margin fell back to 26 h, span fell back to 24 h, one arm, index not built", before.Source, StringComparison.Ordinal);
        Assert.Contains("edge margin fell back to 26 h, span fell back to 24 h, split arms", after.Source, StringComparison.Ordinal);
    }
}
