/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9980's live tests: the payload dimension GC runs after every other table of the retention pass, so a pass whose wall
/// budget runs out in a dimension's cursored delete has already purged the tables that are not part of the GC. Each test
/// runs the product's own sweep on a scratch store. The clock is derived from the rows the held table has lost (0 until
/// its first batch commits, past the budget after it), so the stop lands on the same batch boundary on every run with no
/// wait. Gated on <c>DARLING_TEST_PG</c>.
/// </summary>
/* #1776 own-store: mints its own scratch database through ScratchPostgres and never touches the shared store's tables. */
public sealed class RetentionDimensionGcLastLiveTests
{
    /// <summary>More rows than one cursored batch takes, so the first batch is full and the budget stops the second.</summary>
    private const int BacklogRows = DarlingRetention.PlanDimDeleteRowCap + 10_000;

    private const int FewRows = 5;
    private const int ExpiredDays = 4_000;
    private const string PlanDim = "collect." + PayloadDimensions.QueryPlanDimTable;
    private const string TextDim = "collect." + PayloadDimensions.QueryTextDimTable;
    private const string Sightings = "collect.query_tag_sightings";
    private const string StopLine = "Retention purge stopped at its";

    /// <summary>The paced statements of the dimension GC on a pass whose text prune is due: the PostgreSQL statement text, the plan map, the Query Store text, its self keys and the two dimensions.</summary>
    private const int GcStatements = 6;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private sealed class NeverBehind : ICollectionPressure
    {
        public string? BehindReason() => null;
    }

    private sealed class Store : IAsyncDisposable
    {
        private readonly ScratchPostgres _scratch;

        private Store(ScratchPostgres scratch, NpgsqlConnection connection, NpgsqlConnection counter, NpgsqlDataSource source, CancellationToken ct)
        {
            _scratch = scratch;
            Connection = connection;
            Counter = counter;
            Source = source;
            Ct = ct;
        }

        public NpgsqlConnection Connection { get; }

        /// <summary>The clock's own connection: the clock is read while the pass's statements run on other ones.</summary>
        public NpgsqlConnection Counter { get; }

        public NpgsqlDataSource Source { get; }

        public CancellationToken Ct { get; }

        public static async Task<Store> CreateAsync(string baseConnectionString, CancellationToken ct)
        {
            var scratch = await ScratchPostgres.CreateAsync(baseConnectionString, ct);
            var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            var counter = new NpgsqlConnection(scratch.ConnectionString);
            await counter.OpenAsync(ct);
            return new Store(scratch, connection, counter, NpgsqlDataSource.Create(scratch.ConnectionString), ct);
        }

        public async ValueTask DisposeAsync()
        {
            await Source.DisposeAsync();
            await Counter.DisposeAsync();
            await Connection.DisposeAsync();
            await _scratch.DisposeAsync();
        }
    }

    private static DateTime Expired => DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified).AddDays(-ExpiredDays);

    /// <summary>Plants <paramref name="rows"/> expired rows in a dimension, each with its stamp row, so the cursored delete finds every one.</summary>
    private static async Task PlantExpiredDimensionRowsAsync(Store store, string dimTable, string payloadColumn, int rows)
    {
        await using (var seed = new NpgsqlCommand(
            $"INSERT INTO {dimTable} (digest, {payloadColumn}, last_seen) " +
            $"SELECT sha256(('{dimTable}-' || gs)::bytea), 'payload ' || gs, $2 FROM generate_series(1, $1) gs", store.Connection))
        {
            seed.Parameters.AddWithValue(rows);
            seed.Parameters.AddWithValue(Expired);
            await seed.ExecuteNonQueryAsync(store.Ct);
        }

        await using var stamps = new NpgsqlCommand(
            $"INSERT INTO {dimTable}_seen (digest, last_seen) SELECT digest, last_seen FROM {dimTable} ON CONFLICT (digest) DO NOTHING",
            store.Connection);
        await stamps.ExecuteNonQueryAsync(store.Ct);
    }

    /// <summary>One expired sighting and one expired collection log row: two tables that are not part of the dimension GC.</summary>
    private static async Task PlantExpiredRowsOutsideTheGcAsync(Store store)
    {
        await using (var sighting = new NpgsqlCommand(
            $"INSERT INTO {Sightings} (server_id, key_kind, key, first_seen, last_seen) " +
            $"VALUES (9980, 'hash', '0x9980', now() - interval '{ExpiredDays} days', now() - interval '{ExpiredDays} days')", store.Connection))
        {
            await sighting.ExecuteNonQueryAsync(store.Ct);
        }

        await using var log = new NpgsqlCommand(
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, status) " +
            "VALUES (9980, 9980, 'dim-gc-last', 'query_stats', $1, 'SUCCESS')", store.Connection);
        log.Parameters.AddWithValue(Expired);
        await log.ExecuteNonQueryAsync(store.Ct);
    }

    private static async Task<long> RowsAsync(Store store, string table)
    {
        await using var count = new NpgsqlCommand($"SELECT count(*) FROM {table}", store.Connection);
        return (long)(await count.ExecuteScalarAsync(store.Ct))!;
    }

    private static async Task<long> ExpiredSightingsAsync(Store store)
    {
        await using var count = new NpgsqlCommand(
            $"SELECT count(*) FROM {Sightings} WHERE last_seen < now() - interval '{ExpiredDays - 1} days'", store.Connection);
        return (long)(await count.ExecuteScalarAsync(store.Ct))!;
    }

    private static async Task<long> ExpiredLogRowsAsync(Store store)
    {
        await using var count = new NpgsqlCommand("SELECT count(*) FROM collect.collection_log WHERE collection_time < $1", store.Connection);
        count.Parameters.AddWithValue(Expired.AddDays(1));
        return (long)(await count.ExecuteScalarAsync(store.Ct))!;
    }

    /// <summary>
    /// A pass with a 50 s wall budget on a clock that reads 0 until <paramref name="heldIsUnderWay"/> answers true and 100
    /// from then on: the budget is spent at the first batch boundary after the held table loses its first rows.
    /// </summary>
    private static async Task<(PurgeSummary Summary, RetentionCollectionYield Yield, CapturingTestLogger Log)> PassThatStopsOnceAsync(
        Store store, string heldIsUnderWaySql)
    {
        double WorkClock()
        {
            using var read = new NpgsqlCommand(heldIsUnderWaySql, store.Counter);
            return (bool)read.ExecuteScalar()! ? 100 : 0;
        }

        var yield = new RetentionCollectionYield(
            new NeverBehind(), TimeSpan.FromSeconds(50), logger: null, delay: (_, _) => Task.CompletedTask, secondsClock: WorkClock);
        var pacer = new RetentionWalPacer(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask) { Yield = yield };
        var log = new CapturingTestLogger();
        var summary = await DarlingRetention.PurgeWithPacerAsync(
            store.Source, timescaleAvailable: false, log, store.Ct, retentionDaysFor: null, planContentRetentionDays: 0, walPacer: pacer);
        return (summary, yield, log);
    }

    private static string StopLineOf(CapturingTestLogger log) =>
        Assert.Single(log.Lines, line => line.Contains(StopLine, StringComparison.Ordinal));

    [Fact]
    public async Task APassStoppedInThePlanDimension_HasPurgedTheOtherTables_AndTheNextPassFinishesTheDimension()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9980 live tests.");
        await using var store = await Store.CreateAsync(ConnectionString!, TestContext.Current.CancellationToken);

        await PlantExpiredDimensionRowsAsync(store, PlanDim, "query_plan_xml", BacklogRows);
        await PlantExpiredRowsOutsideTheGcAsync(store);

        var stopped = await PassThatStopsOnceAsync(store, $"SELECT count(*) < {BacklogRows} FROM {PlanDim}");

        /* The tables outside the GC were purged before the plan dimension's delete took the rest of the budget. */
        Assert.True(0L == await ExpiredSightingsAsync(store), stopped.Log.Joined);
        Assert.True(0L == await ExpiredLogRowsAsync(store), stopped.Log.Joined);

        /* The stop is in the plan dimension, one full batch in, and the rows after that batch are left. */
        Assert.True("in " + PayloadDimensions.QueryPlanDimTable == stopped.Yield.StoppedPlace, stopped.Log.Joined);
        Assert.Equal((long)(BacklogRows - DarlingRetention.PlanDimDeleteRowCap), await RowsAsync(store, PlanDim));
        Assert.True(stopped.Summary.StoppedOnBudget, stopped.Log.Joined);
        Assert.Equal(1, stopped.Yield.TablesLeft);

        /* Nothing runs after the plan dimension, so no table is counted as not reached and the stop line names none. */
        Assert.Equal(0, stopped.Yield.TablesNotReached);
        Assert.Null(stopped.Yield.FirstTableNotReached);
        var line = StopLineOf(stopped.Log);
        Assert.Contains("in " + PayloadDimensions.QueryPlanDimTable, line, StringComparison.Ordinal);
        Assert.DoesNotContain("the first of them", line, StringComparison.Ordinal);

        /* A pass with room to finish takes the rows that were left: the dimension GC still runs to the end of its tables. */
        var roomy = new RetentionWalPacer(rateBytesPerSecond: 1_000_000_000, delay: (_, _) => Task.CompletedTask)
        {
            Yield = new RetentionCollectionYield(new NeverBehind(), TimeSpan.FromHours(1), logger: null, delay: (_, _) => Task.CompletedTask),
        };
        var finishLog = new CapturingTestLogger();
        var finished = await DarlingRetention.PurgeWithPacerAsync(
            store.Source, timescaleAvailable: false, finishLog, store.Ct, retentionDaysFor: null, planContentRetentionDays: 0, walPacer: roomy);

        Assert.False(finished.StoppedOnBudget, finishLog.Joined);
        Assert.Equal(0L, await RowsAsync(store, PlanDim));
        Assert.Equal(0L, await RowsAsync(store, PlanDim + "_seen"));
        Assert.DoesNotContain(finishLog.Lines, l => l.Contains(StopLine, StringComparison.Ordinal));
    }

    [Fact]
    public async Task APassStoppedInTheTextDimension_CountsOnlyThePlanDimensionAsNotReached_AndTheStopLineNamesIt()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9980 live tests.");
        await using var store = await Store.CreateAsync(ConnectionString!, TestContext.Current.CancellationToken);

        await PlantExpiredDimensionRowsAsync(store, TextDim, "query_text", BacklogRows);
        await PlantExpiredDimensionRowsAsync(store, PlanDim, "query_plan_xml", FewRows);
        await PlantExpiredRowsOutsideTheGcAsync(store);

        var stopped = await PassThatStopsOnceAsync(store, $"SELECT count(*) < {BacklogRows} FROM {TextDim}");

        Assert.True(0L == await ExpiredSightingsAsync(store), stopped.Log.Joined);
        Assert.True(0L == await ExpiredLogRowsAsync(store), stopped.Log.Joined);
        Assert.True("in " + PayloadDimensions.QueryTextDimTable == stopped.Yield.StoppedPlace, stopped.Log.Joined);

        /* The plan dimension is the one statement after the text dimension, and the only table not reached. */
        Assert.Equal((long)FewRows, await RowsAsync(store, PlanDim));
        Assert.True(1 == stopped.Yield.TablesNotReached, stopped.Log.Joined);
        Assert.Equal(PayloadDimensions.QueryPlanDimTable, stopped.Yield.FirstTableNotReached);
        Assert.Contains(PayloadDimensions.QueryPlanDimTable, StopLineOf(stopped.Log), StringComparison.Ordinal);
    }

    [Fact]
    public async Task APassStoppedAheadOfTheDimensionGc_LeavesTheGcNotReached_AndTheStopLineNamesTheFirstTableNotReached()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #9980 live tests.");
        await using var store = await Store.CreateAsync(ConnectionString!, TestContext.Current.CancellationToken);

        await PlantExpiredDimensionRowsAsync(store, PlanDim, "query_plan_xml", FewRows);
        await PlantExpiredRowsOutsideTheGcAsync(store);

        /* The budget is spent once the sightings move has taken its one expired row. */
        var stopped = await PassThatStopsOnceAsync(
            store, $"SELECT count(*) = 0 FROM {Sightings} WHERE last_seen < now() - interval '{ExpiredDays - 1} days'");

        Assert.True("in " + Sightings == stopped.Yield.StoppedPlace, stopped.Log.Joined);

        /* Every statement after the sightings is not reached: the slim table first, the tables between, and the GC's own. */
        Assert.Equal(QueryTagSlimSightings.Table, stopped.Yield.FirstTableNotReached);
        Assert.True(stopped.Yield.TablesNotReached > GcStatements, stopped.Log.Joined);
        Assert.Equal((long)FewRows, await RowsAsync(store, PlanDim));
        Assert.Equal(1L, await ExpiredLogRowsAsync(store));
        Assert.Contains(QueryTagSlimSightings.Table, StopLineOf(stopped.Log), StringComparison.Ordinal);
    }
}
