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
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9359: a Query Store backfill slice stores its rows and its state (the hole's remainder, the done mark) in ONE
/// transaction. <c>collect.query_store_stats</c> has no unique index, so a slice whose rows committed while its state
/// did not was walked again and stored a second time. The tests fail the slice between its rows and its state and
/// check that the store is as it was, and that the walk that follows stores each row once.
///
/// <para><b>#1776 own-store</b> - every test mints its own scratch database.</para>
/// </summary>
public sealed class QueryStoreBackfillSliceAtomicLiveTests
{
    private const int Server = -935901;
    private const string Database = "slice_db";

    private static DateTime At(int day, int hour) => new(2026, 6, day, hour, 0, 0, DateTimeKind.Unspecified);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> CreateStoreAsync(CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #9359 slice tests (each mints its own scratch database).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        return (scratch, connection);
    }

    private static ServerRuntime RuntimeFor() => new()
    {
        Config = new MonitoredServer { Name = "slice-qs", Host = "slice-qs" },
        ConnectionString = "Server=slice-qs",
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "slice-qs",
        ServerId = Server,
        EngineEdition = 3,
    };

    private static QueryStoreCollector.Row Row(long queryId, DateTime when) => new()
    {
        DatabaseName = Database,
        QueryId = queryId,
        PlanId = queryId,
        ExecutionTypeDesc = "Regular",
        FirstExecutionTime = when,
        LastExecutionTime = when,
        QueryHash = "0x0000" + queryId,
        QueryPlanHash = "0x0000" + queryId,
        ExecutionCount = 1,
        AvgCpuTimeUs = 10,
        AvgDurationUs = 20,
        RuntimeStatsIntervalId = queryId,
    };

    private static (QueryStoreBackfill Backfill, List<QueryStoreCollector.Row> Rows, CollectorContext Context) Build(
        NpgsqlDataSource postgres, DateTime floor, DateTime shippedTo)
    {
        var deltas = new CollectorDeltaCalculator();
        var runner = new DarlingCollectorRunner(postgres, deltas);
        var backfill = new QueryStoreBackfill(postgres, runner, deltas, logger: null, hasContinuousAggregates: () => true);
        var server = RuntimeFor();
        var rows = new List<QueryStoreCollector.Row> { Row(11, floor.AddHours(2)), Row(12, floor.AddHours(3)) };
        var context = new CollectorContext
        {
            ServerId = Server,
            ServerName = server.StorageName,
            CollectionTime = DateTime.UtcNow,
            Deltas = deltas,
            Target = server.Target,
            PerItemShippedBoundary = shippedTo,
        };
        return (backfill, rows, context);
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] args)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.AddWithValue(arg);
        }

        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static Task<long> RawRowsAsync(NpgsqlConnection connection, CancellationToken ct)
        => ScalarAsync(connection, "SELECT count(*) FROM collect.query_store_stats WHERE server_id = $1 AND database_name = $2", ct, Server, Database);

    private static Task<long> DistinctRawRowsAsync(NpgsqlConnection connection, CancellationToken ct)
        => ScalarAsync(connection, "SELECT count(DISTINCT (query_id, plan_id)) FROM collect.query_store_stats WHERE server_id = $1 AND database_name = $2", ct, Server, Database);

    private static async Task<string?> StateAsync(NpgsqlConnection connection, string key, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT state_value FROM collector_state WHERE server_id = $1 AND collector_name = $2 AND state_key = $3", connection);
        command.Parameters.AddWithValue(Server);
        command.Parameters.AddWithValue(QueryStoreBackfill.StateCollectorName);
        command.Parameters.AddWithValue(key);
        return await command.ExecuteScalarAsync(ct) as string;
    }

    private static async Task SetStateAsync(NpgsqlConnection connection, string key, string value, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO collector_state (server_id, collector_name, state_key, state_value, updated_at) VALUES ($1, $2, $3, $4, $5)", connection);
        command.Parameters.AddWithValue(Server);
        command.Parameters.AddWithValue(QueryStoreBackfill.StateCollectorName);
        command.Parameters.AddWithValue(key);
        command.Parameters.AddWithValue(value);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>A hole slice that fails between its rows and its state leaves no rows and the hole record as it was; the
    /// walk that follows stores each row once and retires the hole.</summary>
    [Fact]
    public async Task HoleSlice_FailingAfterItsRows_StoresNothing_AndTheReWalkStoresEachRowOnce()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(10, 0);
        var ceiling = At(10, 12);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var holeKey = QueryStoreBackfillState.HoleKeyPrefix + Database;
        var hole = QueryStoreBackfillState.EncodeHole(floor, ceiling);
        await SetStateAsync(connection, holeKey, hole, ct);

        var (backfill, rows, context) = Build(postgres, floor, shippedTo: floor);
        backfill.BeforeSliceStateForTests = () => throw new InvalidOperationException("injected state fault");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => backfill.StoreSliceAsync(RuntimeFor(), Database, floor, ceiling, floor, isHole: true, rows, context, ct));

        Assert.Equal(0, await RawRowsAsync(connection, ct));
        Assert.Equal(hole, await StateAsync(connection, holeKey, ct));

        backfill.BeforeSliceStateForTests = null;
        await backfill.StoreSliceAsync(RuntimeFor(), Database, floor, ceiling, floor, isHole: true, rows, context, ct);

        Assert.Equal(2, await RawRowsAsync(connection, ct));
        Assert.Equal(2, await DistinctRawRowsAsync(connection, ct));
        Assert.Null(await StateAsync(connection, holeKey, ct));
    }

    /// <summary>The same for a tail slice that reaches the floor: the done mark is written only with the rows.</summary>
    [Fact]
    public async Task TailSlice_FailingAfterItsRows_StoresNothing_AndTheReWalkStoresEachRowOnceAndMarksDone()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(10, 0);
        var ceiling = At(10, 12);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var doneKey = QueryStoreBackfillState.DoneKeyPrefix + Database;
        var (backfill, rows, context) = Build(postgres, floor, shippedTo: floor);
        backfill.BeforeSliceStateForTests = () => throw new InvalidOperationException("injected state fault");

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => backfill.StoreSliceAsync(RuntimeFor(), Database, floor, ceiling, floor, isHole: false, rows, context, ct));

        Assert.Equal(0, await RawRowsAsync(connection, ct));
        Assert.Null(await StateAsync(connection, doneKey, ct));

        backfill.BeforeSliceStateForTests = null;
        await backfill.StoreSliceAsync(RuntimeFor(), Database, floor, ceiling, floor, isHole: false, rows, context, ct);

        Assert.Equal(2, await RawRowsAsync(connection, ct));
        Assert.Equal(2, await DistinctRawRowsAsync(connection, ct));
        Assert.NotNull(await StateAsync(connection, doneKey, ct));
    }

    /// <summary>The normal path: a hole slice that shipped part of its range keeps the rest of the hole, and each row is
    /// stored once. A tail slice that stopped above the floor writes no done mark.</summary>
    [Fact]
    public async Task NormalSlices_StoreEachRowOnce_AndSaveTheirState()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(10, 0);
        var ceiling = At(10, 12);
        var shippedTo = At(10, 4);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var holeKey = QueryStoreBackfillState.HoleKeyPrefix + Database;
        await SetStateAsync(connection, holeKey, QueryStoreBackfillState.EncodeHole(floor, ceiling), ct);

        var (backfill, rows, context) = Build(postgres, floor, shippedTo);
        await backfill.StoreSliceAsync(RuntimeFor(), Database, floor, ceiling, floor, isHole: true, rows, context, ct);

        Assert.Equal(2, await RawRowsAsync(connection, ct));
        Assert.Equal(2, await DistinctRawRowsAsync(connection, ct));
        var remaining = await StateAsync(connection, holeKey, ct);
        Assert.NotNull(remaining);
        Assert.True(QueryStoreBackfillState.TryDecodeHoles(remaining, out var holes));
        Assert.Equal(new[] { (floor, shippedTo) }, holes.ToArray());

        var tail = Build(postgres, floor, shippedTo);
        tail.Rows.Clear();
        tail.Rows.Add(Row(21, At(10, 6)));
        await tail.Backfill.StoreSliceAsync(RuntimeFor(), Database, floor, ceiling, floor, isHole: false, tail.Rows, tail.Context, ct);

        Assert.Equal(3, await RawRowsAsync(connection, ct));
        Assert.Null(await StateAsync(connection, QueryStoreBackfillState.DoneKeyPrefix + Database, ct));
    }

    /// <summary>
    /// A hole the live collector records while a hole slice is between its read and its commit. The slice's progress must survive
    /// and the new hole must be merged after it. Before the recorder read its key FOR UPDATE it merged into the value from before the
    /// slice, and its save put the serviced range back for the backfill to walk, and copy, again.
    /// </summary>
    [Fact]
    public async Task AHoleRecordedWhileASliceIsBetweenItsReadAndItsCommit_MergesAfterTheSlice_AndTheServicedRangeStaysGone()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(10, 0);
        var ceiling = At(10, 12);
        var shippedTo = At(10, 4);
        var (scratch, connection) = await CreateStoreAsync(ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var holeKey = QueryStoreBackfillState.HoleKeyPrefix + Database;
        await SetStateAsync(connection, holeKey, QueryStoreBackfillState.EncodeHole(floor, ceiling), ct);

        var (backfill, rows, context) = Build(postgres, floor, shippedTo);
        var recorder = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        Task? recording = null;
        var newFrom = At(11, 0);
        var newTo = At(11, 2);
        backfill.AfterSliceStateForTests = async () =>
        {
            recording = recorder.RecordQueryStoreBackfillHoleAsync(Server, Database, newFrom, newTo, ct);

            /* Wait until the recorder is held up on the row the slice has locked, so it has either read the pre-slice value and is
               waiting to save it, or is waiting to read. Either way it reaches the store before the commit and finishes after it. */
            for (var attempt = 0; attempt < 200; attempt++)
            {
                if (await ScalarAsync(connection, "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'", ct) > 0)
                {
                    return;
                }

                await Task.Delay(50, ct);
            }

            Assert.Fail("the recorder never waited on the slice's lock");
        };

        await backfill.StoreSliceAsync(RuntimeFor(), Database, floor, ceiling, floor, isHole: true, rows, context, ct);
        Assert.NotNull(recording);
        await recording!;

        Assert.Equal(2, await RawRowsAsync(connection, ct));
        Assert.True(QueryStoreBackfillState.TryDecodeHoles(await StateAsync(connection, holeKey, ct), out var holes));
        Assert.Equal(new[] { (floor, shippedTo), (newFrom, newTo) }, holes.OrderBy(hole => hole.FromUtc).ToArray());
    }
}
