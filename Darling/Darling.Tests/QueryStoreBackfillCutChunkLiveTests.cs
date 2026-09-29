/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4662 end to end against a REAL store: the Query Store backfill's per-tick database list reads from the start
/// of the chunk that holds the backfill floor (TimescaleDB) or walks the index (plain PostgreSQL), instead of
/// filtering <c>collection_time &gt; floor</c> inside the newest chunk, which walked that chunk's index entry by
/// entry.
///
/// <para><b>#1776 own-store</b> - every test mints its own scratch database; nothing here touches the shared
/// live fixture. The floors are fixed dates, not the wall clock, so the chunk each one lands in is fixed too.</para>
/// </summary>
public sealed class QueryStoreBackfillCutChunkLiveTests
{
    private const int Server = -466201;
    private const int OtherServer = -466202;
    private static long s_id = 4662000;

    private static DateTime At(int month, int day, int hour, int minute = 0)
        => new(2026, month, day, hour, minute, 0, DateTimeKind.Unspecified);

    private static readonly IReadOnlyDictionary<string, string> NoState = new Dictionary<string, string>(StringComparer.Ordinal);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> CreateStoreAsync(bool timescale, CancellationToken ct)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the live #4662 candidate-read tests (each mints its own scratch database).");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (timescale)
        {
            Assert.True(await TimescaleSupport.TryEnableAsync(connection, null, ct), "the dev fixture is expected to have TimescaleDB installed");
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        return (scratch, connection);
    }

    private static async Task SeedAsync(NpgsqlConnection connection, int serverId, string? database, DateTime collectionTime, CancellationToken ct)
    {
        const string sql = @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us)
VALUES
    ($1, $2, $3, 'SQL01', $4, 'dbo.GetOrders', '0xABCD', 91, 111, 'Regular', 'Primary',
     1, $2, $2, $2, 1, 100, 100, 100, 100)";
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue(Interlocked.Increment(ref s_id));
        command.Parameters.AddWithValue(collectionTime);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue((object?)database ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static QueryStoreBackfill Backfill(NpgsqlDataSource postgres, bool timescale)
        => new(postgres, new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator()), new CollectorDeltaCalculator(), logger: null,
               hasContinuousAggregates: () => timescale);

    private static Dictionary<string, string> HoleState(string database, DateTime floor)
        => new(StringComparer.Ordinal)
        {
            [QueryStoreBackfillState.HoleKeyPrefix + database] = QueryStoreBackfillState.EncodeHole(floor.AddDays(-1), floor.AddDays(1)),
        };

    /// <summary>Pin 1. The floor is 09:00 inside the 1-day chunk [06-15 00:00, 06-16 00:00). The list is the names
    /// with rows at or after the chunk's start - one with rows after the floor, one with rows only between the
    /// chunk start and the floor - plus the name a hole key adds; a name whose rows are all in the previous chunk,
    /// another server's name and a NULL name are not in it. Ordinal order.</summary>
    [Fact]
    public async Task TimescaleStore_ListsNamesFromTheCutChunkStart_ExactlyThoseAndTheHoleName()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(6, 15, 9);
        var (scratch, connection) = await CreateStoreAsync(timescale: true, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "busy", floor.AddHours(1), ct);
        await SeedAsync(connection, Server, "tail_only", At(6, 15, 3), ct);
        await SeedAsync(connection, Server, "tail_at_start", At(6, 15, 0), ct);
        await SeedAsync(connection, Server, "before_chunk", At(6, 14, 23, 59), ct);
        await SeedAsync(connection, OtherServer, "other_server", floor.AddHours(1), ct);
        await SeedAsync(connection, Server, null, floor.AddHours(1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var list = await Backfill(postgres, timescale: true)
            .GetCandidateDatabasesAsync(Server, floor, HoleState("zz_hole_no_rows", floor), ct);

        Assert.Equal(new[] { "busy", "tail_at_start", "tail_only", "zz_hole_no_rows" }, list);
    }

    /// <summary>Pin 2. Plain PostgreSQL has no chunks: the walk lists every non-NULL name stored for the server,
    /// older ones included, and nothing from another server.</summary>
    [Fact]
    public async Task PlainStore_WalksEveryNameOfTheServer_OlderIncluded_NothingFromAnotherServer()
    {
        var ct = TestContext.Current.CancellationToken;
        var floor = At(6, 15, 9);
        var (scratch, connection) = await CreateStoreAsync(timescale: false, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "busy", floor.AddHours(1), ct);
        await SeedAsync(connection, Server, "older_than_the_floor", At(6, 1, 12), ct);
        await SeedAsync(connection, Server, "older_still", At(5, 20, 12), ct);
        await SeedAsync(connection, OtherServer, "other_server", floor.AddHours(1), ct);
        await SeedAsync(connection, Server, null, floor.AddHours(1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var list = await Backfill(postgres, timescale: false).GetCandidateDatabasesAsync(Server, floor, NoState, ct);

        Assert.Equal(new[] { "busy", "older_still", "older_than_the_floor" }, list);
    }

    /// <summary>Pin 3. The boundary is the catalog's, not a constant: <c>set_chunk_time_interval</c> changes only new
    /// chunks. Old rows sit in 1-day chunks, the interval is then set to 6 hours, newer rows land in 6-hour chunks.
    /// A floor in an old 1-day chunk lists from that chunk's start; a floor in a new 6-hour chunk lists from the
    /// 6-hour chunk's start (a 1-day boundary would also list <c>n_before</c>, a 6-hour one would miss <c>early</c>).</summary>
    [Fact]
    public async Task TimescaleStore_TakesTheBoundaryFromTheCatalog_AcrossAnIntervalChange()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await CreateStoreAsync(timescale: true, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "previous_day", At(6, 9, 22), ct);
        await SeedAsync(connection, Server, "early", At(6, 10, 2), ct);
        await SeedAsync(connection, Server, "late", At(6, 10, 21), ct);
        await ExecAsync(connection, "SELECT set_chunk_time_interval('collect.query_store_stats'::regclass, INTERVAL '6 hours')", ct);
        await SeedAsync(connection, Server, "n_before", At(6, 20, 7), ct);
        await SeedAsync(connection, Server, "n_in", At(6, 20, 13), ct);
        await SeedAsync(connection, Server, "n_after", At(6, 20, 15), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var backfill = Backfill(postgres, timescale: true);

        var inOldChunk = await backfill.GetCandidateDatabasesAsync(Server, At(6, 10, 20), NoState, ct);
        Assert.Equal(new[] { "early", "late", "n_after", "n_before", "n_in" }, inOldChunk);

        var inNewChunk = await backfill.GetCandidateDatabasesAsync(Server, At(6, 20, 14), NoState, ct);
        Assert.Equal(new[] { "n_after", "n_in" }, inNewChunk);
    }

    /// <summary>Pin 4. A floor that no chunk holds (the day between two seeded chunks) has no catalog row, so the
    /// read is the floor-bound statement it always was: names with rows after the floor, none from before it.</summary>
    [Fact]
    public async Task TimescaleStore_AFloorNoChunkHolds_FallsBackToTheFloorBoundList()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await CreateStoreAsync(timescale: true, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await SeedAsync(connection, Server, "old", At(6, 13, 12), ct);
        await SeedAsync(connection, Server, "busy", At(6, 15, 10), ct);
        await SeedAsync(connection, Server, "tail_of_next_chunk", At(6, 15, 1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var list = await Backfill(postgres, timescale: true).GetCandidateDatabasesAsync(Server, At(6, 14, 12), NoState, ct);

        Assert.Equal(new[] { "busy", "tail_of_next_chunk" }, list);
    }
}
