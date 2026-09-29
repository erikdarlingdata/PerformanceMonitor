/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
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

    /* ---- pins 5 to 7: what a whole tick spends on the extra names, a recorded hole, and the walk's plan. Pins 5 and 6
       drive RunServerSliceAsync itself, which computes its floor from the wall clock, so their rows are seeded
       relative to that floor. ---- */

    /// <summary>The floor <c>RunServerSliceAsync</c> will compute moves with the wall clock (now minus the
    /// horizon), and 1-day chunk boundaries fall at UTC midnight. Seeding and the tick are seconds apart, so this
    /// waits until the floor is three minutes clear of a boundary on both sides; a boundary between the seed and
    /// the tick would put the floor in another chunk than the seeded rows. Returns the floor, Unspecified kind.</summary>
    private static async Task<DateTime> WaitForFloorClearOfAChunkBoundaryAsync(CancellationToken ct)
    {
        var clearance = TimeSpan.FromMinutes(3);
        while (true)
        {
            var floor = DateTime.UtcNow - QueryStoreBackfill.HorizonFor(hasContinuousAggregates: true);
            if (floor.TimeOfDay >= clearance && floor.TimeOfDay <= TimeSpan.FromDays(1) - clearance)
            {
                return DateTime.SpecifyKind(floor, DateTimeKind.Unspecified);
            }

            await Task.Delay(TimeSpan.FromSeconds(10), ct);
        }
    }

    /// <summary>A connection string to a loopback port nothing listens on, so a slice - which opens a SqlConnection
    /// before it does anything else - fails at once with a connection <see cref="SqlException"/> instead of reaching
    /// a server. <c>Connect Timeout=2</c> bounds it either way.</summary>
    private static string RefusedConnectionString()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return $"Server=127.0.0.1,{port};User ID=unused;Password=unused;Connect Timeout=2;Encrypt=False;Pooling=False";
    }

    private static ServerRuntime ServerAt(string connectionString) => new()
    {
        Config = new MonitoredServer { Name = "qs4662", Host = "qs4662-host" },
        ConnectionString = connectionString,
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = "qs4662-host",
        ServerId = Server,
        EngineEdition = 3,
    };

    /// <summary>Counts the two floor reads (read A: <c>SELECT 1 ... collection_time &lt;= $3 LIMIT 1</c>; read B:
    /// <c>SELECT MIN(last_execution_time)</c>) on one scratch database. Both are trusted only after the product's own
    /// <see cref="QueryStoreBackfill.GetStoredFloorAsync"/> ran once for a name with no rows (read A misses, so read B
    /// runs) and each counted exactly once; the counts are then read as deltas from that point.</summary>
    private static async Task<(NpgsqlCommandCounter ReadA, NpgsqlCommandCounter ReadB, long BaseA, long BaseB)> CountFloorReadsAsync(
        QueryStoreBackfill backfill, string databaseName, DateTime floor, CancellationToken ct)
    {
        var readA = new NpgsqlCommandCounter(databaseName, "SELECT 1 FROM query_store_stats", "database_name = $2", "collection_time <= $3");
        var readB = new NpgsqlCommandCounter(databaseName, "SELECT MIN(last_execution_time) FROM query_store_stats");

        var missing = await backfill.GetStoredFloorAsync(Server, "a_name_with_no_rows", floor, ct);
        Assert.Null(missing);
        Assert.True(readA.Count == 1 && readB.Count == 1,
            $"The Npgsql activity listener counted read A {readA.Count} times and read B {readB.Count} times for one control call on '{databaseName}' instead of 1 and 1 - it cannot see the database name or the command text, so a count of the ticks' reads would be meaningless.");
        return (readA, readB, readA.Count, readB.Count);
    }

    /// <summary>Pin 5. A name whose rows are all in the cut chunk but before the floor is listed by the cut-chunk read
    /// (the read starts at the chunk's start) and has no Done key, so it reaches the floor reads. It costs exactly one
    /// read A (which hits, the row being at or before the floor) and one Done write, and it never reaches read B or a
    /// slice (a slice would throw here: the server's port refuses). The next tick lists it again, sees the Done key
    /// and runs no read for it.</summary>
    [Fact]
    public async Task TimescaleStore_AnExtraName_CostsOneFloorReadAndOneDoneWrite_Once()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await CreateStoreAsync(timescale: true, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        var floor = await WaitForFloorClearOfAChunkBoundaryAsync(ct);
        await SeedAsync(connection, Server, "quiet_extra", floor.AddMinutes(-1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var backfill = new QueryStoreBackfill(postgres, runner, new CollectorDeltaCalculator(), logger: null, hasContinuousAggregates: () => true);
        var server = ServerAt(RefusedConnectionString());
        var (readA, readB, baseA, baseB) = await CountFloorReadsAsync(backfill, scratch.DatabaseName, floor, ct);
        using var readAOwner = readA;
        using var readBOwner = readB;

        Assert.False(await backfill.RunServerSliceAsync(server, ct), "the first tick has no slice to run");
        Assert.Equal(1, readA.Count - baseA);
        Assert.Equal(0, readB.Count - baseB);
        var state = await runner.GetCollectorStateAsync(Server, QueryStoreBackfill.StateCollectorName, ct);
        Assert.True(state.ContainsKey(QueryStoreBackfillState.DoneKeyPrefix + "quiet_extra"),
            "the first tick must save the Done key for the extra name, or every later tick reads for it again");

        Assert.False(await backfill.RunServerSliceAsync(server, ct), "the second tick has no slice to run");
        Assert.Equal(1, readA.Count - baseA);
        Assert.Equal(0, readB.Count - baseB);
    }

    /// <summary>Pin 6. A database with a recorded hole that is also marked Done is still dug: the hole check runs
    /// before the Done check. The tick attempts the hole's slice (which throws the connection error, the port
    /// refusing) and runs neither floor read for it. This is what keeps a recorded outage gap backfilled after its
    /// database finished its first-contact tail.</summary>
    [Fact]
    public async Task TimescaleStore_ARecordedHoleOnADoneDatabase_IsStillDug_WithoutAFloorRead()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await CreateStoreAsync(timescale: true, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        var floor = await WaitForFloorClearOfAChunkBoundaryAsync(ct);
        await SeedAsync(connection, Server, "holed_db", floor.AddMinutes(-1), ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        var backfill = new QueryStoreBackfill(postgres, runner, new CollectorDeltaCalculator(), logger: null, hasContinuousAggregates: () => true);
        await runner.SaveCollectorStateAsync(Server, QueryStoreBackfill.StateCollectorName, new Dictionary<string, string>(StringComparer.Ordinal)
        {
            [QueryStoreBackfillState.DoneKeyPrefix + "holed_db"] = DateTime.UtcNow.ToString("o", CultureInfo.InvariantCulture),
            [QueryStoreBackfillState.HoleKeyPrefix + "holed_db"] = QueryStoreBackfillState.EncodeHole(floor.AddHours(-2), floor.AddHours(1)),
        }, ct);
        var server = ServerAt(RefusedConnectionString());
        var (readA, readB, baseA, baseB) = await CountFloorReadsAsync(backfill, scratch.DatabaseName, floor, ct);
        using var readAOwner = readA;
        using var readBOwner = readB;

        var stopwatch = Stopwatch.StartNew();
        await Assert.ThrowsAsync<SqlException>(() => backfill.RunServerSliceAsync(server, ct));
        TestContext.Current.SendDiagnosticMessage("QS4662_HOLE_SLICE_REFUSED_SECONDS=" + stopwatch.Elapsed.TotalSeconds.ToString("F2", CultureInfo.InvariantCulture));

        Assert.Equal(0, readA.Count - baseA);
        Assert.Equal(0, readB.Count - baseB);
    }

    /// <summary>Pin 7. On plain PostgreSQL the walk costs one index seek per database, not a scan of the table: the
    /// recursive term's inner select is a Limit over an Index (Only) Scan of <c>query_store_stats</c>, and nothing in
    /// the plan is a Seq Scan. The store carries the index the worker's start step creates (PgTableTuning), the table
    /// holds 30,000 rows of two servers and five names, and it is ANALYZEd, so the index is the planner's own choice
    /// rather than a forced one. The plan is EXPLAIN ANALYZE, not plain EXPLAIN: on PostgreSQL 18.6 the plain form
    /// leaves the correlated select out of the recursive term's plan (it shows only the work table scan), and the
    /// analyzed form runs the walk, which is six index seeks.</summary>
    [Fact]
    public async Task PlainStore_TheWalksInnerSelect_IsAnIndexSeekUnderALimit_NotASeqScan()
    {
        var ct = TestContext.Current.CancellationToken;
        var (scratch, connection) = await CreateStoreAsync(timescale: false, ct);
        await using var scratchOwner = scratch;
        await using var connectionOwner = connection;

        await PgTableTuning.ApplyAsync(connection, null, ct);
        await ExecAsync(connection, @"
INSERT INTO collect.query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, module_name, query_hash,
     query_id, plan_id, execution_type_desc, replica_role,
     runtime_stats_interval_id, interval_start_time_utc, first_execution_time, last_execution_time,
     execution_count, avg_duration_us, avg_cpu_time_us, min_duration_us, max_duration_us)
SELECT 4662500000 + row_number() OVER (), x.t, s.server_id, 'SQL01', d.name, 'dbo.GetOrders', '0xABCD',
       g, g, 'Regular', 'Primary', 1, x.t, x.t, x.t, 1, 100, 100, 100, 100
FROM (VALUES (-466201), (-466202)) AS s(server_id)
CROSS JOIN (VALUES ('db_a'), ('db_b'), ('db_c'), ('db_d'), ('db_e')) AS d(name)
CROSS JOIN generate_series(1, 3000) AS g
CROSS JOIN LATERAL (SELECT TIMESTAMP '2026-06-15 09:00:00' + g * INTERVAL '1 second') AS x(t)", ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_stats", ct);

        await using var explain = new NpgsqlCommand(
            "EXPLAIN (ANALYZE, FORMAT JSON, COSTS OFF, TIMING OFF, SUMMARY OFF) " + QueryStoreBackfill.WalkCandidateSql.Replace("$1", Server.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal),
            connection);
        var planJson = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var plan = JsonDocument.Parse(planJson);
        var root = plan.RootElement[0].GetProperty("Plan");

        static string NodeType(JsonElement node) => node.GetProperty("Node Type").GetString()!;
        static IEnumerable<JsonElement> Nodes(JsonElement node)
        {
            yield return node;
            if (node.TryGetProperty("Plans", out var children))
            {
                foreach (var child in children.EnumerateArray())
                {
                    foreach (var descendant in Nodes(child))
                    {
                        yield return descendant;
                    }
                }
            }
        }

        var recursiveUnion = Nodes(root).Single(node => NodeType(node) == "Recursive Union");
        var recursiveTerm = recursiveUnion.GetProperty("Plans").EnumerateArray()
            .Single(child => child.GetProperty("Parent Relationship").GetString() == "Inner");
        var innerLimits = Nodes(recursiveTerm).Where(node => NodeType(node) == "Limit").ToList();
        var innerScans = innerLimits
            .SelectMany(limit => limit.GetProperty("Plans").EnumerateArray())
            .Where(child => NodeType(child) is "Index Scan" or "Index Only Scan")
            .ToList();

        Assert.False(Nodes(root).Any(node => NodeType(node) == "Seq Scan"), "the walk must not scan the table:\n" + planJson);
        Assert.True(innerScans.Count >= 1,
            "the recursive term's inner select must be a Limit over an Index (Only) Scan:\n" + planJson);
    }
}
