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
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another test's
   rows, so it is deliberately NOT [Collection("live-postgres")]. The scratch database is dropped on disposal, which is
   the cleanup. */

/// <summary>
/// #8570: two statements of the analysis fact reads that grew with the size of the monitored server and not with the
/// window. The database-size probe compared every file of the probed snapshots with every file of the newest one (2.1
/// seconds at 4,000 files), and the sp_configure trace read walked every default-trace event the server had kept. Each
/// test runs the shipped statement and its pre-#8570 form over the same rows: the answer must be equal, and the plan
/// must have the new shape (no pairwise join, a fraction of the blocks). #8980 added the probe's chunk test: its
/// dropped_out arm had no time bound and read every file_io_stats chunk of the server. #9049 covers both database-size
/// statements over a lookback that lies in compressed chunks: each returns the rows of the snapshots it names, not the
/// rows of the lookback, and plans no chunk outside the lookback.
/// </summary>
public sealed class AnalysisReadsAtScaleLiveTests
{
    /// <summary>The database-size probe as shipped before #8570 (the dropped_out column's NOT EXISTS over <c>cur</c>). An oracle, never run by the product.</summary>
    private const string PairwiseDatabaseSizeNewestSql = @"
WITH newest AS (
    SELECT collection_time
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   size_mb > 0
    ORDER BY collection_time DESC
    LIMIT 1
),
cur AS (
    SELECT DISTINCT ON (database_name, file_name) database_name, file_name, size_mb
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT collection_time FROM newest)
    AND   size_mb > 0
    ORDER BY database_name, file_name
),
edge AS (
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time DESC
        LIMIT 1
    )
    UNION
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time ASC
        LIMIT 1
    )
)
SELECT
    (SELECT SUM(size_mb) FROM cur) AS total_size_mb,
    EXISTS (
        SELECT 1
        FROM file_io_stats AS f
        WHERE f.server_id = $1
        AND   f.collection_time IN (SELECT collection_time FROM edge)
        AND   f.size_mb > 0
        AND   NOT EXISTS (
            SELECT 1
            FROM cur AS c
            WHERE c.database_name = f.database_name
            AND   c.file_name = f.file_name
        )
    ) AS dropped_out";

    /// <summary>The database-size probe as shipped before #8980 (the dropped_out arm matched the edge times with no time bound of its own). An oracle, never run by the product.</summary>
    private const string UnboundedDatabaseSizeNewestSql = @"
WITH newest AS (
    SELECT collection_time
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   size_mb > 0
    ORDER BY collection_time DESC
    LIMIT 1
),
cur AS (
    SELECT DISTINCT ON (database_name, file_name) database_name, file_name, size_mb
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT collection_time FROM newest)
    AND   size_mb > 0
    ORDER BY database_name, file_name
),
edge AS (
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time DESC
        LIMIT 1
    )
    UNION
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time ASC
        LIMIT 1
    )
)
SELECT
    (SELECT SUM(size_mb) FROM cur) AS total_size_mb,
    EXISTS (
        SELECT f.database_name, f.file_name
        FROM file_io_stats AS f
        WHERE f.server_id = $1
        AND   f.collection_time IN (SELECT collection_time FROM edge)
        AND   f.size_mb > 0
        EXCEPT
        SELECT c.database_name, c.file_name
        FROM cur AS c
    ) AS dropped_out";

    /// <summary>The database-size probe as shipped before #9049 (cur with no time bound of its own, and the dropped_out arm matching the edge
    /// times through a join to a CTE of them). An oracle, never run by the product.</summary>
    private const string EdgeJoinDatabaseSizeNewestSql = @"
WITH newest AS (
    SELECT collection_time
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   size_mb > 0
    ORDER BY collection_time DESC
    LIMIT 1
),
cur AS (
    SELECT DISTINCT ON (database_name, file_name) database_name, file_name, size_mb
    FROM file_io_stats
    WHERE server_id = $1
    AND   collection_time = (SELECT collection_time FROM newest)
    AND   size_mb > 0
    ORDER BY database_name, file_name
),
edge AS (
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time DESC
        LIMIT 1
    )
    UNION
    (
        SELECT collection_time
        FROM file_io_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time < (SELECT collection_time FROM newest)
        AND   size_mb > 0
        ORDER BY collection_time ASC
        LIMIT 1
    )
)
SELECT
    (SELECT SUM(size_mb) FROM cur) AS total_size_mb,
    EXISTS (
        SELECT f.database_name, f.file_name
        FROM file_io_stats AS f
        WHERE f.server_id = $1
        AND   f.collection_time >= $2
        AND   f.collection_time <= $3
        AND   f.collection_time IN (SELECT collection_time FROM edge)
        AND   f.size_mb > 0
        EXCEPT
        SELECT c.database_name, c.file_name
        FROM cur AS c
    ) AS dropped_out";

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static async Task<(ScratchPostgres Scratch, NpgsqlConnection Connection)> OpenAsync(CancellationToken ct)
    {
        var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        return (scratch, connection);
    }

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static async Task<JsonDocument> ExplainAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var explain = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, c);
        foreach (var p in parameters)
        {
            explain.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        return JsonDocument.Parse((string)(await explain.ExecuteScalarAsync(ct))!);
    }

    /// <summary>The pages the plan's scans read (#8570), not the root node's buffer total: the total also counts the catalog buffers a backend
    /// reads to rebuild a cache entry, and a cache reset from another test creating a database on the same server brings those back.</summary>
    private static long Blocks(JsonDocument explain) => ExplainBuffers.DataPages(explain.RootElement[0].GetProperty("Plan"));

    /// <summary>True when any node of the plan is a nested loop of the given join type.</summary>
    private static bool HasNestedLoop(JsonElement node, string joinType)
    {
        if (node.TryGetProperty("Node Type", out var type) && type.GetString() == "Nested Loop"
            && node.TryGetProperty("Join Type", out var join) && join.GetString() == joinType)
        {
            return true;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                if (HasNestedLoop(child, joinType))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static async Task<(double? Total, bool DroppedOut)> RunProbeAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (reader.IsDBNull(0) ? null : Convert.ToDouble(reader.GetValue(0)), reader.GetBoolean(1));
    }

    [Fact]
    public async Task TheDatabaseSizeProbe_HashesTheFileKeysInsteadOfComparingEveryPair_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-size probe test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;

        const int serverId = -857_001;
        const int files = 1_500;
        var end = LiveClock.AnchoredEnd();

        /* Three snapshots of one server: the newest, the one before it, and the oldest of the 24-hour lookback; every file
           in each of them, so the probe finds nothing dropped out. */
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
SELECT 857_000_000_000 + s.k * 100_000 + f, s.t, $1, 'scale', 'Db' || (f / 2), 'File' || f, 'ROWS', 10 + f
FROM (VALUES (1, $2::timestamp - interval '1 minute'), (2, $2::timestamp - interval '2 minutes'), (3, $2::timestamp - interval '23 hours 50 minutes')) AS s (k, t)
CROSS JOIN generate_series(0, $3 - 1) AS f", ct, serverId, end, files);
        await ExecAsync(c, "ANALYZE file_io_stats", ct);

        object[] parameters = [serverId, end.AddHours(-24), end];
        var shipped = await RunProbeAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        var oracle = await RunProbeAsync(c, PairwiseDatabaseSizeNewestSql, parameters, ct);
        Assert.Equal(oracle, shipped);
        Assert.False(shipped.DroppedOut);

        /* A file that only the oldest snapshot holds: dropped out, in both. */
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
VALUES (857_900_000_001, $2, $1, 'scale', 'GoneDb', 'GoneFile', 'ROWS', 5)", ct, serverId, end.AddMinutes(-(23 * 60 + 50)));
        await ExecAsync(c, "ANALYZE file_io_stats", ct);
        var shippedGone = await RunProbeAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        var oracleGone = await RunProbeAsync(c, PairwiseDatabaseSizeNewestSql, parameters, ct);
        Assert.Equal(oracleGone, shippedGone);
        Assert.True(shippedGone.DroppedOut);

        /* The shape: the keys are hashed once. The pairwise form is a nested loop anti join with one comparison per pair of files. */
        using var newPlan = await ExplainAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        using var oldPlan = await ExplainAsync(c, PairwiseDatabaseSizeNewestSql, parameters, ct);
        Assert.True(HasNestedLoop(oldPlan.RootElement[0].GetProperty("Plan"), "Anti"),
            "the pre-#8570 probe is expected to plan a nested loop anti join at this size; if it no longer does, the plant has stopped showing the defect: " + oldPlan.RootElement);
        Assert.False(HasNestedLoop(newPlan.RootElement[0].GetProperty("Plan"), "Anti"),
            "the probe compares the probed files with the newest snapshot's pair by pair: " + newPlan.RootElement);
    }

    /// <summary>
    /// #8980: the probe's dropped_out arm matched the edge snapshots' times with an IN over the edge CTE and had no time
    /// bound of its own. TimescaleDB cannot exclude a chunk on that, so the arm read, and decompressed, every
    /// file_io_stats chunk the server had, and the cost grew with the table's retention. Both edge times lie inside the
    /// lookback, so the lookback bound changes no answer and lets the planner exclude every chunk that ends before it.
    /// </summary>
    [Fact]
    public async Task TheDatabaseSizeProbe_ReadsNoChunkThatEndsBeforeTheLookback_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-size chunk test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await ExecAsync(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);

        /* Migration leaves the tables plain (#7152): the service's conversion step makes file_io_stats a hypertable, and
           the store compresses its old chunks. */
        await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
        await ExecAsync(c, TimescaleSupport.EnableCompressionSql(FileIoStatsCollector.Instance), ct);

        const int serverId = -857_003;
        var end = LiveClock.AnchoredEnd();
        var lookbackStart = end.AddHours(-24);

        /* Ten days of snapshots of one server, every 15 minutes, 40 files each, so most of the server's chunks end before
           the 24-hour lookback. Those chunks are compressed, as the store's policy leaves them. */
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
SELECT 857_300_000_000 + s.k * 100 + f, $2::timestamp - interval '1 minute' - s.k * interval '15 minutes', $1, 'scale',
       'Db' || (f / 2), 'File' || f, 'ROWS', 10 + f
FROM generate_series(0, 959) AS s (k)
CROSS JOIN generate_series(0, 39) AS f", ct, serverId, end);
        await ExecAsync(c, @"
SELECT compress_chunk(ch, if_not_compressed => true)
FROM show_chunks('file_io_stats', older_than => $1::timestamp) AS ch", ct, lookbackStart);
        await ExecAsync(c, "ANALYZE file_io_stats", ct);

        var outside = new HashSet<string>(StringComparer.Ordinal);
        await using (var chunks = new NpgsqlCommand(@"
SELECT cl.relname
FROM show_chunks('file_io_stats', older_than => $1::timestamp) AS ch
JOIN pg_class AS cl ON cl.oid = ch", c))
        {
            chunks.Parameters.AddWithValue(Naive(lookbackStart));
            await using var reader = await chunks.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                outside.Add(reader.GetString(0));
            }
        }

        Assert.True(outside.Count >= 5, "the plant should leave several chunks wholly before the lookback, found " + outside.Count);

        object[] parameters = [serverId, lookbackStart, end];
        var shipped = await RunProbeAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        var oracle = await RunProbeAsync(c, UnboundedDatabaseSizeNewestSql, parameters, ct);
        Assert.Equal(oracle, shipped);
        Assert.False(shipped.DroppedOut);

        /* A file that only the oldest snapshot of the lookback holds: the arm must still find it. */
        var oldestInLookback = end.AddMinutes(-1 - (95 * 15));
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
VALUES (857_390_000_001, $2, $1, 'scale', 'GoneDb', 'GoneFile', 'ROWS', 5)", ct, serverId, oldestInLookback);
        await ExecAsync(c, "ANALYZE file_io_stats", ct);
        var shippedGone = await RunProbeAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        var oracleGone = await RunProbeAsync(c, UnboundedDatabaseSizeNewestSql, parameters, ct);
        Assert.Equal(oracleGone, shippedGone);
        Assert.True(shippedGone.DroppedOut);

        using var newPlan = await ExplainAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
        using var oldPlan = await ExplainAsync(c, UnboundedDatabaseSizeNewestSql, parameters, ct);
        var oldRead = ExecutedRelationNames(oldPlan.RootElement[0].GetProperty("Plan")).Where(outside.Contains).ToList();
        var newRead = ExecutedRelationNames(newPlan.RootElement[0].GetProperty("Plan")).Where(outside.Contains).ToList();
        Assert.True(oldRead.Count > 0,
            "the pre-#8980 probe is expected to read chunks that end before the lookback; if it no longer does, the plant has stopped showing the defect: " + oldPlan.RootElement);
        Assert.True(newRead.Count == 0,
            "the probe reads chunks that end before the lookback: " + string.Join(", ", newRead));
    }

    /// <summary>Every relation an executed plan node reads, the chunks under a ChunkAppend or a columnar scan among them. A node
    /// the executor never ran (a chunk excluded at run time) reports zero loops and is left out.</summary>
    private static IEnumerable<string> ExecutedRelationNames(JsonElement node)
    {
        if (node.TryGetProperty("Actual Loops", out var loops) && loops.GetDouble() == 0)
        {
            yield break;
        }

        if (node.TryGetProperty("Relation Name", out var name) && name.GetString() is { } relation)
        {
            yield return relation;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                foreach (var nested in ExecutedRelationNames(child))
                {
                    yield return nested;
                }
            }
        }
    }

    /// <summary>Every relation the plan names, the chunks the executor never ran among them: a chunk the planner could not exclude is
    /// planned, and costs planning time, whether or not a run-time test then skips it.</summary>
    private static IEnumerable<string> PlannedRelationNames(JsonElement node)
    {
        if (node.TryGetProperty("Relation Name", out var name) && name.GetString() is { } relation)
        {
            yield return relation;
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                foreach (var nested in PlannedRelationNames(child))
                {
                    yield return nested;
                }
            }
        }
    }

    private static async Task<HashSet<string>> ChunkNamesAsync(NpgsqlConnection c, string showChunks, CancellationToken ct, params DateTime[] parameters)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        await using var cmd = new NpgsqlCommand("SELECT cl.relname FROM " + showChunks + " AS ch JOIN pg_class AS cl ON cl.oid = ch", c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(Naive(p));
        }

        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    private static async Task<double?> RunSizeAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        var value = await cmd.ExecuteScalarAsync(ct);
        return value is null or DBNull ? null : Convert.ToDouble(value);
    }

    /// <summary>
    /// #9049: both database-size statements return the rows of the snapshots they name, not the rows of the lookback, and
    /// plan no chunk outside it. The lookback here lies in compressed chunks, as it does for any window that ends more
    /// than a day in the past. There a join to a CTE of the edge times is a hash join over every row of the lookback,
    /// and a read that numbers every row of the lookback to keep the newest per file sorts them all: 6.4 million rows
    /// for a server with 4,471 files sampled each minute. Each shipped statement runs beside that form over the same
    /// rows, for a server that kept every file and for one that lost a file after the lookback's oldest snapshot.
    /// </summary>
    [Fact]
    public async Task TheDatabaseSizeStatements_ReadTheSnapshotsTheyName_NotTheLookback_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live database-size snapshot test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;
        Assert.SkipUnless(scratch.TimescaleEnabled, "TimescaleDB is not enabled on this cluster.");
        await ExecAsync(c, "SELECT _timescaledb_functions.stop_background_workers()", ct);
        await TimescaleSupport.ConvertToHypertablesAsync(c, null, ct);
        await ExecAsync(c, TimescaleSupport.EnableCompressionSql(FileIoStatsCollector.Instance), ct);

        const int steadyServer = -904_911;
        const int droppedServer = -904_912;
        const int files = 40;
        var newestPlanted = LiveClock.AnchoredEnd();
        var end = newestPlanted.AddDays(-4);
        var lookbackStart = end.AddHours(-24);

        /* Ten days of snapshots of two servers, every 15 minutes, 40 files each. The window ends four days before the
           newest snapshot, so the servers have chunks on both sides of its lookback, and 96 snapshots inside it. The
           second server has one more file, in the lookback's oldest snapshot only. Every chunk is compressed, as the
           store's policy leaves the chunks of a window that old. */
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
SELECT 904_900_000_000 + sv.n * 1_000_000 + s.k * 100 + f, $1::timestamp - interval '1 minute' - s.k * interval '15 minutes', sv.server_id, 'scale',
       'Db' || (f / 2), 'File' || f, 'ROWS', 10 + f
FROM (VALUES (1, $2::int), (2, $3::int)) AS sv (n, server_id)
CROSS JOIN generate_series(0, 959) AS s (k)
CROSS JOIN generate_series(0, $4 - 1) AS f", ct, newestPlanted, steadyServer, droppedServer, files);
        await ExecAsync(c, @"
INSERT INTO file_io_stats (collection_id, collection_time, server_id, server_name, database_name, file_name, file_type, size_mb)
VALUES (904_990_000_001, $2, $1, 'scale', 'GoneDb', 'GoneFile', 'ROWS', 5)", ct, droppedServer, end.AddMinutes(-1 - (95 * 15)));
        await ExecAsync(c, "SELECT compress_chunk(ch, if_not_compressed => true) FROM show_chunks('file_io_stats') AS ch", ct);
        await ExecAsync(c, "ANALYZE file_io_stats", ct);

        var chunks = await ChunkNamesAsync(c, "show_chunks('file_io_stats')", ct);
        var outside = await ChunkNamesAsync(c, "show_chunks('file_io_stats', older_than => $1::timestamp)", ct, lookbackStart);
        var after = await ChunkNamesAsync(c, "show_chunks('file_io_stats', newer_than => $1::timestamp)", ct, end);
        Assert.True(outside.Count >= 3 && after.Count >= 3,
            $"the plant should leave several chunks wholly before the lookback and several wholly after the window, found {outside.Count} and {after.Count}");
        outside.UnionWith(after);

        /* The lookback holds 96 snapshots of the server's files. A statement that reads the snapshots it names returns three
           snapshots' rows at most (the newest, the one before it, the oldest; one of them with the extra file) and the three
           one-row reads that find their times. */
        const long lookbackRows = 96L * files;
        const long snapshotRows = (3L * (files + 1)) + 3;

        foreach (var serverId in new[] { steadyServer, droppedServer })
        {
            var dropped = serverId == droppedServer;
            object[] parameters = [serverId, lookbackStart, end];

            var probe = await RunProbeAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
            Assert.Equal(await RunProbeAsync(c, EdgeJoinDatabaseSizeNewestSql, parameters, ct), probe);
            Assert.Equal((1180d, dropped), probe);

            var size = await RunSizeAsync(c, PgFactCollector.DatabaseSizeSql, parameters, ct);
            Assert.Equal(await RunSizeAsync(c, NewestSnapshotReadsLiveTests.FullLookbackDatabaseSizeSql, parameters, ct), size);
            Assert.Equal(dropped ? 1185d : 1180d, size);

            using var probePlan = await ExplainAsync(c, PgFactCollector.DatabaseSizeNewestSql, parameters, ct);
            using var joinPlan = await ExplainAsync(c, EdgeJoinDatabaseSizeNewestSql, parameters, ct);
            using var sizePlan = await ExplainAsync(c, PgFactCollector.DatabaseSizeSql, parameters, ct);
            using var lookbackPlan = await ExplainAsync(c, NewestSnapshotReadsLiveTests.FullLookbackDatabaseSizeSql, parameters, ct);

            var joinRows = ExplainBuffers.RowsReadAtMost(joinPlan.RootElement.GetRawText(), chunks);
            var probeRows = ExplainBuffers.RowsReadAtLeast(probePlan.RootElement.GetRawText(), chunks);
            Assert.True(joinRows >= lookbackRows,
                $"the pre-#9049 probe is expected to return every row of the lookback from its chunk scans ({lookbackRows}); if it no longer does, the plant has stopped showing the defect: {joinRows} rows, {joinPlan.RootElement}");
            Assert.True(probeRows <= snapshotRows,
                $"the probe returns more rows from its chunk scans than the snapshots it names hold ({snapshotRows}): {probeRows} rows, {probePlan.RootElement}");

            var lookbackReadRows = ExplainBuffers.RowsReadAtMost(lookbackPlan.RootElement.GetRawText(), chunks);
            var sizeRows = ExplainBuffers.RowsReadAtLeast(sizePlan.RootElement.GetRawText(), chunks);
            Assert.True(lookbackReadRows >= lookbackRows,
                $"the read of every row in the lookback is expected to return them all from its chunk scans ({lookbackRows}): {lookbackReadRows} rows, {lookbackPlan.RootElement}");
            Assert.True(sizeRows <= snapshotRows,
                $"the full read returns more rows from its chunk scans than the snapshots it names hold ({snapshotRows}): {sizeRows} rows, {sizePlan.RootElement}");

            var joinPlanned = PlannedRelationNames(joinPlan.RootElement[0].GetProperty("Plan")).Where(outside.Contains).Distinct().ToList();
            var probePlanned = PlannedRelationNames(probePlan.RootElement[0].GetProperty("Plan")).Where(outside.Contains).Distinct().ToList();
            var sizePlanned = PlannedRelationNames(sizePlan.RootElement[0].GetProperty("Plan")).Where(outside.Contains).Distinct().ToList();
            Assert.True(joinPlanned.Count > 0,
                "the pre-#9049 probe is expected to plan chunks outside the lookback; if it no longer does, the plant has stopped showing the defect: " + joinPlan.RootElement);
            Assert.True(probePlanned.Count == 0,
                "the probe plans chunks outside the lookback: " + string.Join(", ", probePlanned));
            Assert.True(sizePlanned.Count == 0,
                "the full read plans chunks outside the lookback: " + string.Join(", ", sizePlanned));
        }
    }

    [Fact]
    public async Task TheReconfigureTraceRead_ReadsTheSpansEvents_NotTheServersWholeTraceHistory_AgainstDevPostgres()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live reconfigure trace test.");
        var ct = TestContext.Current.CancellationToken;
        var (scratch, c) = await OpenAsync(ct);
        await using var scratchLease = scratch;
        await using var connectionLease = c;

        const int serverId = -857_002;
        var end = LiveClock.AnchoredEnd();
        var start = end.AddHours(-4);

        /* Thirty days of default-trace events for this server and another, collected a minute after they happened, one in
           two hundred an sp_configure line (msg 15457). */
        await ExecAsync(c, @"
INSERT INTO default_trace_events (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, error_number, text_data)
SELECT 857_000_000_000 + n, t, $1 + (n % 2), 'scale', t - interval '1 minute', 'ErrorLog',
       CASE WHEN n % 200 = 0 THEN 15457 ELSE 18453 END, 'row ' || n
FROM (SELECT n, $2::timestamp - (n % 43200) * interval '1 minute' AS t FROM generate_series(1, 90000) AS n) AS s", ct, serverId, end);
        /* Two lines the bound must keep: one collected nine hours before its event time (a monitored server's clock running
           fast), one collected two hours after it (a collector that ran late). */
        await ExecAsync(c, @"
INSERT INTO default_trace_events (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, error_number, text_data)
VALUES (857_900_000_001, $2::timestamp - interval '12 hours', $1, 'scale', $2::timestamp - interval '3 hours', 'ErrorLog', 15457, 'fast clock'),
       (857_900_000_002, $2::timestamp, $1, 'scale', $2::timestamp - interval '2 hours', 'ErrorLog', 15457, 'late collector')", ct, serverId, end);
        await ExecAsync(c, "ANALYZE default_trace_events", ct);

        /* The pre-#8570 read: the shipped text with the redundant collection_time bound taken out. */
        var oldSql = Regex.Replace(DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql,
            @"AND\s+dte\.collection_time >= \$2 - interval '1 day'", string.Empty);
        Assert.NotEqual(DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql, oldSql);

        object[] parameters = [serverId, start, end];
        var newRows = await ReadLinesAsync(c, DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql, parameters, ct);
        var oldRows = await ReadLinesAsync(c, oldSql, parameters, ct);
        Assert.Equal(oldRows.Order(StringComparer.Ordinal), newRows.Order(StringComparer.Ordinal));
        Assert.Contains("fast clock", newRows);
        Assert.Contains("late collector", newRows);
        Assert.True(newRows.Count >= 4, "the plant should put several sp_configure lines in the span: " + string.Join(", ", newRows));

        using var newPlan = await ExplainAsync(c, DarlingAnalysisService.ReconfigureTraceLinesForAttributionSql, parameters, ct);
        using var oldPlan = await ExplainAsync(c, oldSql, parameters, ct);
        var newBlocks = Blocks(newPlan);
        var oldBlocks = Blocks(oldPlan);
        Assert.True(newBlocks * 4 <= oldBlocks,
            $"the read should touch a quarter of the pages the whole-history read does or less: old={oldBlocks} new={newBlocks}");
    }

    private static async Task<List<string>> ReadLinesAsync(NpgsqlConnection c, string sql, object[] parameters, CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var p in parameters)
        {
            cmd.Parameters.AddWithValue(p is DateTime dt ? Naive(dt) : p);
        }

        var rows = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(reader.GetString(1));
        }

        return rows;
    }
}
