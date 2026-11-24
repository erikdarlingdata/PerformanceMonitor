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
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #9452: <see cref="OversizedPlanBacklog.RecordSightingsSql"/> counts a server's pending rows on every collection cycle. Through the
/// server index it visited the heap row of every row the server has. V310 adds a partial index of the pending rows alone, so the count
/// reads that index. The gate's second test needs <c>last_seen_at</c>, which the index must not hold, so it is read only when the first
/// test is already over the cap. The skip decision must stay exactly what it was, and a re-sighting must stay a heap-only update.
/// </summary>
public sealed class OversizedPlanBacklogPendingIndexLivePostgresTests
{
    private const int RungVersion = OversizedPlanBacklog.PendingIndexVersion;
    private const string IndexName = OversizedPlanBacklog.PendingIndexName;
    private const int Cap = 100;
    private const int Limit = 200;
    private const int ForeignServer = 4999;
    private const string BacklogTable = "collect.oversized_plan_backlog";

    private static readonly DateTime SeenAt = new(2026, 11, 14, 12, 0, 0, DateTimeKind.Unspecified);

    /// <summary>$3 rows for server $1, keyed by the label $2, sighted at $4, with the given capture ($5), expiry ($6) and attempt ($7) stamps.</summary>
    private const string SeedSql = @"
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, captured_at, expired_at, last_attempt_at)
SELECT $1, 'query_stats', $2 || '_PLAN_' || n, $2 || '_SQL_' || n, 0, 400, 'index_db', NULL, 600000 + (n * 7919) % 100000, $4, $4, $5, $6, $7
FROM generate_series(1, $3) AS n;";

    /// <summary>One shape of a server's rows and one cycle's sightings, and the decision the gate must reach.</summary>
    private sealed record GateCase(
        string Name,
        int Older,
        int AtOrAfter,
        int Captured,
        int Expired,
        int Tried,
        int ResightOlder,
        int ResightTried,
        int NewRows,
        bool Expected);

    /* Cap is 100. The gate over-caps only when BOTH tests pass: pending rows plus new rows is past the cap, and pending rows seen at or after
       the sighting time plus every incoming row is past it. Each pair of cases sits one row either side of a threshold. */
    private static readonly GateCase[] Cases =
    [
        new("first test exactly at the cap", 90, 0, 0, 0, 0, 0, 0, 10, false),
        new("first test one over, second far under", 90, 0, 0, 0, 0, 0, 0, 11, false),
        new("second test exactly at the cap", 50, 90, 0, 0, 0, 0, 0, 10, false),
        new("second test one over", 50, 91, 0, 0, 0, 0, 0, 10, true),
        new("re-sightings of pending rows, second exactly at the cap", 60, 60, 0, 0, 0, 40, 0, 0, false),
        new("re-sightings of pending rows, second one over", 60, 60, 0, 0, 0, 41, 0, 0, true),
        new("re-sightings of tried rows, first under", 0, 50, 0, 0, 70, 0, 70, 0, false),
        new("captured and expired rows are not pending", 40, 0, 60, 60, 0, 0, 0, 10, false),
        new("captured and expired rows do not make the first test pass", 95, 0, 60, 60, 0, 0, 0, 6, false),
        new("all pending rows recent, both over", 10, 100, 0, 0, 0, 0, 0, 1, true),
        new("noise rows beside a true decision", 50, 60, 30, 30, 20, 0, 0, 45, true),
    ];

    public static TheoryData<string> CaseNames()
    {
        var data = new TheoryData<string>();
        foreach (var gateCase in Cases)
        {
            data.Add(gateCase.Name);
        }

        return data;
    }

    private static bool Reference(GateCase c)
    {
        var pending = c.Older + c.AtOrAfter;
        var incoming = c.ResightOlder + c.ResightTried + c.NewRows;
        return pending + c.NewRows > Cap && c.AtOrAfter + incoming > Cap;
    }

    /// <summary>
    /// The decision of the gate is the AND of its two tests. A server's rows are laid out one row either side of each threshold, with
    /// rows that are not pending (captured, expired, tried) beside them and another server's pending rows beside those, and the gate must
    /// decide as the two counts say. The second test is read only when the first passes, and that must not change a decision.
    /// </summary>
    [Theory]
    [MemberData(nameof(CaseNames))]
    public async Task TheGate_DecidesAsTheTwoCountsSay(string caseName)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9452 gate test (it mints its own scratch database).");

        var gateCase = Cases.Single(c => c.Name == caseName);
        Assert.Equal(gateCase.Expected, Reference(gateCase));

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);
        const int server = 5001;
        await RegisterServersAsync(connection, [server, ForeignServer], ct);

        var olderAt = SeenAt.AddSeconds(-1);
        await SeedAsync(connection, ForeignServer, "FOREIGN", 300, SeenAt, null, null, null, ct);
        await SeedAsync(connection, server, "OLDER", gateCase.Older, olderAt, null, null, null, ct);
        await SeedAsync(connection, server, "RECENT", gateCase.AtOrAfter, SeenAt, null, null, null, ct);
        await SeedAsync(connection, server, "CAPTURED", gateCase.Captured, olderAt, SeenAt, null, null, ct);
        await SeedAsync(connection, server, "EXPIRED", gateCase.Expired, SeenAt, null, SeenAt, null, ct);
        await SeedAsync(connection, server, "TRIED", gateCase.Tried, SeenAt, null, null, SeenAt, ct);

        var sightings = new List<OversizedPlanObservation>();
        sightings.AddRange(Sightings("OLDER", gateCase.ResightOlder));
        sightings.AddRange(Sightings("TRIED", gateCase.ResightTried));
        sightings.AddRange(Sightings("NEW", gateCase.NewRows));

        await using var decision = new NpgsqlCommand(OversizedPlanBacklog.GateDecisionSql, connection);
        BindSightings(decision, server, sightings, Cap);
        Assert.Equal(gateCase.Expected, (bool)(await decision.ExecuteScalarAsync(ct))!);
    }

    private static IEnumerable<OversizedPlanObservation> Sightings(string label, int count) =>
        Enumerable.Range(1, count).Select(n => new OversizedPlanObservation(
            label + "_PLAN_" + n.ToString(CultureInfo.InvariantCulture), label + "_SQL_" + n.ToString(CultureInfo.InvariantCulture),
            0, 400, "index_db", null, 600000 + (n * 7919) % 100000));

    /// <summary>
    /// One server has about forty thousand rows, most of them pending, beside four servers of the same size. After a vacuum the pending
    /// count of the gate is an index-only scan of the new index, with almost no heap fetches and a few hundred buffers, and it does not
    /// read the server index. Without the new index the same statement reads thousands of buffers. The gate's second test is never
    /// executed when the first is under the cap, and is read from the heap when it is over.
    /// </summary>
    [Fact]
    public async Task ThePendingCount_IsAnIndexOnlyScanOfTheNewIndex_AndReadsFarFewerBuffersThanWithout()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9452 plan test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);
        const int server = 5001;
        var others = new[] { 5002, 5003, 5004, 5005 };
        await RegisterServersAsync(connection, others.Append(server).ToArray(), ct);

        var olderAt = SeenAt.AddMinutes(-30);
        await SeedAsync(connection, server, "PENDING", 36000, olderAt, null, null, null, ct);
        await SeedAsync(connection, server, "RECENT", 300, SeenAt, null, null, null, ct);
        await SeedAsync(connection, server, "TRIED", 2400, olderAt, null, null, SeenAt, ct);
        await SeedAsync(connection, server, "EXPIRED", 1600, olderAt, null, SeenAt, null, ct);
        foreach (var other in others)
        {
            await SeedAsync(connection, other, "PENDING", 40000, olderAt, null, null, null, ct);
        }

        await ExecAsync(connection, "VACUUM (ANALYZE) collect.oversized_plan_backlog;", ct);

        /* A cycle of five re-sightings of rows the server holds, and the cap far above its rows, so the gate's first test is under it. */
        var sightings = Sightings("PENDING", 5).ToList();

        var withIndex = await ExplainAsync(connection, server, sightings, 1_000_000, ct);
        Walk(withIndex.Plan, out var nodeTypes, out var indexNames);
        Assert.Contains(IndexName, indexNames);
        Assert.Contains("Index Only Scan", nodeTypes);
        Assert.True(IndexOnlyHeapFetches(withIndex.Plan) <= 50, "the pending count fetched heap rows after a vacuum: " + withIndex.Text);
        Assert.True(withIndex.Buffers < 300, $"the pending count read {withIndex.Buffers} buffers: " + withIndex.Text);
        TestContext.Current.SendDiagnosticMessage("#9452 plan with the index, first test under the cap, " + withIndex.Buffers + " buffers:\n" + withIndex.Text);

        var overCap = await ExplainAsync(connection, server, sightings, Cap, ct);
        TestContext.Current.SendDiagnosticMessage("#9452 plan with the index, first test over the cap, " + overCap.Buffers + " buffers:\n" + overCap.Text);

        /* A live store is not freshly vacuumed: nearly every re-sighting is a heap-only update, and each one clears its page's
           all-visible bit until the next autovacuum, so an index-only scan has to visit the heap for the rows on those pages. The
           bound of fifty heap fetches above holds for the vacuumed table only. Here a third of the server's pending rows are
           re-sighted through the service's own upsert, with no vacuum after, and the same count is read again. */
        const int resightedAfterVacuum = 12000;
        await using (var record = new NpgsqlCommand(OversizedPlanBacklog.RecordSightingsSql, connection))
        {
            BindSightings(record, server, Sightings("PENDING", resightedAfterVacuum).ToList(), 1_000_000, withDropLimit: true);
            Assert.Equal(0, Convert.ToInt32(await record.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
        }

        var dirty = await ExplainAsync(connection, server, sightings, 1_000_000, ct);
        Walk(dirty.Plan, out _, out var dirtyIndexNames);
        Assert.Contains(IndexName, dirtyIndexNames);
        TestContext.Current.SendDiagnosticMessage(
            "#9452 plan with the index after re-sighting a third of the pending rows and no vacuum, " + IndexOnlyHeapFetches(dirty.Plan) +
            " heap fetches, " + dirty.Buffers + " buffers:\n" + dirty.Text);

        await ExecAsync(connection, "DROP INDEX collect." + IndexName + ";", ct);
        var without = await ExplainAsync(connection, server, sightings, 1_000_000, ct);
        TestContext.Current.SendDiagnosticMessage("#9452 plan without the index, " + without.Buffers + " buffers:\n" + without.Text);
        Assert.True(without.Buffers > withIndex.Buffers * 5, $"without the index {without.Buffers} buffers, with it {withIndex.Buffers}");
        TestContext.Current.SendDiagnosticMessage("#9452 buffers: vacuumed with the index " + withIndex.Buffers + ", re-sighted with the index " + dirty.Buffers + ", without the index " + without.Buffers);
    }

    private sealed record Explained(JsonElement Plan, string Text, long Buffers);

    private static async Task<Explained> ExplainAsync(
        NpgsqlConnection connection, int server, List<OversizedPlanObservation> sightings, int cap, CancellationToken ct)
    {
        await using var json = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + OversizedPlanBacklog.GateDecisionSql, connection);
        BindSightings(json, server, sightings, cap);
        var document = JsonDocument.Parse((string)(await json.ExecuteScalarAsync(ct))!);
        var plan = document.RootElement[0].GetProperty("Plan").Clone();
        var buffers = plan.GetProperty("Shared Hit Blocks").GetInt64() + plan.GetProperty("Shared Read Blocks").GetInt64();

        await using var text = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS) " + OversizedPlanBacklog.GateDecisionSql, connection);
        BindSightings(text, server, sightings, cap);
        var lines = new List<string>();
        await using (var reader = await text.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                lines.Add(reader.GetString(0));
            }
        }

        return new Explained(plan, string.Join("\n", lines), buffers);
    }

    private static void Walk(JsonElement node, out List<string> nodeTypes, out List<string> indexNames)
    {
        nodeTypes = [];
        indexNames = [];
        WalkInto(node, nodeTypes, indexNames, executedOnly: true);
    }

    /// <summary>Collects the node types and index names of the nodes that ran. A node under a branch that never ran has no loops.</summary>
    private static void WalkInto(JsonElement node, List<string> nodeTypes, List<string> indexNames, bool executedOnly)
    {
        if (executedOnly && node.TryGetProperty("Actual Loops", out var loops) && loops.GetInt64() == 0)
        {
            return;
        }

        nodeTypes.Add(node.GetProperty("Node Type").GetString()!);
        if (node.TryGetProperty("Index Name", out var index))
        {
            indexNames.Add(index.GetString()!);
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                WalkInto(child, nodeTypes, indexNames, executedOnly);
            }
        }
    }

    private static long IndexOnlyHeapFetches(JsonElement node)
    {
        long fetches = 0;
        if (node.GetProperty("Node Type").GetString() == "Index Only Scan" && node.TryGetProperty("Heap Fetches", out var heap))
        {
            fetches += heap.GetInt64();
        }

        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                fetches += IndexOnlyHeapFetches(child);
            }
        }

        return fetches;
    }

    /// <summary>
    /// A re-sighting of a waiting row writes <c>last_seen_at</c> and nothing an index holds, so it must stay a heap-only update. The
    /// statement runs inside a transaction, and the table's heap-only update count for that transaction goes up by one for each row
    /// it re-sighted, the same as its update count. An index on <c>last_seen_at</c>, or with it as an INCLUDE column, ends that.
    /// </summary>
    [Fact]
    public async Task AReSightingOfAWaitingRow_StaysAHeapOnlyUpdate()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9452 heap-only test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);
        const int server = 5001;
        const int resighted = 5;
        await RegisterServersAsync(connection, [server], ct);

        /* Twenty short rows leave room on their page for the new versions of five of them. */
        await SeedAsync(connection, server, "PENDING", 20, SeenAt.AddMinutes(-30), null, null, null, ct);

        await using var transaction = await connection.BeginTransactionAsync(ct);
        var hotBefore = await XactCounterAsync(connection, transaction, "pg_stat_get_xact_tuples_hot_updated", ct);
        var updatedBefore = await XactCounterAsync(connection, transaction, "pg_stat_get_xact_tuples_updated", ct);

        var sightings = Sightings("PENDING", resighted).ToList();
        await using (var record = new NpgsqlCommand(OversizedPlanBacklog.RecordSightingsSql, connection, transaction))
        {
            BindSightings(record, server, sightings, 1_000_000, withDropLimit: true);
            /* The statement returns how many new rows it did not insert: none here, every sighting is of a row the table holds. */
            Assert.Equal(0, Convert.ToInt32(await record.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture));
        }

        Assert.Equal(updatedBefore + resighted, await XactCounterAsync(connection, transaction, "pg_stat_get_xact_tuples_updated", ct));
        Assert.Equal(hotBefore + resighted, await XactCounterAsync(connection, transaction, "pg_stat_get_xact_tuples_hot_updated", ct));
        await transaction.RollbackAsync(ct);
    }

    private static async Task<long> XactCounterAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string function, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand($"SELECT {function}('{BacklogTable}'::regclass);", connection, transaction);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// A store the ladder migrated has the index: on the server alone, partial, so only a pending row is in it. It is the probe's
    /// sentinel for its rung: with it the probe names the rung, without it the rung below, and running the rung again makes it again.
    /// </summary>
    [Fact]
    public async Task AMigratedStore_HasThePendingIndex_AndTheProbeNamesItsRung()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #9452 rung test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);

        var index = await ReadIndexAsync(connection, ct);
        Assert.True(index is not null, $"{BacklogTable} has no index named {IndexName} on a store the ladder migrated");
        Assert.Equal(["server_id"], index!.KeyColumns);
        Assert.Equal(1, index.Columns);
        Assert.True(index.IsPartial, "the index holds every row of the table, the tried and dropped ones included");
        Assert.True(index.IsValid);
        Assert.False(index.IsUnique);
        Assert.Equal("oversized-plan-backlog-pending-index", PgMigrations.Scripts.Single(m => m.Version == RungVersion).Name);

        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion, ct);
        Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));

        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        Assert.Null(await ReadIndexAsync(connection, ct));
        Assert.Equal(RungVersion - 1, await ProbedVersionAsync(connection, ct));

        var rung = PgMigrations.Scripts.Single(m => m.Version == RungVersion).Sql;
        for (var run = 0; run < 2; run++)
        {
            await using var again = new NpgsqlCommand(rung, connection);
            await again.ExecuteNonQueryAsync(ct);
        }

        Assert.NotNull(await ReadIndexAsync(connection, ct));
        Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));
    }

    private sealed record IndexShape(bool IsValid, bool IsUnique, bool IsPartial, string[] KeyColumns, int Columns);

    private const string IndexSql = @"
SELECT
    i.indisvalid,
    i.indisunique,
    i.indpred IS NOT NULL,
    ARRAY
    (
        SELECT a.attname::text
        FROM unnest(i.indkey::smallint[]) WITH ORDINALITY AS k (attnum, ordinal)
        JOIN pg_attribute AS a
          ON  a.attrelid = i.indrelid
          AND a.attnum = k.attnum
        WHERE k.ordinal <= i.indnkeyatts
        ORDER BY k.ordinal
    ),
    i.indnatts::integer
FROM pg_index AS i
JOIN pg_class AS c
  ON c.oid = i.indexrelid
WHERE i.indrelid = 'collect.oversized_plan_backlog'::regclass
AND   c.relname = $1;";

    private static async Task<IndexShape?> ReadIndexAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(IndexSql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = IndexName });
        await using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return new IndexShape(reader.GetBoolean(0), reader.GetBoolean(1), reader.GetBoolean(2), reader.GetFieldValue<string[]>(3), reader.GetInt32(4));
    }

    /// <summary>The schema version the viewer's probe reads from this store.</summary>
    private static async Task<int> ProbedVersionAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var map = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var sentinels = new object[map.GetParameters().Length];

        await using var command = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(sentinels.Length, reader.FieldCount);
        for (var i = 0; i < sentinels.Length; i++)
        {
            sentinels[i] = reader.GetBoolean(i);
        }

        return (int)map.Invoke(null, sentinels)!;
    }

    /// <summary>
    /// The parameters <see cref="OversizedPlanBacklog.RecordSightingsAsync"/> binds, in its order. The gate's own statement leaves out the
    /// last one, the drop limit.
    /// </summary>
    private static void BindSightings(NpgsqlCommand command, int server, IReadOnlyList<OversizedPlanObservation> rows, int cap, bool withDropLimit = false)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = OversizedPlanBacklog.QueryStatsCollectorName });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.PlanHandle).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.SqlHandle).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = rows.Select(r => r.StatementStartOffset).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = rows.Select(r => r.StatementEndOffset).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.DatabaseName).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = rows.Select(r => r.QueryHash).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = rows.Select(r => r.ObservedBytes).ToArray() });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = SeenAt });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = cap });
        if (withDropLimit)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = Limit });
        }
    }

    private static async Task RegisterServersAsync(NpgsqlConnection connection, int[] servers, CancellationToken ct)
    {
        foreach (var server in servers)
        {
            await using var command = new NpgsqlCommand(
                "INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES ($1, 'pending-index-server-' || $1, TRUE);", connection);
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = server });
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task SeedAsync(
        NpgsqlConnection connection, int serverId, string label, int rows, DateTime sightedAt, DateTime? capturedAt, DateTime? expiredAt,
        DateTime? lastAttemptAt, CancellationToken ct)
    {
        await using var seed = new NpgsqlCommand(SeedSql, connection) { CommandTimeout = 300 };
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = label });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = rows });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = sightedAt });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)capturedAt ?? DBNull.Value });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)expiredAt ?? DBNull.Value });
        seed.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = (object?)lastAttemptAt ?? DBNull.Value });
        Assert.Equal(rows, await seed.ExecuteNonQueryAsync(ct));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 600 };
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs the ladder over the scratch database, then opens a connection on the store's search path.</summary>
    private static async Task<NpgsqlConnection> MigratedConnectionAsync(ScratchPostgres scratch, CancellationToken ct)
    {
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, null, ct);
        }

        var connection = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
        {
            SearchPath = PgSchemaGenerator.SearchPath,
        }.ConnectionString);
        try
        {
            await connection.OpenAsync(ct);
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }
}
