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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6750 part 3: the keys of the monitor's own Query Store statements (<see cref="QueryStoreSelfKeys"/>). The triggers on the text table
/// record a key when a landed text carries the marker and remove it when an overwrite lands text without it, for this build's writer and
/// for an older build's statement alike. The backfill records the keys of text stored before the rung and opens the gate. The regressions
/// read (the MCP tool and the Viewer grid) hides the monitor's own statement and counts only the other regressions, with the gate closed
/// (the text probe), open (the keys), and on a store without the V219 tables (the gate answers closed, no error).
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test reaches DARLING_TEST_PG only to CREATE and DROP its
   own database through ScratchPostgres and works entirely inside it. It shares the gap-cache collection with the other regressions
   table tests, because the gate's cadence check reads a process-wide cache. */
[Collection("gap-cache-serial")]
public sealed class QueryStoreSelfKeysLiveTests
{
    private const int ServerId = QueryStoreRegressionsFailureGroupsLiveTests.ServerId;
    private const string Db = QueryStoreRegressionsFailureGroupsLiveTests.Db;
    private const long Own = 43;
    private static readonly long[] Ordinary = [40, 41, 42];

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static string SelfText(long id) => "(@cutoff_time datetime2(7))SELECT /* " + QueryStoreCollector.SelfQueryMarker + " */ qsrs.plan_id -- " + id;

    private static async Task<long> CountAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct));
    }

    private static Task<long> KeysAsync(NpgsqlConnection connection, CancellationToken ct) =>
        CountAsync(connection, "SELECT count(*) FROM collect.query_store_self_keys", ct);

    [Fact]
    public async Task TheTextWriter_RecordsTheKeyOfAMarkedText_AndRemovesItWhenAnOverwriteLandsOtherText()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var now = DateTime.UtcNow;

        /* The real fetch-store path: one marked text and one ordinary one. Only the marked one has a key. */
        await QueryStoreTextWriter.WriteAsync(connection, ServerId, Db,
            [new FetchedQueryText(Own, SelfText(Own), "0x01"), new FetchedQueryText(40, "SELECT 40", "0x02")], now, 30, cancellationToken: ct);
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_self_keys WHERE server_id = {ServerId} AND database_name = '{Db}' AND query_id = {Own}", ct));
        Assert.Equal(1L, await KeysAsync(connection, ct));

        /* A Query Store reset renumbered the id: the same id now names an ordinary statement, and its key goes with the old text. */
        await QueryStoreTextWriter.WriteAsync(connection, ServerId, Db,
            [new FetchedQueryText(Own, "SELECT after_reset", "0x03")], now.AddMinutes(5), 30, cancellationToken: ct);
        Assert.Equal(0L, await KeysAsync(connection, ct));

        /* And back: an ordinary id that is now the monitor's own statement gains a key. */
        await QueryStoreTextWriter.WriteAsync(connection, ServerId, Db,
            [new FetchedQueryText(40, SelfText(40), "0x04")], now.AddMinutes(10), 30, cancellationToken: ct);
        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_self_keys WHERE query_id = 40", ct));

        /* Retention: a key whose text row is gone is pruned, a key whose text row is there stays. */
        await QueryStoreTextWriter.WriteAsync(connection, ServerId, Db,
            [new FetchedQueryText(Own, SelfText(Own), "0x05")], now.AddMinutes(15), 30, cancellationToken: ct);
        Assert.Equal(2L, await KeysAsync(connection, ct));
        await using (var gone = new NpgsqlCommand("DELETE FROM collect.query_store_text WHERE query_id = 40", connection))
        {
            await gone.ExecuteNonQueryAsync(ct);
        }

        await using (var prune = new NpgsqlCommand(QueryStoreSelfKeys.PruneSql, connection))
        {
            prune.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(now, DateTimeKind.Unspecified) });
            Assert.Equal(1, await prune.ExecuteNonQueryAsync(ct));
        }

        Assert.Equal(1L, await CountAsync(connection, $"SELECT count(*) FROM collect.query_store_self_keys WHERE query_id = {Own}", ct));
    }

    /// <summary>The rig: three ordinary queries and the monitor's own statement, each tripling its CPU from the baseline to the recent
    /// window. The stats rows carry ordinary inline text for all four, so only the text table says which one is the monitor's.</summary>
    private static async Task<QueryStoreRegressionsFailureGroupsLiveTests.Rig> OpenRigAsync(DateTime now, CancellationToken ct) =>
        await QueryStoreRegressionsFailureGroupsLiveTests.OpenAsync(async (runner, server, context, today, start) =>
        {
            long id = 9800;
            foreach (var q in Ordinary.Append(Own))
            {
                foreach (var (first, cpu) in new[] { (now.AddDays(-5), 1_000L), (now.AddHours(-3), 3_000L) })
                {
                    id++;
                    await QueryStoreRegressionsFailureGroupsLiveTests.WriteAsync(runner, server, context, "Regular", id, first, q, 100, cpu, cpu * 2, ct, Db, "SELECT " + q);
                }
            }

            await QueryStoreRegressionsFailureGroupsLiveTests.WriteAsync(runner, server, context, "Regular", ++id, start.AddDays(-12), 90, 5, 1_000, 1_000, ct, Db, "SELECT floor");
        }, ct);

    /// <summary>The MCP tool's ids and <c>regression_count</c>, and the Viewer grid's ids.</summary>
    private static async Task<(List<long> Mcp, long Count, List<long> Viewer)> ReadBothAsync(QueryStoreRegressionsFailureGroupsLiveTests.Rig rig, DateTime now, CancellationToken ct)
    {
        var json = await DarlingMcpQueryStoreRegressionTools.GetQueryStoreRegressions(rig.Postgres, "qsfail", 24, DatabaseFilter.All, 30, false, null, NullLogger.Instance, ct);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var ids = doc.RootElement.GetProperty("regressions").EnumerateArray().Select(r => r.GetProperty("query_id").GetInt64()).OrderBy(x => x).ToList();
        var count = doc.RootElement.GetProperty("regression_count").GetInt64();
        await using var viewer = new ViewerDataService(rig.Scratch.ConnectionString);
        var grid = await viewer.GetQueryStoreRegressionsWithScopeAsync(ServerId, now.AddHours(-24), now, cancellationToken: ct);
        return (ids, count, grid.Rows.Select(r => r.QueryId).OrderBy(x => x).ToList());
    }

    private static void AssertOwnHidden((List<long> Mcp, long Count, List<long> Viewer) read)
    {
        Assert.Equal(Ordinary.ToList(), read.Mcp);
        Assert.Equal(3L, read.Count);
        Assert.Equal(Ordinary.ToList(), read.Viewer);
    }

    [Fact]
    public async Task TheRegressionsRead_HidesTheMonitorsOwnStatement_AndCountsThree_WithTheGateClosedAndOpen()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var now = LiveClock.Now();
        await using var rig = await OpenRigAsync(now, ct);

        /* The live shape: the text lands through the fetch-store path, and only the monitor's own carries the marker. */
        await QueryStoreTextWriter.WriteAsync(rig.Connection, ServerId, Db,
            [.. Ordinary.Select(q => new FetchedQueryText(q, "SELECT " + q, null)), new FetchedQueryText(Own, SelfText(Own), null)],
            DateTime.SpecifyKind(now, DateTimeKind.Utc), 30, cancellationToken: ct);
        Assert.Equal(1L, await KeysAsync(rig.Connection, ct));

        /* Gate closed (no backfill yet): the text probe hides it. */
        Assert.False(await QueryStoreSelfKeys.ReadyAsync(rig.Connection, null, 30, ct));
        AssertOwnHidden(await ReadBothAsync(rig, now, ct));

        /* Gate open: the keys hide it, with the same rows and the same count. */
        Assert.Equal(0, await QueryStoreSelfKeys.BackfillAsync(rig.Connection, ct));
        Assert.True(await QueryStoreSelfKeys.ReadyAsync(rig.Connection, null, 30, ct));
        AssertOwnHidden(await ReadBothAsync(rig, now, ct));

        /* Not vacuous: with the gate open it is the key that hides the statement, not the text. Take the key away and it is listed. */
        await using (var drop = new NpgsqlCommand("DELETE FROM collect.query_store_self_keys", rig.Connection))
        {
            await drop.ExecuteNonQueryAsync(ct);
        }

        var unkeyed = await ReadBothAsync(rig, now, ct);
        Assert.Equal(Ordinary.Append(Own).ToList(), unkeyed.Mcp);
        Assert.Equal(Ordinary.Append(Own).ToList(), unkeyed.Viewer);
        Assert.Equal(4L, unkeyed.Count);
    }

    [Fact]
    public async Task TheBackfill_KeysTextStoredBeforeTheTable_AndTheReadHidesItWithTheGateOpen()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var now = LiveClock.Now();
        await using var rig = await OpenRigAsync(now, ct);

        /* Text stored before the rung: written straight to the text table with the insert trigger off, so no key. */
        await using (var off = new NpgsqlCommand($"ALTER TABLE collect.query_store_text DISABLE TRIGGER {QueryStoreSelfKeys.InsertTriggerName}", rig.Connection))
        {
            await off.ExecuteNonQueryAsync(ct);
        }

        await using (var old = new NpgsqlCommand(
            "INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen) SELECT $1, $2, q, CASE WHEN q = $3 THEN $4 ELSE 'SELECT ' || q END, $5 FROM unnest($6::bigint[]) AS q",
            rig.Connection))
        {
            old.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
            old.Parameters.Add(new NpgsqlParameter<string> { TypedValue = Db });
            old.Parameters.Add(new NpgsqlParameter<long> { TypedValue = Own });
            old.Parameters.Add(new NpgsqlParameter<string> { TypedValue = SelfText(Own) });
            old.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(now, DateTimeKind.Unspecified) });
            old.Parameters.Add(new NpgsqlParameter<long[]> { TypedValue = [.. Ordinary, Own] });
            await old.ExecuteNonQueryAsync(ct);
        }

        await using (var on = new NpgsqlCommand($"ALTER TABLE collect.query_store_text ENABLE TRIGGER {QueryStoreSelfKeys.InsertTriggerName}", rig.Connection))
        {
            await on.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(0L, await KeysAsync(rig.Connection, ct));

        /* The step records the one key, opens the gate, and a second run does nothing. */
        Assert.Equal(1, await QueryStoreSelfKeys.BackfillAsync(rig.Connection, ct));
        Assert.True(await QueryStoreSelfKeys.ReadyAsync(rig.Connection, null, 30, ct));
        Assert.Null(await QueryStoreSelfKeys.BackfillAsync(rig.Connection, ct));
        Assert.Equal(1L, await CountAsync(rig.Connection, $"SELECT count(*) FROM collect.query_store_self_keys WHERE query_id = {Own}", ct));

        AssertOwnHidden(await ReadBothAsync(rig, now, ct));
    }

    /// <summary>The text upsert as dev shipped it before #6750 part 3, copied as a literal: the statement an older build runs against a
    /// store a newer build migrated past V219 (a rollback, or an older instance sharing the store). It knows nothing of the key table.</summary>
    private const string PreV219UpsertSql = @"INSERT INTO collect.query_store_text
    (server_id, database_name, query_id, query_sql_text, query_hash, last_seen)
SELECT server_id, database_name, query_id, query_sql_text, query_hash, stamped
FROM unnest($1::integer[], $2::text[], $3::bigint[], $4::text[], $5::text[], $6::timestamp[])
     AS batch(server_id, database_name, query_id, query_sql_text, query_hash, stamped)
ORDER BY server_id, database_name, query_id
ON CONFLICT (server_id, database_name, query_id) DO UPDATE SET
    query_sql_text = EXCLUDED.query_sql_text,
    query_hash = COALESCE(EXCLUDED.query_hash, query_store_text.query_hash),
    last_seen = EXCLUDED.last_seen
WHERE EXCLUDED.last_seen >= query_store_text.last_seen";

    private static async Task OldBuildUpsertAsync(NpgsqlConnection connection, (long QueryId, string Text)[] rows, DateTime stamp, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(PreV219UpsertSql, connection);
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = [.. rows.Select(_ => ServerId)] });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. rows.Select(_ => Db)] });
        command.Parameters.Add(new NpgsqlParameter<long[]> { TypedValue = [.. rows.Select(r => r.QueryId)] });
        command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = [.. rows.Select(r => r.Text)] });
        command.Parameters.Add(new NpgsqlParameter<string?[]> { TypedValue = [.. rows.Select(_ => (string?)null)] });
        command.Parameters.Add(new NpgsqlParameter<DateTime[]> { TypedValue = [.. rows.Select(_ => DateTime.SpecifyKind(stamp, DateTimeKind.Unspecified))] });
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>
    /// Review round 2: the gate stays open once stamped, so every writer of text has to keep the keys, an older build's included. With
    /// the gate open, a marked text landed through the pre-V219 upsert gains a key and the read hides it. Overwriting it with ordinary
    /// text through the same old statement removes the key. Red without the triggers: the old statement writes no key, and the read
    /// lists the monitor's statement (#6654).
    /// </summary>
    [Fact]
    public async Task AnOlderBuildsTextUpsert_KeepsTheKeysCurrent_WithTheGateOpen()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var now = LiveClock.Now();
        await using var rig = await OpenRigAsync(now, ct);

        /* The gate opens over an empty text table, as on a store the newer build backfilled before the rollback. */
        Assert.Equal(0, await QueryStoreSelfKeys.BackfillAsync(rig.Connection, ct));
        Assert.True(await QueryStoreSelfKeys.ReadyAsync(rig.Connection, null, 30, ct));

        /* The older build lands the text. */
        await OldBuildUpsertAsync(rig.Connection, [.. Ordinary.Select(q => (q, "SELECT " + q)), (Own, SelfText(Own))], now, ct);
        Assert.Equal(1L, await CountAsync(rig.Connection, $"SELECT count(*) FROM collect.query_store_self_keys WHERE server_id = {ServerId} AND database_name = '{Db}' AND query_id = {Own}", ct));
        Assert.Equal(1L, await KeysAsync(rig.Connection, ct));
        AssertOwnHidden(await ReadBothAsync(rig, now, ct));

        /* The same id now names an ordinary statement (a Query Store reset), landed by the same old statement: the key goes. */
        await OldBuildUpsertAsync(rig.Connection, [(Own, "SELECT after_reset")], now.AddMinutes(5), ct);
        Assert.Equal(0L, await KeysAsync(rig.Connection, ct));
    }

    /// <summary>
    /// Review round 2: the gate read is fail-safe. The MCP host answers while the worker's migration ladder runs, and the Viewer's connect
    /// gate fails open, so a read can meet a store without the V219 tables. With both dropped (the rewind of
    /// <see cref="RungRewindArtifacts"/>), the gate answers closed, and both the MCP tool and the Viewer grid answer through the text
    /// probe and hide the monitor's statement, with no 42P01. Red without the <c>to_regclass</c> check.
    /// </summary>
    [Fact]
    public async Task WithoutTheV219Tables_TheGateIsClosed_AndBothReadsHideTheMonitorsStatementThroughTheTextProbe()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var now = LiveClock.Now();
        await using var rig = await OpenRigAsync(now, ct);
        await QueryStoreTextWriter.WriteAsync(rig.Connection, ServerId, Db,
            [.. Ordinary.Select(q => new FetchedQueryText(q, "SELECT " + q, null)), new FetchedQueryText(Own, SelfText(Own), null)],
            DateTime.SpecifyKind(now, DateTimeKind.Utc), 30, cancellationToken: ct);
        Assert.Equal(0, await QueryStoreSelfKeys.BackfillAsync(rig.Connection, ct));

        await using (var drop = new NpgsqlCommand(RungRewindArtifacts.Ladder.Single(r => r.Version == 219).DropSql, rig.Connection))
        {
            await drop.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(0L, await CountAsync(rig.Connection, "SELECT CASE WHEN to_regclass('collect.query_store_self_keys_state') IS NULL THEN 0 ELSE 1 END", ct));

        /* The reads first: each meets the missing table inside its own snapshot transaction. */
        AssertOwnHidden(await ReadBothAsync(rig, now, ct));

        /* And the gate itself, in a transaction as the reads run it: closed, and the transaction is still usable. */
        await using (var transaction = await rig.Connection.BeginTransactionAsync(ct))
        {
            Assert.False(await QueryStoreSelfKeys.ReadyAsync(rig.Connection, transaction, 30, ct));
            await using var after = new NpgsqlCommand("SELECT 1", rig.Connection, transaction);
            Assert.Equal(1, await after.ExecuteScalarAsync(ct));
        }
    }

    /// <summary>The backfill as dev shipped it in V219's first build, copied as a literal: the marker test and the ORDER BY on one SELECT,
    /// which on a big text table the planner runs as a walk of the primary key in key order (random heap and TOAST reads).</summary>
    private const string BackfillBeforeTheFilterFirstFix = @"
INSERT INTO collect.query_store_self_keys (server_id, database_name, query_id)
SELECT t.server_id, t.database_name, t.query_id
FROM collect.query_store_text AS t
WHERE strpos(COALESCE(t.query_sql_text, ''), 'PerformanceMonitorLite') > 0
ORDER BY t.server_id, t.database_name, t.query_id
ON CONFLICT (server_id, database_name, query_id) DO NOTHING";

    /// <summary>The scan nodes that read <c>query_store_text</c> in a plan, as <c>Node Type</c> strings.</summary>
    private static List<string> ScanNodesOfTextTable(System.Text.Json.JsonElement node)
    {
        var found = new List<string>();
        void Visit(System.Text.Json.JsonElement n)
        {
            if (n.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "query_store_text")
            {
                found.Add(n.GetProperty("Node Type").GetString()!);
            }

            if (n.TryGetProperty("Plans", out var plans))
            {
                foreach (var child in plans.EnumerateArray())
                {
                    Visit(child);
                }
            }
        }

        Visit(node);
        return found;
    }

    /// <summary>The scan nodes of the text table in the plan of <paramref name="sql"/>. With <paramref name="sortOff"/> the EXPLAIN runs in a
    /// transaction under <c>SET LOCAL enable_sort = off</c>, which prices the sort out: a statement that must return rows in key order
    /// then has one sort-free path, the primary key walk, whatever the platform's costs for a 20,000-row table are. One command per
    /// statement (Npgsql refuses several in a parameterized command, and the SET must not leak past the transaction).</summary>
    private static async Task<List<string>> TextScansOfAsync(NpgsqlConnection connection, string sql, bool sortOff, CancellationToken ct)
    {
        await using var transaction = await connection.BeginTransactionAsync(ct);
        if (sortOff)
        {
            await using var set = new NpgsqlCommand("SET LOCAL enable_sort = off", connection, transaction);
            await set.ExecuteNonQueryAsync(ct);
        }

        await using var explain = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + sql, connection, transaction);
        var json = (string)(await explain.ExecuteScalarAsync(ct))!;
        using var document = System.Text.Json.JsonDocument.Parse(json);
        var scans = ScanNodesOfTextTable(document.RootElement[0].GetProperty("Plan"));
        await transaction.RollbackAsync(ct);
        return scans;
    }

    private static bool WalksAnIndex(List<string> scans) => scans.Any(s => s.StartsWith("Index", StringComparison.Ordinal));

    private static async Task<List<long>> KeyIdsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT query_id FROM collect.query_store_self_keys ORDER BY server_id, database_name, query_id", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var ids = new List<long>();
        while (await reader.ReadAsync(ct))
        {
            ids.Add(reader.GetInt64(0));
        }

        return ids;
    }

    /// <summary>
    /// #6750 part 3, the plan. The backfill filters on the marker, so it has to read the text table in one sequential pass and sort only
    /// the matches. With the <c>ORDER BY</c> on the same <c>SELECT</c> as the filter the planner walks the primary key in key order over
    /// every row (measured on a large store: 452.9 s and 2.2 million blocks cold, against 157.8 s and 1.4 million for the sequential
    /// form). The seed is 20,000 rows with the table ANALYZEd. At that size the planner's choice for <see cref="BackfillBeforeTheFilterFirstFix"/>
    /// at default settings is a near tie that differs by platform (CI's Windows PostgreSQL picked a Seq Scan and a sort), so the plans are read
    /// with <c>enable_sort = off</c>: the old statement then has only the primary key walk, which the test proves first, and the shipped
    /// statement, which sorts a CTE's output, still has to read the text table with a Seq Scan. The shipped one is also read at default settings. It also checks the two statements record the same keys, over a NULL text, a text
    /// with the marker in the middle, and ordinary text.
    /// </summary>
    [Fact]
    public async Task TheBackfill_ReadsTheTextTableWithOneSequentialScan_AndRecordsTheSameKeysAsBefore()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6750 live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* Text stored before the rung: no key for it, so the insert trigger is off while it is written. One command per statement: the
           seed takes a parameter, and Npgsql sends a parameterized command through the extended protocol, which refuses several statements. */
        await using (var off = new NpgsqlCommand($"ALTER TABLE collect.query_store_text DISABLE TRIGGER {QueryStoreSelfKeys.InsertTriggerName}", connection))
        {
            await off.ExecuteNonQueryAsync(ct);
        }

        await using (var seed = new NpgsqlCommand(
            @"INSERT INTO collect.query_store_text (server_id, database_name, query_id, query_sql_text, last_seen)
SELECT 1, 'db', q,
       CASE WHEN q % 50 = 0 THEN '(@cutoff_time datetime2(7))SELECT /* ' || $1 || ' */ qsrs.plan_id -- ' || q
            WHEN q % 97 = 0 THEN NULL
            ELSE 'SELECT ' || q || repeat('x', 200) END,
       now() AT TIME ZONE 'UTC'
FROM generate_series(1, 20000) AS q", connection))
        {
            seed.Parameters.Add(new NpgsqlParameter<string> { TypedValue = QueryStoreCollector.SelfQueryMarker });
            await seed.ExecuteNonQueryAsync(ct);
        }

        await using (var on = new NpgsqlCommand($"ALTER TABLE collect.query_store_text ENABLE TRIGGER {QueryStoreSelfKeys.InsertTriggerName}", connection))
        {
            await on.ExecuteNonQueryAsync(ct);
        }

        await using (var analyze = new NpgsqlCommand("ANALYZE collect.query_store_text", connection))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(0L, await KeysAsync(connection, ct));

        /* The probe can see the defect. With the sort priced out, the old statement's only path that returns rows in key order is the primary
           key walk, so its plan must show one on any planner. (At default settings the choice between that walk and a Seq Scan plus a sort
           is a near tie at this size and falls either way by platform, so the default plan proves nothing.) */
        var before = await TextScansOfAsync(connection, BackfillBeforeTheFilterFirstFix, sortOff: true, ct);
        Assert.True(WalksAnIndex(before),
            "the old statement did not plan as a primary key walk even with the sort priced out (" + string.Join(", ", before) + "), so the plan assertions below prove nothing");

        /* The fix: the shipped statement reads the text table with a Seq Scan and no index walk, with the sort priced out (the walk is the
           only way the old shape avoids the sort, and the CTE cannot use it) and at default settings. */
        foreach (var sortOff in new[] { true, false })
        {
            var after = await TextScansOfAsync(connection, QueryStoreSelfKeys.BackfillSql, sortOff, ct);
            Assert.True(after.Contains("Seq Scan") && !WalksAnIndex(after),
                "the shipped backfill must read the text table with a sequential scan and no index walk (sort off: " + sortOff + "), got " + string.Join(", ", after));
        }

        /* Same keys. The old statement first, then the shipped one over an emptied key table. */
        await using (var old = new NpgsqlCommand(BackfillBeforeTheFilterFirstFix, connection))
        {
            Assert.Equal(400, await old.ExecuteNonQueryAsync(ct));
        }

        var oldKeys = await KeyIdsAsync(connection, ct);
        await using (var clear = new NpgsqlCommand("DELETE FROM collect.query_store_self_keys", connection))
        {
            await clear.ExecuteNonQueryAsync(ct);
        }

        await using (var shipped = new NpgsqlCommand(QueryStoreSelfKeys.BackfillSql, connection))
        {
            Assert.Equal(400, await shipped.ExecuteNonQueryAsync(ct));
        }

        var newKeys = await KeyIdsAsync(connection, ct);
        Assert.Equal(oldKeys, newKeys);
        Assert.Equal(Enumerable.Range(1, 20000).Where(q => q % 50 == 0).Select(q => (long)q).ToList(), newKeys);
    }
}
