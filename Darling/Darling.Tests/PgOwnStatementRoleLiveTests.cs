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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The one own-statement test that needs a login other than the service's (#7567). It creates and drops the role
/// <c>pm7567_app</c>, a cluster-wide object under a fixed name, so it runs in the <c>pg-cluster-roles</c> collection
/// that runs alone (<see cref="PgClusterObjectCensusTests"/>). The seeding helpers are the sibling class's.
/// <para>The chain reads its own statements back from <c>pg_stat_statements</c>, whose hash table is cluster-wide and
/// holds <c>pg_stat_statements.max</c> entries. When a new statement needs a slot in a full table, the module evicts
/// the least-used five percent of the entries, and a statement run once is the least used there is: the chain's own
/// entries can go between the statements and the read, and the read then finds nothing. So the chain first makes sure
/// the table has room for every entry it adds (nothing else runs beside this collection, so the room stays), and it
/// checks the module's eviction counter around its statements, so a pass that ran meanwhile is named instead of
/// showing up as an empty read.</para>
/// </summary>
/* #1776 own-store: not [Collection("live-postgres")]. The test migrates the store itself and cleans by server id, so it needs
   nothing from LivePostgresStoreFixture. */
[Collection("pg-cluster-roles")]
public sealed class PgOwnStatementRoleLiveTests
{
    /// <summary>
    /// The whole chain on real <c>pg_stat_statements</c> data, with nothing seeded by hand (#7567 round 4): marked and
    /// unmarked statements run on a scratch target, the real hourly text fetch reads them and the real upsert stores
    /// <c>own_entries</c>, and the real exclusion reads run over statistics rows carrying the target's own
    /// <c>userid</c>/<c>dbid</c>. The fetch builds the pairs from <c>oid</c> columns, which PostgreSQL writes into JSON as
    /// quoted strings, while the readers probe with <c>bigint</c> numbers: without the cast the containment never
    /// matches, the service's statement is never left out, and every other test here still passes on its hand-written
    /// numbers.
    /// </summary>
    [Fact]
    public async Task TheRealTextFetch_StoresPairsTheExclusionReadsMatch_ServiceLeftOut_ApplicationKept()
    {
        var cs = PgOwnStatementLiveTests.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        await RunChainAsync(cs!, TestContext.Current.CancellationToken, beforeStatements: null);
    }

    /// <summary>
    /// The same chain on a statement table with one free slot, the state a long test run leaves the shared cluster in: the
    /// chain's entries must survive its own statements, and no eviction pass may run between the statements and the read.
    /// Fills the table with statements run twice each, so every filler is better used than the chain's once-run
    /// statements and an eviction pass would take the chain's entries first.
    /// </summary>
    [Fact]
    public async Task TheRealTextFetch_OnAStatementTableWithOneFreeSlot_RunsWithoutAnEvictionPass_AndStillMatches()
    {
        var cs = PgOwnStatementLiveTests.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        await RunChainAsync(cs!, TestContext.Current.CancellationToken, beforeStatements: FillToOneFreeSlotAsync);
    }

    /// <summary>
    /// Runs the chain against the store at <paramref name="cs"/> and a scratch target of its own. <paramref name="beforeStatements"/>
    /// runs on the target after the extension and the role are in place and before the marked statements.
    /// </summary>
    private static async Task RunChainAsync(string cs, CancellationToken ct, Func<NpgsqlConnection, CancellationToken, Task>? beforeStatements)
    {
        /* #1776 own-store: the target is a scratch database of its own (CREATE EXTENSION would leave residue in the shared
           store); the store tables the fetch writes and the readers read are the shared store's, cleaned by server id. */
        var scratch = await ScratchPostgres.CreateAsync(cs, ct, emptyStore: true);
        await using var store = new NpgsqlConnection(cs);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);
        await PgOwnStatementLiveTests.DeleteRowsAsync(store, ct);
        await using var dataSource = NpgsqlDataSource.Create(cs);
        var bodySucceeded = false;
        try
        {
            long serviceQueryId, appQueryId, sharedQueryId;
            await using (var target = new NpgsqlConnection(scratch.ConnectionString))
            {
                await target.OpenAsync(ct);
                try
                {
                    await DarlingMcpTestData.ExecAsync(target, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
                    await DarlingMcpTestData.ExecAsync(target, ct,
                        "DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'pm7567_app') THEN CREATE ROLE pm7567_app NOLOGIN; END IF; END $do$");
                    await DarlingMcpTestData.ExecAsync(target, ct, "GRANT pm7567_app TO CURRENT_USER");
                    await using var probe = new NpgsqlCommand("SELECT count(*) FROM pg_stat_statements", target);
                    await probe.ExecuteScalarAsync(ct);
                }
                catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000" or "42501")
                {
                    Assert.Skip("pg_stat_statements or role setup is not available on this server: " + ex.Message);
                }

                if (beforeStatements is not null)
                {
                    await beforeStatements(target, ct);
                }

                await EnsureStatementTableHeadroomAsync(target, ct);
                var deallocBefore = await DeallocAsync(target, ct);

                /* The service's statement: marked, run by the connecting login. The application's: unmarked, run under another login.
                   The shared one is both: the same statement shape (one queryid) from both logins, two entries. */
                await DarlingMcpTestData.ExecAsync(target, ct, PgOwnStatement.Mark("SELECT 7567 + 1 AS pm7567own_service"));
                await DarlingMcpTestData.ExecAsync(target, ct, PgOwnStatement.Mark("SELECT 7567 - 3 AS pm7567own_shared"));
                await DarlingMcpTestData.ExecAsync(target, ct, "SET ROLE pm7567_app");
                await DarlingMcpTestData.ExecAsync(target, ct, "SELECT 7567 * 2 AS pm7567own_app");
                await DarlingMcpTestData.ExecAsync(target, ct, "SELECT 7567 - 3 AS pm7567own_shared");
                await DarlingMcpTestData.ExecAsync(target, ct, "RESET ROLE");

                /* The real fetch: the hourly text fetch's SQL, marked the way the service sends it, and the real upsert. */
                var context = new CollectorContext
                {
                    ServerId = PgOwnStatementLiveTests.ServerId,
                    ServerName = PgOwnStatementLiveTests.ServerName,
                    CollectionTime = DateTime.UtcNow,
                    Deltas = new CollectorDeltaCalculator(),
                    Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = target.PostgreSqlVersion.Major },
                    PgExtensionSchemas = new Dictionary<string, string>(StringComparer.Ordinal) { ["pg_stat_statements"] = "public" },
                };
                var queryIds = new List<long>();
                var texts = new List<string>();
                var ownEntries = new List<string?>();
                await using (var fetch = new NpgsqlCommand(
                    PgOwnStatement.Mark(PgStatementText.FetchSqlFor(false, target.PostgreSqlVersion.Major, context)), target))
                {
                    fetch.Parameters.AddWithValue(10_000);
                    await using var reader = await fetch.ExecuteReaderAsync(ct);
                    var seen = new HashSet<long>();
                    while (await reader.ReadAsync(ct))
                    {
                        var queryId = reader.GetInt64(0);
                        if (reader.IsDBNull(1) || !seen.Add(queryId))
                        {
                            continue;
                        }

                        queryIds.Add(queryId);
                        texts.Add(reader.GetString(1));
                        ownEntries.Add(reader.IsDBNull(2) ? null : reader.GetString(2));
                    }
                }

                await DarlingMcpTestData.RegisterServerAsync(store, PgOwnStatementLiveTests.ServerId, PgOwnStatementLiveTests.ServerName, ct);
                await using (var upsert = new NpgsqlCommand(PgStatementText.UpsertSql, store))
                {
                    var stamp = new DateTime[queryIds.Count];
                    Array.Fill(stamp, PgStatementText.Naive(DateTime.UtcNow));
                    upsert.Parameters.AddWithValue(Enumerable.Repeat(PgOwnStatementLiveTests.ServerId, queryIds.Count).ToArray());
                    upsert.Parameters.AddWithValue(queryIds.ToArray());
                    upsert.Parameters.AddWithValue(texts.ToArray());
                    upsert.Parameters.AddWithValue(stamp);
                    upsert.Parameters.AddWithValue(ownEntries.ToArray());
                    upsert.Parameters.AddWithValue(new string?[queryIds.Count]);
                    await upsert.ExecuteNonQueryAsync(ct);
                }

                /* The real ids, and the login and database ids the target holds for them: what the stats collector stores. */
                var entries = new List<(string Name, long QueryId, long UserId, long DatabaseId)>();
                await using (var read = new NpgsqlCommand(
                    "SELECT substring(query from 'pm7567own_[a-z]+'), queryid, userid::bigint, dbid::bigint FROM pg_stat_statements "
                    + "WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database()) "
                    + "AND query LIKE '%AS pm7567own\\_%' ORDER BY 1, userid", target))
                await using (var reader = await read.ExecuteReaderAsync(ct))
                {
                    while (await reader.ReadAsync(ct))
                    {
                        entries.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3)));
                    }
                }

                /* An eviction pass between the statements and the read takes the least-used entries, the chain's own among them. */
                var deallocAfter = await DeallocAsync(target, ct);
                Assert.True(deallocBefore == deallocAfter,
                    $"pg_stat_statements ran {deallocAfter - deallocBefore} eviction pass(es) between the chain's statements and its read (dealloc {deallocBefore} -> {deallocAfter}); the read found {entries.Count} of its entries.");

                var service = Assert.Single(entries, e => e.Name == "pm7567own_service");
                var app = Assert.Single(entries, e => e.Name == "pm7567own_app");
                var shared = entries.Where(e => e.Name == "pm7567own_shared").ToArray();
                Assert.Equal(2, shared.Length);
                Assert.Equal(shared[0].QueryId, shared[1].QueryId);
                Assert.NotEqual(service.UserId, app.UserId);
                serviceQueryId = service.QueryId;
                appQueryId = app.QueryId;
                sharedQueryId = shared[0].QueryId;

                for (var i = 6; i >= 1; i--)
                {
                    var at = DateTime.UtcNow.AddMinutes(-i);
                    await PgOwnStatementLiveTests.SeedStatsAsync(store, ct, at, service.QueryId, service.UserId, deltaCalls: 30, deltaTimeMs: 30_000, service.DatabaseId);
                    await PgOwnStatementLiveTests.SeedStatsAsync(store, ct, at, app.QueryId, app.UserId, deltaCalls: 10, deltaTimeMs: 200, app.DatabaseId);
                    foreach (var entry in shared)
                    {
                        var isService = entry.UserId == service.UserId;
                        await PgOwnStatementLiveTests.SeedStatsAsync(store, ct, at, entry.QueryId, entry.UserId, isService ? 30 : 10, isService ? 30_000 : 200, entry.DatabaseId);
                    }
                }
            }

            var from = DateTime.UtcNow.AddHours(-1);
            var to = DateTime.UtcNow.AddMinutes(1);
            var naiveFrom = DateTime.SpecifyKind(from, DateTimeKind.Unspecified);
            var naiveTo = DateTime.SpecifyKind(to, DateTimeKind.Unspecified);

            /* NotOwnOnStats through the fact collector's top statements: the service's statement is out, the application's and
               the shared queryid's application entry are in, with only the application's time (6 x 200 ms each). */
            var windowMs = new Dictionary<long, long>();
            await using (var top = new NpgsqlCommand(PgTargetFactCollector.PgTargetTopStatementsSql, store))
            {
                top.Parameters.AddWithValue(PgOwnStatementLiveTests.ServerId);
                top.Parameters.AddWithValue(naiveFrom);
                top.Parameters.AddWithValue(naiveTo);
                top.Parameters.AddWithValue(10);
                top.Parameters.AddWithValue(CollectorDeltaCalculator.DefaultMaxGapSeconds);
                await using var reader = await top.ExecuteReaderAsync(ct);
                while (await reader.ReadAsync(ct))
                {
                    windowMs[reader.GetInt64(reader.GetOrdinal("queryid"))] = reader.GetInt64(reader.GetOrdinal("total_exec_ms"));
                }
            }

            Assert.DoesNotContain(serviceQueryId, windowMs.Keys);
            Assert.Equal(1200, windowMs[appQueryId]);
            Assert.Equal(1200, windowMs[sharedQueryId]);

            /* The reader's IsServiceStatementSql: the service's row is flagged, the application's and the shared one are not. */
            var page = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, PgOwnStatementLiveTests.ServerId, from, to, 10, ct);
            Assert.True(Assert.Single(page.Rows, r => r.QueryId == serviceQueryId).IsServiceStatement);
            Assert.False(Assert.Single(page.Rows, r => r.QueryId == appQueryId).IsServiceStatement);
            Assert.False(Assert.Single(page.Rows, r => r.QueryId == sharedQueryId).IsServiceStatement);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs, bodySucceeded, async (cleanup, cleanupCt) =>
                await PgOwnStatementLiveTests.DeleteRowsAsync(cleanup, cleanupCt));
            await scratch.DisposeAsync();
            /* Roles are cluster-wide, so the scratch database going does not take it. */
            await using var admin = new NpgsqlConnection(cs);
            await admin.OpenAsync(ct);
            await DarlingMcpTestData.ExecAsync(admin, ct, "DROP ROLE IF EXISTS pm7567_app");
        }
    }

    /// <summary>
    /// The free slots the chain secures before its statements: it adds about fifteen entries (the role setup, the four
    /// statements under two logins, the fetch, the store's register and upsert, and the read), and the margin covers
    /// whatever a background worker on the cluster runs meanwhile.
    /// </summary>
    internal const int ChainEntries = 64;

    /// <summary>
    /// Makes sure <c>pg_stat_statements</c> has at least <see cref="ChainEntries"/> free slots. When it has fewer, runs
    /// small batches of distinct shapes until the module's eviction pass frees five percent of the table; a cluster whose
    /// pass frees less than the chain needs skips with its <c>pg_stat_statements.max</c>. The shapes are a family of
    /// their own (differences, not sums), so none of them lands on an entry the one-free-slot filler already made.
    /// </summary>
    private static async Task EnsureStatementTableHeadroomAsync(NpgsqlConnection target, CancellationToken ct)
    {
        const int batchSize = 8;
        var max = await MaxEntriesAsync(target, ct);
        var free = max - await EntryCountAsync(target, ct);
        var next = 0;
        while (free < ChainEntries && next < ChainEntries + 2 * batchSize)
        {
            var batch = new StringBuilder();
            for (var i = 0; i < batchSize; i++)
            {
                batch.Append(HeadroomStatement(next++)).Append(';');
            }

            await DarlingMcpTestData.ExecAsync(target, ct, batch.ToString());
            free = max - await EntryCountAsync(target, ct);
        }

        Assert.SkipWhen(free < ChainEntries,
            $"pg_stat_statements.max is {max}: an eviction pass leaves {free} free slots, fewer than the {ChainEntries} the chain needs.");
    }

    /// <summary>
    /// The <paramref name="n"/>-th shape of the headroom family: two to nine columns, each <c>1</c> less one to eight more
    /// <c>1</c>s, under one of eight WHERE/LIMIT/OFFSET suffixes. Every shape has at least one subtraction and two columns,
    /// so the family shares no entry with <see cref="FillerStatement"/>'s sums, with a bare <c>SELECT 1</c>, or with the
    /// chain's own single-column statements.
    /// </summary>
    internal static string HeadroomStatement(int n)
    {
        var variant = n & 7;
        var rest = n >> 3;
        var items = rest / 8 + 2;
        var terms = rest % 8 + 1;
        var item = "1" + string.Concat(Enumerable.Repeat("-1", terms));
        var suffix = ((variant & 1) != 0 ? " WHERE true" : string.Empty)
            + ((variant & 2) != 0 ? " LIMIT 1" : string.Empty)
            + ((variant & 4) != 0 ? " OFFSET 0" : string.Empty);
        return "SELECT " + string.Join(",", Enumerable.Repeat(item, items)) + suffix;
    }

    /// <summary>The largest table the filler fills; a cluster set wider than this skips the one-free-slot test.</summary>
    private const int MaxEntriesTheFillerCanFill = 20_000;

    /// <summary>
    /// Fills <c>pg_stat_statements</c> until one slot is free, with a distinct statement shape per entry, each batch run
    /// twice so that every filler is better used than a statement run once.
    /// </summary>
    private static async Task FillToOneFreeSlotAsync(NpgsqlConnection target, CancellationToken ct)
    {
        var max = await MaxEntriesAsync(target, ct);
        Assert.SkipWhen(max > MaxEntriesTheFillerCanFill,
            $"pg_stat_statements.max is {max}, more than the {MaxEntriesTheFillerCanFill} the one-free-slot test fills.");

        var side = (int)Math.Ceiling(Math.Sqrt((max + 1) / 8.0)) + 1;
        var next = 0;
        var free = max - await EntryCountAsync(target, ct);
        while (free > 1)
        {
            var size = Math.Min(free - 1, 400);
            var batch = new StringBuilder();
            for (var i = size; i > 0; i--)
            {
                batch.Append(FillerStatement(next++, side)).Append(';');
            }

            await DarlingMcpTestData.ExecAsync(target, ct, batch.ToString());
            await DarlingMcpTestData.ExecAsync(target, ct, batch.ToString());

            /* A batch of new shapes that leaves the count where it was means the module is not tracking this session's
               statements (pg_stat_statements.track is none for the cluster or for this login); the next batch would add
               nothing either, so the fill stops here instead of running until the job's timeout. */
            var afterBatch = max - await EntryCountAsync(target, ct);
            if (afterBatch == free)
            {
                Assert.Fail($"A batch of {size} new statement shapes left pg_stat_statements at {max - free} of {max} entries: the module is not tracking this session's statements.");
            }

            free = afterBatch;
        }
    }

    /// <summary>
    /// The <paramref name="n"/>-th distinct statement shape of the filler's family: two or more columns of <c>1</c>, each a
    /// sum of some more, under one of eight WHERE/LIMIT/OFFSET suffixes. Constants and aliases do not enter the module's
    /// query id, so a shape has to differ in structure to get an entry of its own, and a single-column shape would be the
    /// chain's own <c>SELECT $1 + $2</c>; <paramref name="side"/> bounds the columns and the terms, and the family holds
    /// <c>8 * side * (side + 1)</c> shapes.
    /// </summary>
    internal static string FillerStatement(int n, int side)
    {
        var variant = n & 7;
        var rest = n >> 3;
        var items = rest / (side + 1) + 2;
        var terms = rest % (side + 1);
        var item = "1" + string.Concat(Enumerable.Repeat("+1", terms));
        var suffix = ((variant & 1) != 0 ? " WHERE true" : string.Empty)
            + ((variant & 2) != 0 ? " LIMIT 1" : string.Empty)
            + ((variant & 4) != 0 ? " OFFSET 0" : string.Empty);
        return "SELECT " + string.Join(",", Enumerable.Repeat(item, items)) + suffix;
    }

    /// <summary><c>pg_stat_statements.max</c>, a server setting readable from any database once the library is preloaded.</summary>
    private static async Task<int> MaxEntriesAsync(NpgsqlConnection target, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT current_setting('pg_stat_statements.max')::int", target);
        return (int)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>How many entries the module's hash table holds right now, without loading the query texts.</summary>
    private static async Task<int> EntryCountAsync(NpgsqlConnection target, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT count(*)::int FROM pg_stat_statements(false)", target);
        return (int)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <summary>The module's eviction counter, <c>pg_stat_statements_info.dealloc</c>: how many passes have run since the last full reset.</summary>
    private static async Task<long> DeallocAsync(NpgsqlConnection target, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("SELECT dealloc FROM pg_stat_statements_info", target);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }
}
