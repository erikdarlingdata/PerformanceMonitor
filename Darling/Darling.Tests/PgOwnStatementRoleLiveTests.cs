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
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the target is a scratch database of its own (CREATE EXTENSION would leave residue in the shared
           store); the store tables the fetch writes and the readers read are the shared store's, cleaned by server id. */
        var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var store = new NpgsqlConnection(cs);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);
        await PgOwnStatementLiveTests.DeleteRowsAsync(store, ct);
        await using var dataSource = NpgsqlDataSource.Create(cs!);
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
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await PgOwnStatementLiveTests.DeleteRowsAsync(cleanup, cleanupCt));
            await scratch.DisposeAsync();
            /* Roles are cluster-wide, so the scratch database going does not take it. */
            await using var admin = new NpgsqlConnection(cs);
            await admin.OpenAsync(ct);
            await DarlingMcpTestData.ExecAsync(admin, ct, "DROP ROLE IF EXISTS pm7567_app");
        }
    }
}
