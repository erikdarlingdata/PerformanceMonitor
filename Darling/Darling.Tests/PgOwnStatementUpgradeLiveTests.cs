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
/// #7681: <c>pg_stat_statements</c> keeps the text of the call that CREATED an entry, so on a server the service monitored
/// before the marker shipped, every entry of a service statement was created by the old, unmarked text and no later marked
/// call changes it. The hourly text fetch must still learn those entries as the service's, by matching the stored text against
/// the statements the service sends.
/// <para>Nothing is seeded by hand: real service statements (the column-statistics and table-bloat collectors' text, which
/// carry literal constants, <c>IN</c> lists and comments inside the statement) run unmarked first, as the old build sent them,
/// then marked, as the new build does, and the real fetch (<see cref="DarlingWorker.ReadPgStatementTextAsync"/>) reads them.</para>
/// </summary>
/* #1776 own-store: like PgOwnStatementRoleLiveTests, not [Collection("live-postgres")]: the test migrates the store itself and
   cleans by server id, and it creates a role, which is cluster-wide. */
[Collection("pg-cluster-roles")]
public sealed class PgOwnStatementUpgradeLiveTests
{
    private const string AppRole = "pm7681_app";

    [Fact]
    public async Task AnEntryCreatedByTheOldUnmarkedText_IsLearnedAsTheServices_ByItsStoredText()
    {
        var cs = PgOwnStatementLiveTests.ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the own-statement tests.");
        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: the target is a scratch database of its own (CREATE EXTENSION would leave residue in the shared
           store, and its entries are keyed by database, so none predates this test); the store tables the fetch writes and the
           readers read are the shared store's, cleaned by server id. */
        var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var store = new NpgsqlConnection(cs);
        await store.OpenAsync(ct);
        await PgMigrations.MigrateAsync(store, ct);
        await PgOwnStatementLiveTests.DeleteRowsAsync(store, ct);
        await using var dataSource = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            var columnStats = PgColumnStatsCollector.QueryText;
            var tableBloat = PgTableBloatStatsCollector.QueryText;
            /* The column-statistics text with one more predicate: another shape (a changed table name is the unit test's pair). */
            var otherTable = columnStats.Replace(
                "c.relkind IN ('r', 'm', 'p')", "c.relkind IN ('r', 'm', 'p') AND c.relnatts <> 3", StringComparison.Ordinal);
            Assert.NotEqual(columnStats, otherTable);
            const string AppOnly = "SELECT count(*) AS pm7681_app_only FROM pg_catalog.pg_class WHERE relkind IN ('r', 'v') AND relpages > 1";

            List<Entry> entries;
            ServerRuntime runtime;
            await using (var target = new NpgsqlConnection(scratch.ConnectionString))
            {
                await target.OpenAsync(ct);
                try
                {
                    await DarlingMcpTestData.ExecAsync(target, ct, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements");
                    await DarlingMcpTestData.ExecAsync(target, ct,
                        "DO $do$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '" + AppRole + "') THEN CREATE ROLE " + AppRole + " NOLOGIN; END IF; END $do$");
                    await DarlingMcpTestData.ExecAsync(target, ct, "GRANT " + AppRole + " TO CURRENT_USER");
                    await using var probe = new NpgsqlCommand("SELECT count(*) FROM pg_stat_statements", target);
                    await probe.ExecuteScalarAsync(ct);
                }
                catch (PostgresException ex) when (ex.SqlState is "55000" or "58P01" or "0A000" or "42501")
                {
                    Assert.Skip("pg_stat_statements or role setup is not available on this server: " + ex.Message);
                }

                /* The old build: the service's statements, unmarked. These create the entries, and keep their text. */
                await DarlingMcpTestData.ExecAsync(target, ct, columnStats);
                await DarlingMcpTestData.ExecAsync(target, ct, tableBloat);
                /* The application: the service's column-statistics text under another login (its own entry, same queryid),
                   the same text with one table name changed, and a statement of another shape, under the service's login. */
                await DarlingMcpTestData.ExecAsync(target, ct, "SET ROLE " + AppRole);
                await DarlingMcpTestData.ExecAsync(target, ct, columnStats);
                await DarlingMcpTestData.ExecAsync(target, ct, "RESET ROLE");
                await DarlingMcpTestData.ExecAsync(target, ct, otherTable);
                await DarlingMcpTestData.ExecAsync(target, ct, AppOnly);

                /* The new build: the same statements, marked. They add calls to the old entries; the stored text stays unmarked. */
                foreach (var sql in new[] { columnStats, tableBloat })
                {
                    await using var marked = PostgresTargetProvider.Instance.CreateCommand(new CollectorQuery(sql), target, 60);
                    await marked.ExecuteNonQueryAsync(ct);
                }

                entries = await ReadEntriesAsync(target, ct);
                var serviceColumnStats = Assert.Single(entries, e => e.Name == "column_stats" && e.RoleName != AppRole);
                var serviceBloat = Assert.Single(entries, e => e.Name == "bloat");
                Assert.False(PgOwnStatement.IsOwn(serviceColumnStats.Text), "the setup must leave an unmarked stored text");
                Assert.False(PgOwnStatement.IsOwn(serviceBloat.Text), "the setup must leave an unmarked stored text");
                Assert.True(serviceColumnStats.Calls >= 2, "column_stats calls " + serviceColumnStats.Calls);
                Assert.True(serviceBloat.Calls >= 2, "bloat calls " + serviceBloat.Calls);
                Assert.Equal(serviceColumnStats.QueryId, Assert.Single(entries, e => e.Name == "column_stats" && e.RoleName == AppRole).QueryId);

                runtime = RuntimeFor(scratch.ConnectionString, target.PostgreSqlVersion.Major);
            }

            /* The real hourly fetch, then the real upsert. */
            var (queryIds, texts, ownEntries) = await DarlingWorker.ReadPgStatementTextAsync(runtime, ct);
            var ownByQueryId = new Dictionary<long, string?>();
            for (var i = 0; i < queryIds.Count; i++)
            {
                ownByQueryId[queryIds[i]] = ownEntries[i];
            }

            var columnStatsService = Assert.Single(entries, e => e.Name == "column_stats" && e.RoleName != AppRole);
            var bloat = Assert.Single(entries, e => e.Name == "bloat");
            var variant = Assert.Single(entries, e => e.Name == "other_table");
            var appOnly = Assert.Single(entries, e => e.Name == "app_only");
            var columnStatsApp = Assert.Single(entries, e => e.Name == "column_stats" && e.RoleName == AppRole);

            /* The service's entries are learned though no stored text carries the marker; the shared queryid lists the service's
               pair only, never the application login's. */
            Assert.Equal(new[] { (columnStatsService.UserId, columnStatsService.DatabaseId) }, Pairs(ownByQueryId[columnStatsService.QueryId]));
            Assert.Equal(new[] { (bloat.UserId, bloat.DatabaseId) }, Pairs(ownByQueryId[bloat.QueryId]));
            /* The application's statements, under the same login or another, stay unknown. */
            Assert.Null(ownByQueryId[variant.QueryId]);
            Assert.Null(ownByQueryId[appOnly.QueryId]);
            Assert.NotEqual(columnStatsService.UserId, columnStatsApp.UserId);

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

            for (var i = 6; i >= 1; i--)
            {
                var at = DateTime.UtcNow.AddMinutes(-i);
                await PgOwnStatementLiveTests.SeedStatsAsync(store, ct, at, columnStatsService.QueryId, columnStatsService.UserId, 30, 30_000, columnStatsService.DatabaseId);
                await PgOwnStatementLiveTests.SeedStatsAsync(store, ct, at, columnStatsApp.QueryId, columnStatsApp.UserId, 10, 200, columnStatsApp.DatabaseId);
                await PgOwnStatementLiveTests.SeedStatsAsync(store, ct, at, bloat.QueryId, bloat.UserId, 30, 30_000, bloat.DatabaseId);
                await PgOwnStatementLiveTests.SeedStatsAsync(store, ct, at, variant.QueryId, variant.UserId, 10, 200, variant.DatabaseId);
                await PgOwnStatementLiveTests.SeedStatsAsync(store, ct, at, appOnly.QueryId, appOnly.UserId, 10, 200, appOnly.DatabaseId);
            }

            var from = DateTime.UtcNow.AddHours(-1);
            var to = DateTime.UtcNow.AddMinutes(1);
            var naiveFrom = DateTime.SpecifyKind(from, DateTimeKind.Unspecified);
            var naiveTo = DateTime.SpecifyKind(to, DateTimeKind.Unspecified);

            /* NotOwnOnStats through the fact collector's top statements: the table-bloat statement is out, the column-statistics
               queryid keeps only the application login's 6 x 200 ms, and the application's own statements keep all of theirs. */
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

            Assert.DoesNotContain(bloat.QueryId, windowMs.Keys);
            Assert.Equal(1200, windowMs[columnStatsService.QueryId]);
            Assert.Equal(1200, windowMs[variant.QueryId]);
            Assert.Equal(1200, windowMs[appOnly.QueryId]);

            var page = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(dataSource, PgOwnStatementLiveTests.ServerId, from, to, 10, ct);
            Assert.True(Assert.Single(page.Rows, r => r.QueryId == bloat.QueryId).IsServiceStatement);
            Assert.False(Assert.Single(page.Rows, r => r.QueryId == columnStatsService.QueryId).IsServiceStatement);
            Assert.False(Assert.Single(page.Rows, r => r.QueryId == variant.QueryId).IsServiceStatement);
            Assert.False(Assert.Single(page.Rows, r => r.QueryId == appOnly.QueryId).IsServiceStatement);
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
            await DarlingMcpTestData.ExecAsync(admin, ct, "DROP ROLE IF EXISTS " + AppRole);
        }
    }

    private sealed record Entry(string Name, long QueryId, long UserId, long DatabaseId, string RoleName, string Text, long Calls);

    /// <summary>The target's entries for this test's statements, named by a fragment of their text.</summary>
    private static async Task<List<Entry>> ReadEntriesAsync(NpgsqlConnection target, CancellationToken ct)
    {
        const string Sql = @"
SELECT CASE
           WHEN s.query LIKE '%c.relnatts%' THEN 'other_table'
           WHEN s.query LIKE '%cardinality(s.most\_common\_vals)%' THEN 'column_stats'
           WHEN s.query LIKE '%estimated\_heap\_pages%' THEN 'bloat'
           ELSE 'app_only' END,
       s.queryid, s.userid::bigint, s.dbid::bigint, r.rolname::text, s.query, s.calls
FROM pg_stat_statements AS s
JOIN pg_roles AS r ON r.oid = s.userid
WHERE s.dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
AND   s.query NOT LIKE '%pg\_stat\_statements%'
AND   s.query NOT LIKE 'CREATE %'
AND   (s.query LIKE '%c.relnatts%' OR s.query LIKE '%cardinality(s.most\_common\_vals)%'
       OR s.query LIKE '%estimated\_heap\_pages%' OR s.query LIKE '%AS pm7681\_app\_only%')
ORDER BY 1, 3";
        var entries = new List<Entry>();
        await using var command = new NpgsqlCommand(Sql, target);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            entries.Add(new Entry(
                reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(3),
                reader.GetString(4), reader.GetString(5), reader.GetInt64(6)));
        }

        return entries;
    }

    private static (long User, long Database)[] Pairs(string? ownEntriesJson)
    {
        Assert.NotNull(ownEntriesJson);
        using var document = JsonDocument.Parse(ownEntriesJson);
        return document.RootElement.EnumerateArray()
            .Select(e => (e.GetProperty("u").GetInt64(), e.GetProperty("d").GetInt64()))
            .ToArray();
    }

    private static ServerRuntime RuntimeFor(string scratchConnectionString, int major)
    {
        var scratch = new NpgsqlConnectionStringBuilder(scratchConnectionString);
        var config = new MonitoredServer
        {
            Name = "pm7681-target",
            Host = scratch.Host ?? "127.0.0.1",
            Port = scratch.Port,
            Database = scratch.Database,
            Engine = "postgres",
            Auth = "sql",
            Username = scratch.Username,
            EncryptMode = "Optional",
        };
        return new ServerRuntime
        {
            Config = config,
            ConnectionString = MonitoredServerConnection.BuildConnectionString(config, scratch.Password ?? "unused"),
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
                PostgresVersionNum = major * 10000,
            },
            StorageName = "pm7681-target",
            ServerId = PgOwnStatementLiveTests.ServerId,
        };
    }
}
