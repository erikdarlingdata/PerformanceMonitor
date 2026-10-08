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
/// #5582, live: the member count of a Query Store RankedTimeSeries panel's group dimension, read from <c>pg_stats.n_distinct</c> of
/// the wide table. Covers the partitioned parent (V171: <c>inherited = true</c>), a plain table (<c>inherited = false</c>), a
/// negative <c>n_distinct</c> (a fraction of <c>reltuples</c>), a table never analyzed (unknown), and the dimensions that take no
/// lookup (<c>server</c>, which is the servers in scope, and <c>query_hash</c>, which is unbounded).
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class QueryStoreGroupMembersLiveTests
{
    private static string Panel(string groupBy) =>
        "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":10,"
        + "\"groupBy\":[\"" + groupBy + "\"],\"viz\":\"line\"}";

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    [Fact]
    public async Task TheMembers_ComeFromPgStats_OfThePartitionedParentAndOfAPlainTable()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5582 group-member live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        /* The wide table is a partitioned parent since V171, so the statistics the planner keeps for it are the inherited ones. */
        Assert.Equal("p", (await ScalarAsync(connection,
            "SELECT relkind::text FROM pg_class WHERE oid = 'collect.query_store_interval_wide'::regclass", ct))!.ToString());

        var moduleName = QueryStoreRankedHarness.Parse(Panel("module_name"));
        var databaseName = QueryStoreRankedHarness.Parse(Panel("database_name"));

        /* Never analyzed: no statistics row, so the count is unknown (the compiler then takes two scans). */
        Assert.Null(await QueryStoreGroupMembers.ResolveAsync(connection, moduleName, 3, ct));

        /* 6,000 rows: 50 modules, and a database name that is different on every row. */
        await ExecAsync(connection,
            "INSERT INTO collect.query_store_interval_wide (collection_time, server_id, first_execution_time, query_id, plan_id, module_name, database_name) "
            + "SELECT TIMESTAMP '2026-08-01 00:00:00' + (g * interval '1 second'), -5582100, TIMESTAMP '2026-08-01 00:00:00' + (g * interval '1 second'), g, g, "
            + "'m' || (g % 50), 'db' || g FROM generate_series(1, 6000) AS g", ct);
        await ExecAsync(connection, "ANALYZE collect.query_store_interval_wide", ct);

        /* A positive n_distinct is the count itself. */
        Assert.Equal(50L, await QueryStoreGroupMembers.ResolveAsync(connection, moduleName, 3, ct));

        /* A negative n_distinct (-1: every row distinct) is a fraction of reltuples, so it is the row count. */
        var reltuples = Convert.ToInt64(await ScalarAsync(connection,
            "SELECT reltuples::bigint FROM pg_class WHERE oid = 'collect.query_store_interval_wide'::regclass", ct), System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal(6000L, reltuples);
        Assert.Equal(6000L, await QueryStoreGroupMembers.ResolveAsync(connection, databaseName, 3, ct));

        /* The partitioned parent has its statistics inherited, and no non-inherited row: the lookup must ask for the inherited ones. */
        Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection,
            "SELECT count(*) FROM pg_stats WHERE schemaname = 'collect' AND tablename = 'query_store_interval_wide' AND attname = 'module_name' AND inherited", ct),
            System.Globalization.CultureInfo.InvariantCulture));
        Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection,
            "SELECT count(*) FROM pg_stats WHERE schemaname = 'collect' AND tablename = 'query_store_interval_wide' AND attname = 'module_name' AND NOT inherited", ct),
            System.Globalization.CultureInfo.InvariantCulture));

        /* server is the servers in scope, exactly; the product of several dimensions is the product of the counts. */
        var serverOnly = QueryStoreRankedHarness.Parse(Panel("server"));
        Assert.Equal(43L, await QueryStoreGroupMembers.ResolveAsync(connection, serverOnly, 43, ct));
        var both = QueryStoreRankedHarness.Parse(
            "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"timeBucket\":\"hour\",\"topN\":10,"
            + "\"groupBy\":[\"server\",\"module_name\"],\"viz\":\"line\"}");
        Assert.Equal(150L, await QueryStoreGroupMembers.ResolveAsync(connection, both, 3, ct));

        /* query_hash has no bound this lookup can give: unknown. A panel that is not a RankedTimeSeries asks for nothing. */
        Assert.Null(await QueryStoreGroupMembers.ResolveAsync(connection, QueryStoreRankedHarness.Parse(Panel("query_hash")), 3, ct));
        Assert.Null(await QueryStoreGroupMembers.ResolveAsync(connection, QueryStoreRankedHarness.Parse(
            "{\"source\":\"query_store_stats\",\"ratio\":\"qs_total_cpu_us\",\"topN\":10,\"groupBy\":[\"module_name\"],\"viz\":\"table\"}"), 3, ct));

        /* A plain table (the wide table before V171): the same lookup, on the non-inherited statistics. */
        await ExecAsync(connection, "CREATE TABLE collect.qsgm_plain (module_name text, database_name text)", ct);
        await ExecAsync(connection,
            "INSERT INTO collect.qsgm_plain SELECT 'm' || (g % 20), 'db' || g FROM generate_series(1, 3000) AS g", ct);
        await ExecAsync(connection, "ANALYZE collect.qsgm_plain", ct);
        Assert.Equal(20d, await QueryStoreGroupMembers.NDistinctMembersAsync(connection, "module_name", ct, "qsgm_plain"));
        Assert.Equal(3000d, await QueryStoreGroupMembers.NDistinctMembersAsync(connection, "database_name", ct, "qsgm_plain"));

        /* A column or table with no statistics row is unknown, not zero. */
        Assert.Null(await QueryStoreGroupMembers.NDistinctMembersAsync(connection, "no_such_column", ct, "qsgm_plain"));
        Assert.Null(await QueryStoreGroupMembers.NDistinctMembersAsync(connection, "module_name", ct, "no_such_table"));
    }

    [Fact]
    public async Task TheRunner_PassesTheMembersOut_OnlyForARankedTimeSeriesPanelOnTheWideRoute()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5582 group-member live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* An empty store is not wide-eligible, whatever the plan: nothing to count, and the default tuple says so. */
        var none = await DarlingWebEndpoints.ResolveQueryStoreWideEligibleAsync(
            postgres, null, DateTime.UtcNow.AddDays(-2), DateTime.UtcNow, null, ct, QueryStoreRankedHarness.Parse(Panel("module_name")));
        Assert.False(none.Eligible);
        Assert.Null(none.GroupMembers);
    }
}
