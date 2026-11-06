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
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each fact mints its own scratch databases through ScratchPostgres and never touches the shared store. */

/// <summary>
/// #7731: the statement collector stores the NAME of the database each statement ran in, read from the <c>pg_database</c> join at
/// ordinal 31. The same statement runs in two databases of one cluster, so pg_stat_statements holds two entries with the same
/// <c>queryid</c> and different <c>dbid</c>. The collector, run against a real target, must return each entry carrying the name of
/// the database whose oid is its <c>dbid</c>: not the other database's name, and not NULL.
/// </summary>
[Collection("live-postgres")]
public sealed class PgStatementStatsDatabaseNameLiveTests
{
    private const int ServerId = -773101;

    [Fact]
    public async Task TheCollectorNamesEachStatementsOwnDatabase_NotTheOtherOne_AndNeverNull()
    {
        var adminConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(adminConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the statement collector database name test.");

        var ct = TestContext.Current.CancellationToken;

        await using var target = await ScratchPostgres.CreateAsync(adminConnectionString!, ct, emptyStore: true);
        await using var first = await ScratchPostgres.CreateAsync(adminConnectionString!, ct, emptyStore: true);
        await using var second = await ScratchPostgres.CreateAsync(adminConnectionString!, ct, emptyStore: true);

        /* pg_stat_statements is cluster-wide: the view in the target database lists the entries of every database. */
        await ExecAsync(target.ConnectionString, "CREATE EXTENSION IF NOT EXISTS pg_stat_statements", ct);

        /* The same statement text in two databases: one queryid, two dbids. */
        foreach (var database in new[] { first, second })
        {
            for (var run = 0; run < 3; run++)
            {
                await ExecAsync(database.ConnectionString, "SELECT 773100 + 1 AS pm7731_database_name", ct);
            }
        }

        var firstOid = await DatabaseOidAsync(target.ConnectionString, first.DatabaseName, ct);
        var secondOid = await DatabaseOidAsync(target.ConnectionString, second.DatabaseName, ct);
        Assert.NotEqual(firstOid, secondOid);

        await using var connection = new NpgsqlConnection(target.ConnectionString);
        await connection.OpenAsync(ct);

        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = "pm7731-target",
            CollectionTime = LiveClock.Now(),
            Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = connection.PostgreSqlVersion.Major },
        };
        Assert.True(await DarlingCollectorRunner.ResolvePgExtensionSchemasAsync(
            context, PgStatementStatsCollector.Instance, connection, null, ct));

        List<PgStatementStatsCollector.Row> rows;
        await using (var command = new NpgsqlCommand(PgStatementStatsCollector.Instance.BuildQuery(context).Text, connection))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            rows = await PgStatementStatsCollector.Instance.ReadAsync(reader, context, ct);
        }

        var names = new Dictionary<long, string>();
        await using (var catalog = new NpgsqlCommand("SELECT oid::bigint, datname::text FROM pg_catalog.pg_database", connection))
        await using (var reader = await catalog.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                names[reader.GetInt64(0)] = reader.GetString(1);
            }
        }

        var ofFirst = rows.Where(r => r.DatabaseId == firstOid).ToList();
        var ofSecond = rows.Where(r => r.DatabaseId == secondOid).ToList();
        Assert.NotEmpty(ofFirst);
        Assert.NotEmpty(ofSecond);
        Assert.All(ofFirst, r => Assert.Equal(first.DatabaseName, r.DatabaseName));
        Assert.All(ofSecond, r => Assert.Equal(second.DatabaseName, r.DatabaseName));

        /* The shared statement shape appears in both, with one queryid, so a join on the wrong column or an ordinal off by one cannot pass. */
        Assert.Contains(ofFirst.Select(r => r.QueryId), id => ofSecond.Any(s => s.QueryId == id));

        /* Every collected row, in whatever database it ran, names the database its dbid points at. An entry left behind by a database
           that was dropped since has no catalog row, so its name is NULL and the row still ships (the LEFT join). */
        Assert.All(rows, r =>
        {
            names.TryGetValue(r.DatabaseId, out var expected);
            Assert.Equal(expected, r.DatabaseName);
        });
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> DatabaseOidAsync(string connectionString, string database, CancellationToken ct)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new NpgsqlCommand("SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname = $1", connection);
        command.Parameters.AddWithValue(database);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }
}
