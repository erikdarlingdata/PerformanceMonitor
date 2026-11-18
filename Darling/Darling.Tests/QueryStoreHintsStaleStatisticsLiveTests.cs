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
using Darling.Tests;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres, so its seed is the only data the plan sees. */

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #8909: <c>get_query_store_hints</c> timed out (60 s) on a store whose <c>collect.query_store_hints</c> statistics were a day old. The table is
/// plain, so the newest hours of a 24 hour window lay past the end of the <c>collection_time</c> histogram, the planner estimated the window at one row,
/// and the absence step ran as nested loops over the window rows (0.7 s with fresh statistics). The test seeds an analyzed history and then 25 hours of
/// captures with no analyze (autovacuum off on the table, so nothing refreshes the statistics), and reads the hints with a five second command timeout.
/// </summary>
public sealed class QueryStoreHintsStaleStatisticsLiveTests
{
    private const int Databases = 2;
    private const int HintsPerDatabase = 30;

    [Fact]
    public async Task TheHintsRead_WithStaleStatisticsOnTheWindow_FinishesAndLeavesThePooledConnectionAsItWas()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live hints tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        const int serverId = 89091;

        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        var succeeded = false;
        try
        {
            var to = DateTime.SpecifyKind(LiveClock.Now(), DateTimeKind.Unspecified);
            to = new DateTime(to.Year, to.Month, to.Day, to.Hour, to.Minute, 0, DateTimeKind.Unspecified);
            var from = to.AddHours(-24);

            await ExecAsync(connection, ct, "ALTER TABLE collect.query_store_hints SET (autovacuum_enabled = false)");
            await SeedAsync(connection, ct, serverId, to.AddHours(-73), to.AddHours(-25));
            await ExecAsync(connection, ct, "ANALYZE collect.query_store_hints");
            await SeedAsync(connection, ct, serverId, to.AddHours(-25).AddMinutes(5), to);

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { MaxPoolSize = 1 };
            await using var dataSource = NpgsqlDataSource.Create(builder.ConnectionString);

            var read = await DarlingQueryStoreHintReader.ReadAsync(dataSource, serverId, DatabaseFilter.All, null, from, to, commandTimeoutSeconds: 5, cancellationToken: ct);

            Assert.Equal(Databases * HintsPerDatabase, read.Hints.Count);
            Assert.Equal(Databases * HintsPerDatabase, read.TotalHints);
            Assert.Equal(Databases, read.Captures.Count);
            Assert.All(read.Captures, c => Assert.Equal(to, c.NewestCapture));
            Assert.All(read.Hints, h =>
            {
                Assert.Equal(to, h.LastSeen);
                Assert.InRange(h.FirstSeen, from, from.AddMinutes(5));
            });

            await using var after = await dataSource.OpenConnectionAsync(ct);
            await using var settings = new NpgsqlCommand("SELECT current_setting('enable_nestloop'), current_setting('transaction_read_only')", after);
            await using var reader = await settings.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(("on", "off"), (reader.GetString(0), reader.GetString(1)));
            succeeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, succeeded, async (cleanupConnection, cleanupCt) =>
                await ExecAsync(cleanupConnection, cleanupCt, "DELETE FROM collect.query_store_hints WHERE server_id = 89091"));
        }
    }

    /// <summary>One capture every five minutes from <paramref name="first"/> to <paramref name="last"/>: a marker row per database and a row per hint, every hint present in every capture.</summary>
    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, DateTime first, DateTime last)
    {
        await ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_hints (collection_id, collection_time, server_id, server_name, database_name)
              SELECT $1 + row_number() OVER (), t, $4, 'a8909', 'Db' || d
              FROM generate_series($2::timestamp, $3::timestamp, interval '5 minutes') t CROSS JOIN generate_series(1, $5) d",
            CollectionIdGenerator.Next(), first, last, serverId, Databases);
        await ExecAsync(connection, ct,
            @"INSERT INTO collect.query_store_hints
                  (collection_id, collection_time, server_id, server_name, database_name, query_hint_id, query_id, query_hint_text, query_hint_failure_count, source_desc)
              SELECT $1 + row_number() OVER (), t, $4, 'a8909', 'Db' || d, i, 1000 + i, 'OPTION (MAXDOP 1)', 0, 'USER'
              FROM generate_series($2::timestamp, $3::timestamp, interval '5 minutes') t CROSS JOIN generate_series(1, $5) d CROSS JOIN generate_series(1, $6) i",
            CollectionIdGenerator.Next() + 50000000, first, last, serverId, Databases, HintsPerDatabase);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, CancellationToken ct, string sql, params object[] parameters)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 120 };
        foreach (var value in parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = value });
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
