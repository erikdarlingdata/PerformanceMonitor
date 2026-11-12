/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the live test mints its own scratch database through ScratchPostgres and touches nothing shared. */

/// <summary>
/// #8716: V266 sets fillfactor 80 on <c>collect.query_tag_sightings</c>, so a re-sighting's new row version has room on its page and can be a
/// HOT update. The rung is metadata only (an existing page keeps its fill), and the Viewer's probe finds it by the table's reloptions.
/// </summary>
public sealed class QueryTagSightingsFillfactorRungTests
{
    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8716 fillfactor rung test.";

    [Fact]
    public void TheProbeMapsTheFillfactorSentinelToV266_AndWithoutItToV265()
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal("hasQueryTagSightingsFillfactor", parameters[^1].Name);

        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(266, (int)method.Invoke(null, all)!);

        var without = (object[])all.Clone();
        without[^1] = false;
        Assert.Equal(265, (int)method.Invoke(null, without)!);
    }

    [Fact]
    public async Task TheRungSetsTheFillfactor_RunsTwice_AndTheViewerProbeSeesIt()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), Skip);
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(connectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        const string reloptions = "SELECT reloptions::text FROM pg_class WHERE oid = 'collect.query_tag_sightings'::regclass";
        var rung = PgMigrations.Scripts.Single(r => r.Version == 266);
        Assert.Equal("query-tag-sightings-fillfactor", rung.Name);

        await using (var rewind = new NpgsqlCommand("ALTER TABLE collect.query_tag_sightings RESET (fillfactor)", connection))
        {
            await rewind.ExecuteNonQueryAsync(ct);
        }

        await using (var before = new NpgsqlCommand(reloptions, connection))
        {
            Assert.True(await before.ExecuteScalarAsync(ct) is DBNull);
        }

        for (var run = 0; run < 2; run++)
        {
            await using var apply = new NpgsqlCommand(rung.Sql, connection);
            await apply.ExecuteNonQueryAsync(ct);
        }

        await using (var after = new NpgsqlCommand(reloptions, connection))
        {
            Assert.Equal("{fillfactor=80}", (string?)await after.ExecuteScalarAsync(ct));
        }

        await using (var probe = new NpgsqlCommand(ViewerDataService.StoreSchemaProbeSql, connection))
        await using (var reader = await probe.ExecuteReaderAsync(ct))
        {
            Assert.True(await reader.ReadAsync(ct));
            Assert.True(reader.GetBoolean(reader.FieldCount - 1));
        }
    }
}
