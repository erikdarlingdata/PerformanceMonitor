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
using System.Reflection;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every test here mints its own scratch database through ScratchPostgres and touches nothing
   shared, so it needs no place in the live-postgres collection. */

/// <summary>
/// #8570: rung V267 puts <c>ix_plan_regression_daily_server_day (server_id, day)</c> on the per-day plan totals, so a read that names one
/// server and a set of days and no query walks two narrow columns instead of the unique key's seven. The probe finds the rung by the index.
/// </summary>
public sealed class PlanRegressionDailyServerDayIndexLiveTests
{
    private const int RungVersion = 267;

    private const string IndexName = "ix_plan_regression_daily_server_day";

    private const string Skip = "Set DARLING_TEST_PG to a Postgres connection string to run the live #8570 per-day totals index tests (they mint their own scratch databases).";

    [Fact]
    public void TheProbeMapsTheIndexSentinelToV267_AndWithoutItToV266()
    {
        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var ordinal = Array.FindIndex(parameters, p => p.Name == "hasPlanRegressionDailyServerDayIndex");
        Assert.True(ordinal >= 0);

        /* Every sentinel but the newer rungs' answers V267; without the index the store maps one rung lower. */
        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        for (var i = ordinal + 1; i < parameters.Length; i++)
        {
            all[i] = false;
        }

        Assert.Equal(RungVersion, (int)method.Invoke(null, all)!);

        var without = (object[])all.Clone();
        without[ordinal] = false;
        Assert.Equal(RungVersion - 1, (int)method.Invoke(null, without)!);
    }

    /// <summary>A store the ladder migrated has the index on its two columns, the probe names the rung by it, and running the rung again
    /// after a rewind builds it again.</summary>
    [Fact]
    public async Task AMigratedStore_HasTheIndex_AndTheProbeNamesItsRung()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);

        Assert.Equal(new[] { "server_id", "day" }, await KeyColumnsAsync(connection, ct));
        // Newer rungs move the probe past this one: drop their sentinels so the probe names this rung.
        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion, ct);
        Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));

        await RungRewindArtifacts.DropSentinelsAboveAsync(connection, RungVersion - 1, ct);
        Assert.Null(await KeyColumnsAsync(connection, ct));
        Assert.Equal(RungVersion - 1, await ProbedVersionAsync(connection, ct));

        var rung = PgMigrations.Scripts.Single(m => m.Version == RungVersion);
        Assert.Equal("plan-regression-daily-server-day-index", rung.Name);
        for (var run = 0; run < 2; run++)
        {
            await using var again = new NpgsqlCommand(rung.Sql, connection);
            await again.ExecuteNonQueryAsync(ct);
        }

        Assert.Equal(new[] { "server_id", "day" }, await KeyColumnsAsync(connection, ct));
        Assert.Equal(RungVersion, await ProbedVersionAsync(connection, ct));
    }

    /// <summary>
    /// 192,000 totals rows over eight servers and fifteen days: the builder's day delete, which names a server and a day and no query, finds
    /// its rows through the new index rather than the unique key.
    /// </summary>
    [Fact]
    public async Task TheDayDelete_ReadsTheServerDayIndex()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), Skip);

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = await MigratedConnectionAsync(scratch, ct);
        await ExecuteAsync(connection, @"
INSERT INTO collect.plan_regression_daily
    (server_id, day, database_name, query_id, plan_id, replica_role, query_plan_hash, execs, cpu_us_sum, dur_us_sum,
     last_exec, is_forced_plan, force_failure_count, rowcount_sum)
SELECT s, DATE '2026-06-01' + d, 'db_' || (n % 37), (n + 1) / 2, n, NULL, md5(n::text), 100, 1000000, 3000000,
       TIMESTAMP '2026-06-01 12:00' + d * INTERVAL '1 day', false, 0, 500
FROM generate_series(1, 8) AS s
CROSS JOIN generate_series(0, 14) AS d
CROSS JOIN generate_series(1, 1600) AS n;", ct);
        await ExecuteAsync(connection, "VACUUM (ANALYZE) collect.plan_regression_daily", ct);

        await using var command = new NpgsqlCommand("EXPLAIN (FORMAT JSON) " + PlanRegressionDaily.DeleteDaySql, connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, 3);
        command.Parameters.AddWithValue(NpgsqlDbType.Date, new DateOnly(2026, 6, 8));
        var json = (string)(await command.ExecuteScalarAsync(ct))!;
        using var document = JsonDocument.Parse(json);
        var indexes = new List<string>();
        Walk(document.RootElement[0].GetProperty("Plan"), indexes);

        Assert.Contains(IndexName, indexes);
    }

    private static void Walk(JsonElement plan, List<string> indexes)
    {
        if (plan.TryGetProperty("Index Name", out var index))
        {
            indexes.Add(index.GetString()!);
        }

        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                Walk(child, indexes);
            }
        }
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string[]?> KeyColumnsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
SELECT ARRAY
(
    SELECT a.attname::text
    FROM unnest(i.indkey::smallint[]) WITH ORDINALITY AS k (attnum, ordinal)
    JOIN pg_attribute AS a
      ON  a.attrelid = i.indrelid
      AND a.attnum = k.attnum
    ORDER BY k.ordinal
)
FROM pg_index AS i
JOIN pg_class AS c
  ON c.oid = i.indexrelid
WHERE i.indrelid = 'collect.plan_regression_daily'::regclass
AND   c.relname = $1;", connection);
        command.Parameters.AddWithValue(IndexName);
        return (string[]?)await command.ExecuteScalarAsync(ct);
    }

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
}
