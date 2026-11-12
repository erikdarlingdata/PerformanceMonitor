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
/// Darling rung V264 (#8691): the table that holds the oversized-plan backlog's dropped counts by the hour. A new table and its
/// key, and nothing else: no row of <c>collect.oversized_plan_backlog</c> moves, so an upgrading store keeps the dropped rows an
/// older build held and the read counts them until they age out.
/// </summary>
public sealed class OversizedPlanBacklogDropCountsRungTests
{
    private const string TableName = "collect.oversized_plan_backlog_drops";

    private static PgMigrations.Migration Rung => PgMigrations.Scripts[^1];

    [Fact]
    public void TheRungIsTheNewestOnADenseLadder_AndCreatesTheCountTable()
    {
        Assert.Equal("oversized-plan-backlog-dropped-counts", Rung.Name);
        Assert.Equal(StorageVersion.SchemaVersion, Rung.Version);
        Assert.Contains(TableName, Rung.Sql, StringComparison.Ordinal);

        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
    }

    [Fact]
    public void TheViewerProbe_FindsTheTable_AndMapsItToTheNewestRung()
    {
        Assert.Contains($"to_regclass('{TableName}') IS NOT NULL", ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        var ordinal = parameters.Length - 1;
        Assert.Equal("hasOversizedPlanBacklogDropCounts", parameters[ordinal].Name);

        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(PgMigrations.Scripts[^1].Version, (int)method.Invoke(null, all)!);

        var without = (object[])all.Clone();
        without[ordinal] = false;
        Assert.Equal(PgMigrations.Scripts[^1].Version - 1, (int)method.Invoke(null, without)!);
    }

    /// <summary>
    /// The rung against a store that holds backlog rows, dropped ones among them: every row is still there afterwards and
    /// still dropped, the count table starts empty, and running the rung again changes nothing.
    /// </summary>
    [Fact]
    public async Task TheRungLeavesEveryHeldRowAsItWas_AndRunsTwice()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8691 rung test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        await Run(connection, @"
INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (4501, 'rung-server', TRUE);
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at, expired_at, last_attempt_at)
VALUES
    (4501, 'query_stats', '0xDROPPED', '0xSQL1', 0, 400, 'rung_db', '0xHASH1', 600001, now() - interval '3 hours', now() - interval '3 hours', now() - interval '1 hour', NULL),
    (4501, 'query_stats', '0xWAITING', '0xSQL2', 0, 400, 'rung_db', '0xHASH2', 600002, now() - interval '3 hours', now() - interval '3 hours', NULL, NULL);", ct);

        /* An older store has no count table. The rung makes it, twice, over the rows that are there. */
        await Run(connection, $"DROP TABLE IF EXISTS {TableName}", ct);
        await Run(connection, Rung.Sql, ct);
        await Run(connection, Rung.Sql, ct);

        Assert.Equal(2L, await Scalar(connection, "SELECT count(*) FROM collect.oversized_plan_backlog", ct));
        Assert.Equal(1L, await Scalar(connection,
            "SELECT count(*) FROM collect.oversized_plan_backlog WHERE expired_at IS NOT NULL AND last_attempt_at IS NULL AND captured_at IS NULL", ct));
        Assert.Equal(0L, await Scalar(connection, $"SELECT count(*) FROM {TableName}", ct));
        Assert.Equal(1L, await Scalar(connection,
            "SELECT count(*) FROM pg_constraint WHERE conname = 'pk_oversized_plan_backlog_drops'", ct));
    }

    private static async Task Run(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<long> Scalar(NpgsqlConnection connection, string sql, System.Threading.CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }
}
