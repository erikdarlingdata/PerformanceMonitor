/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4637: an AVG panel read from the hourly rollup must equal the same panel read from raw, to the last digit.
/// Raw divides in <c>numeric</c> (<c>avg(bigint)</c>) and rounds once to double; the rollup route used to round
/// the SUM to double first and divide in floating point, so a group sum above 2^53 could differ by one unit in
/// the last place. The pin plants one group whose worker-time sum is above 2^53 and whose average is not
/// exactly representable, compiles the same panel as raw and as the rollup route, runs both, and compares the
/// doubles with <c>==</c>.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ComposeRollupAvgNumericLiveTests
{
    private const int ServerId = -463701;
    private const string ServerName = "avg-numeric-4637";

    [Fact]
    public async Task RollupAvg_EqualsRawAvg_ForAGroupSumAboveTwoToThe53()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string (with TimescaleDB installed) to run the #4637 rollup AVG live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var timescaleEnabled = await TimescaleSupport.TryEnableAsync(connection, null, ct);
        Assert.SkipWhen(!timescaleEnabled, "The #4637 pin needs TimescaleDB: the rollup route reads a continuous aggregate.");
        await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        var hour = DateTime.SpecifyKind(DateTime.UtcNow.Date.AddDays(-10).AddHours(10), DateTimeKind.Unspecified);
        for (var i = 0; i < 6; i++)
        {
            await using var insert = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES ($1, $2, $3, $4, 'AvgDb', 'HASHAVG', '0xAVG', $5, $5, 10, 300)", connection);
            insert.Parameters.AddWithValue((long)(i + 1));
            insert.Parameters.AddWithValue(hour.AddMinutes(5 + i));
            insert.Parameters.AddWithValue(ServerId);
            insert.Parameters.AddWithValue(ServerName);
            insert.Parameters.AddWithValue(44_000_000_000_000_005L + (i * 3L));
            await insert.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureContinuousAggregatesAsync(connection, null, ct);
        foreach (var view in new[] { TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsIntervalHourlyView })
        {
            await using var refresh = new NpgsqlCommand($"CALL refresh_continuous_aggregate('collect.{view}'::regclass, $1::timestamp, $2::timestamp)", connection);
            refresh.Parameters.AddWithValue(hour.AddDays(-1));
            refresh.Parameters.AddWithValue(hour.AddDays(1));
            await refresh.ExecuteNonQueryAsync(ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var rollups = await TimescaleSupport.DetectRollupsAsync(dataSource, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(dataSource, rollups, ct);

        var json = (JsonObject)JsonNode.Parse(
            "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"avg\",\"viz\":\"table\"}")!;
        var (plan, parseError) = ComposeSpec.TryParsePanel(json, Array.Empty<string>());
        Assert.True(parseError is null, parseError);

        var start = hour.AddHours(-1);
        var end = hour.AddHours(2);
        var rawContext = new ComposeRunContext(new[] { ServerName }, start, end, ComposeRunContext.NoVariables, rollups, end.AddMinutes(1), coverage);
        var rollupContext = rawContext with { NowUtc = end.AddDays(5) };

        var raw = await RunAsync(connection, plan!, rawContext, ct);
        var rollup = await RunAsync(connection, plan!, rollupContext, ct);

        Assert.Equal(ComposeRoute.Raw, raw.Route);
        Assert.NotEqual(ComposeRoute.Raw, rollup.Route);
        Assert.True(raw.Value == rollup.Value, $"raw {raw.Value:R} != rollup {rollup.Value:R}");
    }

    private static async Task<(double Value, ComposeRoute Route)> RunAsync(
        NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        return (Convert.ToDouble(reader.GetValue(reader.FieldCount - 1)), compiled.Route);
    }
}
