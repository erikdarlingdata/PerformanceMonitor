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
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7565: a Custom View that charts a per-second perfmon counter (<c>Transactions/sec</c>) with <c>perfmon_value</c> draws SQL
/// Server's running total, a line that only climbs. <c>perfmon_value_per_second</c> divides the stored delta by the stored
/// interval seconds, so a counter that rises by 600 over 60 seconds charts about 10 per second, and a counter that is a
/// point-in-time reading (it stores no delta) charts nothing rather than a made-up rate.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only to CREATE
   and DROP its own database through ScratchPostgres, then works entirely inside it. */
public sealed class ComposePerfmonRateLiveTests
{
    private const int ServerId = -756501;
    private const string ServerName = "perfmon-rate";

    private static readonly DateTime WindowStart = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(2);

    [Fact]
    public async Task APerSecondCounter_ChartsItsRate_NotItsRunningTotal_AgainstDevPostgres()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7565 compose live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        long id = 1;
        async Task InsertAsync(DateTime time, string counter, long value, long? delta, int? interval)
        {
            await using var command = new NpgsqlCommand(
                """
                INSERT INTO collect.perfmon_stats
                    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
                     cntr_value, delta_cntr_value, sample_interval_seconds)
                VALUES ($1, $2, $3, $4, 'SQLServer:Test', $5, '', $6, $7, $8)
                """, connection);
            command.Parameters.AddWithValue(id++);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, time);
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(counter);
            command.Parameters.AddWithValue(value);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Bigint, (object?)delta ?? DBNull.Value);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Integer, (object?)interval ?? DBNull.Value);
            await command.ExecuteNonQueryAsync(ct);
        }

        /* Hour 0: a restart marker row (delta 0 over 0 seconds), then two 60-second samples that each rise by 600. The running total
           starts high, as a real one does after days of uptime. */
        await InsertAsync(WindowStart.AddMinutes(1), "Transactions/sec", 1_000_000, 0, 0);
        await InsertAsync(WindowStart.AddMinutes(2), "Transactions/sec", 1_000_600, 600, 60);
        await InsertAsync(WindowStart.AddMinutes(3), "Transactions/sec", 1_001_200, 600, 60);
        /* Hour 1: a second sample pace, 3000 over 60 seconds, so the rate moves with the workload. */
        await InsertAsync(WindowStart.AddMinutes(61), "Transactions/sec", 1_004_200, 3000, 60);
        /* Another counter in the same window must not leak into the filtered panel. */
        await InsertAsync(WindowStart.AddMinutes(2), "Batch Requests/sec", 5_000, 120, 60);
        /* A point-in-time counter stores no delta (V132), so it has no rate. */
        await InsertAsync(WindowStart.AddMinutes(2), "Total Server Memory (KB)", 8_000_000, null, null);

        var h0 = WindowStart;
        var h1 = WindowStart.AddHours(1);

        /* The ratio: SUM(delta) / SUM(interval). Hour 0 is 1200 / 120 = 10 per second (the marker adds 0 / 0), hour 1 is 3000 / 60 = 50. */
        var rate = await RunAsync(connection, Panel("\"ratio\":\"perfmon_value_per_second\"", "Transactions/sec"));
        Assert.Equal(2, rate.Count);
        Assert.True(Math.Abs(rate[h0] - 10.0) < 0.0001, $"hour 0 rate = {rate[h0]}, expected 10 per second");
        Assert.True(Math.Abs(rate[h1] - 50.0) < 0.0001, $"hour 1 rate = {rate[h1]}, expected 50 per second");

        /* The other counter keeps its own rate: 120 / 60 = 2 per second. */
        var other = await RunAsync(connection, Panel("\"ratio\":\"perfmon_value_per_second\"", "Batch Requests/sec"));
        Assert.True(Math.Abs(other[h0] - 2.0) < 0.0001, $"Batch Requests/sec rate = {other[h0]}, expected 2 per second");

        /* The old pick on the same rows: perfmon_value averages the running total, about a million, which is the line the issue reports. */
        var total = await RunAsync(connection, Panel("\"measure\":\"perfmon_value\",\"aggregate\":\"avg\"", "Transactions/sec"));
        Assert.True(total[h0] > 1_000_000, $"perfmon_value hour 0 = {total[h0]}: the running total, not a rate");

        /* A point-in-time counter has no delta, so its rate is empty and not a number. */
        var memory = await RunAsync(connection, Panel("\"ratio\":\"perfmon_value_per_second\"", "Total Server Memory (KB)"));
        Assert.Empty(memory);
    }

    /// <summary>
    /// The rate is SUM(delta) / SUM(interval seconds), and every instance row of a counter repeats the interval, so two instances that
    /// each rose by 600 over 60 seconds chart 1200 / 120 = 10 per second for the pair, not the 20 per second they add up to. A
    /// panel is exact only per instance, so the validator refuses the unpinned panel, and the panel grouped by instance charts 10
    /// per second for each.
    /// </summary>
    [Fact]
    public async Task TheRate_IsExactPerInstance_AndAnUnpinnedInstancePanelIsRefused_AgainstDevPostgres()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #7565 compose live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

        long id = 1;
        foreach (var instance in new[] { "db_one", "db_two" })
        {
            await using var command = new NpgsqlCommand(
                """
                INSERT INTO collect.perfmon_stats
                    (collection_id, collection_time, server_id, server_name, object_name, counter_name, instance_name,
                     cntr_value, delta_cntr_value, sample_interval_seconds)
                VALUES ($1, $2, $3, $4, 'SQLServer:Test', 'Transactions/sec', $5, 1000600, 600, 60)
                """, connection);
            command.Parameters.AddWithValue(id++);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, WindowStart.AddMinutes(2));
            command.Parameters.AddWithValue(ServerId);
            command.Parameters.AddWithValue(ServerName);
            command.Parameters.AddWithValue(instance);
            await command.ExecuteNonQueryAsync(ct);
        }

        const string Rate = "\"ratio\":\"perfmon_value_per_second\"";
        string Dashboard(string extra) =>
            "{\"panels\":[{\"source\":\"perfmon_stats\"," + Rate + ",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"counter_name\",\"op\":\"eq\",\"value\":\"Transactions/sec\"}," +
            "{\"dimension\":\"server\",\"op\":\"eq\",\"value\":\"" + ServerName + "\"}]" + extra + "}]}";

        /* No instance pin: the panel would read 10 per second where the instances add up to 20, so the validator refuses it. */
        var unpinned = DarlingWebEndpoints.ValidateDefinition(Dashboard(""));
        Assert.False(unpinned.IsValid, "a two-instance panel with no instance pin validated clean");
        Assert.Contains("filter instance_name eq", unpinned.Error, StringComparison.Ordinal);

        /* Grouped by instance it validates, and each instance charts its own 600 / 60 = 10 per second. */
        var grouped = Dashboard(",\"groupBy\":[\"instance_name\"]");
        Assert.True(DarlingWebEndpoints.ValidateDefinition(grouped).IsValid);
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(grouped)!["panels"]![0]!.DeepClone(), Array.Empty<string>());
        Assert.True(error is null, error);
        var context = new ComposeRunContext(new[] { ServerName }, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, context);
        Assert.True(compileError is null, compileError);

        var byInstance = new Dictionary<string, double>();
        await using (var command = new NpgsqlCommand(compiled!.Sql, connection))
        {
            foreach (var p in compiled.Parameters)
            {
                command.Parameters.Add(p);
            }

            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                byInstance[reader.GetString(reader.GetOrdinal("instance_name"))] =
                    Convert.ToDouble(reader.GetValue(reader.GetOrdinal("value")), CultureInfo.InvariantCulture);
            }
        }

        Assert.Equal(2, byInstance.Count);
        Assert.True(Math.Abs(byInstance["db_one"] - 10.0) < 0.0001, $"db_one rate = {byInstance["db_one"]}, expected 10 per second");
        Assert.True(Math.Abs(byInstance["db_two"] - 10.0) < 0.0001, $"db_two rate = {byInstance["db_two"]}, expected 10 per second");
    }

    private static PanelPlan Panel(string measureProperty, string counter)
    {
        var json = "{\"source\":\"perfmon_stats\"," + measureProperty + ",\"timeBucket\":\"hour\",\"viz\":\"line\"," +
            "\"filters\":[{\"dimension\":\"counter_name\",\"op\":\"eq\",\"value\":\"" + counter + "\"}]}";
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, Array.Empty<string>());
        Assert.True(error is null, error);
        return plan!;
    }

    private static async Task<Dictionary<DateTime, double>> RunAsync(NpgsqlConnection connection, PanelPlan plan)
    {
        var ct = TestContext.Current.CancellationToken;
        var context = new ComposeRunContext(new[] { ServerName }, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown);
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);

        var actual = new Dictionary<DateTime, double>();
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var value = reader.GetValue(reader.GetOrdinal("value"));
            if (value is not DBNull)
            {
                actual[DateTime.SpecifyKind(reader.GetDateTime(reader.GetOrdinal("bucket")), DateTimeKind.Unspecified)] =
                    Convert.ToDouble(value, CultureInfo.InvariantCulture);
            }
        }

        return actual;
    }
}
