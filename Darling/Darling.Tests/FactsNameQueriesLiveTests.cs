/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the Darling performance monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6640: PARAMETER_SENSITIVITY and QUERY_SPILLS name the queries behind their counts, worst first, at most
/// <see cref="FactRanked.MaxObjects"/>. The Lite twin is <c>FactsNameQueriesTests</c>.
/// </summary>
[Collection("live-postgres")]
public sealed class FactsNameQueriesLiveTests
{
    /* #1776 own-store: seeds and removes rows under its own server id in the shared dev store. */
    private const int TestServerId = -664001;
    private const string TestServerName = "FactsNameQueriesSrv";
    private const string Db = "NamesDb";

    [Fact]
    public async Task ParameterSensitivity_NamesTheOffenders_WorstFirst_WithinTheCap()
    {
        await RunAsync(async (connection, periodEnd, ct) =>
        {
            /* Ratios 50, 200, 20 and 13: four offenders, the cap keeps the worst three. */
            await SeedAsync(connection, "0xQA", "0xPA", 50, 0, periodEnd.AddMinutes(-45), periodEnd, ct);
            await SeedAsync(connection, "0xQB", "0xPB", 200, 0, periodEnd.AddMinutes(-45), periodEnd, ct);
            await SeedAsync(connection, "0xQC", "0xPC", 20, 0, periodEnd.AddMinutes(-45), periodEnd, ct);
            await SeedAsync(connection, "0xQD", "0xPD", 13, 0, periodEnd.AddMinutes(-45), periodEnd, ct);
        }, facts =>
        {
            var fact = Assert.Single(facts, f => f.Key == "PARAMETER_SENSITIVITY");
            Assert.Equal(4.0, fact.Metadata["offender_count"]);
            Assert.Equal($"{Db}/0xQB/0xPB", fact.ObjectName);
            Assert.Equal(
                new[] { $"{Db}/0xQB/0xPB", $"{Db}/0xQA/0xPA", $"{Db}/0xQC/0xPC" },
                fact.Ranked.Select(o => o.ObjectName).ToArray());
            Assert.Equal(fact.Value, fact.Ranked[0].Value);
            Assert.Equal(Db, fact.Ranked[0].DatabaseName);
            Assert.Equal(20000.0, fact.Ranked[0].Figures!["min_worker_us"]);
        });
    }

    [Fact]
    public async Task QuerySpills_NamesTheSpillingQueries_MostSpillsFirst_WithinTheCap()
    {
        await RunAsync(async (connection, periodEnd, ct) =>
        {
            /* Spilled pages per query: A 300 (two snapshots), B 500, C 200, D 100. The aggregate still counts all four. */
            await SeedAsync(connection, "0xSA", "0xSAP", 1, 100, periodEnd.AddMinutes(-60), periodEnd, ct);
            await SeedAsync(connection, "0xSA", "0xSAP", 1, 200, periodEnd.AddMinutes(-30), periodEnd, ct);
            await SeedAsync(connection, "0xSB", "0xSBP", 1, 500, periodEnd.AddMinutes(-45), periodEnd, ct);
            await SeedAsync(connection, "0xSC", "0xSCP", 1, 200, periodEnd.AddMinutes(-45), periodEnd, ct);
            await SeedAsync(connection, "0xSD", "0xSDP", 1, 100, periodEnd.AddMinutes(-45), periodEnd, ct);
            await SeedAsync(connection, "0xSE", "0xSEP", 1, 0, periodEnd.AddMinutes(-45), periodEnd, ct);
        }, facts =>
        {
            var fact = Assert.Single(facts, f => f.Key == "QUERY_SPILLS");
            Assert.Equal(1100.0, fact.Metadata["total_spills"]);
            Assert.Equal(4.0, fact.Metadata["spilling_query_count"]);
            Assert.Equal($"{Db}/0xSB", fact.ObjectName);
            Assert.Equal(
                new[] { $"{Db}/0xSB", $"{Db}/0xSA", $"{Db}/0xSC" },
                fact.Ranked.Select(o => o.ObjectName).ToArray());
            Assert.Equal(new[] { 500.0, 300.0, 200.0 }, fact.Ranked.Select(o => o.Value).ToArray());
        });
    }

    private static async Task RunAsync(
        Func<NpgsqlConnection, DateTime, CancellationToken, Task> seed,
        Action<System.Collections.Generic.List<Fact>> assert)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the #6640 live tests.");
        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DeleteRowsAsync(connection, ct);
        }

        try
        {
            var periodEnd = DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks - (DateTime.UtcNow.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);
            await using (var connection = new NpgsqlConnection(connectionString))
            {
                await connection.OpenAsync(ct);
                await seed(connection, periodEnd, ct);
            }

            var context = new AnalysisContext
            {
                ServerId = TestServerId,
                ServerName = TestServerName,
                TimeRangeStart = periodEnd.AddHours(-4),
                TimeRangeEnd = periodEnd,
                ServerUtcOffset = TimeSpan.Zero,
            };

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            assert(await new PgFactCollector(postgres).CollectFactsAsync(context));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, DeleteRowsAsync);
        }
    }

    /* No server_properties row, so the offset is zero and creation_time is already UTC: a day before the window. */
    private static async Task SeedAsync(
        NpgsqlConnection c, string queryHash, string planHash, int ratio, long spills, DateTime at, DateTime periodEnd, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_plan_hash,
     sql_handle, plan_handle, query_text, creation_time, execution_count, delta_execution_count, delta_spills,
     min_worker_time, max_worker_time, min_grant_kb, max_grant_kb, min_spills, max_spills)
VALUES
    ($1,$2,$3,$4,$5,$6,$7,$8,$9,'SELECT 1',$10,5000,500,$11,20000,$12,1024,1024,0,0)", c);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(TestServerId);
        command.Parameters.AddWithValue(TestServerName);
        command.Parameters.AddWithValue(Db);
        command.Parameters.AddWithValue(queryHash);
        command.Parameters.AddWithValue(planHash);
        command.Parameters.AddWithValue(queryHash + "SQLH");
        command.Parameters.AddWithValue(queryHash + "PLANH");
        command.Parameters.AddWithValue(periodEnd.AddHours(-28));
        command.Parameters.AddWithValue(spills);
        command.Parameters.AddWithValue(20000L * ratio);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand("DELETE FROM collect.query_stats WHERE server_id = $1", connection);
        command.Parameters.AddWithValue(TestServerId);
        await command.ExecuteNonQueryAsync(ct);
    }
}
