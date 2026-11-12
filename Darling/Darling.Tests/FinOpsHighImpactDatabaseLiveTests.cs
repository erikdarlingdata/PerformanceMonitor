/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// #7695: the FinOps High Impact read adds a query hash up across every database that ran it. It named the alphabetically first
/// database, so the Database column could point at a database that did almost none of the work, and Get Actual Plan then ran the
/// statement there. It now names the database with the most CPU (the name breaks a tie, ignoring case) and returns how many
/// databases are in the total.
/// </summary>
public sealed class FinOpsHighImpactDatabaseLiveTests
{
    private const int ServerId = 76950;
    private const string ServerName = "highimpact-databases";

    private static async Task PlantAsync(NpgsqlConnection c, CancellationToken ct, DateTime at, string hash, string database, long cpuUs)
    {
        await using var insert = new NpgsqlCommand(
            @"INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
                query_text, delta_execution_count, delta_worker_time, delta_elapsed_time, delta_logical_reads, delta_logical_writes,
                max_grant_kb, sample_interval_seconds, last_execution_time)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)", c);
        insert.Parameters.AddWithValue(CollectionIdGenerator.Next());
        insert.Parameters.AddWithValue(NpgsqlDbType.Timestamp, at);
        insert.Parameters.AddWithValue(ServerId);
        insert.Parameters.AddWithValue(ServerName);
        insert.Parameters.AddWithValue(database);
        insert.Parameters.AddWithValue(hash);
        insert.Parameters.AddWithValue("0xHANDLE" + hash);
        insert.Parameters.AddWithValue("SELECT /* " + hash + " */ 1");
        insert.Parameters.AddWithValue(10L);
        insert.Parameters.AddWithValue(cpuUs);
        insert.Parameters.AddWithValue(cpuUs * 2);
        insert.Parameters.AddWithValue(1000L);
        insert.Parameters.AddWithValue(10L);
        insert.Parameters.AddWithValue(1024L);
        insert.Parameters.AddWithValue(60);
        insert.Parameters.AddWithValue(NpgsqlDbType.Timestamp, at);
        await insert.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task HighImpact_NamesTheDatabaseWithTheMostCpu_AndCountsTheDatabasesInTheTotal()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live High Impact database test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var at = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-20), DateTimeKind.Unspecified);
        await using (var c = new NpgsqlConnection(scratch.ConnectionString))
        {
            await c.OpenAsync(ct);
            await PgMigrations.MigrateAsync(c, ct);
            await DarlingMcpTestData.RegisterServerAsync(c, ServerId, ServerName, ct);

            /* LATEWINNER: the alphabetically later database did nearly all the CPU, in two collections. */
            await PlantAsync(c, ct, at, "0xLATEWINNER", "Alpha", 1_000_000L);
            await PlantAsync(c, ct, at, "0xLATEWINNER", "Zulu", 5_000_000L);
            await PlantAsync(c, ct, at.AddMinutes(5), "0xLATEWINNER", "Zulu", 4_000_000L);
            /* TIED: equal CPU, so the name decides ignoring case: "alpha" before "Zulu", where a plain sort would put "Zulu" first. */
            await PlantAsync(c, ct, at, "0xTIED", "Zulu", 5_000_000L);
            await PlantAsync(c, ct, at, "0xTIED", "alpha", 5_000_000L);
            /* SOLO: one database. */
            await PlantAsync(c, ct, at, "0xSOLO", "Mid", 3_000_000L);
        }

        var rows = await DarlingFinOpsHighImpactReader.ReadAsync(dataSource, ServerId, 2, 60, ct);

        var late = Assert.Single(rows, r => r.QueryHash == "0xLATEWINNER");
        Assert.Equal("Zulu", late.DatabaseName);
        Assert.Equal(2, late.DatabaseCount);
        Assert.Equal(30L, late.TotalExecutions);
        Assert.Equal(10_000m, late.TotalCpuMs);

        var tied = Assert.Single(rows, r => r.QueryHash == "0xTIED");
        Assert.Equal("alpha", tied.DatabaseName);
        Assert.Equal(2, tied.DatabaseCount);

        var solo = Assert.Single(rows, r => r.QueryHash == "0xSOLO");
        Assert.Equal("Mid", solo.DatabaseName);
        Assert.Equal(1, solo.DatabaseCount);
    }
}
