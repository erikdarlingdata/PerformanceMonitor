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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8503: the FinOps day tables keep a sample stamped past the caller's clock. The caller builds its days through its own clock, and
/// the store's host can write a newer sample when that clock trails it; each per-day read runs the days through the window's newest
/// sample, so a sample at or after the last day's end still lands in the row its day names, and a day with no sample adds no row.
/// </summary>
/* #1776 own-store: the test seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsDayTablesDaysThroughLatestTests
{
    private const string ServerName = "darling-finops-day-tables-days-through-latest";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    /* Fixed instants, never the wall clock: the caller's clock reads half a minute before a UTC midnight, and the store's newest
       sample is a minute later, past that midnight and so past the end of the last day the caller built. */
    private static readonly DateTime CallerNowUtc = new(2026, 11, 9, 23, 59, 30, DateTimeKind.Unspecified);
    private static readonly DateTime Earlier = new(2026, 11, 9, 12, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime PastNow = CallerNowUtc.AddMinutes(1);

    [Fact]
    public async Task TheProvisioningTrend_KeepsASampleOneMinutePastTheCallersClock()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            await SeedCpuAsync(connection, Earlier, ct);
            await SeedCpuAsync(connection, PastNow, ct);
            var cutoff = CallerNowUtc.AddDays(-7);
            var days = DarlingFinOpsUtilizationReader.UtcDays(cutoff, CallerNowUtc);
            Assert.Equal(PastNow.Date, days[^1].EndUtc);

            await using var dataSource = NpgsqlDataSource.Create(cs);
            var rows = await DarlingFinOpsUtilizationReader.GetProvisioningTrendAsync(dataSource, ServerId, cutoff, days, 30, ct);

            Assert.Equal([Earlier.Date, PastNow.Date], rows.Select(r => r.Day).ToArray());
        });
    }

    [Fact]
    public async Task MemoryGrantEfficiency_KeepsASampleOneMinutePastTheCallersClock()
    {
        await WithStoreAsync(async (connection, cs, ct) =>
        {
            await SeedGrantAsync(connection, Earlier, ct);
            await SeedGrantAsync(connection, PastNow, ct);
            var cutoff = CallerNowUtc.AddHours(-24);
            var days = DarlingFinOpsUtilizationReader.UtcDays(cutoff, CallerNowUtc);
            Assert.Equal(PastNow.Date, days[^1].EndUtc);

            await using var dataSource = NpgsqlDataSource.Create(cs);
            var rows = await DarlingFinOpsUtilizationReader.GetMemoryGrantEfficiencyAsync(dataSource, ServerId, cutoff, days, 30, ct);

            Assert.Equal([Earlier.Date, PastNow.Date], rows.Select(r => r.Day).ToArray());
        });
    }

    private static async Task WithStoreAsync(Func<NpgsqlConnection, string, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live FinOps day-table test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await body(connection, scratch.ConnectionString, ct);
    }

    private static Task SeedCpuAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        SeedAsync(connection,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $2, 40, 5)",
            at, ct);

    private static Task SeedGrantAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        SeedAsync(connection,
            "INSERT INTO memory_grant_stats (collection_id, collection_time, server_id, server_name, target_memory_mb, granted_memory_mb, used_memory_mb, grantee_count, waiter_count, timeout_error_count_delta, forced_grant_count_delta) VALUES ($1, $2, $3, $4, 1000, 100, 50, 1, 0, 0, 0)",
            at, ct);

    private static async Task SeedAsync(NpgsqlConnection connection, string sql, DateTime at, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = at.Ticks });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = at });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = ServerId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = ServerName });
        await command.ExecuteNonQueryAsync(ct);
    }
}
