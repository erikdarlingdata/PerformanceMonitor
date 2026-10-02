/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4974: the latest-collector-outcome read must order by <c>collection_time DESC</c> first so the
/// <c>idx_collection_log_watermark</c> index serves it from the newest chunk, with <c>log_id DESC</c> as the
/// tie-break for two runs that share a timestamp. <c>collection_log</c> has no index on <c>log_id</c>, so a
/// <c>log_id</c>-first order sorts every retained row for the pair. A revert returns the same row on a small
/// store, so the order is pinned at the source as well as through the product's own read path.
/// </summary>
public sealed class LatestCollectorOutcomeOrderShapeTests
{
    [Fact]
    public void LatestCollectorOutcomeSql_OrdersByCollectionTimeFirst_ThenLogId()
    {
        var sql = Regex.Replace(DarlingRuntimePrecondition.LatestCollectorOutcomeSql, @"\s+", " ");

        Assert.Contains("ORDER BY collection_time DESC, log_id DESC LIMIT 1", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ORDER BY log_id", sql, StringComparison.Ordinal);
    }
}

[Collection("live-postgres")]
public sealed class LatestCollectorOutcomeOrderLiveTests
{
    private const int ServerId = -497400;
    private const string ServerName = "latest-outcome-4974";
    private const string Collector = "running_jobs";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task NewestRunByTimeWins_EvenWhenAnOlderChunkRowHasTheHigherLogId()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live latest-outcome test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);
        var ok = false;
        try
        {
            var now = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
            /* The newest run succeeded and holds the LOWER id; two older-chunk rows hold higher ids and say
               PERMISSIONS. Time order answers "succeeded" (no precondition); id order would not. */
            await SeedAsync(connection, ct, 100, now.AddMinutes(-1), "SUCCESS");
            await SeedAsync(connection, ct, 200, now.AddDays(-60), "PERMISSIONS");
            await SeedAsync(connection, ct, 300, now.AddDays(-120), "PERMISSIONS");

            await using var postgres = NpgsqlDataSource.Create(cs!);
            Assert.Null(await DarlingRuntimePrecondition.StatusAsync(postgres, ServerId, ServerName, Collector, ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, ok, DeleteAsync);
        }
    }

    [Fact]
    public async Task TwoRunsSharingATimestamp_TheHigherLogIdWins()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live latest-outcome test.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);
        var ok = false;
        try
        {
            var tied = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-2));
            await SeedAsync(connection, ct, 100, tied.AddDays(-60), "SUCCESS");
            await SeedAsync(connection, ct, 200, tied, "SUCCESS");
            await SeedAsync(connection, ct, 300, tied, "PERMISSIONS");

            await using var postgres = NpgsqlDataSource.Create(cs!);
            var message = await DarlingRuntimePrecondition.StatusAsync(postgres, ServerId, ServerName, Collector, ct);
            Assert.NotNull(message);

            /* And with the ids swapped the tie resolves the other way. */
            await DeleteAsync(connection, ct);
            await SeedAsync(connection, ct, 200, tied, "PERMISSIONS");
            await SeedAsync(connection, ct, 300, tied, "SUCCESS");
            Assert.Null(await DarlingRuntimePrecondition.StatusAsync(postgres, ServerId, ServerName, Collector, ct));
            ok = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, ok, DeleteAsync);
        }
    }

    private static Task SeedAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct, long logId, DateTime timeUtc, string status) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, $4, $5, $6)",
            logId, ServerId, ServerName, Collector, DarlingMcpTestData.Naive(timeUtc), status);

    private static Task DeleteAsync(NpgsqlConnection connection, System.Threading.CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", ServerId);
}
