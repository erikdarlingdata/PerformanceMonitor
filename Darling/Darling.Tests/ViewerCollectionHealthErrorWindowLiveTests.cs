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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8533: the Viewer's Collection Cycles card says "in the last 7 days" of each collector's error count and newest error. Those
/// columns come from the viewer's Collection Health rows read, and this pins against a live store that they stop at seven days:
/// an ERROR run just inside the window reaches <c>ErrorCount</c>, <c>LastErrorTime</c> and <c>LastError</c>, and one just outside
/// it reaches none of them.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerCollectionHealthErrorWindowLiveTests
{
    private const string ServerName = "darling-collection-health-error-window-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    private const string InsideAndOutside = "wait_stats";
    private const string OutsideOnly = "query_stats";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AnErrorRunJustInsideSevenDays_ReachesTheErrorColumns_AndOneJustOutside_DoesNot()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live collection-health error-window test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO config_monitored_servers (server_id, name, host, is_enabled) VALUES ($1, $2, $2, TRUE)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE", ServerId, ServerName);

            var utcNow = DateTime.UtcNow;
            var now = new DateTime(utcNow.Year, utcNow.Month, utcNow.Day, utcNow.Hour, utcNow.Minute, utcNow.Second, DateTimeKind.Unspecified);
            var inside = now.AddDays(-7).AddHours(1);
            var outside = now.AddDays(-7).AddHours(-1);
            await InsertRunAsync(connection, InsideAndOutside, outside, "ERROR", "the run outside the window", ct);
            await InsertRunAsync(connection, InsideAndOutside, inside, "ERROR", "the run inside the window", ct);
            await InsertRunAsync(connection, InsideAndOutside, now.AddMinutes(-5), "SUCCESS", null, ct);
            await InsertRunAsync(connection, OutsideOnly, outside, "ERROR", "the run outside the window", ct);
            await InsertRunAsync(connection, OutsideOnly, now.AddMinutes(-5), "SUCCESS", null, ct);

            await using var viewer = new ViewerDataService(cs!);
            var rows = await viewer.GetCollectionHealthAsync(ServerId, ct);

            var both = rows.Single(row => row.CollectorName == InsideAndOutside);
            Assert.Equal(1L, both.ErrorCount);
            Assert.Equal(inside, both.LastErrorTime);
            Assert.Equal("the run inside the window", both.LastError);

            var outsideOnly = rows.Single(row => row.CollectorName == OutsideOnly);
            Assert.Equal(0L, outsideOnly.ErrorCount);
            Assert.Null(outsideOnly.LastErrorTime);
            Assert.True(string.IsNullOrEmpty(outsideOnly.LastError), $"an error from outside the window is still the row's last error: {outsideOnly.LastError}");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) => await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task InsertRunAsync(NpgsqlConnection connection, string collectorName, DateTime collectionTime, string status, string? error, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, error_message)
VALUES ($1,$2,$3,$4,$5,$6,20,10,$7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        command.Parameters.AddWithValue(collectorName);
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue((object?)error ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await using var cleanup = new NpgsqlCommand(
            $"DELETE FROM collection_log WHERE server_id = {ServerId}; "
            + $"DELETE FROM config_monitored_servers WHERE server_id = {ServerId}; "
            + $"DELETE FROM servers WHERE server_id = {ServerId};",
            connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
