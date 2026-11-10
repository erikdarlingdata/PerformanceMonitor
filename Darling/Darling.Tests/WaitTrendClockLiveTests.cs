/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// get_wait_trend stamps each bucket in UTC and its description says so. The server is planted ten hours ahead of
/// UTC, so a bucket stamped on its clock would read ten hours later than the UTC one; the stamp is the UTC bucket's
/// start, which is what the description promises.
/// </summary>
[Collection("live-postgres")]
public sealed class WaitTrendClockLiveTests
{
    private const int ServerId = -950285;
    private const string ServerName = "wait-trend-clock";
    private const int ServerOffsetMinutes = 600;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task WaitTrend_StampsBucketsInUtc_AndItsDescriptionSaysSo()
    {
        var description = McpToolGuideTests.Served("get_wait_trend").Served;
        Assert.Contains("Every stamp here is UTC, not the monitored server's clock", description, StringComparison.Ordinal);

        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live wait-trend clock test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var dataSource = NpgsqlDataSource.Create(cs!);
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, utc_offset_minutes)
VALUES ($1, $2, $3, $4, $5)",
                CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(DateTime.UtcNow.AddMinutes(-5)), ServerId, ServerName, ServerOffsetMinutes);

            /* Two collections inside one hour that started at least 90 minutes ago, so an hourly bucket of a
               4-hour window holds both and starts on that hour, not on the window's start. */
            var start = DateTime.UtcNow.AddMinutes(-90);
            var hour = new DateTime(start.Year, start.Month, start.Day, start.Hour, 0, 0, DateTimeKind.Utc);
            await SeedWaitAsync(connection, ct, hour.AddMinutes(5), 1_200);
            await SeedWaitAsync(connection, ct, hour.AddMinutes(15), 6_000);

            var root = JsonDocument.Parse(await DarlingMcpDataTools.GetWaitTrend(dataSource, "LCK_M_X", ServerName, 4, null, 60)).RootElement;
            var stamps = root.GetProperty("trend").EnumerateArray()
                .Select(p => p.GetProperty("time").GetString()![..19])
                .ToArray();

            Assert.Equal(new[] { hour.ToString("yyyy-MM-ddTHH:mm:ss") }, stamps);
            Assert.DoesNotContain(hour.AddMinutes(ServerOffsetMinutes).ToString("yyyy-MM-ddTHH:mm:ss"), stamps);
        }
        finally
        {
            await DeleteRowsAsync(connection, ct);
        }
    }

    private static async Task SeedWaitAsync(NpgsqlConnection connection, CancellationToken ct, DateTime collectionTimeUtc, long deltaMs) =>
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     delta_wait_time_ms, delta_signal_wait_time_ms, delta_waiting_tasks, sample_interval_seconds)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            CollectionIdGenerator.Next(), DarlingMcpTestData.Naive(collectionTimeUtc),
            ServerId, ServerName, "LCK_M_X", deltaMs, 0L, 1L, 600);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM wait_stats WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM server_properties WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", ServerId);
        await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM config_monitored_servers WHERE server_id = $1", ServerId);
    }
}
