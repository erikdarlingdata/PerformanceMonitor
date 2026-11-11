/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8548: the 7-day provisioning trend lists seven calendar days, the six whole days before today and today so far, in the
/// store's UTC-day read and in the MCP utilization view built on it. The store holds a CPU sample on each of the last nine UTC
/// days, the one seven days back in that day's last second: inside a window that begins at this hour a week ago, and on a day
/// that is not one of the seven. The read returns the seven days and marks only today's row partial. The payload's
/// <c>trend_days</c> is the most rows <c>provisioning_trend</c> holds, and each row's <c>part_day</c> is true on today's only.
/// </summary>
/* #1776 own-store: each fact seeds its own scratch database, so nothing here shares rows with another test. */
public sealed class FinOpsProvisioningTrendSevenDaysLiveTests
{
    private const string ServerName = "darling-finops-trend-seven-days";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);

    [Fact]
    public Task TheStoreRead_ReturnsTheSixWholeDaysBeforeToday_AndTodaySoFar() => WithStoreAsync(async (dataSource, today, ct) =>
    {
        var rows = await DarlingFinOpsUtilizationReader.GetProvisioningTrendAsync(dataSource, ServerId, 30, ct);

        Assert.Equal(SevenDays(today), rows.Select(r => r.Day.Date).ToArray());
    });

    [Fact]
    public Task TheMcpPayload_HoldsTrendDaysRows_AndMarksOnlyTodayAPartDay() => WithStoreAsync(async (dataSource, today, ct) =>
    {
        using var doc = JsonDocument.Parse(await DarlingMcpFinOpsTools.GetFinOps(dataSource, "utilization", ServerName, 24, 10, cancellationToken: ct));
        var root = doc.RootElement;
        Assert.False(root.TryGetProperty("status", out var status) && status.GetString() != "ok", root.GetRawText());

        var trendDays = root.GetProperty("trend_days").GetInt32();
        var trend = root.GetProperty("provisioning_trend").EnumerateArray().ToList();

        Assert.Equal(7, trendDays);
        Assert.Equal(
            SevenDays(today).Select(d => d.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)).ToArray(),
            trend.Select(r => r.GetProperty("day").GetString()).ToArray());
        Assert.Equal(trendDays, trend.Count);

        /* Today's row is the day still running; the six before it are whole. */
        Assert.Equal(
            new[] { false, false, false, false, false, false, true },
            trend.Select(r => r.TryGetProperty("part_day", out var partDay) && partDay.ValueKind == JsonValueKind.True).ToArray());
    });

    private static DateTime[] SevenDays(DateTime today) => Enumerable.Range(0, 7).Select(i => today.AddDays(i - 6)).ToArray();

    private static async Task WithStoreAsync(Func<NpgsqlDataSource, DateTime, CancellationToken, Task> body)
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the live provisioning trend test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        var nowUtc = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var today = nowUtc.Date;
        using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            /* The seven days: a sample at each day's first instant, today's included, so every one is inside any window. */
            for (var daysBack = 0; daysBack <= 6; daysBack++)
            {
                await SeedCpuAsync(connection, today.AddDays(-daysBack), ct);
            }
            /* Seven days back, in the day's last second: after this hour a week ago, and before the first of the seven days. */
            await SeedCpuAsync(connection, today.AddDays(-6).AddSeconds(-1), ct);
            await SeedCpuAsync(connection, today.AddDays(-8).AddHours(12), ct);

            /* The utilization view answers only for a server with a memory sample in the last 24 hours. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, 8192, 3000, 4000, 2000, 512, 50)",
                CollectionIdGenerator.Next(), nowUtc.AddMinutes(-5), ServerId, ServerName);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(dataSource, today, ct);
    }

    private static Task SeedCpuAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $2, 40, 5)",
            CollectionIdGenerator.Next(), at, ServerId, ServerName);
}
