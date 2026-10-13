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
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Storage.FinOps;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */

/// <summary>
/// #5633 review round 2 Low-4: the behaviour pin for Darling's CPU right-sizing window, the twin of Lite's
/// <c>FinOpsTests.VmRightSizing_BusyClusterFourDaysAgo_DoesNotMoveTheTwentyFourHourP95_...</c>. <c>RightSizingWindowTests</c> pins the
/// literal "the last 24 hours" in the reader's source, which still passes if the P95 behind that text stops coming from the
/// Utilization reader's 24-hour window or the row stops following the Utilization verdict. These tests run
/// <see cref="DarlingFinOpsRecommendationsReader.GetRecommendationsAsync"/> over seeded samples and read the rows it returns.
/// </summary>
public sealed class FinOpsCpuRightSizingWindowLiveTests
{
    private const string ServerNameBusyBefore = "darling-cpu-window-busy-before";
    private const string ServerNameSpiked = "darling-cpu-window-spiked";

    /// <summary>
    /// The last 24 hours sit at 8% and a busy stretch (90%) sits four days back. The Utilization view's P95 is 8%, so the CPU row must
    /// quote 8% over "the last 24 hours". A 7-day read (the old shape) blends the busy stretch in, reads a P95 of 90%, and either quotes
    /// a different figure or, as the verdict turns UNDER_PROVISIONED, says nothing.
    /// </summary>
    [Fact]
    public async Task CpuRow_QuotesTheTwentyFourHourP95_NotABlendWithABusyClusterFourDaysAgo()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the CPU right-sizing window live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var serverId = ServerIdHelper.GetDeterministicHashCode(ServerNameBusyBefore);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedServerAsync(connection, ct, serverId, ServerNameBusyBefore, now);
            /* 48 samples 30 minutes apart, the oldest 23h40m old: inside the 24-hour window and old enough to count as watched. */
            for (var i = 0; i < 48; i++)
                await CpuAsync(connection, ct, serverId, ServerNameBusyBefore, now.AddMinutes(-(10 + 30 * i)), 8);
            /* Four days back the CPU sat at 90%: outside the 24-hour window, inside any 7-day one. */
            for (var i = 0; i < 20; i++)
                await CpuAsync(connection, ct, serverId, ServerNameBusyBefore, now.AddDays(-4).AddMinutes(-15 * i), 90);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var recs = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(dataSource, serverId, 0m, 30, cancellationToken: ct);

        var cpu = Assert.Single(recs, r => r.Finding.StartsWith("CPU: reduce from 32", StringComparison.Ordinal));
        Assert.EndsWith("(P95 CPU 8.0%)", cpu.Finding, StringComparison.Ordinal);
        Assert.StartsWith("From the last 24 hours, P95 CPU utilization was 8.0%", cpu.Detail, StringComparison.Ordinal);
    }

    /// <summary>
    /// A spike to 60% inside the 24-hour window leaves the P95 at 8% but the maximum above the idle ceiling, so the Utilization verdict is
    /// RIGHT_SIZED. Neither CPU row may call that server over-provisioned: the rows follow the verdict, not the P95 alone.
    /// </summary>
    [Fact]
    public async Task CpuRow_StaysSilent_WhenTheUtilizationVerdictIsNotOverProvisioned()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the CPU right-sizing verdict live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var serverId = ServerIdHelper.GetDeterministicHashCode(ServerNameSpiked);
        var now = DarlingMcpTestData.Naive(DateTime.UtcNow);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await SeedServerAsync(connection, ct, serverId, ServerNameSpiked, now);
            for (var i = 0; i < 48; i++)
                await CpuAsync(connection, ct, serverId, ServerNameSpiked, now.AddMinutes(-(10 + 30 * i)), i == 20 ? 60 : 8);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var util = await DarlingFinOpsUtilizationReader.GetUtilizationEfficiencyAsync(dataSource, serverId, 30, ct);
        Assert.NotNull(util);
        Assert.Equal(8m, util.P95CpuPct);
        Assert.Equal(ProvisioningVerdict.RightSized, util.ProvisioningStatus);

        var recs = await DarlingFinOpsRecommendationsReader.GetRecommendationsAsync(dataSource, serverId, 0m, 30, cancellationToken: ct);

        Assert.DoesNotContain(recs, r => r.Finding.StartsWith("CPU: reduce from", StringComparison.Ordinal));
        Assert.DoesNotContain(recs, r => r.Finding.StartsWith("CPU over-provisioned", StringComparison.Ordinal));
    }

    /// <summary>A 32-core SQL Server with 64 GB of RAM and a sparse memory series: the CPU rows are the only ones the seed is built to move.</summary>
    private static async Task SeedServerAsync(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, DateTime now)
    {
        await DarlingMcpTestData.RegisterServerAsync(c, serverId, serverName, ct);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, edition, product_version, engine_edition, cpu_count, physical_memory_mb, is_hadr_enabled, ag_replica_role) VALUES ($1,$2,$3,$4,'Standard Edition (64-bit)','15.0.4000.1',2,32,65536,FALSE,'Standalone')",
            CollectionIdGenerator.Next(), now.AddHours(-2), serverId, serverName);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO memory_stats (collection_id, collection_time, server_id, server_name, total_physical_memory_mb, total_server_memory_mb, target_server_memory_mb, buffer_pool_mb, max_workers_count, current_workers_count) VALUES ($1, $2, $3, $4, 65536, 50000, 60000, 9000, 512, 100)",
            CollectionIdGenerator.Next(), now.AddHours(-1), serverId, serverName);
    }

    private static Task CpuAsync(NpgsqlConnection c, CancellationToken ct, int serverId, string serverName, DateTime at, int pct) =>
        DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1, $2, $3, $4, $5, $6, 1)",
            CollectionIdGenerator.Next(), at, serverId, serverName, at, pct);
}
