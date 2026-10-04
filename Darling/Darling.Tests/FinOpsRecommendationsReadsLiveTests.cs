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
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fixture seeds its own scratch database, so nothing here shares rows with another test. */
/// <summary>
/// Live pin for the storage-side recommendation reads over the golden test's seeded store. The edition facts and the
/// engine edition must equal what the viewer reads for the same server; the memory P95 and the query-stats coverage
/// are compared with values worked out by hand from the seed. Server A is an Enterprise standalone server, server B
/// an Azure SQL Database, server C has no rows.
/// </summary>
public sealed class FinOpsRecommendationsReadsLiveTests
{
    private const int TimeoutSeconds = 30;

    [Fact]
    public async Task StorageReads_MatchTheViewerAndTheSeed_ForServersAbAndC()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps recommendation reads test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await FinOpsRecommendationsGoldenLiveTests.SeedAsync(connection, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var cutoff = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified);

        foreach (var id in new[]
                 {
                     FinOpsRecommendationsGoldenLiveTests.ServerIdA,
                     FinOpsRecommendationsGoldenLiveTests.ServerIdB,
                     FinOpsRecommendationsGoldenLiveTests.ServerIdC,
                 })
        {
            var stored = await DarlingFinOpsRecommendationsReader.GetEditionFactsAsync(dataSource, id, TimeoutSeconds, ct);
            var viewed = await viewer.GetEditionFactsAsync(id, ct);
            Assert.Equal(viewed.HasValue, stored.HasValue);
            if (viewed.HasValue)
            {
                Assert.Equal(viewed.Value.Edition, stored!.Value.Edition);
                Assert.Equal(viewed.Value.MajorVersion, stored.Value.MajorVersion);
                Assert.Equal(viewed.Value.CpuCount, stored.Value.CpuCount);
                Assert.Equal(viewed.Value.AgReplicaRole, stored.Value.AgReplicaRole);
                Assert.Equal(viewed.Value.IsHadrEnabled, stored.Value.IsHadrEnabled);
            }

            Assert.Equal(
                await viewer.GetRecommendationEngineEditionAsync(id, ct),
                await DarlingFinOpsRecommendationsReader.GetEngineEditionAsync(dataSource, id, TimeoutSeconds, ct));
        }

        Assert.Null(await DarlingFinOpsRecommendationsReader.GetEditionFactsAsync(dataSource, FinOpsRecommendationsGoldenLiveTests.ServerIdC, TimeoutSeconds, ct));

        var a = await DarlingFinOpsRecommendationsReader.GetEditionFactsAsync(dataSource, FinOpsRecommendationsGoldenLiveTests.ServerIdA, TimeoutSeconds, ct);
        Assert.Equal(new FinOpsEditionFacts("Enterprise Edition (64-bit)", 14, 16, "Standalone", false), a);

        /* Memory P95: 20 samples, value 13000 + 20 * (i % 5), so five values (13000..13080) four times each. The
           sorted position of the 95th percentile is 0.95 * 19 = 18.05, which lies between ranks 18 and 19, both
           13080, so P95 = 13080. The samples run from 1200 minutes back to 60 minutes back, a span of 1140 minutes
           = 19 hours, so the window reads "20 samples over 19 hours". Servers A and B carry the same series. */
        var expected = (13080, 20L, "20 samples over 19 hours");
        Assert.Equal(expected, await DarlingFinOpsRecommendationsReader.GetMemoryP95Async(dataSource, FinOpsRecommendationsGoldenLiveTests.ServerIdA, cutoff, TimeoutSeconds, ct));
        Assert.Equal(expected, await DarlingFinOpsRecommendationsReader.GetMemoryP95Async(dataSource, FinOpsRecommendationsGoldenLiveTests.ServerIdB, cutoff, TimeoutSeconds, ct));

        /* Server C has no memory rows: nothing sampled. */
        Assert.Equal((0, 0L, RightSizingWindow.Describe(0, TimeSpan.Zero)),
            await DarlingFinOpsRecommendationsReader.GetMemoryP95Async(dataSource, FinOpsRecommendationsGoldenLiveTests.ServerIdC, cutoff, TimeoutSeconds, ct));

        /* Coverage: server A's oldest query_stats row is 8 days old, at or before the 7-day cutoff, so true.
           Servers B and C have no query_stats rows, so false. */
        Assert.True(await DarlingFinOpsRecommendationsReader.HasQueryStatsCoverageAsync(dataSource, FinOpsRecommendationsGoldenLiveTests.ServerIdA, cutoff, TimeoutSeconds, ct));
        Assert.False(await DarlingFinOpsRecommendationsReader.HasQueryStatsCoverageAsync(dataSource, FinOpsRecommendationsGoldenLiveTests.ServerIdB, cutoff, TimeoutSeconds, ct));
        Assert.False(await DarlingFinOpsRecommendationsReader.HasQueryStatsCoverageAsync(dataSource, FinOpsRecommendationsGoldenLiveTests.ServerIdC, cutoff, TimeoutSeconds, ct));
    }

    [Fact]
    public async Task MaintenanceCpuStorageTierAndReservedReads_MatchTheSeed_ForServersAbAndC()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live FinOps recommendation reads test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await FinOpsRecommendationsGoldenLiveTests.SeedAsync(connection, ct);
        }

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using var viewer = new ViewerDataService(scratch.ConnectionString);
        var cutoff = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-7), DateTimeKind.Unspecified);
        var idA = FinOpsRecommendationsGoldenLiveTests.ServerIdA;
        var idB = FinOpsRecommendationsGoldenLiveTests.ServerIdB;
        var idC = FinOpsRecommendationsGoldenLiveTests.ServerIdC;

        /* Maintenance runs (server A only; the seed puts no jobs on B or C). WeeklyRebuildA ran long 5 times at 4000 s
           against a 60 s average: avg 4000, max 4000, historical 60, 5 long runs. NightlyLoadA ran long 3 times at
           100, 101 and 101 s: the average is 302 / 3 = 100.67, which converts to 101; max 101, historical 60, 3 long
           runs. HourlyPurgeA ran long only twice, so the HAVING filter drops it. Most long runs first. */
        Assert.Equal(
            new[]
            {
                new MaintenanceJobRun("WeeklyRebuildA", 4000L, 4000L, 60L, 5),
                new MaintenanceJobRun("NightlyLoadA", 101L, 101L, 60L, 3),
            },
            await DarlingFinOpsRecommendationsReader.GetMaintenanceJobRunsAsync(dataSource, idA, cutoff, TimeoutSeconds, ct));
        Assert.Empty(await DarlingFinOpsRecommendationsReader.GetMaintenanceJobRunsAsync(dataSource, idB, cutoff, TimeoutSeconds, ct));
        Assert.Empty(await DarlingFinOpsRecommendationsReader.GetMaintenanceJobRunsAsync(dataSource, idC, cutoff, TimeoutSeconds, ct));

        /* Storage-tier I/O (server A only). Six samples each for OrdersA and SalesA, three hours apart from 2 hours
           back to 17 hours back, every sample 400 reads and 200 writes: 2400 reads and 1200 writes. OrdersA stalls
           2 ms per read and 1 ms per write: 2400 * 2 = 4800 and 1200 * 1 = 1200. SalesA stalls 20 ms per read:
           2400 * 20 = 48000, and 1 ms per write: 1200. Both pass the over-1000-reads filter; ArchiveA (500 reads)
           does not. The first and last sample are 15 hours apart (17 - 2); the query has no ORDER BY, so sort by name. */
        var tiers = (await DarlingFinOpsRecommendationsReader.GetStorageTierIoAsync(dataSource, idA, cutoff, TimeoutSeconds, ct))
            .OrderBy(t => t.DatabaseName, StringComparer.Ordinal).ToList();
        Assert.Equal(new[] { "OrdersA", "SalesA" }, tiers.Select(t => t.DatabaseName).ToArray());
        Assert.Equal(new[] { 2400L, 2400L }, tiers.Select(t => t.TotalReads).ToArray());
        Assert.Equal(new[] { 4800L, 48000L }, tiers.Select(t => t.TotalStallReadMs).ToArray());
        Assert.Equal(new[] { 1200L, 1200L }, tiers.Select(t => t.TotalWrites).ToArray());
        Assert.Equal(new[] { 1200L, 1200L }, tiers.Select(t => t.TotalStallWriteMs).ToArray());
        Assert.Equal(new[] { 6L, 6L }, tiers.Select(t => t.WindowSamples).ToArray());
        foreach (var tier in tiers)
        {
            Assert.Equal(TimeSpan.FromHours(15), tier.LastSample!.Value - tier.FirstSample!.Value);
            Assert.InRange(DateTime.UtcNow - DateTime.SpecifyKind(tier.LastSample.Value, DateTimeKind.Utc), TimeSpan.FromHours(2), TimeSpan.FromHours(2.5));
        }

        Assert.Empty(await DarlingFinOpsRecommendationsReader.GetStorageTierIoAsync(dataSource, idB, cutoff, TimeoutSeconds, ct));
        Assert.Empty(await DarlingFinOpsRecommendationsReader.GetStorageTierIoAsync(dataSource, idC, cutoff, TimeoutSeconds, ct));

        /* CPU P95 (servers A and B carry the same series): 30 samples, value cpu[i % 10] for
           { 20, 22, 24, 21, 23, 22, 20, 24, 22, 21 }, so 20 x 6, 21 x 6, 22 x 9, 23 x 3, 24 x 6. Sorted, the 95th
           percentile sits at 0.95 * 29 = 27.55, between ranks 27 and 28, both 24 (ranks 24 to 29 are the 24s), so
           P95 = 24. The samples run 45 minutes apart, 29 gaps = 1305 minutes = 21.75 hours, which reads "21 hours". */
        var cpuExpected = (24m, "30 samples over 21 hours");
        Assert.Equal(cpuExpected, await DarlingFinOpsRecommendationsReader.GetCpuP95Async(dataSource, idA, cutoff, TimeoutSeconds, ct));
        Assert.Equal(cpuExpected, await DarlingFinOpsRecommendationsReader.GetCpuP95Async(dataSource, idB, cutoff, TimeoutSeconds, ct));
        Assert.Null(await DarlingFinOpsRecommendationsReader.GetCpuP95Async(dataSource, idC, cutoff, TimeoutSeconds, ct));

        /* Reserved capacity: the same 30 samples. Sum of one cycle of ten = 40 + 42 + 66 + 23 + 48 = 219, so the
           mean is 657 / 30 = 21.9. Squared deviations from 21.9 over one cycle: 7.22 + 1.62 + 0.03 + 1.21 + 8.82 =
           18.9, so 56.7 over three; the sample variance is 56.7 / 29 = 1.95517 and the deviation sqrt = 1.39828. */
        foreach (var id in new[] { idA, idB })
        {
            var reserved = await DarlingFinOpsRecommendationsReader.GetReservedCapacityAsync(dataSource, id, cutoff, TimeoutSeconds, ct);
            Assert.NotNull(reserved);
            Assert.Equal(21.9m, reserved!.Value.AvgCpuPct);
            Assert.InRange(reserved.Value.StddevCpuPct, 1.39827m, 1.39829m);
        }

        Assert.Null(await DarlingFinOpsRecommendationsReader.GetReservedCapacityAsync(dataSource, idC, cutoff, TimeoutSeconds, ct));

        /* The viewer's rows are the independent check on the builders: the Storage values through the builders give
           the same rows the viewer returns. Server A is a 16-core server with 98304 MB of physical memory. */
        var (avg, stddev) = (await DarlingFinOpsRecommendationsReader.GetReservedCapacityAsync(dataSource, idA, cutoff, TimeoutSeconds, ct))!.Value;
        var builtReserved = FinOpsRecommendationFigures.ReservedCapacity(avg, stddev);
        Assert.NotNull(builtReserved);
        var viewedA = await viewer.GetRecommendationsAsync(idA, 1000m, ct);
        var viewedReserved = Assert.Single(viewedA, r => r.Category == "Cloud");
        Assert.Equal(builtReserved!.Finding, viewedReserved.Finding);
        Assert.Equal(builtReserved.Detail, viewedReserved.Detail);

        var (p95Cpu, cpuWindow) = (await DarlingFinOpsRecommendationsReader.GetCpuP95Async(dataSource, idA, cutoff, TimeoutSeconds, ct))!.Value;
        var builtCpu = Assert.Single(FinOpsRecommendationFigures.VmRightSizing(p95Cpu, cpuWindow, 16, 98304, 0, 0L, "no samples", 1000m));
        var viewedCpu = Assert.Single(viewedA, r => r.Category == "Hardware" && r.Finding.StartsWith("CPU:", StringComparison.Ordinal));
        Assert.Equal(builtCpu.Finding, viewedCpu.Finding);
        Assert.Equal(builtCpu.Detail, viewedCpu.Detail);
        Assert.Equal(builtCpu.EstMonthlySavings, viewedCpu.EstMonthlySavings);
    }
}
