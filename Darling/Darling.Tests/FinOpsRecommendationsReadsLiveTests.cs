/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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
}
