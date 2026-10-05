/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Live pins for <c>get_read_latency</c> (#4442 scope 2): a field-shaped <c>collect.read_latency</c> fixture
/// with hand-computed p50/p95/p99, the surface/route filters, the honest-empty window, and the
/// <c>ApplicationName</c> the web and MCP store connections carry through the product's own connection
/// builders. The math itself (<see cref="ReadLatencyPercentiles.FromBuckets"/>) is already pinned without a
/// store; this file proves the SQL and the wiring around it, going through
/// <see cref="DarlingMcpReadLatencyTools.GetReadLatency"/> — the tool's own call path — rather than calling
/// <see cref="DarlingReadLatencyReader.GetSummaryAsync"/> directly, so the JSON shape and the sort order are
/// pinned too, not just the reader.
/// </summary>
/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the
   shared store's tables, so it cannot race the live collection. */
public sealed class GetReadLatencyLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /* Route A ("get_wait_stats", web): a fast route, 100 rows, mostly <= 100 ms.
       50 rows at 50 ms, 50 rows at 90 ms. n=100, total=(50*50)+(90*50)=7,000, mean=70, max=90.
       Bucket bounds (ms): 10,20,30,50,75,100,150,200,300,500,750,1000,1500,2000,3000,5000,7500,
       10000,15000,20000,30000,45000,60000,90000,120000 (index 3 = 50, index 5 = 100).
       Cumulative: 50 rows land in bucket "50" (index 3, cum=50), 50 rows land in bucket "100" (index 5, cum=100).
       p50 target = ceil(0.50*100) = 50 -> cum reaches 50 at bucket 50 ms  -> p50 = 50.
       p95 target = ceil(0.95*100) = 95 -> cum reaches 100 at bucket 100 ms -> p95 = 100.
       p99 target = ceil(0.99*100) = 99 -> cum reaches 100 at bucket 100 ms -> p99 = 100. */
    private static readonly long[] RouteAOkMs = Enumerable.Repeat(50L, 50).Concat(Enumerable.Repeat(90L, 50)).ToArray();

    /* Route B ("get_plan_cache", web): a slow tail, 100 non-timeout rows plus 2 Timeout rows over the same
       window.
       94 rows at 150 ms (bucket 150, index 6), 5 rows at 2,000 ms (bucket 2000, index 13; ~5% at 1-5 s),
       1 row at 30,000 ms (bucket 30000, index 20; ~1% at 20-60 s), plus 2 rows at 61,000 ms with
       outcome = Timeout (bucket 90000, index 23 -- the first bound >= 61,000).
       n = 94+5+1+2 = 102. total = 94*150 + 5*2000 + 1*30000 + 2*61000 = 14,100+10,000+30,000+122,000 = 176,100.
       mean = 176,100 / 102 = 1,726 (integer division). max = 61,000. timeouts = 2.
       Cumulative over ALL outcomes together (the bucket histogram sums every outcome):
       bucket 150: cum=94; bucket 2000: cum=99; bucket 30000: cum=100; bucket 90000: cum=102.
       p50 target = ceil(0.50*102) = 51 -> cum reaches 94 at bucket 150 ms  -> p50 = 150.
       p95 target = ceil(0.95*102) = 97 -> cum reaches 99 at bucket 2000 ms -> p95 = 2000.
       p99 target = ceil(0.99*102) = 101 -> cum reaches 102 at bucket 90000 ms -> p99 = 90000. */
    private static readonly long[] RouteBOkMs = Enumerable.Repeat(150L, 94)
        .Concat(Enumerable.Repeat(2_000L, 5))
        .Concat(Enumerable.Repeat(30_000L, 1))
        .ToArray();

    private static readonly long[] RouteBTimeoutMs = { 61_000L, 61_000L };

    private static async Task SeedTwoHoursAsync(NpgsqlConnection connection, DateTime hour1, DateTime hour2, System.Threading.CancellationToken ct)
    {
        var accumulatorHour1 = new ReadLatencyAccumulator();
        foreach (var ms in RouteAOkMs.Take(60))
        {
            accumulatorHour1.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, ms);
        }
        foreach (var ms in RouteBOkMs.Take(60))
        {
            accumulatorHour1.Record(ReadSurface.Web, "get_plan_cache", ReadOutcome.Ok, ms);
        }
        accumulatorHour1.Record(ReadSurface.Web, "get_plan_cache", ReadOutcome.Timeout, RouteBTimeoutMs[0]);
        await accumulatorHour1.FlushAsync(connection, hour1, logger: null, ct);

        var accumulatorHour2 = new ReadLatencyAccumulator();
        foreach (var ms in RouteAOkMs.Skip(60))
        {
            accumulatorHour2.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, ms);
        }
        foreach (var ms in RouteBOkMs.Skip(60))
        {
            accumulatorHour2.Record(ReadSurface.Web, "get_plan_cache", ReadOutcome.Ok, ms);
        }
        accumulatorHour2.Record(ReadSurface.Web, "get_plan_cache", ReadOutcome.Timeout, RouteBTimeoutMs[1]);
        await accumulatorHour2.FlushAsync(connection, hour2, logger: null, ct);
    }

    [Fact]
    public async Task TwoRoutesAcrossTwoHours_MatchHandComputedPercentilesCountsAndSortOrder()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the get_read_latency field-fixture pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var now = DateTime.UtcNow;
            var hour1 = now.AddHours(-2);
            var hour2 = now.AddHours(-1);
            await SeedTwoHoursAsync(connection, hour1, hour2, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, cancellationToken: ct);
            using var document = JsonDocument.Parse(json);
            Assert.True(document.RootElement.TryGetProperty("reads", out var readsElement), json);
            var reads = readsElement.EnumerateArray().ToArray();

            Assert.Equal(2, reads.Length);

            /* Sort order: p95 desc, then count desc -- Route B's p95 (2,000) beats Route A's (100), so B leads. */
            var first = reads[0];
            var second = reads[1];
            Assert.Equal("get_plan_cache", first.GetProperty("route").GetString());
            Assert.Equal("get_wait_stats", second.GetProperty("route").GetString());

            Assert.Equal("web", first.GetProperty("surface").GetString());
            Assert.Equal(102, first.GetProperty("count").GetInt64());
            Assert.Equal(1_726, first.GetProperty("mean_ms").GetInt64());
            Assert.Equal(61_000, first.GetProperty("max_ms").GetInt64());
            Assert.Equal(150, first.GetProperty("p50_ms").GetInt64());
            Assert.Equal(2_000, first.GetProperty("p95_ms").GetInt64());
            Assert.Equal(90_000, first.GetProperty("p99_ms").GetInt64());
            Assert.False(first.GetProperty("p99_is_at_least").GetBoolean());
            Assert.Equal(2, first.GetProperty("timeouts").GetInt64());

            Assert.Equal("web", second.GetProperty("surface").GetString());
            Assert.Equal(100, second.GetProperty("count").GetInt64());
            Assert.Equal(70, second.GetProperty("mean_ms").GetInt64());
            Assert.Equal(90, second.GetProperty("max_ms").GetInt64());
            Assert.Equal(50, second.GetProperty("p50_ms").GetInt64());
            Assert.Equal(100, second.GetProperty("p95_ms").GetInt64());
            Assert.Equal(100, second.GetProperty("p99_ms").GetInt64());
            Assert.Equal(0, second.GetProperty("timeouts").GetInt64());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task FallbackAndGateFailedSamples_AreCountedSeparately_InsideCount_AndTiesOrderBySurfaceThenRoute()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the get_read_latency fallback pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var accumulator = new ReadLatencyAccumulator();
            foreach (var route in new[] { "route_b", "route_a" })
            {
                accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Ok, 50);
                accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.FallbackRaw, 50);
                accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.FallbackRaw, 50);
                accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.GateFailed, 50);
            }

            await accumulator.FlushAsync(connection, DateTime.UtcNow.AddHours(-1), logger: null, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, cancellationToken: ct);
            using var document = JsonDocument.Parse(json);
            var reads = document.RootElement.GetProperty("reads").EnumerateArray().ToArray();

            Assert.Equal(new[] { "route_a", "route_b" }, reads.Select(r => r.GetProperty("route").GetString()).ToArray());
            foreach (var read in reads)
            {
                Assert.Equal(4, read.GetProperty("count").GetInt64());
                Assert.Equal(2, read.GetProperty("fallbacks").GetInt64());
                Assert.Equal(1, read.GetProperty("gate_failures").GetInt64());
                Assert.Equal(0, read.GetProperty("timeouts").GetInt64());
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task ThousandRoutesAtLimit1000_FitTheResponseBudget_InTheTotalOrder_AndSayHowManyWereLeftOut()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the get_read_latency budget pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            /* 1,000 distinct routes, all with the same 50 ms p95. The first 100 carry two samples and the rest
               one, so the total order is: count desc (the first 100), then route ordinal within each group. */
            var accumulator = new ReadLatencyAccumulator();
            for (var i = 0; i < 1000; i++)
            {
                var route = $"route_{i:D4}";
                accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Ok, 50);
                if (i < 100)
                {
                    accumulator.Record(ReadSurface.Mcp, route, ReadOutcome.Ok, 50);
                }
            }

            await accumulator.FlushAsync(connection, DateTime.UtcNow.AddHours(-1), logger: null, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, limit: 1000, cancellationToken: ct);

            var bytes = System.Text.Encoding.UTF8.GetByteCount(json);
            Assert.True(bytes <= McpResponseBudget.DefaultBytes, $"response was {bytes} bytes, over the {McpResponseBudget.DefaultBytes}-byte budget");

            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            var reads = root.GetProperty("reads").EnumerateArray().ToArray();

            Assert.True(root.GetProperty("truncated").GetBoolean());
            Assert.Equal(1000, root.GetProperty("reads_total").GetInt32());
            Assert.Equal(reads.Length, root.GetProperty("reads_returned").GetInt32());
            Assert.InRange(reads.Length, 100, 999);

            /* Greedy fill: the budget is nearly used (one more ~230-byte row would not have fit). */
            Assert.True(bytes > McpResponseBudget.DefaultBytes - 400, $"only {bytes} bytes used; the page stopped early");

            var expected = Enumerable.Range(0, 1000)
                .OrderBy(i => i < 100 ? 0 : 1)
                .ThenBy(i => $"route_{i:D4}", StringComparer.Ordinal)
                .Take(reads.Length)
                .Select(i => $"route_{i:D4}")
                .ToArray();
            Assert.Equal(expected, reads.Select(r => r.GetProperty("route").GetString()).ToArray());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task SurfaceAndRouteFilters_EachScopeToOneRow()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the get_read_latency filter pins.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            var hour1 = DateTime.UtcNow.AddHours(-1);

            var accumulator = new ReadLatencyAccumulator();
            accumulator.Record(ReadSurface.Web, "get_wait_stats", ReadOutcome.Ok, 50);
            accumulator.Record(ReadSurface.Compose, "custom-view", ReadOutcome.Ok, 200);
            await accumulator.FlushAsync(connection, hour1, logger: null, ct);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

            var bySurface = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, surface: "compose", cancellationToken: ct);
            using var surfaceDoc = JsonDocument.Parse(bySurface);
            var surfaceReads = surfaceDoc.RootElement.GetProperty("reads").EnumerateArray().ToArray();
            var surfaceRead = Assert.Single(surfaceReads);
            Assert.Equal("compose", surfaceRead.GetProperty("surface").GetString());
            Assert.Equal("custom-view", surfaceRead.GetProperty("route").GetString());

            var byRoute = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 48, route: "get_wait_stats", cancellationToken: ct);
            using var routeDoc = JsonDocument.Parse(byRoute);
            var routeReads = routeDoc.RootElement.GetProperty("reads").EnumerateArray().ToArray();
            var routeRead = Assert.Single(routeReads);
            Assert.Equal("web", routeRead.GetProperty("surface").GetString());
            Assert.Equal("get_wait_stats", routeRead.GetProperty("route").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    [Fact]
    public async Task AnEmptyWindow_GivesTheDocumentedHonestEmptyResult()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the get_read_latency empty-window pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        try
        {
            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var json = await DarlingMcpReadLatencyTools.GetReadLatency(postgres, hours: 24, cancellationToken: ct);
            using var document = JsonDocument.Parse(json);
            Assert.Equal("empty", document.RootElement.GetProperty("status").GetString());
            Assert.Contains("No read recorded", document.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// The wiring pin (#4442 scope 2): opens the web (viewer) and MCP (mcp) store connections through the
    /// PRODUCT's own <see cref="DarlingStoreLogins.BuildComposeStoreRoleConnectionString"/> builder — no
    /// hand-written connection string standing in for the code under test — and asserts each backend's
    /// <c>application_name</c> in <c>pg_stat_activity</c> matches the constant the tool wraps around it.
    /// </summary>
    [Fact]
    public async Task ApplicationName_ThroughTheProductsOwnConnectionBuilder_NamesEachSurfacesBackend()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the get_read_latency ApplicationName pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var ownerBuilder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString);
        var ownerAdminConnectionString = scratch.ConnectionString;

        /* viewer/mcp are the LITERAL role names DarlingStoreLogins.BuildComposeStoreRoleConnectionString
           matches against to pick the ApplicationName (DarlingManagedPostgres.ViewerRoleName / McpRoleName).
           Roles are cluster-scoped in Postgres, not per-database, so this scratch-only fact drops both roles
           in its own finally regardless of outcome -- it cannot leak into a later test in the same run. */
        const string ViewerPassword = "Viewer4442Pw";
        const string McpPassword = "Mcp4442Pw";

        try
        {
            await using (var owner = new NpgsqlConnection(scratch.ConnectionString))
            {
                await owner.OpenAsync(ct);

                /* Roles are cluster-scoped, not per-database: drop any stale viewer/mcp left by an earlier
                   run of this same fact (a prior process kill, for example) before creating fresh ones. */
                foreach (var staleRole in new[] { DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.McpRoleName })
                {
                    try
                    {
                        await using var dropOwned = new NpgsqlCommand($"DROP OWNED BY {staleRole};", owner);
                        await dropOwned.ExecuteNonQueryAsync(ct);
                    }
                    catch (PostgresException)
                    {
                        /* nothing owned by a role that does not exist yet */
                    }

                    await using var dropRoleStale = new NpgsqlCommand($"DROP ROLE IF EXISTS {staleRole};", owner);
                    await dropRoleStale.ExecuteNonQueryAsync(ct);
                }

                await using var createViewer = new NpgsqlCommand(
                    $"CREATE ROLE {DarlingManagedPostgres.ViewerRoleName} LOGIN PASSWORD '{ViewerPassword}'; "
                    + $"GRANT CONNECT ON DATABASE \"{ownerBuilder.Database}\" TO {DarlingManagedPostgres.ViewerRoleName};", owner);
                await createViewer.ExecuteNonQueryAsync(ct);
                await using var createMcp = new NpgsqlCommand(
                    $"CREATE ROLE {DarlingManagedPostgres.McpRoleName} LOGIN PASSWORD '{McpPassword}'; "
                    + $"GRANT CONNECT ON DATABASE \"{ownerBuilder.Database}\" TO {DarlingManagedPostgres.McpRoleName};", owner);
                await createMcp.ExecuteNonQueryAsync(ct);
            }

            var viewerConnectionString = DarlingStoreLogins.BuildComposeStoreRoleConnectionString(
                ownerAdminConnectionString, DarlingManagedPostgres.ViewerRoleName, ViewerPassword);
            var mcpConnectionString = DarlingStoreLogins.BuildComposeStoreRoleConnectionString(
                ownerAdminConnectionString, DarlingManagedPostgres.McpRoleName, McpPassword);

            await using (var viewer = new NpgsqlConnection(viewerConnectionString))
            {
                await viewer.OpenAsync(ct);
                await using var check = new NpgsqlCommand(
                    "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()", viewer);
                var applicationName = (string)(await check.ExecuteScalarAsync(ct))!;
                Assert.Equal(DarlingManagedPostgres.WebApplicationName, applicationName);
            }

            await using (var mcp = new NpgsqlConnection(mcpConnectionString))
            {
                await mcp.OpenAsync(ct);
                await using var check = new NpgsqlCommand(
                    "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()", mcp);
                var applicationName = (string)(await check.ExecuteScalarAsync(ct))!;
                Assert.Equal(DarlingManagedPostgres.McpApplicationName, applicationName);
            }

            bodySucceeded = true;
        }
        finally
        {
            await using var owner = new NpgsqlConnection(scratch.ConnectionString);
            await owner.OpenAsync(CancellationToken.None);
            foreach (var role in new[] { DarlingManagedPostgres.ViewerRoleName, DarlingManagedPostgres.McpRoleName })
            {
                try
                {
                    await using var dropOwned = new NpgsqlCommand($"DROP OWNED BY {role};", owner);
                    await dropOwned.ExecuteNonQueryAsync(CancellationToken.None);
                }
                catch (PostgresException)
                {
                    /* nothing owned by a role that never got created */
                }

                await using var dropRole = new NpgsqlCommand($"DROP ROLE IF EXISTS {role};", owner);
                await dropRole.ExecuteNonQueryAsync(CancellationToken.None);
            }

            await LiveStoreCleanup.RunAsync(ownerAdminConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
