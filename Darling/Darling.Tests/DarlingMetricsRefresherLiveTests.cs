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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Metrics;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres, because the no-store-read
   tests count commands per database. */

/// <summary>
/// #9013: the metrics refresher reads the fleet exactly as <c>/api/fleet</c> does, then the server set, and builds one
/// snapshot. A server with no data yet gets its band and nothing else, a refused admission is a failed refresh, and a
/// scrape never reads the store: not before the worker collects, and not on a warm snapshot.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMetricsRefresherLiveTests
{
    private const int SeededId = 1;
    private const int EmptyId = 2;
    private const string SeededName = "metrics-refresher-seeded";
    private const string EmptyName = "metrics-refresher-empty";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task Refresh_ProducesFleetAndServerSet_ForASeededServer()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics refresher tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedServerWithDataAsync(postgres, LiveClock.Now(), ct);

        var snapshot = await Build(postgres, Collecting()).RefreshAsync(ct);

        foreach (var metric in new[] { "darling_server_health_band", "darling_server_cpu_percent", "darling_memory_total_server_mb" })
        {
            var sample = Assert.Single(snapshot.Samples, s => s.Name == metric);
            Assert.Equal(DarlingMetricCatalog.ServerLabel, sample.Labels[0].Key);
            Assert.Equal(SeededName, sample.Labels[0].Value);
        }

        Assert.Equal(34, Assert.Single(snapshot.Samples, s => s.Name == "darling_server_cpu_percent").Value);
        Assert.Equal(1000, Assert.Single(snapshot.Samples, s => s.Name == "darling_memory_total_server_mb").Value);
        Assert.Single(snapshot.LastCollections, s => s.Server == SeededName);
    }

    /// <summary>[Erik] A registered server with no rows gets the band the card shows, and no value series: never zeros.</summary>
    [Fact]
    public async Task ServerWithNoDataYet_GetsTheBandOnly_NoZeros()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics refresher tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await InsertServerAsync(connection, EmptyId, EmptyName, ct);
        }

        var snapshot = await Build(postgres, Collecting()).RefreshAsync(ct);

        var now = DateTime.UtcNow;
        var fleet = await DarlingFleetReader.GetFleetOverviewAsync(postgres, now.AddHours(-1), now, now, cancellationToken: ct);
        var card = Assert.Single(fleet.Cards, c => c.ServerId == EmptyId);

        var sample = Assert.Single(snapshot.Samples, s => s.Labels.Any(l => l.Key == DarlingMetricCatalog.ServerLabel && l.Value == EmptyName));
        Assert.Equal("darling_server_health_band", sample.Name);
        Assert.Equal((int)card.Band, sample.Value);
        Assert.DoesNotContain(snapshot.LastCollections, s => s.Server == EmptyName);
    }

    /// <summary>The display-name collision line comes once per change in the collision sets, not on every refresh.</summary>
    [Fact]
    public async Task ACollision_IsLoggedOnce_AndAgainOnlyWhenTheCollisionsChange()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics refresher tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        const string shared = "metrics-refresher-shared";
        await using (var connection = await postgres.OpenConnectionAsync(ct))
        {
            await InsertServerAsync(connection, 11, shared, ct);
            await InsertServerAsync(connection, 12, shared, ct);
            await InsertServerAsync(connection, 13, "metrics-refresher-alone", ct);
        }

        var log = new CapturingTestLogger();
        var refresher = new DarlingMetricsRefresher(
            _ => Task.FromResult<NpgsqlDataSource?>(postgres), new ReadAdmission(), registry: null, Collecting(), TimeProvider.System, log);
        int CollisionLines() => log.Lines.Count(l => l.Contains("share a display name", StringComparison.Ordinal));

        await refresher.RefreshAsync(ct);
        await refresher.RefreshAsync(ct);
        Assert.Equal(1, CollisionLines());

        await using (var connection = await postgres.OpenConnectionAsync(ct))
        await using (var rename = new NpgsqlCommand("UPDATE servers SET display_name = $1 WHERE server_id = 13", connection))
        {
            rename.Parameters.AddWithValue(shared);
            await rename.ExecuteNonQueryAsync(ct);
        }

        await refresher.RefreshAsync(ct);
        Assert.Equal(2, CollisionLines());
        Assert.Contains(log.Lines, l => l.Contains("11, 12, 13", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AdmissionRefused_IsAFailedRefresh()
    {
        var ct = TestContext.Current.CancellationToken;
        var admission = new ReadAdmission(TimeProvider.System, waitCap: TimeSpan.Zero, logger: null, maxHeavyReads: 1);
        /* A light read passes with no slot, as /api/fleet's does, so the route is made heavy for the refusal to be reachable. */
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long>
        {
            [(ReadSurface.Web, "get_fleet_overview", ReadWindowBand.OneHour)] = ReadAdmission.HeavyThresholdMs + 1,
        });

        /* The one slot is held from a flow of its own, as another request would hold it: a slot held by the test's own
           flow would make the refresher's admission a nested one, which passes. */
        using var holder = await Task.Run(() => admission.AdmitAsync(ReadSurface.Web, "get_fleet_overview", ReadWindowBand.OneHour, ct), ct);
        Assert.True(holder.MayRun);

        /* Never opened: the refusal comes before any store read. */
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=unused");
        var refresher = new DarlingMetricsRefresher(
            _ => Task.FromResult<NpgsqlDataSource?>(postgres), admission, registry: null, Collecting(), TimeProvider.System, new CapturingTestLogger());

        await Assert.ThrowsAsync<InvalidOperationException>(() => refresher.RefreshAsync(ct));
    }

    [Fact]
    public async Task BeforeCollecting_ScrapeAnswersMetaOnly_AndTouchesNoStore()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics refresher tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedServerWithDataAsync(postgres, LiveClock.Now(), ct);
        await AssertTheCounterSeesThisStoreAsync(scratch, postgres, ct);

        /* A fresh state has published nothing: the worker has not reached Collecting. */
        var refresher = Build(postgres, new CollectorRuntimeState());
        var cache = new DarlingMetricsSnapshotCache(refresher.RefreshAsync, refresher.CanRefresh, TimeProvider.System, new CapturingTestLogger(), ct);

        var bodies = new List<string>();
        long commands;
        using (var counter = new NpgsqlCommandCounter(scratch.DatabaseName, ""))
        {
            for (var i = 0; i < 3; i++)
            {
                var scrape = cache.Scrape();
                Assert.Null(cache.InFlight);
                bodies.Add(PrometheusText.Write(scrape, DateTime.UtcNow));
            }

            commands = counter.Count;
        }

        Assert.Equal(0, commands);
        Assert.False(refresher.CanRefresh());
        foreach (var body in bodies)
        {
            Assert.Contains("darling_metrics_last_refresh_success 0", body);
            Assert.DoesNotContain("darling_server_", body);
            Assert.DoesNotContain("server=", body);
        }
    }

    /// <summary>[Erik] Scrapes read the cached snapshot only: N scrapes on a warm snapshot run no store command.</summary>
    [Fact]
    public async Task WarmSnapshot_NScrapes_RunZeroStoreCommands()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live metrics refresher tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        await SeedServerWithDataAsync(postgres, LiveClock.Now(), ct);

        var refresher = Build(postgres, Collecting());
        var cache = new DarlingMetricsSnapshotCache(refresher.RefreshAsync, refresher.CanRefresh, TimeProvider.System, new CapturingTestLogger(), ct);

        cache.Scrape();
        var first = cache.InFlight;
        Assert.NotNull(first);
        await first;
        var warm = cache.Scrape();
        Assert.True(warm.LastRefreshSucceeded);
        Assert.NotNull(warm.Snapshot);

        long scrapeCommands;
        using (var counter = new NpgsqlCommandCounter(scratch.DatabaseName, ""))
        {
            for (var i = 0; i < 20; i++)
            {
                var scrape = cache.Scrape();
                var body = PrometheusText.Write(scrape, DateTime.UtcNow);
                Assert.Contains("darling_server_health_band{server=\"" + SeededName + "\"}", body);
            }

            Assert.Null(cache.InFlight);
            scrapeCommands = counter.Count;
        }

        Assert.Equal(0, scrapeCommands);

        /* The control: the same counter does see the refresher's own reads, so the 0 above is not a deaf counter. */
        long refreshCommands;
        using (var counter = new NpgsqlCommandCounter(scratch.DatabaseName, ""))
        {
            await refresher.RefreshAsync(ct);
            refreshCommands = counter.Count;
        }

        Assert.True(refreshCommands > 0, "the counter saw no command of a refresh, so a 0 from it proves nothing");
    }

    private static DarlingMetricsRefresher Build(NpgsqlDataSource postgres, CollectorRuntimeState collector) =>
        new(_ => Task.FromResult<NpgsqlDataSource?>(postgres), new ReadAdmission(), registry: null, collector, TimeProvider.System, new CapturingTestLogger());

    private static CollectorRuntimeState Collecting()
    {
        var state = new CollectorRuntimeState();
        state.PublishCollecting();
        return state;
    }

    /// <summary>The control, as <c>CollectorStateBatchLiveTests</c> runs it: one command of our own is counted once, so a
    /// renamed Npgsql tag reads as a loud failure rather than a silent 0.</summary>
    private static async Task AssertTheCounterSeesThisStoreAsync(ScratchPostgres scratch, NpgsqlDataSource postgres, CancellationToken ct)
    {
        long control;
        using (var counter = new NpgsqlCommandCounter(scratch.DatabaseName, ""))
        {
            await using var connection = await postgres.OpenConnectionAsync(ct);
            await using var command = new NpgsqlCommand("SELECT 1", connection);
            await command.ExecuteScalarAsync(ct);
            control = counter.Count;
        }

        Assert.Equal(1, control);
    }

    /// <summary>One SQL Server that is Online (a fresh collection_log row) with a CPU sample and a memory snapshot.</summary>
    private static async Task SeedServerWithDataAsync(NpgsqlDataSource postgres, DateTime now, CancellationToken ct)
    {
        await using var connection = await postgres.OpenConnectionAsync(ct);
        await InsertServerAsync(connection, SeededId, SeededName, ct);
        await InsertCollectionLogAsync(connection, SeededId, SeededName, now.AddSeconds(-30), ct);
        await InsertSqlServerCpuAsync(connection, SeededId, SeededName, now.AddMinutes(-1), 30, 4, ct);
        await InsertMemoryAsync(connection, SeededId, SeededName, now.AddMinutes(-1), ct);
    }

    private static async Task InsertServerAsync(NpgsqlConnection connection, int serverId, string name, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_engine_edition, engine_kind)
VALUES ($1, $2, $2, TRUE, 0, $3)", connection);
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(MonitoredEngineKind.SqlServer);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertCollectionLogAsync(
        NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, status)
VALUES ($1, $2, $3, 'wait_stats', $4, 'SUCCESS')", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertSqlServerCpuAsync(
        NpgsqlConnection connection, int serverId, string name,
        DateTime collectionTime, int sqlCpu, int otherCpu, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats
    (collection_id, collection_time, server_id, server_name,
     sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6, $7)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(sqlCpu);
        command.Parameters.AddWithValue(otherCpu);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task InsertMemoryAsync(
        NpgsqlConnection connection, int serverId, string name, DateTime collectionTime, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO memory_stats
    (collection_id, collection_time, server_id, server_name,
     total_server_memory_mb, target_server_memory_mb, plan_cache_mb, available_physical_memory_mb)
VALUES ($1, $2, $3, $4, 1000, 2000, 300, 400)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(DateTime.SpecifyKind(collectionTime, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(name);
        await command.ExecuteNonQueryAsync(ct);
    }
}
