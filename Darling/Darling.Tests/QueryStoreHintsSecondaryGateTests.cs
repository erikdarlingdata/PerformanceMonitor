/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8361: the scheduler does not run <c>query_store_hints</c> on a server whose every Query Store database is a readable
/// secondary. The collector's own statement skips such a database, so a run there reads the database list, collects nothing
/// and logs a success with zero rows every slot. The slot is served the way a refused collector's backoff serves one (#6186):
/// counted, the stamp moved on, and no run, so no collection_log row. A primary runs it, and the gate follows the server's
/// last known role in both directions.
/// </summary>
public sealed class QueryStoreHintsSecondaryGateTests
{
    private static readonly string Hints = QueryStoreHintsCollector.Instance.Name;
    private const string Peer = "wait_stats";

    private sealed record Rig(DarlingWorker Worker, DarlingWorker.ServerLoopState Server, Dictionary<string, int> Runs, bool[] Secondary);

    private static Rig MakeRig(int serverId)
    {
        var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
        var server = DailyRunKit.MakeServer(serverId, Hints);
        var runs = new Dictionary<string, int>(StringComparer.Ordinal);
        var secondary = new[] { true };
        worker.RunOneBodyOverride = (_, _, collector, _) =>
        {
            runs[collector] = runs.GetValueOrDefault(collector) + 1;
            return Task.FromResult(1);
        };
        /* The seam the worker reads the server's last known role through, standing in for the store's facts: true means every
           Query Store database on the server is a readable secondary. */
        worker.QueryStoreReadableSecondaryOverride = id => id == serverId && secondary[0];
        return new Rig(worker, server, runs, secondary);
    }

    /// <summary>One scheduler pass with the named collectors due, as they are every interval.</summary>
    private static async Task PassAsync(Rig rig, params string[] due)
    {
        foreach (var name in due)
        {
            rig.Server.NextDue[name] = DateTime.UtcNow.AddMinutes(-10);
        }

        await rig.Worker.RunDueCollectorsAsync(rig.Server, null!, TestContext.Current.CancellationToken);
        await Task.WhenAll(rig.Worker.InFlightDetachedRuns);
    }

    private static int RunsOf(Rig rig, string collector) => rig.Runs.GetValueOrDefault(collector);

    [Fact]
    public async Task OnAReadableSecondary_TheHintsSlotIsServedWithoutARun_AndItsPeersStillRun()
    {
        var rig = MakeRig(8361_01);
        var stamp = DateTime.UtcNow.AddMinutes(-10);

        await PassAsync(rig, Hints, Peer);

        Assert.Equal(0, RunsOf(rig, Hints));
        Assert.Equal(1, RunsOf(rig, Peer));
        Assert.True(rig.Server.NextDue[Hints] > stamp, "the slot was served: the stamp moved on, so the gate is asked again at the next slot, not every tick");
    }

    [Fact]
    public async Task APrimaryRunsIt_AndTheGateFollowsTheRoleBothWays()
    {
        var rig = MakeRig(8361_02);

        rig.Secondary[0] = false;
        await PassAsync(rig, Hints);
        Assert.Equal(1, RunsOf(rig, Hints));

        rig.Secondary[0] = true;
        await PassAsync(rig, Hints);
        Assert.Equal(1, RunsOf(rig, Hints));

        rig.Secondary[0] = false;
        await PassAsync(rig, Hints);
        Assert.Equal(2, RunsOf(rig, Hints));
    }

    [Fact]
    public async Task AGatedSlot_IsCountedByTheFleetGate_LikeAnyOtherSlot()
    {
        /* The fleet gate measures whether the sweep loop keeps cadence. A slot the grid served is counted whether or not the
           role gate then holds the collector back from running it. */
        var rig = MakeRig(8361_03);
        rig.Worker.SkipCreditFloorForTest.Resume(DateTime.UtcNow.AddHours(-1));
        const int passes = 3;

        for (var pass = 0; pass < passes; pass++)
        {
            await PassAsync(rig, Hints);
        }

        Assert.Equal(0, RunsOf(rig, Hints));
        var snapshot = rig.Worker.FleetGateStatsForTest!.Snapshot();
        Assert.Equal(passes, snapshot.Run);
    }
}

/* #1776 own-store: each fact mints its own scratch database through ScratchPostgres and never touches the shared store. */

/// <summary>
/// The same gate read from the store's own facts, not the seam: the newest <c>query_store_health</c> capture and the newest
/// <c>ag_replica_role</c>. A capture decides only while the <c>query_store_health</c> collector can refresh it on that server;
/// when its effective schedule disables it, the capture is frozen and the role alone decides, so a failover reaches the gate.
/// </summary>
public sealed class QueryStoreHintsSecondaryGateLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly string Hints = QueryStoreHintsCollector.Instance.Name;

    private static async Task ExecAsync(NpgsqlConnection c, string sql, CancellationToken ct, params object[] args)
    {
        await using var cmd = new NpgsqlCommand(sql, c);
        foreach (var a in args)
        {
            cmd.Parameters.AddWithValue(a);
        }

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private static Task RegisterAsync(NpgsqlConnection c, int id, string name, CancellationToken ct) =>
        ExecAsync(c,
            """
            INSERT INTO servers (server_id, server_name, display_name, is_enabled, engine_kind, created_date, modified_date)
            VALUES ($1, $2, $2, TRUE, $3, $4, $4)
            """, ct, id, name, MonitoredEngineKind.SqlServer, Naive(DateTime.UtcNow));

    /// <summary>The newest <c>server_properties</c> row's role, as the collector writes it (PRIMARY, SECONDARY, Standalone).</summary>
    private static Task RoleAsync(NpgsqlConnection c, int id, string name, string role, TimeSpan age, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO server_properties (collection_id, collection_time, server_id, server_name, ag_replica_role) VALUES ($1, $2, $3, $4, $5)",
            ct, CollectionIdGenerator.Next(), Naive(DateTime.UtcNow - age), id, name, role);

    /// <summary>One database of a <c>query_store_health</c> capture; the readable-secondary bit is <c>readonly_reason</c> 8.</summary>
    private static Task CaptureAsync(NpgsqlConnection c, int id, string name, string database, int readonlyReason, TimeSpan age, CancellationToken ct) =>
        ExecAsync(c,
            """
            INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason,
                                            current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days,
                                            max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode)
            VALUES ($1, $2, $3, $4, $5, $6, 'READ_WRITE', $7, 100, 1000, 'AUTO', 30, 200, 60, 'AUTO', 'ON')
            """, ct, CollectionIdGenerator.Next(), Naive(DateTime.UtcNow - age), id, name, database,
            (readonlyReason & 8) != 0 ? "READ_ONLY" : "READ_WRITE", readonlyReason);

    private static async Task RunAsync(Func<NpgsqlConnection, NpgsqlDataSource, CancellationToken, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the readable-secondary gate's store-fact tests (each mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await body(connection, postgres, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    [Fact]
    public async Task AFrozenCapture_DoesNotKeepTheGateOn_AfterAFailover_WhenQueryStoreHealthCannotRefreshIt()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* The issue's servers: the capture flags every database as a readable secondary, and query_store_health is disabled
               for the server, so nothing will ever refresh it. The server fails over and server_properties reads PRIMARY. */
            const int id = 836_104;
            const string name = "qs-failover";
            await RegisterAsync(connection, id, name, ct);
            await CaptureAsync(connection, id, name, "db1", 8, TimeSpan.FromDays(2), ct);
            await CaptureAsync(connection, id, name, "db2", 8, TimeSpan.FromDays(2), ct);
            await RoleAsync(connection, id, name, "PRIMARY", TimeSpan.FromMinutes(5), ct);

            var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
            worker.StoreForTests = postgres;
            var server = DailyRunKit.MakeServer(id, Hints);
            var runtime = server.Runtime!;

            /* With query_store_health on its default schedule the capture can refresh, so it decides: still gated. */
            Assert.True(await worker.IsQueryStoreReadableSecondaryAsync(server, runtime, ct));

            /* Disabled for this server: the frozen capture no longer counts, and the role says the server is now the primary. */
            worker.ScheduleOverridesForTest = new[] { new ScheduleOverride(id, QueryStoreHealthCollector.Instance.Name, null, null, false) };
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(server, runtime, ct));

            /* The reverse: the server becomes a secondary again, and the role alone gates it. */
            await RoleAsync(connection, id, name, "SECONDARY", TimeSpan.FromMinutes(1), ct);
            Assert.True(await worker.IsQueryStoreReadableSecondaryAsync(server, runtime, ct));

            /* A primary with no capture at all is not gated. */
            await RoleAsync(connection, id, name, "PRIMARY", TimeSpan.Zero, ct);
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(server, runtime, ct));
        });
    }
}
