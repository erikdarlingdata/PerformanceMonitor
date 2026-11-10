/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
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
/// <c>ag_replica_role</c>. A capture decides while the <c>query_store_health</c> collector can refresh it on that server; when its
/// effective schedule disables it, the capture is frozen: the collector's own run that captured no database keeps it counting
/// (#8541), and otherwise only a PRIMARY or SECONDARY role overrules it, so a failover reaches the
/// gate while a readable standby with no Availability Group role stays gated. The Query Store reads reach the same verdict.
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

    /// <summary>A schedule row that disables <paramref name="collector"/> for the server, the way an operator does for a secondary's Query Store collectors.</summary>
    private static Task DisableAsync(NpgsqlConnection c, int id, string collector, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO config.config_collector_schedules (server_id, collector_name, frequency_minutes, retention_days, enabled) VALUES ($1, $2, 60, 30, FALSE)",
            ct, id, collector);

    /// <summary>
    /// A successful <c>query_store</c> collector run on the server, as the runner logs it. <paramref name="fanout"/> is the number of
    /// databases its enumeration found to read (<c>fanout_item_count</c>); the enumeration skips every readable-secondary database and
    /// every database whose Query Store is off, and a run that found none writes no fanout at all (NULL) beside its zero rows. A live
    /// primary's run stores zero rows most slots too (closed intervals only), so rows say nothing and the fanout is the fact.
    /// </summary>
    private static Task QueryStoreRunAsync(NpgsqlConnection c, int id, string name, int? fanout, TimeSpan age, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected, fanout_item_count) VALUES ($1, $2, $3, $4, 'query_store', 'SUCCESS', 100, 0, $5)",
            ct, CollectionIdGenerator.Next(), Naive(DateTime.UtcNow - age), id, name, fanout.HasValue ? fanout.Value : DBNull.Value);

    [Fact]
    public async Task AQuietPrimary_WhoseRunFoundDatabasesButStoredNoRows_RunsTheCollector_AndARunOlderThanTheCaptureDecidesNothing()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* The same frozen all-secondary capture, query_store_health disabled and a Standalone role on three servers. The quiet
               primary's newest query_store run found two databases to read and stored no rows, as a primary's run does most slots:
               it runs the collector. The second server's only run is older than the capture and decides nothing, so the role
               decides: not gated. The third was a standby whose frozen capture lists only databases with Query Store off: nothing
               in it is a readable-secondary database, so the role decides there too, whatever its empty run says. */
            const int quietId = 840_207;
            const string quiet = "qs-quiet-primary";
            const int oldRunId = 840_208;
            const string oldRun = "qs-old-run";
            const int offId = 840_209;
            const string off = "qs-off";
            foreach (var (id, name) in new[] { (quietId, quiet), (oldRunId, oldRun), (offId, off) })
            {
                await RegisterAsync(connection, id, name, ct);
                await RoleAsync(connection, id, name, "Standalone", TimeSpan.FromMinutes(5), ct);
                await DisableAsync(connection, id, QueryStoreHealthCollector.Instance.Name, ct);
            }

            await CaptureAsync(connection, quietId, quiet, "db1", 8, TimeSpan.FromDays(19), ct);
            await QueryStoreRunAsync(connection, quietId, quiet, 2, TimeSpan.FromMinutes(2), ct);
            await CaptureAsync(connection, oldRunId, oldRun, "db1", 8, TimeSpan.FromDays(19), ct);
            await QueryStoreRunAsync(connection, oldRunId, oldRun, null, TimeSpan.FromDays(20), ct);
            await ExecAsync(connection,
                """
                INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason,
                                                current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days,
                                                max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode)
                VALUES ($1, $2, $3, $4, 'db1', 'OFF', 'OFF', 0, 0, 1000, 'AUTO', 30, 200, 60, 'AUTO', 'ON')
                """, ct, CollectionIdGenerator.Next(), Naive(DateTime.UtcNow - TimeSpan.FromDays(19)), offId, off);
            await QueryStoreRunAsync(connection, offId, off, null, TimeSpan.FromMinutes(2), ct);

            var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
            worker.StoreForTests = postgres;
            worker.ScheduleOverridesForTest = new[]
            {
                new ScheduleOverride(quietId, QueryStoreHealthCollector.Instance.Name, null, null, false),
                new ScheduleOverride(oldRunId, QueryStoreHealthCollector.Instance.Name, null, null, false),
                new ScheduleOverride(offId, QueryStoreHealthCollector.Instance.Name, null, null, false),
            };
            foreach (var (id, name) in new[] { (quietId, quiet), (oldRunId, oldRun), (offId, off) })
            {
                var server = DailyRunKit.MakeServer(id, Hints);
                Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(server, server.Runtime!, ct));
                Assert.Null(await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, id, name, ct));
            }
        });
    }

    [Fact]
    public async Task APromotedPrimary_WithTheSameFrozenCaptureAndAStandaloneRole_RunsTheCollector_OnceQueryStoreCollectsRows_WhileTheStandbyStaysGated()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* Two servers outside any availability group with the same facts a readable standby leaves behind: a frozen all-secondary
               capture, query_store_health disabled, and a Standalone role, which the properties collector writes for a standby and a
               primary alike. The newest query_store run tells them apart: on the standby it reads nothing (its statement skips the
               readable-secondary databases), on the promoted primary it collects rows. The capture alone never decides. */
            const int standbyId = 840_204;
            const string standby = "qs-standby-run";
            const int promotedId = 840_205;
            const string promoted = "qs-promoted";
            foreach (var (id, name) in new[] { (standbyId, standby), (promotedId, promoted) })
            {
                await RegisterAsync(connection, id, name, ct);
                await CaptureAsync(connection, id, name, "db1", 8, TimeSpan.FromDays(19), ct);
                await RoleAsync(connection, id, name, "Standalone", TimeSpan.FromMinutes(5), ct);
                await DisableAsync(connection, id, QueryStoreHealthCollector.Instance.Name, ct);
            }

            await QueryStoreRunAsync(connection, standbyId, standby, null, TimeSpan.FromMinutes(2), ct);
            await QueryStoreRunAsync(connection, promotedId, promoted, 2, TimeSpan.FromMinutes(2), ct);

            var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
            worker.StoreForTests = postgres;
            worker.ScheduleOverridesForTest = new[]
            {
                new ScheduleOverride(standbyId, QueryStoreHealthCollector.Instance.Name, null, null, false),
                new ScheduleOverride(promotedId, QueryStoreHealthCollector.Instance.Name, null, null, false),
            };
            var standbyServer = DailyRunKit.MakeServer(standbyId, Hints);
            var promotedServer = DailyRunKit.MakeServer(promotedId, Hints);

            Assert.True(await worker.IsQueryStoreReadableSecondaryAsync(standbyServer, standbyServer.Runtime!, ct));
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(promotedServer, promotedServer.Runtime!, ct));
            Assert.NotNull(await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, standbyId, standby, ct));
            Assert.Null(await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, promotedId, promoted, ct));

            /* With no query_store run logged at all, the frozen capture is not trusted alone: the collector runs. */
            const int quietId = 840_206;
            const string quiet = "qs-quiet";
            await RegisterAsync(connection, quietId, quiet, ct);
            await CaptureAsync(connection, quietId, quiet, "db1", 8, TimeSpan.FromDays(19), ct);
            worker.ScheduleOverridesForTest = new[] { new ScheduleOverride(quietId, QueryStoreHealthCollector.Instance.Name, null, null, false) };
            var quietServer = DailyRunKit.MakeServer(quietId, Hints);
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(quietServer, quietServer.Runtime!, ct));
        });
    }

    [Fact]
    public async Task AFrozenCapture_StillGatesAReadableStandby_WhoseRoleCarriesNoAvailabilityGroupAnswer()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* A readable standby outside any availability group: the capture, taken weeks ago, flags every database as a readable
               secondary, query_store_health is disabled for the server, server_properties carries no Availability Group role
               for it (no row at all, then Standalone, as a read replica reads), and the newest query_store run read nothing. The
               capture, corroborated by that run, is the evidence the Query Store reads judge, so the gate judges it too. */
            const int id = 840_201;
            const string name = "qs-standby";
            await RegisterAsync(connection, id, name, ct);
            await CaptureAsync(connection, id, name, "db1", 8, TimeSpan.FromDays(19), ct);
            await CaptureAsync(connection, id, name, "db2", 8, TimeSpan.FromDays(19), ct);
            await QueryStoreRunAsync(connection, id, name, null, TimeSpan.FromMinutes(2), ct);

            var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
            worker.StoreForTests = postgres;
            worker.ScheduleOverridesForTest = new[] { new ScheduleOverride(id, QueryStoreHealthCollector.Instance.Name, null, null, false) };
            var server = DailyRunKit.MakeServer(id, Hints);
            var runtime = server.Runtime!;

            /* No role row at all: the capture decides, and the slot is gated. */
            Assert.True(await worker.IsQueryStoreReadableSecondaryAsync(server, runtime, ct));

            /* Standalone carries no answer against the capture either. */
            await RoleAsync(connection, id, name, "Standalone", TimeSpan.FromMinutes(5), ct);
            Assert.True(await worker.IsQueryStoreReadableSecondaryAsync(server, runtime, ct));

            /* PRIMARY contradicts the frozen capture: a failover, and the collector runs. */
            await RoleAsync(connection, id, name, "PRIMARY", TimeSpan.FromMinutes(1), ct);
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(server, runtime, ct));
        });
    }

    [Fact]
    public async Task TheQueryStoreReads_ReachTheGatesVerdict_OnAFrozenCapture_EitherWay()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* Two servers with the same frozen all-secondary capture and query_store_health disabled in the store's schedule rows.
               One carries no role and is a readable standby: the gate holds the collector, and the reads answer not_collected.
               The other failed over and reads PRIMARY: the gate runs the collector, and the reads must not call it a secondary. */
            const int standbyId = 840_202;
            const string standby = "qs-standby-read";
            const int primaryId = 840_203;
            const string primary = "qs-failover-read";
            foreach (var (id, name) in new[] { (standbyId, standby), (primaryId, primary) })
            {
                await RegisterAsync(connection, id, name, ct);
                await CaptureAsync(connection, id, name, "db1", 8, TimeSpan.FromDays(19), ct);
                await DisableAsync(connection, id, QueryStoreHealthCollector.Instance.Name, ct);
            }

            await RoleAsync(connection, primaryId, primary, "PRIMARY", TimeSpan.FromMinutes(5), ct);
            await QueryStoreRunAsync(connection, standbyId, standby, null, TimeSpan.FromMinutes(2), ct);

            var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
            worker.StoreForTests = postgres;
            worker.ScheduleOverridesForTest = new[]
            {
                new ScheduleOverride(standbyId, QueryStoreHealthCollector.Instance.Name, null, null, false),
                new ScheduleOverride(primaryId, QueryStoreHealthCollector.Instance.Name, null, null, false),
            };
            var standbyServer = DailyRunKit.MakeServer(standbyId, Hints);
            var primaryServer = DailyRunKit.MakeServer(primaryId, Hints);

            Assert.True(await worker.IsQueryStoreReadableSecondaryAsync(standbyServer, standbyServer.Runtime!, ct));
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(primaryServer, primaryServer.Runtime!, ct));

            var standbyRead = await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, standbyId, standby, ct);
            Assert.NotNull(standbyRead);
            Assert.Contains("readable secondary", standbyRead, StringComparison.Ordinal);
            Assert.Null(await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, primaryId, primary, ct));
        });
    }

    /// <summary>
    /// A successful <c>query_store_hints</c> run on the server, as the runner logs it. The collector writes one marker row for every
    /// database it captures and skips a database whose Query Store is a readable secondary's, so <paramref name="rows"/> zero is a
    /// run that read each database's own state and captured none.
    /// </summary>
    private static Task HintsRunAsync(NpgsqlConnection c, int id, string name, int rows, TimeSpan age, CancellationToken ct) =>
        ExecAsync(c,
            "INSERT INTO collection_log (log_id, collection_time, server_id, server_name, collector_name, status, duration_ms, rows_collected) VALUES ($1, $2, $3, $4, 'query_store_hints', 'SUCCESS', 40, $5)",
            ct, CollectionIdGenerator.Next(), Naive(DateTime.UtcNow - age), id, name, rows);

    [Fact]
    public async Task AForwarder_WhoseRoleReadsPrimary_StandsDown_OnceItsOwnRunCapturedNoDatabase_AndAFailedOverPrimaryKeepsRunning()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* #8541: three servers with the same frozen all-secondary capture, query_store_health disabled, no query_store run, and
               a role that reads PRIMARY. The forwarder of a distributed availability group reads PRIMARY for its local group while
               every database's Query Store is a secondary's: its newest query_store_hints run captured no database, so the gate
               closes and the reads call it a secondary. The promoted server's run wrote marker rows, so it is a primary and the
               collector runs. The third server's empty run is two hours old: the reads keep its answer, and the gate opens for one
               run so the databases' own state is read again. */
            const int forwarderId = 854_101;
            const string forwarder = "qs-forwarder";
            const int promotedId = 854_102;
            const string promoted = "qs-promoted-primary";
            const int probeId = 854_103;
            const string probe = "qs-forwarder-probe-due";
            foreach (var (id, name) in new[] { (forwarderId, forwarder), (promotedId, promoted), (probeId, probe) })
            {
                await RegisterAsync(connection, id, name, ct);
                await RoleAsync(connection, id, name, "PRIMARY", TimeSpan.FromMinutes(5), ct);
                await DisableAsync(connection, id, QueryStoreHealthCollector.Instance.Name, ct);
                await CaptureAsync(connection, id, name, "db1", 8, TimeSpan.FromDays(19), ct);
            }

            await HintsRunAsync(connection, forwarderId, forwarder, 0, TimeSpan.FromMinutes(2), ct);
            await HintsRunAsync(connection, promotedId, promoted, 3, TimeSpan.FromMinutes(2), ct);
            await HintsRunAsync(connection, probeId, probe, 0, TimeSpan.FromHours(2), ct);

            var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
            worker.StoreForTests = postgres;
            worker.ScheduleOverridesForTest = new[]
            {
                new ScheduleOverride(forwarderId, QueryStoreHealthCollector.Instance.Name, null, null, false),
                new ScheduleOverride(promotedId, QueryStoreHealthCollector.Instance.Name, null, null, false),
                new ScheduleOverride(probeId, QueryStoreHealthCollector.Instance.Name, null, null, false),
            };

            var forwarderServer = DailyRunKit.MakeServer(forwarderId, Hints);
            Assert.True(await worker.IsQueryStoreReadableSecondaryAsync(forwarderServer, forwarderServer.Runtime!, ct));
            Assert.NotNull(await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, forwarderId, forwarder, ct));

            var promotedServer = DailyRunKit.MakeServer(promotedId, Hints);
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(promotedServer, promotedServer.Runtime!, ct));
            Assert.Null(await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, promotedId, promoted, ct));

            var probeServer = DailyRunKit.MakeServer(probeId, Hints);
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(probeServer, probeServer.Runtime!, ct));
            Assert.NotNull(await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, probeId, probe, ct));
        });
    }

    [Fact]
    public async Task AForwarder_WhoseCaptureHasAgedOutOfRetention_StaysStoodDown_OnItsOwnRunThatStoredNoRow_AndRunsAgainOnceThatRunIsAnHourOld()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* #8541: the daily purge removes a query_store_health capture past its retention whether or not its collector runs, so
               the capture that called these servers secondaries is gone, and the role still reads PRIMARY. The collector's own
               newest run is what is left: one that stored no row found no database to capture, and the gate stays closed for an
               hour on it. A run that old lets one run through, and a run that stored marker rows is a server with databases to
               read. With no capture the store holds nothing that says secondary, so the reads do not say it. */
            const int agedOutId = 854_131;
            const string agedOut = "qs-forwarder-no-capture";
            const int probeId = 854_132;
            const string probe = "qs-forwarder-no-capture-probe-due";
            const int capturingId = 854_133;
            const string capturing = "qs-primary-no-capture";
            foreach (var (id, name) in new[] { (agedOutId, agedOut), (probeId, probe), (capturingId, capturing) })
            {
                await RegisterAsync(connection, id, name, ct);
                await RoleAsync(connection, id, name, "PRIMARY", TimeSpan.FromMinutes(5), ct);
                await DisableAsync(connection, id, QueryStoreHealthCollector.Instance.Name, ct);
            }

            await HintsRunAsync(connection, agedOutId, agedOut, 0, TimeSpan.FromMinutes(2), ct);
            await HintsRunAsync(connection, probeId, probe, 0, TimeSpan.FromHours(2), ct);
            await HintsRunAsync(connection, capturingId, capturing, 3, TimeSpan.FromMinutes(2), ct);

            var worker = DailyRunKit.MakeWorker(new DailyRunLogger());
            worker.StoreForTests = postgres;
            worker.ScheduleOverridesForTest = new[]
            {
                new ScheduleOverride(agedOutId, QueryStoreHealthCollector.Instance.Name, null, null, false),
                new ScheduleOverride(probeId, QueryStoreHealthCollector.Instance.Name, null, null, false),
                new ScheduleOverride(capturingId, QueryStoreHealthCollector.Instance.Name, null, null, false),
            };

            var agedOutServer = DailyRunKit.MakeServer(agedOutId, Hints);
            Assert.True(await worker.IsQueryStoreReadableSecondaryAsync(agedOutServer, agedOutServer.Runtime!, ct));
            Assert.Null(await DarlingRuntimePrecondition.QueryStoreReadableSecondaryMessageAsync(postgres, agedOutId, agedOut, ct));

            var probeServer = DailyRunKit.MakeServer(probeId, Hints);
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(probeServer, probeServer.Runtime!, ct));

            var capturingServer = DailyRunKit.MakeServer(capturingId, Hints);
            Assert.False(await worker.IsQueryStoreReadableSecondaryAsync(capturingServer, capturingServer.Runtime!, ct));
        });
    }

    [Fact]
    public async Task AReadOnlyQueryStore_WhoseReasonIsTheSecondaryReplica_IsNamedAsThat_AndIsNotToldToRaiseTheStorageCap()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* #8541: the capture's own readonly_reason is 8, a secondary replica. No storage setting changes that, so the answer
               names the cause and gives no storage cap to raise. */
            const int id = 854_111;
            const string name = "qs-readonly-secondary";
            await RegisterAsync(connection, id, name, ct);
            await CaptureAsync(connection, id, name, "db1", 8, TimeSpan.FromMinutes(5), ct);

            var answer = await DarlingRuntimePrecondition.QueryStoreStatusAsync(postgres, id, name, (string?)null, ct);

            Assert.NotNull(answer);
            Assert.DoesNotContain("MAX_STORAGE_SIZE_MB", answer, StringComparison.Ordinal);
            Assert.Contains("secondary replica", answer, StringComparison.Ordinal);
            Assert.Contains("readonly_reason 8", answer, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task AStaleHealthCapture_SaysTheCollectorIsDisabled_WhereItsScheduleIsOff_AndNamesTheHourlyCadenceOnlyWhereItRuns()
    {
        await RunAsync(async (connection, postgres, ct) =>
        {
            /* #8541: two servers whose newest capture is 19 days old. The first has query_store_health disabled, so no newer
               capture is coming and the note says so. The second runs the collector, and its note keeps the hourly cadence. */
            const int disabledId = 854_121;
            const string disabled = "qs-health-disabled";
            const int enabledId = 854_122;
            const string enabled = "qs-health-enabled";
            foreach (var (id, name) in new[] { (disabledId, disabled), (enabledId, enabled) })
            {
                await RegisterAsync(connection, id, name, ct);
                await CaptureAsync(connection, id, name, "db1", 0, TimeSpan.FromDays(19), ct);
            }

            await DisableAsync(connection, disabledId, QueryStoreHealthCollector.Instance.Name, ct);

            using var disabledAnswer = JsonDocument.Parse(await DarlingMcpConfigHistoryTools.GetQueryStoreHealth(postgres, disabled));
            var disabledNote = disabledAnswer.RootElement.GetProperty("note").GetString();
            Assert.NotNull(disabledNote);
            Assert.DoesNotContain("runs hourly", disabledNote, StringComparison.Ordinal);
            Assert.Contains("is disabled", disabledNote, StringComparison.Ordinal);

            using var enabledAnswer = JsonDocument.Parse(await DarlingMcpConfigHistoryTools.GetQueryStoreHealth(postgres, enabled));
            var enabledNote = enabledAnswer.RootElement.GetProperty("note").GetString();
            Assert.NotNull(enabledNote);
            Assert.Contains("runs hourly", enabledNote, StringComparison.Ordinal);
        });
    }
}
