/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5735: the per-server tally of stepped-over slots, the report worded from it, and the 5-minute floor between rows.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CycleSkipAccumulatorTests
{
    private static readonly DateTime T0 = new(2026, 10, 12, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>Three slots stepped over on one server, across two collectors, are one report: 3 slots, both ends of the gap
    /// named, the top collectors listed, no equals sign (a baseline marker read keys on one).</summary>
    [Fact]
    public void ThreeSlotsOnOneServer_AreOneReport_WithBothEndsAndNoEqualsSign()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("query_stats", 2, T0.AddMinutes(-3));
        tally.AddGrid("wait_stats", 1, T0.AddMinutes(-2));

        var taken = tally.TryTake(T0, DarlingCycleSkip.RowMinInterval);
        Assert.NotNull(taken);
        var report = DarlingCycleSkip.Build(taken!, new CycleSkipCauses(185_000, "query_store", 170_000, 0), T0);

        Assert.NotNull(report);
        Assert.Equal(3, report!.SlotsSkipped);
        Assert.Equal(185_000, report.DurationMs);
        Assert.Equal("query_store", report.SlowestItem);
        Assert.Equal(170_000, report.SlowestItemMs);
        Assert.Contains("3 slot(s) skipped across 2 collector(s) (top: query_stats 2, wait_stats 1)", report.Message, StringComparison.Ordinal);
        Assert.Contains("the previous collection pass took 185s", report.Message, StringComparison.Ordinal);
        Assert.Contains("The gap ran from 2026-10-12 11:57:00 UTC to 2026-10-12 12:00:00 UTC.", report.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("=", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCauseIsTheLargestMeasured_AndTheMessageListsEveryOne()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("wait_stats", 4, T0.AddMinutes(-6));
        tally.AddHeld("query_stats", 5, T0.AddMinutes(-6));
        tally.AddDailyLost("index_object_stats", 1, T0.AddMinutes(-6));
        var taken = tally.TryTake(T0, DarlingCycleSkip.RowMinInterval)!;

        var report = DarlingCycleSkip.Build(taken, new CycleSkipCauses(0, null, 0, 45_000), T0)!;
        Assert.Equal(10, report.SlotsSkipped);

        /* The hold's length is the gap (6 minutes), longer than the 45 s gate wait. */
        Assert.Equal(DarlingCycleSkip.LaunchGuardHoldName, report.SlowestItem);
        Assert.Equal(360_000, report.SlowestItemMs);
        Assert.Contains("waited 45s for a fleet collection slot", report.Message, StringComparison.Ordinal);
        Assert.Contains("memory launch guard held collection off for 5 slot(s)", report.Message, StringComparison.Ordinal);
        Assert.Contains("1 run-time collector slot(s) lost their day (index object stats)", report.Message, StringComparison.Ordinal);

        var gateOnly = new CycleSkipAccumulator();
        gateOnly.AddGrid("wait_stats", 1, T0.AddMinutes(-1));
        var gate = DarlingCycleSkip.Build(gateOnly.TryTake(T0, DarlingCycleSkip.RowMinInterval)!, new CycleSkipCauses(0, null, 0, 90_000), T0)!;
        Assert.Equal(DarlingCycleSkip.GateWaitName, gate.SlowestItem);
        Assert.Equal(90_000, gate.SlowestItemMs);
        Assert.Equal(90_000, gate.DurationMs);
    }

    [Fact]
    public void ANoticeableCauseIsNeeded_QuickBodiesAreNotBlamed()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("wait_stats", 1, T0.AddMinutes(-1));
        var report = DarlingCycleSkip.Build(tally.TryTake(T0, DarlingCycleSkip.RowMinInterval)!, new CycleSkipCauses(4_000, "wait_stats", 3_000, 200), T0)!;
        Assert.Equal(DarlingCycleSkip.UnmeasuredName, report.SlowestItem);
        Assert.Equal(0, report.SlowestItemMs);
        Assert.Contains("no single cause was measured", report.Message, StringComparison.Ordinal);
    }

    /// <summary>The 5-minute floor holds: after a row, counts keep adding but no snapshot is offered before 5 minutes, and the
    /// row then carries everything added in between. A write that failed (no Commit) keeps its counts.</summary>
    [Fact]
    public void TheFiveMinuteFloorHolds_CountsKeepAdding_AndAFailedWriteKeepsThem()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("wait_stats", 3, T0.AddMinutes(-3));
        var first = tally.TryTake(T0, DarlingCycleSkip.RowMinInterval)!;

        /* The write failed: nothing committed, so the next body still has all three. */
        Assert.Equal(3, tally.TryTake(T0.AddMinutes(1), DarlingCycleSkip.RowMinInterval)!.Total);

        tally.Commit(first, T0);
        Assert.Equal(0, tally.Pending);
        Assert.Null(tally.TryTake(T0.AddMinutes(1), DarlingCycleSkip.RowMinInterval));

        tally.AddGrid("wait_stats", 2, T0.AddMinutes(1));
        tally.AddGrid("query_stats", 1, T0.AddMinutes(2));
        Assert.Equal(3, tally.Pending);
        Assert.Null(tally.TryTake(T0.AddMinutes(4).AddSeconds(59), DarlingCycleSkip.RowMinInterval));

        var second = tally.TryTake(T0.AddMinutes(5), DarlingCycleSkip.RowMinInterval)!;
        Assert.Equal(3, second.Total);
        Assert.Equal(T0.AddMinutes(1), second.EarliestUtc);

        /* Slots added while the second write was in flight stay pending after it commits. */
        tally.AddGrid("wait_stats", 4, T0.AddMinutes(5));
        tally.Commit(second, T0.AddMinutes(5));
        Assert.Equal(4, tally.Pending);
    }

    [Fact]
    public void ZeroSlots_AddNothing_AndEmptyTalliesOfferNothing()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("wait_stats", 0, T0);
        tally.AddHeld("wait_stats", -1, T0);
        Assert.Null(tally.TryTake(T0.AddHours(1), DarlingCycleSkip.RowMinInterval));
    }

    /// <summary>The tally is per server, so two servers' bodies write two rows.</summary>
    [Fact]
    public void TwoServers_KeepTwoTallies()
    {
        var a = SelfAlertFleetPassTests.MakeServer("a", connected: true);
        var b = SelfAlertFleetPassTests.MakeServer("b", connected: true);
        a.CycleSkips.AddGrid("wait_stats", 3, T0);
        Assert.Equal(3, a.CycleSkips.Pending);
        Assert.Equal(0, b.CycleSkips.Pending);
    }
}

/// <summary>#5735: the places that step over a slot feed the server's tally, and a held or slept host writes nothing it should not.</summary>
public sealed class CycleSkipFeedTests
{
    /// <summary>CountHeldSlots feeds each server's own tally, per collector, and never writes (it runs on the sweep thread).</summary>
    [Fact]
    public void CountHeldSlots_FeedsEachServersTally_PerCollector()
    {
        var worker = SelfAlertFleetPassTests.MakeWorker();
        var one = SelfAlertFleetPassTests.MakeServer("held-one", connected: true);
        var two = SelfAlertFleetPassTests.MakeServer("held-two", connected: true);
        var held = DateTime.UtcNow.AddMinutes(-10);
        one.NextDue["wait_stats"] = held;
        two.NextDue["wait_stats"] = held;
        two.NextDue["query_stats"] = held;

        var counted = worker.CountHeldSlots([one, two], held.AddMinutes(10));

        Assert.Equal(30, counted);
        Assert.Equal(10, one.CycleSkips.Pending);
        Assert.Equal(20, two.CycleSkips.Pending);
        var snapshot = two.CycleSkips.TryTake(DateTime.UtcNow, DarlingCycleSkip.RowMinInterval)!;
        Assert.Equal(10, snapshot.HeldSlots["wait_stats"]);
        Assert.Equal(10, snapshot.HeldSlots["query_stats"]);
    }

    /// <summary>The three feed sites and the flush are where the plan puts them; a later edit cannot drop one without this saying so.</summary>
    [Fact]
    public void TheFeedSitesAndTheFlushAreWired()
    {
        var worker = File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));
        Assert.Contains("server.CycleSkips.AddGrid(name, gridSkipped, due);", worker, StringComparison.Ordinal);
        Assert.Contains("server.CycleSkips.AddDailyLost(name, step.Skipped, due);", worker, StringComparison.Ordinal);
        Assert.Contains("server.CycleSkips.AddHeld(name, newlyHeld, due);", worker, StringComparison.Ordinal);
        Assert.Contains("await FlushCycleSkipAsync(server, runtime, priorCauses, cancellationToken);", worker, StringComparison.Ordinal);
        Assert.Contains("server.LastGateWaitMs = (long)gateWaited.TotalMilliseconds;", worker, StringComparison.Ordinal);
    }

    internal static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}

/// <summary>
/// #5735 live: the writer binds all 32 parameters, a tally becomes one row, and every reader keeps its old answer with a
/// skipped-slots row present.
/// </summary>
[Collection("live-postgres")]
public sealed class CollectionCycleRowLiveTests
{
    private const int WriterServerId = -573501;
    private const int ServerA = -573502;
    private const int ServerB = -573503;
    private const int ReaderServerOld = -573504;   // a collector row, then a NEWER skip row
    private const int ReaderServerOnly = -573505;  // skip rows only
    private static readonly int[] AllIds = [WriterServerId, ServerA, ServerB, ReaderServerOld, ReaderServerOnly];

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static ServerRuntime Runtime(int serverId, string name)
    {
        var config = new MonitoredServer { Name = name, Host = name + ".invalid" };
        return new ServerRuntime
        {
            Config = config,
            ConnectionString = "Server=tcp:" + name + ",1433;Initial Catalog=master;Encrypt=True",
            Target = new CollectorTargetInfo(),
            StorageName = name,
            ServerId = serverId,
        };
    }

    private static DarlingWorker.ServerLoopState LoopServer(string name, int serverId)
    {
        var runtime = Runtime(serverId, name);
        return new DarlingWorker.ServerLoopState { Config = runtime.Config, Runtime = runtime };
    }

    private static DateTime MinutesAgo(int minutes) =>
        DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow.AddMinutes(-minutes));

    private static async Task CleanAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var id in AllIds)
        {
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM collection_log WHERE server_id = $1", id);
            await DarlingMcpTestData.ExecAsync(connection, ct, "DELETE FROM servers WHERE server_id = $1", id);
        }
    }

    /// <summary>The writer: one row lands, every column read back. A wrong parameter count fails silently (a Debug log), so
    /// this reads the whole row rather than counting it.</summary>
    [Fact]
    public async Task TheWriter_LandsOneRow_WithEveryColumnAsDocumented()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live cycle-skip tests.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var stamp = MinutesAgo(2);
            var report = new CycleSkipReport(7, 185_000, "query_store", 170_000, "Collection fell behind on this server: 7 slot(s).");

            var written = await DarlingObservability.LogCycleSkipAsync(postgres, Runtime(WriterServerId, "cycle-writer"), stamp, report, null, ct);
            Assert.True(written);

            using var command = new NpgsqlCommand("SELECT * FROM collection_log WHERE server_id = $1", connection);
            command.Parameters.AddWithValue(WriterServerId);
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));

            var values = Enumerable.Range(0, reader.FieldCount)
                .ToDictionary(reader.GetName, i => reader.IsDBNull(i) ? null : reader.GetValue(i));
            Assert.False(await reader.ReadAsync(ct), "exactly one row");

            Assert.Equal("cycle-writer", values["server_name"]);
            Assert.Equal(CollectionCycleRow.CollectorName, values["collector_name"]);
            Assert.Equal(CollectionCycleRow.Status, values["status"]);
            Assert.Equal(stamp, (DateTime)values["collection_time"]!);
            Assert.Equal(185_000, values["duration_ms"]);
            Assert.Equal(0, values["rows_collected"]);
            Assert.Equal(7, values["fanout_item_count"]);
            Assert.Equal("query_store", values["slowest_item"]);
            Assert.Equal(170_000, values["slowest_item_ms"]);
            Assert.Equal("Collection fell behind on this server: 7 slot(s).", values["error_message"]);
            Assert.Null(values["sql_duration_ms"]);
            Assert.Null(values["duckdb_duration_ms"]);

            var expected = new HashSet<string>
            {
                "log_id", "server_id", "server_name", "collector_name", "collection_time", "duration_ms", "status",
                "error_message", "rows_collected", "fanout_item_count", "slowest_item", "slowest_item_ms",
            };
            var unexpected = values.Where(p => !expected.Contains(p.Key) && p.Value is not null).Select(p => p.Key).ToList();
            Assert.True(unexpected.Count == 0, "columns that should be NULL: " + string.Join(", ", unexpected));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, CleanAsync);
        }
    }

    /// <summary>Three stepped-over slots on one server write one row (fanout 3); a second flush inside the 5 minutes writes
    /// none; a second server writes its own row.</summary>
    [Fact]
    public async Task ATally_BecomesOneRowPerServer_AndTheFiveMinuteCapHolds()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live cycle-skip tests.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var worker = SelfAlertFleetPassTests.MakeWorker();
            worker.StoreForTests = postgres;

            var a = LoopServer("cycle-a", ServerA);
            var b = LoopServer("cycle-b", ServerB);
            var none = default(CycleSkipCauses);

            a.CycleSkips.AddGrid("wait_stats", 2, DateTime.UtcNow.AddMinutes(-3));
            a.CycleSkips.AddGrid("query_stats", 1, DateTime.UtcNow.AddMinutes(-3));
            b.CycleSkips.AddGrid("wait_stats", 5, DateTime.UtcNow.AddMinutes(-3));

            await worker.FlushCycleSkipAsync(a, a.Runtime!, none, ct);
            await worker.FlushCycleSkipAsync(b, b.Runtime!, none, ct);
            Assert.Equal(0, a.CycleSkips.Pending);

            /* More slots inside the 5 minutes: counted, not written. */
            a.CycleSkips.AddGrid("wait_stats", 4, DateTime.UtcNow.AddMinutes(-1));
            await worker.FlushCycleSkipAsync(a, a.Runtime!, none, ct);
            Assert.Equal(4, a.CycleSkips.Pending);

            var rows = new Dictionary<int, List<int>>();
            using var command = new NpgsqlCommand(
                "SELECT server_id, fanout_item_count FROM collection_log WHERE collector_name = $1 AND server_id = ANY($2)", connection);
            command.Parameters.AddWithValue(CollectionCycleRow.CollectorName);
            command.Parameters.AddWithValue(new[] { ServerA, ServerB });
            await using (var reader = await command.ExecuteReaderAsync(ct))
            {
                while (await reader.ReadAsync(ct))
                {
                    if (!rows.TryGetValue(reader.GetInt32(0), out var list))
                    {
                        rows[reader.GetInt32(0)] = list = [];
                    }

                    list.Add(reader.GetInt32(1));
                }
            }

            Assert.Equal([3], rows[ServerA]);
            Assert.Equal([5], rows[ServerB]);

            /* A retired server writes nothing. */
            var retired = LoopServer("cycle-retired", ServerA);
            retired.Retired = true;
            retired.CycleSkips.AddGrid("wait_stats", 1, DateTime.UtcNow.AddMinutes(-9));
            await worker.FlushCycleSkipAsync(retired, retired.Runtime!, none, ct);
            Assert.Equal(1, retired.CycleSkips.Pending);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, CleanAsync);
        }
    }

    private sealed record Fixture(NpgsqlDataSource Postgres, DateTime OldRun, DateTime NewestSkip);

    /// <summary>ReaderServerOld: one wait_stats SUCCESS 30 minutes ago and a NEWER skip row (5 minutes ago).
    /// ReaderServerOnly: skip rows only.</summary>
    private static async Task<Fixture> SeedReadersAsync(NpgsqlConnection connection, NpgsqlDataSource postgres, CancellationToken ct)
    {
        foreach (var (id, name) in new[] { (ReaderServerOld, "cycle-reader-old"), (ReaderServerOnly, "cycle-reader-only") })
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, name, ct);
        }

        var oldRun = MinutesAgo(30);
        var newestSkip = MinutesAgo(5);
        await InsertAsync(connection, ct, ReaderServerOld, "cycle-reader-old", "wait_stats", "SUCCESS", oldRun, 10);
        await InsertAsync(connection, ct, ReaderServerOld, "cycle-reader-old", CollectionCycleRow.CollectorName, CollectionCycleRow.Status, newestSkip, 0);
        await InsertAsync(connection, ct, ReaderServerOnly, "cycle-reader-only", CollectionCycleRow.CollectorName, CollectionCycleRow.Status, newestSkip, 0);
        await InsertAsync(connection, ct, ReaderServerOnly, "cycle-reader-only", CollectionCycleRow.CollectorName, CollectionCycleRow.Status, MinutesAgo(10), 0);
        return new Fixture(postgres, oldRun, newestSkip);
    }

    private static Task InsertAsync(NpgsqlConnection connection, CancellationToken ct, int serverId, string name, string collector, string status, DateTime at, int rows) =>
        DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, $3, $4, $5, 100, $6, NULL, $7, 80, 20)",
            CollectionIdGenerator.Next(), serverId, name, collector, DarlingMcpTestData.Naive(at), status, rows);

    private static async Task RunReaderAsync(Func<Fixture, NpgsqlConnection, CancellationToken, Task> body)
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live cycle-skip tests.");
        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            await using var postgres = NpgsqlDataSource.Create(cs!);
            var fixture = await SeedReadersAsync(connection, postgres, ct);
            await body(fixture, connection, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, CleanAsync);
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlDataSource postgres, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = postgres.CreateCommand(sql);
        foreach (var v in values)
        {
            command.Parameters.AddWithValue(v);
        }

        var result = await command.ExecuteScalarAsync(ct);
        return result is DBNull ? null : result;
    }

    /// <summary>Group 1: the self-alert signals. The Collection Stopped live test is in DarlingSelfAlertTests; this is the read.</summary>
    [Fact]
    public Task Group1_CollectionSignals_IgnoreSkipRows() => RunReaderAsync(async (f, _, ct) =>
    {
        var evaluator = DarlingSelfAlertTests.LiveReadEvaluatorForTests();
        var (last, runs, success) = await evaluator.ReadCollectionSignalsAsync(f.Postgres, ReaderServerOld, 10, ct);
        Assert.Equal(f.OldRun, last);
        Assert.Equal(1, runs);
        Assert.Equal(1, success);

        var (lastOnly, runsOnly, successOnly) = await evaluator.ReadCollectionSignalsAsync(f.Postgres, ReaderServerOnly, 10, ct);
        Assert.Null(lastOnly);
        Assert.Equal(0, runsOnly);
        Assert.Equal(0, successOnly);
    });

    /// <summary>Group 2: get_collection_health's per-collector aggregate lists the collector, not the cycle row.</summary>
    [Fact]
    public Task Group2_CollectionHealth_ListsNoCycleCollector() => RunReaderAsync(async (f, _, ct) =>
    {
        var health = await DarlingDataReader.GetCollectionHealthAsync(f.Postgres, ReaderServerOld, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)), ct);
        var row = Assert.Single(health);
        Assert.Equal("wait_stats", row.CollectorName);
        Assert.Equal(1, row.TotalRuns);
        Assert.Empty(await DarlingDataReader.GetCollectionHealthAsync(f.Postgres, ReaderServerOnly, DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)), ct));
    });

    /// <summary>Group 3: the fleet's raw health read, and the rollup arm (the cagg read and the head and hole slices carry the same predicate).</summary>
    [Fact]
    public Task Group3_FleetHealth_RawAndRollup() => RunReaderAsync(async (f, _, ct) =>
    {
        await using var command = f.Postgres.CreateCommand(DarlingFleetReader.FleetCollectionHealthSql);
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)));
        var collectors = new List<(int Server, string Collector)>();
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                collectors.Add((reader.GetInt32(0), reader.GetString(1)));
            }
        }

        Assert.Contains((ReaderServerOld, "wait_stats"), collectors);
        Assert.DoesNotContain(collectors, c => c.Collector == CollectionCycleRow.CollectorName);

        var composed = CollectionHealthRollupSupport.ComposeFleetSql(DarlingFleetReader.FleetCollectionHealthSql, [DateTime.UtcNow.Date]);
        var rollupArm = composed.Split("UNION ALL", StringSplitOptions.None)[0];
        Assert.Contains(CollectionCycleRow.NotCycleRowSql, rollupArm, StringComparison.Ordinal);
        Assert.True(Regex.Matches(composed, Regex.Escape(CollectionCycleRow.NotCycleRowSql)).Count >= 3,
            "the cagg arm, the head slice and the hole slice each carry the exclusion");
    });

    /// <summary>Group 4: "last collected" reads answer with the collector's run, not the newer skip row.</summary>
    [Fact]
    public Task Group4_NewestRowReads_AnswerWithTheCollectorRun() => RunReaderAsync(async (f, _, ct) =>
    {
        var list = await DarlingDataReader.GetServerListAsync(f.Postgres, ct);
        Assert.Equal(f.OldRun, list.Single(s => s.ServerId == ReaderServerOld).LastCollection);
        Assert.Null(list.Single(s => s.ServerId == ReaderServerOnly).LastCollection);

        Assert.Equal(f.OldRun, await ScalarAsync(f.Postgres, DarlingHealthReader.ServerSummaryLastCollectionSql, ct, ReaderServerOld));
        Assert.Null(await ScalarAsync(f.Postgres, DarlingHealthReader.ServerSummaryLastCollectionSql, ct, ReaderServerOnly));

        Assert.Equal(f.OldRun, await ScalarAsync(f.Postgres, ViewerDataService.ServerSummaryLastCollectionSql, ct, ReaderServerOld));

        await using var command = f.Postgres.CreateCommand(DarlingRuntimePrecondition.CollectorLastRunSql);
        command.Parameters.AddWithValue(ReaderServerOld);
        command.Parameters.AddWithValue("wait_stats");
        await using var reader = await command.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        var lastCollected = reader.GetDateTime(reader.GetOrdinal("server_last_collected"));
        var firstCollected = reader.GetDateTime(reader.GetOrdinal("server_first_collected"));
        Assert.Equal(f.OldRun, lastCollected);
        Assert.Equal(f.OldRun, firstCollected);
    });

    /// <summary>Group 5: the per-day run count is made of collector runs only (text pin: the read takes seven parameters).</summary>
    [Fact]
    public void Group5_DailySummaryRunCount_CarriesTheExclusion() =>
        Assert.Contains(CollectionCycleRow.NotCycleRowSql, DailySummarySql.RangeSql, StringComparison.Ordinal);

    /// <summary>Group 6: "any row" existence is false for a server whose only rows are skip rows.</summary>
    [Fact]
    public Task Group6_AnyRowExistence_IgnoresSkipRows() => RunReaderAsync(async (f, _, ct) =>
    {
        Assert.Null(await ScalarAsync(f.Postgres, DarlingDataReader.HasAnyCollectionLogSql, ct, ReaderServerOnly));
        Assert.NotNull(await ScalarAsync(f.Postgres, DarlingDataReader.HasAnyCollectionLogSql, ct, ReaderServerOld));
        Assert.Null(await ScalarAsync(f.Postgres, ServerTrendSql.HasAnyCollectionLog, ct, ReaderServerOnly));
        Assert.NotNull(await ScalarAsync(f.Postgres, ServerTrendSql.HasAnyCollectionLog, ct, ReaderServerOld));
    });

    /// <summary>Group 7: the Viewer's Collection Health grid lists the collector, not the cycle row.</summary>
    [Fact]
    public Task Group7_ViewerCollectionHealth_ListsNoCycleCollector() => RunReaderAsync(async (f, _, ct) =>
    {
        await using var command = f.Postgres.CreateCommand(ViewerDataService.CollectionHealthSql);
        command.Parameters.AddWithValue(ReaderServerOld);
        command.Parameters.AddWithValue(DarlingMcpTestData.Naive(DateTime.UtcNow.AddDays(-7)));
        var names = new List<string>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            names.Add(reader.GetString(0));
        }

        Assert.Equal(["wait_stats"], names);
    });
}

/// <summary>
/// #5735: every read of <c>collection_log</c> in Darling's code either leaves the skipped-slots row out, names its collector
/// or <c>status = 'SUCCESS'</c> (which the row never has), or is on the safe list below with its reason. And no file but
/// <c>CollectionCycleRow.cs</c> spells the collector's name as a literal.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CollectionCycleReaderCensusTests
{
    private static readonly string[] SourceRoots =
    [
        "PerformanceMonitor.Darling.Service", "PerformanceMonitor.Darling.Storage", "PerformanceMonitor.Darling.Viewer",
        "PerformanceMonitor.Darling.Analysis",
    ];

    /// <summary>File name to the reason every unfiltered read in it is safe. A new unfiltered read in a file NOT listed fails
    /// the census; a listed file's reason must still hold for the new read.</summary>
    private static readonly Dictionary<string, string> SafeList = new(StringComparer.Ordinal)
    {
        ["DarlingDataReader.cs"] = "get_collection_log and its fleet twin are the raw log by design",
        ["DarlingObservability.cs"] = "the writers, and the sentinel-server retention reads",
        ["DarlingStoreUpgrade.cs"] = "a plain post-upgrade row count that proves the cluster answers; any row will do",
        ["PgMigrations.cs"] = "one-time, frozen DDL and data rewrites, never a reader",
        ["TimescaleSupport.cs"] = "the collection_health_hourly aggregate definition: it groups by collector_name, so the extra group is harmless, and a changed definition would force a drop and rebuild; the readers filter instead (CollectionHealthRollupSupport)",
        ["ViewerDataService.CollectionHealth.cs"] = "the raw log grid by design (RecentCollectionLogSql); the other reads join the already-filtered per-collector aggregate or name a collector",
        ["DarlingSelfAlertEvaluator.cs"] = "MissingCaptureSessionsSql names its two collectors",
    };

    private static readonly Regex ReadStart = new(@"\bFROM\s+(?:collect\.)?(?:v_)?collection_log\b", RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static IEnumerable<string> SourceFiles()
    {
        var root = Path.Combine(CycleSkipFeedTests.RepoRoot(), "Darling");
        foreach (var project in SourceRoots)
        {
            var dir = Path.Combine(root, project);
            if (!Directory.Exists(dir))
            {
                continue;
            }

            foreach (var file in Directory.EnumerateFiles(dir, "*.cs", SearchOption.AllDirectories))
            {
                var rel = file.Replace('\\', '/');
                if (rel.Contains("/obj/", StringComparison.Ordinal) || rel.Contains("/bin/", StringComparison.Ordinal))
                {
                    continue;
                }

                yield return file;
            }
        }
    }

    /// <summary>The code with comments blanked, so a doc comment that mentions a read is not a read.</summary>
    private static string WithoutComments(string text) =>
        Regex.Replace(text, @"/\*.*?\*/|//[^\r\n]*", string.Empty, RegexOptions.Singleline | RegexOptions.CultureInvariant);

    [Fact]
    public void EveryCollectionLogRead_ExcludesTheCycleRow_NamesACollector_OrIsOnTheSafeList()
    {
        var violations = new List<string>();
        var read = 0;
        foreach (var file in SourceFiles())
        {
            var name = Path.GetFileName(file);
            var text = WithoutComments(File.ReadAllText(file));
            foreach (Match m in ReadStart.Matches(text))
            {
                read++;

                /* The statement: from the FROM to the end of its string literal (a closing quote, a semicolon, or 900 chars). */
                var tail = text.Substring(m.Index, Math.Min(900, text.Length - m.Index));
                var end = tail.IndexOf("\"\"\"", StringComparison.Ordinal);
                var close = tail.IndexOf("\";", StringComparison.Ordinal);
                if (close >= 0 && (end < 0 || close < end))
                {
                    end = close;
                }

                var statement = end > 0 ? tail[..end] : tail;
                var safe = statement.Contains("NotCycleRowSql", StringComparison.Ordinal)
                    || Regex.IsMatch(statement, @"collector_name\s*(=|IN\b|<>\s*ALL|LIKE\b)", RegexOptions.IgnoreCase)
                    || Regex.IsMatch(statement, @"\bstatus\s*=\s*'(SUCCESS|PERMISSIONS|SESSION_MISSING)'", RegexOptions.IgnoreCase)
                    || Regex.IsMatch(statement, @"server_id\s*=\s*\$?\{?\w*FleetServerId", RegexOptions.IgnoreCase)
                    || SafeList.ContainsKey(name);
                if (!safe)
                {
                    var line = text[..m.Index].Count(c => c == '\n') + 1;
                    violations.Add($"{name}:{line}");
                }
            }
        }

        Assert.True(read > 20, $"the census saw only {read} collection_log reads; the pattern or the roots drifted");
        Assert.True(violations.Count == 0,
            "collection_log reads with no collection_cycle exclusion, collector name or safe-list reason (#5735): " + string.Join(", ", violations));
    }

    [Fact]
    public void OnlyTheConstantsFile_SpellsTheCollectorNameAsALiteral()
    {
        var offenders = new List<string>();
        foreach (var file in SourceFiles())
        {
            if (Path.GetFileName(file) == "CollectionCycleRow.cs")
            {
                continue;
            }

            var text = File.ReadAllText(file);
            if (text.Contains("'collection_cycle'", StringComparison.Ordinal) || text.Contains("\"collection_cycle\"", StringComparison.Ordinal))
            {
                offenders.Add(Path.GetFileName(file));
            }
        }

        Assert.True(offenders.Count == 0, "the collector name is spelled in " + string.Join(", ", offenders) + "; use CollectionCycleRow.CollectorName");
    }

    [Fact]
    public void TheSafeList_NamesFilesThatExist()
    {
        var names = SourceFiles().Select(Path.GetFileName).ToHashSet(StringComparer.Ordinal);
        Assert.All(SafeList.Keys, k => Assert.Contains(k, names));
    }
}
