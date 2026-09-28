/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4659: the forced-plan failure read runs its two-hour aggregate only when nothing it depends on has changed;
/// otherwise the previous answer is reused, less any plan whose older collection left the window. Every live row
/// here is written through the collector runner's own <c>WriteBatchAsync</c>, so the write fence the runner brackets
/// each Query Store batch with is the real one, and each answer is compared with <c>ForcePlanFailuresSql</c> run
/// directly.
/// </summary>
[Collection("live-postgres")]
public sealed class ForcePlanFailuresMemoLiveTests
{
    private const int ServerId = -465900;

    private static ForcePlanFailureInfo Info(long plan) => new() { DatabaseName = "d", QueryId = 1, PlanId = plan };

    private static DateTime At(int minute) =>
        DateTime.SpecifyKind(new DateTime(2026, 1, 1, 12, 0, 0).AddMinutes(minute), DateTimeKind.Unspecified);

    [Fact]
    public void ReuseForWindow_KeepsRowsWhosePriorIsAfterTheStart_DropsEqualAndOlder_AndKeepsOrder()
    {
        var rows = new List<(ForcePlanFailureInfo Info, DateTime PriorObservedAt)>
        {
            (Info(3), At(11)),
            (Info(1), At(10)),
            (Info(2), At(9)),
            (Info(4), At(20)),
        };

        var kept = DarlingAlertReadAdapter.ReuseForWindow(rows, At(10));

        Assert.Equal(new long[] { 3, 4 }, kept.Select(k => k.PlanId).ToArray());
    }

    [Fact]
    public void ReuseForWindow_AnEmptyListStaysEmpty()
    {
        Assert.Empty(DarlingAlertReadAdapter.ReuseForWindow(
            new List<(ForcePlanFailureInfo Info, DateTime PriorObservedAt)>(), At(0)));
    }

    private const string ServerName = "memo-live";

    private static readonly MethodInfo WriteBatchMethod = typeof(DarlingCollectorRunner)
        .GetMethod("WriteBatchAsync", BindingFlags.NonPublic | BindingFlags.Instance)!;

    private static readonly ServerRuntime Server = new()
    {
        Config = new MonitoredServer { Name = ServerName, Host = ServerName },
        ConnectionString = "Server=" + ServerName,
        Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
        StorageName = ServerName,
        ServerId = ServerId,
        EngineEdition = 3,
    };

    private sealed class Rig : IAsyncDisposable
    {
        public required ScratchPostgres Scratch { get; init; }
        public required NpgsqlConnection Connection { get; init; }
        public required NpgsqlDataSource Postgres { get; init; }
        public required QueryStoreWriteFence Fence { get; init; }
        public required DarlingCollectorRunner Runner { get; init; }
        public required DarlingAlertReadAdapter Adapter { get; init; }
        public required DateTime Anchor { get; init; }

        public async ValueTask DisposeAsync()
        {
            await Postgres.DisposeAsync();
            await Connection.DisposeAsync();
            await Scratch.DisposeAsync();
        }
    }

    /// <summary>A migrated scratch store, a runner and an adapter sharing ONE fence (or, with
    /// <paramref name="fenced"/> false, an adapter with none).</summary>
    private static async Task<Rig?> OpenRigAsync(CancellationToken ct, bool fenced = true)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4659 memo live tests.");

        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        if (await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct))
        {
            await TimescaleSupport.ConvertToHypertablesAsync(connection, null, ct);
        }

        var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var fence = new QueryStoreWriteFence();
        var now = DateTime.UtcNow;
        return new Rig
        {
            Scratch = scratch,
            Connection = connection,
            Postgres = postgres,
            Fence = fence,
            Runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator(), queryStoreWriteFence: fence),
            Adapter = new DarlingAlertReadAdapter(postgres, queryStoreWriteFence: fenced ? fence : null),
            Anchor = DateTime.SpecifyKind(DarlingAlertReadAdapter.FloorToMicrosecond(now), DateTimeKind.Unspecified),
        };
    }

    private static QueryStoreCollector.Row PlanRow(string database, long plan, long failures) => new()
    {
        DatabaseName = database,
        QueryId = plan,
        PlanId = plan,
        ExecutionTypeDesc = "Regular",
        PlanForcingType = "MANUAL",
        IsForcedPlan = true,
        ForceFailureCount = failures,
        LastForceFailureReason = "GENERAL_FAILURE",
        ExecutionCount = 5,
        AvgDurationUs = 900,
        RuntimeStatsIntervalId = 1,
    };

    /// <summary>One database's batch, written through the runner's real <c>WriteBatchAsync</c> (the COPY
    /// chokepoint) on the rig's own connection.</summary>
    private static async Task WriteAsync(Rig rig, DateTime collectionTime, params QueryStoreCollector.Row[] rows)
    {
        var ct = TestContext.Current.CancellationToken;
        var context = new CollectorContext
        {
            ServerId = ServerId,
            ServerName = ServerName,
            CollectionTime = collectionTime,
            Deltas = new CollectorDeltaCalculator(),
            Target = Server.Target,
        };
        var task = (Task)WriteBatchMethod.MakeGenericMethod(typeof(QueryStoreCollector.Row)).Invoke(
            rig.Runner,
            new object?[] { rig.Connection, QueryStoreCollector.Instance, rows.ToList(), Server, collectionTime, context, ct })!;
        await task;
    }

    private static async Task<List<(string Db, long Query, long Plan, long Delta, long Total)>> ViaAdapterAsync(Rig rig)
    {
        var infos = await rig.Adapter.GetForcePlanFailuresAsync(
            ServerId.ToString(CultureInfo.InvariantCulture), TestContext.Current.CancellationToken);
        return infos.Select(f => (f.DatabaseName, f.QueryId, f.PlanId, f.FailureDelta, f.TotalFailures)).ToList();
    }

    private static async Task<List<(string Db, long Query, long Plan, long Delta, long Total)>> DirectAsync(Rig rig)
    {
        var result = new List<(string, long, long, long, long)>();
        using var command = new NpgsqlCommand(DarlingAlertReadAdapter.ForcePlanFailuresSql, rig.Connection);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(DarlingAlertReadAdapter.FloorToMicrosecond(
            DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified) - DarlingAlertReadAdapter.ForcePlanFailureWindow));
        using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        while (await reader.ReadAsync(TestContext.Current.CancellationToken))
        {
            result.Add((reader.GetString(0), reader.GetInt64(1), reader.GetInt64(2), reader.GetInt64(5), reader.GetInt64(6)));
        }

        return result;
    }

    [Fact]
    public void FloorToMicrosecond_DropsSubMicrosecondTicks_AndKeepsTheKind()
    {
        var value = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Unspecified).AddTicks(1234567);
        var floored = DarlingAlertReadAdapter.FloorToMicrosecond(value);
        Assert.Equal(1234560, floored.Ticks - new DateTime(2026, 1, 1, 12, 0, 0).Ticks);
        Assert.Equal(DateTimeKind.Unspecified, floored.Kind);

        var whole = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc).AddTicks(30);
        Assert.Equal(whole, DarlingAlertReadAdapter.FloorToMicrosecond(whole));
        Assert.Equal(DateTimeKind.Utc, DarlingAlertReadAdapter.FloorToMicrosecond(whole).Kind);
    }

    [Fact]
    public void TheFence_BeginBumpsAndFlagsInFlight_EndBumpsAndClears_OverlapsCount_ServersAreIndependent()
    {
        var fence = new QueryStoreWriteFence();
        Assert.Equal((0L, true), fence.Snapshot(1));

        fence.BeginWrite(1);
        Assert.Equal((1L, false), fence.Snapshot(1));

        fence.BeginWrite(1);
        Assert.Equal((2L, false), fence.Snapshot(1));

        fence.EndWrite(1, true);
        Assert.Equal((3L, false), fence.Snapshot(1));

        fence.EndWrite(1, true);
        Assert.Equal((4L, true), fence.Snapshot(1));

        /* An unmatched End never drives the count below zero. */
        fence.EndWrite(1, true);
        Assert.Equal((5L, true), fence.Snapshot(1));

        Assert.Equal((0L, true), fence.Snapshot(2));
    }

    [Fact]
    public async Task TheMemo_SkipsTheFullRead_UntilANewCollectionLands_AndMatchesTheDirectRead()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = (await OpenRigAsync(ct))!;
        var t0 = rig.Anchor.AddMinutes(-30);
        await WriteAsync(rig, t0, PlanRow("d1", 10, 3));
        await WriteAsync(rig, t0.AddMinutes(5), PlanRow("d1", 10, 5));

        var first = await ViaAdapterAsync(rig);
        Assert.Equal(1, rig.Adapter.ForcePlanFailuresFullReads);
        Assert.Equal(("d1", 10L, 10L, 2L, 5L), Assert.Single(first));
        Assert.Equal(await DirectAsync(rig), first);

        var second = await ViaAdapterAsync(rig);
        Assert.Equal(1, rig.Adapter.ForcePlanFailuresFullReads);
        Assert.Equal(first, second);

        /* A newer collection where the counter no longer rises: the answer changes. */
        await WriteAsync(rig, t0.AddMinutes(10), PlanRow("d1", 10, 5));
        var third = await ViaAdapterAsync(rig);
        Assert.Equal(2, rig.Adapter.ForcePlanFailuresFullReads);
        Assert.Empty(third);
        Assert.Equal(await DirectAsync(rig), third);
    }

    [Fact]
    public async Task ASecondDatabaseCommittingUnderTheSameCollectionTime_IsSeenByTheNextPass()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = (await OpenRigAsync(ct))!;
        var t = rig.Anchor.AddMinutes(-20);
        var previous = t.AddMinutes(-15);

        await WriteAsync(rig, previous, PlanRow("db1", 101, 1));
        await WriteAsync(rig, previous, PlanRow("db2", 202, 0));
        await WriteAsync(rig, t, PlanRow("db1", 101, 3));

        var pass1 = await ViaAdapterAsync(rig);
        Assert.Equal(("db1", 101L, 101L, 2L, 3L), Assert.Single(pass1));

        /* The same fan-out's next database commits under the SAME collection_time: the newest collection the probe
           reads does not move. */
        await WriteAsync(rig, t, PlanRow("db2", 202, 2));

        var pass2 = await ViaAdapterAsync(rig);
        var direct = await DirectAsync(rig);
        Assert.Equal(direct, pass2);
        Assert.Contains(pass2, r => r.Plan == 202);
        Assert.Equal(2, rig.Adapter.ForcePlanFailuresFullReads);
    }

    [Fact]
    public async Task ACommitBelowTheNewestButAboveTheOlderCollection_ChangesTheDelta_AndIsSeenByTheNextPass()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = (await OpenRigAsync(ct))!;
        var t0 = rig.Anchor.AddMinutes(-30);
        await WriteAsync(rig, t0, PlanRow("d1", 10, 3));
        await WriteAsync(rig, t0.AddMinutes(10), PlanRow("d1", 10, 5));

        var first = await ViaAdapterAsync(rig);
        Assert.Equal(2L, Assert.Single(first).Delta);

        /* A collection between the two: it becomes the older sighting (4 -> 5 is a delta of 1), and the newest
           collection does not move. */
        await WriteAsync(rig, t0.AddMinutes(5), PlanRow("d1", 10, 4));

        var next = await ViaAdapterAsync(rig);
        var direct = await DirectAsync(rig);
        Assert.Equal(1L, Assert.Single(direct).Delta);
        Assert.Equal(direct, next);
    }

    [Fact]
    public async Task AWriteLandingBetweenTheFullReadAndTheMemoStore_IsNotSaved_AndTheNextPassRereads()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = (await OpenRigAsync(ct))!;
        var t = rig.Anchor.AddMinutes(-20);
        await WriteAsync(rig, t.AddMinutes(-15), PlanRow("db1", 101, 1), PlanRow("db1", 102, 0));
        await WriteAsync(rig, t, PlanRow("db1", 101, 3), PlanRow("db1", 102, 0));

        var wrote = false;
        rig.Adapter.BeforeMemoStoreForTests = async () =>
        {
            if (!wrote)
            {
                wrote = true;
                await WriteAsync(rig, t, PlanRow("db1", 102, 4));
            }
        };

        var pass1 = await ViaAdapterAsync(rig);
        Assert.True(wrote);
        Assert.Equal(1, rig.Adapter.ForcePlanFailuresFullReads);
        Assert.DoesNotContain(pass1, r => r.Plan == 102);

        var pass2 = await ViaAdapterAsync(rig);
        Assert.Equal(2, rig.Adapter.ForcePlanFailuresFullReads);
        Assert.Equal(await DirectAsync(rig), pass2);
        Assert.Contains(pass2, r => r.Plan == 102);

        /* Now nothing races: the answer is saved and reused. */
        var pass3 = await ViaAdapterAsync(rig);
        Assert.Equal(2, rig.Adapter.ForcePlanFailuresFullReads);
        Assert.Equal(pass2, pass3);
    }

    [Fact]
    public async Task AWriteThatThrows_StillEndsTheFence()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = (await OpenRigAsync(ct))!;
        var bad = PlanRow("db1", 101, 1);
        /* Postgres text cannot hold a NUL, so the COPY faults inside the fenced region. */
        bad.DatabaseName = "db\0x";

        await Assert.ThrowsAnyAsync<Exception>(() => WriteAsync(rig, rig.Anchor.AddMinutes(-20), bad));

        var (generation, quiet) = rig.Fence.Snapshot(ServerId);
        Assert.False(quiet, "a write that exited by exception may have landed, so the server stays non-quiet");
        Assert.True(generation >= 2L, "the failed batch still began and ended a write");
    }

    [Fact]
    public async Task AfterAFailedWrite_PassesReadInFull_AndStoreNoMemo_UntilACleanWriteLands()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = (await OpenRigAsync(ct))!;
        var t0 = rig.Anchor.AddMinutes(-30);
        await WriteAsync(rig, t0, PlanRow("d1", 10, 3));
        await WriteAsync(rig, t0.AddMinutes(5), PlanRow("d1", 10, 5));

        /* A commit whose acknowledgement never arrived. */
        rig.Fence.BeginWrite(ServerId);
        rig.Fence.EndWrite(ServerId, false);

        var first = await ViaAdapterAsync(rig);
        var second = await ViaAdapterAsync(rig);
        Assert.Equal(2, rig.Adapter.ForcePlanFailuresFullReads);
        Assert.Null(rig.Adapter.MemoGenerationForTests(ServerId));
        Assert.Equal(await DirectAsync(rig), first);
        Assert.Equal(first, second);

        /* A clean write proves the table state is known again: the next pass memoises, the one after reuses. */
        await WriteAsync(rig, t0.AddMinutes(10), PlanRow("d1", 10, 6));
        await ViaAdapterAsync(rig);
        Assert.Equal(3, rig.Adapter.ForcePlanFailuresFullReads);
        Assert.NotNull(rig.Adapter.MemoGenerationForTests(ServerId));
        await ViaAdapterAsync(rig);
        Assert.Equal(3, rig.Adapter.ForcePlanFailuresFullReads);
    }

    [Fact]
    public void AMemoStore_WithALowerGeneration_DoesNotOverwriteANewerMemo()
    {
        var adapter = new DarlingAlertReadAdapter(Npgsql.NpgsqlDataSource.Create("Host=127.0.0.1;Database=none"), queryStoreWriteFence: new QueryStoreWriteFence());
        var now = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);
        adapter.StoreMemoForTests(ServerId, 7, now);
        adapter.StoreMemoForTests(ServerId, 5, now);
        Assert.Equal(7L, adapter.MemoGenerationForTests(ServerId));
        adapter.StoreMemoForTests(ServerId, 9, now);
        Assert.Equal(9L, adapter.MemoGenerationForTests(ServerId));
    }

    [Fact]
    public async Task WithoutAFence_EveryPassRunsTheFullRead()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = (await OpenRigAsync(ct, fenced: false))!;
        var t0 = rig.Anchor.AddMinutes(-30);
        await WriteAsync(rig, t0, PlanRow("d1", 10, 3));
        await WriteAsync(rig, t0.AddMinutes(5), PlanRow("d1", 10, 5));

        await ViaAdapterAsync(rig);
        await ViaAdapterAsync(rig);
        await ViaAdapterAsync(rig);

        Assert.Equal(3, rig.Adapter.ForcePlanFailuresFullReads);
    }

    [Theory]
    [InlineData(4659)]
    [InlineData(20260928)]
    [InlineData(7)]
    public async Task AfterEveryPass_TheAnswerEqualsTheDirectRead_UnderRandomWrites(int seed)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var rig = (await OpenRigAsync(ct))!;
        var random = new Random(seed);
        var newest = rig.Anchor.AddMinutes(-60);
        var passes = 0;

        for (var step = 0; step < 40; step++)
        {
            if (random.Next(3) == 0)
            {
                passes++;
                var expected = await DirectAsync(rig);
                var actual = await ViaAdapterAsync(rig);
                Assert.True(
                    expected.SequenceEqual(actual),
                    $"seed {seed}, step {step}: the adapter's answer differs from the direct read " +
                    $"({actual.Count} vs {expected.Count} rows)");
                continue;
            }

            var database = random.Next(2) == 0 ? "db1" : "db2";
            var plan = random.Next(1, 4);
            var failures = random.Next(0, 7);
            DateTime when;
            if (random.Next(2) == 0)
            {
                newest = newest.AddMinutes(1);
                when = newest;
            }
            else
            {
                /* Inside the window, at or below the newest collection, at microsecond resolution. */
                var span = (newest - rig.Anchor.AddMinutes(-100)).Ticks / 10;
                when = rig.Anchor.AddMinutes(-100).AddTicks(random.NextInt64(span) * 10);
            }

            await WriteAsync(rig, when, PlanRow(database, plan, failures));
        }

        Assert.True(passes > 5, $"seed {seed}: only {passes} passes ran");
    }
}
