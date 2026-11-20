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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9149: <c>analyze_server</c> ran about 146 statements one after another. The family reads of an on-demand pass now run
/// at once, at most four at a time, and the facts and failures land in family order, whatever order the reads finish in.
/// </summary>
public sealed class AnalyzeServerFamilyOverlapTests
{
    private static Func<AnalysisContext, List<Fact>, Task> Family(string key, Func<AnalysisContext, Task>? body = null) =>
        async (context, facts) =>
        {
            if (body is not null)
                await body(context);
            facts.Add(new Fact { Source = "test", Key = key });
        };

    /// <summary>A pass that asks for more than the ceiling overlaps its reads, holds at the ceiling, and keeps the family order of the facts.</summary>
    [Fact]
    public async Task FamilyReads_OverlapUpToTheCeiling_AndTheFactsKeepFamilyOrder()
    {
        var running = 0;
        var peak = 0;
        var ceilingReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        async Task Hold(AnalysisContext _)
        {
            var now = Interlocked.Increment(ref running);
            int seen;
            do { seen = Volatile.Read(ref peak); } while (now > seen && Interlocked.CompareExchange(ref peak, now, seen) != seen);
            if (now >= PgFactCollector.MaxFamilyParallelism)
                ceilingReached.TrySetResult();

            /* No read finishes until the ceiling's worth are running at once, so a serial loop never gets past the first one. */
            await ceilingReached.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

            /* The later families finish sooner, so a merge in completion order would reverse the facts. */
            await Task.Delay(1, TestContext.Current.CancellationToken);
            Interlocked.Decrement(ref running);
        }

        var keys = Enumerable.Range(0, 9).Select(i => $"F{i}").ToArray();
        var families = keys.Select(k => Family(k, Hold)).ToArray();
        var context = new AnalysisContext { FamilyParallelism = 16 };
        var facts = new List<Fact> { new() { Source = "test", Key = "coverage" } };

        await PgFactCollector.RunFamiliesAsync(context, facts, families);

        Assert.Equal(PgFactCollector.MaxFamilyParallelism, peak);
        Assert.Equal(new[] { "coverage" }.Concat(keys), facts.Select(f => f.Key));
    }

    /// <summary>The default pass is the serial loop: one read at a time, in order, straight into the caller's list.</summary>
    [Fact]
    public async Task FamilyReads_RunOneAtATime_WhenTheContextKeepsTheDefault()
    {
        var running = 0;
        var peak = 0;

        async Task Hold(AnalysisContext _)
        {
            peak = Math.Max(peak, Interlocked.Increment(ref running));
            await Task.Delay(2, TestContext.Current.CancellationToken);
            Interlocked.Decrement(ref running);
        }

        var families = Enumerable.Range(0, 5).Select(i => Family($"F{i}", Hold)).ToArray();
        var context = new AnalysisContext();
        var facts = new List<Fact>();

        await PgFactCollector.RunFamiliesAsync(context, facts, families);

        Assert.Equal(1, peak);
        Assert.Equal(new[] { "F0", "F1", "F2", "F3", "F4" }, facts.Select(f => f.Key));
    }

    /// <summary>
    /// Failures recorded by families that finish out of order still read in family order, and a failure recorded on the
    /// pass's own flow afterwards lands in the context's list, not in a family's buffer.
    /// </summary>
    [Fact]
    public async Task RecordedFailures_ReadInFamilyOrder_WhateverOrderTheyLand()
    {
        Func<AnalysisContext, List<Fact>, Task> Failing(string family, int delayMs) =>
            async (context, facts) =>
            {
                await Task.Delay(delayMs, TestContext.Current.CancellationToken);
                context.RecordCollectionFailure(family, "read", CollectionFailureOutcome.Timeout, new TimeoutException());
            };

        var context = new AnalysisContext { FamilyParallelism = 4 };
        var facts = new List<Fact>();

        await PgFactCollector.RunFamiliesAsync(context, facts,
        [
            Failing("first", 150),
            Failing("second", 100),
            Failing("third", 50),
            Failing("fourth", 1)
        ]);
        context.RecordCollectionFailure("after", "read", CollectionFailureOutcome.Error, new InvalidOperationException());

        Assert.Equal(new[] { "first", "second", "third", "fourth", "after" }, context.CollectionFailures.Select(f => f.Family));
    }

    /// <summary>
    /// A family that throws ends the pass as the serial loop would: the earliest failing family's exception comes out, the
    /// families before it keep their failures, and no family that had not started yet starts.
    /// </summary>
    [Fact]
    public async Task AFamilyThatThrows_EndsThePass_WithTheEarliestFailure_AndStartsNothingNew()
    {
        var started = new List<int>();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        Func<AnalysisContext, List<Fact>, Task> Step(int index, bool throws) =>
            async (context, facts) =>
            {
                lock (started) started.Add(index);
                if (throws)
                {
                    await Task.Delay(1, TestContext.Current.CancellationToken);
                    release.TrySetResult();
                    throw new OperationCanceledException($"stopped {index}");
                }

                if (index == 0)
                    context.RecordCollectionFailure("zero", "read", CollectionFailureOutcome.Error, new InvalidOperationException());

                await release.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

                /* Outlast the thrower's own unwind, so no gate slot frees before the pass has noted the failure. */
                await Task.Delay(200, TestContext.Current.CancellationToken);
                facts.Add(new Fact { Source = "test", Key = $"F{index}" });
            };

        /* Four families fill the gate; family 2 throws while 0, 1 and 3 wait on it; 4 to 7 have not started. */
        var families = Enumerable.Range(0, 8).Select(i => Step(i, throws: i == 2)).ToArray();
        var context = new AnalysisContext { FamilyParallelism = 4 };
        var facts = new List<Fact>();

        var thrown = await Assert.ThrowsAsync<OperationCanceledException>(
            () => PgFactCollector.RunFamiliesAsync(context, facts, families));

        Assert.Equal("stopped 2", thrown.Message);
        Assert.Equal(new[] { 0, 1, 2, 3 }, started.OrderBy(i => i));
        Assert.Equal(new[] { "F0", "F1" }, facts.Select(f => f.Key));
        Assert.Equal(new[] { "zero" }, context.CollectionFailures.Select(f => f.Family));
    }
}

/// <summary>
/// #9149: against a real store, the parallel pass answers exactly what the serial pass answers, facts in the same order.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalyzeServerFamilyOverlapLiveTests
{
    private const int ServerId = -914901;
    private const string ServerName = "family-overlap-e2e";

    [Fact]
    public async Task TheParallelPass_ReturnsTheFactsOfTheSerialPass_InTheSameOrder()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live family-overlap test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var collector = new PgFactCollector(postgres);

        var bodySucceeded = false;
        try
        {
            var now = DateTime.UtcNow;
            var windowEnd = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
            var windowStart = windowEnd.AddHours(-1);

            /* Several families get rows: four waits (a lock wait and a CX wait that the grouping helpers fold, and two plain ones)
               and two cpu samples, so a merge in the wrong order, or grouping at the wrong point, would show. */
            foreach (var waitType in new[] { "LCK_M_X", "CXPACKET", "SOS_SCHEDULER_YIELD", "PAGEIOLATCH_SH" })
            {
                foreach (var (time, tasks, waitMs) in new[] { (windowStart, 0L, 0L), (windowEnd, 3000L, 900_000L) })
                {
                    using var plant = new NpgsqlCommand(@"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
                    plant.Parameters.AddWithValue(CollectionIdGenerator.Next());
                    plant.Parameters.AddWithValue(time);
                    plant.Parameters.AddWithValue(ServerId);
                    plant.Parameters.AddWithValue(ServerName);
                    plant.Parameters.AddWithValue(waitType);
                    plant.Parameters.AddWithValue(tasks);
                    plant.Parameters.AddWithValue(waitMs);
                    plant.Parameters.AddWithValue(waitMs / 9);
                    await plant.ExecuteNonQueryAsync(ct);
                }
            }

            foreach (var cpuPercent in new[] { 80, 90 })
            {
                using var plant = new NpgsqlCommand(@"
INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sqlserver_cpu_utilization, other_process_cpu_utilization)
VALUES ($1, $2, $3, $4, $5, $6)", connection);
                plant.Parameters.AddWithValue(CollectionIdGenerator.Next());
                plant.Parameters.AddWithValue(windowStart.AddMinutes(30));
                plant.Parameters.AddWithValue(ServerId);
                plant.Parameters.AddWithValue(ServerName);
                plant.Parameters.AddWithValue(cpuPercent);
                plant.Parameters.AddWithValue(5);
                await plant.ExecuteNonQueryAsync(ct);
            }

            AnalysisContext NewContext(int parallelism) => new()
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = windowStart,
                TimeRangeEnd = windowEnd,
                ServerUtcOffset = TimeSpan.Zero,
                FamilyParallelism = parallelism
            };

            var serialContext = NewContext(1);
            var serial = await collector.CollectFactsAsync(serialContext);
            var parallelContext = NewContext(4);
            var parallel = await collector.CollectFactsAsync(parallelContext);

            Assert.True(serial.Count >= 4, $"the fixture should give the passes several facts to compare, got {serial.Count}");
            Assert.Equal(JsonSerializer.Serialize(serial), JsonSerializer.Serialize(parallel));
            Assert.Equal(
                serialContext.CollectionFailures.Select(f => (f.Family, f.Outcome)),
                parallelContext.CollectionFailures.Select(f => (f.Family, f.Outcome)));
            Assert.Equal(serialContext.ObservedDurationMs, parallelContext.ObservedDurationMs);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private const int SwitchServerId = -914902;

    /// <summary>
    /// Stands in for the SQL Server fact collector and runs a handful of timed reads through the real family runner, with the
    /// context the service built. It reports how many of them ran at once, which is what the service's choice of
    /// <see cref="AnalysisContext.FamilyParallelism"/> decides.
    /// </summary>
    private sealed class OverlapProbeCollector : IFactCollector
    {
        public int Peak;

        public async Task<List<Fact>> CollectFactsAsync(AnalysisContext context)
        {
            var running = 0;
            var ceilingReached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            async Task Hold(AnalysisContext _)
            {
                var now = Interlocked.Increment(ref running);
                int seen;
                do { seen = Volatile.Read(ref Peak); } while (now > seen && Interlocked.CompareExchange(ref Peak, now, seen) != seen);
                if (now >= PgFactCollector.MaxFamilyParallelism)
                    ceilingReached.TrySetResult();

                /* A pass that overlaps gets the ceiling's worth running at once and moves on. A serial pass waits out the short
                   delay for each read, so the probe stays quick. */
                await Task.WhenAny(ceilingReached.Task, Task.Delay(150, context.CancellationToken));
                Interlocked.Decrement(ref running);
            }

            var families = Enumerable.Range(0, 6).Select(_ => (Func<AnalysisContext, List<Fact>, Task>)((c, _) => Hold(c))).ToArray();
            var facts = new List<Fact>();
            await PgFactCollector.RunFamiliesAsync(context, facts, families);
            return facts;
        }
    }

    /// <summary>
    /// The service decides which passes overlap their family reads: the on-demand reads (<c>analyze_server</c> and
    /// <c>get_analysis_facts</c>) run them at once, the scheduled pass keeps them in series.
    /// </summary>
    [Fact]
    public async Task OnDemandPasses_OverlapTheirFamilyReads_AndTheScheduledPassStaysSerial()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live family-overlap switch test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteSwitchRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        var bodySucceeded = false;
        try
        {
            /* Two wait rows give the server some history, so the pass reaches its collector. */
            var end = DateTime.UtcNow;
            foreach (var time in new[] { end.AddHours(-2), end.AddMinutes(-5) })
            {
                using var plant = new NpgsqlCommand(@"
INSERT INTO wait_stats (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, 'SOS_SCHEDULER_YIELD', 1, 1, 0)", connection);
                plant.Parameters.AddWithValue(CollectionIdGenerator.Next());
                plant.Parameters.AddWithValue(time);
                plant.Parameters.AddWithValue(SwitchServerId);
                plant.Parameters.AddWithValue("family-overlap-switch");
                await plant.ExecuteNonQueryAsync(ct);
            }

            async Task<int> PeakOfAsync(Func<DarlingAnalysisService, Task> run)
            {
                var probe = new OverlapProbeCollector();
                var service = new DarlingAnalysisService(postgres, sqlServerCollectorWrapper: _ => probe) { MinimumDataHours = 0 };
                await run(service);
                return probe.Peak;
            }

            var onDemand = await PeakOfAsync(s => s.AnalyzeAsync(SwitchServerId, "family-overlap-switch", 4, ct, persistCollectionCaveats: false));
            var scheduled = await PeakOfAsync(s => s.AnalyzeAsync(SwitchServerId, "family-overlap-switch", 4, ct, persistCollectionCaveats: true));
            var facts = await PeakOfAsync(s => s.CollectAndScoreFactsAsync(SwitchServerId, "family-overlap-switch", 4, null, ct));

            Assert.Equal(PgFactCollector.MaxFamilyParallelism, onDemand);
            Assert.Equal(1, scheduled);
            Assert.Equal(PgFactCollector.MaxFamilyParallelism, facts);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteSwitchRowsAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteSwitchRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {SwitchServerId}; DELETE FROM collect.analysis_collection_caveats WHERE server_id = {SwitchServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; DELETE FROM cpu_utilization_stats WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
