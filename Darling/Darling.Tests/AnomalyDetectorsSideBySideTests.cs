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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9879: an on-demand pass runs the anomaly detector's detectors side by side, at most
/// <see cref="PgAnomalyDetector.MaxDetectorParallelism"/> at once, and reports their facts and their failure lines
/// in detector order. These tests drive <see cref="PgAnomalyDetector.RunDetectorsAsync"/> with planted detectors,
/// so no store is read. Every wait is a held hook: a detector waits for a later one, which only a side by side run
/// can satisfy, and <see cref="Limit"/> only bounds a run that is not side by side.
/// </summary>
public sealed class AnomalyDetectorsSideBySideTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    private static TaskCompletionSource Hook() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    /// <summary>A detector over a data source that is never opened: the planted detectors read no store.</summary>
    private static PgAnomalyDetector Detector(NpgsqlDataSource postgres, ILogger? logger = null) =>
        new(postgres, new PgBaselineProvider(postgres), logger);

    private static NpgsqlDataSource NeverOpened() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Database=nothing");

    private static string Shape(Fact fact) =>
        string.Join("|", fact.Source, fact.Key, fact.Value.ToString("R", CultureInfo.InvariantCulture), fact.DatabaseName, fact.ObjectName,
            string.Join(",", fact.Metadata.OrderBy(p => p.Key, StringComparer.Ordinal)
                .Select(p => p.Key + "=" + p.Value.ToString("R", CultureInfo.InvariantCulture))));

    internal static List<string> Shapes(IEnumerable<Fact> facts) => facts.Select(Shape).ToList();

    /// <summary>
    /// Six detectors that add two facts each. With <paramref name="held"/>, each of the first three waits for the
    /// detector after it to finish, so the first four finish in reverse order.
    /// </summary>
    private static Func<AnalysisContext, List<Fact>, Task>[] SixDetectors(bool held)
    {
        var finished = Enumerable.Range(0, 6).Select(_ => Hook()).ToArray();
        return Enumerable.Range(0, 6).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (_, facts) =>
        {
            if (held && i < 3)
                await finished[i + 1].Task.WaitAsync(Limit);
            facts.Add(new Fact { Source = "anomaly", Key = "PLANTED_" + i, Value = i + 0.5, Metadata = { ["first"] = i } });
            facts.Add(new Fact { Source = "anomaly", Key = "PLANTED_" + i + "_B", Value = i * 10, DatabaseName = "db" + i });
            finished[i].TrySetResult();
        })).ToArray();
    }

    [Fact]
    public async Task TheFacts_AreTheSameInOrderAndContent_OneAtATimeAndFourAtOnce()
    {
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);

        var serial = new List<Fact>();
        await detector.RunDetectorsAsync(new AnalysisContext(), serial, SixDetectors(held: false), 1);

        var sideBySide = new List<Fact>();
        await detector.RunDetectorsAsync(new AnalysisContext(), sideBySide, SixDetectors(held: true), 4);

        Assert.Equal(12, serial.Count);
        Assert.Equal(Shapes(serial), Shapes(sideBySide));
    }

    [Fact]
    public async Task AtMostFourDetectors_RunAtOnce()
    {
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);
        var fourIn = Hook();
        var release = Hook();
        var sync = new Lock();
        int running = 0, started = 0, most = 0;

        var detectors = Enumerable.Range(0, 9).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (_, facts) =>
        {
            lock (sync)
            {
                most = Math.Max(most, ++running);
                if (++started == PgAnomalyDetector.MaxDetectorParallelism)
                    fourIn.TrySetResult();
            }

            await release.Task.WaitAsync(Limit);
            facts.Add(new Fact { Source = "anomaly", Key = "PLANTED_" + i });
            lock (sync)
                running--;
        })).ToArray();

        var anomalies = new List<Fact>();
        var run = detector.RunDetectorsAsync(new AnalysisContext(), anomalies, detectors, PgAnomalyDetector.MaxDetectorParallelism);

        await fourIn.Task.WaitAsync(Limit);
        /* Four hold every slot and none has been released, so the fifth cannot have started. */
        lock (sync)
        {
            Assert.Equal(4, running);
            Assert.Equal(4, started);
        }

        release.TrySetResult();
        await run.WaitAsync(Limit);

        Assert.Equal(4, most);
        Assert.Equal(Enumerable.Range(0, 9).Select(i => "PLANTED_" + i), anomalies.Select(f => f.Key));
    }

    [Fact]
    public async Task AFailingDetectorsLines_ComeOutInDetectorOrder_AndTheOthersStillAddFacts()
    {
        await using var postgres = NeverOpened();
        var logger = new CapturingTestLogger();
        var detector = Detector(postgres, logger);
        var thirdFailed = Hook();

        /* A planted detector fails the way the real ones do: it catches its own read failure and logs it. */
        void Fail(string name)
        {
            try
            {
                throw new InvalidOperationException("planted read failure");
            }
            catch (InvalidOperationException ex)
            {
                detector.Logger?.LogError("[PgAnomalyDetector] {Name} anomaly detection failed: {Message}", name, ex.Message);
            }
        }

        Func<AnalysisContext, List<Fact>, Task>[] detectors =
        [
            async (_, _) =>
            {
                /* The first detector fails only after the third has, so it logs second. */
                await thirdFailed.Task.WaitAsync(Limit);
                Fail("First");
            },
            (_, facts) =>
            {
                facts.Add(new Fact { Source = "anomaly", Key = "SECOND" });
                return Task.CompletedTask;
            },
            (_, _) =>
            {
                Fail("Third");
                thirdFailed.TrySetResult();
                return Task.CompletedTask;
            },
            (_, facts) =>
            {
                facts.Add(new Fact { Source = "anomaly", Key = "FOURTH" });
                return Task.CompletedTask;
            }
        ];

        var anomalies = new List<Fact>();
        await detector.RunDetectorsAsync(new AnalysisContext(), anomalies, detectors, 4);

        Assert.Equal(new[] { "SECOND", "FOURTH" }, anomalies.Select(f => f.Key));
        var lines = logger.Lines;
        Assert.Equal(2, lines.Count);
        Assert.Contains("First anomaly detection failed", lines[0], StringComparison.Ordinal);
        Assert.Contains("Third anomaly detection failed", lines[1], StringComparison.Ordinal);
        Assert.Equal(2, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public async Task ACancelledPassToken_StopsTheDetectors_AndIsRethrown()
    {
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);
        using var pass = new CancellationTokenSource();
        var context = new AnalysisContext { CancellationToken = pass.Token };
        var fourWaiting = Hook();
        int waiting = 0, started = 0;

        /* The first detector finishes with a fact. The next four wait the way a store read does, until the pass's token
           ends them. The last four are never started. */
        var detectors = Enumerable.Range(0, 9).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (ctx, facts) =>
        {
            Interlocked.Increment(ref started);
            if (i == 0)
            {
                facts.Add(new Fact { Source = "anomaly", Key = "FIRST" });
                return;
            }

            if (Interlocked.Increment(ref waiting) == 4)
                fourWaiting.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ctx.CancellationToken);
            facts.Add(new Fact { Source = "anomaly", Key = "NEVER_" + i });
        })).ToArray();

        var anomalies = new List<Fact>();
        var run = detector.RunDetectorsAsync(context, anomalies, detectors, 4);

        await fourWaiting.Task.WaitAsync(Limit);
        pass.Cancel();

        var stop = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run);
        Assert.Equal(pass.Token, stop.CancellationToken);
        Assert.Equal(5, Volatile.Read(ref started));
        Assert.Equal(new[] { "FIRST" }, anomalies.Select(f => f.Key));
    }

    private static TaskCompletionSource[] Hooks(int count) => Enumerable.Range(0, count).Select(_ => Hook()).ToArray();

    /// <summary>An on-demand pass over an observed window: the families and the detectors both overlap.</summary>
    private static AnalysisContext OnDemand() => new()
    {
        FamilyParallelism = PgFactCollector.MaxFamilyParallelism,
        DetectorParallelism = PgAnomalyDetector.MaxDetectorParallelism,
        Coverage = new WindowCoverage { NominalMs = 3_600_000, ObservedMs = 3_600_000, SampleCount = 4 }
    };

    /// <summary>
    /// Beside the family reads the detectors take their slots from the families' gate. Planted families and planted
    /// detectors each count one held slot while they run, through the real family stage and the real detector dispatch.
    /// The first detector starts on the anomaly read's slot while every family is still held, each further detector
    /// starts beside it only once a family has finished, and no more than
    /// <see cref="PgFactCollector.MaxFamilyParallelism"/> slots are ever held at once.
    /// </summary>
    [Fact]
    public async Task BesideTheFamilies_TheDetectorsDrawFromTheFamiliesGate()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);

        /* With the anomaly read, three families hold every slot of the gate and none is left waiting for one. */
        const int Families = PgFactCollector.MaxFamilyParallelism - 1;
        const int Detectors = 6;
        var sync = new Lock();
        int held = 0, peak = 0, familiesHeld = 0, familiesFinished = 0, detectorsStarted = 0;
        var familiesHeldAtStart = new int[Detectors];
        var familiesFinishedAtStart = new int[Detectors];
        var familyRelease = Hooks(Families);
        var detectorStarted = Hooks(Detectors);
        var detectorRelease = Hooks(Detectors);
        var storeRead = Hook();
        var dispatched = Hook();

        var families = Enumerable.Range(0, Families).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (_, facts) =>
        {
            lock (sync)
            {
                peak = Math.Max(peak, ++held);
                familiesHeld++;
            }

            await familyRelease[i].Task.WaitAsync(Limit, ct);
            facts.Add(new Fact { Source = "test", Key = "F" + i });
            lock (sync)
            {
                held--;
                familiesHeld--;
                familiesFinished++;
            }
        })).ToArray();

        var detectors = Enumerable.Range(0, Detectors).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (_, facts) =>
        {
            lock (sync)
            {
                peak = Math.Max(peak, ++held);
                detectorsStarted++;
                familiesHeldAtStart[i] = familiesHeld;
                familiesFinishedAtStart[i] = familiesFinished;
            }

            detectorStarted[i].TrySetResult();
            await detectorRelease[i].Task.WaitAsync(Limit, ct);
            facts.Add(new Fact { Source = "anomaly", Key = "D" + i });
            lock (sync)
                held--;
        })).ToArray();

        var context = OnDemand();
        var beside = new AnomalyReadBesideFamilies(async c =>
        {
            /* The real read asks the store whether a baseline exists before it starts a detector. The hook stands for
               that read: the families have taken their slots by the time the detectors ask for theirs. */
            await storeRead.Task.WaitAsync(Limit, ct);
            var anomalies = new List<Fact>();
            var run = detector.RunDetectorsAsync(c, anomalies, detectors);
            dispatched.TrySetResult();
            await run;
            return anomalies;
        });
        context.AnomalyReadBesideFamilies = beside;
        var facts = new List<Fact>();

        var stage = PgFactCollector.RunFamiliesAsync(context, facts, families);

        /* The stage came back at its first wait: the anomaly read holds one slot and the families the other three. */
        lock (sync)
        {
            Assert.Equal(Families, familiesHeld);
            Assert.Equal(0, detectorsStarted);
        }

        storeRead.SetResult();
        await dispatched.Task.WaitAsync(Limit, ct);

        /* The dispatch has come back, so every detector that starts without waiting for a slot has started. That is the
           first one alone, on the anomaly read's slot, while every family is still held. */
        lock (sync)
        {
            Assert.Equal(1, detectorsStarted);
            Assert.Equal(Families, familiesHeldAtStart[0]);
            Assert.Equal(0, familiesFinishedAtStart[0]);
            Assert.Equal(PgFactCollector.MaxFamilyParallelism, held);
        }

        /* Each family that finishes gives up one slot, and one more detector starts beside the ones still held. */
        for (var f = 0; f < Families; f++)
        {
            familyRelease[f].SetResult();
            await detectorStarted[f + 1].Task.WaitAsync(Limit, ct);
            lock (sync)
            {
                Assert.Equal(f + 2, detectorsStarted);
                Assert.Equal(f + 1, familiesFinishedAtStart[f + 1]);
                Assert.Equal(Families - f - 1, familiesHeldAtStart[f + 1]);
                Assert.Equal(PgFactCollector.MaxFamilyParallelism, held);
            }
        }

        /* Four detectors hold the four slots and the families are finished. The stage waits for the anomaly read, and the
           fifth detector starts only when one of the four ends, on the slot that one's worker keeps. */
        Assert.False(stage.IsCompleted);
        detectorRelease[0].SetResult();
        await detectorStarted[4].Task.WaitAsync(Limit, ct);
        lock (sync)
        {
            Assert.Equal(5, detectorsStarted);
            Assert.Equal(PgFactCollector.MaxFamilyParallelism, held);
        }

        for (var d = 1; d < Detectors; d++)
            detectorRelease[d].SetResult();
        await stage.WaitAsync(Limit, ct);

        Assert.Equal(PgFactCollector.MaxFamilyParallelism, peak);
        Assert.Equal(Enumerable.Range(0, Families).Select(i => "F" + i), facts.Select(f => f.Key));
        Assert.Equal(Enumerable.Range(0, Detectors).Select(i => "D" + i), (await beside.Run!).Select(f => f.Key));
        Assert.Null(beside.FamilyGate);
    }

    /// <summary>
    /// Beside the family reads, a worker that is still waiting for a slot when the last detector is taken stops waiting.
    /// So the anomaly read ends as soon as its detectors have, while every family still holds its slot, and the slot it
    /// gives back goes to a family that was waiting for one.
    /// </summary>
    [Fact]
    public async Task BesideTheFamilies_TheReadEndsWithItsDetectors_AndGivesItsSlotToAWaitingFamily()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);

        /* Three families run beside the anomaly read and the fourth waits for a slot. */
        const int Families = PgFactCollector.MaxFamilyParallelism;
        var familyStarted = Hooks(Families);
        var releaseFamilies = Hook();
        var detectorStarted = Hooks(2);
        var detectorRelease = Hooks(2);
        var storeRead = Hook();

        var families = Enumerable.Range(0, Families).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (_, facts) =>
        {
            familyStarted[i].TrySetResult();
            await releaseFamilies.Task.WaitAsync(Limit, ct);
            facts.Add(new Fact { Source = "test", Key = "F" + i });
        })).ToArray();

        var detectors = Enumerable.Range(0, 2).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (_, facts) =>
        {
            detectorStarted[i].TrySetResult();
            await detectorRelease[i].Task.WaitAsync(Limit, ct);
            facts.Add(new Fact { Source = "anomaly", Key = "D" + i });
        })).ToArray();

        var context = OnDemand();
        var beside = new AnomalyReadBesideFamilies(async c =>
        {
            await storeRead.Task.WaitAsync(Limit, ct);
            var anomalies = new List<Fact>();
            await detector.RunDetectorsAsync(c, anomalies, detectors);
            return anomalies;
        });
        context.AnomalyReadBesideFamilies = beside;
        var facts = new List<Fact>();

        var stage = PgFactCollector.RunFamiliesAsync(context, facts, families);
        Assert.True(familyStarted[Families - 2].Task.IsCompleted);
        Assert.False(familyStarted[Families - 1].Task.IsCompleted);

        /* Both detectors run on the anomaly read's slot, one after the other: no family has given up a slot. */
        storeRead.SetResult();
        await detectorStarted[0].Task.WaitAsync(Limit, ct);
        detectorRelease[0].SetResult();
        await detectorStarted[1].Task.WaitAsync(Limit, ct);
        Assert.False(familyStarted[Families - 1].Task.IsCompleted);
        detectorRelease[1].SetResult();

        /* No detector is left, so the read ends here, with three families still held. */
        Assert.Equal(new[] { "D0", "D1" }, (await beside.Run!.WaitAsync(Limit, ct)).Select(f => f.Key));
        await familyStarted[Families - 1].Task.WaitAsync(Limit, ct);
        Assert.False(stage.IsCompleted);

        releaseFamilies.SetResult();
        await stage.WaitAsync(Limit, ct);
        Assert.Equal(Enumerable.Range(0, Families).Select(i => "F" + i), facts.Select(f => f.Key));
    }

    /// <summary>
    /// Beside the family reads, a stop from the pass's token ends the detector that is running and the workers still
    /// waiting for a slot. The anomaly read rethrows the stop while every family still holds its slot, no further
    /// detector starts, and the family stage still finishes and leaves the stop in the read's task for the pass.
    /// </summary>
    [Fact]
    public async Task BesideTheFamilies_ACancelledPassToken_EndsTheWaitingWorkers_AndTheReadRethrowsIt()
    {
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);
        using var pass = new CancellationTokenSource();
        const int Families = PgFactCollector.MaxFamilyParallelism - 1;
        var releaseFamilies = Hook();
        var firstDetectorWaiting = Hook();
        var storeRead = Hook();
        var started = 0;

        var families = Enumerable.Range(0, Families).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (_, facts) =>
        {
            await releaseFamilies.Task.WaitAsync(Limit, ct);
            facts.Add(new Fact { Source = "test", Key = "F" + i });
        })).ToArray();

        /* The first detector waits the way a store read does, until the pass's token ends it. The other three wait for a
           slot no family gives up, so they are never started. */
        var detectors = Enumerable.Range(0, 4).Select(i => (Func<AnalysisContext, List<Fact>, Task>)(async (c, facts) =>
        {
            Interlocked.Increment(ref started);
            firstDetectorWaiting.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, c.CancellationToken);
            facts.Add(new Fact { Source = "anomaly", Key = "NEVER_" + i });
        })).ToArray();

        var context = OnDemand();
        context.CancellationToken = pass.Token;
        var beside = new AnomalyReadBesideFamilies(async c =>
        {
            await storeRead.Task.WaitAsync(Limit, ct);
            var anomalies = new List<Fact>();
            await detector.RunDetectorsAsync(c, anomalies, detectors);
            return anomalies;
        });
        context.AnomalyReadBesideFamilies = beside;
        var facts = new List<Fact>();

        var stage = PgFactCollector.RunFamiliesAsync(context, facts, families);
        storeRead.SetResult();
        await firstDetectorWaiting.Task.WaitAsync(Limit, ct);
        pass.Cancel();

        var stop = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => beside.Run!.WaitAsync(Limit, ct));
        Assert.Equal(pass.Token, stop.CancellationToken);
        Assert.Equal(1, Volatile.Read(ref started));
        Assert.False(stage.IsCompleted);

        releaseFamilies.SetResult();
        await stage.WaitAsync(Limit, ct);
        Assert.Equal(Enumerable.Range(0, Families).Select(i => "F" + i), facts.Select(f => f.Key));
    }
}

/// <summary>
/// #9879, against a live store: the real detectors over a planted CPU history and a planted table growth return the
/// same facts, in the same order, run one after another and four at once.
/// </summary>
[Collection("live-postgres")]
public sealed class AnomalyDetectorsSideBySideLiveTests
{
    private const int ServerId = -987913;
    private const string ServerName = "side-by-side-9879";

    [Fact]
    public async Task TheRealDetectors_ReturnTheSameFacts_OneAtATimeAndFourAtOnce()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live detectors side by side test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var end = DateTime.SpecifyKind(new DateTime(DateTime.UtcNow.Ticks / TimeSpan.TicksPerSecond * TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);

        /* 29 days of CPU samples 15 minutes apart: 10 to 12 percent, then 95 percent across the last four hours. */
        using (var cpu = new NpgsqlCommand(@"
INSERT INTO collect.cpu_utilization_stats
(collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization)
SELECT 987900000 + g, $1 - make_interval(mins => 1 + g * 15), $2, $3, $1 - make_interval(mins => 1 + g * 15),
       CASE WHEN g < 16 THEN 95 ELSE 10 + (g % 3) END, 0
FROM generate_series(0, 2783) AS g", connection))
        {
            cpu.Parameters.AddWithValue(end);
            cpu.Parameters.AddWithValue(ServerId);
            cpu.Parameters.AddWithValue(ServerName);
            await cpu.ExecuteNonQueryAsync(ct);
        }

        /* One table that grows from empty to 500 MB inside a day. */
        using (var growth = new NpgsqlCommand(@"
INSERT INTO collect.index_object_stats
(collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name, index_id,
 reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES (987800001, $1 - interval '1 day', $2, $3, 'db1', 1, 'dbo', 401, 'Staging', 1, 0, 0, 0),
       (987800002, $1 - interval '1 minute', $2, $3, 'db1', 1, 'dbo', 401, 'Staging', 1, 500, 0, 0)", connection))
        {
            growth.Parameters.AddWithValue(end);
            growth.Parameters.AddWithValue(ServerId);
            growth.Parameters.AddWithValue(ServerName);
            await growth.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        async Task<(List<string> Facts, string Log)> RunAsync(int parallelism)
        {
            var logger = new CapturingTestLogger();
            var detector = new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres), logger);
            var facts = await detector.DetectAnomaliesAsync(new AnalysisContext
            {
                ServerId = ServerId,
                ServerName = ServerName,
                TimeRangeStart = DateTime.SpecifyKind(end.AddHours(-4), DateTimeKind.Utc),
                TimeRangeEnd = DateTime.SpecifyKind(end, DateTimeKind.Utc),
                ServerUtcOffset = TimeSpan.Zero,
                DetectorParallelism = parallelism,
                CancellationToken = ct
            });
            return (AnomalyDetectorsSideBySideTests.Shapes(facts), logger.Joined);
        }

        var serial = await RunAsync(1);
        var sideBySide = await RunAsync(PgAnomalyDetector.MaxDetectorParallelism);

        Assert.True(serial.Facts.Count >= 1, $"the planted store gave no anomaly fact (log: {serial.Log})");
        Assert.Equal(serial.Facts, sideBySide.Facts);
        Assert.Equal(serial.Log, sideBySide.Log);
    }
}
