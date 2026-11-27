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
