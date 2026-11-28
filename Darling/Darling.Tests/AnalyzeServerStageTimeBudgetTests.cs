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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9879: an on-demand pass gives its anomaly detectors and its drill-downs a stated time, as it does its heavy family
/// reads. These tests drive <see cref="PgAnomalyDetector.RunDetectorsAsync"/> and
/// <see cref="PgDrillDownCollector.EnrichEachAsync"/> with planted reads, so no store is read. The deadline is a token the test
/// fires by hand while a read is held, and a held read never finishes by itself, so no assert waits on a clock:
/// <see cref="Limit"/> only bounds a run that hangs.
/// </summary>
public sealed class AnalyzeServerStageTimeBudgetTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    private static TaskCompletionSource Hook() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static NpgsqlDataSource NeverOpened() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Database=nothing");

    private static PgAnomalyDetector Detector(NpgsqlDataSource postgres, ILogger? logger = null) =>
        new(postgres, new PgBaselineProvider(postgres), logger);

    /// <summary>Three planted detectors, named the way the real ones are so a reply can name them.</summary>
    private sealed class Planted
    {
        public TaskCompletionSource HeldStarted { get; } = Hook();

        /// <summary>Ends the held read when no token does.</summary>
        public TaskCompletionSource Release { get; } = Hook();

        public TaskCompletionSource LastFinished { get; } = Hook();

        public int Started;

        public Task DetectFirstAnomalies(AnalysisContext context, List<Fact> facts)
        {
            Interlocked.Increment(ref Started);
            facts.Add(new Fact { Source = "anomaly", Key = "FIRST" });
            return Task.CompletedTask;
        }

        /// <summary>The held read: adds a fact, then waits on the token its context hands it or for the test to release it.</summary>
        public async Task DetectHeldReadAnomalies(AnalysisContext context, List<Fact> facts)
        {
            Interlocked.Increment(ref Started);
            facts.Add(new Fact { Source = "anomaly", Key = "HELD_BEFORE" });
            HeldStarted.TrySetResult();
            await Release.Task.WaitAsync(context.CancellationToken);
            facts.Add(new Fact { Source = "anomaly", Key = "HELD_AFTER" });
        }

        public Task DetectLastAnomalies(AnalysisContext context, List<Fact> facts)
        {
            Interlocked.Increment(ref Started);
            facts.Add(new Fact { Source = "anomaly", Key = "LAST" });
            LastFinished.TrySetResult();
            return Task.CompletedTask;
        }

        public Func<AnalysisContext, List<Fact>, Task>[] All => [DetectFirstAnomalies, DetectHeldReadAnomalies, DetectLastAnomalies];
    }

    [Fact]
    public async Task SideBySide_ADetectorTheDeadlineStops_IsNamed_AddsNoFactOrFailure_AndTheOthersKeepTheirs()
    {
        await using var postgres = NeverOpened();
        var logger = new CapturingTestLogger();
        var detector = Detector(postgres, logger);
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { DetectorDeadline = deadline.Token, DetectorBudget = TimeSpan.FromSeconds(3) };
        var planted = new Planted();
        var anomalies = new List<Fact>();

        var run = detector.RunDetectorsAsync(context, anomalies, planted.All, 4);
        await planted.HeldStarted.Task.WaitAsync(Limit);
        await planted.LastFinished.Task.WaitAsync(Limit);
        deadline.Cancel();
        await run.WaitAsync(Limit);

        /* The held detector's fact from before the stop is dropped with it. */
        Assert.Equal(new[] { "FIRST", "LAST" }, anomalies.Select(f => f.Key));
        Assert.Equal(new[] { "held_read" }, context.DetectorsLeftOutForTime);
        Assert.Empty(context.CollectionFailures);
        Assert.Equal(0, logger.CountAtLevel(LogLevel.Error));
    }

    [Fact]
    public async Task OneAtATime_ADetectorNotStartedWhenTheDeadlineHasFired_IsLeftOutUnread()
    {
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { DetectorDeadline = deadline.Token, DetectorBudget = TimeSpan.FromSeconds(3) };
        var planted = new Planted();
        var anomalies = new List<Fact>();

        var run = detector.RunDetectorsAsync(context, anomalies, planted.All, 1);
        await planted.HeldStarted.Task.WaitAsync(Limit);
        deadline.Cancel();
        await run.WaitAsync(Limit);

        Assert.Equal(new[] { "FIRST" }, anomalies.Select(f => f.Key));
        Assert.Equal(new[] { "held_read", "last" }, context.DetectorsLeftOutForTime);
        Assert.Equal(2, Volatile.Read(ref planted.Started));
    }

    /// <summary>
    /// Nothing held: with no deadline (the scheduled pass) and with one that never fires, every detector runs to its end
    /// and none is named.
    /// </summary>
    [Theory]
    [InlineData(false, 1)]
    [InlineData(false, 4)]
    [InlineData(true, 1)]
    [InlineData(true, 4)]
    public async Task WithNothingHeld_EveryDetectorRunsToItsEnd_AndNoneIsNamed(bool withDeadline, int parallelism)
    {
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { DetectorDeadline = withDeadline ? deadline.Token : CancellationToken.None };
        var planted = new Planted();
        planted.Release.TrySetResult();
        var anomalies = new List<Fact>();

        await detector.RunDetectorsAsync(context, anomalies, planted.All, parallelism).WaitAsync(Limit);

        Assert.Equal(new[] { "FIRST", "HELD_BEFORE", "HELD_AFTER", "LAST" }, anomalies.Select(f => f.Key));
        Assert.Empty(context.DetectorsLeftOutForTime);
    }

    /// <summary>The scheduled pass has no deadline: a read still held when an on-demand deadline would have fired runs to its end.</summary>
    [Fact]
    public async Task WithNoDeadline_AHeldDetectorIsNotStopped_AndFinishesWhenItsReadDoes()
    {
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);
        using var notOnTheContext = new CancellationTokenSource();
        var context = new AnalysisContext();
        var planted = new Planted();
        var anomalies = new List<Fact>();

        var run = detector.RunDetectorsAsync(context, anomalies, planted.All, 1);
        await planted.HeldStarted.Task.WaitAsync(Limit);
        notOnTheContext.Cancel();
        Assert.False(run.IsCompleted);
        planted.Release.TrySetResult();
        await run.WaitAsync(Limit);

        Assert.Equal(new[] { "FIRST", "HELD_BEFORE", "HELD_AFTER", "LAST" }, anomalies.Select(f => f.Key));
        Assert.Empty(context.DetectorsLeftOutForTime);
    }

    [Fact]
    public async Task AStopFromThePassToken_GetsOutOfADetectorUnderTheDeadline_AndIsNotABudgetCut()
    {
        await using var postgres = NeverOpened();
        var detector = Detector(postgres);
        using var pass = new CancellationTokenSource();
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { CancellationToken = pass.Token, DetectorDeadline = deadline.Token };
        var planted = new Planted();
        var anomalies = new List<Fact>();

        var run = detector.RunDetectorsAsync(context, anomalies, planted.All, 4);
        await planted.HeldStarted.Task.WaitAsync(Limit);
        pass.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Limit));
        Assert.Empty(context.DetectorsLeftOutForTime);
    }

    private static AnalysisFinding Finding(string rootFactKey) => new() { RootFactKey = rootFactKey, StoryPath = rootFactKey };

    [Fact]
    public async Task ADrillDownTheDeadlineStops_LeavesItsFindingWithNoDrillDown_NamesIt_AndTheNextIsNotDrilled()
    {
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { DrillDownDeadline = deadline.Token, DrillDownBudget = TimeSpan.FromSeconds(4.5) };
        var findings = new List<AnalysisFinding> { Finding("FIRST"), Finding("HELD"), Finding("NEVER") };
        var heldStarted = Hook();
        var drilled = new List<string>();

        async Task DrillInto(AnalysisFinding finding, AnalysisContext ctx)
        {
            drilled.Add(finding.RootFactKey);
            finding.DrillDown = new Dictionary<string, object> { ["rows"] = 1 };
            if (finding.RootFactKey != "HELD")
                return;
            heldStarted.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ctx.CancellationToken);
        }

        var run = PgDrillDownCollector.EnrichEachAsync(findings, context, DrillInto);
        await heldStarted.Task.WaitAsync(Limit);
        deadline.Cancel();
        await run.WaitAsync(Limit);

        /* Every finding is still in the list. The held one keeps nothing of its half-read drill-down. */
        Assert.Equal(new[] { "FIRST", "HELD", "NEVER" }, findings.Select(f => f.RootFactKey));
        Assert.Equal(new[] { "FIRST", "HELD" }, drilled);
        Assert.NotNull(findings[0].DrillDown);
        Assert.Null(findings[1].DrillDown);
        Assert.Null(findings[2].DrillDown);
        Assert.Equal(new[] { "HELD", "NEVER" }, context.DrillDownsLeftOutForTime);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task WithNothingHeld_EveryFindingIsDrilled_AndNoneIsNamed(bool withDeadline)
    {
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { DrillDownDeadline = withDeadline ? deadline.Token : CancellationToken.None };
        var findings = new List<AnalysisFinding> { Finding("FIRST"), Finding("SECOND") };

        await PgDrillDownCollector.EnrichEachAsync(findings, context, (finding, _) =>
        {
            finding.DrillDown = new Dictionary<string, object> { ["rows"] = 1 };
            return Task.CompletedTask;
        }).WaitAsync(Limit);

        Assert.All(findings, finding => Assert.NotNull(finding.DrillDown));
        Assert.Empty(context.DrillDownsLeftOutForTime);
    }

    [Fact]
    public async Task AStopFromThePassToken_GetsOutOfADrillDownUnderTheDeadline_AndIsNotABudgetCut()
    {
        using var pass = new CancellationTokenSource();
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { CancellationToken = pass.Token, DrillDownDeadline = deadline.Token };
        var findings = new List<AnalysisFinding> { Finding("HELD"), Finding("NEVER") };
        var heldStarted = Hook();

        var run = PgDrillDownCollector.EnrichEachAsync(findings, context, async (_, ctx) =>
        {
            heldStarted.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ctx.CancellationToken);
        });
        await heldStarted.Task.WaitAsync(Limit);
        pass.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Limit));
        Assert.Empty(context.DrillDownsLeftOutForTime);
    }

    [Fact]
    public void TheReplyHelper_NamesEachDetectorAndFinding_AndReturnsAnUncutPayloadUntouched()
    {
        var options = new JsonSerializerOptions();
        var payload = new { status = "ok" };

        Assert.Same(payload, StageTimeBudget.Attach(payload, StageTimeCuts.None, options));
        Assert.Null(StageTimeBudget.Describe(StageTimeCuts.None));

        var cuts = new StageTimeCuts(
            ["query_duration", "batch_request"], TimeSpan.FromSeconds(3),
            ["QUERY_SPILLS", "BAD_ACTOR", "QUERY_SPILLS"], TimeSpan.FromSeconds(4.5));
        var reply = Assert.IsType<JsonObject>(StageTimeBudget.Attach(payload, cuts, options));

        var detectors = reply["detectors_left_out"]!.AsArray();
        Assert.Equal(new[] { "query_duration", "batch_request" }, detectors.Select(d => d!["detector"]!.GetValue<string>()));
        Assert.All(detectors, d => Assert.Equal(StageTimeBudget.Reason, d!["reason"]!.GetValue<string>()));
        Assert.All(detectors, d => Assert.Equal(3.0, d!["budget_seconds"]!.GetValue<double>()));

        var drillDowns = reply["drill_downs_left_out"]!.AsArray();
        Assert.Equal(new[] { "QUERY_SPILLS", "BAD_ACTOR" }, drillDowns.Select(d => d!["finding"]!.GetValue<string>()));
        Assert.Equal(new[] { 2, 1 }, drillDowns.Select(d => d!["findings"]!.GetValue<int>()));
        Assert.All(drillDowns, d => Assert.Equal(4.5, d!["budget_seconds"]!.GetValue<double>()));

        var sentence = StageTimeBudget.Describe(cuts)!;
        Assert.Contains("2 anomaly detectors were left out for time (query_duration, batch_request)", sentence, StringComparison.Ordinal);
        Assert.Contains("not an all-clear", sentence, StringComparison.Ordinal);
        Assert.Contains("3 findings have no drill-down detail (QUERY_SPILLS x2, BAD_ACTOR)", sentence, StringComparison.Ordinal);

        /* Only the list that has an entry is added. */
        var drillOnly = Assert.IsType<JsonObject>(StageTimeBudget.Attach(payload, cuts with { Detectors = [] }, options));
        Assert.False(drillOnly.ContainsKey("detectors_left_out"));
        Assert.True(drillOnly.ContainsKey("drill_downs_left_out"));
    }
}

/// <summary>
/// #9879: the same budgets against a real store. A read is held by a table lock another session keeps, so it never
/// finishes by itself, and the test fires the deadline by hand once the store shows that read waiting for the lock.
/// </summary>
[Collection("live-postgres")]
public sealed class AnalyzeServerStageBudgetLiveTests
{
    private const int ScratchServerId = -987964;
    private const string ScratchServerName = "stage-budget-9879";
    private const string ServerName = "darling-analyze-stage-budget-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    /* The nine detectors, in the order the pass runs and names them. */
    private static readonly string[] Detectors =
        ["cpu", "wait", "blocking", "io", "batch_request", "session", "query_duration", "memory", "object_stats"];

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>Takes a lock on <paramref name="table"/> that every read of it waits behind, until the transaction ends.</summary>
    private static async Task<NpgsqlTransaction> HoldReadsOfAsync(NpgsqlConnection locker, string table, CancellationToken ct)
    {
        var transaction = await locker.BeginTransactionAsync(ct);
        using var lockTable = new NpgsqlCommand($"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE", locker, transaction);
        await lockTable.ExecuteNonQueryAsync(ct);
        return transaction;
    }

    /// <summary>Returns once a session of this database waits for a lock: the held read has reached the store.</summary>
    private static async Task AReadWaitsForTheLockAsync(NpgsqlConnection watcher, CancellationToken ct)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(Limit);
        using var waiting = new NpgsqlCommand(
            "SELECT COUNT(*) FROM pg_locks WHERE NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())", watcher);
        while ((long)(await waiting.ExecuteScalarAsync(limit.Token))! == 0)
            await Task.Delay(TimeSpan.FromMilliseconds(20), limit.Token);
    }

    [Fact]
    public async Task ADetectorHeldOnItsStoreRead_IsNamedAtTheDeadline_ItsFindingIsAbsent_AndWithNoDeadlineItIsRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live detector budget test (it mints its own scratch database).");

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
SELECT 987600000 + g, $1 - make_interval(mins => 1 + g * 15), $2, $3, $1 - make_interval(mins => 1 + g * 15),
       CASE WHEN g < 16 THEN 95 ELSE 10 + (g % 3) END, 0
FROM generate_series(0, 2783) AS g", connection))
        {
            cpu.Parameters.AddWithValue(end);
            cpu.Parameters.AddWithValue(ScratchServerId);
            cpu.Parameters.AddWithValue(ScratchServerName);
            await cpu.ExecuteNonQueryAsync(ct);
        }

        /* One table that grows from empty to 500 MB inside a day: the object stats detector's finding. */
        using (var growth = new NpgsqlCommand(@"
INSERT INTO collect.index_object_stats
(collection_id, collection_time, server_id, server_name, database_name, database_id, schema_name, object_id, table_name, index_id,
 reserved_mb, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES (987500001, $1 - interval '1 day', $2, $3, 'db1', 1, 'dbo', 401, 'Staging', 1, 0, 0, 0),
       (987500002, $1 - interval '1 minute', $2, $3, 'db1', 1, 'dbo', 401, 'Staging', 1, 500, 0, 0)", connection))
        {
            growth.Parameters.AddWithValue(end);
            growth.Parameters.AddWithValue(ScratchServerId);
            growth.Parameters.AddWithValue(ScratchServerName);
            await growth.ExecuteNonQueryAsync(ct);
        }

        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        AnalysisContext Pass(CancellationToken deadline) => new()
        {
            ServerId = ScratchServerId,
            ServerName = ScratchServerName,
            TimeRangeStart = DateTime.SpecifyKind(end.AddHours(-4), DateTimeKind.Utc),
            TimeRangeEnd = DateTime.SpecifyKind(end, DateTimeKind.Utc),
            ServerUtcOffset = TimeSpan.Zero,
            DetectorParallelism = 1,
            DetectorDeadline = deadline,
            CancellationToken = ct
        };

        /* No deadline, as on the scheduled pass: every detector reads to its end. */
        var whole = Pass(CancellationToken.None);
        var wholeFacts = await new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres)).DetectAnomaliesAsync(whole);
        Assert.Empty(whole.DetectorsLeftOutForTime);
        Assert.Contains(wholeFacts, fact => fact.Key == "ANOMALY_OBJECT_GROWTH");
        Assert.Contains(wholeFacts, fact => fact.Key == "ANOMALY_CPU_SPIKE");

        /* The object stats detector is the last of the nine. Its read of the object stats table is held by a lock, the
           eight before it finish, and the deadline fires while it waits. */
        using var deadline = new CancellationTokenSource();
        var cut = Pass(deadline.Token);
        var logger = new CapturingTestLogger();
        List<Fact> cutFacts;
        await using (var locker = new NpgsqlConnection(scratch.ConnectionString))
        {
            await locker.OpenAsync(ct);
            await using var held = await HoldReadsOfAsync(locker, "collect.index_object_stats", ct);
            var run = new PgAnomalyDetector(postgres, new PgBaselineProvider(postgres), logger).DetectAnomaliesAsync(cut);
            await AReadWaitsForTheLockAsync(connection, ct);
            deadline.Cancel();
            cutFacts = await run.WaitAsync(Limit, ct);
            await held.RollbackAsync(ct);
        }

        Assert.Equal(new[] { "object_stats" }, cut.DetectorsLeftOutForTime);
        Assert.DoesNotContain(cutFacts, fact => fact.Key == "ANOMALY_OBJECT_GROWTH");
        Assert.Contains(cutFacts, fact => fact.Key == "ANOMALY_CPU_SPIKE");
        Assert.DoesNotContain("Object stats anomaly detection failed", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("not an all-clear", StageTimeBudget.Describe(new StageTimeCuts(cut.DetectorsLeftOutForTime, null, [], null))!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADrillDownHeldOnItsStoreRead_LeavesTheFindingWithNoDrillDownAtTheDeadline_AndWithNoDeadlineItIsRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live drill-down budget test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        var now = DateTime.UtcNow;
        AnalysisContext Pass(CancellationToken deadline) => new()
        {
            ServerId = ScratchServerId,
            ServerName = ScratchServerName,
            TimeRangeStart = now.AddHours(-4),
            TimeRangeEnd = now,
            DrillDownDeadline = deadline,
            CancellationToken = ct
        };

        /* A finding whose drill-down reads the query history of the window (the top spilling queries). */
        static AnalysisFinding Spills() => new() { RootFactKey = "QUERY_SPILLS", StoryPath = "QUERY_SPILLS", PathKeys = ["QUERY_SPILLS"], Severity = 0.8 };

        var collector = new PgDrillDownCollector(postgres);

        /* No deadline, as on the scheduled pass: the drill-down reads to its end. */
        var whole = Pass(CancellationToken.None);
        await collector.EnrichFindingsAsync([Spills()], whole).WaitAsync(Limit, ct);
        Assert.Empty(whole.DrillDownsLeftOutForTime);

        using var deadline = new CancellationTokenSource();
        var cut = Pass(deadline.Token);
        var findings = new List<AnalysisFinding> { Spills(), Spills() };
        await using (var locker = new NpgsqlConnection(scratch.ConnectionString))
        {
            await locker.OpenAsync(ct);
            await using var held = await HoldReadsOfAsync(locker, "collect.query_stats", ct);
            var run = collector.EnrichFindingsAsync(findings, cut);
            await AReadWaitsForTheLockAsync(connection, ct);
            deadline.Cancel();
            await run.WaitAsync(Limit, ct);
            await held.RollbackAsync(ct);
        }

        /* Both findings stay. The first was stopped mid-read and the second was never drilled. */
        Assert.Equal(2, findings.Count);
        Assert.All(findings, finding => Assert.Null(finding.DrillDown));
        Assert.Equal(new[] { "QUERY_SPILLS", "QUERY_SPILLS" }, cut.DrillDownsLeftOutForTime);
        Assert.Contains(
            "2 findings have no drill-down detail (QUERY_SPILLS x2)",
            StageTimeBudget.Describe(new StageTimeCuts([], null, cut.DrillDownsLeftOutForTime, null))!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnEndedDetectorBudget_NamesTheNineDetectorsInTheReply_AndTheScheduledPassLeavesNoneOut()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live stage-budget reply test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            using (var register = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)
ON CONFLICT (server_id) DO UPDATE SET is_enabled = TRUE, sql_major_version = 15;", connection))
            {
                register.Parameters.AddWithValue(ServerId);
                register.Parameters.AddWithValue(ServerName);
                register.Parameters.AddWithValue(Naive(DateTime.UtcNow));
                await register.ExecuteNonQueryAsync(ct);
            }

            /* More than a day of history, and a reading every fifteen minutes across the default four-hour window, so the
               pass gets past its history gate, reads an observed window, and the detectors have a baseline period to read. */
            var now = DateTime.UtcNow;
            await PlantWaitAsync(connection, Naive(now.AddHours(-30)), ct);
            for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
                await PlantWaitAsync(connection, Naive(now.AddMinutes(-minutesAgo)), ct);

            /* Both stage budgets ended before the pass starts, so no detector is started. No family budget: the family
               reads are not what this test cuts. */
            var service = new DarlingAnalysisService(postgres)
            {
                OnDemandFamilyBudget = null,
                OnDemandDetectorBudget = TimeSpan.Zero,
                OnDemandDrillDownBudget = TimeSpan.Zero
            };

            var cut = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(cut))
            {
                var entries = LeftOut(doc.RootElement, "detectors_left_out");
                Assert.Equal(Detectors, entries.Select(e => e.GetProperty("detector").GetString()!));
                Assert.All(entries, e => Assert.Equal(StageTimeBudget.Reason, e.GetProperty("reason").GetString()));
                Assert.All(entries, e => Assert.Equal(0.0, e.GetProperty("budget_seconds").GetDouble()));
                Assert.Empty(LeftOut(doc.RootElement, "families_left_out"));

                /* A detector the budget stopped is not a failed read, and the reply's prose carries the sentence the
                   structured entries stand for. */
                Assert.Empty(service.LastCollectionFailures);
                Assert.Contains(
                    StageTimeBudget.Describe(new StageTimeCuts(Detectors, TimeSpan.Zero, [], TimeSpan.Zero))!,
                    doc.RootElement.GetProperty("message").GetString()!,
                    StringComparison.Ordinal);

                /* A pass that left a detector out stored no findings, and the reply says where the stored ones are. */
                Assert.Contains(StageTimeBudget.NotStored, doc.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);
                Assert.All(entries, e => Assert.Contains(StageTimeBudget.LastWholePass, e.GetProperty("read_it_with").GetString()!, StringComparison.Ordinal));
            }
            Assert.Equal(Detectors, service.LastStageCuts.Detectors);
            Assert.Equal(TimeSpan.Zero, service.LastStageCuts.DetectorBudget);

            /* The scheduled pass of the same service has no budget, whatever the on-demand ones are set to. */
            await service.AnalyzeAsync(ServerId, ServerName, 4, ct, persistCollectionCaveats: true);
            Assert.Empty(service.LastStageCuts.Detectors);
            Assert.Empty(service.LastStageCuts.DrillDowns);
            Assert.Null(service.LastStageCuts.DetectorBudget);
            Assert.Null(service.LastStageCuts.DrillDownBudget);

            /* An on-demand call with no budget runs every detector, and its reply carries neither field. */
            service.OnDemandDetectorBudget = null;
            service.OnDemandDrillDownBudget = null;
            var whole = await DarlingMcpTools.AnalyzeServer(service, postgres, ServerName);
            using (var doc = JsonDocument.Parse(whole))
            {
                Assert.Empty(LeftOut(doc.RootElement, "detectors_left_out"));
                Assert.Empty(LeftOut(doc.RootElement, "drill_downs_left_out"));
            }
            Assert.DoesNotContain(StageTimeBudget.NotStored, whole, StringComparison.Ordinal);
            Assert.Empty(service.LastStageCuts.Detectors);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    private const int NotStoredServerId = -987965;
    private const string NotStoredServerName = "cut-pass-not-stored-9879";

    /// <summary>
    /// Stands in front of the real detector. With a deadline to fire, it holds the object stats detector, the last of the
    /// nine, on a table lock, fires the deadline once the store shows that read waiting, and lets the pass go on.
    /// </summary>
    private sealed class LastDetectorHeld(IAnomalyDetector inner, string connectionString) : IAnomalyDetector
    {
        /// <summary>The deadline of the pass to cut, or null to run every detector to its end.</summary>
        public CancellationTokenSource? DeadlineToFire { get; set; }

        public async Task<List<Fact>> DetectAnomaliesAsync(AnalysisContext context)
        {
            if (DeadlineToFire is not { } deadline)
                return await inner.DetectAnomaliesAsync(context);

            var ct = context.PassToken;
            await using var locker = new NpgsqlConnection(connectionString);
            await locker.OpenAsync(ct);
            await using var watcher = new NpgsqlConnection(connectionString);
            await watcher.OpenAsync(ct);
            await using var held = await HoldReadsOfAsync(locker, "collect.index_object_stats", ct);
            var run = inner.DetectAnomaliesAsync(context);
            await AReadWaitsForTheLockAsync(watcher, ct);
            deadline.Cancel();
            var facts = await run.WaitAsync(Limit, ct);
            await held.RollbackAsync(ct);
            return facts;
        }
    }

    /// <summary>
    /// A current pass that left something out for time stores no findings, so the newest stored batch stays the last
    /// whole pass for the latest-batch read and for <c>get_analysis_findings</c>, with the drill-down and the remediation
    /// that batch stored. Each stage is cut in turn. The family and drill-down deadlines have fired before the pass starts.
    /// The detector deadline is fired by hand while the last detector waits on a lock, after the wait detector has added
    /// the pass's finding. Nothing here waits on a clock. A current pass that leaves nothing out stores its batch.
    /// </summary>
    [Theory]
    [InlineData("family")]
    [InlineData("detector")]
    [InlineData("drill_down")]
    public async Task ACurrentPassThatLeftSomethingOutForTime_StoresNoFindings_AndTheLastWholePassStaysTheLatest(string stage)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live cut-pass persistence test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        using (var register = new NpgsqlCommand(@"
INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date)
VALUES ($1, $2, $2, TRUE, 15, $3, $3)", connection))
        {
            register.Parameters.AddWithValue(NotStoredServerId);
            register.Parameters.AddWithValue(NotStoredServerName);
            register.Parameters.AddWithValue(Naive(DateTime.UtcNow));
            await register.ExecuteNonQueryAsync(ct);
        }

        /* More than a day of wait history in one hour-of-day and day-of-week bucket, then three heavy collections a week
           later at the same hour: one ANOMALY_WAIT_PROFILE fact, which roots a finding. */
        var day = DateTime.UtcNow.Date.AddDays(-8);
        while (day.DayOfWeek != DayOfWeek.Monday) day = day.AddDays(-1);
        var historyStart = Naive(day.AddHours(10));
        for (var i = 0; i < 12; i++)
            await PlantNotStoredWaitAsync(connection, historyStart.AddMinutes(5 * i), (i == 5 || i == 6) ? 0L : 60_000L, ct);
        var incidentStart = historyStart.AddDays(7);
        for (var minutes = 5; minutes <= 15; minutes += 5)
            await PlantNotStoredWaitAsync(connection, incidentStart.AddMinutes(minutes), 200_000L, ct);

        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* A current pass (no as_of) over the incident's hour. */
        AnalysisContext Pass() => new()
        {
            ServerId = NotStoredServerId,
            ServerName = NotStoredServerName,
            TimeRangeStart = incidentStart,
            TimeRangeEnd = incidentStart.AddHours(1),
            ServerUtcOffset = TimeSpan.Zero,
            CancellationToken = ct
        };

        LastDetectorHeld? detector = null;
        var service = new DarlingAnalysisService(
            postgres, sqlServerDetectorWrapper: inner => detector = new LastDetectorHeld(inner, scratch.ConnectionString));

        /* The whole pass: nothing left out, so its findings are stored. */
        var whole = Pass();
        Assert.NotEmpty(await service.AnalyzeAsync(whole).WaitAsync(Limit, ct));
        Assert.False(whole.LeftOutForTime);
        Assert.True(whole.StoresFindings);
        Assert.False(service.LastPassLeftOutForTime);

        /* This store's finding is an anomaly, which has no drill-down of its own. The whole pass's rows are given the
           drill-down and the remediation a drilled finding stores, through the store's own serializers, so the reads below
           have detail to lose. */
        using (var detail = new NpgsqlCommand(
            "UPDATE analysis_findings SET drill_down_json = $2, remediation_action_json = $3 WHERE server_id = $1", connection))
        {
            detail.Parameters.AddWithValue(NotStoredServerId);
            detail.Parameters.AddWithValue(DrillDownSerializer.Serialize(new Dictionary<string, object> { ["top_waits"] = "FB9879N_TEST_WAIT" })!);
            detail.Parameters.AddWithValue(AlertContextSerializer.SerializeAction(new RemediationAction(
                FactKey: "MISSING_INDEX",
                Action: "set",
                Targets: [],
                MissingIndexTargets: [new MissingIndexTarget("dbo.Posts", 83.2, "CREATE NONCLUSTERED INDEX [ix_fb9879n] ON [dbo].[Posts] ([OwnerUserId]);")])));
            Assert.True(await detail.ExecuteNonQueryAsync(ct) > 0);
        }

        var storedByTheWholePass = await StoredFindingRowsAsync(connection, ct);
        Assert.NotEmpty(storedByTheWholePass);
        var latestOfTheWholePass = LatestBatch(await service.GetLatestFindingsAsync(NotStoredServerId));
        Assert.NotEmpty(latestOfTheWholePass);
        Assert.All(latestOfTheWholePass, line => Assert.EndsWith("drill_down=True remediation=True", line, StringComparison.Ordinal));
        var readOfTheWholePass = ReadBack(await DarlingMcpTools.GetAnalysisFindings(service, postgres, NotStoredServerName, include_drilldown: true));
        Assert.NotEmpty(readOfTheWholePass);
        Assert.All(readOfTheWholePass, line => Assert.Contains("FB9879N_TEST_WAIT", line, StringComparison.Ordinal));
        Assert.All(readOfTheWholePass, line => Assert.Contains("ix_fb9879n", line, StringComparison.Ordinal));

        /* The same pass again with one stage cut. */
        var fired = new CancellationToken(canceled: true);
        using var detectorDeadline = new CancellationTokenSource();
        var cut = Pass();
        if (stage == "family")
        {
            cut.FamilyDeadline = fired;
        }
        else if (stage == "detector")
        {
            cut.DetectorDeadline = detectorDeadline.Token;
            detector!.DeadlineToFire = detectorDeadline;
        }
        else
        {
            cut.DrillDownDeadline = fired;
        }

        var cutFindings = await service.AnalyzeAsync(cut).WaitAsync(Limit, ct);
        detector!.DeadlineToFire = null;

        if (stage == "family")
            Assert.NotEmpty(cut.FamiliesLeftOutForTime);
        else if (stage == "detector")
            Assert.Equal(new[] { "object_stats" }, cut.DetectorsLeftOutForTime);
        else
            Assert.NotEmpty(cut.DrillDownsLeftOutForTime);
        Assert.True(cut.LeftOutForTime);
        Assert.True(cut.PersistFindings);
        Assert.False(cut.StoresFindings);
        Assert.True(service.LastPassLeftOutForTime);

        /* The cut pass still has the finding to write, so it is the rule that keeps it out of the store. */
        Assert.NotEmpty(cutFindings);
        Assert.Contains(cutFindings, finding => finding.RootFactKey == "ANOMALY_WAIT_PROFILE");

        /* No row was written, and both reads still return the whole pass's batch with the detail it stored. */
        Assert.Equal(storedByTheWholePass, await StoredFindingRowsAsync(connection, ct));
        Assert.Equal(latestOfTheWholePass, LatestBatch(await service.GetLatestFindingsAsync(NotStoredServerId)));
        Assert.Equal(readOfTheWholePass, ReadBack(await DarlingMcpTools.GetAnalysisFindings(service, postgres, NotStoredServerName, include_drilldown: true)));

        /* A current pass that leaves nothing out stores its batch, and that batch is then the latest. */
        var again = Pass();
        Assert.NotEmpty(await service.AnalyzeAsync(again).WaitAsync(Limit, ct));
        Assert.True(again.StoresFindings);
        Assert.False(service.LastPassLeftOutForTime);
        Assert.Equal(storedByTheWholePass.Count * 2, (await StoredFindingRowsAsync(connection, ct)).Count);
        Assert.NotEqual(latestOfTheWholePass, LatestBatch(await service.GetLatestFindingsAsync(NotStoredServerId)));
    }

    /// <summary>Every stored finding row of the server: its id, its batch time, its drill-down and its remediation, in id order.</summary>
    private static async Task<List<string>> StoredFindingRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
SELECT finding_id::text || ' at ' || analysis_time::text
       || ' drill_down=' || COALESCE(drill_down_json, 'null')
       || ' remediation=' || COALESCE(remediation_action_json, 'null')
FROM analysis_findings
WHERE server_id = $1
ORDER BY finding_id", connection);
        command.Parameters.AddWithValue(NotStoredServerId);
        var rows = new List<string>();
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add(reader.GetString(0));
        return rows;
    }

    /// <summary>The latest-batch read, one line per finding: which row it is and whether it carries a drill-down and a remediation.</summary>
    private static List<string> LatestBatch(List<AnalysisFinding> findings) =>
        findings.Select(f => $"{f.FindingId} at {f.AnalysisTime:o} drill_down={f.DrillDown is not null} remediation={f.Remediation is not null}").ToList();

    /// <summary>The <c>get_analysis_findings</c> reply, one line per finding: which row represents it, how often it occurred, and its drill-down and remediation as served.</summary>
    private static List<string> ReadBack(string reply)
    {
        static string Field(JsonElement finding, string name) =>
            finding.TryGetProperty(name, out var value) ? value.GetRawText() : "absent";

        using var doc = JsonDocument.Parse(reply);
        return doc.RootElement.GetProperty("findings").EnumerateArray()
            .Select(f => $"{Field(f, "finding_id")} at {Field(f, "analysis_time")} x{Field(f, "occurrences")} drill_down={Field(f, "drill_down")} remediation={Field(f, "remediation_command")}")
            .ToList();
    }

    private static async Task PlantNotStoredWaitAsync(NpgsqlConnection connection, DateTime at, long waitMs, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, 'FB9879N_TEST_WAIT', 50, $5)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(NotStoredServerId);
        command.Parameters.AddWithValue(NotStoredServerName);
        command.Parameters.AddWithValue(waitMs);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>The reply's entries under <paramref name="field"/>, from the top level or from <c>hints</c>, and none when the reply has no such field.</summary>
    private static List<JsonElement> LeftOut(JsonElement reply, string field)
    {
        if (reply.TryGetProperty(field, out var top))
            return top.EnumerateArray().ToList();
        if (reply.TryGetProperty("hints", out var hints) && hints.ValueKind == JsonValueKind.Object
            && hints.TryGetProperty(field, out var nested))
            return nested.EnumerateArray().ToList();
        return [];
    }

    private static async Task PlantWaitAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type, delta_waiting_tasks, delta_wait_time_ms)
VALUES ($1, $2, $3, $4, 'FB9879S_BENIGN_WAIT', 50, 100)", connection);
        command.Parameters.AddWithValue(CollectionIdGenerator.Next());
        command.Parameters.AddWithValue(at);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(ServerName);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM wait_stats WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {ServerId}; " +
            $"DELETE FROM analysis_muted WHERE server_id = {ServerId}; " +
            $"DELETE FROM collect.analysis_collection_caveats WHERE server_id = {ServerId}; " +
            $"DELETE FROM servers WHERE server_id = {ServerId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
