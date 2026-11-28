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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9966: an on-demand pass on a PostgreSQL target gives its two heavy families, its anomaly detectors and its
/// drill-downs the time budgets the SQL Server engine has. These tests drive
/// <see cref="PgTargetFactCollector.RunUnderTheFamilyDeadlineAsync"/> and the detector loop the PostgreSQL-target detector
/// runs (<see cref="StageDeadline.RunDetectorsInTurnAsync"/>) with planted reads, so no store is read. The deadline is a
/// token the test fires by hand while a read is held, and a held read never finishes by itself, so no assert waits on a
/// clock: <see cref="Limit"/> only bounds a run that hangs.
/// </summary>
public sealed class PgTargetStageTimeBudgetTests
{
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    private static TaskCompletionSource Hook() => new(TaskCreationOptions.RunContinuationsAsynchronously);

    private static NpgsqlDataSource NeverOpened() =>
        NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=nobody;Database=nothing");

    private static Fact Earlier() => new() { Source = "config", Key = "EARLIER" };

    /// <summary>A planted family: reads the fact ahead of it, adds a fact and a failure, then waits on its held read.</summary>
    private sealed class PlantedFamily
    {
        public TaskCompletionSource Started { get; } = Hook();

        /// <summary>Ends the held read when no token does.</summary>
        public TaskCompletionSource Release { get; } = Hook();

        public int Calls;

        public string? SawAhead;

        public async Task CollectHeldFactsAsync(AnalysisContext context, List<Fact> facts)
        {
            Interlocked.Increment(ref Calls);
            SawAhead = facts.Find(fact => fact.Key == "EARLIER")?.Key;
            facts.Add(new Fact { Source = "queries", Key = "HELD_BEFORE" });
            context.RecordCollectionFailure("queries", nameof(CollectHeldFactsAsync), CollectionFailureOutcome.Timeout, new TimeoutException("planted"));
            Started.TrySetResult();
            await Release.Task.WaitAsync(context.CancellationToken);
            facts.Add(new Fact { Source = "queries", Key = "HELD_AFTER" });
        }
    }

    [Fact]
    public async Task AHeavyFamilyTheDeadlineStops_IsNamed_ItsFactsAndFailuresAreRemoved_AndTheFactsAheadOfItStay()
    {
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { FamilyDeadline = deadline.Token, FamilyBudget = TimeSpan.FromSeconds(3) };
        var facts = new List<Fact> { Earlier() };
        var family = new PlantedFamily();

        var run = PgTargetFactCollector.RunUnderTheFamilyDeadlineAsync(context, facts, family.CollectHeldFactsAsync, "queries");
        await family.Started.Task.WaitAsync(Limit, TestContext.Current.CancellationToken);
        Assert.False(run.IsCompleted);
        deadline.Cancel();
        await run.WaitAsync(Limit, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { "queries" }, context.FamiliesLeftOutForTime);
        /* The family read the fact ahead of it from the shared list, and what it added is gone again. */
        Assert.Equal("EARLIER", family.SawAhead);
        Assert.Equal(new[] { "EARLIER" }, facts.Select(fact => fact.Key));
        Assert.Empty(context.CollectionFailures);
        Assert.Contains("queries", FamilyTimeBudget.Describe(context.FamiliesLeftOutForTime, context.FamilyBudget)!, StringComparison.Ordinal);

        /* A failure recorded after the family ended goes to the pass's own list again. */
        context.RecordCollectionFailure("cpu", "later", CollectionFailureOutcome.Timeout, new TimeoutException("planted"));
        Assert.Single(context.CollectionFailures);
    }

    [Fact]
    public async Task AHeavyFamilyNotStartedWhenTheDeadlineHasFired_IsLeftOutUnread()
    {
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { FamilyDeadline = deadline.Token };
        var facts = new List<Fact> { Earlier() };
        var family = new PlantedFamily();
        deadline.Cancel();

        await PgTargetFactCollector.RunUnderTheFamilyDeadlineAsync(context, facts, family.CollectHeldFactsAsync, "io")
            .WaitAsync(Limit, TestContext.Current.CancellationToken);

        Assert.Equal(0, family.Calls);
        Assert.Equal(new[] { "io" }, context.FamiliesLeftOutForTime);
        Assert.Equal(new[] { "EARLIER" }, facts.Select(fact => fact.Key));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AHeavyFamilyThatFinishes_KeepsItsFactsAndItsFailures_AndIsNotNamed(bool withDeadline)
    {
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { FamilyDeadline = withDeadline ? deadline.Token : CancellationToken.None };
        var facts = new List<Fact> { Earlier() };
        var family = new PlantedFamily();
        family.Release.TrySetResult();

        await PgTargetFactCollector.RunUnderTheFamilyDeadlineAsync(context, facts, family.CollectHeldFactsAsync, "queries")
            .WaitAsync(Limit, TestContext.Current.CancellationToken);

        Assert.Empty(context.FamiliesLeftOutForTime);
        Assert.Equal(new[] { "EARLIER", "HELD_BEFORE", "HELD_AFTER" }, facts.Select(fact => fact.Key));
        Assert.Equal("queries", Assert.Single(context.CollectionFailures).Family);
    }

    [Fact]
    public async Task WithNoDeadline_AHeldHeavyFamilyIsNotStopped_AndFinishesWhenItsReadDoes()
    {
        /* The scheduled pass: the context carries no family deadline, so a token that fires elsewhere stops nothing. */
        using var notOnTheContext = new CancellationTokenSource();
        var context = new AnalysisContext();
        var facts = new List<Fact> { Earlier() };
        var family = new PlantedFamily();

        var run = PgTargetFactCollector.RunUnderTheFamilyDeadlineAsync(context, facts, family.CollectHeldFactsAsync, "queries");
        await family.Started.Task.WaitAsync(Limit, TestContext.Current.CancellationToken);
        notOnTheContext.Cancel();
        Assert.False(run.IsCompleted);
        family.Release.TrySetResult();
        await run.WaitAsync(Limit, TestContext.Current.CancellationToken);

        Assert.Empty(context.FamiliesLeftOutForTime);
        Assert.Equal(new[] { "EARLIER", "HELD_BEFORE", "HELD_AFTER" }, facts.Select(fact => fact.Key));
    }

    [Fact]
    public async Task AStopFromThePassToken_GetsOutOfAHeavyFamilyUnderTheDeadline_AndIsNotABudgetCut()
    {
        using var pass = new CancellationTokenSource();
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { CancellationToken = pass.Token, FamilyDeadline = deadline.Token };
        var facts = new List<Fact> { Earlier() };
        var family = new PlantedFamily();

        var run = PgTargetFactCollector.RunUnderTheFamilyDeadlineAsync(context, facts, family.CollectHeldFactsAsync, "queries");
        await family.Started.Task.WaitAsync(Limit, TestContext.Current.CancellationToken);
        pass.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => run.WaitAsync(Limit, TestContext.Current.CancellationToken));
        Assert.Empty(context.FamiliesLeftOutForTime);
    }

    [Fact]
    public void TheTwoHeavyFamilies_AreNamedAsTheirCollectionFailuresAre()
    {
        Assert.Equal("queries", PgTargetFactCollector.QueriesFamily);
        Assert.Equal("io", PgTargetFactCollector.IoFamily);
    }

    /// <summary>Three planted detectors, named the way the real ones are so a reply can name them.</summary>
    private sealed class PlantedDetectors
    {
        public TaskCompletionSource HeldStarted { get; } = Hook();

        public TaskCompletionSource Release { get; } = Hook();

        public int Started;

        public Task DetectFirstAnomalies(AnalysisContext context, List<Fact> facts)
        {
            Interlocked.Increment(ref Started);
            facts.Add(new Fact { Source = "anomaly", Key = "FIRST" });
            return Task.CompletedTask;
        }

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
            return Task.CompletedTask;
        }

        public Func<AnalysisContext, List<Fact>, Task>[] All => [DetectFirstAnomalies, DetectHeldReadAnomalies, DetectLastAnomalies];
    }

    [Fact]
    public async Task InTurn_ADetectorTheDeadlineStops_AndTheOneAfterIt_AreNamed_AndTheOneBeforeKeepsItsFact()
    {
        using var deadline = new CancellationTokenSource();
        var context = new AnalysisContext { DetectorDeadline = deadline.Token, DetectorBudget = TimeSpan.FromSeconds(3) };
        var planted = new PlantedDetectors();
        var anomalies = new List<Fact>();

        var run = StageDeadline.RunDetectorsInTurnAsync(context, anomalies, planted.All);
        await planted.HeldStarted.Task.WaitAsync(Limit, TestContext.Current.CancellationToken);
        deadline.Cancel();
        await run.WaitAsync(Limit, TestContext.Current.CancellationToken);

        Assert.Equal(new[] { "held_read", "last" }, context.DetectorsLeftOutForTime);
        Assert.Equal(new[] { "FIRST" }, anomalies.Select(fact => fact.Key));
        Assert.Equal(2, planted.Started);
    }

    [Fact]
    public async Task InTurn_WithNoDeadline_EveryDetectorRunsToItsEnd_AndNoneIsNamed()
    {
        var context = new AnalysisContext();
        var planted = new PlantedDetectors();
        var anomalies = new List<Fact>();
        planted.Release.TrySetResult();

        await StageDeadline.RunDetectorsInTurnAsync(context, anomalies, planted.All).WaitAsync(Limit, TestContext.Current.CancellationToken);

        Assert.Empty(context.DetectorsLeftOutForTime);
        Assert.Equal(new[] { "FIRST", "HELD_BEFORE", "HELD_AFTER", "LAST" }, anomalies.Select(fact => fact.Key));
    }

    [Fact]
    public async Task ThePostgreSqlTargetDetectors_AreNamedInTheOrderThePassRunsThem()
    {
        await using var postgres = NeverOpened();
        var detector = new PgTargetAnomalyDetector(postgres, new PgTargetBaselineProvider(postgres));

        Assert.Equal(PgTargetStageBudgetLiveTests.Detectors, detector.Detectors().Select(PgAnomalyDetector.DetectorName));
    }
}

/// <summary>
/// #9966: the same budgets through the real PostgreSQL-target collector, detector and drill-down collector against a
/// real store. A read is held by a table lock another session keeps, so it never finishes by itself, and the test fires
/// the deadline by hand once the store shows a read waiting for that table.
/// </summary>
[Collection("live-postgres")]
public sealed class PgTargetStageBudgetLiveTests
{
    private const int ServerId = -996601;
    private const string ServerName = "pg-target-stage-budget-9966";
    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    /* The fourteen detectors, in the order the pass runs and names them. */
    internal static readonly string[] Detectors =
    [
        "tps", "session", "cpu", "deadlock_rate", "wait_profile", "io", "replication", "wal_volume", "blocking",
        "sampled_wait_profile", "plan_regression", "cpu_burn", "bad_actor_share", "database_growth"
    ];

    private static DateTime TruncateToMinutes(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerMinute)), DateTimeKind.Unspecified);

    /// <summary>Takes a lock on <paramref name="table"/> that every read of it waits behind, until the transaction ends.</summary>
    private static async Task<NpgsqlTransaction> HoldReadsOfAsync(NpgsqlConnection locker, string table, CancellationToken ct)
    {
        var transaction = await locker.BeginTransactionAsync(ct);
        using var lockTable = new NpgsqlCommand($"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE", locker, transaction);
        await lockTable.ExecuteNonQueryAsync(ct);
        return transaction;
    }

    /// <summary>Returns once a session waits for a lock on <paramref name="table"/>: the held read has reached the store.</summary>
    private static async Task AReadWaitsForAsync(NpgsqlConnection watcher, string table, CancellationToken ct)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(Limit);
        using var waiting = new NpgsqlCommand("SELECT COUNT(*) FROM pg_locks WHERE NOT granted AND relation = to_regclass($1)", watcher);
        waiting.Parameters.AddWithValue(table);
        while ((long)(await waiting.ExecuteScalarAsync(limit.Token))! == 0)
            await Task.Delay(TimeSpan.FromMilliseconds(20), limit.Token);
    }

    /// <summary>A migrated scratch store with one registered PostgreSQL target and ten minutes of its database series.</summary>
    private static async Task<DateTime> PlantTargetAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        await PgMigrations.MigrateAsync(connection, ct);
        await PgTargetFactCollectorTests.RegisterServerAsync(connection, ServerId, ServerName, MonitoredEngineKind.Postgres, 18, ct);
        var windowEnd = TruncateToMinutes(DateTime.UtcNow).AddMinutes(-1);
        for (var minute = 10; minute >= 0; minute--)
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, ServerId, ServerName, windowEnd.AddMinutes(-minute), ct);
        return windowEnd;
    }

    private static AnalysisContext Pass(DateTime windowEnd, CancellationToken ct) => new()
    {
        ServerId = ServerId,
        ServerName = ServerName,
        TimeRangeStart = windowEnd.AddHours(-4),
        TimeRangeEnd = windowEnd,
        ServerUtcOffset = TimeSpan.Zero,
        CancellationToken = ct
    };

    [Fact]
    public async Task AHeavyFamilyHeldOnItsStoreRead_IsNamedAtTheDeadline_TheFamiliesAfterItStillRead_AndWithNoDeadlineEveryFamilyIsRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL-target family budget test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var windowEnd = await PlantTargetAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var collector = new PgTargetFactCollector(postgres);

        /* No deadline, as on the scheduled pass: every family reads to its end. */
        var whole = Pass(windowEnd, ct);
        var wholeFacts = await collector.CollectFactsAsync(whole).WaitAsync(Limit, ct);
        Assert.Empty(whole.FamiliesLeftOutForTime);
        Assert.Contains(wholeFacts, fact => fact.Key == PgTargetFactKeys.ServerMajorVersion);

        /* A deadline that has fired before the pass starts: the two heavy families are not started, every other family
           reads. This is what the pass holds without them. */
        using var fired = new CancellationTokenSource();
        fired.Cancel();
        var unread = Pass(windowEnd, ct);
        unread.FamilyDeadline = fired.Token;
        var unreadFacts = await collector.CollectFactsAsync(unread).WaitAsync(Limit, ct);
        Assert.Equal(new[] { "queries", "io" }, unread.FamiliesLeftOutForTime);
        Assert.Equal(whole.CollectionFailures.Count, unread.CollectionFailures.Count);
        Assert.NotEmpty(unreadFacts);
        Assert.All(unreadFacts, fact => Assert.Contains(wholeFacts, other => other.Key == fact.Key));

        /* The queries family's read is held on the statement text table, which no other family reads, and the growth
           family's (the last family, not a heavy one) on the database size table. The deadline fires while the queries
           read waits. The io family is then not started, and the growth family still starts its read after the deadline
           and finishes it once its table is free. */
        using var deadline = new CancellationTokenSource();
        var cut = Pass(windowEnd, ct);
        cut.FamilyDeadline = deadline.Token;
        cut.FamilyBudget = TimeSpan.FromSeconds(3);
        List<Fact> cutFacts;
        await using (var statementLocker = new NpgsqlConnection(scratch.ConnectionString))
        await using (var growthLocker = new NpgsqlConnection(scratch.ConnectionString))
        {
            await statementLocker.OpenAsync(ct);
            await growthLocker.OpenAsync(ct);
            await using var heldStatements = await HoldReadsOfAsync(statementLocker, "pg_statement_text", ct);
            await using var heldGrowth = await HoldReadsOfAsync(growthLocker, "pg_database_size_stats", ct);
            var run = collector.CollectFactsAsync(cut);
            await AReadWaitsForAsync(connection, "pg_statement_text", ct);
            deadline.Cancel();
            await AReadWaitsForAsync(connection, "pg_database_size_stats", ct);
            Assert.False(run.IsCompleted);
            await heldGrowth.RollbackAsync(ct);
            cutFacts = await run.WaitAsync(Limit, ct);
            await heldStatements.RollbackAsync(ct);
        }

        Assert.Equal(new[] { "queries", "io" }, cut.FamiliesLeftOutForTime);
        Assert.Equal(whole.CollectionFailures.Count, cut.CollectionFailures.Count);
        Assert.Equal(unreadFacts.Select(fact => fact.Key), cutFacts.Select(fact => fact.Key));
        Assert.Contains("2 families were left out for time (queries, io)", FamilyTimeBudget.Describe(cut.FamiliesLeftOutForTime, cut.FamilyBudget)!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADetectorHeldOnItsBaselineRead_IsNamedAtTheDeadline_AndWithNoDeadlineEveryDetectorIsRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL-target detector budget test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var windowEnd = await PlantTargetAsync(connection, ct);
        await TimescaleSupport.EnsureBaselineFallbackViewsAsync(connection, null, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);

        /* Each run has its own provider, so no run finds a baseline another one computed. */
        PgTargetAnomalyDetector Detector(CapturingTestLogger? logger = null) =>
            new(postgres, new PgTargetBaselineProvider(postgres, logger), logger);

        /* No deadline, as on the scheduled pass: every detector reads to its end. */
        var whole = Pass(windowEnd, ct);
        await Detector().DetectAnomaliesAsync(whole).WaitAsync(Limit, ct);
        Assert.Empty(whole.DetectorsLeftOutForTime);

        /* A deadline that has fired before the detectors start: none is started and each is named, in order. */
        using var fired = new CancellationTokenSource();
        fired.Cancel();
        var unread = Pass(windowEnd, ct);
        unread.DetectorDeadline = fired.Token;
        Assert.Empty(await Detector().DetectAnomaliesAsync(unread).WaitAsync(Limit, ct));
        Assert.Equal(Detectors, unread.DetectorsLeftOutForTime);

        /* The growth detector is the last of the fourteen. Its baseline read of the database size table is held by a
           lock, the thirteen before it finish, and the deadline fires while it waits. */
        using var deadline = new CancellationTokenSource();
        var cut = Pass(windowEnd, ct);
        cut.DetectorDeadline = deadline.Token;
        var logger = new CapturingTestLogger();
        await using (var locker = new NpgsqlConnection(scratch.ConnectionString))
        {
            await locker.OpenAsync(ct);
            await using var held = await HoldReadsOfAsync(locker, "pg_database_size_stats", ct);
            var run = Detector(logger).DetectAnomaliesAsync(cut);
            await AReadWaitsForAsync(connection, "pg_database_size_stats", ct);
            deadline.Cancel();
            await run.WaitAsync(Limit, ct);
            await held.RollbackAsync(ct);
        }

        Assert.Equal(new[] { "database_growth" }, cut.DetectorsLeftOutForTime);
        Assert.DoesNotContain("Database-growth anomaly detection failed", logger.Joined, StringComparison.Ordinal);
        Assert.Contains("not an all-clear", StageTimeBudget.Describe(new StageTimeCuts(cut.DetectorsLeftOutForTime, null, [], null))!, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ADrillDownHeldOnItsStoreRead_LeavesTheFindingWithNoDrillDownAtTheDeadline_AndWithNoDeadlineItIsRead()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live PostgreSQL-target drill-down budget test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        var windowEnd = await PlantTargetAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var logger = new CapturingTestLogger();
        var collector = new PgTargetDrillDownCollector(postgres, logger);

        /* A finding whose drill-down reads the deadlock reports of the window. */
        static AnalysisFinding Deadlocks() => new()
        {
            RootFactKey = PgTargetFactKeys.DeadlockRate,
            StoryPath = PgTargetFactKeys.DeadlockRate,
            PathKeys = [PgTargetFactKeys.DeadlockRate],
            Severity = 0.8
        };

        /* No deadline, as on the scheduled pass: the drill-down reads to its end. */
        var whole = Pass(windowEnd, ct);
        await collector.EnrichFindingsAsync([Deadlocks()], whole).WaitAsync(Limit, ct);
        Assert.Empty(whole.DrillDownsLeftOutForTime);

        using var deadline = new CancellationTokenSource();
        var cut = Pass(windowEnd, ct);
        cut.DrillDownDeadline = deadline.Token;
        var findings = new List<AnalysisFinding> { Deadlocks(), Deadlocks() };
        await using (var locker = new NpgsqlConnection(scratch.ConnectionString))
        {
            await locker.OpenAsync(ct);
            await using var held = await HoldReadsOfAsync(locker, "pg_deadlocks", ct);
            var run = collector.EnrichFindingsAsync(findings, cut);
            await AReadWaitsForAsync(connection, "pg_deadlocks", ct);
            deadline.Cancel();
            await run.WaitAsync(Limit, ct);
            await held.RollbackAsync(ct);
        }

        /* Both findings stay. The first was stopped mid-read and the second was never drilled. */
        Assert.Equal(2, findings.Count);
        Assert.All(findings, finding => Assert.Null(finding.DrillDown));
        Assert.Equal(new[] { PgTargetFactKeys.DeadlockRate, PgTargetFactKeys.DeadlockRate }, cut.DrillDownsLeftOutForTime);
        Assert.DoesNotContain("Drill-down failed", logger.Joined, StringComparison.Ordinal);
        Assert.Contains(
            $"2 findings have no drill-down detail ({PgTargetFactKeys.DeadlockRate} x2)",
            StageTimeBudget.Describe(new StageTimeCuts([], null, cut.DrillDownsLeftOutForTime, null))!, StringComparison.Ordinal);
    }
}
