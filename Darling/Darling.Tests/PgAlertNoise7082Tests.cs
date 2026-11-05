/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7082 parts 1 to 3: the PostgreSQL Poison Wait Warning holds for two back-to-back windows, its detail text names
/// the top wait events and statements, and High CPU needs instance CPU at the bar as well as capacity use.
/// </summary>
public sealed class PgAlertNoise7082Tests
{
    private const long WindowMs = 10 * 60_000;

    private static PostgresPoisonWaitAlertInfo Poison(double currentAvgWaiters, double priorAvgWaiters, string waitEvent = "BtreePage") =>
        new("IPC", waitEvent, (long)(currentAvgWaiters * WindowMs), 100_000, new DateTime(2026, 11, 1, 2, 0, 0), (long)(priorAvgWaiters * WindowMs));

    /* ---------------- part 1: the Warning hold ---------------- */

    [Fact]
    public void AnEightMinuteEpisodeOverTheWarningBar_DoesNotPage()
    {
        /* 8 minutes at 2.4 waiters: the window it sits in averages 1.92, the window before holds none. */
        Assert.Null(PostgresAlertEvaluator.EvaluatePoisonWait(Poison(8 * 2.4 / 10, 0)));
    }

    [Fact]
    public void AnElevenMinuteEpisode_DoesNotSatisfyTheTwoWindowHold_OnASlidingWindow()
    {
        /* The window slides, so at minute 11 the current window holds 10 minutes of the episode (2.4 average)
           and the window before it holds one minute (0.24). The hold is on the collector's clock, so it is not met. */
        Assert.Null(PostgresAlertEvaluator.EvaluatePoisonWait(Poison(2.4, 0.24)));
    }

    [Fact]
    public void AnEpisodeThatHoldsForTwentyMinutes_Pages_AsAWarning()
    {
        var finding = PostgresAlertEvaluator.EvaluatePoisonWait(Poison(2.4, 2.4));

        Assert.NotNull(finding);
        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
        Assert.Equal("IPC:BtreePage", finding.Subject);
    }

    [Theory]
    [InlineData(1.0, true)]
    [InlineData(0.999, false)]
    public void TheEarlierWindowHasToBeAtTheWarningBarItself(double priorAvgWaiters, bool pages)
    {
        var finding = PostgresAlertEvaluator.EvaluatePoisonWait(Poison(1.5, priorAvgWaiters));

        Assert.Equal(pages, finding is not null);
    }

    [Fact]
    public void ACriticalPoisonWait_StillPagesOnTodaysRule_WithNothingInTheEarlierWindow()
    {
        var finding = PostgresAlertEvaluator.EvaluatePoisonWait(Poison(PoisonWaitEvaluator.CriticalAvgWaiters, 0));

        Assert.NotNull(finding);
        Assert.Equal(AlertSeverityLevel.Critical, finding!.Severity);
    }

    [Fact]
    public void SqlServerPoisonWait_StillPagesOnTodaysRule_WithoutAnEarlierWindow()
    {
        /* One window at the Warning bar pages at once: the SQL Server evaluator has no hold. */
        var finding = PoisonWaitEvaluator.EvaluateSqlServer(
            new PoisonWaitAccumulation("THREADPOOL", (long)(1.0 * WindowMs), 5_000, 10, new DateTime(2026, 11, 1, 2, 0, 0)));

        Assert.NotNull(finding);
        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
    }

    /* ---------------- part 2: the detail text ---------------- */

    [Fact]
    public void ThePoisonWaitDetail_NamesTheTopWaitEventsAndTheStatements()
    {
        var detail = new PostgresPoisonWaitDetail(
            new[]
            {
                new PostgresPoisonWaitTopEvent("IPC", "BtreePage", 2.41),
                new PostgresPoisonWaitTopEvent("IPC", "BufferIo", 1.2),
                new PostgresPoisonWaitTopEvent("LWLock", "BufferMapping", 0.3),
            },
            new[]
            {
                new PostgresPoisonWaitStatement(111, "INSERT INTO orders (id) VALUES (?)", 480, "pg_wait_sampling"),
                new PostgresPoisonWaitStatement(222, null, 12, "pg_session_states"),
            });

        var text = PostgresAlertEvaluator.BuildPoisonWaitDetailText(detail, "IPC:BtreePage")!;

        Assert.Contains("IPC:BtreePage  2.4", text, StringComparison.Ordinal);
        Assert.Contains("IPC:BufferIo  1.2", text, StringComparison.Ordinal);
        Assert.Contains("LWLock:BufferMapping  0.3", text, StringComparison.Ordinal);
        Assert.Contains("Top statements waiting on IPC:BtreePage:", text, StringComparison.Ordinal);
        Assert.Contains("queryid 111 (480 samples): INSERT INTO orders (id) VALUES (?)", text, StringComparison.Ordinal);
        Assert.Contains("queryid 222 (12 captures)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePoisonWaitDetail_SaysSoInOneLine_WhenNoStatementDataExistsForTheWindow()
    {
        var detail = new PostgresPoisonWaitDetail(
            new[] { new PostgresPoisonWaitTopEvent("IPC", "BufferIo", 1.5) },
            Array.Empty<PostgresPoisonWaitStatement>());

        var text = PostgresAlertEvaluator.BuildPoisonWaitDetailText(detail, "IPC:BufferIo")!;

        Assert.Contains("Statements waiting on IPC:BufferIo: none recorded in the window", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePoisonWaitDetail_IsAbsent_WhenTheDetailReadFailed()
    {
        Assert.Null(PostgresAlertEvaluator.BuildPoisonWaitDetailText(null, "IPC:BufferIo"));
    }

    [Theory]
    [InlineData(McpDataDetail.Full, "SELECT * FROM t WHERE name = 'bob' AND id = 42")]
    [InlineData(McpDataDetail.MaskLiterals, "SELECT * FROM t WHERE name = '?' AND id = ?")]
    [InlineData(McpDataDetail.MetadataOnly, null)]
    public void StatementTextInTheDetail_FollowsMcpDataDetail(McpDataDetail mode, string? expected)
    {
        Assert.Equal(
            expected,
            DarlingPostgresAlertReadAdapter.PoisonStatementTextAt("SELECT * FROM t WHERE name = 'bob' AND id = 42", mode));
    }

    /* ---------------- part 3: High CPU needs instance CPU ---------------- */

    private static PersistenceOutcome Run(double capacity, double instanceCpu, int samples)
    {
        var state = PersistenceState.Initial;
        var outcome = PersistenceOutcome.None;
        for (var i = 0; i < samples && outcome != PersistenceOutcome.Fire; i++)
        {
            var evaluation = AlertPersistenceGate.Evaluate(
                state,
                DarlingWorker.PgCpuSampleBreaches(capacity, instanceCpu, 90),
                AlertEngine.CpuBreachSamples,
                AlertEngine.CpuClearSamples);
            state = evaluation.State;
            outcome = evaluation.Outcome;
        }

        return outcome;
    }

    [Fact]
    public void CapacityAtTheCeiling_WithInstanceCpuAtFivePercent_DoesNotPage()
    {
        Assert.Equal(PersistenceOutcome.None, Run(capacity: 100, instanceCpu: 5, samples: 100));
    }

    [Fact]
    public void CapacityAt92Percent_WithInstanceCpuAt100Percent_Pages()
    {
        Assert.Equal(PersistenceOutcome.Fire, Run(capacity: 92, instanceCpu: 100, samples: AlertEngine.CpuBreachSamples));
    }

    [Theory]
    [InlineData(90, 90, true)]
    [InlineData(89.9, 100, false)]
    [InlineData(100, 89.9, false)]
    public void BothFiguresAreHeldToTheSameBar(double capacity, double instanceCpu, bool breaches)
    {
        Assert.Equal(breaches, DarlingWorker.PgCpuSampleBreaches(capacity, instanceCpu, 90));
    }
}
