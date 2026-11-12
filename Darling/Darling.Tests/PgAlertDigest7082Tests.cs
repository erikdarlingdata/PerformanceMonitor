/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7082, the held-back (digest) parts: a PostgreSQL deadlock fire pages on a new statement pair or at 5 reports in the
/// hour and is held otherwise, capacity at the ceiling with low instance CPU writes a held High CPU row and never
/// pages, a Poison Wait Warning the two-window hold keeps back writes one held row per episode, and a sample with no
/// capacity figure breaches High CPU on instance CPU alone only on a target known to be provisioned.
/// </summary>
public sealed class PgAlertDigest7082Tests
{
    /* ---------------- deadlocks: the statement-pair key ---------------- */

    private static string Graph(string first, string second) =>
        "Process 3101 waits for ShareLock on transaction 5501; blocked by process 3102.\n"
        + "Process 3102 waits for ShareLock on transaction 5500; blocked by process 3101.\n"
        + $"Process 3101: {first}\n"
        + $"Process 3102: {second}";

    private const string UpdateOrders = "UPDATE orders SET qty = ? WHERE id = ?";
    private const string UpdateStock = "UPDATE stock SET qty = ? WHERE id = ?";

    [Fact]
    public void TheSamePairWithTheRolesSwappedAndOtherPids_IsOnePattern()
    {
        var a = PgDeadlockSignature.For(Graph(UpdateOrders, UpdateStock), 2, "ShareLock", "transaction");
        var b = PgDeadlockSignature.For(Graph(UpdateStock, UpdateOrders).Replace("3101", "9001").Replace("3102", "9002"), 2, "ShareLock", "transaction");

        Assert.Equal(a.Value, b.Value);
        Assert.StartsWith("pg:", a.Value, StringComparison.Ordinal);
        Assert.Equal(2, a.Statements.Count);
    }

    [Fact]
    public void ADifferentPair_IsADifferentPattern()
    {
        var known = PgDeadlockSignature.For(Graph(UpdateOrders, UpdateStock), 2, null, null);
        var other = PgDeadlockSignature.For(Graph(UpdateOrders, "UPDATE ledger SET qty = ? WHERE id = ?"), 2, null, null);

        Assert.NotEqual(known.Value, other.Value);
    }

    [Fact]
    public void AReportWithNoStatements_FallsBackToTheLockShape()
    {
        var one = PgDeadlockSignature.For(null, 2, "ShareLock", "transaction 5500");
        var same = PgDeadlockSignature.For("", 2, "ShareLock", "transaction 5500");
        var other = PgDeadlockSignature.For(null, 3, "ShareLock", "transaction 5500");

        Assert.Empty(one.Statements);
        Assert.Equal(one.Value, same.Value);
        Assert.NotEqual(one.Value, other.Value);
        Assert.StartsWith("pg:", one.Value, StringComparison.Ordinal);
    }

    /* ---------------- deadlocks: page or hold ---------------- */

    [Fact]
    public void ANewStatementPair_Pages()
    {
        Assert.True(DarlingWorker.PgDeadlockPagesNow(distinctReportsInHour: 1, unannouncedNewPairs: 1, storeOk: true));
    }

    [Fact]
    public void TheSamePairThreeTimesInAnHour_IsHeld()
    {
        Assert.False(DarlingWorker.PgDeadlockPagesNow(distinctReportsInHour: 3, unannouncedNewPairs: 0, storeOk: true));
        Assert.False(DarlingWorker.PgDeadlockPagesNow(distinctReportsInHour: 4, unannouncedNewPairs: 0, storeOk: true));
    }

    [Fact]
    public void FiveReportsInAnHour_PageWhateverThePair()
    {
        Assert.True(DarlingWorker.PgDeadlockPagesNow(distinctReportsInHour: 5, unannouncedNewPairs: 0, storeOk: true));
    }

    [Fact]
    public void AStoreThatCannotBeRead_Pages()
    {
        Assert.True(DarlingWorker.PgDeadlockPagesNow(distinctReportsInHour: 1, unannouncedNewPairs: 0, storeOk: false));
    }

    [Fact]
    public void TheDeadlockDetailText_NamesThePairAndItsReportCount()
    {
        var key = PgDeadlockSignature.For(Graph(UpdateOrders, UpdateStock), 2, null, null);
        var rows = new List<DarlingPgDeadlockReader.PgDeadlockRow>();
        var patterns = new DarlingWorker.PgDeadlockPatterns();
        for (var i = 0; i < 3; i++)
        {
            rows.Add(new DarlingPgDeadlockReader.PgDeadlockRow(
                DateTime.UtcNow, 3101, 2, "H" + i, null, null, UpdateOrders, 1, Graph(UpdateOrders, UpdateStock)));
            patterns.ByReport["H" + i] = key;
        }

        var text = DarlingWorker.BuildPgDeadlockDetailText(rows, patterns);

        Assert.NotNull(text);
        Assert.Contains("(3 reports)", text, StringComparison.Ordinal);
        Assert.Contains(UpdateOrders, text, StringComparison.Ordinal);
        Assert.Contains(UpdateStock, text, StringComparison.Ordinal);
        Assert.Null(DarlingWorker.BuildPgDeadlockDetailText(new List<DarlingPgDeadlockReader.PgDeadlockRow>(), patterns));
    }

    /* ---------------- High CPU: capacity at the ceiling, low instance CPU ---------------- */

    [Fact]
    public void CapacityAtTheCeilingWithInstanceCpuAtFivePercent_WritesOneHeldRowPerCooldown_AndNeverPages()
    {
        var cooldown = TimeSpan.FromMinutes(15);
        var start = new DateTime(2026, 11, 1, 3, 0, 0, DateTimeKind.Utc);
        DateTime? lastHeld = null;
        var heldRows = 0;
        var state = PersistenceState.Initial;
        var fires = 0;

        for (var minute = 0; minute < 40; minute++)
        {
            var now = start.AddMinutes(minute);
            if (DarlingWorker.PgCpuAtCeilingWithLowCpu(100, 5, 90) && DarlingWorker.PgCpuHeldRowDue(lastHeld, now, cooldown))
            {
                lastHeld = now;
                heldRows++;
            }

            var evaluation = AlertPersistenceGate.Evaluate(
                state, DarlingWorker.PgCpuSampleBreaches(100, 5, 90), AlertEngine.CpuBreachSamples, AlertEngine.CpuClearSamples);
            state = evaluation.State;
            fires += evaluation.Outcome == PersistenceOutcome.Fire ? 1 : 0;
        }

        Assert.Equal(0, fires);
        Assert.Equal(3, heldRows);
    }

    [Theory]
    [InlineData(100, 5, true)]
    [InlineData(90, 89.9, true)]
    [InlineData(100, 90, false)]
    [InlineData(89, 5, false)]
    public void OnlyCapacityAtTheBarWithInstanceCpuUnderIt_IsHeld(double capacity, double instanceCpu, bool held)
    {
        Assert.Equal(held, DarlingWorker.PgCpuAtCeilingWithLowCpu(capacity, instanceCpu, 90));
    }

    [Fact]
    public void ASampleWithNoCapacityFigure_IsNeverAHeldCeilingRow()
    {
        Assert.False(DarlingWorker.PgCpuAtCeilingWithLowCpu(null, 5, 90));
    }

    /* ---------------- High CPU: provisioned instances carry no capacity figure ---------------- */

    [Fact]
    public void AProvisionedInstanceWithNoCapacityFigure_PagesOnInstanceCpuAlone()
    {
        var state = PersistenceState.Initial;
        var outcome = PersistenceOutcome.None;
        for (var i = 0; i < AlertEngine.CpuBreachSamples && outcome != PersistenceOutcome.Fire; i++)
        {
            var evaluation = AlertPersistenceGate.Evaluate(
                state, DarlingWorker.PgCpuGateInput(null, 97, 90, knownProvisioned: true) is true,
                AlertEngine.CpuBreachSamples, AlertEngine.CpuClearSamples);
            state = evaluation.State;
            outcome = evaluation.Outcome;
        }

        Assert.Equal(PersistenceOutcome.Fire, outcome);
        Assert.True(DarlingWorker.PgCpuSampleBreaches(null, 97, 90, knownProvisioned: true));
    }

    /* #7082 review: a serverless instance has minutes Performance Insights returned a CPU point and no ACU point for. At
       100% of the allocated capacity such a minute is the routine scale-up trigger, not saturation (#3281), so it must
       not count toward a page, and it must not count as a clear either. */
    [Fact]
    public void AServerlessMinuteWithNoAcuPoint_AtFullInstanceCpu_DoesNotBreach()
    {
        Assert.False(DarlingWorker.PgCpuSampleBreaches(null, 100, 90));
        Assert.False(DarlingWorker.PgCpuSampleBreaches(null, 100, 90, knownProvisioned: false));
    }

    [Fact]
    public void ServerlessMinutesWithNoAcuPoint_AtFullInstanceCpu_NeverFillTheGate_AndNeitherBreachNorClear()
    {
        for (var i = 0; i < 10; i++)
        {
            Assert.Null(DarlingWorker.PgCpuGateInput(null, 100, 90, knownProvisioned: false));
            Assert.Null(DarlingWorker.PgCpuGateInput(null, 5, 90, knownProvisioned: false));
        }
    }

    [Fact]
    public void AProvisionedInstanceMinuteWithNoFigure_BreachesOrClearsOnInstanceCpu()
    {
        Assert.True(DarlingWorker.PgCpuGateInput(null, 95, 90, knownProvisioned: true));
        Assert.False(DarlingWorker.PgCpuGateInput(null, 20, 90, knownProvisioned: true));
        Assert.True(DarlingWorker.PgCpuGateInput(92, 100, 90, knownProvisioned: false));
    }

    [Fact]
    public void AProvisionedInstanceUnderTheBar_DoesNotBreach()
    {
        Assert.False(DarlingWorker.PgCpuSampleBreaches(null, 60, 90, knownProvisioned: true));
    }

    private static DarlingPgCpuUtilizationReader.CpuSample CpuSampleAt(double? acuPercent) =>
        new(new DateTime(2026, 11, 1, 2, 0, 0, DateTimeKind.Utc), 97, acuPercent, acuPercent.HasValue ? 4 : null, acuPercent.HasValue ? 12 : null, Memory: null);

    /* Review follow-up: the provisioned-instance history read happens once, ahead of the loop over the batch, and only when
       a sample in it has no capacity figure. A read that throws throws there, before any sample has moved the gate. */
    [Fact]
    public async Task TheHistoryRead_IsMadeOnce_OnlyWhenASampleHasNoCapacityFigure()
    {
        var reads = 0;
        Task<bool> Read() { reads++; return Task.FromResult(true); }

        Assert.Null(await DarlingWorker.PgCpuBatchKnownProvisionedAsync([CpuSampleAt(40), CpuSampleAt(95)], Read));
        Assert.Equal(0, reads);

        Assert.Null(await DarlingWorker.PgCpuBatchKnownProvisionedAsync([], Read));
        Assert.Equal(0, reads);

        Assert.True(await DarlingWorker.PgCpuBatchKnownProvisionedAsync([CpuSampleAt(40), CpuSampleAt(null), CpuSampleAt(null)], Read));
        Assert.Equal(1, reads);
    }

    [Fact]
    public async Task AFailedHistoryRead_Throws_BeforeAnySampleIsHandedToTheGate()
    {
        var gateInputs = 0;
        var batch = new List<DarlingPgCpuUtilizationReader.CpuSample> { CpuSampleAt(95), CpuSampleAt(95), CpuSampleAt(null) };

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            /* The worker awaits this ahead of its loop, so a throw here leaves no sample counted. */
            await DarlingWorker.PgCpuBatchKnownProvisionedAsync(
                batch, () => throw new InvalidOperationException("history read failed"));
            gateInputs++;
        });

        Assert.Equal(0, gateInputs);
    }

    [Fact]
    public void ASampleWithACapacityFigure_StillNeedsBothFigures()
    {
        Assert.False(DarlingWorker.PgCpuSampleBreaches(100, 5, 90));
        Assert.True(DarlingWorker.PgCpuSampleBreaches(92, 100, 90));
    }

    /* ---------------- Poison Wait: one held row per Warning episode ---------------- */

    private const double BackendsPerMinuteMs = 60_000;

    /* An episode of `backends` average waiting backends from minute 0 to `endMinute`, read at minute t the way the
       collector's windows read it: the 10 minutes ending at t, and the 10 minutes before them. */
    private static PostgresPoisonWaitAlertInfo EpisodeAt(double backends, double endMinute, double t)
    {
        static double Overlap(double from, double to, double endMinute) =>
            Math.Max(0, Math.Min(to, endMinute) - Math.Max(from, 0));

        var current = Overlap(t - 10, t, endMinute) * backends * BackendsPerMinuteMs;
        var prior = Overlap(t - 20, t - 10, endMinute) * backends * BackendsPerMinuteMs;
        return new PostgresPoisonWaitAlertInfo(
            "IPC", "BtreePage", (long)current, 10_000, new DateTime(2026, 11, 1, 2, 0, 0).AddMinutes(t), (long)prior);
    }

    private static (int HeldRows, int PagingSweeps) Replay(double backends, double endMinute)
    {
        var episodes = new ConcurrentDictionary<string, bool>(StringComparer.Ordinal);
        var heldRows = 0;
        var pagingSweeps = 0;
        for (var t = 1; t <= 60; t++)
        {
            var row = EpisodeAt(backends, endMinute, t);
            var paging = new HashSet<string>(StringComparer.Ordinal);
            if (PostgresAlertEvaluator.EvaluatePoisonWait(row) is { } finding)
            {
                paging.Add(finding.Subject);
                pagingSweeps++;
            }

            heldRows += DarlingWorker.TakeHeldPoisonWarnings(new[] { row }, "srv", PostgresAlertEvaluator.PoisonWaitMetric, episodes, paging).Count;
        }

        return (heldRows, pagingSweeps);
    }

    [Fact]
    public void ATwelveMinuteWarningEpisode_WritesOneHeldRow_AndNeverPages()
    {
        var (heldRows, pagingSweeps) = Replay(backends: 1.5, endMinute: 12);

        Assert.Equal(1, heldRows);
        Assert.Equal(0, pagingSweeps);
    }

    [Fact]
    public void ATwentyFiveMinuteWarningEpisode_Pages_AndWritesOneHeldRowBeforeIt()
    {
        var (heldRows, pagingSweeps) = Replay(backends: 1.5, endMinute: 25);

        Assert.True(pagingSweeps > 0);
        Assert.Equal(1, heldRows);
    }

    [Fact]
    public void ASecondEpisodeWritesItsOwnHeldRow()
    {
        var episodes = new ConcurrentDictionary<string, bool>(StringComparer.Ordinal);
        var none = new HashSet<string>(StringComparer.Ordinal);
        var rows = 0;

        foreach (var (backends, end, t) in new[] { (1.5, 12.0, 7.0), (1.5, 12.0, 9.0), (1.5, 12.0, 40.0), (1.5, 200.0, 7.0), (1.5, 200.0, 9.0) })
        {
            rows += DarlingWorker.TakeHeldPoisonWarnings(
                new[] { EpisodeAt(backends, end, t) }, "srv", PostgresAlertEvaluator.PoisonWaitMetric, episodes, none).Count;
        }

        Assert.Equal(2, rows);
    }

    [Fact]
    public void ACriticalWarning_OrAPagingOne_IsNotHeld()
    {
        Assert.Null(PostgresAlertEvaluator.EvaluateHeldPoisonWait(EpisodeAt(12, 100, 10)));
        Assert.Null(PostgresAlertEvaluator.EvaluateHeldPoisonWait(EpisodeAt(1.5, 100, 40)));
        Assert.Null(PostgresAlertEvaluator.EvaluateHeldPoisonWait(EpisodeAt(0.5, 100, 5)));
    }
}
