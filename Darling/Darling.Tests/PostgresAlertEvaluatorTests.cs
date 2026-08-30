/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the PostgreSQL alert thresholds. These fire pages, so the tests are about the boundaries and the
/// cases that must NOT fire — a predictor that cries wolf gets muted, and a muted outage predictor is worse
/// than none.
/// </summary>
public class PostgresAlertEvaluatorTests
{
    private const long StockFreezeMaxAge = 200_000_000;

    /// <summary>PostgreSQL's own default for the MultiXact counter — TWICE the XID default, which is the
    /// whole reason grading one against the other's setting was wrong.</summary>
    private const long StockMultixactFreezeMaxAge = 400_000_000;

    private static PostgresWraparoundAlertInfo Wrap(
        long xid,
        long multi = 0,
        long freezeMax = StockFreezeMaxAge,
        long multixactFreezeMax = StockMultixactFreezeMaxAge,
        long windowPeakXid = 0,
        long windowPeakMulti = 0)
        => new("appdb", xid, multi, freezeMax, multixactFreezeMax, windowPeakXid, windowPeakMulti);

    /// <summary>
    /// Thresholds scale to the SERVER's own autovacuum_freeze_max_age, not to a constant. A cluster tuned to
    /// 1.5 billion is in a different place at 400 million than a stock one, and a fixed threshold would
    /// either never fire for the first or constantly for the second.
    /// </summary>
    [Fact]
    public void WraparoundThresholdsScaleToTheServersOwnFreezeMaxAge()
    {
        /* 400M is CRITICAL on a stock 200M server (2x)... */
        Assert.Equal(
            AlertSeverityLevel.Critical,
            PostgresAlertEvaluator.EvaluateWraparound(Wrap(400_000_000))!.Severity);

        /* ...and does not fire at all on one tuned to 1.5 billion, where 90% of the setting is 1.35B. */
        Assert.Null(PostgresAlertEvaluator.EvaluateWraparound(Wrap(400_000_000, freezeMax: 1_500_000_000)));
    }

    /// <summary>
    /// #2689: the relative Warning arm moved from 90% of freeze_max_age to 100% (the crossing point itself,
    /// where autovacuum forces the vacuum) - these boundaries use the default FreezingIsKeepingUp=false
    /// (no window peak supplied), the conservative "not recovering" case, so the relative arm is reachable
    /// at all. <see cref="WarningIsSuppressedWhileTheCounterIsComingBackDownFromItsPeak"/> covers the gate.
    /// </summary>
    [Theory]
    [InlineData(199_999_999, null)]              // just under 100% of 200M
    [InlineData(200_000_000, "Warning")]         // exactly at the setting - the forced-vacuum crossing point
    [InlineData(399_999_999, "Warning")]         // just under 2x
    [InlineData(400_000_000, "Critical")]        // exactly 2x
    [InlineData(1_900_000_000, "Critical")]
    public void WraparoundGradesAtTheDocumentedBoundaries(long age, string? expected)
    {
        var finding = PostgresAlertEvaluator.EvaluateWraparound(Wrap(age));

        if (expected is null)
        {
            Assert.Null(finding);
            return;
        }

        Assert.Equal(Enum.Parse<AlertSeverityLevel>(expected), finding!.Severity);
    }

    /// <summary>
    /// #2689: the whole point of the fix. A database sitting AT or ABOVE its own forced-vacuum crossing
    /// point is the routine case if autovacuum is bringing it back down every cycle - the healthy sawtooth,
    /// not a risk. The old unconditional 90% arm could not tell this from a stuck climb and paged on every
    /// healthy database.
    /// </summary>
    [Fact]
    public void WarningIsSuppressedWhileTheCounterIsComingBackDownFromItsPeak()
    {
        /* Age is past the crossing point (200M), but the window saw it higher (250M) - it HAS come down. */
        var finding = PostgresAlertEvaluator.EvaluateWraparound(Wrap(200_000_000, windowPeakXid: 250_000_000));

        Assert.Null(finding);
    }

    /// <summary>
    /// The other half of the gate: past the crossing point and the window has NEVER seen it lower - autovacuum
    /// is not winning the race, which is the actual risk #2689 asks this alert to signal.
    /// </summary>
    [Fact]
    public void WarningFiresWhenTheCounterHasNeverComeBackDownInTheWindow()
    {
        /* The current reading IS the window's peak - it has only ever climbed. */
        var finding = PostgresAlertEvaluator.EvaluateWraparound(Wrap(200_000_000, windowPeakXid: 200_000_000));

        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
        Assert.Contains("not recovering", finding.ThresholdValue, StringComparison.Ordinal);
    }

    /// <summary>
    /// The absolute Warning arm, mirroring <see cref="CriticalIsReachableOnAClusterTunedPastHalfTheWall"/> one
    /// severity down: on a cluster tuned high enough, 1.0x setting sits at or past the Critical ceiling arm,
    /// so the relative Warning arm is unreachable there too. Without the absolute floor at 50% of the true
    /// wraparound space, that cluster would jump straight from silent to Critical with no early warning.
    /// </summary>
    [Fact]
    public void WarningIsReachableOnAClusterTunedPastHalfTheWall()
    {
        const long Tuned = 2_200_000_000;

        /* Relative Warning would be 2.2B - at or past the 2^31 (2,147,483,648) wall, unreachable. */
        Assert.True(Tuned >= PostgresAlertEvaluator.WraparoundCeiling);

        /* 1.1B is past 50% of the space (~1.07B) but short of Critical's 74.5% (~1.6B) and short of the
           2x-tuned relative Critical (4B) - only the absolute Warning arm can fire here. */
        var finding = PostgresAlertEvaluator.EvaluateWraparound(Wrap(1_100_000_000, freezeMax: Tuned));

        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
        Assert.Contains("wraparound space", finding.ThresholdValue, StringComparison.Ordinal);
    }

    /// <summary>
    /// MultiXact exhaustion stops writes exactly as XID exhaustion does, and is the less familiar of the
    /// two, so the worse counter has to win AND be named — the remedies differ.
    /// </summary>
    [Fact]
    public void MultiXactAgeCanBeTheWorseCounterAndIsNamedAsSuch()
    {
        var finding = PostgresAlertEvaluator.EvaluateWraparound(Wrap(1_000, multi: 500_000_000));

        Assert.NotNull(finding);
        Assert.Contains("MultiXact", finding!.CurrentValue, StringComparison.Ordinal);
    }

    /// <summary>
    /// Each counter is graded against ITS OWN setting. This assertion changed with the fix and the change is
    /// the point: 500M MultiXacts used to grade Critical because it was measured against
    /// autovacuum_freeze_max_age (200M, so 2.5x), when the governing setting is
    /// autovacuum_multixact_freeze_max_age — 400M by default, making 500M a 1.25x Warning. The old behaviour
    /// fired Critical 2.2x premature on every MultiXact-heavy workload.
    /// </summary>
    [Fact]
    public void MultiXactIsGradedAgainstItsOwnSettingNotTheXidOne()
    {
        var finding = PostgresAlertEvaluator.EvaluateWraparound(Wrap(1_000, multi: 500_000_000));

        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);

        /* And the body must quote the setting it actually judged against — it used to print
           "autovacuum_freeze_max_age N" beside a counter name saying MultiXact, contradicting itself. */
        Assert.Contains("autovacuum_multixact_freeze_max_age", finding.ThresholdValue, StringComparison.Ordinal);
        Assert.Contains("400,000,000", finding.ThresholdValue, StringComparison.Ordinal);
    }

    /// <summary>
    /// Crossing twice the MultiXact setting IS Critical — the fix moves the line, it does not remove it.
    /// </summary>
    [Fact]
    public void MultiXactGoesCriticalAtTwiceItsOwnSetting()
    {
        Assert.Equal(
            AlertSeverityLevel.Critical,
            PostgresAlertEvaluator.EvaluateWraparound(Wrap(1_000, multi: 800_000_000))!.Severity);
    }

    /// <summary>
    /// The absolute arm. On a cluster tuned near the top of the range, <c>2 x setting</c> lands BEYOND the
    /// 2^31 wall, so the relative Critical was unreachable — on exactly the clusters closest to a write
    /// outage. vacuum_failsafe_age (~74.5% of the space) is the floor that makes it reachable.
    /// </summary>
    [Fact]
    public void CriticalIsReachableOnAClusterTunedPastHalfTheWall()
    {
        const long Tuned = 1_500_000_000;

        /* Relative critical would be 3B — past the 2^31 wall, i.e. unreachable. */
        Assert.True(Tuned * 2 > PostgresAlertEvaluator.WraparoundCeiling);

        /* 1.7B is 79% of the space and past vacuum_failsafe_age, so it must be Critical anyway. */
        var finding = PostgresAlertEvaluator.EvaluateWraparound(Wrap(1_700_000_000, freezeMax: Tuned));

        Assert.Equal(AlertSeverityLevel.Critical, finding!.Severity);
        Assert.Contains("wraparound space", finding.ThresholdValue, StringComparison.Ordinal);
        Assert.Contains("vacuum_failsafe_age", finding.ShortMessage, StringComparison.Ordinal);
    }

    /// <summary>
    /// One counter having an unusable setting must not silence the other. A server can have a sane
    /// autovacuum_freeze_max_age and a nonsensical multixact one.
    /// </summary>
    [Fact]
    public void AnUnjudgeableCounterDoesNotSilenceTheJudgeableOne()
    {
        var finding = PostgresAlertEvaluator.EvaluateWraparound(
            Wrap(200_000_000, multi: 900_000_000, multixactFreezeMax: 0));

        Assert.NotNull(finding);
        Assert.Contains("XID", finding!.CurrentValue, StringComparison.Ordinal);
    }

    /// <summary>
    /// A missing or nonsensical setting makes every derived threshold zero, which would fire on every
    /// database on every sweep forever. "Cannot judge" must mean silence, not criticality.
    /// </summary>
    [Theory]
    [InlineData(0L)]
    [InlineData(-1L)]
    public void AMissingFreezeMaxAgeSettingSilencesRatherThanFiringOnEverything(long freezeMax)
    {
        /* Both settings unusable = cannot judge either counter = silence. */
        Assert.Null(PostgresAlertEvaluator.EvaluateWraparound(
            Wrap(1_000_000_000, freezeMax: freezeMax, multixactFreezeMax: freezeMax)));
    }

    /// <summary>
    /// The persistence gate is what keeps this from firing on every long-running report. Same age, same
    /// holder — only the persistence differs, and only the chronic one alerts.
    /// </summary>
    [Fact]
    public void XminFiresOnlyForAChronicHolderNotALongQuery()
    {
        var chronic = new PostgresXminHorizonAlertInfo("session", "12345", 100_000_000, 30, 40, "idle in transaction");
        var transient = new PostgresXminHorizonAlertInfo("session", "12345", 100_000_000, 2, 40, "running");

        Assert.NotNull(PostgresAlertEvaluator.EvaluateXmin(chronic));
        Assert.Null(PostgresAlertEvaluator.EvaluateXmin(transient));
    }

    [Fact]
    public void XminBelowTheAgeThresholdNeverFiresHoweverPersistent()
    {
        Assert.Null(PostgresAlertEvaluator.EvaluateXmin(
            new PostgresXminHorizonAlertInfo("session", "1", 1_000_000, 40, 40, null)));
    }

    /// <summary>A zero denominator must not divide — it means nothing was observed, so nothing fires.</summary>
    [Fact]
    public void XminWithNoObservationsDoesNotFireOrThrow()
    {
        Assert.Null(PostgresAlertEvaluator.EvaluateXmin(
            new PostgresXminHorizonAlertInfo("session", "1", 900_000_000, 0, 0, null)));
    }

    /// <summary>
    /// Each of the five causes is indistinguishable from the others by symptom and needs a different fix, so
    /// the alert body must carry that fix rather than making the reader work out which one it is.
    /// </summary>
    [Theory]
    [InlineData("session", "idle in transaction")]
    [InlineData("replication_slot", "dropped")]
    [InlineData("replication_slot_catalog", "catalog_xmin")]
    [InlineData("standby_feedback", "hot_standby_feedback")]
    [InlineData("prepared_transaction", "PREPARED")]
    public void XminMessageCarriesTheRemedyForItsCause(string source, string fragment)
    {
        var finding = PostgresAlertEvaluator.EvaluateXmin(
            new PostgresXminHorizonAlertInfo(source, "x", 100_000_000, 10, 10, null));

        Assert.Contains(fragment, finding!.ShortMessage, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// lost and unreserved are failures that have already happened, so they fire at ANY size — a byte
    /// threshold would let a small-but-broken slot pass silently.
    /// </summary>
    [Theory]
    [InlineData("lost")]
    [InlineData("unreserved")]
    public void TerminalSlotStatesFireCriticalAtAnySize(string walStatus)
    {
        var finding = PostgresAlertEvaluator.EvaluateSlot(
            new PostgresSlotAlertInfo("s1", walStatus, IsActive: true, RetainedWalBytes: 1024,
                RetainedWalGrowthBytes: 0, InactiveSince: null));

        Assert.NotNull(finding);
        Assert.Equal(AlertSeverityLevel.Critical, finding!.Severity);
    }

    /// <summary>
    /// The disk-fill emergency is the conjunction: over the line, nobody consuming, still growing. Each part
    /// alone is a lesser finding, and grading them all critical would make the critical meaningless.
    /// </summary>
    [Fact]
    public void TheOrphanFillingDiskIsCriticalButItsPartsAloneAreNot()
    {
        const long OverLine = 20L * 1024 * 1024 * 1024;

        Assert.Equal(AlertSeverityLevel.Critical, PostgresAlertEvaluator.EvaluateSlot(
            new PostgresSlotAlertInfo("s", "extended", false, OverLine, 5L * 1024 * 1024 * 1024, null))!.Severity);

        /* Active consumer: behind, but someone is draining it. */
        Assert.Equal(AlertSeverityLevel.Warning, PostgresAlertEvaluator.EvaluateSlot(
            new PostgresSlotAlertInfo("s", "extended", true, OverLine, 5L * 1024 * 1024 * 1024, null))!.Severity);

        /* Inactive but flat: a consumer between polls, not a volume filling. */
        Assert.Equal(AlertSeverityLevel.Warning, PostgresAlertEvaluator.EvaluateSlot(
            new PostgresSlotAlertInfo("s", "extended", false, OverLine, 0, null))!.Severity);
    }

    /// <summary>A healthy slot under the byte line is silent, growing or not.</summary>
    [Fact]
    public void ASlotUnderTheByteLineDoesNotFire()
    {
        Assert.Null(PostgresAlertEvaluator.EvaluateSlot(
            new PostgresSlotAlertInfo("s", "reserved", false, 1024, 512, null)));
    }

    /// <summary>The healthy fleet case: nothing fires, and an empty list is not an error.</summary>
    [Fact]
    public void AHealthyServerProducesNoFindings()
    {
        var findings = PostgresAlertEvaluator.Evaluate(
            new[] { Wrap(1_000_000) },
            new PostgresXminHorizonAlertInfo("session", "1", 1_000, 40, 40, null),
            new[] { new PostgresSlotAlertInfo("s", "reserved", true, 0, 0, null) });

        Assert.Empty(findings);
    }

    /// <summary>Nulls throughout (nothing collected yet) must be silent rather than throwing mid-sweep.</summary>
    [Fact]
    public void NoDataIsSilentRatherThanThrowing()
    {
        Assert.Empty(PostgresAlertEvaluator.Evaluate(null, null, null));
    }

    /// <summary>Worst-first, so a host that caps delivery keeps the ones that matter.</summary>
    [Fact]
    public void FindingsAreOrderedWorstFirst()
    {
        var findings = PostgresAlertEvaluator.Evaluate(
            new[] { Wrap(200_000_000) },                                   // Warning
            null,
            new[] { new PostgresSlotAlertInfo("s", "lost", false, 1, 0, null) });  // Critical

        Assert.Equal(AlertSeverityLevel.Critical, findings[0].Severity);
        Assert.True(findings.Count > 1);
    }

    /// <summary>
    /// Metric names are the key mute rules and history filtering match on, so they are part of the contract
    /// and must not drift casually.
    /// </summary>
    [Fact]
    public void MetricNamesArePinnedAndDistinct()
    {
        var names = new[]
        {
            PostgresAlertEvaluator.WraparoundMetric,
            PostgresAlertEvaluator.XminHorizonMetric,
            PostgresAlertEvaluator.SlotRetentionMetric,
        };

        Assert.Equal(names.Length, names.Distinct().Count());
        Assert.All(names, n => Assert.StartsWith("PostgreSQL ", n, StringComparison.Ordinal));
    }

    /// <summary>
    /// Every finding carries a subject, because the host builds its dedup fingerprint from it — without one,
    /// two databases breaching at once would collapse into a single alert.
    /// </summary>
    [Fact]
    public void EveryFindingIdentifiesItsSubject()
    {
        var findings = PostgresAlertEvaluator.Evaluate(
            new[] { new PostgresWraparoundAlertInfo("db_a", 500_000_000, 0, StockFreezeMaxAge, StockMultixactFreezeMaxAge),
                    new PostgresWraparoundAlertInfo("db_b", 500_000_000, 0, StockFreezeMaxAge, StockMultixactFreezeMaxAge) },
            null,
            null);

        Assert.Equal(2, findings.Count);
        Assert.Equal(2, findings.Select(f => f.Subject).Distinct().Count());
        Assert.All(findings, f => Assert.False(string.IsNullOrWhiteSpace(f.Subject)));
    }
}
