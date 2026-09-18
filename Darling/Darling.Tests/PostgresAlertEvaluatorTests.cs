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
    /// holder — only the persistence differs, and only the chronic one alerts. The chronic case is the
    /// classic single-pid incident, and it still fires through the IDENTITY arm with the holder as the
    /// subject and the original "in N of M observations" wording — truthful now that the floor guarantees
    /// M is a real sample.
    /// </summary>
    [Fact]
    public void XminFiresOnlyForAChronicHolderNotALongQuery()
    {
        var chronic = new PostgresXminHorizonAlertInfo("session", "12345", 100_000_000, 30, 40, "idle in transaction");
        var transient = new PostgresXminHorizonAlertInfo("session", "12345", 100_000_000, 2, 40, "running");

        var finding = PostgresAlertEvaluator.EvaluateXmin(chronic);
        Assert.NotNull(finding);
        Assert.Equal("session:12345", finding!.Subject);
        Assert.Contains("in 30 of 40 observations", finding.ShortMessage, StringComparison.Ordinal);

        Assert.Null(PostgresAlertEvaluator.EvaluateXmin(transient));
    }

    /// <summary>
    /// #3537 edge 1: the identity denominator counts only holder-bearing collections, so the first holder
    /// after quiet hours arrived as 1 win in 1 observation — 100%, "chronic", off a single sample. The
    /// observation floor closes every denominator too small for its majority to mean anything.
    /// </summary>
    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 2)]
    [InlineData(4, 4)]
    [InlineData(3, 4)]
    public void XminDoesNotFireBeneathTheObservationFloorHoweverTotalTheFraction(int held, int total)
    {
        Assert.Null(PostgresAlertEvaluator.EvaluateXmin(
            new PostgresXminHorizonAlertInfo("session", "1", 900_000_000, held, total, null)));
    }

    /// <summary>The floor is a floor, not a fudge: at exactly the minimum, a majority still fires.</summary>
    [Fact]
    public void XminIdentityArmFiresAtExactlyTheObservationFloor()
    {
        Assert.NotNull(PostgresAlertEvaluator.EvaluateXmin(
            new PostgresXminHorizonAlertInfo(
                "session", "1", 900_000_000,
                PostgresAlertEvaluator.XminMinimumObservations,
                PostgresAlertEvaluator.XminMinimumObservations,
                null)));
    }

    /// <summary>
    /// #3537 edge 2: a horizon continuously pinned past the threshold by a PARADE of distinct holders never
    /// accumulates any single holder's identity fraction, and the old gate never fired while the alert's own
    /// claim was true the whole time. The horizon arm fires on the horizon's persistence across the window's
    /// real captures, names the rotating pattern, still carries the latest holder's remedy — and subjects
    /// the stable sentinel, not the latest member, so the host's per-subject cooldown holds across
    /// rotations.
    /// </summary>
    [Fact]
    public void XminRotatingHoldersFireTheHorizonArmUnderTheStableSubject()
    {
        var finding = PostgresAlertEvaluator.EvaluateXmin(new PostgresXminHorizonAlertInfo(
            "session", "9101", 80_000_000, ObservationsHeld: 1, ObservationsTotal: 60,
            "state=idle in transaction", ObservationsAboveThreshold: 70, CapturesInWindow: 120));

        Assert.NotNull(finding);
        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
        Assert.Equal(PostgresAlertEvaluator.XminRotatingHoldersSubject, finding.Subject);
        Assert.Contains("succession of different holders", finding.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("session:9101", finding.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("70 of the window's 120 collections", finding.ShortMessage, StringComparison.Ordinal);
        /* The remedy names the latest holder's cause — the one actionable thing either way. */
        Assert.Contains("idle in transaction", finding.ShortMessage, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// The horizon arm's own boundaries: a majority of the window's real captures fires, one capture short
    /// of it does not, and a window with fewer captures than the floor cannot fire at any fraction — which
    /// is also what keeps the compat default (no capture data supplied) silent.
    /// </summary>
    [Theory]
    [InlineData(60, 120, true)]   // exactly the majority
    [InlineData(59, 120, false)]  // one capture short
    [InlineData(4, 4, false)]     // 100%, but beneath the capture floor
    [InlineData(0, 0, false)]     // no capture data supplied — the compat default
    public void XminHorizonArmNeedsAMajorityOfAtLeastTheFloorsWorthOfCaptures(
        int above, int captures, bool fires)
    {
        var finding = PostgresAlertEvaluator.EvaluateXmin(new PostgresXminHorizonAlertInfo(
            "session", "1", 80_000_000, ObservationsHeld: 1, ObservationsTotal: 60, null,
            ObservationsAboveThreshold: above, CapturesInWindow: captures));

        Assert.Equal(fires, finding is not null);
    }

    /// <summary>
    /// The overlap case, told apart by the identity FRACTION alone: a service restarted into an incident
    /// already underway sees a stable holder through a window still too young for the identity floor. The
    /// horizon arm supplies the persistence evidence, but the wording must not claim a "succession" and
    /// the subject stays the holder — there is exactly one, and it is the thing to kill.
    /// </summary>
    [Fact]
    public void XminStableHolderInAYoungWindowKeepsTheHolderSubjectOnAHorizonArmFire()
    {
        var finding = PostgresAlertEvaluator.EvaluateXmin(new PostgresXminHorizonAlertInfo(
            "session", "77", 80_000_000, ObservationsHeld: 3, ObservationsTotal: 3, null,
            ObservationsAboveThreshold: 3, CapturesInWindow: 6));

        Assert.NotNull(finding);
        Assert.Equal("session:77", finding!.Subject);
        Assert.DoesNotContain("succession", finding.ShortMessage, StringComparison.Ordinal);
        Assert.Contains("3 of the window's 6 collections", finding.ShortMessage, StringComparison.Ordinal);
    }

    [Fact]
    public void XminBelowTheAgeThresholdNeverFiresHoweverPersistent()
    {
        Assert.Null(PostgresAlertEvaluator.EvaluateXmin(
            new PostgresXminHorizonAlertInfo("session", "1", 1_000_000, 40, 40, null)));

        /* Both arms saturated, current age below the bar: the age gate answers first. The latest reading
           is what the alert would quote as "current", so a horizon that has already come back under the
           threshold must not page off its own history. */
        Assert.Null(PostgresAlertEvaluator.EvaluateXmin(
            new PostgresXminHorizonAlertInfo("session", "1", 1_000_000, 40, 40, null,
                ObservationsAboveThreshold: 120, CapturesInWindow: 120)));
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

    /* ---------------- poison waits (#2711) ---------------- */

    private static PostgresPoisonWaitAlertInfo Poison(
        long accumulatedMs,
        string waitEvent = "BtreePage",
        long waits = 100_000,
        string waitType = "IPC")
        => new(waitType, waitEvent, accumulatedMs, waits, new DateTime(2026, 8, 31, 0, 0, 0));

    /// <summary>
    /// The defining departure from the SQL Server shape, straight from the #2711 fleet research: the
    /// Postgres poison events average 1-2 ms per wait at six-figure volumes, so an avg-ms-per-wait bar
    /// (SQL Server's PoisonWaitThresholdMs, default 500) would NEVER see them. Accumulated time is what
    /// identifies the poison state — 600 seconds of wait across 300,000 two-millisecond waits is one
    /// backend continuously stuck for the whole window, and it must fire despite a 2 ms per-wait average.
    /// </summary>
    [Fact]
    public void PoisonWaitFiresOnAccumulatedTimeNotPerWaitAverage()
    {
        var finding = PostgresAlertEvaluator.EvaluatePoisonWait(Poison(600_000, waits: 300_000));

        Assert.NotNull(finding);
        Assert.Equal(AlertSeverityLevel.Warning, finding!.Severity);
        Assert.Equal("IPC:BtreePage", finding.Subject);
    }

    /// <summary>
    /// The boundaries: Warning at an average of one backend continuously stuck across the window
    /// (600,000 ms over 10 minutes), Critical at ten. Exactly-at fires; one below does not.
    /// </summary>
    [Theory]
    [InlineData(599_999, null)]
    [InlineData(600_000, "Warning")]
    [InlineData(5_999_999, "Warning")]
    [InlineData(6_000_000, "Critical")]
    public void PoisonWaitGradesAtTheDocumentedBoundaries(long accumulatedMs, string? expected)
    {
        var finding = PostgresAlertEvaluator.EvaluatePoisonWait(Poison(accumulatedMs));

        if (expected is null)
        {
            Assert.Null(finding);
            return;
        }

        Assert.Equal(Enum.Parse<AlertSeverityLevel>(expected), finding!.Severity);
    }

    /// <summary>
    /// The fleet-quiet pin. The WORST server in the #2711 research (segments-multitenant) accumulated
    /// 538,850 ms of IPC:BtreePage over 24 hours — a 10-minute share of ~3,742 ms — and even a burst
    /// packing that entire day's wait into a single hour (~89,808 ms per 10 minutes) must stay silent.
    /// Nothing measured on the fleet to date may fire this alert; it exists for a categorically worse
    /// state, the same near-zero-baseline trait that defines the SQL Server poison set.
    /// </summary>
    [Theory]
    [InlineData(3_742)]
    [InlineData(89_808)]
    public void PoisonWaitStaysSilentOnTheWorstFleetBaselineObserved(long accumulatedMs)
    {
        Assert.Null(PostgresAlertEvaluator.EvaluatePoisonWait(Poison(accumulatedMs, waits: 250_000)));
    }

    /// <summary>
    /// Per event, not summed across the poison set: BtreePage and BufferIo are different incidents with
    /// different remedies, so one over the bar fires alone and one under it cannot ride along — and the
    /// host's per-subject cooldown (#1140) depends on each being its own finding.
    /// </summary>
    [Fact]
    public void PoisonWaitEventsAreJudgedIndependently()
    {
        var findings = PostgresAlertEvaluator.EvaluatePoisonWaits(new[]
        {
            Poison(700_000, waitEvent: "BtreePage"),
            Poison(500_000, waitEvent: "BufferIo"),
        });

        var finding = Assert.Single(findings);
        Assert.Equal("IPC:BtreePage", finding.Subject);
    }

    /// <summary>Worst-first, the same contract as <see cref="FindingsAreOrderedWorstFirst"/>.</summary>
    [Fact]
    public void PoisonWaitFindingsAreOrderedWorstFirst()
    {
        var findings = PostgresAlertEvaluator.EvaluatePoisonWaits(new[]
        {
            Poison(600_000, waitEvent: "BtreePage"),      // Warning
            Poison(6_000_000, waitEvent: "BufferIo"),     // Critical
        });

        Assert.Equal(2, findings.Count);
        Assert.Equal(AlertSeverityLevel.Critical, findings[0].Severity);
        Assert.Equal("IPC:BufferIo", findings[0].Subject);
    }

    /// <summary>Null and empty are both the healthy silence — and the only possible answer on a
    /// non-Aurora target, where the cumulative wait counters do not exist.</summary>
    [Fact]
    public void PoisonWaitNoDataIsSilentRatherThanThrowing()
    {
        Assert.Empty(PostgresAlertEvaluator.EvaluatePoisonWaits(null));
        Assert.Empty(PostgresAlertEvaluator.EvaluatePoisonWaits(Array.Empty<PostgresPoisonWaitAlertInfo>()));
    }

    /// <summary>
    /// Deliberately the EXACT SQL Server metric string, NOT "PostgreSQL "-prefixed like the Tier 0 trio —
    /// the #2711 Deadlocks/Blocking parity reasoning: mute rules, history filters and the shared
    /// PoisonWaitEnabled switch are engine-agnostic. Pinned separately from
    /// <see cref="MetricNamesArePinnedAndDistinct"/> because that test's prefix assertion is exactly the
    /// convention this name must not follow.
    /// </summary>
    [Fact]
    public void PoisonWaitMetricIsTheSqlServerParityString()
    {
        Assert.Equal("Poison Wait", PostgresAlertEvaluator.PoisonWaitMetric);
        Assert.DoesNotContain(PostgresAlertEvaluator.PoisonWaitMetric, new[]
        {
            PostgresAlertEvaluator.WraparoundMetric,
            PostgresAlertEvaluator.XminHorizonMetric,
            PostgresAlertEvaluator.SlotRetentionMetric,
        });
    }

    /// <summary>
    /// The two events need completely different fixes, so the message carries the right one — matched
    /// case-insensitively because wait-event name casing differs between Aurora majors (the same trap
    /// the collector documents for AutoVacuumMain/AutovacuumMain).
    /// </summary>
    [Theory]
    [InlineData("BtreePage", "index")]
    [InlineData("BTREEPAGE", "index")]
    [InlineData("BufferIo", "in-flight page reads")]
    [InlineData("bufferio", "in-flight page reads")]
    [InlineData("SomethingElse", "pg_stat_activity")]
    public void PoisonWaitMessageCarriesTheRemedyForItsEvent(string waitEvent, string fragment)
    {
        Assert.Contains(fragment, PostgresAlertEvaluator.PoisonWaitRemedyFor(waitEvent), StringComparison.Ordinal);
    }

    /// <summary>
    /// One subject definition for evaluator and host: the host matches read rows back to findings by this
    /// string for the #2704 collection-time guard, so a drifted twin would silently disconnect the guard.
    /// </summary>
    [Fact]
    public void PoisonWaitSubjectIsTheTypeColonEventPairInStoredCasing()
    {
        var row = Poison(700_000);

        Assert.Equal("IPC:BtreePage", PostgresAlertEvaluator.PoisonWaitSubject(row));
        Assert.Equal(
            PostgresAlertEvaluator.PoisonWaitSubject(row),
            PostgresAlertEvaluator.EvaluatePoisonWait(row)!.Subject);
    }

    /* ---------------- poison waits: the SQL Server port (#3539 A4) ---------------- */

    private static PoisonWaitAccumulation SqlPoison(long accumulatedMs, string waitType = "THREADPOOL", long waits = 1)
        => new(waitType, accumulatedMs, waits, 10, new DateTime(2026, 9, 18, 12, 0, 0));

    /// <summary>
    /// The constants are LITERALLY shared — one definition on <see cref="PoisonWaitEvaluator"/>, with the
    /// PostgreSQL names as aliases of it in source, not merely three numbers that happen to agree today. The
    /// value equality is the cheap half; the source pin is the load-bearing one, because two equal literals
    /// are exactly the state that drifts (a future "tune the SQL Server bar" edit would leave the PostgreSQL
    /// one behind, and one alert name under one mute key would mean two things again — the #3539 finding).
    /// </summary>
    [Fact]
    public void PoisonWaitConstantsAreOneDefinition_SharedByBothEngines()
    {
        Assert.Equal(PoisonWaitEvaluator.WindowMinutes, PostgresAlertEvaluator.PoisonWaitWindowMinutes);
        Assert.Equal(PoisonWaitEvaluator.WarningAvgWaiters, PostgresAlertEvaluator.PoisonWaitWarningAvgWaiters);
        Assert.Equal(PoisonWaitEvaluator.CriticalAvgWaiters, PostgresAlertEvaluator.PoisonWaitCriticalAvgWaiters);

        var source = RepoFile.ReadRepoFile("PerformanceMonitor.Alerting", "PostgresAlertEvaluator.cs");
        Assert.Contains("public const int PoisonWaitWindowMinutes = PoisonWaitEvaluator.WindowMinutes;", source, StringComparison.Ordinal);
        Assert.Contains("public const double PoisonWaitWarningAvgWaiters = PoisonWaitEvaluator.WarningAvgWaiters;", source, StringComparison.Ordinal);
        Assert.Contains("public const double PoisonWaitCriticalAvgWaiters = PoisonWaitEvaluator.CriticalAvgWaiters;", source, StringComparison.Ordinal);
        /* The positive control for the pin below: the same Contains form does find a literal initializer
           that IS in the file (the metric name), so its silence on a "= 1.0" for the poison bar is a real
           absence rather than a matcher that never matches. */
        Assert.Contains("public const string PoisonWaitMetric = \"Poison Wait\";", source, StringComparison.Ordinal);
        Assert.DoesNotContain("PoisonWaitWarningAvgWaiters = 1.0", source, StringComparison.Ordinal);

        /* And the figures themselves, as documented on the shared home: 10 minutes, one waiter, ten. */
        Assert.Equal(10, PoisonWaitEvaluator.WindowMinutes);
        Assert.Equal(600_000d, PoisonWaitEvaluator.WindowMs);
        Assert.Equal(1.0, PoisonWaitEvaluator.WarningAvgWaiters);
        Assert.Equal(10.0, PoisonWaitEvaluator.CriticalAvgWaiters);
    }

    /// <summary>
    /// Both engines grade the same accumulated milliseconds to the same tier at every documented boundary —
    /// the parity the shared constants promise, checked through both evaluators' own entry points rather
    /// than through the constants alone.
    /// </summary>
    [Theory]
    [InlineData(0, null)]
    [InlineData(599_999, null)]
    [InlineData(600_000, "Warning")]
    [InlineData(5_999_999, "Warning")]
    [InlineData(6_000_000, "Critical")]
    public void PoisonWaitGradesIdenticallyOnBothEngines(long accumulatedMs, string? expected)
    {
        var pg = PostgresAlertEvaluator.EvaluatePoisonWait(Poison(accumulatedMs));
        var sql = PoisonWaitEvaluator.EvaluateSqlServer(SqlPoison(accumulatedMs));
        var shared = PoisonWaitEvaluator.Grade(accumulatedMs);

        if (expected is null)
        {
            Assert.Null(pg);
            Assert.Null(sql);
            Assert.Null(shared);
            return;
        }

        var tier = Enum.Parse<AlertSeverityLevel>(expected);
        Assert.Equal(tier, pg!.Severity);
        Assert.Equal(tier, sql!.Severity);
        Assert.Equal(tier, shared);
        /* Same numeric pair, too: accumulated ms against the breached bar in ms. */
        Assert.Equal(pg.NumericCurrentValue, (double)sql.AccumulatedWaitMs);
        Assert.Equal(pg.NumericThresholdValue, sql.NumericThresholdValue);
    }

    /// <summary>
    /// The SQL Server mirror of <see cref="PoisonWaitFiresOnAccumulatedTimeNotPerWaitAverage"/>: 300,000
    /// THREADPOOL waits of 2 ms each is one task continuously starved for the whole window and fires
    /// Warning; the retired avg-ms-per-wait bar (500) read the same window as 2 ms. And the mirror of the
    /// false page: one 600 ms wait — the shape the old bar paged CRITICAL on — is silent.
    /// </summary>
    [Fact]
    public void SqlServerPoisonWaitFiresOnTheStorm_AndNotOnOneSlowWait()
    {
        var storm = PoisonWaitEvaluator.EvaluateSqlServer(SqlPoison(600_000, waits: 300_000));
        Assert.NotNull(storm);
        Assert.Equal(AlertSeverityLevel.Warning, storm!.Severity);
        Assert.Equal(1.0, storm.AvgWaiters);
        Assert.Equal("THREADPOOL (600s in 10m)", storm.CurrentValueClause);

        Assert.Null(PoisonWaitEvaluator.EvaluateSqlServer(SqlPoison(600, waits: 1)));
    }

    /// <summary>
    /// The SQL Server fleet-quiet pin, the twin of <see cref="PoisonWaitStaysSilentOnTheWorstFleetBaselineObserved"/>:
    /// on 43 servers over 4 days the worst ten-minute bucket anywhere held 5,795 ms of THREADPOOL (0.0097
    /// avg waiters — ~100x under the bar), the largest single row was 703 tasks at 8.2 ms (5,779 ms), and
    /// one server's daily compile burst sat at 3,154 ms over 8 tasks just under the OLD bar. None may fire.
    /// </summary>
    [Theory]
    [InlineData(5_795, "THREADPOOL")]
    [InlineData(5_779, "THREADPOOL")]
    [InlineData(3_154, "RESOURCE_SEMAPHORE_QUERY_COMPILE")]
    public void SqlServerPoisonWaitStaysSilentOnTheWorstFleetBucketObserved(long accumulatedMs, string waitType)
    {
        Assert.Null(PoisonWaitEvaluator.EvaluateSqlServer(SqlPoison(accumulatedMs, waitType)));
    }

    /// <summary>
    /// A (0, 0) row — the delta calculator's "no delta is knowable here" marker, indistinguishable in
    /// wait_stats from a genuinely idle interval — is not evidence of anything: it does not fire (nothing
    /// accumulated) and it is not a finding of quiet either; the evaluator returns no finding for it and
    /// says nothing about the window's health. Which of "observed" and "silent" applies is the engine's
    /// call, made on whether rows came back at all (pinned in AlertEngineTests), never on a zero.
    /// </summary>
    [Fact]
    public void SqlServerPoisonWaitTreatsAZeroRowAsNoEvidence()
    {
        Assert.Null(PoisonWaitEvaluator.EvaluateSqlServer(SqlPoison(0, waits: 0)));
        Assert.Empty(PoisonWaitEvaluator.EvaluateSqlServer(new[] { SqlPoison(0, waits: 0), SqlPoison(0, "RESOURCE_SEMAPHORE", 0) }));
        Assert.Empty(PoisonWaitEvaluator.EvaluateSqlServer((IReadOnlyList<PoisonWaitAccumulation>?)null));
        Assert.Empty(PoisonWaitEvaluator.EvaluateSqlServer(Array.Empty<PoisonWaitAccumulation>()));
    }

    /// <summary>Worst-first: severity, then accumulated wait — so the engine's "worst" and mute key are stable.</summary>
    [Fact]
    public void SqlServerPoisonWaitFindingsAreOrderedWorstFirst()
    {
        var findings = PoisonWaitEvaluator.EvaluateSqlServer(new[]
        {
            SqlPoison(700_000, "THREADPOOL"),                          // Warning, more ms
            SqlPoison(6_000_000, "RESOURCE_SEMAPHORE"),                // Critical
            SqlPoison(650_000, "RESOURCE_SEMAPHORE_QUERY_COMPILE"),    // Warning, fewer ms
            SqlPoison(100, "THREADPOOL"),                              // under the bar
        });

        Assert.Equal(new[] { "RESOURCE_SEMAPHORE", "THREADPOOL", "RESOURCE_SEMAPHORE_QUERY_COMPILE" },
            findings.Select(f => f.WaitType).ToArray());
        Assert.Equal(10.0, findings[0].BreachedAvgWaiters);
        Assert.Equal(1.0, findings[1].BreachedAvgWaiters);
    }

    /// <summary>
    /// The two engines' messages share one clause order — seconds accumulated, the window, the wait count,
    /// the average stuck — with only the noun differing (task / backend), so a "Poison Wait" read alike from
    /// either engine; and the SQL remedy names the fix for its type like the PostgreSQL one does.
    /// </summary>
    [Fact]
    public void SqlServerPoisonWaitMessageMirrorsThePostgresShape()
    {
        var sql = PoisonWaitEvaluator.EvaluateSqlServer(SqlPoison(600_000, waits: 300_000))!;
        var pg = PostgresAlertEvaluator.EvaluatePoisonWait(Poison(600_000, waits: 300_000))!;

        Assert.Equal(
            "[THREADPOOL] 600s of wait accumulated in the last 10 minutes across 300,000 waits — on average 1.0 task(s) continuously stuck",
            sql.ShortMessage);
        Assert.StartsWith("[IPC:BtreePage] 600s of wait accumulated in the last 10 minutes across 300,000 waits — on average 1.0 backend(s) continuously stuck", pg.ShortMessage, StringComparison.Ordinal);
        Assert.Equal(
            pg.ThresholdValue.Replace("backend(s)", "task(s)", StringComparison.Ordinal),
            sql.ThresholdValue);
    }

    [Theory]
    [InlineData("THREADPOOL", "worker thread")]
    [InlineData("threadpool", "worker thread")]
    [InlineData("RESOURCE_SEMAPHORE", "memory grants")]
    [InlineData("RESOURCE_SEMAPHORE_QUERY_COMPILE", "compile memory")]
    [InlineData("SOMETHING_ELSE", "active-query snapshots")]
    public void SqlServerPoisonWaitRemedyNamesTheFixForItsType(string waitType, string fragment)
    {
        Assert.Contains(fragment, PoisonWaitEvaluator.SqlServerRemedyFor(waitType), StringComparison.Ordinal);
    }
}
