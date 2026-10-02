/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3272: a card stops reporting <b>Healthy</b> for a metric nothing measured.
///
/// <para><b>The defect.</b> A PostgreSQL target has no row in <c>v_memory_grant_stats</c>,
/// <c>v_blocked_process_reports</c> / <c>v_dmv_blocking_snapshots</c> or <c>v_deadlocks</c>, so the
/// per-metric reads handed its card zeros — and <c>MemorySeverity(bool)</c>,
/// <c>BlockingSeverity(int, double)</c> and <c>DeadlockSeverity(int)</c> (their signatures at the time) took non-nullable parameters, so
/// there was no way for a zero to mean "absent" rather than "calm". All three answered Healthy and painted a
/// green dot. That is a positive claim of health, which is worse than the null beside it, and worse than
/// what <see cref="ServerHealthClassifier.CpuSeverity"/> and
/// <see cref="ServerHealthClassifier.ThreadsSeverity"/> already did by taking nullable inputs.
///
/// <para><b>Deadlocks left the unmeasured set in #3539.</b> A PostgreSQL target's deadlocks are now
/// counted from its own <c>pg_stat_database.deadlocks</c> counter (differenced over the window) and banded
/// through the shared rate tiers, so that row is a MEASUREMENT on both engines and this file asserts it
/// bands — Healthy at zero, Warning and Critical at the tiers — rather than reading Unknown. Memory pressure
/// and blocking stay unmeasured on PostgreSQL for the reasons <see cref="ServerMetricSources"/> gives, and
/// those two are what the Unknown pins below are about.</para>
///
/// <para><b>The invariant this is held to.</b> A MEASURED metric must band exactly as it does today. That is
/// the real constraint, and it is not the same as "leave the SQL Server path alone": the fix necessarily
/// edits functions every SQL Server surface calls. <see cref="AMeasuredCardBandsExactlyAsItDidBefore"/> is
/// the pin, and it fails when the engine gate that decides "measured" is broken in either direction.</para>
///
/// <para><b>Why adding an Unknown cannot move a band or a rank.</b> Both reducers already treat Unknown as
/// inert — <see cref="ServerHealthClassifier.OverallMetricSeverity"/> only escalates on <c>Warning</c> and
/// <c>Critical</c>, and <see cref="ServerHealthClassifier.FleetHealthScore"/>'s magnitude counts only those
/// two — so an unmeasured metric neither improves nor worsens a server's position. That is a property of the
/// existing code rather than a happy accident of this change, and
/// <see cref="UnknownIsBandAndRankNeutral_SoTheFixCannotReorderTheFleet"/> pins it so an "improvement" that
/// made Unknown escalate would fail here instead of silently reordering the fleet.</para>
///
/// <para><b>#3539 A6, the two edges of the same family.</b> The collectors row had a Healthy arm no other
/// metric here has: <c>(failed 0, banded 0)</c> — a server nothing had banded yet — read Healthy, a green
/// dot for a collection nobody had classified. And the fold over a card on which NOTHING was measured
/// answered Healthy, so an online server with six Unknowns counted in the fleet's healthy mass with a
/// "0 of 6 measured" qualifier as its only tell. Both now read Unknown, and the all-Unknown card bands
/// Warning — the never-collected server's band. Neutrality is unchanged wherever anything IS measured:
/// the pins below hold a one-of-six card exactly where #3528 left it.</para>
/// </summary>
public sealed class UnmeasuredMetricsAreNotHealthyTests
{
    private static readonly DateTime Now = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    /// <param name="bandedCollectors">Collectors banded for this server, none failing — forty by default so
    /// the card's collectors row is a MEASURED calm reading and the DMV metrics stay this file's only
    /// variable. Zero is the #3539 A6 shape: nothing banded, nothing measured.</param>
    /// <param name="deadlocks">The SQL Server extended-event count — what a SQL Server card believes.</param>
    /// <param name="pgDeadlocks">The PostgreSQL counter-difference count (#3539) — what a PostgreSQL card
    /// believes. Both are always handed in so a test can assert the card took the ENGINE'S row and ignored
    /// the other engine's structural zero.</param>
    /// <param name="pgDeadlockIntervals">How many differences that count was summed over — the PostgreSQL
    /// arm's measured/not-measured test. Zero by default: a PostgreSQL card is UNMEASURED unless a test
    /// says a difference was taken, which is the direction a forgotten argument must fail in.</param>
    /// <param name="pgDeadlockBand">The <c>pg_database_stats</c> collector's band, for the coverage
    /// arm.</param>
    private static FleetServerCard Card(
        string? engineKind,
        bool memoryPressure = false,
        int blocking = 0,
        long maxBlockingWaitMs = 0,
        int deadlocks = 0,
        int bandedCollectors = 40,
        int pgDeadlocks = 0,
        long pgDeadlockIntervals = 0,
        string? pgDeadlockBand = null) =>
        DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(1, "t", "t", null, engineKind, false),
            default,
            default,
            default,
            memoryPressure ? new DarlingFleetReader.MemoryPressureRow(3, 0, 0, 128) : default,
            default,
            new DarlingFleetReader.BlockingRow(blocking, maxBlockingWaitMs, 0, 0),
            new DarlingFleetReader.DeadlockRow(deadlocks, deadlocks > 0 ? Now.AddMinutes(-5) : null),
            new DarlingFleetReader.PgDeadlockRow(pgDeadlocks, pgDeadlocks > 0 ? Now.AddMinutes(-7) : null, pgDeadlockIntervals),
            Now.AddSeconds(-30),
            /* #3819 appended Regressed after Total: zero here, because this file's subject is the
               measured-vs-unmeasured distinction and a regressed collector would move the band under it. */
            new DarlingFleetReader.CollectorCounts(bandedCollectors, 0, bandedCollectors, 0, null, pgDeadlockBand),
            null,
            Now,
            /* #3368: a real one-hour window and the shipped tiers. This file's subject is the
               measured-vs-unmeasured distinction, so the window has to be one a rate CAN be computed over —
               otherwise a Postgres card's Unknown would be indistinguishable from the unrateable-window
               Unknown and the pins would pass for the wrong reason. */
            TimeSpan.FromHours(1),
            DeadlockRateThresholds.Default);

    private static ServerSummaryItem ViewerCard(
        string? engineKind,
        bool memoryPressure = false,
        int blocking = 0,
        long maxBlockingWaitMs = 0,
        int deadlocks = 0,
        int bandedCollectors = 40)
    {
        var card = new ServerSummaryItem
        {
            ServerName = "t",
            ServerId = 1,
            HealthyCollectorCount = bandedCollectors,
            CollectorCount = bandedCollectors,
            MemoryWaiterCount = memoryPressure ? 3 : 0,
            BlockingCount = blocking,
            MaxBlockingWaitMs = maxBlockingWaitMs,
            DeadlockCount = deadlocks,
            /* #3368: a rateable window, for the reason the fleet-card helper above gives — this file's
               subject is Postgres-vs-SQL-Server, so the window must not be the thing producing the
               Unknown. */
            DeadlockWindow = TimeSpan.FromHours(1),
            BlockingWindow = TimeSpan.FromHours(1),
            IsPostgres = MonitoredEngineKind.IsPostgres(engineKind),
            IsAurora = MonitoredEngineKind.IsAurora(engineKind),
            LastCollectionTime = Now.AddSeconds(-30),
        };
        card.ApplyFreshness(Now);
        return card;
    }

    /* ─────────────────────────── the defect ─────────────────────────── */

    /// <summary>
    /// A PostgreSQL card's two DMV-sourced metrics read Unknown, not Healthy. Red against the unfixed
    /// classifiers, which had no arm that could return anything else for a zero. Deadlocks read Unknown
    /// here too, but for #3539's OWN reason rather than #3272's: the helper hands this card no counter
    /// difference (zero intervals), and a difference of nothing is not a zero. The measured case — a
    /// difference taken, zero deadlocks — is Healthy with a published 0.0/hr, and is asserted beside it so
    /// the two readings of the same card cannot be confused.
    /// </summary>
    [Theory]
    [InlineData(MonitoredEngineKind.Postgres)]
    [InlineData(MonitoredEngineKind.AuroraPostgres)]
    public void APostgresCard_ClaimsNoHealthForWhatItNeverMeasured(string engineKind)
    {
        var card = Card(engineKind);

        Assert.Equal(HealthSeverity.Unknown, card.MemorySeverity);
        Assert.Equal(HealthSeverity.Unknown, card.BlockingSeverity);
        Assert.Equal(HealthSeverity.Unknown, card.DeadlockSeverity);
        Assert.Null(card.DeadlockRatePerHour);
        Assert.False(card.DeadlockMeasured);

        /* Threads already reached Unknown on its own (a null ceiling), and CPU does since #3267. So after
           this the card makes NO unearned claim on any metric row - which is the property worth asserting,
           rather than separate arms that happen to agree today. */
        Assert.Equal(HealthSeverity.Unknown, card.ThreadsSeverity);
        Assert.DoesNotContain(
            HealthSeverity.Healthy,
            new[] { card.MemorySeverity, card.BlockingSeverity, card.DeadlockSeverity, card.ThreadsSeverity, card.CpuSeverity });

        /* #3539: ONE difference taken and the same zero is a measurement. Healthy is EARNED here - a zero
           counter difference over a rateable hour - and the rate is published beside it, because the chip
           and the viewer detail render the rate on non-null alone and a band with no rate beside it is the
           #3368 defect. */
        var measured = Card(engineKind, pgDeadlockIntervals: 1);
        Assert.True(measured.DeadlockMeasured);
        Assert.Equal(HealthSeverity.Healthy, measured.DeadlockSeverity);
        Assert.Equal(0.0, measured.DeadlockRatePerHour);
        Assert.Equal(0, measured.DeadlockCount);
    }

    /// <summary>The viewer's card, same server, same answer — the #2473 rule.</summary>
    [Theory]
    [InlineData(MonitoredEngineKind.Postgres)]
    [InlineData(MonitoredEngineKind.AuroraPostgres)]
    public void TheViewerCardAgrees(string engineKind)
    {
        var card = ViewerCard(engineKind);

        Assert.Equal(HealthSeverity.Unknown, card.MemorySeverity);
        Assert.Equal(HealthSeverity.Unknown, card.BlockingSeverity);
        /* No difference taken (the helper leaves PgDeadlockIntervals at zero), so Unknown - and one
           difference makes the same zero Healthy, as on the fleet card. */
        Assert.Equal(HealthSeverity.Unknown, card.DeadlockSeverity);
        Assert.Null(card.DeadlockRatePerHour);

        var measured = ViewerCard(engineKind);
        measured.PgDeadlockIntervals = 1;
        Assert.Equal(HealthSeverity.Healthy, measured.DeadlockSeverity);
        Assert.Equal(0.0, measured.DeadlockRatePerHour);
    }

    /// <summary>
    /// The published COUNTS are deliberately unchanged, and #3017's disclosure reads the collector state on
    /// this engine too since #3539. The fleet's <c>total_deadlocks</c> is summed from the cards and
    /// <c>deadlock_coverage</c> is what explains it; nulling the counts would leave that denominator
    /// describing nothing. A PostgreSQL card whose <c>pg_database_stats</c> collector left no band is
    /// UNCOVERED (silent), exactly as a SQL Server card with no <c>deadlocks</c> band is — the pre-#3539
    /// answer, <c>PostgresTarget</c> on the engine alone, would have called a server nothing read "counted".
    /// </summary>
    [Fact]
    public void TheCountsAndTheCoverageDisclosureAreUntouched()
    {
        var card = Card(MonitoredEngineKind.AuroraPostgres);

        Assert.Equal(0, card.BlockingCount);
        Assert.Equal(0, card.DeadlockCount);
        Assert.False(card.HasMemoryPressure);
        Assert.Equal(FleetDeadlockSource.CollectorSilent, card.DeadlockSource);

        var counted = Card(MonitoredEngineKind.AuroraPostgres, pgDeadlockBand: CollectorHealthClassifier.Healthy);
        Assert.Equal(FleetDeadlockSource.PostgresTarget, counted.DeadlockSource);
    }

    /* ─────────────────────────── the PostgreSQL deadlock band (#3539) ─────────────────────────── */

    /// <summary>
    /// The PostgreSQL card bands its OWN count through the SAME tiers the SQL Server card uses: over the
    /// helper's one-hour window the count is the rate, so 4 is under the shipped 5/hr Warning bar, 5 is
    /// Warning, and 20 is the shipped Critical bar. The SQL Server row handed in alongside is a
    /// structural zero for this engine and must be IGNORED — a card that summed the two would be right by
    /// accident here and wrong the moment either read produced a non-zero for the wrong engine.
    /// </summary>
    [Theory]
    [InlineData(0, HealthSeverity.Healthy)]
    [InlineData(4, HealthSeverity.Healthy)]
    [InlineData(5, HealthSeverity.Warning)]
    [InlineData(19, HealthSeverity.Warning)]
    [InlineData(20, HealthSeverity.Critical)]
    public void APostgresCard_BandsItsCounterDifferenceThroughTheSharedTiers(int pgDeadlocks, HealthSeverity expected)
    {
        var card = Card(MonitoredEngineKind.Postgres, deadlocks: 999, pgDeadlocks: pgDeadlocks,
            pgDeadlockIntervals: 59, pgDeadlockBand: CollectorHealthClassifier.Healthy);

        Assert.Equal(pgDeadlocks, card.DeadlockCount);
        Assert.True(card.DeadlockMeasured);
        Assert.Equal(expected, card.DeadlockSeverity);
        Assert.Equal(pgDeadlocks, card.DeadlockRatePerHour);
        Assert.Equal(pgDeadlocks > 0 ? Now.AddMinutes(-7) : null, card.DeadlockLastSeen);
        Assert.Equal(FleetDeadlockSource.PostgresTarget, card.DeadlockSource);

        /* The overall band follows, so the fleet's worst-first ranking sees a deadlocking PostgreSQL
           server the way it sees a deadlocking SQL Server. */
        Assert.Equal(expected == HealthSeverity.Healthy ? FleetHealthBand.Healthy
            : expected == HealthSeverity.Warning ? FleetHealthBand.Warning : FleetHealthBand.Critical, card.Band);
    }

    /// <summary>The mirror image: a SQL Server card takes the extended-event row and ignores the PostgreSQL
    /// row, and its collector band is the <c>deadlocks</c> collector's, not <c>pg_database_stats</c>'.</summary>
    [Fact]
    public void ASqlServerCard_IgnoresThePostgresRow()
    {
        var card = Card(MonitoredEngineKind.SqlServer, deadlocks: 2, pgDeadlocks: 999, pgDeadlockIntervals: 59, pgDeadlockBand: CollectorHealthClassifier.Healthy);

        Assert.Equal(2, card.DeadlockCount);
        /* A SQL Server count is a measurement whatever the PostgreSQL row's interval count says - and
           whatever its own collector band says, which is #3272's engine-not-collector rule. */
        Assert.True(card.DeadlockMeasured);
        Assert.True(Card(MonitoredEngineKind.SqlServer).DeadlockMeasured);
        Assert.Equal(Now.AddMinutes(-5), card.DeadlockLastSeen);
        Assert.Equal(HealthSeverity.Healthy, card.DeadlockSeverity);
        /* No deadlocks-collector band was handed in, so a SQL Server is silent whatever pg_database_stats
           says about it. */
        Assert.Equal(FleetDeadlockSource.CollectorSilent, card.DeadlockSource);
    }

    /// <summary>
    /// #3528's label counts the row as measured on a PostgreSQL card now: the fold over the six metric
    /// severities finds one more non-Unknown than it did, so "Healthy — N of 6 measured" moves by one on
    /// every PostgreSQL card. The re-scoring bundle carries the count as a reading too, so the worst-first
    /// rank sees the same measurement the dot does.
    /// </summary>
    [Fact]
    public void ThePostgresCardsMeasuredMetricCountIncludesDeadlocks()
    {
        var card = Card(MonitoredEngineKind.Postgres, pgDeadlocks: 1, pgDeadlockIntervals: 59);

        /* CPU (no source), Threads, Memory and Blocking are Unknown; Deadlocks and Collectors (the
           helper's forty) are measured. Written as the two numbers rather than "one more than before" so
           a regression that un-measured a different row could not pass by coincidence. */
        Assert.Equal(2, card.MeasuredMetricCount);
        Assert.Equal(6, card.MetricCount);
        Assert.Equal((long?)1L, card.ToHealthMetricsValue.DeadlockCount);

        /* And with no difference taken the re-band bundle carries null, the way the card banded - so the
           worst-first score cannot see a measurement the dot did not. */
        Assert.Null(Card(MonitoredEngineKind.Postgres).ToHealthMetricsValue.DeadlockCount);
        Assert.Equal(1, Card(MonitoredEngineKind.Postgres).MeasuredMetricCount);
    }

    /* ─────────────────────────── the invariant ─────────────────────────── */

    /// <summary>
    /// A MEASURED card bands exactly as it did before, across the combinations that reach every arm of all
    /// three classifiers. This is the pin the whole change is held to, and it fails in BOTH directions: an
    /// engine gate that wrongly reports "not measured" drops a real Critical to Unknown, and one that
    /// wrongly reports "measured" puts a green dot back on a PostgreSQL card.
    ///
    /// <para>The expected values are written out rather than computed, so the assertion cannot follow the
    /// implementation. Every one of them is the answer the pre-#3272 classifiers gave.</para>
    /// </summary>
    [Theory]
    /* calm SQL Server: all three measured and Healthy, card Healthy */
    [InlineData(false, 0, 0L, 0, HealthSeverity.Healthy, HealthSeverity.Healthy, HealthSeverity.Healthy, FleetHealthBand.Healthy)]
    /* memory pressure -> Critical */
    [InlineData(true, 0, 0L, 0, HealthSeverity.Critical, HealthSeverity.Healthy, HealthSeverity.Healthy, FleetHealthBand.Critical)]
    /* The blocking COUNT arm bands on a RATE as of #3539 A3, so these rows track the CURRENT band the way
       the deadlock rows below do. Both helpers window one hour, so the count IS the per-hour rate: one or
       two reports is the measured quiet mode and Healthy by count, five is the Warning tier, twenty the
       Critical one — where the old count ladder called one Warning and five Critical. */
    [InlineData(false, 1, 0L, 0, HealthSeverity.Healthy, HealthSeverity.Healthy, HealthSeverity.Healthy, FleetHealthBand.Healthy)]
    [InlineData(false, 2, 0L, 0, HealthSeverity.Healthy, HealthSeverity.Healthy, HealthSeverity.Healthy, FleetHealthBand.Healthy)]
    [InlineData(false, 5, 0L, 0, HealthSeverity.Healthy, HealthSeverity.Warning, HealthSeverity.Healthy, FleetHealthBand.Warning)]
    [InlineData(false, 20, 0L, 0, HealthSeverity.Healthy, HealthSeverity.Critical, HealthSeverity.Healthy, FleetHealthBand.Critical)]
    /* a long wait alone -> Warning at 10s, Critical at 60s, with a count of 1 */
    [InlineData(false, 1, 10_000L, 0, HealthSeverity.Healthy, HealthSeverity.Warning, HealthSeverity.Healthy, FleetHealthBand.Warning)]
    [InlineData(false, 1, 60_000L, 0, HealthSeverity.Healthy, HealthSeverity.Critical, HealthSeverity.Healthy, FleetHealthBand.Critical)]
    /* The deadlock axis bands on a RATE as of #3368, so these rows track the CURRENT band rather than the
       pre-#3368 one -- the method name is about the unmeasured-metrics change, not about this axis. Card()
       and ViewerCard() both window one hour, so the count IS the per-hour rate here: one deadlock is 1/hr
       and Healthy, where the old count band called it Critical. */
    [InlineData(false, 0, 0L, 1, HealthSeverity.Healthy, HealthSeverity.Healthy, HealthSeverity.Healthy, FleetHealthBand.Healthy)]
    /* Both tiers on that same one-hour window, so a revert to counting cannot keep this Theory green. */
    [InlineData(false, 0, 0L, 5, HealthSeverity.Healthy, HealthSeverity.Healthy, HealthSeverity.Warning, FleetHealthBand.Warning)]
    [InlineData(false, 0, 0L, 20, HealthSeverity.Healthy, HealthSeverity.Healthy, HealthSeverity.Critical, FleetHealthBand.Critical)]
    public void AMeasuredCardBandsExactlyAsItDidBefore(
        bool memoryPressure, int blocking, long maxWaitMs, int deadlocks,
        HealthSeverity expectedMemory, HealthSeverity expectedBlocking, HealthSeverity expectedDeadlock,
        FleetHealthBand expectedBand)
    {
        foreach (var engineKind in new string?[] { MonitoredEngineKind.SqlServer, null, "some-future-engine" })
        {
            var card = Card(engineKind, memoryPressure, blocking, maxWaitMs, deadlocks);

            Assert.Equal(expectedMemory, card.MemorySeverity);
            Assert.Equal(expectedBlocking, card.BlockingSeverity);
            Assert.Equal(expectedDeadlock, card.DeadlockSeverity);
            Assert.Equal(expectedBand, card.Band);

            var viewer = ViewerCard(engineKind, memoryPressure, blocking, maxWaitMs, deadlocks);
            Assert.Equal(expectedMemory, viewer.MemorySeverity);
            Assert.Equal(expectedBlocking, viewer.BlockingSeverity);
            Assert.Equal(expectedDeadlock, viewer.DeadlockSeverity);
        }
    }

    /// <summary>
    /// The classifiers' own arms, unchanged for every value that is a measurement — so the change is a NEW
    /// arm rather than a re-banding. Held separately from the card-level pin above because a card can only
    /// reach these through one read path each.
    /// </summary>
    [Fact]
    public void TheClassifiersMeasuredArmsAreUnchanged()
    {
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.MemorySeverity(false));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.MemorySeverity(true));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.MemorySeverity(null));

        /* #3539 A3 re-banded the blocking COUNT arm on a RATE, so, as for deadlocks below, "unchanged"
           holds for the null arm this file is about and the measured arms are asserted at their
           post-#3539 values over a rateable hour: 0/hr Healthy, 5/hr Warning, 20/hr Critical. */
        var hour = TimeSpan.FromHours(1);
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.BlockingSeverity(0, 0, hour));
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(5, 0, hour));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(20, 0, hour));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.BlockingSeverity(null, 0, hour));
        /* A max wait with no population is still Unknown - the count is the only spelling of "unmeasured",
           so a stray duration cannot smuggle a band back in — at any window length, including none. */
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.BlockingSeverity(null, 600, hour));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.BlockingSeverity(null, 600, TimeSpan.Zero));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.BlockingSeverity(null, 600, TimeSpan.FromHours(168)));

        /* #3368 re-banded this one on a RATE, so "unchanged" holds only for the null arm this file is
           about. The measured arms are asserted at their post-#3368 values, over a window a rate can be
           computed on: 0/hr Healthy, 30/hr Critical. That the NULL arm still answers Unknown at every
           window length is the claim that belongs here. */
        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.DeadlockSeverity(0, hour, DeadlockRateThresholds.Default));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.DeadlockSeverity(30, hour, DeadlockRateThresholds.Default));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.DeadlockSeverity(null, hour, DeadlockRateThresholds.Default));

        /* An engine with no deadlock source stays Unknown whatever the window says — the #3272 arm is read
           BEFORE any rate arithmetic, so an unrateable window cannot turn a structural absence into the
           Warning an unrateable COUNT gets. */
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.DeadlockSeverity(null, TimeSpan.Zero, DeadlockRateThresholds.Default));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.DeadlockSeverity(null, TimeSpan.FromHours(168), DeadlockRateThresholds.Default));
    }

    /* ─────────────────────────── neutrality ─────────────────────────── */

    /// <summary>
    /// Replacing a Healthy with an Unknown changes neither the overall band nor the fleet score, so nothing
    /// this fix touches can reorder the worst-first ranking. The property the coordinator asked to be
    /// checked, asserted rather than argued — and it holds because both reducers already ignore Unknown, not
    /// because of anything added here.
    ///
    /// <para>Written over a card that is otherwise WARNING as well as one that is calm: a neutrality claim
    /// tested only on a healthy card would not notice a reducer that let Unknown suppress an existing
    /// escalation.</para>
    /// </summary>
    [Theory]
    [InlineData(0, 0L)]
    [InlineData(2, 0L)]
    [InlineData(1, 60_000L)]
    public void UnknownIsBandAndRankNeutral_SoTheFixCannotReorderTheFleet(int blocking, long maxWaitMs)
    {
        var measured = new ServerHealthMetrics
        {
            CpuPercentForAlert = 85,
            HasMemoryPressure = false,
            BlockingCount = blocking,
            MaxBlockedSeconds = maxWaitMs / 1000.0,
            BlockingWindow = TimeSpan.FromHours(1),
            DeadlockCount = 0,
            TotalThreads = 512,
            AvailableThreads = 500,
            FailedCollectorCount = 0,
        };

        /* The same server with the three DMV metrics unmeasured instead of measured-and-calm. Blocking keeps
           its value where it was non-zero, because nulling a real reading would be a different change. */
        var unmeasured = measured with
        {
            HasMemoryPressure = null,
            DeadlockCount = null,
            BlockingCount = blocking == 0 ? null : blocking,
        };

        var measuredOverall = ServerHealthClassifier.OverallMetricSeverity(measured);
        var unmeasuredOverall = ServerHealthClassifier.OverallMetricSeverity(unmeasured);
        Assert.Equal(measuredOverall, unmeasuredOverall);

        var band = ServerHealthClassifier.ClassifyBand(true, false, false, measuredOverall);
        Assert.Equal(
            ServerHealthClassifier.FleetHealthScore(band, measured),
            ServerHealthClassifier.FleetHealthScore(band, unmeasured));
    }

    /// <summary>
    /// <c>default(ServerHealthMetrics)</c> flipped from "all zero, therefore Healthy" to "all null,
    /// therefore Unknown" when the fields became nullable, and #3539 A6 closed the last gap: its collectors
    /// row <c>(0, 0)</c> is Unknown too, so a bundle nobody populated measures NOTHING — and the fold says
    /// so rather than answering Healthy from six Unknowns. Its band is the never-collected server's Warning,
    /// and the score is that band's rank alone: the magnitude terms still skip Unknown, so it cannot climb
    /// within the band on readings it does not have.
    /// </summary>
    [Fact]
    public void AnUnpopulatedMetricBundleMeasuresNothing_AndIsNotHealthy()
    {
        var empty = default(ServerHealthMetrics);

        Assert.Equal((0, 6), ServerHealthClassifier.MeasuredMetricCounts(empty));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.OverallMetricSeverity(empty));

        var band = ServerHealthClassifier.ClassifyBand(true, false, false, HealthSeverity.Unknown);
        Assert.Equal(FleetHealthBand.Warning, band);
        /* The Warning rank step and nothing else: no Critical/Warning metric to add magnitude, no incident
           count — so an all-Unknown card sorts at the foot of the Warning band, under any card with a real
           amber reading. */
        Assert.Equal(2000L, ServerHealthClassifier.FleetHealthScore(band, empty));
    }

    /* ─────────────────────────── #3539 A6: nothing banded, nothing measured ─────────────────────────── */

    /// <summary>
    /// The collectors row with NO collector banded reads Unknown on both cards, not Healthy — the #3539 A6
    /// sibling. The viewer's offline arm and the web's <c>is_online === false</c> chip cover a KNOWN-dark
    /// server; this is a reachable one whose collection nobody has classified, and "nothing failing" is not
    /// a health claim when nothing could have failed. Held on a SQL Server card so the other five rows are
    /// measured and the collectors row is the only thing that changed.
    /// </summary>
    [Fact]
    public void ZeroBandedCollectors_ReadUnknownNotHealthy_OnBothCards()
    {
        var card = Card(MonitoredEngineKind.SqlServer, bandedCollectors: 0);
        Assert.Equal(0, card.CollectorCount);
        Assert.Equal(0, card.FailedCollectorCount);
        Assert.Equal(HealthSeverity.Unknown, card.CollectorSeverity);

        var viewer = ViewerCard(MonitoredEngineKind.SqlServer, bandedCollectors: 0);
        Assert.Equal(HealthSeverity.Unknown, viewer.CollectorSeverity);
        /* The word beside the dot agrees with it: "--" is the card's spelling of "no reading", where "OK"
           was a green word under what is now a grey dot. */
        Assert.Equal("--", viewer.CollectorDisplay);

        /* And with ONE collector banded the arm is Healthy again, on both — the fix is the zero, not the
           count. */
        Assert.Equal(HealthSeverity.Healthy, Card(MonitoredEngineKind.SqlServer, bandedCollectors: 1).CollectorSeverity);
        Assert.Equal(HealthSeverity.Healthy, ViewerCard(MonitoredEngineKind.SqlServer, bandedCollectors: 1).CollectorSeverity);
        Assert.Equal("OK", ViewerCard(MonitoredEngineKind.SqlServer, bandedCollectors: 1).CollectorDisplay);

        /* The collectors row is one of the six the coverage counts fold over: this helper's SQL Server card
           measures memory, blocking and deadlocks (no CPU or threads row is handed in), so it says "3 of 6
           measured" with nothing banded and "4 of 6" with one collector — the row moved, and only the row. */
        Assert.Equal(3, card.MeasuredMetricCount);
        Assert.Equal(6, card.MetricCount);
        Assert.Equal(4, Card(MonitoredEngineKind.SqlServer, bandedCollectors: 1).MeasuredMetricCount);
        Assert.Equal(FleetHealthBand.Healthy, card.Band);
    }

    /// <summary>
    /// The A6 card itself: an ONLINE PostgreSQL target with nothing banded — no CPU source, no threads, the
    /// three DMV rows structurally null, zero collectors — measures nothing, and is NOT Healthy. Before this
    /// it banded Healthy with "0 of 6 measured" as its only tell and counted in <c>healthy_count</c> on the
    /// web fleet page, <c>get_fleet_overview</c> and the viewer's rollup. Now it bands Warning like a server
    /// awaiting its first collection, leaves the healthy mass on every one of those surfaces, and the
    /// ranking's reason says why in words rather than falling to "Needs attention".
    /// </summary>
    [Theory]
    [InlineData(MonitoredEngineKind.Postgres)]
    [InlineData(MonitoredEngineKind.AuroraPostgres)]
    public void AnOnlineCardMeasuringNothing_IsNotInTheHealthyMass_OnAnySurface(string engineKind)
    {
        var card = Card(engineKind, bandedCollectors: 0);
        Assert.True(card.IsOnline);
        Assert.False(card.AwaitingFirstCollection);
        Assert.Equal(0, card.MeasuredMetricCount);
        Assert.Equal(6, card.MetricCount);
        Assert.Equal(HealthSeverity.Unknown, card.OverallMetricSeverity);
        Assert.Equal(FleetHealthBand.Warning, card.Band);

        /* The service's rollup — /api/fleet and get_fleet_overview read this: zero healthy, one warning. */
        var rollup = DarlingFleetReader.BuildRollup(new[] { card }, Now, Now.AddHours(-1), Now);
        Assert.Equal(0, rollup.HealthyCount);
        Assert.Equal(1, rollup.WarningCount);
        var ranked = Assert.Single(rollup.WorstServers);
        Assert.Equal(DarlingFleetReader.NoMetricMeasuredReason, ranked.Reason);

        /* The viewer's card and rollup, same server, same answer (#2473). */
        var viewer = ViewerCard(engineKind, bandedCollectors: 0);
        Assert.Equal(0, viewer.MeasuredMetricCount);
        Assert.Equal(HealthSeverity.Unknown, viewer.OverallMetricSeverity);
        Assert.Equal(FleetHealthBand.Warning, FleetRollup.ClassifyBand(viewer));
        Assert.Equal(FleetRollup.NoMetricMeasuredReason, FleetRollup.BuildReason(viewer));
        Assert.Equal(DarlingFleetReader.NoMetricMeasuredReason, FleetRollup.NoMetricMeasuredReason);

        var viewerRollup = FleetRollup.Build(new[] { viewer }, new FleetTotals());
        Assert.Equal(0, viewerRollup.HealthyCount);
        Assert.Equal(1, viewerRollup.WarningCount);
        Assert.Contains(viewer, FleetRollup.NeedsAttention(new[] { viewer }));

        /* The tooltip's headline names the band and the reason once — not "Warning — no metric measured yet
           · 0 of 6 measured", which would say the same thing twice. */
        Assert.StartsWith("Warning — " + FleetRollup.NoMetricMeasuredReason, viewer.StatusTooltip, StringComparison.Ordinal);
        Assert.DoesNotContain("0 of 6", viewer.StatusTooltip, StringComparison.Ordinal);

        /* The border agrees with the band: the awaiting-first-collection amber, not the calm dark. */
        Assert.Equal("#FFFFD54F", viewer.CardBorderBrush.Color.ToString());

        /* ONE banded collector and the same card is #3528's "Healthy — 1 of 6 measured", exactly where
           that issue left it: the healthy mass loses only the cards that measured nothing. */
        var one = Card(engineKind, bandedCollectors: 1);
        Assert.Equal(1, one.MeasuredMetricCount);
        Assert.Equal(FleetHealthBand.Healthy, one.Band);
        Assert.Equal(1, DarlingFleetReader.BuildRollup(new[] { one }, Now, Now.AddHours(-1), Now).HealthyCount);
        var oneViewer = ViewerCard(engineKind, bandedCollectors: 1);
        Assert.Equal(FleetHealthBand.Healthy, FleetRollup.ClassifyBand(oneViewer));
        Assert.StartsWith("Healthy — 1 of 6 measured", oneViewer.StatusTooltip, StringComparison.Ordinal);
    }

    /* ─────────────────────────── the ordering the guards depend on ─────────────────────────── */

    /// <summary>
    /// <see cref="HealthSeverity.Unknown"/> must stay the LOWEST member. Several surfaces gate on
    /// <c>severity &gt;= HealthSeverity.Warning</c> — <c>DarlingFleetReader.BuildReason</c>, the viewer's
    /// <c>FleetRollup.BuildReason</c> and its Needs-Attention filter — and this change makes three more
    /// metrics able to return Unknown, so those guards now depend on the ordering for correctness rather
    /// than incidentally. Reorder the enum to make Unknown "worst" and every one of them starts writing
    /// clauses about metrics that were never read.
    /// </summary>
    [Fact]
    public void UnknownIsTheLowestSeverity_BecauseTheReasonGuardsCompareOnIt()
    {
        Assert.Equal(0, (int)HealthSeverity.Unknown);
        Assert.True(HealthSeverity.Unknown < HealthSeverity.Healthy);
        Assert.True(HealthSeverity.Unknown < HealthSeverity.Warning);
        Assert.Equal(
            new[] { HealthSeverity.Unknown, HealthSeverity.Healthy, HealthSeverity.Warning, HealthSeverity.Critical },
            Enum.GetValues<HealthSeverity>().OrderBy(s => (int)s).ToArray());
    }

    /// <summary>
    /// And the consequence, on the string a reader actually sees: a PostgreSQL card's worst-first reason
    /// names none of the three unmeasured metrics. Before the enum check above this was luck; it is now
    /// covered from both ends.
    /// </summary>
    [Fact]
    public void APostgresCardsReasonNamesNoUnmeasuredMetric()
    {
        var reason = DarlingFleetReader.BuildReason(Card(MonitoredEngineKind.AuroraPostgres));

        Assert.DoesNotContain("Memory", reason, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Blocking", reason, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Deadlock", reason, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("Threads", reason, StringComparison.OrdinalIgnoreCase);
    }

    /* ─────────────────────────── one decision, both surfaces ─────────────────────────── */

    /// <summary>
    /// Both surfaces reach the shared "is this a measurement" decision, and no third file decides it for
    /// itself. Same census shape as <c>FleetCardPostgresCpuTests</c>'s, and the same reason: the failure
    /// mode is a surface that never learns the concept exists.
    /// </summary>
    [Fact]
    public void BothSurfacesReachTheSharedMeasuredDecision()
    {
        var callers = System.IO.Directory
            .EnumerateFiles(System.IO.Path.Combine(RepoFile.Root, "Darling"), "*.cs", System.IO.SearchOption.AllDirectories)
            .Where(f =>
            {
                var segments = f.Split(System.IO.Path.DirectorySeparatorChar, System.IO.Path.AltDirectorySeparatorChar);
                return !segments.Contains("bin") && !segments.Contains("obj") && !segments.Contains("Darling.Tests");
            })
            .Where(f => CSharpSourceWalker
                .StripCommentsAndStrings(System.IO.File.ReadAllText(f))
                .Contains("ServerMetricSources.DmvSourced", StringComparison.Ordinal))
            .Select(System.IO.Path.GetFileName)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(new[] { "DarlingFleetReader.cs", "ViewerDataService.Overview.cs" }, callers);
    }
}
