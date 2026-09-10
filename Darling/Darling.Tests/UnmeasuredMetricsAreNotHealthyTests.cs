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
/// <c>BlockingSeverity(int, double)</c> and <c>DeadlockSeverity(int)</c> took non-nullable parameters, so
/// there was no way for a zero to mean "absent" rather than "calm". All three answered Healthy and painted a
/// green dot. That is a positive claim of health, which is worse than the null beside it, and worse than
/// what <see cref="ServerHealthClassifier.CpuSeverity"/> and
/// <see cref="ServerHealthClassifier.ThreadsSeverity"/> already did by taking nullable inputs.
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
/// </summary>
public sealed class UnmeasuredMetricsAreNotHealthyTests
{
    private static readonly DateTime Now = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private static FleetServerCard Card(
        string? engineKind,
        bool memoryPressure = false,
        int blocking = 0,
        long maxBlockingWaitMs = 0,
        int deadlocks = 0) =>
        DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(1, "t", "t", null, engineKind, false),
            default,
            null,
            default,
            memoryPressure ? new DarlingFleetReader.MemoryPressureRow(3, 0, 0, 128) : default,
            default,
            new DarlingFleetReader.BlockingRow(blocking, maxBlockingWaitMs, 0, 0),
            new DarlingFleetReader.DeadlockRow(deadlocks, deadlocks > 0 ? Now.AddMinutes(-5) : null),
            Now.AddSeconds(-30),
            default,
            null,
            Now);

    private static ServerSummaryItem ViewerCard(
        string? engineKind,
        bool memoryPressure = false,
        int blocking = 0,
        long maxBlockingWaitMs = 0,
        int deadlocks = 0)
    {
        var card = new ServerSummaryItem
        {
            ServerName = "t",
            ServerId = 1,
            MemoryWaiterCount = memoryPressure ? 3 : 0,
            BlockingCount = blocking,
            MaxBlockingWaitMs = maxBlockingWaitMs,
            DeadlockCount = deadlocks,
            IsPostgres = MonitoredEngineKind.IsPostgres(engineKind),
            IsAurora = MonitoredEngineKind.IsAurora(engineKind),
            LastCollectionTime = Now.AddSeconds(-30),
        };
        card.ApplyFreshness(Now);
        return card;
    }

    /* ─────────────────────────── the defect ─────────────────────────── */

    /// <summary>
    /// A PostgreSQL card's three DMV-sourced metrics read Unknown, not Healthy. Red against the unfixed
    /// classifiers, which had no arm that could return anything else for a zero.
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

        /* Threads already reached Unknown on its own (a null ceiling), and CPU does since #3267. So after
           this the card makes NO unearned claim on any metric row - which is the property worth asserting,
           rather than three separate arms that happen to agree today. */
        Assert.Equal(HealthSeverity.Unknown, card.ThreadsSeverity);
        Assert.DoesNotContain(
            HealthSeverity.Healthy,
            new[] { card.MemorySeverity, card.BlockingSeverity, card.DeadlockSeverity, card.ThreadsSeverity, card.CpuSeverity });
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
        Assert.Equal(HealthSeverity.Unknown, card.DeadlockSeverity);
    }

    /// <summary>
    /// The published COUNTS are deliberately unchanged, and #3017's disclosure still reads the same. The
    /// fleet's <c>total_deadlocks</c> is summed from those zeros and <c>deadlock_coverage</c> is what
    /// explains it; nulling them would leave that denominator describing nothing. So the band stopped
    /// claiming health while the count kept saying what it counted — and the two now AGREE, where before
    /// <c>deadlock_source: PostgresTarget</c> sat beside <c>deadlock_severity: Healthy</c>.
    /// </summary>
    [Fact]
    public void TheCountsAndTheCoverageDisclosureAreUntouched()
    {
        var card = Card(MonitoredEngineKind.AuroraPostgres);

        Assert.Equal(0, card.BlockingCount);
        Assert.Equal(0, card.DeadlockCount);
        Assert.False(card.HasMemoryPressure);
        Assert.Equal(FleetDeadlockSource.PostgresTarget, card.DeadlockSource);
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
    /* one blocking event -> Warning */
    [InlineData(false, 1, 0L, 0, HealthSeverity.Healthy, HealthSeverity.Warning, HealthSeverity.Healthy, FleetHealthBand.Warning)]
    /* two events -> Warning; five -> Critical */
    [InlineData(false, 2, 0L, 0, HealthSeverity.Healthy, HealthSeverity.Warning, HealthSeverity.Healthy, FleetHealthBand.Warning)]
    [InlineData(false, 5, 0L, 0, HealthSeverity.Healthy, HealthSeverity.Critical, HealthSeverity.Healthy, FleetHealthBand.Critical)]
    /* a long wait alone -> Warning at 10s, Critical at 60s, with a count of 1 */
    [InlineData(false, 1, 10_000L, 0, HealthSeverity.Healthy, HealthSeverity.Warning, HealthSeverity.Healthy, FleetHealthBand.Warning)]
    [InlineData(false, 1, 60_000L, 0, HealthSeverity.Healthy, HealthSeverity.Critical, HealthSeverity.Healthy, FleetHealthBand.Critical)]
    /* any deadlock -> Critical */
    [InlineData(false, 0, 0L, 1, HealthSeverity.Healthy, HealthSeverity.Healthy, HealthSeverity.Critical, FleetHealthBand.Critical)]
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

        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.BlockingSeverity(0, 0));
        Assert.Equal(HealthSeverity.Warning, ServerHealthClassifier.BlockingSeverity(1, 0));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.BlockingSeverity(5, 0));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.BlockingSeverity(null, 0));
        /* A max wait with no population is still Unknown - the count is the only spelling of "unmeasured",
           so a stray duration cannot smuggle a band back in. */
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.BlockingSeverity(null, 600));

        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.DeadlockSeverity(0));
        Assert.Equal(HealthSeverity.Critical, ServerHealthClassifier.DeadlockSeverity(1));
        Assert.Equal(HealthSeverity.Unknown, ServerHealthClassifier.DeadlockSeverity(null));
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
    /// therefore Unknown" when the fields became nullable. That is only safe BECAUSE Unknown is inert, so it
    /// is pinned rather than assumed: a bundle nobody populated still bands and scores as it did.
    /// </summary>
    [Fact]
    public void AnUnpopulatedMetricBundleBandsAndScoresAsItAlwaysDid()
    {
        var empty = default(ServerHealthMetrics);

        Assert.Equal(HealthSeverity.Healthy, ServerHealthClassifier.OverallMetricSeverity(empty));
        Assert.Equal(0L, ServerHealthClassifier.FleetHealthScore(FleetHealthBand.Healthy, empty));
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
