/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using Amazon.PI.Model;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// V115 / #3281: a CPU figure is banded against the capacity it is a fraction OF, and where that capacity
/// was never measured the band reads Unknown rather than Healthy.
///
/// <para><b>The defect.</b> <c>collect.pg_cpu_utilization.cpu_percent</c> holds Performance Insights'
/// <c>os.cpuUtilization.total.avg</c>, which is percent of the capacity CURRENTLY ALLOCATED. All 153 Aurora
/// PostgreSQL instances in the measured fleet are <c>db.serverless</c>, where the allocation is re-sized
/// continuously — so a one-vCPU instance reads exactly <c>100.0</c> with <c>idle</c> exactly <c>0.0</c>
/// whenever one core stays busy for a minute, which is the routine trigger for scaling up. Both consumers
/// banded that as an incident: the built-in High CPU alert at an absolute 80%, and the fleet card's CPU
/// severity ladder (#3267/#3271). At the measured minute the instance held <b>4 of 12 configured ACUs — 33%
/// of the ceiling</b>, and 39 of the last 50 alerts on the PostgreSQL store were High CPU / CPU Resolved
/// pairs on that pattern across about eleven hours.</para>
///
/// <para><b>Nothing was miscollected</b>, which is why every pin here is about the DENOMINATOR rather than
/// the reading. <c>cpu_percent</c> stays collected and stays published.</para>
///
/// <para><b>What is pinned.</b> The measured sample bands Healthy and the ceiling bands Critical; an absent
/// capacity reading bands Unknown on BOTH surfaces and does not fire the alert; the ingestor asks for every
/// metric name it stores and the <c>os.general.</c> prefixes are pinned as literals (a wrong name is a
/// runtime exception and a passing unit test otherwise); a null PI <c>Value</c> becomes NULL and never 0;
/// and the four metric queries cannot be conflated into four CPU readings.</para>
/// </summary>
public sealed class PgCpuCapacityHeadroomTests
{
    internal const int RungVersion = 115;

    /// <summary>The version a store one rung behind this one reports.</summary>
    private const int PreviousVersion = 114;

    /// <summary>This rung's sentinel ordinal in the viewer probe. Its OWN ordinal, which never moves.</summary>
    internal const int ProbeOrdinal = 90;

    private static readonly DateTime Now = new(2026, 9, 10, 12, 0, 0, DateTimeKind.Utc);

    private static FleetServerCard Card(
        string? engineKind,
        double? instanceCpu = null,
        double? acuUtilization = null,
        double? maxConfiguredAcu = null) =>
        DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(1, "pg-1", "pg-1", null, engineKind, false),
            default,
            new DarlingFleetReader.PgCpuRow(instanceCpu, acuUtilization, maxConfiguredAcu),
            default,
            default,
            default,
            default,
            default,
            Now.AddSeconds(-30),
            default,
            null,
            Now);

    private static ServerSummaryItem ViewerCard(
        string? engineKind,
        double? instanceCpu = null,
        double? acuUtilization = null,
        double? maxConfiguredAcu = null)
    {
        var card = new ServerSummaryItem
        {
            ServerName = "pg-1",
            ServerId = 1,
            InstanceCpuPercent = instanceCpu,
            AcuUtilizationPercent = acuUtilization,
            MaxConfiguredAcu = maxConfiguredAcu,
            IsPostgres = MonitoredEngineKind.IsPostgres(engineKind),
            IsAurora = MonitoredEngineKind.IsAurora(engineKind),
            LastCollectionTime = Now.AddSeconds(-30),
        };
        card.ApplyFreshness(Now);
        return card;
    }

    /* ─────────────────────── the measured sample, both directions ─────────────────────── */

    /// <summary>
    /// THE test, over the sample the issue measured: <c>cpu_percent = 100.0</c> with <c>idle = 0.0</c> while
    /// the instance held 4 of 12 configured ACUs. That is 33.33% of the ceiling and it is Healthy — two
    /// thirds of the configured capacity was unused and an operator paged on it would have found nothing.
    ///
    /// <para>Red against the unfixed ladder in the strongest way available: the old band on this input is
    /// <see cref="HealthSeverity.Critical"/>, so the expectation is not merely different, it is the opposite
    /// end of the ladder.</para>
    /// </summary>
    [Fact]
    public void TheMeasuredServerlessSample_IsHealthy_NotAnIncident()
    {
        var card = Card(
            MonitoredEngineKind.AuroraPostgres,
            instanceCpu: 100.0,
            acuUtilization: 33.33,
            maxConfiguredAcu: 12.0);

        Assert.Equal(HealthSeverity.Healthy, card.CpuSeverity);
        Assert.Equal(FleetHealthBand.Healthy, card.Band);

        /* The raw reading is still PUBLISHED — it answers "was a core pinned" — it is simply not banded. */
        Assert.Equal(100.0, card.TotalCpuPercent);
        Assert.Equal(100.0, card.InstanceCpuPercent);
        Assert.Equal(33.33, card.AcuUtilizationPercent);
        Assert.Equal(12.0, card.MaxConfiguredAcu);

        /* And the viewer's card agrees, including the rendered string a human actually reads: the headline
           stays the raw reading with the banded figure beside it, so a green 100% explains itself. */
        var viewer = ViewerCard(
            MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: 33.33, maxConfiguredAcu: 12.0);

        Assert.Equal(HealthSeverity.Healthy, viewer.CpuSeverity);
        Assert.Equal("100% (ACU 33%)", viewer.CpuDisplay);
        Assert.Equal("ACU 33% of 12 configured", viewer.CpuDetail);
    }

    /// <summary>
    /// The other direction, which is what stops this being a mute button: at the CONFIGURED ceiling the band
    /// is Critical on the same card shape. 100% of the ceiling means there is no more capacity to scale
    /// into, which is a real incident — and the raw CPU is held at exactly the same 100.0 as the Healthy
    /// case above, so the only thing that moved is the denominator.
    /// </summary>
    [Theory]
    [InlineData(100.0, HealthSeverity.Critical)]
    [InlineData(95.0, HealthSeverity.Critical)]
    [InlineData(94.9, HealthSeverity.Warning)]
    [InlineData(80.0, HealthSeverity.Warning)]
    [InlineData(79.9, HealthSeverity.Healthy)]
    [InlineData(33.33, HealthSeverity.Healthy)]
    [InlineData(0.0, HealthSeverity.Healthy)]
    public void TheBandFollowsTheCeiling_NotTheAllocation(double acuUtilization, HealthSeverity expected)
    {
        Assert.Equal(
            expected,
            Card(MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: acuUtilization)
                .CpuSeverity);

        Assert.Equal(
            expected,
            ViewerCard(MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: acuUtilization)
                .CpuSeverity);
    }

    /// <summary>
    /// The SQL Server arm is untouched: its reading is a fraction of a FIXED host, so it is already the
    /// saturation figure and the same ladder reads it directly. Asserted here rather than trusted, because
    /// "no currently-correct band changes meaning" is half of what this change claims.
    /// </summary>
    [Theory]
    [InlineData(60.0, 35.0, HealthSeverity.Critical)]
    [InlineData(60.0, 21.0, HealthSeverity.Warning)]
    [InlineData(40.0, 3.0, HealthSeverity.Healthy)]
    public void TheRingBufferArmBandsOnItsOwnReading(double sqlCpu, double otherCpu, HealthSeverity expected)
    {
        var card = DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(1, "sql-1", "sql-1", null, MonitoredEngineKind.SqlServer, false),
            new DarlingFleetReader.CpuRow(sqlCpu, otherCpu),
            default,
            default,
            default,
            default,
            default,
            default,
            Now.AddSeconds(-30),
            default,
            null,
            Now);

        Assert.Equal(expected, card.CpuSeverity);
        Assert.Equal(FleetCpuSource.RingBuffer, card.CpuSource);

        /* And no capacity figure is invented for it. */
        Assert.Null(card.AcuUtilizationPercent);
        Assert.Null(card.MaxConfiguredAcu);
    }

    /* ─────────────────────── absent capacity is Unknown, never Healthy ─────────────────────── */

    /// <summary>
    /// The pin that fails toward the quieter label if it is written backwards, so it is asserted directly on
    /// both surfaces: a Performance Insights CPU reading with NO capacity reading beside it bands
    /// <see cref="HealthSeverity.Unknown"/>. #3271's own lesson — a card must not claim Healthy for
    /// something it never measured — and the tempting fallback-to-raw-CPU shape would silently restore
    /// exactly the band this replaced.
    ///
    /// <para>Every rung of the raw reading is covered, including the 100 that used to page someone and the 0
    /// that would look innocuous. Unknown on ALL of them: what is unknown is not the CPU, it is what the CPU
    /// is a fraction of.</para>
    /// </summary>
    [Theory]
    [InlineData(100.0)]
    [InlineData(96.0)]
    [InlineData(80.0)]
    [InlineData(33.0)]
    [InlineData(0.0)]
    public void APerformanceInsightsReadingWithNoCapacitySample_IsUnknown_NotHealthy(double instanceCpu)
    {
        var card = Card(MonitoredEngineKind.AuroraPostgres, instanceCpu: instanceCpu);
        var viewer = ViewerCard(MonitoredEngineKind.AuroraPostgres, instanceCpu: instanceCpu);

        Assert.Equal(HealthSeverity.Unknown, card.CpuSeverity);
        Assert.Equal(HealthSeverity.Unknown, viewer.CpuSeverity);

        Assert.NotEqual(HealthSeverity.Healthy, card.CpuSeverity);
        Assert.NotEqual(HealthSeverity.Healthy, viewer.CpuSeverity);

        /* The provenance arm is unchanged — a reading WAS collected, and the card still says so. What is
           missing is the capacity, and Unknown is the band's way of saying which of the two it is. */
        Assert.Equal(FleetCpuSource.PerformanceInsights, card.CpuSource);
        Assert.Equal(instanceCpu, card.TotalCpuPercent);
    }

    /// <summary>
    /// The shared decision returns NULL rather than the raw CPU when no capacity was sampled — the one
    /// mechanism every consumer of this change routes through, so it is asserted on its own as well as
    /// through the cards. A fallback here is the whole defect, reintroduced in one line, in a place that
    /// compiles and reads reasonably.
    /// </summary>
    [Fact]
    public void TheSharedBandInput_NeverFallsBackToTheRawCpu()
    {
        Assert.Null(FleetCpuProvenance.CpuBandInputPercent(100.0, null, FleetCpuSource.PerformanceInsights));
        Assert.Equal(
            33.33,
            FleetCpuProvenance.CpuBandInputPercent(100.0, 33.33, FleetCpuSource.PerformanceInsights));

        /* Every other arm reads the total, including the two that mean "no number": the capacity column is
           null on a SQL Server target and must not become the band input by accident. */
        foreach (var source in new[]
                 {
                     FleetCpuSource.RingBuffer,
                     FleetCpuSource.NotCollected,
                     FleetCpuSource.NoSourceForEngine,
                 })
        {
            Assert.Equal(43.0, FleetCpuProvenance.CpuBandInputPercent(43.0, 99.0, source));
            Assert.Null(FleetCpuProvenance.CpuBandInputPercent(null, 99.0, source));
        }
    }

    /// <summary>
    /// The ladder takes all three inputs as REQUIRED parameters, with no overload and no defaults.
    ///
    /// <para>This is the structural half of the fix and it is the one worth pinning by reflection: a
    /// defaulted or one-argument <c>CpuSeverity</c> would let any caller — a new surface, or one of the two
    /// existing ones during a refactor — compile while banding a serverless instance's percent-of-allocated
    /// as saturation again. The compiler is the only thing that can catch that at every call site at once,
    /// and it can only do it while there is nothing to fall back to.</para>
    /// </summary>
    [Fact]
    public void TheCpuLadderHasNoDefaultedOrSingleArgumentForm()
    {
        var overloads = typeof(ServerHealthClassifier)
            .GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == nameof(ServerHealthClassifier.CpuSeverity))
            .ToList();

        var only = Assert.Single(overloads);
        var parameters = only.GetParameters();

        Assert.Equal(3, parameters.Length);
        Assert.All(parameters, p => Assert.False(
            p.HasDefaultValue,
            $"'{p.Name}' has a default, so a caller can omit it and band the wrong percentage silently"));

        /* And the third one really is the source discriminant rather than a second number that could be
           passed in either order. */
        Assert.Equal(typeof(FleetCpuSource), parameters[2].ParameterType);
    }

    /// <summary>
    /// The per-metric dot is half a card. The OVERALL band paints its border and the fleet score orders the
    /// worst-first list, and both go through <c>ToHealthMetrics()</c> — a SECOND
    /// <see cref="ServerHealthMetrics"/> construction site on each of the two surfaces, in the same file as
    /// the first.
    ///
    /// <para>Asserted because a bundle built without the capacity figure falls through to the raw
    /// percent-of-allocated reading, so the measured sample's CPU dot goes green while the card's own
    /// border turns red and the fleet ranking still treats a routine scale-up as maxed out. A card that
    /// contradicts itself on one screen is the failure #3272 fixed for three other metrics.</para>
    /// </summary>
    [Fact]
    public void TheOverallBandAndTheFleetScore_ReadTheCapacityFigureToo()
    {
        var measured = Card(
            MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: 33.33, maxConfiguredAcu: 12.0);
        var viewer = ViewerCard(
            MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: 33.33, maxConfiguredAcu: 12.0);

        Assert.Equal(
            HealthSeverity.Healthy,
            ServerHealthClassifier.OverallMetricSeverity(measured.ToHealthMetricsValue));
        Assert.Equal(HealthSeverity.Healthy, viewer.OverallMetricSeverity);

        /* And the RANKING: a routine scale-up must not outrank a genuinely idle instance. Compared against
           an idle card rather than against a literal, because the score's own terms are not this pin's
           subject — what is, is that the two cards are indistinguishable to it. */
        var idle = Card(
            MonitoredEngineKind.AuroraPostgres, instanceCpu: 0.0, acuUtilization: 0.0, maxConfiguredAcu: 12.0);

        Assert.Equal(
            ServerHealthClassifier.FleetHealthScore(idle.Band, idle.ToHealthMetricsValue),
            ServerHealthClassifier.FleetHealthScore(measured.Band, measured.ToHealthMetricsValue));
        /* Through the viewer's own shipped entry point, so what is asserted is the score the Overview's
           worst-first list really orders on. */
        Assert.Equal(
            FleetRollup.FleetHealthScore(
                ViewerCard(MonitoredEngineKind.AuroraPostgres, instanceCpu: 0.0, acuUtilization: 0.0)),
            FleetRollup.FleetHealthScore(viewer));

        /* At the CEILING the same path must still escalate, so this is not a mute button on the rollup. */
        var pinned = Card(
            MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: 99.0, maxConfiguredAcu: 12.0);

        Assert.Equal(
            HealthSeverity.Critical,
            ServerHealthClassifier.OverallMetricSeverity(pinned.ToHealthMetricsValue));
        Assert.Equal(
            HealthSeverity.Critical,
            ViewerCard(MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: 99.0)
                .OverallMetricSeverity);
    }

    /// <summary>
    /// The category repair, not the instance: <b>every</b> metric bundle in production code that carries a
    /// CPU reading also carries the capacity figure and the source.
    ///
    /// <para>This is the check that would have caught the defect above.
    /// <see cref="FleetCardPostgresCpuTests.TheOnlyTwoSurfacesThatBandCpu_BothReachTheSharedDecision"/>
    /// censuses FILES, and both missed construction sites sat in the two files that census already
    /// approved — so a per-file scan reported a clean tree while half the sites in it were wrong. The unit
    /// of the invariant is the INITIALIZER.</para>
    ///
    /// <para>Production code only. A test bundle that sets a bare <c>CpuPercentForAlert</c> is exercising
    /// the fixed-capacity arm on purpose and the default source is the right one for it; forcing the
    /// spelling there would be churn without an invariant behind it.</para>
    /// </summary>
    [Fact]
    public void EveryProductionMetricBundleWithACpuReading_AlsoCarriesTheCapacityAndTheSource()
    {
        var scanned = 0;
        var bundles = 0;
        var offenders = new List<string>();

        foreach (var file in Directory.EnumerateFiles(RepoFile.Root, "*.cs", SearchOption.AllDirectories))
        {
            var segments = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

            if (segments.Contains("bin") || segments.Contains("obj")
                || segments.Any(s => s.EndsWith(".Tests", StringComparison.Ordinal)))
            {
                continue;
            }

            scanned++;
            var code = CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(file));
            var name = Path.GetFileName(file);

            foreach (var initializer in MetricBundleInitializers(code))
            {
                bundles++;

                if (!initializer.Contains(nameof(ServerHealthMetrics.CapacityUtilizationPercent), StringComparison.Ordinal)
                    || !initializer.Contains(nameof(ServerHealthMetrics.CpuSource), StringComparison.Ordinal))
                {
                    offenders.Add(name);
                }
            }
        }

        /* Both floors, because the scan IS the enforcement: a tree it cannot read, or a bundle count it
           cannot find, satisfies an empty offender list for a reason unrelated to the invariant. */
        Assert.True(scanned > 200, $"the sweep read only {scanned} production .cs file(s)");
        Assert.True(bundles >= 3, $"the sweep found only {bundles} CPU-carrying metric bundle(s)");

        /* Named rather than counted: the whole value of this pin is telling the next person WHICH
           initializer, and "found 2" would send them back to the scan instead of to the file. */
        var named = offenders.Distinct().OrderBy(f => f, StringComparer.Ordinal).ToArray();

        Assert.True(
            named.Length == 0,
            "these production metric bundles carry a CPU reading with no capacity figure and no source "
          + "beside it, so they band percent-of-allocated: " + string.Join(", ", named));
    }

    /// <summary>
    /// Every object-initializer body in stripped source that ASSIGNS
    /// <see cref="ServerHealthMetrics.CpuPercentForAlert"/>, brace matched outward from the assignment.
    ///
    /// <para><b>Keyed on the member, not on the type name, and that is the whole point.</b> A scan for
    /// <c>new ServerHealthMetrics</c> finds one of the three production sites: the other two are
    /// target-typed <c>ToHealthMetrics() =&gt; new() { ... }</c>, where the type appears only in the return
    /// signature. That is precisely why both survived a review and a file-level census. The member is
    /// declared on exactly one type, so an initializer assigning it IS this bundle however it is
    /// spelled.</para>
    ///
    /// <para>The declaration itself is excluded by requiring an <c>=</c> that is not <c>=&gt;</c>, and a
    /// READ is excluded by refusing a preceding <c>.</c> — the same receiver rule
    /// <c>RepoFileAdoptionTests</c>'s call regex uses, and for the same reason.</para>
    /// </summary>
    private static IEnumerable<string> MetricBundleInitializers(string code)
    {
        foreach (var match in AssignsCpuReading.Matches(code).Cast<System.Text.RegularExpressions.Match>())
        {
            /* Outward to the enclosing initializer: back to the nearest unmatched `{`, then forward to
               the `}` that closes it. */
            var depth = 0;
            var open = -1;

            for (var i = match.Index; i >= 0; i--)
            {
                if (code[i] == '}')
                {
                    depth++;
                }
                else if (code[i] == '{' && depth-- == 0)
                {
                    open = i;
                    break;
                }
            }

            if (open < 0)
            {
                continue;
            }

            depth = 0;

            for (var i = open; i < code.Length; i++)
            {
                if (code[i] == '{')
                {
                    depth++;
                }
                else if (code[i] == '}' && --depth == 0)
                {
                    yield return code[open..(i + 1)];
                    break;
                }
            }
        }
    }

    /// <summary>An ASSIGNMENT of the CPU reading in an initializer: no preceding <c>.</c> (which would make
    /// it a read), and an <c>=</c> that is not the <c>=&gt;</c> of an expression-bodied property.</summary>
    private static readonly System.Text.RegularExpressions.Regex AssignsCpuReading = new(
        @"(?<![\w.])CpuPercentForAlert\s*=(?!>)",
        System.Text.RegularExpressions.RegexOptions.Compiled);

    /// <summary>
    /// Both surfaces' "why is this server in its band" line names the CAPACITY figure for a serverless
    /// target, in the same words — and neither leads with the raw reading under a "CPU" label.
    ///
    /// <para>The two reason lines are where #2473's rule is easiest to break with nothing failing, because
    /// the difference is PROSE. The service's line is built from the card's fields; the viewer's is built
    /// from its display strings, and <c>CpuDisplay</c> legitimately renders
    /// <c>"100% (ACU 87%)"</c> — true, and it leads with the percent-of-allocated figure under the one
    /// label #3281 exists to stop a reader trusting.</para>
    ///
    /// <para>Asserted as a shared CLAUSE rather than as two equal strings: the fixed-capacity arm's text
    /// differs between the surfaces on purpose (the viewer's carries the per-process split the service's
    /// card has no room for), so equality would forbid a difference that is correct.</para>
    /// </summary>
    [Fact]
    public void BothReasonLinesNameTheCapacityFigure_InTheSameWords()
    {
        /* Inside the Warning band on the CEILING, so both reason lines have something to say. */
        var card = Card(
            MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: 87.0, maxConfiguredAcu: 12.0);
        var viewer = ViewerCard(
            MonitoredEngineKind.AuroraPostgres, instanceCpu: 100.0, acuUtilization: 87.0, maxConfiguredAcu: 12.0);

        var clause = FleetCpuProvenance.CapacityBandClause(87.0, FleetCpuSource.PerformanceInsights);
        Assert.NotNull(clause);

        var serviceReason = DarlingFleetReader.BuildReason(card);
        var viewerReason = FleetRollup.BuildReason(viewer);

        Assert.Contains(clause!, serviceReason, StringComparison.Ordinal);
        Assert.Contains(clause!, viewerReason, StringComparison.Ordinal);

        /* And neither leads the CPU clause with the raw reading. "CPU 100%" is the exact framing the issue
           is about, in both spellings the two surfaces would reach it by. */
        Assert.DoesNotContain("CPU 100%", serviceReason, StringComparison.Ordinal);
        Assert.DoesNotContain("CPU 100%", viewerReason, StringComparison.Ordinal);
        Assert.DoesNotContain("CPU 100% (ACU", viewerReason, StringComparison.Ordinal);

        /* The SQL Server arm still names CPU, so this is not a blanket removal of the word. */
        var sqlServer = DarlingFleetReader.BuildCard(
            new DarlingFleetReader.FleetServerRow(2, "sql-1", "sql-1", null, MonitoredEngineKind.SqlServer, false),
            new DarlingFleetReader.CpuRow(60.0, 30.0),
            default, default, default, default, default, default,
            Now.AddSeconds(-30), default, null, Now);

        Assert.Contains("CPU 90%", DarlingFleetReader.BuildReason(sqlServer), StringComparison.Ordinal);
        Assert.Null(FleetCpuProvenance.CapacityBandClause(null, FleetCpuSource.RingBuffer));
        Assert.Null(FleetCpuProvenance.CapacityBandClause(87.0, FleetCpuSource.RingBuffer));
        Assert.Null(FleetCpuProvenance.CapacityBandClause(null, FleetCpuSource.PerformanceInsights));
    }

    /// <summary>
    /// The clause is spelled in exactly ONE production file, so a third reason line cannot hand-write its
    /// own wording and drift again. The census is the repair for the category: the defect this catches was
    /// two copies of a sentence, one of which nobody updated.
    /// </summary>
    [Fact]
    public void TheCapacityClauseIsSpelledInExactlyOnePlace()
    {
        var spellings = new List<string>();
        var scanned = 0;

        foreach (var file in Directory.EnumerateFiles(RepoFile.Root, "*.cs", SearchOption.AllDirectories))
        {
            var segments = file.Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);

            if (segments.Contains("bin") || segments.Contains("obj")
                || segments.Any(s => s.EndsWith(".Tests", StringComparison.Ordinal)))
            {
                continue;
            }

            scanned++;

            /* The RAW text, not stripped: the clause lives inside a string literal, which is exactly what
               the comment-and-string stripper removes. A stripped scan here would find nothing and read as
               a clean tree. */
            if (File.ReadAllText(file).Contains("of configured ACU", StringComparison.Ordinal))
            {
                spellings.Add(Path.GetFileName(file));
            }
        }

        Assert.True(scanned > 200, $"the sweep read only {scanned} production .cs file(s)");
        Assert.Equal(new[] { "FleetCpuSource.cs" }, spellings.Distinct().OrderBy(f => f, StringComparer.Ordinal).ToArray());
    }

    /* ─────────────────────── the alert reads the same figure ─────────────────────── */

    /// <summary>
    /// The High CPU alert thresholds capacity headroom through the SAME shared decision the cards band on,
    /// rather than holding its own copy of the rule. That is what makes
    /// <see cref="TheSharedBandInput_NeverFallsBackToTheRawCpu"/> a pin on the alert too: break the shared
    /// function and the alert goes wrong with the card, in one place, where a test can see it.
    ///
    /// <para>Asserted at the source, because the evaluator is private, needs a live store and a configured
    /// deliverer, and the behaviour that matters is WHICH figure it compares. The two negative clauses are
    /// the load-bearing half: a threshold comparison against <c>CpuPercent</c> is the alert this replaced,
    /// and it would otherwise sit one word away from correct.</para>
    /// </summary>
    [Fact]
    public void ThePostgresCpuAlertThresholdsCapacity_ThroughTheSharedDecision()
    {
        var body = EvaluatePgCpuBody();

        Assert.Contains("FleetCpuProvenance.CpuBandInputPercent", body, StringComparison.Ordinal);
        Assert.Contains("capacityPercent.Value >= alertSettings.CpuThresholdPercent", body, StringComparison.Ordinal);

        /* The old comparison, in either spelling. Neither may come back. */
        Assert.DoesNotContain("CpuPercent >= alertSettings", body, StringComparison.Ordinal);
        Assert.DoesNotContain("alertSettings.CpuThresholdPercent <= reading", body, StringComparison.Ordinal);

        /* And the reading it thresholds carries the capacity at all, from the same row as the CPU. */
        Assert.Contains("acu_utilization_percent", DarlingPgCpuUtilizationReader.LatestCpuSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The rendered alert text names the figure that crossed and the one beside it, each labelled with what
    /// it is a fraction OF. "Total CPU: 100%" was true and read as saturation, which is the whole defect
    /// restated as a notification — so the strings are pinned rather than left to drift back.
    /// </summary>
    [Fact]
    public void TheAlertTextNamesWhatEachPercentageIsAFractionOf()
    {
        var body = EvaluatePgCpuBody();

        /* The denominator through the SHARED constant, not a fourth hand-written copy of the words — the
           reference is the assertion, because a literal here would pass while drifting. */
        Assert.Contains("FleetCpuProvenance.CapacityDenominator", body, StringComparison.Ordinal);
        Assert.DoesNotContain("% of configured ACU", body, StringComparison.Ordinal);
        Assert.Contains("of currently allocated capacity", body, StringComparison.Ordinal);

        /* The metric NAMES are deliberately unchanged, for the engine-parity reason #2719 recorded: a mute
           rule or history filter built on these strings keeps working across the fix. */
        Assert.Contains("\"High CPU\"", body, StringComparison.Ordinal);
        Assert.Contains("\"CPU Resolved\"", body, StringComparison.Ordinal);
    }

    /// <summary>The <c>EvaluatePgCpuAsync</c> body, sliced from its own declaration to the next member at
    /// the same indent. Sliced rather than searched whole-file because every assertion above would
    /// otherwise be satisfied by text belonging to the SQL Server CPU alert in the same file — a pin that
    /// reads a neighbour is not a pin.</summary>
    private static string EvaluatePgCpuBody()
    {
        var source = RepoFile.ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        const string start = "private async Task EvaluatePgCpuAsync(";
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, "EvaluatePgCpuAsync was not found, so this pin would read nothing");

        var to = source.IndexOf("\n    private ", from + start.Length, StringComparison.Ordinal);
        Assert.True(to > from, "the method's end was not found, so this pin would read the rest of the file");

        var body = source[from..to];

        /* The slice has to be a method, not a fragment: an off-by-one on either bound silently shrinks it
           and every Assert.DoesNotContain above starts passing for the wrong reason. */
        Assert.Contains("var reading = await DarlingPgCpuUtilizationReader.GetLatestAsync", body, StringComparison.Ordinal);
        Assert.True(body.Length > 1500, $"the sliced body is only {body.Length} chars, which cannot be this method");

        return body;
    }

    /* ─────────────────────── collection: the names, and the nulls ─────────────────────── */

    /// <summary>
    /// Every metric name the ingestor ASKS for is a name it STORES, and the reverse — a set equality rather
    /// than a count, because a count passes while naming the wrong metric.
    ///
    /// <para>The failure this exists for is silent in the worst way: a name dropped from
    /// <c>RequestedMetrics</c> leaves its column never populated on every target forever, which reads
    /// downstream as "the capacity was never measured" — and since that is a legitimate state, nothing
    /// fails. The reverse omission is a metric fetched over the API and thrown away every cycle.</para>
    /// </summary>
    [Fact]
    public void TheIngestorRequestsExactlyTheMetricsItStores()
    {
        var requested = RdsCpuIngestor.RequestedMetrics;

        /* Four distinct names — a duplicate would make the count right and the coverage wrong. */
        Assert.Equal(4, requested.Count);
        Assert.Equal(requested.Count, requested.Distinct(StringComparer.Ordinal).Count());

        /* Each requested name reaches a column, proven by the row the ingestor builds: give every metric a
           distinguishable value at one timestamp and every payload column must come back carrying one.
           Asserting the LIST of names against a hand-written list would only prove the list was copied. */
        var stored = PgCpuUtilizationCollector.Instance.PayloadColumns.Select(c => c.Name).ToList();

        Assert.Equal(
            new[] { "sample_time", "cpu_percent", "acu_utilization_percent", "serverless_capacity_acu", "max_configured_acu" },
            stored);

        var sample = Assert.Single(RdsCpuIngestor.BuildSamples(
            requested
                .Select((metric, i) => Point(metric, Now, 10.0 + i))
                .ToList(),
            watermark: null));

        /* One value per requested metric landed in its own field: four distinct values, none dropped and
           none duplicated, which is what a per-metric keying buys over a flattened sequence. */
        Assert.Equal(
            new double?[] { 10.0, 11.0, 12.0, 13.0 },
            new double?[]
            {
                sample.CpuPercent,
                sample.AcuUtilizationPercent,
                sample.ServerlessCapacityAcu,
                sample.MaxConfiguredAcu,
            });
    }

    /// <summary>
    /// When Performance Insights rejects the request SHAPE, the CPU metric is asked for ALONE — so the
    /// worst case on an instance class with no ACU concept is exactly today's behaviour, not the loss of
    /// <c>cpu_percent</c> as well.
    ///
    /// <para>This ingestor's dispatch is on <c>RdsEndpoint.TryParse</c> succeeding and NOT on
    /// <c>PgCpuUtilizationCollector.AppliesTo</c>, so every Aurora and every plain RDS PostgreSQL target
    /// reaches it, provisioned as well as serverless. PI rejects an unknown metric name with
    /// <see cref="InvalidArgumentException"/>, and that failure is TOTAL — the whole call, so the whole
    /// cycle for that target. The measured fleet is 153 of 153 serverless, so whether PI treats "not
    /// applicable to this resource" that way cannot be settled by observation from here; the retry makes
    /// the answer not matter.</para>
    ///
    /// <para><b>The second half is what makes this a pin rather than a retry.</b> An authorization refusal
    /// must NOT earn a retry: it is a different fault, the runner classifies it as PERMISSIONS, and a
    /// blanket catch would spend a second call on it and blur the two. Asserted by attempt COUNT, which is
    /// the only thing that can tell "propagated" from "retried and then propagated".</para>
    /// </summary>
    [Fact]
    public async Task WhenPerformanceInsightsRejectsTheRequestShape_TheCpuMetricIsAskedForAlone()
    {
        var asked = new List<IReadOnlyList<string>>();

        var answered = await RdsCpuIngestor.WithCapacityFallbackAsync(metrics =>
        {
            asked.Add(metrics);

            return metrics.Count > 1
                ? throw new InvalidArgumentException("The specified metric is not a known metric")
                : Task.FromResult("answered");
        });

        Assert.Equal("answered", answered);
        Assert.Equal(2, asked.Count);

        /* First the four, then the one — and the one is the CPU metric, not merely a shorter list. */
        Assert.Equal(RdsCpuIngestor.RequestedMetrics, asked[0]);
        Assert.Equal(new[] { "os.cpuUtilization.total.avg" }, asked[1].ToArray());
        Assert.Equal(RdsCpuIngestor.CpuOnlyMetrics, asked[1]);

        /* An authorization refusal is a DIFFERENT fault and is not retried: one attempt, then out, so the
           runner still classifies it as PERMISSIONS instead of seeing a second call's failure. */
        var refused = new List<IReadOnlyList<string>>();

        await Assert.ThrowsAsync<NotAuthorizedException>(() =>
            RdsCpuIngestor.WithCapacityFallbackAsync<string>(metrics =>
            {
                refused.Add(metrics);
                throw new NotAuthorizedException("not authorized to perform pi:GetResourceMetrics");
            }));

        Assert.Single(refused);

        /* And a rejection that survives the retry propagates rather than being swallowed into an empty
           window, which would read downstream as "PI answered and had nothing". */
        var twice = 0;

        await Assert.ThrowsAsync<InvalidArgumentException>(() =>
            RdsCpuIngestor.WithCapacityFallbackAsync<string>(_ =>
            {
                twice++;
                throw new InvalidArgumentException("still rejected");
            }));

        Assert.Equal(2, twice);
    }

    /// <summary>
    /// The <c>os.general.</c> prefixes, as literals.
    ///
    /// <para>Worth a pin of its own because the failure mode is asymmetric: <c>os.acuUtilization.avg</c> —
    /// the name anyone would guess from the CPU metric's shape — does not exist, and Performance Insights
    /// rejects it with <c>InvalidArgumentException: The specified metric is not a known metric</c>, which
    /// fails the whole call and so the whole cycle for that target. Every unit test that does not name the
    /// string keeps passing. These four were verified live against a production instance.</para>
    /// </summary>
    [Fact]
    public void TheMetricNamesArePinnedAsLiterals()
    {
        Assert.Equal(
            new[]
            {
                "os.cpuUtilization.total.avg",
                "os.general.acuUtilization.avg",
                "os.general.serverlessDatabaseCapacity.avg",
                "os.general.maxConfiguredAcu.avg",
            },
            RdsCpuIngestor.RequestedMetrics.ToArray());

        /* The prefix specifically, stated as the negative that cost the discovery: no requested name may be
           the os.-without-general spelling of a capacity metric. */
        Assert.DoesNotContain("os.acuUtilization.avg", RdsCpuIngestor.RequestedMetrics);
        Assert.DoesNotContain("os.serverlessDatabaseCapacity.avg", RdsCpuIngestor.RequestedMetrics);
        Assert.DoesNotContain("os.maxConfiguredAcu.avg", RdsCpuIngestor.RequestedMetrics);
    }

    /// <summary>
    /// A Performance Insights data point with a null <c>Value</c> stays NULL for every one of the new
    /// metrics, and never becomes 0.
    ///
    /// <para>0 is the dangerous default here in a way it is not for a counter: <c>acu_utilization_percent</c>
    /// of 0 reads as "no capacity in use", which bands Healthy — the precise claim the whole change exists
    /// to stop a card making about something nothing measured. <c>max_configured_acu</c> of 0 would be a
    /// ceiling of zero.</para>
    /// </summary>
    [Fact]
    public void ANullPerformanceInsightsValueStaysNull_ForEveryNewMetric()
    {
        var sample = Assert.Single(RdsCpuIngestor.BuildSamples(
            new[]
            {
                Point("os.cpuUtilization.total.avg", Now, 100.0),
                Point("os.general.acuUtilization.avg", Now, null),
                Point("os.general.serverlessDatabaseCapacity.avg", Now, null),
                Point("os.general.maxConfiguredAcu.avg", Now, null),
            },
            watermark: null));

        Assert.Equal(100.0, sample.CpuPercent);
        Assert.Null(sample.AcuUtilizationPercent);
        Assert.Null(sample.ServerlessCapacityAcu);
        Assert.Null(sample.MaxConfiguredAcu);

        /* Which is exactly the input the Unknown band is for, joined up end to end rather than asserted
           twice in isolation: a stored NULL must reach the card as Unknown. */
        Assert.Equal(
            HealthSeverity.Unknown,
            Card(MonitoredEngineKind.AuroraPostgres,
                instanceCpu: sample.CpuPercent,
                acuUtilization: sample.AcuUtilizationPercent).CpuSeverity);
    }

    /// <summary>
    /// Four metric queries come back as four <c>MetricList</c> entries, and their data points carry no
    /// metric of their own — so the response has to be keyed by <c>Key.Metric</c>. A flattened sequence,
    /// which is what the single-metric form did and what a reviewer would not notice, stores four unlabelled
    /// values per minute as four CPU readings.
    ///
    /// <para>Made adversarial rather than convenient: the CPU entry is LAST in the list and the capacity
    /// values are chosen so a flattened read would produce a plausible number. An entry with a null
    /// <c>Key</c> is included because the SDK omits response members rather than emptying them.</para>
    /// </summary>
    [Fact]
    public void TheFourMetricQueriesAreKeyedByName_NotFlattened()
    {
        var minute = Now;
        var next = Now.AddMinutes(1);

        var samples = RdsCpuIngestor.BuildSamples(
            new[]
            {
                new MetricKeyDataPoints { Key = null, DataPoints = [new DataPoint { Timestamp = minute, Value = 999.0 }] },
                Series("os.general.maxConfiguredAcu.avg", (minute, 12.0), (next, 12.0)),
                Series("os.general.serverlessDatabaseCapacity.avg", (minute, 4.0), (next, 6.0)),
                Series("os.general.acuUtilization.avg", (minute, 33.33), (next, 50.0)),
                Series("os.cpuUtilization.total.avg", (minute, 100.0), (next, 49.2)),
            },
            watermark: null);

        Assert.Equal(2, samples.Count);

        /* Ordered by sample time, whatever order the metric entries arrived in. */
        Assert.Equal(minute, samples[0].SampleTime);
        Assert.Equal(next, samples[1].SampleTime);

        Assert.Equal(100.0, samples[0].CpuPercent);
        Assert.Equal(33.33, samples[0].AcuUtilizationPercent);
        Assert.Equal(4.0, samples[0].ServerlessCapacityAcu);
        Assert.Equal(12.0, samples[0].MaxConfiguredAcu);

        Assert.Equal(49.2, samples[1].CpuPercent);
        Assert.Equal(50.0, samples[1].AcuUtilizationPercent);
        Assert.Equal(6.0, samples[1].ServerlessCapacityAcu);
        Assert.Equal(12.0, samples[1].MaxConfiguredAcu);

        /* The unkeyed entry's 999.0 reached no field of either row. */
        var everyStoredValue = samples
            .SelectMany(s => new[]
            {
                s.CpuPercent, s.AcuUtilizationPercent, s.ServerlessCapacityAcu, s.MaxConfiguredAcu,
            })
            .ToList();

        Assert.DoesNotContain((double?)999.0, everyStoredValue);

        /* Naive-UTC-unsafe values never leave here: the store's timestamp columns reject Kind=Utc, and the
           collector's payload writer is what specifies Unspecified — so the row carries Utc deliberately. */
        Assert.All(samples, s => Assert.Equal(DateTimeKind.Utc, s.SampleTime.Kind));
    }

    /// <summary>
    /// The row's identity is the CPU sample, which is a DELIBERATE choice and not an oversight: the resume
    /// watermark is <c>MAX(sample_time)</c>, so a capacity-only row would advance it past a CPU point that
    /// arrived one cycle later and retire that point unread. A minute with capacity and no CPU therefore
    /// produces no row, exactly as it did before this change.
    /// </summary>
    [Fact]
    public void AMinuteWithCapacityButNoCpu_ProducesNoRow()
    {
        Assert.Empty(RdsCpuIngestor.BuildSamples(
            new[]
            {
                Series("os.general.acuUtilization.avg", (Now, 33.33)),
                Series("os.general.maxConfiguredAcu.avg", (Now, 12.0)),
            },
            watermark: null));

        /* And the watermark still governs which CPU samples are new, unchanged by the extra metrics. */
        var samples = RdsCpuIngestor.BuildSamples(
            new[]
            {
                Series("os.cpuUtilization.total.avg", (Now, 100.0), (Now.AddMinutes(1), 20.0)),
                Series("os.general.acuUtilization.avg", (Now, 33.33), (Now.AddMinutes(1), 10.0)),
            },
            watermark: Now);

        var only = Assert.Single(samples);
        Assert.Equal(Now.AddMinutes(1), only.SampleTime);
        Assert.Equal(10.0, only.AcuUtilizationPercent);
    }

    /* ─────────────────────── the reads carry the columns ─────────────────────── */

    /// <summary>
    /// Both cross-server reads select the capacity columns, and — the part worth stating — neither filters
    /// on them. A second <c>IS NOT NULL</c> would drop a card's current CPU reading in order to find an
    /// older row that happened to carry a capacity sample, trading a current measurement for a stale one.
    /// </summary>
    [Fact]
    public void BothFleetReadsSelectTheCapacityColumns_AndFilterOnNeither()
    {
        foreach (var sql in new[]
                 {
                     DarlingFleetReader.FleetPgCpuSql,
                     ViewerDataService.ServerSummaryPgCpuSql,
                 })
        {
            Assert.Contains("acu_utilization_percent", sql, StringComparison.Ordinal);
            Assert.Contains("max_configured_acu", sql, StringComparison.Ordinal);

            /* The CPU filter stays — a NULL cpu_percent row is not the newest measurement. */
            Assert.Contains("cpu_percent IS NOT NULL", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("acu_utilization_percent IS NOT NULL", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("max_configured_acu IS NOT NULL", sql, StringComparison.Ordinal);
        }

        /* The alert's and the MCP read's own SQL carry all three, because those two report the allocation
           in ACUs as well as the ratio. */
        foreach (var sql in new[]
                 {
                     DarlingPgCpuUtilizationReader.LatestCpuSql,
                     DarlingPgCpuUtilizationReader.HistorySql,
                 })
        {
            Assert.Contains("acu_utilization_percent", sql, StringComparison.Ordinal);
            Assert.Contains("serverless_capacity_acu", sql, StringComparison.Ordinal);
            Assert.Contains("max_configured_acu", sql, StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The MCP read's note no longer claims the reading is "true OS-level utilization, not CloudWatch's
    /// capacity-relative CPUUtilization metric". That sentence was the distinction a reader was meant to
    /// trust, and it does not survive a serverless instance class: Performance Insights' figure is
    /// capacity-relative too, to a different denominator. Pinned as a negative because the text is the
    /// whole deliverable of that half of the issue — the tool answers to an operator who cannot check it.
    /// </summary>
    [Fact]
    public void TheMcpReadNoLongerClaimsTheReadingIsNotCapacityRelative()
    {
        var source = RepoFile.ReadRepoFileLf(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgCpuUtilizationTools.cs");

        var note = source[source.IndexOf("note =", StringComparison.Ordinal)..];
        note = note[..note.IndexOf("samples =", StringComparison.Ordinal)];

        Assert.DoesNotContain("true OS-level utilization", note, StringComparison.Ordinal);
        Assert.Contains("currently allocated", note, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("acu_utilization_percent", note, StringComparison.Ordinal);
        Assert.Contains("never headroom", note, StringComparison.Ordinal);

        /* The claim is gone from the whole consumer-facing surface, not just relocated into the tool's
           Description where an agent reads it first. */
        Assert.DoesNotContain("true OS-level utilization", source, StringComparison.Ordinal);
    }

    /* ─────────────────────── the rung ─────────────────────── */

    [Fact]
    public void TheRungIsRegisteredAtTheTopOfADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(
            "pg-cpu-capacity-headroom",
            PgMigrations.Scripts.Single(s => s.Version == RungVersion).Name);

        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(StorageVersion.SchemaVersion, versions.Max());
        Assert.Equal(RungVersion, StorageVersion.SchemaVersion);

        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        var above = versions.Where(v => v > 45).OrderBy(v => v).ToList();
        Assert.Equal(Enumerable.Range(above[0], above.Count), above);
    }

    /// <summary>
    /// The rung ADDS and drops nothing, and the columns are asserted individually rather than counted — a
    /// count passes while naming the wrong column, and the write side indexes these positionally.
    ///
    /// <para>V106's own text carries the same three columns, because a store created FRESH runs the
    /// generated schema from that rung and never applies this one. Nothing forces the two texts to agree
    /// except <c>PgSchemaGeneratorTests</c>, and a drift between them is a permanent invisible split
    /// between stores created before and after today.</para>
    /// </summary>
    [Fact]
    public void TheRungAddsEveryCapacityColumn_AndSoDoesTheCreatingRung()
    {
        var rung = PgMigrations.Scripts.Single(s => s.Version == RungVersion).Sql;
        var creating = PgMigrations.Scripts.Single(s => s.Version == 106).Sql;

        foreach (var column in new[]
                 {
                     "acu_utilization_percent double precision",
                     "serverless_capacity_acu double precision",
                     "max_configured_acu double precision",
                 })
        {
            Assert.Contains("ADD COLUMN IF NOT EXISTS " + column, rung, StringComparison.Ordinal);
            Assert.Contains(column, creating, StringComparison.Ordinal);
        }

        /* Schema-qualified for the reason every rung here is: the migrate session's search_path puts
           collect first, so a bare name resolves wherever that points rather than where the rung meant. */
        Assert.Contains("ALTER TABLE collect.pg_cpu_utilization", rung, StringComparison.Ordinal);

        Assert.DoesNotContain("DROP COLUMN", rung, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DROP TABLE", rung, StringComparison.OrdinalIgnoreCase);

        /* Nullable with no DEFAULT: PI holds no history this store can reach for minutes already recorded,
           and a 0 would claim measured headroom for a window nobody measured. */
        Assert.DoesNotContain("DEFAULT", rung, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("NOT NULL", rung.Replace("IF NOT EXISTS", "", StringComparison.Ordinal), StringComparison.Ordinal);
    }

    /// <summary>
    /// The connect-time gate. A COLUMN sentinel, because <c>collect.pg_cpu_utilization</c> has existed since
    /// V106 and table existence cannot separate the rungs. <c>acu_utilization_percent</c> is the one chosen
    /// because it is what every CPU band reads: without it a card and the High CPU alert both fall to
    /// Unknown, which is the failure this probe exists to prevent.
    ///
    /// <para><b>The top-rung guard.</b> Every sentinel true must map to exactly this version, or the viewer
    /// refuses a fully-migrated store forever. The all-true argument list is built by reflection so the
    /// arity tracks the signature: the literal-true form silently defaults a newly added sentinel to false
    /// and maps one version low, which is the failure this guard exists for. When a later rung lands, this
    /// clause moves to it and the equality below becomes <c>ProbeOrdinal &lt; arity - 1</c> here.</para>
    /// </summary>
    [Fact]
    public void TheProbeAsksForTheCapacityColumn_AndMapsAFullyMigratedStoreToThisRung()
    {
        Assert.Contains(
            "table_name = 'pg_cpu_utilization'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);
        Assert.Contains(
            "column_name = 'acu_utilization_percent'",
            ViewerDataService.StoreSchemaProbeSql, StringComparison.Ordinal);

        var viewer = RepoFile.ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.Contains("hasPgCpuCapacityHeadroom", viewer, StringComparison.Ordinal);

        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var method = typeof(ViewerDataService)
            .GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var arity = method.GetParameters().Length;

        Assert.Equal(ProbeOrdinal, arity - 1);

        var all = Enumerable.Repeat((object)true, arity).ToArray();
        Assert.Equal(StorageVersion.SchemaVersion, (int)method.Invoke(null, all)!);

        /* One rung behind: every sentinel EXCEPT this one must report 114. Without this the arm above could
           be satisfied by an unconditional return and nothing would notice. */
        var allButMine = Enumerable.Repeat((object)true, arity).ToArray();
        allButMine[ProbeOrdinal] = false;
        Assert.Equal(PreviousVersion, (int)method.Invoke(null, allButMine)!);
    }

    /* ─────────────────────── helpers ─────────────────────── */

    private static MetricKeyDataPoints Point(string metric, DateTime timestamp, double? value) =>
        new()
        {
            Key = new ResponseResourceMetricKey { Metric = metric },
            DataPoints = [new DataPoint { Timestamp = timestamp, Value = value }],
        };

    private static MetricKeyDataPoints Series(string metric, params (DateTime At, double Value)[] points) =>
        new()
        {
            Key = new ResponseResourceMetricKey { Metric = metric },
            DataPoints = points
                .Select(p => new DataPoint { Timestamp = p.At, Value = p.Value })
                .ToList(),
        };
}
