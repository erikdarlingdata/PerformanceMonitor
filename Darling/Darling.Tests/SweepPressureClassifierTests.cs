/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Decision-table pins for the shared <see cref="SweepPressureClassifier"/> (#2296) — the roll-up both
/// SKUs' get_collection_health serve so half-rate collection stops being visible only as a service-log
/// warning. This SAME table is pinned identically in Lite.Tests so the two SKUs cannot drift.
///
/// <para>The load-bearing case is the motivating measurement: prod-sql-use2-multi-01's four heavy
/// collectors averaged 22,141 + 16,590 + 13,544 + 8,437 ms against a 60s cadence — the body could not
/// fit, every relaunch was skipped (~50 warnings/hour), the server collected at half rate, and all 40
/// collectors read HEALTHY, because from each one's own seat nothing was wrong.</para>
///
/// <para>#2446 added the second dimension and the second load-bearing case, which is the OPPOSITE shape:
/// prod-sql-use2-multi-49 logged six skipped relaunches in three hours while reading OK at 20.4%, because
/// its 37-second collector runs once a day and amortizes to 26 ms/min. The pins below assert both — that
/// the new dimension catches it, and that the VERDICT is unmoved by it, which is the whole reason the two
/// are separate fields.</para>
///
/// <para>#2460 fixed the INPUT to that second dimension. #2446 built the aligned cycle out of each
/// collector's mean, which is a contradiction on a collector whose runs come in two sizes — and the same
/// server has one. <c>query_store</c> averaged 13,834 ms over 1,155 runs of which 958 carried the
/// empty-enumeration note and cost about 36 ms each, so its 197 PRODUCTIVE runs cost ~80,933 ms apiece:
/// each one, on its own, larger than the whole 60,000 ms budget. The collectors now carry a p95 beside
/// the mean and the cycle is charged <see cref="SweepPressureClassifier.PeakRunMs"/>. The pins below add
/// the three properties that has to have: it finds the ~67,000 ms #2446 could not see, it moves the
/// VERDICT not at all, and it can never compute a SMALLER cycle than #2446 did.</para>
/// </summary>
public sealed class SweepPressureClassifierTests
{
    /// <summary>
    /// A UNIMODAL collector: every run costs about the mean, so its p95 IS its mean. Every #2446 fixture
    /// below is built from this overload deliberately — with p95 == avg the classifier must reproduce
    /// #2446's published numbers to the digit, which makes those pins a regression baseline for #2460
    /// rather than just history.
    /// </summary>
    private static (string, double, double, int) C(string name, double avgMs, int freqMin) => (name, avgMs, avgMs, freqMin);

    /// <summary>A collector whose runs come in two sizes: the mean it reports, and the p95 a heavy run costs.</summary>
    private static (string, double, double, int) C(string name, double avgMs, double p95Ms, int freqMin) => (name, avgMs, p95Ms, freqMin);

    /* --- The two measured servers, as fixtures. Both come from get_collection_health on the dogfood
       fleet; the tier roll-ups stand in for the ~35 cheap collectors whose individual names are not the
       point, and are the real per-tier sums, so each fixture reconciles to the busy_ms_per_minute the
       tool actually reported for that server. --- */

    /// <summary>
    /// prod-sql-use2-multi-49 (#2446): 12,248 ms/min sustained — comfortably OK — and a 73,408 ms body on
    /// the cycle where every cadence coincides. index_object_stats is 37,207 ms of that in one run.
    ///
    /// <para>Every collector here is UNIMODAL by default — p95 == mean — which is the conservative
    /// assumption and the one #2446 made implicitly for all of them. <paramref name="queryStoreP95Ms"/>
    /// is the one that is known NOT to hold: pass 80,933 for the measured bimodal profile. That figure is
    /// not invented. It is what the store's own numbers force — 958 runs carrying the empty-enumeration
    /// note at the 36 ms prod-sql-use2-alpha-01 pays for the identical note, 197 productive runs, and a
    /// reported mean of 13,834 ms over all 1,155 — and it reconciles: 958 x 36 + 197 x 80,933 over 1,155
    /// runs gives 13,834.02 ms, the mean the tool reported.</para>
    /// </summary>
    private static IReadOnlyList<(string, double, double, int)> Multi49(
        double indexObjectStatsMs = 37_207,
        double queryStoreP95Ms = 13_834) => new[]
    {
        C("procedure_stats", 3_205, 1),
        C("query_stats", 2_674, 1),
        C("other_one_minute_collectors", 948, 1),
        C("query_store", 13_834, queryStoreP95Ms, 5),
        C("plan_correction", 11_246, 5),
        C("other_five_minute_collectors", 1_680, 5),
        C("hourly_collectors", 2_614, 60),
        C("index_object_stats", indexObjectStatsMs, 1440),
    };

    /// <summary>
    /// prod-sql-use2-alpha-01: the negative control, and a real one — same fleet, same collectors, same
    /// box, no skipped relaunches in the log. Its index_object_stats averages 4,097 ms, not 37,207.
    ///
    /// <para>Unimodal throughout, and its query_store measurably so: that collector yields nothing on ALL
    /// 1,551 of its runs here and pays 36 ms for each, so mean, p95 and max are the same 36 ms. This is
    /// the fixture that has to stay quiet after #2460 — a change that makes the healthy control fire is
    /// not a sharper signal, it is a broken one.</para>
    /// </summary>
    private static IReadOnlyList<(string, double, double, int)> Apex() => new[]
    {
        C("query_stats", 2_562, 1),
        C("procedure_stats", 461, 1),
        C("other_one_minute_collectors", 635, 1),
        C("plan_correction", 1_501, 5),
        C("other_five_minute_collectors", 1_174, 5),
        C("hourly_collectors", 777, 60),
        C("index_object_stats", 4_097, 1440),
    };

    /// <summary>The #2296 measurement verbatim: ~101% of the minute — SATURATED, not a warning-log easter egg.</summary>
    [Fact]
    public void TheMotivatingServerReadsSaturated()
    {
        var pressure = SweepPressureClassifier.Compute(new[]
        {
            C("procedure_stats", 22_141, 1),
            C("query_store", 16_590, 1),
            C("plan_correction", 13_544, 1),
            C("query_stats", 8_437, 1),
        });

        Assert.Equal(SweepPressureClassifier.Saturated, pressure.Verdict);
        Assert.Equal(60_712, pressure.BusyMsPerMinute, 3);
        Assert.True(pressure.BusyPercent > 100.0);

        /* #2446: on a server saturated by per-minute collectors the two dimensions AGREE — every cycle is
           the aligned cycle when everything runs every minute. The dimensions being orthogonal does not
           mean they must disagree; it means neither can be derived from the other. */
        Assert.Equal(60_712, pressure.PeakCycleMs, 3);
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, pressure.PeakCycleRisk);
        Assert.Equal("procedure_stats", pressure.PeakCollectorName);
    }

    /// <summary>An ordinary in-region profile sits far below every threshold, on both dimensions.</summary>
    [Fact]
    public void AHealthyProfileReadsOk()
    {
        var pressure = SweepPressureClassifier.Compute(new[]
        {
            C("wait_stats", 180, 1),
            C("cpu_utilization", 95, 1),
            C("query_stats", 2_400, 1),
            C("database_size_stats", 1_200, 60),
        });

        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
        Assert.True(pressure.BusyPercent < 5.0);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.True(pressure.PeakCyclePercent < 10.0);
    }

    /// <summary>
    /// The band edges, both inclusive: 45,000 ms/min is exactly 75% (AT_RISK), 60,000 exactly 100%
    /// (SATURATED). Inclusive because the average already smooths spikes — a body that AVERAGES the
    /// boundary is over it half the time.
    /// </summary>
    [Fact]
    public void TheBandEdgesAreInclusive()
    {
        Assert.Equal(SweepPressureClassifier.Ok,
            SweepPressureClassifier.Compute(new[] { C("a", 44_999, 1) }).Verdict);
        Assert.Equal(SweepPressureClassifier.AtRisk,
            SweepPressureClassifier.Compute(new[] { C("a", 45_000, 1) }).Verdict);
        Assert.Equal(SweepPressureClassifier.AtRisk,
            SweepPressureClassifier.Compute(new[] { C("a", 59_999, 1) }).Verdict);
        Assert.Equal(SweepPressureClassifier.Saturated,
            SweepPressureClassifier.Compute(new[] { C("a", 60_000, 1) }).Verdict);
    }

    /// <summary>
    /// A non-recurring collector (frequency 0: on-load, unknown name) contributes nothing however long it
    /// runs — it does not compete for the sweep. A zero-duration entry likewise adds nothing.
    /// </summary>
    [Fact]
    public void OnLoadAndZeroDurationCollectorsAreExcluded()
    {
        var pressure = SweepPressureClassifier.Compute(new[]
        {
            C("database_config", 500_000, 0),
            C("trace_flags", 0, 1),
            C("wait_stats", 300, 1),
        });

        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
        Assert.Equal(300, pressure.BusyMsPerMinute, 3);

        /* #2446: the SAME exclusion on the peak cycle, and it is load-bearing there too. An on-load
           collector runs on connect, not in any scheduled cycle, so a 500-second one must not manufacture
           a BODY_OVERRUN on a server whose recurring body is 300 ms. It must also not be able to become
           the peak collector, which would name the wrong thing at the top of the block. */
        Assert.Equal(300, pressure.PeakCycleMs, 3);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.Equal("wait_stats", pressure.PeakCollectorName);
    }

    /// <summary>
    /// Amortization is by each collector's OWN cadence: an hourly collector averaging 30s costs 500 ms of
    /// every minute, not 30,000 — the mistake this pin forbids is charging slow collectors at the fast
    /// cadence, which would flag every server with a heavy daily job.
    /// </summary>
    [Fact]
    public void SlowCollectorsAreAmortizedByTheirOwnCadence()
    {
        var pressure = SweepPressureClassifier.Compute(new[] { C("index_object_stats", 30_000, 60) });

        Assert.Equal(500, pressure.BusyMsPerMinute, 3);
        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);

        /* #2446 does NOT undo that: the peak cycle takes the same collector UNdivided, and 30s of a 60s
           budget on a server with nothing else scheduled still fits. The new dimension is not "any slow
           collector is bad". */
        Assert.Equal(30_000, pressure.PeakCycleMs, 3);
        Assert.Equal(50.0, pressure.PeakCyclePercent, 3);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
    }

    /// <summary>No collectors — a server before first collection — is OK with zero demand, never a verdict from nothing.</summary>
    [Fact]
    public void AnEmptyWindowReadsOkWithZeroDemand()
    {
        var pressure = SweepPressureClassifier.Compute(Array.Empty<(string, double, double, int)>());

        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
        Assert.Equal(0, pressure.BusyMsPerMinute);
        Assert.Equal(0, pressure.BusyPercent);

        /* #2446: and no peak collector invented out of an empty set. Null, not "" and not a zero-cost
           name, so a caller rendering the block has something to branch on. */
        Assert.Equal(0, pressure.PeakCycleMs);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.Null(pressure.PeakCollectorName);
    }

    /* ---------------------------------------------------------------------------------------------
       #2446. The case the amortized model answers correctly and an operator still reads as wrong.
       --------------------------------------------------------------------------------------------- */

    /// <summary>
    /// prod-sql-use2-multi-49 verbatim: six skipped relaunches in three hours while sweep_pressure read
    /// busy_percent 20.4, verdict OK, every collector HEALTHY. The verdict is RIGHT — sustained demand
    /// genuinely fits — and the server genuinely overruns, because index_object_stats takes 37,207 ms of
    /// a 60,000 ms body and its 1440-minute cadence amortizes that to 26 ms/min. This is the pin that
    /// fails on dev.
    /// </summary>
    [Fact]
    public void AnInfrequentHeavyCollectorReadsOkAndBodyOverrun()
    {
        var pressure = SweepPressureClassifier.Compute(Multi49());

        /* The verdict is unmoved, deliberately. An operator told SATURATED because of a once-daily
           collector learns to ignore the next SATURATED, and the capacity lever that verdict recommends
           is the wrong lever for a schedule-shape problem. */
        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
        Assert.Equal(12_248.4, pressure.BusyMsPerMinute, 1);
        Assert.Equal(20.4, pressure.BusyPercent, 1);

        /* The second dimension sees what the first cannot. */
        Assert.Equal(73_408, pressure.PeakCycleMs, 3);
        Assert.Equal(122.3, pressure.PeakCyclePercent, 1);
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, pressure.PeakCycleRisk);

        /* And it names the collector heaviest_collectors structurally cannot: that list ranks by
           amortized contribution, and on this server index_object_stats ranks LAST of the eight on that
           key while owning 62% of a single body. */
        Assert.Equal("index_object_stats", pressure.PeakCollectorName);
        Assert.Equal(37_207, pressure.PeakCollectorAvgDurationMs, 3);
        Assert.Equal(1440, pressure.PeakCollectorFrequencyMinutes);

        /* #2460: with p95 == mean for every collector this profile is #2446 verbatim, which is the point
           of pinning it that way — the tail statistic must change nothing where there is no tail. */
        Assert.Equal(37_207, pressure.PeakCollectorPeakRunMs, 3);

        var amortized = pressure.PeakCollectorAvgDurationMs / pressure.PeakCollectorFrequencyMinutes;
        Assert.True(amortized < 30, $"amortized share was {amortized} ms/min");
        Assert.True(pressure.PeakCollectorAvgDurationMs / SweepPressureClassifier.SweepBudgetMs > 0.6);
    }

    /// <summary>
    /// prod-sql-use2-alpha-01: the cry-wolf control, and the reason the threshold is where it is. Same
    /// fleet, same collector set, same 60s budget, no skipped relaunches — and it must stay quiet on BOTH
    /// dimensions. A second signal that fires on a healthy server is worth less than no second signal.
    /// </summary>
    [Fact]
    public void AGenuinelyHealthyServerTripsNeitherDimension()
    {
        var pressure = SweepPressureClassifier.Compute(Apex());

        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
        Assert.Equal(4_208.8, pressure.BusyMsPerMinute, 1);
        Assert.Equal(7.0, pressure.BusyPercent, 1);

        Assert.Equal(11_207, pressure.PeakCycleMs, 3);
        Assert.Equal(18.7, pressure.PeakCyclePercent, 1);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.Equal(string.Empty, SweepPressureClassifier.FormatPeakCycleNote(pressure));
    }

    /// <summary>
    /// The variable isolated. One collector's single-run average is the ONLY difference between the two
    /// fixtures — multi-49's index_object_stats at 37,207 ms against apex's 4,097 — and it moves the peak
    /// cycle across the budget while moving the verdict not at all (20.4% to 20.4%). That is the property
    /// the whole change exists for: the two dimensions are independent, and neither is derivable from the
    /// other.
    /// </summary>
    [Fact]
    public void OnlyTheSingleRunCostSeparatesThemAndTheVerdictDoesNotMove()
    {
        var heavy = SweepPressureClassifier.Compute(Multi49());
        var light = SweepPressureClassifier.Compute(Multi49(indexObjectStatsMs: 4_097));

        Assert.Equal(SweepPressureClassifier.Ok, heavy.Verdict);
        Assert.Equal(SweepPressureClassifier.Ok, light.Verdict);
        Assert.Equal(heavy.BusyPercent, light.BusyPercent, 1);

        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, heavy.PeakCycleRisk);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, light.PeakCycleRisk);

        /* With the daily collector down to 4s the heaviest single run on the server is query_store, and
           13,834 ms of a 60,000 ms budget is genuinely fine — the block still names a peak collector, it
           just no longer names a problem. */
        Assert.Equal("query_store", light.PeakCollectorName);
    }

    /// <summary>
    /// The peak-cycle edge, inclusive at exactly the budget for the same reason the amortized edges are —
    /// and pinned on a 1440-minute collector so the case cannot be confused with saturation: 60,000 ms
    /// once a day is 42 ms/min, which is 0.07% of the budget. The verdict reads OK on both sides of the
    /// edge while the risk flips, which is the orthogonality stated as an assertion.
    /// </summary>
    [Fact]
    public void ThePeakCycleEdgeIsInclusiveAndIndependentOfTheVerdict()
    {
        var under = SweepPressureClassifier.Compute(new[] { C("index_object_stats", 59_999, 1440) });
        var at = SweepPressureClassifier.Compute(new[] { C("index_object_stats", 60_000, 1440) });

        Assert.Equal(SweepPressureClassifier.PeakCycleFits, under.PeakCycleRisk);
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, at.PeakCycleRisk);

        Assert.Equal(SweepPressureClassifier.Ok, under.Verdict);
        Assert.Equal(SweepPressureClassifier.Ok, at.Verdict);
        Assert.True(at.BusyPercent < 1.0, $"busy_percent was {at.BusyPercent}");
    }

    /// <summary>
    /// The note is composed in the classifier, not at either SKU's tool, so the two cannot render the same
    /// finding differently — and it carries the numbers that make it actionable rather than restating the
    /// risk string. Empty on the healthy side, because a note that fires on FITS is how a signal teaches
    /// people to skip it.
    /// </summary>
    [Fact]
    public void ThePeakCycleNoteNamesTheCollectorAndItsShareOfOneBody()
    {
        var note = SweepPressureClassifier.FormatPeakCycleNote(SweepPressureClassifier.Compute(Multi49()));

        Assert.Contains("index_object_stats", note, StringComparison.Ordinal);
        Assert.Contains("73,408 ms", note, StringComparison.Ordinal);
        Assert.Contains("122.3%", note, StringComparison.Ordinal);
        Assert.Contains("37,207 ms on a heavy run", note, StringComparison.Ordinal);
        Assert.Contains("62.0% of the budget", note, StringComparison.Ordinal);
        Assert.Contains("every 1440 minutes", note, StringComparison.Ordinal);
        Assert.Contains("26 ms per minute", note, StringComparison.Ordinal);

        /* #2460's bimodal clause must NOT fire here: index_object_stats' runs all cost about the same,
           and "its MEAN run is only 37,207 ms ... understates one body by 0 ms" is how a note teaches
           people to stop reading notes. */
        Assert.DoesNotContain("bimodal", note, StringComparison.Ordinal);

        /* The sustained figure is quoted too: the note has to explain why the verdict beside it disagrees,
           or it reads as the two contradicting each other. */
        Assert.Contains("20.4%", note, StringComparison.Ordinal);

        Assert.Equal(string.Empty,
            SweepPressureClassifier.FormatPeakCycleNote(SweepPressureClassifier.Compute(Apex())));
    }

    /* ---------------------------------------------------------------------------------------------
       #2460. One mean over two populations, and what the cycle built from it was missing.
       --------------------------------------------------------------------------------------------- */

    /// <summary>
    /// The floor rule on its own, because it is the one piece of #2460 that is not obvious and the one
    /// that decides whether this change can retract an answer #2446 already gave.
    ///
    /// <para>p95 is NOT guaranteed to sit above the mean. A collector with 19 runs at 100 ms and one
    /// pathological 1,220,000 ms run has a mean of 61,095 ms and a p95 of 100 ms — the percentile is doing
    /// exactly its job, discarding the outlier, and the mean is the number that happens to remember it.
    /// Taking the p95 unconditionally would compute a 100 ms aligned cycle for that collector where #2446
    /// computed 61,095 ms, turning a BODY_OVERRUN it correctly caught into a FITS. Flooring at the mean
    /// makes #2460 monotonic: the aligned cycle can only ever go up.</para>
    /// </summary>
    [Fact]
    public void ThePeakRunIsFlooredAtTheMeanSoTheCycleCanOnlyEverRise()
    {
        Assert.Equal(80_933, SweepPressureClassifier.PeakRunMs(13_834, 80_933), 3);
        Assert.Equal(61_095, SweepPressureClassifier.PeakRunMs(61_095, 100), 3);
        Assert.Equal(37_207, SweepPressureClassifier.PeakRunMs(37_207, 37_207), 3);

        var outlier = SweepPressureClassifier.Compute(new[] { C("index_object_stats", 61_095, 100, 1440) });

        Assert.Equal(61_095, outlier.PeakCycleMs, 3);
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, outlier.PeakCycleRisk);
        Assert.Equal(61_095, outlier.PeakCollectorPeakRunMs, 3);
    }

    /// <summary>
    /// prod-sql-use2-multi-49's query_store, as the store's own numbers force it. #2446 charged the aligned
    /// cycle 13,834 ms for this collector — a mean blended 958/197 out of a 36 ms empty run and an ~80,933 ms
    /// productive one, describing neither. Charged the p95 instead, the same server's aligned body goes from
    /// 73,408 ms to 140,507 ms: the ~67,000 ms #2459 said out loud it was missing, found.
    ///
    /// <para>And the collector NAMED changes with it, which is the half that would have closed #2446 on its
    /// own. index_object_stats has the larger mean (37,207 against 13,834) and was therefore the peak
    /// collector; query_store has by far the larger heavy run, runs every 5 minutes rather than once a day,
    /// and is what actually puts that body over the budget.</para>
    /// </summary>
    [Fact]
    public void TheBimodalCollectorIsChargedItsHeavyRunAndBecomesThePeakCollector()
    {
        var pressure = SweepPressureClassifier.Compute(Multi49(queryStoreP95Ms: 80_933));

        Assert.Equal(140_507, pressure.PeakCycleMs, 3);
        Assert.Equal(234.2, pressure.PeakCyclePercent, 1);
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, pressure.PeakCycleRisk);

        Assert.Equal("query_store", pressure.PeakCollectorName);
        Assert.Equal(80_933, pressure.PeakCollectorPeakRunMs, 3);
        Assert.Equal(13_834, pressure.PeakCollectorAvgDurationMs, 3);
        Assert.Equal(5, pressure.PeakCollectorFrequencyMinutes);

        /* One run of this collector costs more than the entire budget by itself — which is the sentence
           #2446 needed and could not reach, because 13,834 ms of 60,000 reads like 23% of a body. */
        Assert.True(pressure.PeakCollectorPeakRunMs > SweepPressureClassifier.SweepBudgetMs,
            $"one heavy run was {pressure.PeakCollectorPeakRunMs} ms against a {SweepPressureClassifier.SweepBudgetMs} ms budget");
    }

    /// <summary>
    /// The variable isolated again, in the OTHER dimension. Only query_store's p95 differs between the two
    /// fixtures — the mean is 13,834 ms on both sides — so the sustained answer is bit-for-bit identical
    /// while the aligned cycle moves by 67,099 ms. That is the property #2460 exists for, and it is also
    /// the guard on the thing most likely to be "simplified" later: the verdict must keep amortizing the
    /// MEAN. Sustained demand over a window IS the mean; a rate built from a tail claims work the server
    /// never sustains.
    /// </summary>
    [Fact]
    public void TheTailMovesThePeakCycleAndLeavesTheVerdictExactlyWhereItWas()
    {
        var blended = SweepPressureClassifier.Compute(Multi49());
        var measured = SweepPressureClassifier.Compute(Multi49(queryStoreP95Ms: 80_933));

        Assert.Equal(blended.BusyMsPerMinute, measured.BusyMsPerMinute, 6);
        Assert.Equal(blended.BusyPercent, measured.BusyPercent, 6);
        Assert.Equal(SweepPressureClassifier.Ok, blended.Verdict);
        Assert.Equal(SweepPressureClassifier.Ok, measured.Verdict);

        Assert.Equal(67_099, measured.PeakCycleMs - blended.PeakCycleMs, 3);
        Assert.True(measured.PeakCycleMs > blended.PeakCycleMs,
            "#2460 must never compute a smaller aligned cycle than #2446 did");
    }

    /// <summary>
    /// The healthy control after the change, which matters more than the positive case. alpha-01's
    /// query_store is measurably unimodal — it yields nothing on all 1,551 runs and pays 36 ms every time —
    /// so nothing about it moves, and the server that logs no skipped relaunches still trips neither
    /// dimension and still gets no note. A second signal that starts firing on the quiet server is worth
    /// less than no second signal.
    /// </summary>
    [Fact]
    public void TheHealthyControlIsUnmovedByTheTailStatistic()
    {
        var pressure = SweepPressureClassifier.Compute(Apex());

        Assert.Equal(4_208.8, pressure.BusyMsPerMinute, 1);
        Assert.Equal(11_207, pressure.PeakCycleMs, 3);
        Assert.Equal(SweepPressureClassifier.PeakCycleFits, pressure.PeakCycleRisk);
        Assert.Equal(string.Empty, SweepPressureClassifier.FormatPeakCycleNote(pressure));
    }

    /// <summary>
    /// The note has to explain the disagreement it is sitting next to, and after #2460 there are two of
    /// them: the aligned cycle against the verdict, and the collector's heavy run against its own mean.
    /// Both numbers appear, the gap is stated as a number rather than left for the reader to subtract, and
    /// the amortized figure is still computed from the MEAN — 13,834 / 5, not 80,933 / 5 — because that is
    /// what the verdict beside it is made of.
    /// </summary>
    [Fact]
    public void TheNoteSaysTheMeanDescribesNeitherPopulation()
    {
        var note = SweepPressureClassifier.FormatPeakCycleNote(
            SweepPressureClassifier.Compute(Multi49(queryStoreP95Ms: 80_933)));

        Assert.Contains("query_store", note, StringComparison.Ordinal);
        Assert.Contains("140,507 ms", note, StringComparison.Ordinal);
        Assert.Contains("234.2%", note, StringComparison.Ordinal);
        Assert.Contains("80,933 ms on a heavy run", note, StringComparison.Ordinal);
        Assert.Contains("134.9% of the budget", note, StringComparison.Ordinal);
        Assert.Contains("every 5 minutes", note, StringComparison.Ordinal);
        Assert.Contains("2,767 ms per minute", note, StringComparison.Ordinal);

        Assert.Contains("MEAN run is only 13,834 ms", note, StringComparison.Ordinal);
        Assert.Contains("bimodal", note, StringComparison.Ordinal);
        Assert.Contains("understates one body by 67,099 ms", note, StringComparison.Ordinal);
    }

    /// <summary>
    /// The threshold on that clause, pinned so it cannot drift into either uselessness. At exactly twice
    /// the mean it fires — inclusive, like every other edge in this table — and a hair under it does not,
    /// because run-to-run variance is not bimodality and a note that fires on ordinary variance is a note
    /// nobody finishes reading.
    /// </summary>
    [Fact]
    public void TheBimodalClauseNeedsARealGapNotOrdinaryVariance()
    {
        var atTheEdge = SweepPressureClassifier.Compute(new[] { C("a", 45_000, 90_000, 1) });
        var justUnder = SweepPressureClassifier.Compute(new[] { C("a", 45_000, 89_999, 1) });

        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, atTheEdge.PeakCycleRisk);
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, justUnder.PeakCycleRisk);

        Assert.Contains("bimodal", SweepPressureClassifier.FormatPeakCycleNote(atTheEdge), StringComparison.Ordinal);
        Assert.DoesNotContain("bimodal", SweepPressureClassifier.FormatPeakCycleNote(justUnder), StringComparison.Ordinal);
    }

    /// <summary>
    /// A collector whose mean rounds away to nothing but whose tail does not is still part of the body.
    /// The old guard skipped any collector with a non-positive mean, which after #2460 would have dropped
    /// exactly the shape this change is about — the rare-and-enormous run — from the cycle entirely. The
    /// on-load and genuinely-idle exclusions are unchanged.
    /// </summary>
    [Fact]
    public void ACollectorWithNoMeanButARealTailStillCounts()
    {
        var pressure = SweepPressureClassifier.Compute(new[]
        {
            C("rare_and_enormous", 0, 70_000, 5),
            C("never_runs", 0, 0, 1),
            C("on_load_collector", 0, 500_000, 0),
        });

        Assert.Equal(70_000, pressure.PeakCycleMs, 3);
        Assert.Equal(SweepPressureClassifier.PeakCycleBodyOverrun, pressure.PeakCycleRisk);
        Assert.Equal("rare_and_enormous", pressure.PeakCollectorName);

        /* Nothing was measured as sustained demand, so the verdict says nothing — the two dimensions stay
           independent even at this edge, and an on-load collector still cannot reach either number. */
        Assert.Equal(0, pressure.BusyMsPerMinute, 6);
        Assert.Equal(SweepPressureClassifier.Ok, pressure.Verdict);
    }
}
