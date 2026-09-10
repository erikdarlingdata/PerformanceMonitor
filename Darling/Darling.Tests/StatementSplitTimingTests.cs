/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the open-vs-drain timing split (#2164). It exists because a single blended <c>sql:</c> number could
/// not answer the question a 5x payload cut raised on production: the byte budget moved bytes 5x and the
/// batch clock ~0%, so the cost is upstream of shipping — but WHICH statement was unprovable from the log,
/// and the next fix would have been a guess. Open time (everything before the first rowset) and drain time
/// (row streaming) have different fixes, so they must be separately visible.
/// </summary>
public sealed class StatementSplitTimingTests
{
    private static CollectorContext NewContext() => new()
    {
        ServerId = 1,
        ServerName = "s",
        CollectionTime = new DateTime(2026, 8, 10, 0, 0, 0, DateTimeKind.Utc),
        Deltas = new CollectorDeltaCalculator(),
    };

    [Fact]
    public void OpenMs_DefaultsToZero_SoAnUnmeasuredHostIsNotReadAsInstant()
    {
        /* Lite does not measure this today. Zero must mean "not measured", which is why the log only emits
           the split when the value is positive rather than printing "open:0ms" and inviting the reader to
           conclude the aggregate was free. */
        Assert.Equal(0, NewContext().PerItemOpenMs);
    }

    [Theory]
    /* An aggregate-bound pass: nearly all the batch is spent before the first row arrives, so no client
       byte budget can shorten it — the query_store shape measured on the field server. */
    [InlineData(100_000L, 0L, 98_000L, 2_000L)]
    /* A drain-bound pass: rows are cheap to produce and expensive to move, where the budget IS the lever. */
    [InlineData(100_000L, 0L, 3_000L, 97_000L)]
    /* The watermark phase is a STORE round trip the driver's stopwatch already started before. It must come
       out of drain, not inflate it — the review catch this arithmetic exists to prevent. */
    [InlineData(100_000L, 40_000L, 55_000L, 5_000L)]
    /* Degenerate: phases exceeding the batch total (skew across separate stopwatches) must clamp at zero
       rather than print a negative drain, which would read as a measurement bug in the field. */
    [InlineData(5_000L, 3_000L, 6_000L, 0L)]
    public void DrainExcludesWatermarkAndOpen_AndNeverGoesNegative(long sqlMs, long watermarkMs, long openMs, long expectedDrain)
    {
        var context = NewContext();
        context.PerItemWatermarkMs = watermarkMs;
        context.PerItemOpenMs = openMs;

        /* Calls the SHIPPED arithmetic (CollectorContext.DrainMsFrom) — the log line calls the same method,
           so this cannot drift into pinning a copy of the formula the way the first cut did. */
        Assert.Equal(expectedDrain, context.DrainMsFrom(sqlMs));
    }

    [Fact]
    public void EveryPhaseAccountedFor_ThePartsNeverExceedTheWhole()
    {
        /* The split's contract as a reader sees it: wm + open + drain == the sql: total, so nothing is
           silently unattributed. Holds for any measurement where the phases fit inside the total. */
        var context = NewContext();
        context.PerItemWatermarkMs = 1_200;
        context.PerItemOpenMs = 300_000;
        const long sqlMs = 350_000;

        Assert.Equal(sqlMs, context.PerItemWatermarkMs + context.PerItemOpenMs + context.DrainMsFrom(sqlMs));
    }

    /// <summary>
    /// #2312: the separate plan-XML and text fetches run INSIDE the driver's <c>sql:</c> stopwatch but are
    /// their own queries against the Query Store catalogs — on omega-01 a 0-row closed-only cycle still cost
    /// 298s and the blended number could not say where. They must come out of drain exactly like the
    /// watermark phase, or drain silently absorbs the one cost this investigation needs isolated.
    /// </summary>
    [Fact]
    public void DrainExcludesTheSeparateFetchPhases()
    {
        var context = NewContext();
        context.PerItemWatermarkMs = 5_000;
        context.PerItemOpenMs = 1_000;
        context.PerItemPlanFetchMs = 200_000;
        context.PerItemTextFetchMs = 90_000;
        const long sqlMs = 300_000;

        Assert.Equal(4_000, context.DrainMsFrom(sqlMs));
        Assert.Equal(sqlMs,
            context.PerItemWatermarkMs + context.PerItemOpenMs
            + context.PerItemPlanFetchMs + context.PerItemTextFetchMs + context.DrainMsFrom(sqlMs));
    }

    /// <summary>Zero must mean "no separate fetch ran" — the log gates its long form on exactly that.</summary>
    [Fact]
    public void FetchPhases_DefaultToZero_SoAFetchlessCollectorIsNotReadAsMeasured()
    {
        var context = NewContext();
        Assert.Equal(0, context.PerItemPlanFetchMs);
        Assert.Equal(0, context.PerItemTextFetchMs);
    }

    /// <summary>
    /// #2811: the fetch phases pinned above are each a whole METHOD — store connection open, a store probe
    /// round trip, at most two target statements, and a store write. Three of those four steps are Postgres,
    /// which is why a 189,562ms plan_fetch was read as SQL Server query time for a day and tuned on that
    /// premise: the tuning measured 0.508s in isolation and moved production nothing. These pins hold the
    /// sub-split to the one property that makes it trustworthy — the parts SUM to the parent, so no
    /// milliseconds hide between the printed terms.
    /// </summary>
    [Fact]
    public void PlanSubSplit_PartsSumToTheParent_SoNoMillisecondsAreUnattributed()
    {
        var context = NewContext();
        context.PerItemPlanFetchMs = 189_562;
        context.PerItemPlanProbeMs = 120;
        context.PerItemPlanTargetMs = 1_240;
        context.PerItemPlanWriteMs = 188_000;

        /* other: is the residual by construction, not a fourth measurement — that is what makes the
           identity exact rather than approximate. */
        Assert.Equal(202, context.PlanFetchOtherMs);
        Assert.Equal(context.PerItemPlanFetchMs,
            context.PerItemPlanProbeMs + context.PerItemPlanTargetMs
            + context.PerItemPlanWriteMs + context.PlanFetchOtherMs);
    }

    [Fact]
    public void TextSubSplit_PartsSumToTheParent_SoNoMillisecondsAreUnattributed()
    {
        var context = NewContext();
        context.PerItemTextFetchMs = 16_031;
        context.PerItemTextProbeMs = 90;
        context.PerItemTextTargetMs = 14_500;
        context.PerItemTextWriteMs = 1_400;

        Assert.Equal(41, context.TextFetchOtherMs);
        Assert.Equal(context.PerItemTextFetchMs,
            context.PerItemTextProbeMs + context.PerItemTextTargetMs
            + context.PerItemTextWriteMs + context.TextFetchOtherMs);
    }

    /// <summary>
    /// The two readings the whole change exists to tell apart. Same 189,562ms parent, opposite conclusions:
    /// one says tune the target query, the other says the target query is irrelevant and the store is the
    /// cost. A blended number cannot distinguish them, and a night was spent arguing both from the same log.
    /// </summary>
    [Theory]
    /* Store-bound: the target statement is a rounding error. Hinting it can only ever move ~1.2s of 189s. */
    [InlineData(189_562L, 120L, 1_240L, 188_000L, 202L)]
    /* Target-bound: the same parent, but now the SQL Server statement IS the bill and a hint is the lever. */
    [InlineData(189_562L, 120L, 188_000L, 1_240L, 202L)]
    public void PlanSubSplit_DistinguishesAStoreBoundPassFromATargetBoundOne(
        long fetchMs, long probeMs, long targetMs, long writeMs, long expectedOther)
    {
        var context = NewContext();
        context.PerItemPlanFetchMs = fetchMs;
        context.PerItemPlanProbeMs = probeMs;
        context.PerItemPlanTargetMs = targetMs;
        context.PerItemPlanWriteMs = writeMs;

        Assert.Equal(expectedOther, context.PlanFetchOtherMs);
        Assert.Equal(fetchMs, probeMs + targetMs + writeMs + context.PlanFetchOtherMs);
    }

    /// <summary>
    /// Stopwatch skew across four separate watches must never print a negative residual — the same clamp
    /// argument as <c>DrainMsFrom</c>, and for the same reason: a negative reads as a measurement bug in the
    /// field and discredits every other number on the line.
    /// </summary>
    [Fact]
    public void SubSplitResiduals_ClampAtZero_RatherThanPrintingNegativeTime()
    {
        var context = NewContext();
        context.PerItemPlanFetchMs = 1_000;
        context.PerItemPlanProbeMs = 600;
        context.PerItemPlanTargetMs = 600;
        context.PerItemPlanWriteMs = 600;
        Assert.Equal(0, context.PlanFetchOtherMs);

        context.PerItemTextFetchMs = 1_000;
        context.PerItemTextProbeMs = 600;
        context.PerItemTextTargetMs = 600;
        context.PerItemTextWriteMs = 600;
        Assert.Equal(0, context.TextFetchOtherMs);
    }

    /// <summary>
    /// A large <c>other:</c> is itself the finding — it would mean the cost is in the collector's own
    /// bookkeeping (candidate capping, the size estimator, the carry-over maths) rather than in either
    /// database. The residual must therefore be able to express that, not be defined away as noise.
    /// </summary>
    [Fact]
    public void Residual_SurfacesTimeSpentInNeitherDatabase()
    {
        var context = NewContext();
        context.PerItemPlanFetchMs = 60_000;
        context.PerItemPlanProbeMs = 100;
        context.PerItemPlanTargetMs = 200;
        context.PerItemPlanWriteMs = 300;

        Assert.Equal(59_400, context.PlanFetchOtherMs);
    }

    /// <summary>
    /// The id and chunk counts exist so a production pass can be compared to a benchmark honestly. A
    /// benchmark over hot recently-executed plans and a production pass over cold missing ones differ by
    /// orders of magnitude per id, and comparing the totals is exactly how a 0.508s measurement came to be
    /// believed about a 189s phase.
    /// </summary>
    [Fact]
    public void IdCounts_MakeThePerIdCostComputable()
    {
        var context = NewContext();
        context.PerItemPlanTargetMs = 1_240;
        context.PerItemPlanChunks = 2;
        context.PerItemPlanIdsAttempted = 512;

        Assert.Equal(512, context.PerItemPlanIdsAttempted);
        Assert.Equal(2, context.PerItemPlanChunks);
        /* 2.42ms/id here; the hot-plan benchmark that misled us was ~1.6ms/id. Same order — which is the
           point: only the per-id figure makes the two comparable at all. */
        Assert.Equal(2.42, Math.Round((double)context.PerItemPlanTargetMs / context.PerItemPlanIdsAttempted, 2));
    }

    /// <summary>Zero must mean "not measured" for the sub-phases too, on the same contract as their parents.</summary>
    [Fact]
    public void SubPhases_DefaultToZero()
    {
        var context = NewContext();
        Assert.Equal(0, context.PerItemPlanProbeMs);
        Assert.Equal(0, context.PerItemPlanTargetMs);
        Assert.Equal(0, context.PerItemPlanWriteMs);
        Assert.Equal(0, context.PerItemPlanChunks);
        Assert.Equal(0, context.PerItemPlanIdsAttempted);
        Assert.Equal(0, context.PerItemTextProbeMs);
        Assert.Equal(0, context.PerItemTextTargetMs);
        Assert.Equal(0, context.PerItemTextWriteMs);
        Assert.Equal(0, context.PerItemTextChunks);
        Assert.Equal(0, context.PerItemTextIdsAttempted);
        Assert.Equal(0, context.PlanFetchOtherMs);
        Assert.Equal(0, context.TextFetchOtherMs);
    }
}
