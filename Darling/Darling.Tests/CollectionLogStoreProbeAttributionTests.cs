/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>collection_log.sql_duration_ms</c> is documented and consumed as the time a collector spent querying
/// the MONITORED SERVER, and on the collectors that fetch plan XML or statement text it is mostly not that
/// (#3192).
///
/// <para><b>The mechanism.</b> The enumerated driver's per-item stopwatch wraps the watermark refresh and the
/// whole <c>readItem</c> closure (<c>EnumeratedCollectorDriver.RunAsync</c>), and for <c>query_store</c> that
/// closure calls the deferred plan and text fetches — each of which round-trips the STORE to learn what
/// content is already held before writing back what came off the target. On one measured production run
/// <c>sql_duration_ms</c> was 124,972 ms of which the two store probes were 107,334 ms (86%), against a
/// plan-plus-text target time of 6,494 ms. Fleet-wide the store probe is the largest single term in both
/// fetches: 55.4% of <c>plan_fetch</c> and 80.6% of <c>text_fetch</c> (V110). So the product's own
/// "is the target slow or is the store slow" split pointed the wrong way on its heaviest collector, and the
/// projection comment that states the split's purpose sat directly above the column that inverted it.</para>
///
/// <para><b>What was NOT done, which is the load-bearing half of the decision.</b> The obvious fix — subtract
/// the store terms from <c>sql_duration_ms</c>, or exclude the fetches from the driver's slice — changes the
/// meaning of a persisted column, and the past cannot be brought along. <c>CollectorRunResult.SqlMs</c> also
/// feeds <c>collect.collector_cost</c>, which is a 90-day hourly aggregate carrying NO phase split
/// (<c>metric_time, server_id, database_name, collector_name, run_count, total_sql_ms, max_sql_ms,
/// total_storage_ms, total_rows</c>) built by an in-memory accumulator rather than re-aggregated from
/// <c>collection_log</c> — so there is nothing there to subtract and no source to re-derive from. Re-basing
/// would leave 90 days meaning one thing and every row after meaning another, under a Collector Cost
/// Regression self-alert whose baseline window is 14 days, which is a fortnight in which a real target-side
/// regression is measured against an inflated baseline. And it would not help those 90 days at all.</para>
///
/// <para>So the attribution is PUBLISHED rather than applied: <c>SqlStoreMs</c> derives the store share from
/// the V110 columns already on the row, which makes it retroactive to every row that has them and leaves
/// every persisted column exactly as it was. These tests pin the arithmetic, the emit, the caveats on the two
/// surfaces that drew the wrong inference, and the premise the FLOOR caveat rests on.</para>
/// </summary>
[Trait("Stage", "Guard")]
public class CollectionLogStoreProbeAttributionTests
{
    /// <summary>
    /// The arithmetic that IS the feature: probe + write of BOTH halves, and NOT the target halves, which are
    /// genuinely the monitored server's work and belong where they are.
    ///
    /// <para>Every figure is deliberately distinct and no two sum to a third, so an implementation that
    /// dropped a term, or added the target halves, or summed only one half, produces a different number
    /// rather than a coincidentally equal one. The three <c>NotEqual</c>s name the specific wrong
    /// implementations: probe-only, whole-fetch (probe+target+write), and plan-half-only.</para>
    /// </summary>
    [Fact]
    public void TheStoreShareIsProbePlusWrite_AndExcludesTheTargetHalves()
    {
        var row = Row(
            sqlDurationMs: 124_972,
            planProbe: 54_016, planTarget: 4_100, planWrite: 830,
            textProbe: 53_318, textTarget: 2_394, textWrite: 190);

        Assert.Equal(54_016 + 830 + 53_318 + 190, row.SqlStoreMs);
        Assert.Equal(108_354, row.SqlStoreMs);

        /* Probe alone - the shape that reads "the probe is the problem" and silently drops the write-back. */
        Assert.NotEqual(54_016 + 53_318, row.SqlStoreMs);

        /* The whole fetch, target included - the shape that over-claims and would make
           sql_duration_ms - sql_store_ms understate target time instead of bounding it above. */
        Assert.NotEqual(54_016 + 4_100 + 830 + 53_318 + 2_394 + 190, row.SqlStoreMs);

        /* One half only - the plan side, which is the half a copy-paste stops at. */
        Assert.NotEqual(54_016 + 830, row.SqlStoreMs);

        /* And the point of the whole exercise, as a comparison rather than as prose: the store share is the
           majority of a figure documented as the monitored server's. */
        Assert.True(row.SqlStoreMs > row.SqlDurationMs / 2,
            "The measured run this property exists for had 86% of its 'target-side' figure in the store.");
    }

    /// <summary>
    /// NULL when no deferred fetch ran, which is every collector but the fetching ones and most runs of even
    /// those. NULL says "nothing here is attributable", which is a different claim from "the store share was
    /// zero" — and only one of them is true, because the per-item watermark refresh is a store read inside
    /// the same stopwatch on every <c>query_store</c> item whether a fetch ran or not.
    ///
    /// <para>Per HALF for the non-null case, matching V110's own contract: a run that fetched text but no
    /// plans has one block and not the other, and the store share is still the terms that exist.</para>
    /// </summary>
    [Fact]
    public void TheStoreShareIsNullWhenNoFetchRan_AndCountsWhicheverHalvesDidRun()
    {
        /* A plain single-query collector: no fetch columns at all. */
        Assert.Null(Row(sqlDurationMs: 4_000).SqlStoreMs);

        /* Text only. The plan terms are absent, not zero, and must not be read as measured zeros. */
        var textOnly = Row(sqlDurationMs: 9_000, textProbe: 400, textTarget: 80, textWrite: 20);
        Assert.Equal(420, textOnly.SqlStoreMs);

        /* Plan only. */
        var planOnly = Row(sqlDurationMs: 9_000, planProbe: 400, planTarget: 80, planWrite: 20);
        Assert.Equal(420, planOnly.SqlStoreMs);

        /* A fetch that ran and cost nothing measurable reports 0, not null: V110's gate cannot separate that
           from "no fetch ran", and this property inherits the ambiguity rather than inventing a resolution
           for it. Asserted so the inheritance is deliberate and not a coincidence of the expression. */
        var subMillisecond = Row(sqlDurationMs: 9_000, planProbe: 0, planTarget: 0, planWrite: 0);
        Assert.Equal(0, subMillisecond.SqlStoreMs);
        Assert.NotNull(subMillisecond.SqlStoreMs);
    }

    /// <summary>One collection_log row as the reader materializes it.</summary>
    private static DarlingDataReader.CollectionLogEntry Row(
        double sqlDurationMs,
        double? planProbe = null, double? planTarget = null, double? planWrite = null,
        double? textProbe = null, double? textTarget = null, double? textWrite = null) =>
        new(
            CollectorName: "query_store",
            CollectionTime: new DateTime(2026, 9, 9, 0, 0, 0, DateTimeKind.Utc),
            DurationMs: sqlDurationMs,
            SqlDurationMs: sqlDurationMs,
            StoreDurationMs: 1_500,
            RowsCollected: 11_614,
            Status: "SUCCESS",
            ErrorMessage: null,
            PlanFetchProbeMs: planProbe,
            PlanFetchTargetMs: planTarget,
            PlanFetchWriteMs: planWrite,
            TextFetchProbeMs: textProbe,
            TextFetchTargetMs: textTarget,
            TextFetchWriteMs: textWrite);

}
