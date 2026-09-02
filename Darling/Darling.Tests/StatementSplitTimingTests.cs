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
    /// their own queries against the Query Store catalogs — on ayr-01 a 0-row closed-only cycle still cost
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

    /* ---------------------------------------------------------------------------------------------
       #2789: splitting open: itself. open: is the staging statement PLUS the shipping SELECT, because
       that SELECT ends ORDER BY last_execution_time — a blocking sort, so the server finishes every row
       before the client sees one. That made a 0-row database costing 120 s completely unattributable.
       --------------------------------------------------------------------------------------------- */

    [Fact]
    public void SlicePhase_DefaultsToZero_SoAnUnmeasuredHostIsNotReadAsInstant()
    {
        var context = NewContext();
        Assert.Equal(0, context.PerItemSliceMs);
        Assert.Equal(0, context.PerItemSliceRows);
    }

    [Fact]
    public void PayloadIsOpenMinusSlice_SoTheTwoAlwaysSumToOpen()
    {
        var context = NewContext();
        context.PerItemOpenMs = 120_471;
        context.PerItemSliceMs = 119_900;

        Assert.Equal(571, context.PayloadMsFromOpen());
        Assert.Equal(context.PerItemOpenMs, context.PerItemSliceMs + context.PayloadMsFromOpen());
    }

    /// <summary>
    /// The slice is measured server-side with DATEDIFF while open: comes off the host's Stopwatch, so the
    /// two can disagree by a few milliseconds in either direction. Skew must clamp, never surface as a
    /// negative phase that would read as the payload having taken less than no time.
    /// </summary>
    [Fact]
    public void PayloadClampsAtZero_WhenServerClockExceedsTheHostStopwatch()
    {
        var context = NewContext();
        context.PerItemOpenMs = 1_000;
        context.PerItemSliceMs = 1_040;

        Assert.Equal(0, context.PayloadMsFromOpen());
    }

    /// <summary>
    /// The slice is a SUBSET of open, not a sibling phase, so it must stay out of the drain subtraction —
    /// counting it there would deduct the same milliseconds twice and silently deflate drain. This pins the
    /// relationship rather than the arithmetic, because the arithmetic is what a future edit would "tidy".
    /// </summary>
    [Fact]
    public void SliceIsNotSubtractedFromDrain_BecauseItIsAlreadyInsideOpen()
    {
        var context = NewContext();
        context.PerItemOpenMs = 10_000;
        const long sqlMs = 25_000;

        var drainWithoutSlice = context.DrainMsFrom(sqlMs);
        context.PerItemSliceMs = 9_500;

        Assert.Equal(drainWithoutSlice, context.DrainMsFrom(sqlMs));
    }

    [Theory]
    [InlineData("PMQS_PHASE slice_ms=1234 slice_rows=8086", 1234L, 8086L)]
    [InlineData("PMQS_PHASE slice_ms=0 slice_rows=0", 0L, 0L)]
    public void SliceTimingMessageParsesBackToItsNumbers(string message, long expectedMs, long expectedRows)
    {
        var parsed = QueryStoreCollector.TryParseSliceTiming(message);

        Assert.NotNull(parsed);
        Assert.Equal(expectedMs, parsed!.Value.SliceMs);
        Assert.Equal(expectedRows, parsed.Value.SliceRows);
    }

    /// <summary>
    /// The handler sees EVERY informational message on the connection — context changes, NOCOUNT chatter,
    /// anything the target chooses to say. Anything that is not ours must parse to null rather than to a
    /// zero, because a zero would be indistinguishable from a genuinely instant staging statement.
    /// </summary>
    [Theory]
    [InlineData("Changed database context to 'AYR'.")]
    [InlineData("PMQS_PHASE slice_ms=12")]
    [InlineData("PMQS_PHASE slice_rows=12")]
    [InlineData("")]
    [InlineData(null)]
    public void UnrelatedOrPartialMessagesParseToNull_NotToZero(string? message)
        => Assert.Null(QueryStoreCollector.TryParseSliceTiming(message));

    /// <summary>
    /// The shipped payload must actually raise the message the host listens for, and must still carry
    /// OPTION(RECOMPILE) on BOTH statements — the staging statement's own hint is load-bearing (it stops
    /// sp_executesql caching a plan sniffed across live and backfill windows), and a timing edit sitting
    /// right beside it is exactly how that would get lost. Derived from the shipped builder rather than a
    /// copy of the SQL, so it cannot pass while the real query drifts.
    /// </summary>
    [Fact]
    public void ShippedPayloadRaisesTheSliceTimingMessageAndKeepsBothRecompileHints()
    {
        var body = QueryStoreCollector.Instance.BuildPerItemQuery("AYR", NewContext()).Text;

        Assert.Contains(QueryStoreCollector.SliceTimingMarker, body, StringComparison.Ordinal);
        Assert.Contains("WITH NOWAIT", body, StringComparison.Ordinal);
        Assert.Equal(2, body.Split("OPTION(RECOMPILE)").Length - 1);
    }

    /// <summary>
    /// The on-prem path quote-doubles this body into <c>[db].sys.sp_executesql N'...'</c>, and the builder's
    /// contract is that it contains no double quotes and no braces so it survives both that escaping and the
    /// C# interpolation that produced it. The RAISERROR added in #2789 is the first new literal in that
    /// string in a while; this pins that it did not break the contract.
    /// </summary>
    [Fact]
    public void ShippedPayloadSurvivesTheOnPremNestingContract()
    {
        var body = QueryStoreCollector.Instance.BuildPerItemQuery("AYR", NewContext()).Text;

        Assert.DoesNotContain('"', body);
        Assert.DoesNotContain('{', body);
        Assert.DoesNotContain('}', body);
        Assert.Contains(QueryStoreCollector.SliceTimingMarker, body, StringComparison.Ordinal);
    }

    /// <summary>The backfill body runs the same two statements, so it must report the same way.</summary>
    [Fact]
    public void BackfillPayloadAlsoRaisesTheSliceTimingMessage()
    {
        var body = QueryStoreCollector.Instance.BuildBackfillPerItemQuery(
            "AYR",
            NewContext(),
            new DateTime(2026, 8, 1, 0, 0, 0, DateTimeKind.Utc),
            new DateTime(2026, 8, 2, 0, 0, 0, DateTimeKind.Utc)).Text;

        Assert.Contains(QueryStoreCollector.SliceTimingMarker, body, StringComparison.Ordinal);
        Assert.Equal(2, body.Split("OPTION(RECOMPILE)").Length - 1);
    }
}
