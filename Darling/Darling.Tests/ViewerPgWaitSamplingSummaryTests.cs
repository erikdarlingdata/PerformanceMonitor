/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6849: the Viewer's Wait Sampling note counts the rows attributed to a statement and, on the service tier,
/// carries the one shared caveat. A target below PostgreSQL 14 reports no query id, so every row has a null one and
/// none of them is attributed to anything.
/// </summary>
public sealed class ViewerPgWaitSamplingSummaryTests
{
    private static DarlingPgWaitSamplingReader.PgWaitSamplingRow Row(long? queryId) => new(
        EventType: "Lock",
        Event: "relation",
        QueryId: queryId,
        SampleCount: 5,
        EstimatedWaitMs: 5_000,
        BackendCount: 1,
        CounterReset: false,
        CaptureTime: new DateTime(2026, 10, 27, 12, 0, 0, DateTimeKind.Utc));

    [Fact]
    public void APageOfNullQueryIds_ReadsZeroAttributed()
    {
        var rows = new List<DarlingPgWaitSamplingReader.PgWaitSamplingRow> { Row(null), Row(null), Row(null) };

        Assert.Equal(0, PgDisplay.WaitSamplingAttributedCount(rows));
        Assert.StartsWith("3 wait event(s), 0 attributed to a query.",
            PgDisplay.WaitSamplingSummary(rows, serviceTier: true, instrumentKnown: true, "every 5 minutes", reset: false),
            StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyARealQueryIdIsAttributed_NotNullAndNotZero()
    {
        var rows = new List<DarlingPgWaitSamplingReader.PgWaitSamplingRow> { Row(null), Row(0), Row(42), Row(-7) };

        Assert.Equal(2, PgDisplay.WaitSamplingAttributedCount(rows));
    }

    [Fact]
    public void TheServiceTierNote_CarriesTheSharedCaveat_AndTheExtensionTierDoesNot()
    {
        var rows = new List<DarlingPgWaitSamplingReader.PgWaitSamplingRow> { Row(42) };

        Assert.Contains(PgWaitInstrument.ServiceSampledCaveat,
            PgDisplay.WaitSamplingSummary(rows, serviceTier: true, instrumentKnown: true, "every 5 minutes", reset: false),
            StringComparison.Ordinal);
        Assert.DoesNotContain(PgWaitInstrument.ServiceSampledCaveat,
            PgDisplay.WaitSamplingSummary(rows, serviceTier: false, instrumentKnown: true, "every 5 minutes", reset: false),
            StringComparison.Ordinal);
    }
}
