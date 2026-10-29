// Copyright (c) 2026 Erik Darling, Darling Data LLC
//
// This file is part of the SQL Server Performance Monitor.
//
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6903: every query_stats run on SQL Server writes a measurement note from the deferred plan fetch
/// (<c>plans_rendered=0 plans_rendered_bytes=0 plan_fetch_ms=31</c>), so the "ran clean, stored nothing" answer of
/// <see cref="CollectorRuntimePrecondition.HealthyEmptyMessage"/> never fired. A note made only of measurement counts is
/// the same as no note, and a note with any other text still blocks it. The notes here come from the producer
/// (<c>StampPlanFetch</c>, <c>CollectorMeasurementNote.Compose</c>), not from a hand-typed string.
/// </summary>
public sealed class HealthyEmptyMeasurementNoteTests
{
    private static readonly DateTime At = new(2026, 10, 29, 12, 0, 0, DateTimeKind.Utc);

    private static string? Answer(string? status, string? note) =>
        CollectorRuntimePrecondition.HealthyEmptyMessage("srv", "query_stats", "query statistics", status, note, At);

    private static string PlanFetchNote(int rows = 0, long bytes = 0, long ms = 31)
    {
        var context = new CollectorContext { ServerId = 1, ServerName = "s", CollectionTime = At, Deltas = new NoDeltas() };
        DarlingCollectorRunner.StampPlanFetch(context, rows, bytes, ms);
        return Assert.IsType<string>(CollectorMeasurementNote.Render(context.Measurements));
    }

    [Fact]
    public void ThePlanFetchNote_HasTheDocumentedShape()
    {
        Assert.Equal("plans_rendered=0 plans_rendered_bytes=0 plan_fetch_ms=31", PlanFetchNote());
    }

    [Theory]
    [InlineData(0, 0L, 31L)]
    [InlineData(3, 1234L, 17L)]
    public void AMeasurementOnlyNote_GivesTheEmptyAnswer(int rows, long bytes, long ms)
    {
        var message = Answer("SUCCESS", PlanFetchNote(rows, bytes, ms));

        Assert.NotNull(message);
        Assert.Contains("true negative", message);
        Assert.Contains("completed without a fault and its source returned no rows", message);
        Assert.DoesNotContain("or a note", message);
    }

    [Fact]
    public void AMeasurementOnlyNote_WithTheScrubCounters_GivesTheEmptyAnswer()
    {
        var note = CollectorMeasurementNote.Compose(
            null,
            new[]
            {
                new CollectorMeasurement(CollectorContext.StatementScrubNamedMeasurement, 0),
                new CollectorMeasurement(CollectorContext.StatementScrubMsMeasurement, 4),
                new CollectorMeasurement("plans_rendered", 0),
            });

        Assert.NotNull(Answer("SUCCESS", note));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ABlankNote_StillGivesTheEmptyAnswer(string? note)
    {
        Assert.NotNull(Answer("SUCCESS", note));
    }

    [Fact]
    public void ANoteWithRealText_GivesNull()
    {
        Assert.Null(Answer("SUCCESS", "2 online user database(s) have no user for this login, so nothing was collected from them."));
    }

    [Fact]
    public void AMixedNote_HostSentenceBeforeTheCounts_GivesNull()
    {
        var mixed = CollectorMeasurementNote.Compose(
            "1 of 4 databases failed to read",
            new[] { new CollectorMeasurement("plans_rendered", 0), new CollectorMeasurement("plan_fetch_ms", 31) });

        Assert.Contains("; plans_rendered=0 plan_fetch_ms=31", mixed);
        Assert.Null(Answer("SUCCESS", mixed));
    }

    [Theory]
    [InlineData("plans_rendered=0 skipped because the login lacks VIEW SERVER STATE")]
    [InlineData("plans_rendered=0 plan_fetch_ms=")]
    [InlineData("plans_rendered=0 plan_fetch_ms=3x")]
    [InlineData("plans_rendered=0 plan_fetch_ms=-5")]
    [InlineData("Plans_Rendered=0")]
    [InlineData("plans_rendered=0  plan_fetch_ms=31")]
    [InlineData("plans_rendered=0 plan_fetch_")]
    [InlineData("=5")]
    public void ANoteThatIsNotExactlyTheProducersForm_GivesNull(string note)
    {
        Assert.Null(Answer("SUCCESS", note));
    }

    [Fact]
    public void AMeasurementOnlyNote_DoesNotOverrideANonSuccessRunOrNoRun()
    {
        var note = PlanFetchNote();

        Assert.Null(Answer("ERROR", note));
        Assert.Null(CollectorRuntimePrecondition.HealthyEmptyMessage("srv", "query_stats", "query statistics", "SUCCESS", note, null));
    }

    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }
}
