/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6132, Darling side: the two wording defects in <c>analyze_server</c> answers. The engine is shared with Lite
/// (<c>Lite.Tests</c> drives the same code through Lite's service); here it is driven through Darling's own
/// coverage builder, which both SQL Server and PostgreSQL targets use.
/// </summary>
public sealed class AnalyzeServerWordingTests
{
    private static readonly DateTime End = new(2026, 10, 19, 12, 0, 0, DateTimeKind.Utc);

    private static AnalysisContext ContextWith(long sampleCount, DateTime? first, DateTime? last)
    {
        var context = new AnalysisContext { ServerName = "target", TimeRangeStart = End.AddHours(-4), TimeRangeEnd = End };
        context.Coverage = PgFactCollector.BuildCoverage(
            context, nominalMs: 4 * 3_600_000.0, sampleCount, observedSeconds: 0, largestDiscardedSeconds: 0,
            orphanCount: sampleCount > 0 ? 1 : 0, first, last);
        return context;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void ACollectorThatResumedSecondsAgo_IsToldAsResumed_NotAsStoppedOrBroken(int pointInTimeFacts)
    {
        var resumedAt = End.AddSeconds(-50);
        var context = ContextWith(1, resumedAt, resumedAt);

        var message = WindowEmptyText.Compose(context, dataSpanHours: 216.0, pointInTimeFacts);

        Assert.Contains("Collection was missing until about 2026-10-19 11:59:10 UTC and has resumed.", message, StringComparison.Ordinal);
        Assert.Contains("NOT an all-clear", message, StringComparison.Ordinal);
        Assert.DoesNotContain("stopped", message, StringComparison.Ordinal);
        Assert.DoesNotContain("broken", message, StringComparison.Ordinal);
        Assert.Equal(pointInTimeFacts > 0, message.Contains("point-in-time", StringComparison.Ordinal));
    }

    [Fact]
    public void AWindowWithNoCollectionAtAll_SaysNothingLanded_AndClaimsNeitherOutcome()
    {
        var context = ContextWith(0, null, null);

        var message = WindowEmptyText.Compose(context, dataSpanHours: 216.0, factCount: 0);

        Assert.Contains("No collection landed inside it", message, StringComparison.Ordinal);
        Assert.Contains("NOT an all-clear", message, StringComparison.Ordinal);
        Assert.DoesNotContain("has resumed.", message, StringComparison.Ordinal);
        Assert.DoesNotContain("appears to have stopped", message, StringComparison.Ordinal);
    }

    private static AnalysisStory StoryFor(string key, string hash) => new()
    {
        RootFactKey = key,
        StoryPathHash = hash,
        StoryPath = key,
        Path = [key],
        Severity = 1.0,
        Category = "test",
        StoryText = FactAdvice.SerializeForStoryText(new AdviceBlock($"{key} headline", $"{key} investigation.", $"{key} remediation.")),
    };

    [Fact]
    public void ASettingsStoryIsNotRecurring_ButATimeSeriesStoryThatFiredTheSameWeeksIs()
    {
        var slot = new DateTime(2026, 10, 19, 12, 0, 0);
        PriorOccurrence Prior(string hash, int weeksAgo) => new(hash, "X", slot.AddDays(-7 * weeksAgo), null);

        var setting = StoryFor("CONFIG_PG_SHARED_BUFFERS", "h-setting");
        var series = StoryFor("PG_WAIT_IO", "h-series");
        var facts = new List<Fact>
        {
            new() { Source = PgTargetSources.ConfigSource, Key = "CONFIG_PG_SHARED_BUFFERS", Value = 1 },
            new() { Source = PgTargetSources.WaitsSource, Key = "PG_WAIT_IO", Value = 1 },
        };
        var prior = new PriorOccurrenceRead(
            0, slot.AddMinutes(37),
            [Prior("h-setting", 1), Prior("h-setting", 2), Prior("h-series", 1), Prior("h-series", 2)]);

        RecurrenceLabeler.Label([setting, series], facts, prior);

        Assert.Null(RecurrenceLabeler.TryReadLabel(setting.StoryText));
        Assert.Equal(3, RecurrenceLabeler.TryReadLabel(series.StoryText)!.RecurrenceWeeks);
    }
}
