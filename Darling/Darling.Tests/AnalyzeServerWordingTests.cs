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
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
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

    /// <summary>
    /// #8910: a window that ends before the oldest row the store keeps, with that row at the retention horizon, was removed by
    /// retention. The message says so, names the oldest row and the retention, and does not blame collection or quote the
    /// lifetime history as if the data should be there.
    /// </summary>
    [Theory]
    [InlineData(0)]
    [InlineData(2)]
    public void AWindowBeforeTheOldestRetainedRow_SaysRetentionRemovedIt_NotThatCollectionBroke(int pointInTimeFacts)
    {
        var context = ContextWith(0, null, null);
        var oldest = End.AddDays(30).AddHours(-1);
        var retention = Boundary(oldest, oldest);

        var message = WindowEmptyText.Compose(context, dataSpanHours: 731.3, pointInTimeFacts, retention);

        Assert.Contains("This window ends before the oldest row this store keeps (2026-11-18 11:00 UTC, 30 days of retention for wait_stats).", message, StringComparison.Ordinal);
        Assert.Contains("Retention removed it, and collection did not break.", message, StringComparison.Ordinal);
        Assert.Contains("NOT an all-clear", message, StringComparison.Ordinal);
        Assert.DoesNotContain("stopped", message, StringComparison.Ordinal);
        Assert.DoesNotContain("broken", message.Replace("did not break", string.Empty, StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.DoesNotContain("total collected history", message, StringComparison.Ordinal);
        Assert.Equal(pointInTimeFacts > 0, message.Contains("point-in-time", StringComparison.Ordinal));
        Assert.Equal(string.Empty, WindowEmptyText.HealthPointer(removedByRetention: true));
        Assert.Contains("get_collection_health", WindowEmptyText.HealthPointer(removedByRetention: false), StringComparison.Ordinal);
    }

    /// <summary>
    /// #8910: the two windows that keep today's text. One lies inside the span the store keeps, so collection missed it. The
    /// other ends before the store's first row, but that row is far newer than the retention horizon (a server monitored for five
    /// days, asked about ten days ago), so retention did not remove it and the message must not say it did.
    /// </summary>
    [Theory]
    [InlineData(-10, -10)]
    [InlineData(30, 5)]
    public void AWindowInsideTheSpan_OrBeforeAYoungStoresFirstRow_KeepsTheCollectionText(int oldestDaysFromWindowEnd, int horizonDaysFromWindowEnd)
    {
        var context = ContextWith(0, null, null);
        var retention = Boundary(End.AddDays(oldestDaysFromWindowEnd), End.AddDays(horizonDaysFromWindowEnd));

        var message = WindowEmptyText.Compose(context, dataSpanHours: 216.0, factCount: 0, retention);

        Assert.Contains("No collection landed inside it", message, StringComparison.Ordinal);
        Assert.Contains("Collection may have stopped or broken", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Retention removed it", message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheOldestRowMayBeADayPastTheHorizon_AndNoMore_ForRetentionToOwnTheWindow()
    {
        var horizon = End.AddDays(30);
        Assert.True(Boundary(horizon.AddHours(-3), horizon).RemovedWindow(End));
        Assert.True(Boundary(horizon.AddHours(23), horizon).RemovedWindow(End));
        Assert.False(Boundary(horizon.AddHours(25), horizon).RemovedWindow(End));
        Assert.False(Boundary(horizon, horizon).RemovedWindow(horizon.AddHours(1)));
    }

    /// <summary>
    /// #8910: a purge that has stalled leaves the oldest row far BEFORE the horizon. A window before the server was first
    /// monitored then ends before that row too, but the store never held it, and the text keeps the collection wording. A row a
    /// chunk or a purge interval before the horizon is where a running purge leaves it, and still counts as removed.
    /// </summary>
    [Fact]
    public void AStoreWhosePurgeStalled_KeepsTheCollectionText_ForAWindowBeforeItsFirstRow()
    {
        var horizon = End.AddDays(30);
        var stalled = Boundary(horizon.AddDays(-10), horizon);
        Assert.False(stalled.RemovedWindow(End));

        var message = WindowEmptyText.Compose(ContextWith(0, null, null), dataSpanHours: 960.0, factCount: 0, stalled);
        Assert.Contains("No collection landed inside it", message, StringComparison.Ordinal);
        Assert.Contains("Collection may have stopped or broken", message, StringComparison.Ordinal);
        Assert.DoesNotContain("Retention removed it", message, StringComparison.Ordinal);

        Assert.True(Boundary(horizon.AddHours(-40), horizon).RemovedWindow(End));
        Assert.False(Boundary(horizon.AddHours(-50), horizon).RemovedWindow(End));
    }

    /// <summary>
    /// #8910: each product states the latest instant its own purge leaves the oldest row at. Lite's archive purge deletes whole
    /// months, so the oldest row sits up to a month past the cutoff, and that boundary keeps the claim there. The horizon plus
    /// a slack, the one rule Darling's chunk purge needs, would not.
    /// </summary>
    [Fact]
    public void AProductWithALaterPurgeRule_OwnsTheWindow_WhereTheHorizonPlusSlackWouldNot()
    {
        var horizon = End.AddDays(30);
        var monthLater = horizon.AddDays(20);
        var lite = new RetentionBoundary(monthLater, horizon, monthLater + RetentionBoundary.HorizonSlack, horizon - TimeSpan.FromDays(1), "wait_stats", "x");
        Assert.True(lite.RemovedWindow(End));
        Assert.False(Boundary(monthLater, horizon).RemovedWindow(End));
    }

    /// <summary>A Darling-shaped boundary: the horizon plus the slack above it, one chunk and one purge interval below it.</summary>
    /// <summary>
    /// #9104: the window notice an empty answer carries (<c>hints.truncation_note</c>) closes with the retention sentence when the window
    /// is one retention removed, the same sentence the answer's message carries, and not with the pointer at collection health. With no
    /// boundary, and with a floor the probe found, it reads as it did.
    /// </summary>
    [Fact]
    public void TheEmptyWindowNotice_ForAWindowRetentionRemoved_SaysWhatTheMessageSays_AndOtherwiseKeepsItsText()
    {
        var horizon = End.AddDays(30);
        var retention = Boundary(horizon.AddHours(-3), horizon);
        var start = End.AddHours(-4);

        var removed = DarlingMcpWindowNotice.Build(null, start, "wait_stats", emptyAnswer: true, retentionRemoved: retention);
        Assert.True(removed.WindowTruncated);
        Assert.Null(removed.EffectiveStart);
        Assert.StartsWith("The store holds no collection of wait_stats for this server in this window, so nothing was read", removed.TruncationNote, StringComparison.Ordinal);
        Assert.EndsWith(retention.Describe(), removed.TruncationNote, StringComparison.Ordinal);
        Assert.DoesNotContain("may have stopped", removed.TruncationNote, StringComparison.Ordinal);
        Assert.DoesNotContain("get_collection_health", removed.TruncationNote, StringComparison.Ordinal);

        var collection = DarlingMcpWindowNotice.Build(null, start, "wait_stats", emptyAnswer: true);
        Assert.EndsWith("or collection may have stopped; get_collection_health shows which.", collection.TruncationNote, StringComparison.Ordinal);
        Assert.DoesNotContain("Retention removed it", collection.TruncationNote, StringComparison.Ordinal);

        var floored = DarlingMcpWindowNotice.Build(End.AddHours(-1), start, "wait_stats", emptyAnswer: true, retentionRemoved: retention);
        Assert.DoesNotContain("Retention removed it", floored.TruncationNote, StringComparison.Ordinal);
    }

    private static RetentionBoundary Boundary(DateTime oldest, DateTime horizon) => new(
        oldest, horizon, horizon + RetentionBoundary.HorizonSlack, horizon.AddDays(-(TimescaleSupport.ChunkIntervalDays + 1)), "wait_stats", "30 days of retention for wait_stats");

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

    [Fact]
    public void AFreeSpaceStoryAndASettingsChainAreNotRecurring_ButAWaitBurstThatFiredTheSameWeeksIs()
    {
        var slot = new DateTime(2026, 10, 19, 12, 0, 0);
        PriorOccurrence Prior(string hash, int weeksAgo) => new(hash, "X", slot.AddDays(-7 * weeksAgo), null);

        var disk = StoryFor("DISK_SPACE", "h-disk");
        var chain = StoryFor("CONFIG_PG_SHARED_BUFFERS", "h-chain");
        chain.Path = ["CONFIG_PG_SHARED_BUFFERS", "PG_WAIT_IO"];
        var burst = StoryFor("PG_WAIT_IO", "h-burst");
        var facts = new List<Fact>
        {
            new() { Source = "disk", Key = "DISK_SPACE", Value = 1 },
            new() { Source = PgTargetSources.ConfigSource, Key = "CONFIG_PG_SHARED_BUFFERS", Value = 1 },
            new() { Source = PgTargetSources.WaitsSource, Key = "PG_WAIT_IO", Value = 1 },
        };
        var prior = new PriorOccurrenceRead(
            0, slot.AddMinutes(37),
            [Prior("h-disk", 1), Prior("h-disk", 2), Prior("h-chain", 1), Prior("h-chain", 2), Prior("h-burst", 1), Prior("h-burst", 2)]);

        RecurrenceLabeler.Label([disk, chain, burst], facts, prior);

        Assert.Null(RecurrenceLabeler.TryReadLabel(disk.StoryText));
        Assert.Null(RecurrenceLabeler.TryReadLabel(chain.StoryText));
        Assert.Equal(3, RecurrenceLabeler.TryReadLabel(burst.StoryText)!.RecurrenceWeeks);
    }
}
