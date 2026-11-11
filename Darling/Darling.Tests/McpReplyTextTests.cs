/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8572: small defects in what an MCP reply says. Rounding where the reply is built, no em dash and no tracker number or internal word in
/// reader text, a note that reads as two sentences, a bad-actor sentence chosen from the per-execution cost, and counts with separators.
/// Each test names the reply text a reader would have seen.
/// </summary>
public sealed class McpReplyTextTests
{
    private const string EmDash = "—";

    // ── Row 1: unrounded floats ──────────────────────────────────────────────────────────────────────

    [Fact]
    public void WaitTrend_RoundsTheRatesWhereTheReplyIsBuilt()
    {
        var points = new[]
        {
            new WaitBucketPoint(new DateTime(2026, 11, 10, 10, 0, 0, DateTimeKind.Utc), 257.70384995064165, 12.123456789012, 300.55555555555),
        };

        using var doc = JsonDocument.Parse(TrendPayloads.WaitTrend("srv", "CXPACKET", 24, points, 10, false, 144, new { }));
        var point = doc.RootElement.GetProperty("trend")[0];

        Assert.Equal("257.704", point.GetProperty("wait_time_ms_per_second").GetRawText());
        Assert.Equal("12.123", point.GetProperty("signal_wait_time_ms_per_second").GetRawText());
        Assert.Equal("300.556", point.GetProperty("peak_wait_time_ms_per_second").GetRawText());
    }

    [Fact]
    public void DatabaseSizes_RoundsTheSummedTotalsWhereTheReplyIsBuilt()
    {
        /* 0.1 + 0.2 is 0.30000000000000004 in a double: the shape of 985829.2999999999 in a real reply. */
        var when = new DateTime(2026, 10, 1, 12, 0, 0, DateTimeKind.Utc);
        var rows = new[]
        {
            new DarlingObjectStatsReader.DatabaseSizeRow(when, "appdb", "d1", "ROWS", 0.1, 0.1, null, null, null, null, null, 1),
            new DarlingObjectStatsReader.DatabaseSizeRow(when, "appdb", "d2", "ROWS", 0.2, 0.2, null, null, null, null, null, 2),
        };

        using var doc = JsonDocument.Parse(DarlingMcpObjectStatsTools.DatabaseSizesPayload("srv", rows));
        var database = doc.RootElement.GetProperty("databases")[0];

        Assert.Equal("0.3", database.GetProperty("total_size_mb").GetRawText());
        Assert.Equal("0.3", database.GetProperty("used_size_mb").GetRawText());
    }

    // ── Rows 2 and 3: em dashes, tracker numbers and internal words in reader text ───────────────────

    [Fact]
    public void ExtensionMissingPrecondition_HasNoEmDash()
    {
        var message = CollectorRuntimePrecondition.CollectionOutcomeMessage(
            "srv", "pg_buffer_usage", CollectorRuntimePrecondition.ExtensionMissingStatus, null, null);

        Assert.NotNull(message);
        Assert.DoesNotContain(EmDash, message, StringComparison.Ordinal);
        Assert.Contains("Installing it is the remedy", message, StringComparison.Ordinal);
    }

    [Fact]
    public void RoutingReasons_HaveNoEmDash()
    {
        var reasons = new List<string>
        {
            FindingRouting.Classify("SOME_ROOT", 1, 0, 5, FindingRoute.Digest).Reason,
            FindingRouting.Classify("SOME_ROOT", 1, 0, 0, FindingRoute.Digest).Reason,
            FindingRouting.Classify("SOME_ROOT", 2, 1, 5, FindingRoute.Digest).Reason,
            FindingRouting.Classify("SOME_ROOT", 1, 1, 5, FindingRoute.Digest).Reason,
            FindingRouting.Classify("SOME_ROOT", 1, 1, 5, FindingRoute.Digest, severity: 0.5, notifySeverity: 0.5).Reason,
            FindingRouting.Classify("SOME_ROOT", 1, 0, 5, FindingRoute.Digest, recurringWeeks: 3).Reason,
            FindingRouting.ClassifyPersisted("SOME_ROOT", 0.2, 1, FindingRoute.Digest, 0.5, 0.5, null).Reason,
        };

        foreach (var reason in reasons)
        {
            Assert.DoesNotContain(EmDash, reason, StringComparison.Ordinal);
        }

        Assert.Contains("no second fact corroborates it. A co-fire check matched", reasons[4], StringComparison.Ordinal);
    }

    [Fact]
    public void CpuAttributionNotes_HaveNoEmDash()
    {
        var start = new DateTime(2026, 11, 10, 10, 0, 0, DateTimeKind.Utc);
        var end = start.AddMinutes(10);

        /* 50% of 4 cores for 600 s is 1200 CPU-seconds: 10 explains under half of it, 2400 is more than the process consumed. */
        var low = CpuAttribution.Compute(10, start, end, 10, start, end, 50, 4);
        var over = CpuAttribution.Compute(2400, start, end, 10, start, end, 50, 4);

        Assert.NotNull(low.Note);
        Assert.NotNull(over.Note);
        Assert.DoesNotContain(EmDash, low.Note, StringComparison.Ordinal);
        Assert.DoesNotContain(EmDash, over.Note, StringComparison.Ordinal);
    }

    // ── Row 4: the ring buffer note ──────────────────────────────────────────────────────────────────

    [Fact]
    public void RingBufferLossNote_SaysWhoFoundTheLossInItsOwnSentence()
    {
        var t = new DateTime(2026, 11, 10, 11, 26, 0, DateTimeKind.Utc);
        var window = new RingBufferLossNote.LossWindow(0, 0, 1, t, t, "ring_buffer_evicted=0 ring_buffer_loss_window_s=60 ring_buffer_events_limit=1000");

        var note = (string)window.ToPayload()["note"]!;

        Assert.Contains("1 span after a session restart could not be counted. The loss was found by the run at 2026-11-10 11:26 UTC", note, StringComparison.Ordinal);
        Assert.DoesNotContain("could not be counted found", note, StringComparison.Ordinal);
    }

    // ── Rows 9 and 10: bad-actor sentences ───────────────────────────────────────────────────────────

    private static AdviceBlock BadActor(double executions, double cpuMs, double reads)
    {
        var key = "BAD_ACTOR_abc123";
        var fact = new Fact
        {
            Source = "",
            Key = key,
            ServerId = 1,
            Metadata = new Dictionary<string, double>
            {
                ["execution_count"] = executions,
                ["avg_cpu_ms"] = cpuMs,
                ["avg_reads"] = reads,
            },
        };
        return FactAdvice.Compose(key, new Dictionary<string, Fact>(StringComparer.Ordinal) { [key] = fact })!;
    }

    [Fact]
    public void BadActor_ThatCostsASecondOfCpuEachTime_IsNotCalledIndividuallyCheap()
    {
        var advice = BadActor(1502, 1210.8, 250734);

        Assert.DoesNotContain("individually-cheap", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("expensive each time", advice.Investigation, StringComparison.Ordinal);
        /* The counts beside it carry the same grouping. */
        Assert.Contains("1,502 executions", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("averaging 1,210.8 ms CPU", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("250,734 reads each", advice.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void BadActor_ThatIsFrequentAndCheapEachTime_KeepsTheCheapSentence()
    {
        var advice = BadActor(121340, 0.1, 4);

        Assert.Contains("individually-cheap", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("expensive each time", advice.Investigation, StringComparison.Ordinal);
    }
}
