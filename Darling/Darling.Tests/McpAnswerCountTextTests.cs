/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A count in a tool answer or a service message reads the way a person says it: "1 sample" and "3 samples", "1 table" and
/// "33 tables", never "N sample(s)". Each fact builds the real text with a count of one and a count above one and reads the
/// realized words, so a count that drifted back to the "(s)" form, or a verb left disagreeing with its count, fails here.
/// </summary>
public sealed class McpAnswerCountTextTests
{
    private static readonly DateTime T0 = new(2026, 8, 23, 10, 0, 0, DateTimeKind.Utc);

    [Theory]
    [InlineData(1, 1, "in 1 of 1 sample, but its horizon age never rose")]
    [InlineData(3, 10, "in 3 of 10 samples, but its horizon age never rose")]
    public void SessionStateHorizonFinding_CountsSamplesAsAPersonWould(int holderSamples, int sampleCount, string expected)
    {
        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(Row(sampleCount, holderSamples, peakHorizonAge: 0));

        Assert.Contains(expected, finding, StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", finding, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, "1 object matched")]
    [InlineData(3, "3 objects matched")]
    [InlineData(1200, "1,200 objects matched")]
    public void StoreMetricsListNote_CountsMatchedObjectsAsAPersonWould(int matched, string expected)
    {
        var note = DarlingMcpStoreMetricsTools.ViewNote(
            DarlingStoreMetricsReader.StoreMetricsView.List, kind: "hypertable", name: null, matched, returned: Math.Min(matched, 50), truncated: false, limit: 50);

        Assert.StartsWith(expected, note, StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", note, StringComparison.Ordinal);
    }

    [Fact]
    public void CycleSkipReport_OneSlotOnOneCollector_ReadsSingular()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("query_stats", 1, T0.AddMinutes(-3));

        var report = DarlingCycleSkip.Build(tally.TryTake(T0, DarlingCycleSkip.RowMinInterval)!, new CycleSkipCauses(185_000, "query_store", 170_000, 0), T0);

        Assert.NotNull(report);
        Assert.Contains("1 slot skipped across 1 collector (top: query_stats 1)", report!.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", report.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void CycleSkipReport_ThreeSlotsOnTwoCollectors_ReadsPlural()
    {
        var tally = new CycleSkipAccumulator();
        tally.AddGrid("query_stats", 2, T0.AddMinutes(-3));
        tally.AddGrid("wait_stats", 1, T0.AddMinutes(-2));

        var report = DarlingCycleSkip.Build(tally.TryTake(T0, DarlingCycleSkip.RowMinInterval)!, new CycleSkipCauses(185_000, "query_store", 170_000, 90_000, GateWaitAnalysisHolders: 1), T0);

        Assert.NotNull(report);
        Assert.Contains("3 slots skipped across 2 collectors (top: query_stats 2, wait_stats 1)", report!.Message, StringComparison.Ordinal);
        Assert.Contains("(1 slot was held by scheduled analysis passes when it got one)", report.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("(s)", report.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1, 1, 1, "Purged 1 table: 1 row deleted, 1 chunk dropped")]
    [InlineData(33, 1200, 42, "Purged 33 tables: 1,200 rows deleted, 42 chunks dropped")]
    public void RetentionRunRecord_CountsTablesRowsAndChunksAsAPersonWould(int tables, int rows, int chunks, string expected)
    {
        var (status, message) = DarlingRetention.BuildRunRecordSummary(tables, rows, chunks, tablesFailed: 0);

        Assert.Equal("SUCCESS", status);
        Assert.StartsWith(expected, message, StringComparison.Ordinal);
    }


    private static DarlingPgSessionStatesReader.PgSessionStateRow Row(int sampleCount, int horizonHolderSamples, long peakHorizonAge)
        => new(
            BackendId: 17_874_796_750_069_283,
            Pid: 69_283,
            DatabaseName: "appdb",
            Username: "app_user",
            ApplicationName: "checkout-worker",
            ClientAddr: null,
            BackendType: "client backend",
            LastState: "idle in transaction",
            LastWaitEventType: "Client",
            LastWaitEvent: "ClientRead",
            LastCommandTag: "UPDATE",
            LastQueryId: 111,
            FirstSeenAt: new DateTime(2026, 8, 23, 10, 0, 0, DateTimeKind.Unspecified),
            LastSeenAt: new DateTime(2026, 8, 23, 10, 10, 0, DateTimeKind.Unspecified),
            SampleCount: sampleCount,
            IdleInTransactionSamples: 0,
            HorizonHolderSamples: horizonHolderSamples,
            PeakStateDurationMs: 1_000,
            PeakXactDurationMs: 1_000,
            PeakQueryDurationMs: 1_000,
            PeakBackendDurationMs: 2_000,
            PeakHorizonAge: peakHorizonAge,
            PeakXminAge: -1,
            PeakXidAge: peakHorizonAge,
            StateWasRedacted: false,
            TotalSessions: 20,
            ActiveSessions: 2,
            IdleInTransactionSessions: 4,
            ReportableSessions: 4,
            CaptureWasTruncated: false);
}
