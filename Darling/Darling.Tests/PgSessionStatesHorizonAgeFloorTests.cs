/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8092: the sustained-holder rule behind get_pg_session_states' Critical band, the "PINS THE XMIN HORIZON" text and
/// the Viewer's red row needs a peak horizon age VACUUM would notice, not merely one above 0. A maintenance login that
/// runs a short write transaction every few minutes is the oldest holder in every sample that sees it, at an age of 1
/// or 2: nothing waited behind it for longer than its own 30 ms, and get_pg_xmin_horizon over the same window says
/// nothing is holding the horizon back. The band must say the same.
/// </summary>
public sealed class PgSessionStatesHorizonAgeFloorTests
{
    /// <summary>The issue's repro: three samples five minutes apart, each seeing one held xid for a moment.</summary>
    private static DarlingPgSessionStatesReader.PgSessionStateRow ShortWriter(long peakHorizonAge = 2, long peakStateDurationMs = 30, int idleSamples = 0)
        => Row(sampleCount: 3, horizonHolderSamples: 3, idleInTransactionSamples: idleSamples,
            peakHorizonAge: peakHorizonAge, peakStateDurationMs: peakStateDurationMs);

    [Fact]
    public void AShortWriterSeenAtEverySample_WithATrivialHorizonAge_IsNotASustainedHolder()
    {
        var row = ShortWriter();

        Assert.False(DarlingPgSessionStatesReader.IsSustainedHolder(row));
        Assert.Equal("Healthy", DarlingMcpPgSessionStatesTools.SessionSeverity(row));

        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(row);
        Assert.DoesNotContain("PINS THE XMIN HORIZON", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("terminating the backend", finding, StringComparison.Ordinal);
        /* The text names the age it saw and the floor it fell short of, so the row explains its own band. */
        Assert.Contains("3 of 3", finding, StringComparison.Ordinal);
        Assert.Contains("2 transaction", finding, StringComparison.Ordinal);
        Assert.Contains("10,000", finding, StringComparison.Ordinal);
    }

    /// <summary>The Viewer paints the red off the same rule, so the short writer is not red there either.</summary>
    [Fact]
    public void TheViewer_DoesNotPaintTheShortWriterRed()
    {
        var display = PgDisplay.SessionState(ShortWriter());

        Assert.False(display.IsSustainedHorizonHolder);
        Assert.False(display.IsLongIdleTransaction);
        Assert.False(display.StateUnknown);
    }

    /// <summary>
    /// The same short-age holder left idle in transaction past the attention floor is a finding on the other axis, and
    /// the band and the text say Warning together rather than Critical.
    /// </summary>
    [Fact]
    public void TheShortWriter_LongIdleInTransaction_IsWarning_AndTheTextAgrees()
    {
        var row = ShortWriter(peakStateDurationMs: 600_000, idleSamples: 3);

        Assert.Equal("Warning", DarlingMcpPgSessionStatesTools.SessionSeverity(row));

        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(row);
        Assert.DoesNotContain("PINS THE XMIN HORIZON", finding, StringComparison.Ordinal);
        Assert.Contains("OTHER axis", finding, StringComparison.Ordinal);
        Assert.Contains("idle inside a transaction", finding, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(1L)]
    [InlineData(2L)]
    [InlineData(999L)]
    [InlineData(9_999L)]
    public void AHolderBelowTheAgeFloor_IsNeverCritical_HoweverManySamplesSawIt(long peakHorizonAge)
    {
        var row = Row(sampleCount: 10, horizonHolderSamples: 10, peakHorizonAge: peakHorizonAge);

        Assert.False(DarlingPgSessionStatesReader.IsSustainedHolder(row));
        Assert.NotEqual("Critical", DarlingMcpPgSessionStatesTools.SessionSeverity(row));
        Assert.DoesNotContain("PINS THE XMIN HORIZON", DarlingMcpPgSessionStatesTools.HorizonFinding(row), StringComparison.Ordinal);
    }

    /// <summary>At the floor and above, the hold is what the band has always said it is.</summary>
    [Theory]
    [InlineData(10_000L)]
    [InlineData(40_000L)]
    [InlineData(5_000_000L)]
    public void AHolderAtOrAboveTheAgeFloor_IsCritical(long peakHorizonAge)
    {
        var row = Row(sampleCount: 10, horizonHolderSamples: 10, peakHorizonAge: peakHorizonAge);

        Assert.True(DarlingPgSessionStatesReader.IsSustainedHolder(row));
        Assert.Equal("Critical", DarlingMcpPgSessionStatesTools.SessionSeverity(row));
        Assert.Contains("PINS THE XMIN HORIZON", DarlingMcpPgSessionStatesTools.HorizonFinding(row), StringComparison.Ordinal);
    }

    /// <summary>The Critical text names the age the claim rests on, since the age and not the duration is the evidence.</summary>
    [Fact]
    public void ASustainedHolder_IsToldThePeakAgeItWasSeenAt()
    {
        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(
            Row(sampleCount: 10, horizonHolderSamples: 9, idleInTransactionSamples: 10, peakHorizonAge: 40_000));

        Assert.Contains("PINS THE XMIN HORIZON", finding, StringComparison.Ordinal);
        Assert.Contains("40,000", finding, StringComparison.Ordinal);
        Assert.Contains("9 of 10", finding, StringComparison.Ordinal);
    }

    /// <summary>A passing sighting (one of ten) at a trivial age keeps its own sentence: it is not a count-sustained hold.</summary>
    [Fact]
    public void APassingSighting_AtATrivialAge_IsStillCalledAPassingSighting()
    {
        var finding = DarlingMcpPgSessionStatesTools.HorizonFinding(
            Row(sampleCount: 10, horizonHolderSamples: 1, peakHorizonAge: 2));

        Assert.Contains("passing sighting", finding, StringComparison.Ordinal);
        Assert.DoesNotContain("10,000", finding, StringComparison.Ordinal);
    }

    private static DarlingPgSessionStatesReader.PgSessionStateRow Row(
        int sampleCount = 10,
        int horizonHolderSamples = 0,
        int idleInTransactionSamples = 0,
        long peakHorizonAge = -1,
        long peakStateDurationMs = 1_000,
        bool stateWasRedacted = false)
        => new(
            BackendId: 17_874_796_750_069_283,
            Pid: 69_283,
            DatabaseName: "appdb",
            Username: "maint_user",
            ApplicationName: "nightly-maintenance",
            ClientAddr: null,
            BackendType: "client backend",
            LastState: idleInTransactionSamples > 0 ? "idle in transaction" : "active",
            LastWaitEventType: null,
            LastWaitEvent: null,
            LastCommandTag: "UPDATE",
            LastQueryId: 111,
            FirstSeenAt: new DateTime(2026, 8, 23, 10, 0, 0, DateTimeKind.Unspecified),
            LastSeenAt: new DateTime(2026, 8, 23, 10, 10, 0, DateTimeKind.Unspecified),
            SampleCount: sampleCount,
            IdleInTransactionSamples: idleInTransactionSamples,
            HorizonHolderSamples: horizonHolderSamples,
            PeakStateDurationMs: peakStateDurationMs,
            PeakXactDurationMs: peakStateDurationMs,
            PeakQueryDurationMs: peakStateDurationMs,
            PeakBackendDurationMs: peakStateDurationMs + 1_000,
            PeakHorizonAge: peakHorizonAge,
            PeakXminAge: -1,
            PeakXidAge: peakHorizonAge,
            StateWasRedacted: stateWasRedacted,
            TotalSessions: 20,
            ActiveSessions: 2,
            IdleInTransactionSessions: 4,
            ReportableSessions: 4,
            CaptureWasTruncated: false);
}
