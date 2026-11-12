/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8576: five small Viewer defects, held through the functions that word them: the Active Queries plan tab title and header, the
/// Query Store row's age cue, "Generate now" on a read-only seat, and the capture and analysis stamps that sat on a second clock.
/// </summary>
[Collection("viewer-time-statics")]
public sealed class ViewerSmallDefects8576Tests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;

    public ViewerSmallDefects8576Tests() => ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.UTC;

    public void Dispose() => ViewerTimeHelper.CurrentDisplayMode = _savedMode;

    private static readonly DateTime Captured = new(2026, 10, 28, 12, 0, 0, DateTimeKind.Utc);

    // ── 1: the plan tab title ──────────────────────────────────────────────────

    [Theory]
    [InlineData(false, "Estimated Plan - Session 126")]
    [InlineData(true, "Live Plan - Session 126")]
    public void ThePlanTabTitle_UsesAPlainHyphen(bool live, string expected)
    {
        var title = ViewerActiveQueriesText.PlanTabLabel(live, 126);

        Assert.Equal(expected, title);
        Assert.DoesNotContain('—', title);
    }

    // ── 4: the header ───────────────────────────────────────────────────────────

    [Fact]
    public void TheActiveQueriesHeader_NamesTheCapturesTheGridShows_NotWhatRunsNow()
    {
        Assert.Equal("Captured queries", ViewerActiveQueriesText.SectionHeader);
    }

    // ── 3: Generate now on a read-only seat ────────────────────────────────────

    [Fact]
    public void GenerateNow_OnAReadOnlySeat_IsOffWithTheReadOnlyReason()
    {
        var (enabled, tip) = ViewerCollectorRun.GenerateNowState(readOnly: true);

        Assert.False(enabled);
        Assert.StartsWith("This viewer is connected read-only", tip, StringComparison.Ordinal);
        Assert.EndsWith(ViewerReadOnlyException.HowToConnectAsAdmin, tip, StringComparison.Ordinal);
        Assert.NotEqual(ViewerCollectorRun.GenerateNowReadyTip, tip);
    }

    [Fact]
    public void GenerateNow_OnAWritableSeat_IsOnWithItsOrdinaryTip_AndOffWhileItsOwnRunGoes()
    {
        Assert.Equal((true, ViewerCollectorRun.GenerateNowReadyTip), ViewerCollectorRun.GenerateNowState(readOnly: false));
        Assert.Equal((false, ViewerCollectorRun.GenerateNowReadyTip), ViewerCollectorRun.GenerateNowState(readOnly: false, analyzing: true));
    }

    // ── 2: the Query Store row's age cue ───────────────────────────────────────

    private static QueryStoreHealthRow HealthRow(int intervalMinutes) => new()
    {
        CaptureTime = new DateTime(2026, 10, 20, 12, 0, 0, DateTimeKind.Utc),
        DatabaseName = "db",
        CollectorIntervalMinutes = intervalMinutes,
    };

    [Fact]
    public void AQueryStoreRowOlderThanItsCollectorsCadence_SaysHowOldItIs()
    {
        var text = HealthRow(60).CollectedText(new DateTime(2026, 11, 10, 9, 0, 0, DateTimeKind.Utc));

        Assert.Equal("2026-10-20 12:00:00 (20 days 21 hours old)", text);
    }

    [Fact]
    public void AQueryStoreRowWithinItsCollectorsCadence_SaysNothingOfAge()
    {
        var row = HealthRow(60);

        Assert.Equal("2026-10-20 12:00:00", row.CollectedText(row.CaptureTime.AddMinutes(90)));
        Assert.Equal("2026-10-20 12:00:00", row.CollectedText(row.CaptureTime.AddMinutes(120)));
        Assert.Equal("2026-10-20 12:00:00 (2 hours old)", row.CollectedText(row.CaptureTime.AddMinutes(121)));
    }

    [Fact]
    public void AQueryStoreRowWhoseCadenceIsNotKnown_SaysNothingOfAge()
    {
        Assert.Equal("2026-10-20 12:00:00", HealthRow(0).CollectedText(new DateTime(2026, 11, 10, 9, 0, 0, DateTimeKind.Utc)));
    }

    // ── 5: the capture and analysis stamps follow the page's clock ──────────────

    private static ViewerDatabaseProtectionRead ProtectionRead()
    {
        var fact = new DatabaseProtectionFact(
            "a", "ONLINE", "FULL", Captured.AddDays(-400), null, "native",
            Captured.AddDays(-1), "checked", Captured.AddDays(-1), "recorded", Captured.AddMinutes(-5), "recorded");
        var banded = new List<(DatabaseProtectionFact Fact, DatabaseProtectionBand Band)>
        {
            (fact, DatabaseProtectionBands.Evaluate(fact, Captured, DatabaseProtectionThresholds.Default)),
        };
        return new ViewerDatabaseProtectionRead(ViewerDatabaseProtectionRead.Ok, null, Captured, banded, DatabaseProtectionBands.Summarize(banded));
    }

    /// <summary>The zone label the page's own refresh stamp ends in for <paramref name="mode"/>.</summary>
    private static string RefreshStampZone(TimeDisplayMode mode) =>
        ViewerTimeHelper.RefreshedStamp(Captured, mode)[(ViewerTimeHelper.RefreshedStamp(Captured, mode).IndexOf(' ') + 1)..];

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void TheCaptureTimeOnDatabaseProtection_IsOnTheClockOfTheRefreshStamp(TimeDisplayMode mode)
    {
        ViewerTimeHelper.CurrentDisplayMode = mode;

        var line = ViewerDatabaseProtectionText.StatusLine(ProtectionRead());

        var expected = ViewerTimeHelper.FormatFleetWide(Captured, "yyyy-MM-dd HH:mm:ss", mode);
        Assert.EndsWith("Captured: " + expected, line, StringComparison.Ordinal);
        Assert.EndsWith(RefreshStampZone(mode), line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCaptureTimeOnDatabaseProtection_InServerTimeMode_ReadsUtcAsTheRefreshStampDoes()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;

        Assert.EndsWith("Captured: 2026-10-28 12:00:00 UTC", ViewerDatabaseProtectionText.StatusLine(ProtectionRead()), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(TimeDisplayMode.ServerTime)]
    [InlineData(TimeDisplayMode.UTC)]
    [InlineData(TimeDisplayMode.LocalTime)]
    public void TheLastAnalyzedLine_IsOnTheClockOfTheRefreshStamp(TimeDisplayMode mode)
    {
        var analyzed = new DateTime(2026, 7, 15, 14, 0, 0, DateTimeKind.Utc);

        var line = RecommendationsViewModel.FormatLastAnalyzed(analyzed, mode);

        Assert.Equal("Last analyzed " + ViewerTimeHelper.FormatFleetWide(analyzed, "yyyy-MM-dd HH:mm:ss", mode), line);
    }

    [Fact]
    public void TheLastAnalyzedLine_InServerTimeMode_ReadsUtcAsTheRefreshStampDoes()
    {
        var analyzed = new DateTime(2026, 7, 15, 14, 0, 0, DateTimeKind.Utc);

        Assert.Equal(
            "Last analyzed 2026-07-15 14:00:00 UTC",
            RecommendationsViewModel.FormatLastAnalyzed(analyzed, TimeDisplayMode.ServerTime));
    }
}
