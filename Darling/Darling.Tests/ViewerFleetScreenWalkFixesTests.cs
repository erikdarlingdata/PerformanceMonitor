/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling Viewer's fleet screens from the final walk of the release: the Overview card's Last Collect
/// date (D4), one named clock on the Alert History and Job History time columns (D5), the Alert History row-cap note
/// (D7), the Overview search narrowing the roll-up above the cards (D9) and a stale Availability Group badge (D10).
/// Every check is a view-model or pure helper call; none needs a window.
/// </summary>
public sealed class ViewerFleetScreenWalkFixesTests : IDisposable
{
    private readonly TimeDisplayMode _savedMode = ViewerTimeHelper.CurrentDisplayMode;
    private readonly ServerClock _savedClock = ViewerTimeHelper.ActiveServerClock;
    private readonly CultureInfo _savedCulture = CultureInfo.CurrentCulture;

    public ViewerFleetScreenWalkFixesTests()
    {
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
    }

    public void Dispose()
    {
        ViewerTimeHelper.CurrentDisplayMode = _savedMode;
        ViewerTimeHelper.ActiveServerClock = _savedClock;
        CultureInfo.CurrentCulture = _savedCulture;
    }

    private static DateTime Utc(int y, int mo, int d, int h, int mi, int s = 0) => new(y, mo, d, h, mi, s, DateTimeKind.Unspecified);

    // ── D4: the Overview card's Last Collect carries its date when it is not today ─────────────────

    [Fact]
    public void LastCollect_FromToday_IsTimeOnly()
    {
        var text = ServerSummaryItem.FormatLastCollect(Utc(2026, 10, 8, 9, 56, 40), TimeZoneInfo.Utc, Utc(2026, 10, 8, 15, 0));
        Assert.Equal("09:56:40", text);
    }

    [Fact]
    public void LastCollect_FromAnotherDay_CarriesTheDate()
    {
        var text = ServerSummaryItem.FormatLastCollect(Utc(2026, 9, 25, 9, 56, 40), TimeZoneInfo.Utc, Utc(2026, 10, 8, 15, 0));
        Assert.Equal("2026-09-25 09:56:40", text);
    }

    [Fact]
    public void LastCollect_TodayIsJudgedInTheDisplayZone_NotInUtc()
    {
        /* 03:30 UTC on the 8th is 23:30 on the 7th at UTC-4; "now" 12:00 UTC is the 8th there: a different day. */
        var minusFour = TimeZoneInfo.CreateCustomTimeZone("walk-minus-four", TimeSpan.FromHours(-4), "walk", "walk");
        var text = ServerSummaryItem.FormatLastCollect(Utc(2026, 10, 8, 3, 30), minusFour, Utc(2026, 10, 8, 12, 0));
        Assert.Equal("2026-10-07 23:30:00", text);
    }

    // ── D5: one named clock on the time columns ─────────────────────────────────────────────────────

    [Theory]
    [InlineData(TimeDisplayMode.UTC, "Time (UTC)")]
    [InlineData(TimeDisplayMode.LocalTime, "Time (local)")]
    [InlineData(TimeDisplayMode.ServerTime, "Time (server)")]
    public void TimeColumnTitle_NamesTheClockOfTheChosenMode(TimeDisplayMode mode, string expected)
    {
        Assert.Equal(expected, TimeColumnTitle.For("Time", mode));
    }

    [Fact]
    public void JobHistoryRun_ConvertsOnItsOwnServersClock_NotTheActiveServers()
    {
        ViewerTimeHelper.CurrentDisplayMode = TimeDisplayMode.ServerTime;
        ViewerTimeHelper.ActiveServerClock = ServerClock.FixedOffset(0);
        var dto = new DarlingJobHistoryRow(
            1, "SQL2019", 1, "job", "Nightly", true, null, 0, null, 1, "Succeeded", Utc(2026, 10, 8, 12, 0), 5, 0, null, null, false);

        var pacific = ViewerJobHistoryRow.From(dto, ServerClock.FixedOffset(-420));
        var noClock = ViewerJobHistoryRow.From(dto);

        Assert.Equal("2026-10-08 05:00:00", pacific.RunTimeLocal);
        Assert.Equal("2026-10-08 12:00:00", noClock.RunTimeLocal);
    }

    // ── D7: the Alert History count says when the read hit its cap ──────────────────────────────────

    [Fact]
    public void AlertCount_AtTheCap_SaysShowingTheNewest()
    {
        Assert.Equal("500 alert(s) (showing the newest 500)", JobHistoryCap.CountText(500, 500, 500, "alert(s)"));
    }

    [Fact]
    public void AlertCount_ColumnFilterNarrowingTheGrid_KeepsTheReadsCapNote()
    {
        Assert.Equal("120 alert(s) (showing the newest 500)", JobHistoryCap.CountText(120, 500, 500, "alert(s)"));
    }

    [Fact]
    public void AlertCount_BelowTheCap_HasNoNote_AndNothingShownIsBlank()
    {
        Assert.Equal("37 alert(s)", JobHistoryCap.CountText(37, 37, 500, "alert(s)"));
        Assert.Equal("", JobHistoryCap.CountText(0, 500, 500, "alert(s)"));
    }

    [Fact]
    public void JobCount_WordingIsUnchanged()
    {
        Assert.Equal("2000 run(s) (showing the newest 2,000)", JobHistoryCap.CountText(2000, 2000, 2000));
    }

    [Fact]
    public void AlertsTab_ReadsAndLabelsWithTheSameCap()
    {
        Assert.Equal(500, PerformanceMonitor.Darling.Viewer.AlertsHistoryTab.RowCap);
    }

    // ── D9: the Overview search narrows the roll-up above the cards ─────────────────────────────────

    private static ServerSummaryItem Healthy(string name, int id) =>
        new() { DisplayName = name, ServerName = name, ServerId = id, IsOnline = true };

    private static ServerSummaryItem Offline(string name, int id) =>
        new() { DisplayName = name, ServerName = name, ServerId = id, IsOnline = false };

    [Fact]
    public void Rollup_WithNoSearch_IsTheWholeFleet()
    {
        var cards = new List<ServerSummaryItem> { Healthy("prod-a", 1), Offline("prod-b", 2), Offline("test-c", 3) };

        var rollup = OverviewCardView.BuildRollup(cards, new FleetTotals(), registeredCount: 4, search: "  ");

        Assert.Equal(4, rollup.TotalServers);
        Assert.Null(rollup.FilteredOfTotal);
        Assert.Equal("Monitoring 4 servers", rollup.MonitoringText);
        Assert.Equal(2, rollup.WorstServers.Count);
    }

    [Fact]
    public void Rollup_WithASearch_CountsAndRanksOnlyTheMatchingServers()
    {
        var cards = new List<ServerSummaryItem> { Healthy("prod-a", 1), Offline("prod-b", 2), Offline("test-c", 3) };

        var rollup = OverviewCardView.BuildRollup(cards, new FleetTotals(), registeredCount: 4, search: "prod");

        Assert.Equal(2, rollup.TotalServers);
        Assert.Equal(4, rollup.FilteredOfTotal);
        Assert.Equal("Monitoring 2 of 4 servers", rollup.MonitoringText);
        Assert.Equal(new[] { "prod-b" }, rollup.WorstServers.Select(w => w.DisplayName).ToArray());
        Assert.Equal(0, rollup.UnknownCount);
    }

    [Fact]
    public void Rollup_WithASearchThatMatchesNothing_ReadsZeroOfTotal()
    {
        var cards = new List<ServerSummaryItem> { Healthy("prod-a", 1) };

        var rollup = OverviewCardView.BuildRollup(cards, new FleetTotals(), registeredCount: 1, search: "zzz");

        Assert.Equal("Monitoring 0 of 1 server", rollup.MonitoringText);
        Assert.Empty(rollup.WorstServers);
    }

    // ── D10: an Availability Group on old data is Stale, never Healthy ──────────────────────────────

    private static AgTopologyReplicaRow Replica(DateTime collected) => new()
    {
        ServerId = 1,
        ServerName = "ag-fixture",
        CollectionTime = collected,
        AgName = "AG1",
        ReplicaServerName = "ag-fixture",
        RoleDesc = "PRIMARY",
        IsLocal = true,
        OperationalStateDesc = "ONLINE",
        ConnectedStateDesc = "CONNECTED",
        RecoveryHealthDesc = "ONLINE",
        SynchronizationHealthDesc = "HEALTHY",
        AvailabilityModeDesc = "SYNCHRONOUS_COMMIT",
        FailoverModeDesc = "AUTOMATIC",
    };

    [Fact]
    public void AgCard_On13DayOldData_IsStaleAndWarning_NotHealthy()
    {
        var now = Utc(2026, 10, 8, 12, 0);

        var card = AgTopology.BuildCards(new[] { Replica(Utc(2026, 9, 25, 12, 0)) }, Array.Empty<AgTopologyDatabaseRow>(), now).Single();

        Assert.True(card.IsStale);
        Assert.Equal("Stale", card.SeverityLabel);
        Assert.Equal(HealthSeverity.Warning, card.Severity);
    }

    [Fact]
    public void AgCard_WithinTheOfflineThreshold_StaysHealthy()
    {
        var now = Utc(2026, 10, 8, 12, 0);

        var card = AgTopology.BuildCards(new[] { Replica(now.AddMinutes(-29)) }, Array.Empty<AgTopologyDatabaseRow>(), now).Single();

        Assert.False(card.IsStale);
        Assert.Equal("Healthy", card.SeverityLabel);
    }

    [Fact]
    public void AgCard_StaleMark_ChangesTheDigest_SoTheTabRedrawsEvenWhenTheRowsDidNot()
    {
        var rows = new[] { Replica(Utc(2026, 10, 8, 11, 45)) };

        var fresh = AgTopology.BuildCards(rows, Array.Empty<AgTopologyDatabaseRow>(), Utc(2026, 10, 8, 12, 0));
        var later = AgTopology.BuildCards(rows, Array.Empty<AgTopologyDatabaseRow>(), Utc(2026, 10, 8, 14, 0));

        Assert.NotEqual(AgTopology.ComputeDigest(fresh), AgTopology.ComputeDigest(later));
    }

    [Fact]
    public void AgCard_StaleMark_SurvivesAnInPlaceUpdate()
    {
        var rows = new[] { Replica(Utc(2026, 9, 25, 12, 0)) };
        var shown = AgTopology.BuildCards(rows, Array.Empty<AgTopologyDatabaseRow>(), Utc(2026, 9, 25, 12, 5)).Single();
        Assert.False(shown.IsStale);

        shown.UpdateFrom(AgTopology.BuildCards(rows, Array.Empty<AgTopologyDatabaseRow>(), Utc(2026, 10, 8, 12, 0)).Single());

        Assert.True(shown.IsStale);
        Assert.Equal("Stale", shown.SeverityLabel);
    }
}
