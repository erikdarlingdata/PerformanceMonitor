/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Run at line under the Collector Schedules grid, for a server that is on "Use default schedule" (#4938). The grid then shows
/// the FLEET's schedule, so the selected row's Run at cell holds the fleet's own time, and the line used to describe that as a
/// time the server had of its own. It now says the server uses the fleet-wide run time, the same sentence as for a server row set to
/// "Use default". All of it is pure, so it is tested without a window.
/// </summary>
public sealed class CollectorScheduleUseDefaultRunAtLineTests
{
    private const string Daily = "index_object_stats";
    private const int ServerId = 41;
    private static readonly DateTime Now = new(2026, 10, 2, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void UnderUseDefaultSchedule_TheRunAtLine_SaysTheServerUsesTheFleetWideRunTime()
    {
        var fleetGrid = CollectorScheduleOverlay.BuildEffectiveSchedule(
            Array.Empty<CollectorScheduleRow>(), new[] { new CollectorRunTimeRow(null, Daily, 120) }, null);
        var cell = fleetGrid.Single(i => i.Name == Daily);
        Assert.Equal("02:00", cell.RunAtText);

        var line = CollectorScheduleRunAtText.Describe(
            Daily, cell.RunAtText, cell.FrequencyMinutes, ServerId, 120, ServerClock.Utc, false, Now, usesDefaultSchedule: true);
        var slot = CollectorRunTime.SlotUtc(new DateOnly(2026, 10, 2), 120, ServerId, ServerClock.Utc.ToUtc);

        Assert.StartsWith("Uses the fleet-wide run time (02:00). ", line, StringComparison.Ordinal);
        Assert.Contains($"{slot:HH:mm} server time", line, StringComparison.Ordinal);

        /* The same cell on a server that customizes its schedule is that server's own time, and says nothing about the fleet. */
        var own = CollectorScheduleRunAtText.Describe(
            Daily, cell.RunAtText, cell.FrequencyMinutes, ServerId, 120, ServerClock.Utc, false, Now);
        Assert.DoesNotContain("fleet-wide", own, StringComparison.Ordinal);
    }

    [Fact]
    public void UnderUseDefaultSchedule_WithNoFleetWideRunTime_TheRunAtLine_SaysThereIsNoFixedTime()
    {
        var line = CollectorScheduleRunAtText.Describe(
            Daily, "Use default", 1440, ServerId, null, ServerClock.Utc, false, Now, usesDefaultSchedule: true);

        Assert.StartsWith("No fixed run time", line, StringComparison.Ordinal);
        Assert.DoesNotContain("fleet-wide", line, StringComparison.Ordinal);
    }

    /// <summary>The fleet scope has no "Use default schedule", so the flag changes nothing on a fleet row.</summary>
    [Fact]
    public void TheFleetRow_IgnoresUseDefaultSchedule()
    {
        var plain = CollectorScheduleRunAtText.Describe(Daily, "02:00", 1440, null, 120, ServerClock.Utc, false, Now);
        var flagged = CollectorScheduleRunAtText.Describe(Daily, "02:00", 1440, null, 120, ServerClock.Utc, false, Now, usesDefaultSchedule: true);

        Assert.Equal(plain, flagged);
    }

    [Fact]
    public void TheWindow_PassesUseDefaultScheduleToTheRunAtLine()
    {
        var window = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "CollectorScheduleEditorWindow.xaml.cs"));
        var refresh = window[window.IndexOf("private void RefreshRunAtDetail(", StringComparison.Ordinal)..window.IndexOf("private void RebuildEditingForScope(", StringComparison.Ordinal)];

        Assert.Contains("usesDefaultSchedule:", refresh, StringComparison.Ordinal);
        Assert.Contains("UseDefaultCheckBox.IsChecked == true", refresh, StringComparison.Ordinal);
    }
}
