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
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A Save of the Collector Schedules window that follows a failed read (#4938): no successful read, no write. A failed read leaves
/// the window holding nothing for it, which looks the same as a store with nothing stored, so a Save must not write what the window
/// never saw. The Save's contents come from a pure plan (<see cref="CollectorScheduleOverlay.BuildSavePlan"/>), tested here without
/// a window or a store, and source pins on the window check that it records a failed read, keeps its warning and sends the plan.
/// </summary>
public sealed class CollectorScheduleEditorSaveGuardTests
{
    private const string Daily = "index_object_stats";
    private const int ServerId = 41;

    /// <summary>The grid of a scope the window read nothing for: every cell the code default, every Run at cell "Use default".</summary>
    private static List<CollectorScheduleEditItem> Grid() => CollectorSchedulePresets.BuildDefaultSchedule();

    private static string Window() => CSharpSourceWalker.StripCommentsAndStrings(
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "CollectorScheduleEditorWindow.xaml.cs"));

    /// <summary>A server whose only override is a run-time row (set from the command line, say) opens on "Use default schedule" when
    /// the run-time read fails, because the window then holds no run times. A plain Save of that window, nothing changed, must not
    /// send the clear that "Use default schedule" otherwise sends: that clear deleted the run-time row it never showed.</summary>
    [Fact]
    public void ASave_AfterAFailedRunTimeRead_ThatChangedNothing_SendsNoRunTimeDeleteOrClear()
    {
        var plan = CollectorScheduleOverlay.BuildSavePlan(
            Grid(), Array.Empty<CollectorRunTimeRow>(), ServerId, usesDefault: true, resetToDefaults: false, schedulesRead: true, runTimesRead: false);

        Assert.Null(plan.Refusal);
        Assert.Empty(plan.RunTimeChanges);
        Assert.False(plan.ClearRunTimes);
        Assert.Empty(plan.Rows);
    }

    /// <summary>The control: the same window after a read that worked still clears the scope's run times on "Use default schedule",
    /// which is what that checkbox means. The guard is for the unread case only.</summary>
    [Fact]
    public void ASave_AfterASuccessfulRead_OnUseDefaultSchedule_StillClearsTheServersRunTimes()
    {
        var plan = CollectorScheduleOverlay.BuildSavePlan(
            Grid(), new[] { new CollectorRunTimeRow(ServerId, Daily, 300) }, ServerId, usesDefault: true, resetToDefaults: false, schedulesRead: true, runTimesRead: true);

        Assert.Null(plan.Refusal);
        Assert.True(plan.ClearRunTimes);
    }

    /// <summary>A server that does customize its schedule still saves it when the run-time read failed, and sends no run-time change
    /// with it: not a delete, not an upsert, not a clear.</summary>
    [Fact]
    public void ASave_AfterAFailedRunTimeRead_StillSavesTheSchedule_WithNoRunTimeChange()
    {
        var grid = Grid();
        grid.Single(i => i.Name == "wait_stats").FrequencyMinutes = 10;

        var plan = CollectorScheduleOverlay.BuildSavePlan(
            grid, Array.Empty<CollectorRunTimeRow>(), ServerId, usesDefault: false, resetToDefaults: false, schedulesRead: true, runTimesRead: false);

        Assert.Null(plan.Refusal);
        Assert.Contains(plan.Rows, r => r.CollectorName == "wait_stats" && r.FrequencyMinutes == 10);
        Assert.Empty(plan.RunTimeChanges);
        Assert.False(plan.ClearRunTimes);
    }

    /// <summary>A run time typed into a cell, or a Reset to Defaults, is something the user asked for. With the run times unread it
    /// cannot be written safely, so the Save is refused outright and says why, and sends nothing, instead of saving the rest and
    /// quietly dropping that.</summary>
    [Fact]
    public void ASave_AfterAFailedRunTimeRead_IsRefused_WhenItWouldDropATypedRunTimeOrAReset()
    {
        var typed = Grid();
        typed.Single(i => i.Name == Daily).RunAtText = "02:00";

        var withTime = CollectorScheduleOverlay.BuildSavePlan(
            typed, Array.Empty<CollectorRunTimeRow>(), ServerId, usesDefault: false, resetToDefaults: false, schedulesRead: true, runTimesRead: false);
        var withReset = CollectorScheduleOverlay.BuildSavePlan(
            Grid(), Array.Empty<CollectorRunTimeRow>(), ServerId, usesDefault: false, resetToDefaults: true, schedulesRead: true, runTimesRead: false);

        foreach (var plan in new[] { withTime, withReset })
        {
            Assert.Equal(CollectorScheduleOverlay.RunTimesUnreadRefusal, plan.Refusal);
            Assert.Empty(plan.Rows);
            Assert.Empty(plan.RunTimeChanges);
            Assert.False(plan.ClearRunTimes);
        }
    }

    /// <summary>The schedule read has the same shape: a failed read leaves the window on the code defaults, and a Save of that
    /// replaces the scope's stored rows with nothing. After a failed schedule read the Save is refused and sends nothing, for the
    /// fleet scope and for a server alike.</summary>
    [Theory]
    [InlineData(null, false)]
    [InlineData(ServerId, true)]
    [InlineData(ServerId, false)]
    public void ASave_AfterAFailedScheduleRead_IsRefused_AndSendsNothing(int? serverId, bool usesDefault)
    {
        var plan = CollectorScheduleOverlay.BuildSavePlan(
            Grid(), Array.Empty<CollectorRunTimeRow>(), serverId, usesDefault, resetToDefaults: false, schedulesRead: false, runTimesRead: true);

        Assert.Equal(CollectorScheduleOverlay.SchedulesUnreadRefusal, plan.Refusal);
        Assert.Empty(plan.Rows);
        Assert.Empty(plan.RunTimeChanges);
        Assert.False(plan.ClearRunTimes);
    }

    /// <summary>The window records a failed read, keeps its warning in front of every later status text, and builds its Save from
    /// the plan. Its status line is written in ONE place, because any other assignment to it is what overwrote the warning.</summary>
    [Fact]
    public void TheWindow_KeepsAFailedReadsWarningVisible_AndBuildsItsSaveFromThePlan()
    {
        var window = Window();

        Assert.Single(Regex.Matches(window, @"StatusText\.Text = "));
        Assert.Contains("CollectorScheduleOverlay.BuildSavePlan(", window, StringComparison.Ordinal);
        Assert.Contains("schedulesRead: SchedulesRead", window, StringComparison.Ordinal);
        Assert.Contains("runTimesRead: RunTimesRead", window, StringComparison.Ordinal);
        Assert.Contains(".SaveCollectorScheduleAsync(_scopeServerId, plan.Rows, plan.RunTimeChanges, plan.ClearRunTimes)", window, StringComparison.Ordinal);

        /* Both reads set a warning of their own when they fail, and the same reads clear it when they work. */
        var runTimeRead = window[window.IndexOf("private async Task ReloadRunTimesAsync(", StringComparison.Ordinal)..window.IndexOf("private void ShowStatus(", StringComparison.Ordinal)];
        Assert.Contains("_runTimeReadWarning = ", runTimeRead, StringComparison.Ordinal);
        var scheduleRead = window[window.IndexOf("private async Task ReloadSchedulesAsync(", StringComparison.Ordinal)..window.IndexOf("private async Task ReloadRunTimesAsync(", StringComparison.Ordinal)];
        Assert.Contains("_scheduleReadWarning = ", scheduleRead, StringComparison.Ordinal);

        /* "Apply Default to All Servers" re-reads through the same two steps, so a re-read that fails guards the next Save too. */
        var applyAll = window[window.IndexOf("private async void ApplyDefaultToAll_Click(", StringComparison.Ordinal)..window.IndexOf("private void CancelButton_Click(", StringComparison.Ordinal)];
        Assert.Contains("await ReloadSchedulesAsync();", applyAll, StringComparison.Ordinal);
        Assert.Contains("await ReloadRunTimesAsync();", applyAll, StringComparison.Ordinal);
    }
}
