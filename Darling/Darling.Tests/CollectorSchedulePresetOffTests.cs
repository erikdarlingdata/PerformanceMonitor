/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Off preset of the Collector Schedules editor (#9569): Off clears Enabled on every row and keeps everything
/// else, and a named preset turns every row back on only when every row was off, so a collector somebody turned
/// off on purpose among running ones stays off.
/// </summary>
public sealed class CollectorSchedulePresetOffTests
{
    private static List<CollectorScheduleEditItem> MixedSchedule()
    {
        var schedule = CollectorSchedulePresets.BuildDefaultSchedule();
        for (var i = 0; i < schedule.Count; i++)
        {
            schedule[i].Enabled = i % 2 == 0;
            schedule[i].RunAtText = i % 3 == 0 ? "02:30" : schedule[i].RunAtText;
        }

        return schedule;
    }

    [Fact]
    public void Off_clears_every_row_and_keeps_frequencies_retention_and_run_times()
    {
        var schedule = MixedSchedule();
        var before = schedule.ToDictionary(s => s.Name, s => (s.FrequencyMinutes, s.RetentionDays, s.RunAtText));
        Assert.Contains(schedule, s => s.Enabled);

        CollectorSchedulePresets.ApplyPreset(schedule, CollectorSchedulePresets.Off);

        Assert.All(schedule, s => Assert.False(s.Enabled));
        foreach (var item in schedule)
        {
            Assert.Equal(before[item.Name], (item.FrequencyMinutes, item.RetentionDays, item.RunAtText));
        }

        Assert.Equal(CollectorSchedulePresets.Off, CollectorSchedulePresets.DetectPreset(schedule));
    }

    [Theory]
    [InlineData("Aggressive")]
    [InlineData("Balanced")]
    [InlineData("Low-Impact")]
    public void A_named_preset_after_Off_turns_every_row_on_and_sets_its_frequencies(string preset)
    {
        var schedule = MixedSchedule();
        var retentionBefore = schedule.ToDictionary(s => s.Name, s => s.RetentionDays);
        CollectorSchedulePresets.ApplyPreset(schedule, CollectorSchedulePresets.Off);

        CollectorSchedulePresets.ApplyPreset(schedule, preset);

        Assert.All(schedule, s => Assert.True(s.Enabled));
        foreach (var (collector, freq) in CollectorSchedulePresets.Presets[preset])
        {
            var row = schedule.FirstOrDefault(s => s.Name == collector);
            if (row is not null)
            {
                Assert.Equal(freq, row.FrequencyMinutes);
            }
        }

        Assert.All(schedule, s => Assert.Equal(retentionBefore[s.Name], s.RetentionDays));
        Assert.Equal(preset, CollectorSchedulePresets.DetectPreset(schedule));
    }

    [Fact]
    public void A_named_preset_with_one_row_off_leaves_that_row_off_and_sets_frequencies()
    {
        var schedule = CollectorSchedulePresets.BuildDefaultSchedule();
        Assert.All(schedule.Where(s => s.Name != "wait_stats"), s => s.Enabled = true);
        var waitStats = schedule.Single(s => s.Name == "wait_stats");
        waitStats.Enabled = false;
        var enabledBefore = schedule.ToDictionary(s => s.Name, s => s.Enabled);

        CollectorSchedulePresets.ApplyPreset(schedule, "Aggressive");

        Assert.False(waitStats.Enabled);
        Assert.Equal(CollectorSchedulePresets.Presets["Aggressive"]["wait_stats"], waitStats.FrequencyMinutes);
        Assert.All(schedule, s => Assert.Equal(enabledBefore[s.Name], s.Enabled));
        Assert.Equal("Aggressive", CollectorSchedulePresets.DetectPreset(schedule));
    }

    [Fact]
    public void An_empty_list_is_never_Off()
    {
        var empty = new List<CollectorScheduleEditItem>();

        Assert.NotEqual(CollectorSchedulePresets.Off, CollectorSchedulePresets.DetectPreset(empty));
        CollectorSchedulePresets.ApplyPreset(empty, "Balanced");
        Assert.Empty(empty);
    }

    [Fact]
    public void Off_is_not_a_frequency_table()
    {
        Assert.False(CollectorSchedulePresets.Presets.ContainsKey(CollectorSchedulePresets.Off));
    }
}
