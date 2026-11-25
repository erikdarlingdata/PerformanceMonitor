using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Off preset of the Collector Schedules editor (#9569): Off clears Enabled on every row and keeps everything
/// else, and a named preset returns each row to its shipped default only when every row was off. A collector
/// that ships off stays off, and one somebody turned off on purpose among running ones stays off.
/// </summary>
public sealed class CollectorSchedulePresetOffTests : IDisposable
{
    private readonly string _configDir = Path.Combine(Path.GetTempPath(), "pm-preset-off-" + Guid.NewGuid().ToString("N"));

    public CollectorSchedulePresetOffTests() => Directory.CreateDirectory(_configDir);

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    private List<CollectorSchedule> MixedSchedule()
    {
        var schedule = new ScheduleManager(_configDir).GetDefaultSchedule()
            .Select(s => new CollectorSchedule
            {
                Name = s.Name,
                Enabled = s.Enabled,
                FrequencyMinutes = s.FrequencyMinutes,
                RetentionDays = s.RetentionDays,
                RunAt = s.RunAt,
            })
            .ToList();
        for (var i = 0; i < schedule.Count; i++)
        {
            schedule[i].Enabled = i % 2 == 0;
            if (i % 3 == 0)
            {
                schedule[i].RunAt = "02:30";
            }
        }

        return schedule;
    }

    [Fact]
    public void Off_clears_every_row_and_keeps_frequencies_retention_and_run_times()
    {
        var schedule = MixedSchedule();
        var before = schedule.ToDictionary(s => s.Name, s => (s.FrequencyMinutes, s.RetentionDays, s.RunAt));
        Assert.Contains(schedule, s => s.Enabled);

        ScheduleManager.ApplyPreset(schedule, ScheduleManager.PresetOff);

        Assert.All(schedule, s => Assert.False(s.Enabled));
        foreach (var item in schedule)
        {
            Assert.Equal(before[item.Name], (item.FrequencyMinutes, item.RetentionDays, item.RunAt));
        }

        Assert.Equal("Off", ScheduleManager.DetectPreset(schedule));
    }

    [Theory]
    [InlineData("Aggressive")]
    [InlineData("Balanced")]
    [InlineData("Low-Impact")]
    public void A_named_preset_after_Off_returns_each_row_to_its_shipped_default_and_sets_its_frequencies(string preset)
    {
        var schedule = ScheduleManager.GetDefaultSchedules();
        var shipped = ScheduleManager.GetDefaultSchedules().ToDictionary(s => s.Name, s => s.Enabled);
        var retentionBefore = schedule.ToDictionary(s => s.Name, s => s.RetentionDays);
        ScheduleManager.ApplyPreset(schedule, ScheduleManager.PresetOff);

        ScheduleManager.ApplyPreset(schedule, preset);

        // A collector that ships off (long_query_completions) must not start an Extended Events session on Off then a named preset.
        Assert.Contains(schedule, s => s.Name == "long_query_completions" && !s.Enabled);
        Assert.Contains(schedule, s => s.Enabled);
        Assert.All(schedule, s => Assert.Equal(shipped[s.Name], s.Enabled));
        Assert.All(schedule, s => Assert.Equal(retentionBefore[s.Name], s.RetentionDays));
        Assert.Equal(preset, ScheduleManager.DetectPreset(schedule));
    }

    [Fact]
    public void A_named_preset_with_one_row_off_leaves_that_row_off_and_sets_frequencies()
    {
        var schedule = MixedSchedule();
        schedule.ForEach(s => s.Enabled = true);
        var waitStats = schedule.Single(s => s.Name == "wait_stats");
        waitStats.Enabled = false;
        waitStats.FrequencyMinutes = 77;

        ScheduleManager.ApplyPreset(schedule, "Aggressive");

        Assert.False(waitStats.Enabled);
        Assert.Equal(1, waitStats.FrequencyMinutes);
        Assert.Equal(schedule.Count - 1, schedule.Count(s => s.Enabled));
        Assert.Equal("Aggressive", ScheduleManager.DetectPreset(schedule));
    }

    [Fact]
    public void An_empty_list_is_never_Off()
    {
        var empty = new List<CollectorSchedule>();

        Assert.NotEqual("Off", ScheduleManager.DetectPreset(empty));
    }

    [Fact]
    public void An_all_off_list_is_described_as_every_collector_off_not_as_matching_an_Off_preset()
    {
        var schedule = MixedSchedule();
        ScheduleManager.ApplyPreset(schedule, ScheduleManager.PresetOff);

        var detail = ScheduleManager.DescribePresetMatch(schedule);

        Assert.Equal("Every collector is off.", detail);
        Assert.DoesNotContain("matches the Off preset", detail);
    }

    [Fact]
    public void The_server_summary_of_an_all_off_default_schedule_reads_Off()
    {
        var manager = new ScheduleManager(_configDir);
        foreach (var s in manager.GetDefaultSchedule())
        {
            manager.UpdateSchedule(s.Name, enabled: false);
        }

        var summary = manager.GetServerScheduleSummary("preset-off-server");

        Assert.Equal("Off", summary.Preset);
        Assert.Equal("Every collector is off.", summary.PresetDetail);
    }
}
