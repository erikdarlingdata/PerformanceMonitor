using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Settings > Collector Schedules showed Preset "Custom" next to Status "Default" on every server. The two columns
/// answer different questions (what the schedule matches, whose schedule it is), and the "Custom" was real: the
/// saved default schedule still carried deadlocks at 1 minute, the interval before #1963 made it 5. These pin one
/// read for both columns, wording that names what each means, and a tooltip that names the intervals that differ.
/// </summary>
public sealed class ServerScheduleSummaryTests : IDisposable
{
    private const string ServerKey = "schedule-summary-server";
    private readonly string _configDir = Path.Combine(Path.GetTempPath(), "pm-schedule-summary-" + Guid.NewGuid().ToString("N"));

    public ServerScheduleSummaryTests() => Directory.CreateDirectory(_configDir);

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public void Shipped_defaults_read_as_Balanced_and_follow_the_default_schedule()
    {
        var manager = new ScheduleManager(_configDir);

        var summary = manager.GetServerScheduleSummary(ServerKey);

        Assert.Equal("Balanced", summary.Preset);
        Assert.Equal(ScheduleManager.StatusUsesDefault, summary.Status);
        Assert.Equal("Uses default", summary.Status);
        Assert.Contains("matches the Balanced preset", summary.PresetDetail);
        Assert.Equal(summary.Preset, manager.GetActivePresetForServer(ServerKey));
        Assert.Equal(summary.Preset, manager.GetActivePreset());
    }

    [Fact]
    public void An_older_interval_kept_in_the_default_schedule_reads_Custom_and_the_tooltip_names_it()
    {
        var manager = new ScheduleManager(_configDir);
        manager.UpdateSchedule("deadlocks", frequencyMinutes: 1);

        var summary = manager.GetServerScheduleSummary(ServerKey);

        Assert.Equal("Custom", summary.Preset);
        Assert.Equal("Uses default", summary.Status);
        Assert.Contains("deadlocks 1m (Balanced 5m)", summary.PresetDetail);
        Assert.Contains("Closest is Balanced", summary.PresetDetail);
    }

    [Fact]
    public void A_server_with_its_own_schedule_reads_Own_schedule_and_its_own_preset()
    {
        var manager = new ScheduleManager(_configDir);
        var own = manager.GetDefaultSchedule().Select(s => new CollectorSchedule
        {
            Name = s.Name, Enabled = s.Enabled, FrequencyMinutes = s.FrequencyMinutes, RetentionDays = s.RetentionDays,
        }).ToList();
        manager.SetScheduleForServer(ServerKey, own);
        ScheduleManager.ApplyPreset(own, "Low-Impact");
        manager.SetScheduleForServer(ServerKey, own);

        var summary = manager.GetServerScheduleSummary(ServerKey);

        Assert.Equal("Low-Impact", summary.Preset);
        Assert.Equal("Own schedule", summary.Status);
        Assert.Equal(summary.Preset, manager.GetActivePresetForServer(ServerKey));
        Assert.Equal("Balanced", manager.GetActivePreset());
    }

    [Fact]
    public void One_changed_interval_on_a_server_schedule_reads_Custom_with_Own_schedule()
    {
        var manager = new ScheduleManager(_configDir);
        var own = manager.GetDefaultSchedule().Select(s => new CollectorSchedule
        {
            Name = s.Name, Enabled = s.Enabled, FrequencyMinutes = s.FrequencyMinutes, RetentionDays = s.RetentionDays,
        }).ToList();
        own.First(s => s.Name == "wait_stats").FrequencyMinutes = 3;
        manager.SetScheduleForServer(ServerKey, own);

        var summary = manager.GetServerScheduleSummary(ServerKey);

        Assert.Equal("Custom", summary.Preset);
        Assert.Equal("Own schedule", summary.Status);
        Assert.Contains("wait_stats 3m", summary.PresetDetail);
    }

    [Fact]
    public void The_tooltip_lists_five_differences_and_counts_the_rest()
    {
        var schedules = ScheduleManager.GetDefaultSchedules();
        foreach (var name in new[] { "wait_stats", "latch_stats", "spinlock_stats", "cpu_scheduler_stats", "query_stats", "procedure_stats", "deadlocks" })
        {
            schedules.First(s => s.Name == name).FrequencyMinutes = 7;
        }

        var detail = ScheduleManager.DescribePresetMatch(schedules);

        Assert.StartsWith("Custom: no preset matches.", detail);
        Assert.Contains("and 2 more", detail);
    }

    [Fact]
    public void Settings_window_reads_both_columns_from_the_one_summary()
    {
        var code = File.ReadAllText(Path.Combine(FindRepoRoot(), "Lite", "Windows", "SettingsWindow.xaml.cs"));
        var start = code.IndexOf("private void LoadServerScheduleSummary()", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = code.Substring(start, code.IndexOf("DefaultPresetText.Text", start, StringComparison.Ordinal) - start);

        Assert.Contains("GetServerScheduleSummary(", body);
        Assert.DoesNotContain("HasServerOverride(", body);
        Assert.DoesNotContain("GetActivePresetForServer(", body);
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir != null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }
        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }
}
