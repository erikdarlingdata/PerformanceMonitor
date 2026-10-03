using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4938: a daily collector can carry a run time, a "run_at" of HH:MM on the monitored server's clock in
/// collection_schedule.json. These pin Lite's half: the file field, the due rule in
/// <see cref="ScheduleManager.GetDueCollectorsForServer(string, DateTime)"/>, and the tab-open selection. Every
/// instant is derived from <see cref="CollectorRunTime.SlotUtc"/>, because a day's slot is the run time plus the
/// server's fixed spread of up to an hour, never a hand-written clock time.
/// </summary>
public sealed class CollectorRunTimeScheduleTests : IDisposable
{
    private const string ServerKey = "run-time-server";
    private const string ServerName = "run-time-server-name";
    private const string Daily = "daily_job";
    private const int StorageId = 1_234_567_891;

    private static readonly ServerClock Eastern = ServerClock.Resolve("Eastern Standard Time", -300);

    private readonly string _configDir = Directory.CreateTempSubdirectory("pm-lite-run-time-tests-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    private string SchedulePath => Path.Combine(_configDir, "collection_schedule.json");

    private static DateTime Slot(DateOnly date, int runAtMinute, ServerClock? clock = null) =>
        CollectorRunTime.SlotUtc(date, runAtMinute, StorageId, clock is null ? CollectorRunTime.LocalIsUtc : clock.ToUtc);

    private ScheduleManager Manager(string? runAt = "02:00", int frequency = 1440, ServerClock? clock = null,
        ILogger<ScheduleManager>? log = null, string collector = Daily)
    {
        var manager = new ScheduleManager(_configDir, log);
        manager.SetScheduleForServer(ServerKey, new List<CollectorSchedule>
        {
            new() { Name = collector, Enabled = true, FrequencyMinutes = frequency, RunAt = runAt },
        });
        manager.SetServerRunContext(ServerKey, StorageId, ServerName, clock);
        return manager;
    }

    /* A hand-edited collection_schedule.json can carry a run time the editor and the write APIs would have refused. The
       load keeps only collectors the dispatch knows, so the name is a real one; the load also adds the rest of the defaults. */
    private ScheduleManager ManagerFromFile(string collector, string runAt, int frequency, ILogger<ScheduleManager>? log = null)
    {
        File.WriteAllText(SchedulePath, $$"""
            {
              "version": 2,
              "default_schedule": [],
              "server_overrides": {
                "{{ServerKey}}": { "collectors": [ { "name": "{{collector}}", "enabled": true, "frequency_minutes": {{frequency}}, "retention_days": 30, "run_at": "{{runAt}}" } ] }
              }
            }
            """);
        var manager = new ScheduleManager(_configDir, log);
        manager.SetServerRunContext(ServerKey, StorageId, ServerName, clock: null);
        return manager;
    }

    private static bool IsDue(ScheduleManager manager, string collector, DateTime atUtc) =>
        manager.GetDueCollectorsForServer(ServerKey, atUtc).Any(s => s.Name == collector);

    private static List<string> Due(ScheduleManager manager, DateTime atUtc) =>
        manager.GetDueCollectorsForServer(ServerKey, atUtc).Select(s => s.Name).ToList();

    private static readonly DateOnly Day = new(2026, 7, 15);

    // ── the due rule ─────────────────────────────────────────────────────────────

    [Fact]
    public void ACollectorWithARunTime_IsNotDueBeforeTheDaysTime()
    {
        var manager = Manager(clock: Eastern);
        var slot = Slot(Day, 120, Eastern);

        manager.MarkCollectorRunForServer(ServerKey, Daily, slot - TimeSpan.FromDays(1) + TimeSpan.FromMinutes(5));

        Assert.Empty(Due(manager, slot - TimeSpan.FromMinutes(1)));
        Assert.Empty(Due(manager, slot - TimeSpan.FromHours(6)));
    }

    [Fact]
    public void ACollectorWithARunTime_IsDueInsideTheGrace_FromTheSlotToTheEndOfTheGrace()
    {
        var manager = Manager(clock: Eastern);
        var slot = Slot(Day, 120, Eastern);

        manager.MarkCollectorRunForServer(ServerKey, Daily, slot - TimeSpan.FromDays(1) + TimeSpan.FromMinutes(5));

        Assert.Equal(new[] { Daily }, Due(manager, slot));
        Assert.Equal(new[] { Daily }, Due(manager, slot + TimeSpan.FromMinutes(30)));
        Assert.Equal(new[] { Daily }, Due(manager, slot + CollectorRunTime.Grace));
    }

    [Fact]
    public void ACollectorWithARunTime_AfterTheGrace_IsNotDue_AndTheDayIsSkipped()
    {
        var manager = Manager(clock: Eastern);
        var slot = Slot(Day, 120, Eastern);

        /* Lite was closed through the whole grace: the day is skipped, and the collector is not replayed later in the
           day. It is due again at the next day's time. */
        manager.MarkCollectorRunForServer(ServerKey, Daily, slot - TimeSpan.FromDays(1) + TimeSpan.FromMinutes(5));

        Assert.Empty(Due(manager, slot + CollectorRunTime.Grace + TimeSpan.FromMinutes(1)));
        Assert.Empty(Due(manager, slot + TimeSpan.FromHours(10)));
        Assert.Equal(new[] { Daily }, Due(manager, Slot(Day.AddDays(1), 120, Eastern) + TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void ACollectorWithARunTime_AfterARunInsideTheGrace_IsNotDueAgainTheSameDay()
    {
        var manager = Manager(clock: Eastern);
        var slot = Slot(Day, 120, Eastern);
        var ranAt = slot + TimeSpan.FromMinutes(2);

        Assert.Equal(new[] { Daily }, Due(manager, ranAt));
        manager.MarkCollectorRunForServer(ServerKey, Daily, ranAt);

        Assert.Empty(Due(manager, ranAt + TimeSpan.FromMinutes(1)));
        Assert.Empty(Due(manager, slot + CollectorRunTime.Grace));
        Assert.Empty(Due(manager, slot + TimeSpan.FromHours(12)));
        Assert.Equal(new[] { Daily }, Due(manager, Slot(Day.AddDays(1), 120, Eastern) + TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void AnAttemptThatDidNotSucceed_EndsTheDay_OfACollectorWithARunTime_AndTheNextDaysSlotIsDue()
    {
        var manager = Manager(clock: Eastern);
        var slot = Slot(Day, 120, Eastern);
        var attemptAt = slot + TimeSpan.FromMinutes(2);

        /* A failed run writes its collection_log row, and a restart reads that row as the day's run. The session
           counts the attempt the same way, so the failing collector is not run again on every sweep of the hour. */
        Assert.Equal(new[] { Daily }, Due(manager, attemptAt));
        manager.MarkCollectorAttemptForServer(ServerKey, Daily, attemptAt);

        Assert.Empty(Due(manager, attemptAt + TimeSpan.FromMinutes(1)));
        Assert.Empty(Due(manager, slot + CollectorRunTime.Grace));
        Assert.Empty(Due(manager, slot + TimeSpan.FromHours(12)));
        Assert.Empty(Due(manager, Slot(Day.AddDays(1), 120, Eastern) - TimeSpan.FromMinutes(1)));
        Assert.Equal(new[] { Daily }, Due(manager, Slot(Day.AddDays(1), 120, Eastern) + TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void AnAttempt_OfACollectorWithNoRunTime_OrOneWhoseRunTimeDoesNotApply_ChangesNothing()
    {
        var plainDaily = Manager(runAt: null, clock: Eastern);
        var now = Slot(Day, 120, Eastern) + TimeSpan.FromMinutes(2);

        /* No run time: the interval rule stands, and a collector that has never run is due on every sweep until one
           succeeds. */
        plainDaily.MarkCollectorAttemptForServer(ServerKey, Daily, now);
        Assert.Equal(new[] { Daily }, Due(plainDaily, now + TimeSpan.FromMinutes(1)));

        /* A run time on an hourly interval is refused and ignored with a warning, so the interval rule stands there too. */
        var hourly = ManagerFromFile("wait_stats", "02:00", frequency: 60);
        hourly.MarkCollectorAttemptForServer(ServerKey, "wait_stats", now);
        Assert.True(IsDue(hourly, "wait_stats", now + TimeSpan.FromMinutes(1)));

        /* A collector the schedule does not list has nothing to record. */
        plainDaily.MarkCollectorAttemptForServer(ServerKey, "not_in_the_schedule", now);
        Assert.Equal(new[] { Daily }, Due(plainDaily, now + TimeSpan.FromMinutes(2)));
    }

    [Fact]
    public void ANeverRunCollectorWithARunTime_WaitsForTheNextDaysTime_ThenRunsInsideItsGrace()
    {
        var manager = Manager(clock: Eastern);
        var slot = Slot(Day, 120, Eastern);

        /* Past today's grace with no run on record: it does not run at start-up, it waits for tomorrow's time. */
        Assert.Empty(Due(manager, slot + CollectorRunTime.Grace + TimeSpan.FromMinutes(1)));
        Assert.Empty(Due(manager, slot + TimeSpan.FromHours(5)));
        Assert.Equal(new[] { Daily }, Due(manager, Slot(Day.AddDays(1), 120, Eastern) + TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void ASpringForwardDay_KeepsTheRunTimeOnTheServersLocalClock()
    {
        var manager = Manager(runAt: "04:00", clock: Eastern);
        var before = Slot(new DateOnly(2026, 3, 7), 240, Eastern);
        var change = Slot(new DateOnly(2026, 3, 8), 240, Eastern);

        Assert.Equal(TimeSpan.FromHours(23), change - before);

        manager.MarkCollectorRunForServer(ServerKey, Daily, before + TimeSpan.FromMinutes(2));

        /* A slot of last run + 1440 minutes would land an hour later, at 05:00 local. */
        Assert.Empty(Due(manager, change - TimeSpan.FromMinutes(1)));
        Assert.Equal(new[] { Daily }, Due(manager, change + TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void AFallBackDay_KeepsTheRunTimeOnTheServersLocalClock()
    {
        var manager = Manager(runAt: "04:00", clock: Eastern);
        var before = Slot(new DateOnly(2026, 10, 31), 240, Eastern);
        var change = Slot(new DateOnly(2026, 11, 1), 240, Eastern);

        Assert.Equal(TimeSpan.FromHours(25), change - before);

        manager.MarkCollectorRunForServer(ServerKey, Daily, before + TimeSpan.FromMinutes(2));

        /* A slot of last run + 1440 minutes would land an hour early, at 03:00 local. */
        Assert.Empty(Due(manager, change - TimeSpan.FromMinutes(30)));
        Assert.Equal(new[] { Daily }, Due(manager, change + TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void WithNoClockYet_TheRunTimeIsReadAsUtc_AndTheClockMovesItWhenItArrives()
    {
        var manager = Manager(clock: null);
        var utcSlot = Slot(Day, 120);
        var easternSlot = Slot(Day, 120, Eastern);

        manager.MarkCollectorRunForServer(ServerKey, Daily, utcSlot - TimeSpan.FromDays(1) + TimeSpan.FromMinutes(5));
        Assert.Empty(Due(manager, utcSlot - TimeSpan.FromMinutes(1)));
        Assert.Equal(new[] { Daily }, Due(manager, utcSlot + TimeSpan.FromMinutes(1)));

        /* The server's clock arrives: the same instant is no longer inside the grace, and the new slot is. */
        manager.SetServerRunContext(ServerKey, StorageId, ServerName, Eastern);
        Assert.Empty(Due(manager, utcSlot + TimeSpan.FromMinutes(1)));
        Assert.Equal(new[] { Daily }, Due(manager, easternSlot + TimeSpan.FromMinutes(1)));
    }

    [Fact]
    public void ACollectorWithoutARunTime_KeepsTodaysRule()
    {
        var manager = Manager(runAt: null);
        var now = new DateTime(2026, 7, 15, 9, 0, 0, DateTimeKind.Utc);

        Assert.Equal(new[] { Daily }, Due(manager, now));

        manager.MarkCollectorRunForServer(ServerKey, Daily, now - TimeSpan.FromHours(23));
        Assert.Empty(Due(manager, now));

        manager.MarkCollectorRunForServer(ServerKey, Daily, now - TimeSpan.FromHours(24));
        Assert.Equal(new[] { Daily }, Due(manager, now));
    }

    [Fact]
    public void ARunTimeOnAnHourlyCollector_IsIgnored_WithOneWarningThatNamesTheCollectorAndTheServer()
    {
        var log = new CapturingLog();
        var manager = ManagerFromFile("server_config", runAt: "02:00", frequency: 60, log);
        var now = new DateTime(2026, 7, 15, 9, 0, 0, DateTimeKind.Utc);

        /* The plain interval rule, as if no run time were set. */
        Assert.True(IsDue(manager, "server_config", now));
        manager.MarkCollectorRunForServer(ServerKey, "server_config", now - TimeSpan.FromMinutes(30));
        Assert.False(IsDue(manager, "server_config", now));
        manager.MarkCollectorRunForServer(ServerKey, "server_config", now - TimeSpan.FromMinutes(61));
        Assert.True(IsDue(manager, "server_config", now));

        var warning = Assert.Single(log.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("server_config", StringComparison.Ordinal));
        Assert.Contains(ServerName, warning.Message, StringComparison.Ordinal);
        Assert.Contains("60 minutes", warning.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ARunTimeThatIsNotAnHHMMTime_IsIgnored_WithOneWarning()
    {
        var log = new CapturingLog();
        var manager = ManagerFromFile("index_object_stats", runAt: "25:00", frequency: 1440, log);
        var now = new DateTime(2026, 7, 15, 9, 0, 0, DateTimeKind.Utc);

        Assert.True(IsDue(manager, "index_object_stats", now));
        Assert.True(IsDue(manager, "index_object_stats", now.AddMinutes(1)));

        var warning = Assert.Single(log.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("index_object_stats", StringComparison.Ordinal));
        Assert.Contains(ServerName, warning.Message, StringComparison.Ordinal);
    }

    // ── the editor's checks and its next-run text ───────────────────────────────

    private const string InvalidRunAtText = "Run at must be a 24-hour time from 00:00 to 23:59, such as 02:00.";

    [Theory]
    [InlineData("24:00")]
    [InlineData("25:00")]
    [InlineData("02:60")]
    [InlineData("2:00")]
    [InlineData("0200")]
    [InlineData("abc")]
    [InlineData("-1:00")]
    public void TheEditorCheck_RefusesATimeThatIsNot24HourHHMM_WithTheShippedText(string runAt)
    {
        Assert.Equal(InvalidRunAtText, ScheduleManager.RunAtError(Daily, 1440, runAt));
    }

    [Theory]
    [InlineData("00:00")]
    [InlineData("02:00")]
    [InlineData("23:59")]
    [InlineData(" 04:30 ")]
    public void TheEditorCheck_AcceptsA24HourTime_OnAWholeDayInterval(string runAt)
    {
        Assert.Null(ScheduleManager.RunAtError(Daily, 1440, runAt));
        Assert.Null(ScheduleManager.RunAtError(Daily, 2880, runAt));
    }

    [Fact]
    public void TheEditorCheck_RefusesARunTimeOnAnIntervalThatIsNotWholeDays_WithTheShippedText()
    {
        Assert.Equal(
            "'wait_stats' runs every 60 minutes. A run time works only for a collector that runs once a day or less often (1440 minutes, or a multiple of 1440). Change the frequency or clear the run time.",
            ScheduleManager.RunAtError("wait_stats", 60, "02:00"));
        Assert.Contains("every 720 minutes", ScheduleManager.RunAtError(Daily, 720, "02:00"), StringComparison.Ordinal);
        Assert.Contains("every 1500 minutes", ScheduleManager.RunAtError(Daily, 1500, "02:00"), StringComparison.Ordinal);
        Assert.Contains("every 1 minutes", ScheduleManager.RunAtError(Daily, 1, "02:00"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheEditorCheck_JudgesAnOnLoadCollectorOnItsDailyRecapture_AndABlankTimeIsNone()
    {
        /* The 5 on-load collectors (frequency 0) re-capture daily, so they may have a run time. */
        Assert.Null(ScheduleManager.RunAtError("trace_flags", 0, "02:00"));

        Assert.Null(ScheduleManager.RunAtError("wait_stats", 1, null));
        Assert.Null(ScheduleManager.RunAtError("wait_stats", 1, ""));
        Assert.Null(ScheduleManager.RunAtError("wait_stats", 1, "   "));
        Assert.Null(ScheduleManager.NormalizeRunAt("  "));
        Assert.Equal("02:00", ScheduleManager.NormalizeRunAt(" 02:00 "));
    }

    [Fact]
    public void TheEditorNote_SaysLiteRunsTheCollectorOnlyWhileItIsOpen()
    {
        Assert.Equal(
            "Lite collects only while it is open. If Lite is closed at this time, that day's run is skipped.",
            ScheduleManager.RunAtLiteClosedNote);
    }

    [Fact]
    public void TheEditorWindow_UsesTheChecksTheNoteAndTheDefaultPathsRunTime()
    {
        /* The window needs a desktop to open, so its wiring is read from the source: the check, the note, and the
           default-schedule save passing the run time (UpdateSchedule keeps the one it has unless told to change it). */
        var editor = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Windows", "CollectorScheduleEditorWindow.xaml.cs"))
            .Replace("\r\n", "\n");
        Assert.Contains("ScheduleManager.RunAtError(item.Name, item.FrequencyMinutes, item.RunAt)", editor, StringComparison.Ordinal);
        Assert.Contains("ScheduleManager.RunAtLiteClosedNote", editor, StringComparison.Ordinal);
        Assert.Contains("changeRunAt: true", editor, StringComparison.Ordinal);

        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Windows", "CollectorScheduleEditorWindow.xaml"));
        Assert.Contains("Binding=\"{Binding RunAt, UpdateSourceTrigger=LostFocus}\"", xaml, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultScheduleUpdate_SetsAndClearsARunTime_AndKeepsItWhenOnlyOtherFieldsChange()
    {
        var manager = new ScheduleManager(_configDir);
        string? RunAt() => manager.GetDefaultSchedule().First(s => s.Name == "index_object_stats").RunAt;

        manager.UpdateSchedule("index_object_stats", runAt: " 02:00 ", changeRunAt: true);
        Assert.Equal("02:00", RunAt());
        Assert.Contains("\"run_at\": \"02:00\"", File.ReadAllText(SchedulePath), StringComparison.Ordinal);

        /* Other fields change and the run time stays. */
        manager.UpdateSchedule("index_object_stats", retentionDays: 45);
        Assert.Equal("02:00", RunAt());

        manager.UpdateSchedule("index_object_stats", runAt: "", changeRunAt: true);
        Assert.Null(RunAt());
        Assert.DoesNotContain("run_at", File.ReadAllText(SchedulePath), StringComparison.Ordinal);
    }

    [Fact]
    public void TheDefaultScheduleUpdate_RefusesABadRunTime_AndAFrequencyThatLeavesARunTimeOnAnHourlyCollector_ChangingNothing()
    {
        var manager = new ScheduleManager(_configDir);
        string? RunAt() => manager.GetDefaultSchedule().First(s => s.Name == "index_object_stats").RunAt;
        int Frequency() => manager.GetDefaultSchedule().First(s => s.Name == "index_object_stats").FrequencyMinutes;
        manager.UpdateSchedule("index_object_stats", runAt: "02:00", changeRunAt: true);
        var frequency = Frequency();

        var bad = Assert.Throws<InvalidOperationException>(() =>
            manager.UpdateSchedule("index_object_stats", retentionDays: 99, runAt: "25:00", changeRunAt: true));
        Assert.Equal(InvalidRunAtText, bad.Message);

        var hourly = Assert.Throws<InvalidOperationException>(() =>
            manager.UpdateSchedule("index_object_stats", frequencyMinutes: 60));
        Assert.StartsWith("'index_object_stats' runs every 60 minutes. A run time works only", hourly.Message, StringComparison.Ordinal);

        Assert.Equal("02:00", RunAt());
        Assert.Equal(frequency, Frequency());
        Assert.NotEqual(99, manager.GetDefaultSchedule().First(s => s.Name == "index_object_stats").RetentionDays);

        /* Clearing the run time and moving to hourly in one update is fine. */
        manager.UpdateSchedule("index_object_stats", frequencyMinutes: 60, runAt: null, changeRunAt: true);
        Assert.Null(RunAt());
        Assert.Equal(60, Frequency());
    }

    [Fact]
    public void AServerScheduleSave_RefusesABadRunTime_AndStoresABlankOneAsNone()
    {
        var manager = new ScheduleManager(_configDir);

        var bad = Assert.Throws<InvalidOperationException>(() => manager.SetScheduleForServer(ServerKey, new List<CollectorSchedule>
        {
            new() { Name = "wait_stats", Enabled = true, FrequencyMinutes = 1, RunAt = "02:00" },
        }));
        Assert.StartsWith("'wait_stats' runs every 1 minutes.", bad.Message, StringComparison.Ordinal);
        Assert.False(manager.HasServerOverride(ServerKey));

        manager.SetScheduleForServer(ServerKey, new List<CollectorSchedule>
        {
            new() { Name = Daily, Enabled = true, FrequencyMinutes = 1440, RunAt = "  " },
        });
        Assert.Null(manager.GetScheduleForServer(ServerKey, Daily)!.RunAt);
        Assert.DoesNotContain("run_at", File.ReadAllText(SchedulePath), StringComparison.Ordinal);
    }

    [Fact]
    public void TheNextRunText_GivesTheExactMinuteOnAServer_InTheServersClockAndUtc()
    {
        var manager = Manager(clock: Eastern);
        var slot = Slot(Day.AddDays(1), 120, Eastern);
        manager.MarkCollectorRunForServer(ServerKey, Daily, Slot(Day, 120, Eastern) + TimeSpan.FromMinutes(3));
        var now = Slot(Day, 120, Eastern) + TimeSpan.FromHours(5);

        var line = Assert.Single(manager.DescribeRunTimes(ServerKey, manager.GetSchedulesForServer(ServerKey), now));

        Assert.Equal(
            $"{Daily}: next run {Eastern.ToServerLocal(slot):yyyy-MM-dd HH:mm} server time ({slot:yyyy-MM-dd HH:mm} UTC).",
            line);
        Assert.Contains("02:", line, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNextRunText_SaysDueNowInsideTheHour_AndIsEmptyWhereNoRunTimeApplies()
    {
        var manager = Manager(clock: Eastern);
        var slot = Slot(Day, 120, Eastern);

        Assert.Equal($"{Daily}: due now, inside today's hour.",
            Assert.Single(manager.DescribeRunTimes(ServerKey, manager.GetSchedulesForServer(ServerKey), slot + TimeSpan.FromMinutes(5))));

        var schedules = new List<CollectorSchedule>
        {
            new() { Name = "no_run_time", Enabled = true, FrequencyMinutes = 1440 },
            new() { Name = "hourly", Enabled = true, FrequencyMinutes = 60, RunAt = "02:00" },
            new() { Name = "bad_time", Enabled = true, FrequencyMinutes = 1440, RunAt = "99:99" },
            new() { Name = "disabled", Enabled = false, FrequencyMinutes = 1440, RunAt = "02:00" },
        };
        Assert.Empty(manager.DescribeRunTimes(ServerKey, schedules, slot));
    }

    [Fact]
    public void TheNextRunText_ForTheDefaultSchedule_GivesTheHourTheServersSpreadAcross_AndWrapsPastMidnight()
    {
        var manager = new ScheduleManager(_configDir);
        var schedules = new List<CollectorSchedule>
        {
            new() { Name = "early", Enabled = true, FrequencyMinutes = 1440, RunAt = "02:00" },
            new() { Name = "late", Enabled = true, FrequencyMinutes = 1440, RunAt = "23:30" },
        };

        Assert.Equal(
            new[]
            {
                "early: runs between 02:00 and 03:00 on each server's clock, each server at its own minute.",
                "late: runs between 23:30 and 00:30 on each server's clock, each server at its own minute.",
            },
            manager.DescribeRunTimes(null, schedules, Slot(Day, 120)));
    }

    [Fact]
    public void TheNextRunText_WaitsForTheFirstCollection_AndSaysWhenTheClockIsNotKnown()
    {
        var manager = new ScheduleManager(_configDir);
        manager.SetScheduleForServer(ServerKey, new List<CollectorSchedule>
        {
            new() { Name = Daily, Enabled = true, FrequencyMinutes = 1440, RunAt = "02:00" },
        });

        /* Nothing has been read for this server yet: no clock, no run history. */
        Assert.Equal($"{Daily}: the next run shows once Lite has collected from this server.",
            Assert.Single(manager.DescribeRunTimes(ServerKey, manager.GetSchedulesForServer(ServerKey), Slot(Day, 120))));

        manager.SetServerRunContext(ServerKey, StorageId, ServerName, clock: null);
        manager.MarkCollectorRunForServer(ServerKey, Daily, Slot(Day, 120) + TimeSpan.FromMinutes(2));
        var tomorrow = Slot(Day.AddDays(1), 120);

        Assert.Equal(
            $"{Daily}: next run {tomorrow:yyyy-MM-dd HH:mm} UTC (the server's clock is not known yet, so the run time is read as UTC).",
            Assert.Single(manager.DescribeRunTimes(ServerKey, manager.GetSchedulesForServer(ServerKey), Slot(Day, 120) + TimeSpan.FromHours(3))));
    }

    // ── the tab-open run ─────────────────────────────────────────────────────────

    [Fact]
    public void TheTabOpenRun_SkipsARunTimeCollectorAndADailyOneThatIsNotDue_AndKeepsTheOnLoadCollectorsAndTheRest()
    {
        var now = new DateTime(2026, 7, 15, 9, 0, 0, DateTimeKind.Utc);
        var manager = new ScheduleManager(_configDir);
        manager.SetScheduleForServer(ServerKey, new List<CollectorSchedule>
        {
            new() { Name = "on_load", Enabled = true, FrequencyMinutes = 0 },
            new() { Name = "on_load_with_run_time", Enabled = true, FrequencyMinutes = 0, RunAt = "03:00" },
            new() { Name = "daily_with_run_time", Enabled = true, FrequencyMinutes = 1440, RunAt = "03:00" },
            new() { Name = "daily_not_due", Enabled = true, FrequencyMinutes = 1440 },
            new() { Name = "daily_due", Enabled = true, FrequencyMinutes = 1440 },
            new() { Name = "daily_never_run", Enabled = true, FrequencyMinutes = 1440 },
            new() { Name = "hourly", Enabled = true, FrequencyMinutes = 60 },
            new() { Name = "daily_disabled", Enabled = false, FrequencyMinutes = 1440 },
        });
        manager.SetServerRunContext(ServerKey, StorageId, ServerName, clock: null);
        manager.MarkCollectorRunForServer(ServerKey, "daily_not_due", now - TimeSpan.FromHours(1));
        manager.MarkCollectorRunForServer(ServerKey, "daily_due", now - TimeSpan.FromHours(25));
        manager.MarkCollectorRunForServer(ServerKey, "hourly", now - TimeSpan.FromMinutes(1));

        var names = manager.GetCollectorsForTabOpen(ServerKey, now).Select(s => s.Name).ToList();

        Assert.Equal(
            new[] { "on_load", "on_load_with_run_time", "daily_due", "daily_never_run", "hourly" },
            names);
    }

    [Fact]
    public void ARunOfServerProperties_RefreshesTheClockTheRunTimeReads()
    {
        /* The new server_properties row is the server's clock arriving or changing. A run needs a live server, so the
           wiring is read from the source; the refresh itself is pinned by TheClock_IsTheNewestServerPropertiesRow below. */
        var source = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Services", "RemoteCollectorService.cs"));
        var run = MethodBody(source, "public async Task RunCollectorAsync(ServerConnection server, string collectorName, DateTime? scheduledAtUtc");

        Assert.Contains("\"server_properties\"", run, StringComparison.Ordinal);
        Assert.Contains("RefreshRunTimeClockAsync(", run, StringComparison.Ordinal);
    }

    // ── the file ─────────────────────────────────────────────────────────────────

    [Fact]
    public void TheFile_WritesRunAtOnlyWhereOneIsSet_AndReadsItBack()
    {
        var first = new ScheduleManager(_configDir);
        Assert.DoesNotContain("run_at", File.ReadAllText(SchedulePath), StringComparison.Ordinal);

        first.GetDefaultSchedule().First(s => s.Name == "index_object_stats").RunAt = "02:00";
        first.SaveSchedules();
        first.SetScheduleForServer(ServerKey, new List<CollectorSchedule>
        {
            new() { Name = "index_object_stats", Enabled = true, FrequencyMinutes = 1440, RunAt = "04:30" },
            new() { Name = "wait_stats", Enabled = true, FrequencyMinutes = 1 },
        });

        var json = File.ReadAllText(SchedulePath);
        Assert.Equal(2, CountOf(json, "\"run_at\""));
        Assert.Contains("\"run_at\": \"02:00\"", json, StringComparison.Ordinal);
        Assert.Contains("\"run_at\": \"04:30\"", json, StringComparison.Ordinal);

        var second = new ScheduleManager(_configDir);
        Assert.Equal("02:00", second.GetDefaultSchedule().First(s => s.Name == "index_object_stats").RunAt);
        Assert.Null(second.GetDefaultSchedule().First(s => s.Name == "wait_stats").RunAt);
        Assert.Equal("04:30", second.GetScheduleForServer(ServerKey, "index_object_stats")!.RunAt);
        Assert.Null(second.GetScheduleForServer(ServerKey, "wait_stats")!.RunAt);
    }

    [Fact]
    public void TheFile_WithAFieldTheTypeDoesNotHave_Loads_SoAnOlderLiteReadsTheNewFileAndIgnoresRunAt()
    {
        /* An older build has no run_at on its type: for it, run_at is exactly this, a field it does not know. The
           reader sets no unmapped-member rule, so System.Text.Json's default skips it. A load that threw would drop
           every schedule back to the defaults, which a frequency of 7 minutes on wait_stats shows. */
        var json = """
            {
              "version": 2,
              "added_at_top_level": { "nested": [1, 2, 3] },
              "default_schedule": [
                { "name": "wait_stats", "enabled": true, "frequency_minutes": 7, "retention_days": 30, "added_by_a_newer_version": "02:00" }
              ],
              "server_overrides": {
                "srv": { "collectors": [ { "name": "wait_stats", "enabled": true, "frequency_minutes": 9, "retention_days": 30, "added_by_a_newer_version": 5 } ] }
              }
            }
            """;
        File.WriteAllText(SchedulePath, json);

        var manager = new ScheduleManager(_configDir);

        Assert.Equal(7, manager.GetDefaultSchedule().First(s => s.Name == "wait_stats").FrequencyMinutes);
        Assert.Equal(9, manager.GetScheduleForServer("srv", "wait_stats")!.FrequencyMinutes);
    }

    [Fact]
    public void AServerWithoutAnOverride_SeesTheDefaultsRunAt_AndTheEditorCopiesItToo()
    {
        var manager = new ScheduleManager(_configDir);
        manager.GetDefaultSchedule().First(s => s.Name == "index_object_stats").RunAt = "02:00";

        Assert.Equal("02:00", manager.GetSchedulesForServer("no-override").First(s => s.Name == "index_object_stats").RunAt);

        /* The editor saves a server's override from its own copy of the list; a copy that dropped RunAt would erase a
           hand-set run time on the first save. */
        var editor = File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "Windows", "CollectorScheduleEditorWindow.xaml.cs"));
        Assert.Contains("RunAt = s.RunAt", editor, StringComparison.Ordinal);
    }

    // ── the startup seed of the scheduler's state ───────────────────────────────

    [Fact]
    public void SeedingTheLastRuns_KeepsTheLaterOfTheSeedAndARunMadeSince()
    {
        var now = new DateTime(2026, 7, 15, 9, 0, 0, DateTimeKind.Utc);
        var manager = new ScheduleManager(_configDir);
        manager.SetScheduleForServer(ServerKey, new List<CollectorSchedule>
        {
            new() { Name = "seeded_daily", Enabled = true, FrequencyMinutes = 1440 },
            new() { Name = "ran_since", Enabled = true, FrequencyMinutes = 1440 },
            new() { Name = "ran_before", Enabled = true, FrequencyMinutes = 1440 },
        });
        manager.MarkCollectorRunForServer(ServerKey, "ran_since", now - TimeSpan.FromMinutes(5));
        manager.MarkCollectorRunForServer(ServerKey, "ran_before", now - TimeSpan.FromHours(30));

        manager.SeedLastRunsForServer(ServerKey, new Dictionary<string, DateTime>
        {
            ["seeded_daily"] = now - TimeSpan.FromHours(2),
            ["ran_since"] = now - TimeSpan.FromHours(20),
            ["ran_before"] = now - TimeSpan.FromHours(3),
        });

        Assert.Empty(Due(manager, now));
    }

    [Fact]
    public void TheDailyCollectors_AreThoseWhoseEffectiveIntervalIsADayOrMore_OnLoadOnesIncluded()
    {
        var manager = new ScheduleManager(_configDir);
        manager.SetScheduleForServer(ServerKey, new List<CollectorSchedule>
        {
            new() { Name = "on_load", Enabled = true, FrequencyMinutes = 0 },
            new() { Name = "daily", Enabled = true, FrequencyMinutes = 1440 },
            new() { Name = "weekly", Enabled = true, FrequencyMinutes = 10080 },
            new() { Name = "hourly", Enabled = true, FrequencyMinutes = 60 },
            new() { Name = "daily_disabled", Enabled = false, FrequencyMinutes = 1440 },
        });

        var daily = manager.GetDailyCollectorIntervalsForServer(ServerKey);

        Assert.Equal(
            new Dictionary<string, int> { ["on_load"] = 1440, ["daily"] = 1440, ["weekly"] = 10080 }.OrderBy(p => p.Key),
            daily.OrderBy(p => p.Key));
    }

    // ── helpers ──────────────────────────────────────────────────────────────────

    private static int CountOf(string text, string needle)
    {
        var count = 0;
        for (var at = text.IndexOf(needle, StringComparison.Ordinal); at >= 0; at = text.IndexOf(needle, at + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));

    /* The text from the method's signature to the closing brace at four spaces of indent. */
    private static string MethodBody(string source, string signature)
    {
        source = source.Replace("\r\n", "\n");
        var start = source.IndexOf(signature, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{signature} is in the source");
        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, "the method ends");
        return source[start..end];
    }

    private sealed class CapturingLog : ILogger<ScheduleManager>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = new();

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => Entries.Add((logLevel, formatter(state, exception)));
    }
}

/// <summary>
/// #4938: at start-up Lite reads each server's last run of every daily collector from its own collection_log, with
/// or without a run time, so a restart or a tab open no longer re-runs one; and it reads the server's clock from the
/// newest server_properties row.
/// </summary>
public sealed class CollectorRunTimeStartupSeedTests : IDisposable
{
    private static readonly DateTime Now = DateTime.UtcNow;
    private static long s_nextLogId = -1;

    private readonly string _dir = Directory.CreateTempSubdirectory("pm-lite-run-time-seed-tests-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private async Task<(DuckDbInitializer DuckDb, ServerManager Servers, ServerConnection Server)> OpenAsync()
    {
        var duckDb = new DuckDbInitializer(Path.Combine(_dir, "pm.duckdb"));
        await duckDb.InitializeAsync();
        var servers = new ServerManager(_dir);
        var server = new ServerConnection { ServerName = "seed-test", DisplayName = "seed-test" };
        servers.AddServer(server);
        return (duckDb, servers, server);
    }

    private static async Task LogAsync(DuckDbInitializer duckDb, ServerConnection server, string collector, DateTime utc, string status)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status)
            VALUES ($1, $2, 'seed-test', $3, $4, 10, $5)";
        command.Parameters.Add(new DuckDBParameter { Value = Interlocked.Decrement(ref s_nextLogId) });
        command.Parameters.Add(new DuckDBParameter { Value = RemoteCollectorService.GetServerId(server) });
        command.Parameters.Add(new DuckDBParameter { Value = collector });
        command.Parameters.Add(new DuckDBParameter { Value = utc });
        command.Parameters.Add(new DuckDBParameter { Value = status });
        await command.ExecuteNonQueryAsync();
    }

    private static async Task ServerPropertiesAsync(DuckDbInitializer duckDb, ServerConnection server, DateTime utc, int offset, string? zoneId)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        await connection.OpenAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"INSERT INTO server_properties
            (collection_id, collection_time, server_id, server_name,
             edition, product_version, product_level, engine_edition,
             cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes, time_zone_id)
            VALUES ($1, $2, $3, 'seed-test', 'Developer Edition', '16.0.4150.1', 'RTM', 3, 8, 1, 16384, $4, $5)";
        command.Parameters.Add(new DuckDBParameter { Value = Interlocked.Decrement(ref s_nextLogId) });
        command.Parameters.Add(new DuckDBParameter { Value = utc });
        command.Parameters.Add(new DuckDBParameter { Value = RemoteCollectorService.GetServerId(server) });
        command.Parameters.Add(new DuckDBParameter { Value = offset });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)zoneId ?? DBNull.Value });
        await command.ExecuteNonQueryAsync();
    }

    private static List<string> Due(ScheduleManager manager, ServerConnection server) =>
        manager.GetDueCollectorsForServer(server.Id, DateTime.UtcNow).Select(s => s.Name).ToList();

    [Fact]
    public async Task TheStartupRead_SeedsEveryDailyCollectorFromItsLog_SoARestartTheSameDayDoesNotMakeItDue()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            await LogAsync(duckDb, server, "index_object_stats", Now.AddHours(-3), "SUCCESS");
            await LogAsync(duckDb, server, "trace_flags", Now.AddHours(-2), "SUCCESS");
            await LogAsync(duckDb, server, "server_config", Now.AddHours(-30), "SUCCESS");
            await LogAsync(duckDb, server, "wait_stats", Now.AddMinutes(-10), "SUCCESS");

            var first = new ScheduleManager(_dir);
            var firstService = new RemoteCollectorService(duckDb, servers, first);
            await firstService.EnsureRunTimeReadyAsync(server);

            var due = Due(first, server);
            Assert.DoesNotContain("index_object_stats", due);
            Assert.DoesNotContain("trace_flags", due);
            Assert.Contains("server_config", due);
            Assert.Contains("database_config", due);

            /* An hourly collector is not seeded: its in-memory rule is unchanged. */
            Assert.Contains("wait_stats", due);

            /* The same day, a restart: a new scheduler and a new service over the same local database. */
            var second = new ScheduleManager(_dir);
            var secondService = new RemoteCollectorService(duckDb, servers, second);
            Assert.Contains("index_object_stats", Due(second, server));
            await secondService.EnsureRunTimeReadyAsync(server);
            Assert.DoesNotContain("index_object_stats", Due(second, server));
            Assert.DoesNotContain("trace_flags", Due(second, server));

            /* The tab-open run skips them too, and still runs the on-load collectors. */
            var tabOpen = second.GetCollectorsForTabOpen(server.Id, DateTime.UtcNow).Select(s => s.Name).ToList();
            Assert.DoesNotContain("index_object_stats", tabOpen);
            Assert.Contains("trace_flags", tabOpen);
            Assert.Contains("server_config", tabOpen);
            Assert.Contains("wait_stats", tabOpen);
        }
        finally
        {
            duckDb.Dispose();
        }
    }

    [Fact]
    public async Task TheStartupRead_CountsAnyStatusAsARun_AsDarlingsConnectTimeReadDoes()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            await LogAsync(duckDb, server, "index_object_stats", Now.AddHours(-3), "ERROR");
            await LogAsync(duckDb, server, "trace_flags", Now.AddHours(-2), "PERMISSIONS");
            await LogAsync(duckDb, server, "server_config", Now.AddHours(-1), "CANCELLED");

            var manager = new ScheduleManager(_dir);
            await new RemoteCollectorService(duckDb, servers, manager).EnsureRunTimeReadyAsync(server);

            var due = Due(manager, server);
            Assert.DoesNotContain("index_object_stats", due);
            Assert.DoesNotContain("trace_flags", due);
            Assert.DoesNotContain("server_config", due);
        }
        finally
        {
            duckDb.Dispose();
        }
    }

    [Fact]
    public async Task TheStartupRead_IsDoneOncePerServer_NotOnEveryCycle()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            var manager = new ScheduleManager(_dir);
            var service = new RemoteCollectorService(duckDb, servers, manager);
            await service.EnsureRunTimeReadyAsync(server);
            Assert.Contains("index_object_stats", Due(manager, server));

            /* A row that lands after the read is not read again: the in-memory state carries this run. */
            await LogAsync(duckDb, server, "index_object_stats", Now.AddHours(-1), "SUCCESS");
            await service.EnsureRunTimeReadyAsync(server);
            Assert.Contains("index_object_stats", Due(manager, server));
        }
        finally
        {
            duckDb.Dispose();
        }
    }

    [Fact]
    public async Task TheClock_IsTheNewestServerPropertiesRow_AndTheNextRowMovesTheRunTime()
    {
        var (duckDb, servers, server) = await OpenAsync();
        try
        {
            var day = new DateOnly(2026, 7, 15);
            var storageId = RemoteCollectorService.GetServerId(server);
            var eastern = ServerClock.Resolve("Eastern Standard Time", -300);
            var pacific = ServerClock.Resolve("Pacific Standard Time", -480);
            DateTime Slot(ServerClock clock) => CollectorRunTime.SlotUtc(day, 240, storageId, clock.ToUtc);

            await ServerPropertiesAsync(duckDb, server, new DateTime(2026, 7, 1, 0, 0, 0, DateTimeKind.Utc), -240, "Eastern Standard Time");

            var manager = new ScheduleManager(_dir);
            manager.SetScheduleForServer(server.Id, new List<CollectorSchedule>
            {
                new() { Name = "index_object_stats", Enabled = true, FrequencyMinutes = 1440, RunAt = "04:00" },
            });
            manager.MarkCollectorRunForServer(server.Id, "index_object_stats", Slot(eastern) - TimeSpan.FromDays(1) + TimeSpan.FromMinutes(5));
            var service = new RemoteCollectorService(duckDb, servers, manager);
            await service.EnsureRunTimeReadyAsync(server);

            Assert.Empty(manager.GetDueCollectorsForServer(server.Id, Slot(eastern) - TimeSpan.FromMinutes(1)));
            Assert.Single(manager.GetDueCollectorsForServer(server.Id, Slot(eastern) + TimeSpan.FromMinutes(1)));

            /* A newer row with another zone: the next refresh moves the slot to the new zone's 04:00. */
            await ServerPropertiesAsync(duckDb, server, new DateTime(2026, 7, 2, 0, 0, 0, DateTimeKind.Utc), -420, "Pacific Standard Time");
            await service.RefreshRunTimeClockAsync(server);

            Assert.Empty(manager.GetDueCollectorsForServer(server.Id, Slot(eastern) + TimeSpan.FromMinutes(1)));
            Assert.Single(manager.GetDueCollectorsForServer(server.Id, Slot(pacific) + TimeSpan.FromMinutes(1)));
        }
        finally
        {
            duckDb.Dispose();
        }
    }
}
