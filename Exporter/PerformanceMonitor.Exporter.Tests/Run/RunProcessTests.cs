using System.Diagnostics;
using System.Globalization;
using System.Text.Json.Nodes;
using PerformanceMonitor.Exporter.Run;
using Xunit;
using static PerformanceMonitor.Exporter.Tests.Run.RunRoot;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>R18, R19 (memory guard with a real child process), R21-R23 (log, retention, cleanup) and plan-only.</summary>
public class RunProcessTests
{
    private static ChildCall Call(RunRoot r, params string[] args)
    {
        Directory.CreateDirectory(r.Logs);
        var log = Path.Combine(r.Logs, "c-" + Guid.NewGuid().ToString("N") + ".log");
        return new ChildCall(TestChildPath.Exe, args, log, log + ".err");
    }

    private static ProcessChildRunner Runner(MemoryGuardSettings g, IHostMemory m, ChildRunnerTiming? t = null) =>
        new(g, m, TextWriter.Null, t ?? new ChildRunnerTiming(TimeSpan.FromMilliseconds(50), 2));

    // ---- R19: memory guard ----

    [Fact]
    public async Task R19_refuses_to_start_below_twice_the_floor_and_never_starts_the_child()
    {
        using var r = new RunRoot();
        var marker = Path.Combine(r.Root, "started");
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 6144 }, new FakeHostMemory(12287)).RunAsync(Call(r, "--tc-write-file", marker), CancellationToken.None);
        Assert.Equal("only 12287 MB free; need at least 12288 MB to start", result.NotStarted);
        Assert.False(File.Exists(marker));
    }

    [Fact]
    public async Task R19_starts_at_exactly_twice_the_floor_and_a_short_child_returns_at_once_with_its_output_and_exit_code()
    {
        using var r = new RunRoot();
        var sw = Stopwatch.StartNew();
        // The production cadence: a child that exits in well under a second must not wait out a 1 s or 10 s tick.
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 6144 }, new FakeHostMemory(12288), ChildRunnerTiming.Production)
            .RunAsync(Call(r, "--tc-print", "hello out", "--tc-stderr", "hello err", "--tc-exit", "3"), CancellationToken.None);
        sw.Stop();
        Assert.Null(result.NotStarted);
        Assert.Equal(3, result.ExitCode);
        Assert.False(result.Aborted);
        Assert.True(sw.Elapsed.TotalSeconds < 8, $"took {sw.Elapsed.TotalSeconds:N1} s");
        Assert.True(result.ElapsedSeconds < 8);
        Assert.Equal(12288, result.MinFreeMb);
        Assert.Contains("hello out", File.ReadAllText(result.LogPath));
        Assert.Contains("hello err", File.ReadAllText(result.ErrPath));
    }

    [Fact]
    public async Task R19_kills_the_child_when_free_memory_falls_below_the_floor_and_reports_exit_9_not_3()
    {
        using var r = new RunRoot();
        var calls = 0;
        // 4000 MB at the start check, then the host runs out. The child would sleep 60 s; the abort must come first.
        var memory = new FakeHostMemory(() => ++calls == 1 ? 4000 : 100);
        var sw = Stopwatch.StartNew();
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 1000 }, memory).RunAsync(Call(r, "--tc-sleep-ms", "60000"), CancellationToken.None);
        Assert.True(sw.Elapsed.TotalSeconds < 20);
        Assert.True(result.Aborted);
        Assert.Equal(9, result.ExitCode);
        Assert.NotEqual(3, result.ExitCode);
        Assert.Equal(100, result.AbortFreeMb);
        Assert.Equal(1000, result.FloorMb);
        Assert.Equal(100, result.MinFreeMb);
    }

    [Fact]
    public async Task R19_the_free_memory_check_runs_every_nth_tick_not_every_tick()
    {
        using var r = new RunRoot();
        var calls = 0;
        var memory = new FakeHostMemory(() => { calls++; return 50000; });
        await Runner(new MemoryGuardSettings { MinFreeMb = 1000 }, memory, new ChildRunnerTiming(TimeSpan.FromMilliseconds(50), 4))
            .RunAsync(Call(r, "--tc-sleep-ms", "1000"), CancellationToken.None);
        // ~20 ticks in 1 s: one start check plus about 5 periodic checks, never ~20.
        Assert.InRange(calls, 2, 12);
    }

    [Fact]
    public async Task R19_a_quick_child_returns_when_it_exits_not_when_the_next_sample_tick_comes_due()
    {
        // A quick exporter returns at once (no 10 s sleep floor): here the tick is 60 s, so any wait for it is obvious.
        using var r = new RunRoot();
        var sw = Stopwatch.StartNew();
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 1 }, new FakeHostMemory(50000), new ChildRunnerTiming(TimeSpan.FromSeconds(60), 10))
            .RunAsync(Call(r, "--tc-print", "quick"), CancellationToken.None);
        Assert.Equal(0, result.ExitCode);
        Assert.True(sw.Elapsed.TotalSeconds < 15, $"took {sw.Elapsed.TotalSeconds:N1} s");
        Assert.True(result.ElapsedSeconds < 15);
    }

    [Fact]
    public async Task R19_reports_the_lowest_free_memory_seen_not_the_last_one()
    {
        using var r = new RunRoot();
        var calls = 0;
        var memory = new FakeHostMemory(() => ++calls switch { 1 => 4000, 2 => 200, _ => 5000 });   // start, one dip above the floor, then recovery
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 50 }, memory, new ChildRunnerTiming(TimeSpan.FromMilliseconds(50), 1))
            .RunAsync(Call(r, "--tc-sleep-ms", "1200"), CancellationToken.None);
        Assert.False(result.Aborted);
        Assert.True(calls > 3);
        Assert.Equal(200, result.MinFreeMb);
    }

    [Fact]
    public async Task R19_reports_the_peak_private_memory_of_a_child_that_allocates()
    {
        using var r = new RunRoot();
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 1 }, new FakeHostMemory(50000))
            .RunAsync(Call(r, "--tc-alloc-mb", "200", "--tc-sleep-ms", "1500"), CancellationToken.None);
        Assert.Equal(0, result.ExitCode);
        Assert.True(result.PeakPrivateMb >= 150, $"peak {result.PeakPrivateMb} MB");
    }

    [Fact]
    public async Task R19_runs_the_child_at_below_normal_priority()
    {
        if (OperatingSystem.IsWindows()) { /* PriorityClass is settable on Windows too; the same assertion holds */ }
        using var r = new RunRoot();
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 1 }, new FakeHostMemory(50000))
            .RunAsync(Call(r, "--tc-sleep-ms", "300", "--tc-print-priority", "x"), CancellationToken.None);
        Assert.Contains("priority=BelowNormal", File.ReadAllText(result.LogPath));
    }

    [Fact]
    public async Task R19_a_host_that_cannot_report_free_memory_skips_the_guard_and_the_child_runs()
    {
        using var r = new RunRoot();
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 6144 }, new FakeHostMemory((int?)null)).RunAsync(Call(r, "--tc-sleep-ms", "300"), CancellationToken.None);
        Assert.Null(result.NotStarted);
        Assert.Equal(0, result.ExitCode);
        Assert.Null(result.MinFreeMb);
    }

    [Fact]
    public async Task R19_a_disabled_guard_never_asks_for_free_memory()
    {
        using var r = new RunRoot();
        var asked = 0;
        var result = await Runner(new MemoryGuardSettings { Enabled = false, MinFreeMb = 99999999 }, new FakeHostMemory(() => { asked++; return 1; }))
            .RunAsync(Call(r, "--tc-sleep-ms", "300"), CancellationToken.None);
        Assert.Equal(0, result.ExitCode);
        Assert.Equal(0, asked);
    }

    [Fact]
    public async Task R19_a_child_exe_that_does_not_exist_is_a_not_started_failure_not_an_exception()
    {
        using var r = new RunRoot();
        Directory.CreateDirectory(r.Logs);
        var log = Path.Combine(r.Logs, "x.log");
        var result = await Runner(new MemoryGuardSettings { MinFreeMb = 1 }, new FakeHostMemory(50000)).RunAsync(new ChildCall("/nonexistent/exporter", ["export"], log, log + ".err"), CancellationToken.None);
        Assert.StartsWith("could not start", result.NotStarted);
    }

    [Fact]
    public async Task R19_a_guard_abort_through_the_scheduler_is_a_failure_exit_1_and_last_to_does_not_advance()
    {
        using var r = new RunRoot(); r.SeedState();
        var calls = 0;
        var memory = new FakeHostMemory(() => ++calls == 1 ? 50000 : 10);
        var s = r.Settings(tweak: x => x with
        {
            ChildExe = TestChildPath.Exe, ChildBaseArgs = ["--tc-sleep-ms", "60000"], MemoryGuard = new MemoryGuardSettings { MinFreeMb = 1000, StartMultiplier = 2 },
        });
        var output = new StringWriter();
        var runner = new ProcessChildRunner(s.MemoryGuard, memory, output, new ChildRunnerTiming(TimeSpan.FromMilliseconds(50), 2));
        var (exit, _) = await Runs.Go(s, runner, Runs.Pin, memory: memory);
        var st = r.ReadState();
        Assert.Equal(1, exit);
        Assert.Equal("2026-09-28T12:00:00.0000000Z", st["fresh"]!["last_to"]!.GetValue<string>());
        Assert.Contains("memory guard stopped the exporter (exit 9)", st["fresh"]!["last_error"]!.GetValue<string>());
        Assert.Equal(9, st["fresh"]!["last_exit_code"] is null ? 9 : st["last_exit_code"]!.GetValue<int>());
    }

    [Fact]
    public void R19_linux_free_memory_is_mem_available_in_mb_and_a_missing_line_is_unknown()
    {
        Assert.Equal(2048, SystemHostMemory.ParseMemAvailable(["MemTotal:       16384000 kB", "MemFree:         100000 kB", "MemAvailable:    2097152 kB"]));
        Assert.Null(SystemHostMemory.ParseMemAvailable(["MemTotal: 1 kB"]));
    }

    [Fact]
    public void R19_the_test_override_environment_variables_win_over_the_platform_probe()
    {
        using var r = new RunRoot();
        var file = Path.Combine(r.Root, "free.txt");
        File.WriteAllText(file, " 321 \n");
        Environment.SetEnvironmentVariable("DARLING_EXPORT_TEST_FREE_MB_FILE", file);
        Environment.SetEnvironmentVariable("DARLING_EXPORT_TEST_FREE_MB", "999");
        try
        {
            Assert.Equal(321, new SystemHostMemory(TextWriter.Null).FreeMb());
            File.Delete(file);
            Assert.Equal(999, new SystemHostMemory(TextWriter.Null).FreeMb());
        }
        finally
        {
            Environment.SetEnvironmentVariable("DARLING_EXPORT_TEST_FREE_MB_FILE", null);
            Environment.SetEnvironmentVariable("DARLING_EXPORT_TEST_FREE_MB", null);
        }
        // Without overrides this host reports a real number (Linux/Windows) or warns once and returns null (macOS).
        var warn = new StringWriter();
        var m = new SystemHostMemory(warn);
        var first = m.FreeMb(); var second = m.FreeMb();
        if (OperatingSystem.IsLinux() || OperatingSystem.IsWindows()) Assert.True(first > 0);
        else { Assert.Null(first); Assert.Null(second); Assert.Single(warn.ToString().TrimEnd().Split('\n')); }
    }

    // ---- R18: single instance ----

    [Fact]
    public async Task R18_a_second_run_that_finds_the_lock_held_prints_the_message_exits_0_and_starts_no_child()
    {
        using var r = new RunRoot(); r.SeedState();
        var before = File.ReadAllBytes(r.StatePath);
        using var held = RunLock.TryAcquire(r.State);
        Assert.NotNull(held);
        var (exit, output) = await Runs.Go(r.Settings(), new NoChildAllowed(), Runs.Pin);
        Assert.Equal(0, exit);
        Assert.Contains("another run holds the lock; exiting", output);
        Assert.Equal(before, File.ReadAllBytes(r.StatePath));
    }

    [Fact]
    public async Task R18_two_concurrent_runs_one_runs_the_child_the_other_exits_0_with_no_calls()
    {
        using var r = new RunRoot(); r.SeedState();
        var pidFile = Path.Combine(r.Root, "pid");
        var slow = r.Settings(tweak: x => x with
        {
            ChildExe = TestChildPath.Exe, ChildBaseArgs = ["--tc-sleep-ms", "3000", "--tc-write-file", pidFile],
        });
        var runnerA = new ProcessChildRunner(slow.MemoryGuard, new FakeHostMemory(50000), TextWriter.Null);
        var first = Runs.Go(slow, runnerA, Runs.Pin);
        var waited = Stopwatch.StartNew();
        while (!File.Exists(pidFile) && waited.Elapsed.TotalSeconds < 20) await Task.Delay(50);
        Assert.True(File.Exists(pidFile), "the first run never started its child");
        var (exit2, output2) = await Runs.Go(r.Settings(), new NoChildAllowed(), Runs.Pin);
        Assert.Equal(0, exit2);
        Assert.Contains("another run holds the lock; exiting", output2);
        Assert.Equal(0, (await first).Exit);
    }

    [Fact]
    public async Task R18_the_lock_is_released_when_the_run_ends_even_after_a_failure()
    {
        using var r = new RunRoot(); r.SeedState();
        await Runs.Go(r.Settings(), new FakeChild { ExitFor = _ => 1 }, Runs.Pin);
        using var again = RunLock.TryAcquire(r.State);
        Assert.NotNull(again);
    }

    // ---- R21: the daily log ----

    [Fact]
    public async Task R21_the_daily_log_has_the_exact_start_stdout_stderr_hook_and_end_lines_without_a_bom_and_per_run_files_are_gone()
    {
        using var r = new RunRoot(); r.SeedState();
        var hook = new PostRunSettings { Name = "alert-check", Command = [TestChildPath.Exe, "--tc-print", "hook says hi", "--tc-exit", "0"] };
        var child = new FakeChild { ExitFor = _ => 1, StderrFor = _ => "stub-boom" };
        var (exit, _) = await Runs.Go(r.Settings(tweak: x => x with { PostRun = hook }), child, Utc(2026, 9, 28, 12, 0) is var n ? Utc(2026, 9, 28, 13, 20) : n);
        Assert.Equal(1, exit);
        var files = Directory.GetFiles(r.Logs);
        Assert.Equal(new[] { "export-testh-20260928.log" }, files.Select(Path.GetFileName));
        var bytes = File.ReadAllBytes(files[0]);
        Assert.False(bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF);
        var text = File.ReadAllText(files[0]);
        Assert.StartsWith("===== run 20260928T132000Z start export testh\nstub ran\n----- stderr fresh\nstub-boom\n", text);
        Assert.Contains("----- alert-check exit 0\nhook says hi\n", text);
        Assert.Matches(@"\n===== run 20260928T132000Z end exit=1 elapsed=\d+s\n$", text);
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(text, @"(?m)^===== run 20260928T132000Z start export testh$"));
        Assert.DoesNotContain("\r", text);
    }

    [Fact]
    public async Task R21_a_success_ends_exit_0_with_no_stderr_section_and_a_second_run_appends_and_a_new_day_starts_a_new_file()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings();
        await Runs.Go(s, new FakeChild(), Utc(2026, 9, 28, 13, 20));
        var text = File.ReadAllText(Path.Combine(r.Logs, "export-testh-20260928.log"));
        Assert.Matches(@"(?m)^===== run 20260928T132000Z end exit=0 elapsed=\d+s$", text);
        Assert.DoesNotContain("stderr", text);
        await Runs.Go(s, new FakeChild(), Utc(2026, 9, 28, 14, 20));
        text = File.ReadAllText(Path.Combine(r.Logs, "export-testh-20260928.log"));
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(text, @"(?m)^===== run .* start export testh$").Count);
        await Runs.Go(s, new FakeChild(), Utc(2026, 9, 29, 0, 20));
        Assert.Equal(new[] { "export-testh-20260928.log", "export-testh-20260929.log" }, Directory.GetFiles(r.Logs).Select(Path.GetFileName).OrderBy(x => x, StringComparer.Ordinal));
    }

    [Fact]
    public async Task R21_the_log_path_in_last_run_names_a_file_that_exists_after_the_run()
    {
        using var r = new RunRoot(); r.SeedState();
        await Runs.Go(r.Settings(), new FakeChild(), Runs.Pin);
        Assert.True(File.Exists(r.ReadState()["last_run"]!["log_path"]!.GetValue<string>()));
    }

    // ---- R22: log retention ----

    [Fact]
    public async Task R22_15_day_daily_and_25_hour_leftovers_go_13_day_and_23_hour_ones_stay()
    {
        using var r = new RunRoot(); r.SeedState();
        Directory.CreateDirectory(r.Logs);
        void Make(string name, DateTime? written = null)
        {
            var p = Path.Combine(r.Logs, name);
            File.WriteAllText(p, "x");
            if (written is { } w) File.SetLastWriteTimeUtc(p, w);
        }
        var real = DateTime.UtcNow;
        Make("export-testh-20260913.log"); Make("export-testh-20260915.log");
        Make("alert-check-20260913-010101.log", real.AddHours(-25));
        Make("export-testh-20260913T010101Z.log", real.AddHours(-25)); Make("export-testh-20260913T010101Z.log.err", real.AddHours(-25));
        Make("export-testh-20260928T000101Z.log", real.AddHours(-23)); Make("alert-check-20260928-000101.log", real.AddHours(-23));
        var hook = new PostRunSettings { Name = "alert-check", Command = [TestChildPath.Exe] };
        // Real clock for the leftover rule; --now (the scheduling instant) for the daily-file date rule.
        await Runs.Go(r.Settings(tweak: x => x with { PostRun = hook }), new FakeChild(), Utc(2026, 9, 28, 12, 0), clockNow: real);
        var names = Directory.GetFiles(r.Logs).Select(Path.GetFileName).OrderBy(x => x, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "alert-check-20260928-000101.log", "export-testh-20260915.log", "export-testh-20260928.log", "export-testh-20260928T000101Z.log" }, names);
    }

    [Fact]
    public async Task R22_with_no_post_run_hook_alert_check_leftovers_and_15_day_plan_only_files_still_go()
    {
        using var r = new RunRoot(); r.SeedState();
        Directory.CreateDirectory(r.Logs);
        void Make(string name, DateTime? written = null)
        {
            var p = Path.Combine(r.Logs, name);
            File.WriteAllText(p, "x");
            if (written is { } w) File.SetLastWriteTimeUtc(p, w);
        }
        var real = DateTime.UtcNow;
        Make("alert-check-20260913-010101.log", real.AddHours(-25)); Make("alert-check-20260928-000101.log", real.AddHours(-23));
        foreach (var old in new[] { "plan-only-stdout-testh-20260913.log", "plan-only-stderr-testh-20260913.log", "plan-only-testh-20260913.jsonl", "plan-actual-testh-20260913.jsonl" }) Make(old);
        foreach (var kept in new[] { "plan-only-stdout-testh-20260915.log", "plan-only-testh-20260915.jsonl", "plan-actual-testh-20260915.jsonl" }) Make(kept);
        await Runs.Go(r.Settings(), new FakeChild(), Utc(2026, 9, 28, 12, 0), clockNow: real);
        var names = Directory.GetFiles(r.Logs).Select(Path.GetFileName).OrderBy(x => x, StringComparer.Ordinal).ToArray();
        Assert.Equal(new[] { "alert-check-20260928-000101.log", "export-testh-20260928.log", "plan-actual-testh-20260915.jsonl", "plan-only-stdout-testh-20260915.log", "plan-only-testh-20260915.jsonl" }, names);
    }

    // ---- R23: cleanup ----

    [Fact]
    public void R23_out_files_older_than_48_hours_go_newer_stay_emptied_folders_go_and_the_store_folder_stays()
    {
        using var r = new RunRoot();
        var now = Utc(2026, 10, 11, 12, 0);
        var store = Path.Combine(r.Out, "store_a");
        var oldDir = Path.Combine(store, "perfmon_stats");
        var keepDir = Path.Combine(store, "_manifests");
        Directory.CreateDirectory(oldDir); Directory.CreateDirectory(keepDir);
        string Make(string dir, string name, double hoursOld)
        {
            var p = Path.Combine(dir, name); File.WriteAllText(p, "x"); File.SetLastWriteTimeUtc(p, now.AddHours(-hoursOld)); return p;
        }
        var old = Make(oldDir, "old.parquet", 49);
        var recent = Make(keepDir, "new.json", 47);
        var edge = Path.Combine(keepDir, "edge.json"); File.WriteAllText(edge, "x"); File.SetLastWriteTimeUtc(edge, now.AddHours(-48).AddSeconds(1));
        RunFiles.ClearOldExportOutput(store, now, 48);
        Assert.False(File.Exists(old));
        Assert.True(File.Exists(recent));
        Assert.True(File.Exists(edge));
        Assert.False(Directory.Exists(oldDir));
        Assert.True(Directory.Exists(store));
        RunFiles.ClearOldExportOutput(Path.Combine(r.Out, "missing"), now, 48);   // a missing folder is a no-op
    }

    [Fact]
    public void R23_stale_temp_entries_with_the_exporters_prefixes_go_files_and_folders_other_names_never()
    {
        using var r = new RunRoot();
        var now = Utc(2026, 10, 11, 12, 0);
        string File_(string name, double hours) { var p = Path.Combine(r.Temp, name); File.WriteAllText(p, "x"); File.SetLastWriteTimeUtc(p, now.AddHours(-hours)); return p; }
        var staleDb = File_("darling-export-0123.duckdb", 25);
        var staleNew = File_("darling-export-0123.duckdb", 25);
        var freshDb = File_("darling-export-4567.duckdb", 1);
        var other = File_("other.tmp", 100);
        var staleDir = Path.Combine(r.Temp, "darling-export-diag-keys"); Directory.CreateDirectory(staleDir);
        File.WriteAllText(Path.Combine(staleDir, "a.csv"), "x");
        Directory.SetLastWriteTimeUtc(staleDir, now.AddHours(-30));
        RunFiles.ClearStaleTemp(r.Temp, ["darling-export-", "darling-export-"], now, 24);
        Assert.False(File.Exists(staleDb));
        Assert.False(File.Exists(staleNew));
        Assert.False(Directory.Exists(staleDir));
        Assert.True(File.Exists(freshDb));
        Assert.True(File.Exists(other));
        RunFiles.ClearStaleTemp(Path.Combine(r.Root, "no-such-temp"), ["darling-export-"], now, 24);
    }

    [Fact]
    public async Task R23_cleanup_never_throws_even_for_a_missing_out_folder_and_runs_while_holding_the_lock()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings();
        // out/<store> does not exist and the temp dir is missing: the run still ends cleanly.
        var (exit, _) = await Runs.Go(s with { Retention = s.Retention with { TempDir = Path.Combine(r.Root, "nope") } }, new FakeChild(), Runs.Pin);
        Assert.Equal(0, exit);
        var old = Path.Combine(r.Out, "testh", "x"); Directory.CreateDirectory(old);
        var f = Path.Combine(old, "a.parquet"); File.WriteAllText(f, "x"); File.SetLastWriteTimeUtc(f, Runs.Pin.AddHours(-60));
        await Runs.Go(s, new FakeChild(), Runs.Pin);
        Assert.False(File.Exists(f));
        Assert.True(Directory.Exists(Path.Combine(r.Out, "testh")));
    }

    // ---- plan-only (design §6) ----

    private static JsonObject ReadPlan(RunRoot r, string store = "testh", string day = "20260928")
    {
        var path = Path.Combine(r.Logs, $"plan-only-{store}-{day}.jsonl");
        var lines = File.ReadAllLines(path);
        return (JsonObject)JsonNode.Parse(lines[^1])!;
    }

    [Fact]
    public async Task Plan_only_writes_one_json_line_with_exactly_the_field_names_and_shapes_in_design_6()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = Fast(r, 10, x => x with { Families = x.Families with { OptionalTables = ["plan_correction"], QueryStoreLagHours = 2, LateRowMarginMinutes = 5 } });
        var (exit, _) = await Runs.Go(s, new NoChildAllowed(), Runs.Pin, planOnly: true);
        Assert.Equal(0, exit);
        var plan = ReadPlan(r);
        Assert.Equal("kind,now_utc,store,decision,restart_utc,free_mb,query_store_lag,late_margin,optional_tables,families,calls,cutoff_minute,exempt_from_cutoff,state_file",
            string.Join(',', plan.Select(p => p.Key)));
        Assert.Equal("plan", plan["kind"]!.GetValue<string>());
        Assert.Equal("2026-09-28T16:30:00.0000000Z", plan["now_utc"]!.GetValue<string>());
        Assert.Equal("testh", plan["store"]!.GetValue<string>());
        Assert.Equal("run", plan["decision"]!.GetValue<string>());
        Assert.Null(plan["restart_utc"]);
        Assert.Equal(32768, plan["free_mb"]!.GetValue<int>());
        Assert.Equal(("2", "config"), (plan["query_store_lag"]!["value"]!.ToString(), plan["query_store_lag"]!["source"]!.GetValue<string>()));
        Assert.Equal(("5", "config"), (plan["late_margin"]!["value"]!.ToString(), plan["late_margin"]!["source"]!.GetValue<string>()));
        Assert.Equal("plan_correction", plan["optional_tables"]![0]!.GetValue<string>());
        Assert.Equal(60, plan["cutoff_minute"]!.GetValue<int>());
        Assert.Equal("fresh_fast", plan["exempt_from_cutoff"]![0]!.GetValue<string>());
        Assert.Equal("loaded", plan["state_file"]!.GetValue<string>());
        var fam = plan["families"]!.AsArray();
        Assert.Equal(new[] { "fresh_fast", "fresh", "query_store" }, fam.Select(f => f!["name"]!.GetValue<string>()));
        Assert.All(fam, f => Assert.Equal("name,status,entries", string.Join(',', f!.AsObject().Select(p => p.Key))));
        Assert.Equal("due", fam[0]!["status"]!.GetValue<string>());
        Assert.Equal("from,to,gap", string.Join(',', fam[0]!["entries"]![0]!.AsObject().Select(p => p.Key)));
        var calls = plan["calls"]!.AsArray();
        Assert.Equal("family,from,to,tables,dimension_origin,also,query_store_lag_hours,fresh_guard_minutes,argv", string.Join(',', calls[0]!.AsObject().Select(p => p.Key)));
        var fast = calls.Where(c => c!["family"]!.GetValue<string>() == "fresh_fast").ToArray();
        Assert.Equal(5, fast.Length);
        Assert.Equal(5, fast[0]!["fresh_guard_minutes"]!.GetValue<int>());
        var argv = fast[0]!["argv"]!.AsArray().Select(a => a!.GetValue<string>()).ToArray();
        Assert.Equal(["export", "--store", "testh", "--from", "2026-09-28T12:00:00Z"], argv[..5]);
        Assert.Equal("5", argv[Array.IndexOf(argv, "--fresh-guard-minutes") + 1]);
        Assert.Null(fast[0]!["dimension_origin"]);
        Assert.Equal("2026-09-28T12:00:00Z", fast[0]!["from"]!.GetValue<string>());
        var freshCall = calls.First(c => c!["family"]!.GetValue<string>() == "fresh")!;
        Assert.Equal(("query_stats", "plan_correction"), (freshCall["dimension_origin"]!.GetValue<string>(), freshCall["also"]!.GetValue<string>()));
        var qsCall = calls.First(c => c!["family"]!.GetValue<string>() == "query_store")!;
        Assert.Equal(2, qsCall["query_store_lag_hours"]!.GetValue<int>());
        Assert.Null(qsCall["fresh_guard_minutes"]);
    }

    private static RunSettings Fast(RunRoot r, int n, Func<RunSettings, RunSettings> more) =>
        r.Settings(tweak: s => more(s) with { Families = more(s).Families with { FastWindowMinutes = n } });

    [Fact]
    public async Task Plan_only_calls_equal_the_calls_a_real_run_makes_at_the_same_instant()
    {
        using var planRoot = new RunRoot(); planRoot.SeedState();
        using var runRoot = new RunRoot(); runRoot.SeedState();
        await Runs.Go(planRoot.Settings(), new NoChildAllowed(), Runs.Pin, planOnly: true);
        var child = new FakeChild();
        await Runs.Go(runRoot.Settings(), child, Runs.Pin);
        var planned = ReadPlan(planRoot)["calls"]!.AsArray();
        Assert.Equal(child.Calls.Count, planned.Count);
        for (var i = 0; i < planned.Count; i++)
        {
            Assert.Equal(FakeChild.Arg(child.Calls[i], "--from"), planned[i]!["from"]!.GetValue<string>());
            Assert.Equal(FakeChild.Arg(child.Calls[i], "--to"), planned[i]!["to"]!.GetValue<string>());
            Assert.Equal(FakeChild.Arg(child.Calls[i], "--tables"), planned[i]!["tables"]!.GetValue<string>());
        }
    }

    [Fact]
    public async Task Plan_only_with_an_unreachable_store_and_an_unwritable_destination_still_plans_and_leaves_state_byte_identical_and_touches_nothing_else()
    {
        using var r = new RunRoot(); r.SeedState();
        var before = File.ReadAllBytes(r.StatePath);
        var s = r.Settings(tweak: x => x with
        {
            Upload = "s3://no-such-bucket-12345/landing",     // the "unwritable destination": nothing may ever try it
            ChildExe = "/nonexistent/darling-export",           // the "unreachable store" lives behind this child
            PgUser = "nobody", PgCredentialFile = "/nonexistent/credential",
            PostRun = new PostRunSettings { Name = "alert-check", Command = ["/nonexistent/alert-check"] },
            AwsRegion = "us-nowhere-1",
        });
        // Old files that a real run's cleanup would delete.
        Directory.CreateDirectory(r.Logs);
        var oldLog = Path.Combine(r.Logs, "export-testh-20200101.log"); File.WriteAllText(oldLog, "x");
        var oldOut = Path.Combine(r.Out, "testh", "old.parquet"); Directory.CreateDirectory(Path.GetDirectoryName(oldOut)!); File.WriteAllText(oldOut, "x"); File.SetLastWriteTimeUtc(oldOut, Utc(2020, 1, 1, 0, 0));
        var (exit, output) = await Runs.Go(s, new NoChildAllowed(), Runs.Pin, planOnly: true);
        Assert.Equal(0, exit);
        Assert.Contains("plan-only: run", output);
        Assert.Equal(before, File.ReadAllBytes(r.StatePath));
        Assert.True(File.Exists(oldLog));
        Assert.True(File.Exists(oldOut));
        Assert.False(File.Exists(RunLock.PathFor(r.State)), "plan-only must take no lock");
        Assert.False(File.Exists(Path.Combine(r.Logs, "export-testh-20260928.log")), "plan-only must write no daily log");
        Assert.Equal(new[] { "export-testh-20200101.log", "plan-only-testh-20260928.jsonl" }, Directory.GetFiles(r.Logs).Select(Path.GetFileName).OrderBy(x => x, StringComparer.Ordinal));
        Assert.DoesNotContain(Directory.GetFiles(r.State), f => !f.EndsWith("export-schedule.json"));
        Assert.NotEmpty(ReadPlan(r)["calls"]!.AsArray());
    }

    [Fact]
    public async Task Plan_only_appends_lines_and_is_valid_with_no_state_file_at_all()
    {
        using var r = new RunRoot();
        await Runs.Go(r.Settings(), new NoChildAllowed(), Runs.Pin, planOnly: true);
        await Runs.Go(r.Settings(), new NoChildAllowed(), Runs.Pin.AddMinutes(10), planOnly: true);
        Assert.Equal(2, File.ReadAllLines(Path.Combine(r.Logs, "plan-only-testh-20260928.jsonl")).Length);
        Assert.Equal("missing", ReadPlan(r)["state_file"]!.GetValue<string>());
        Assert.False(File.Exists(r.StatePath));
        Assert.False(Directory.Exists(r.State));
    }

    [Theory]
    [InlineData("{\"store\":\"testh\",\"last_exported_day\":\"2026-09-27\"}", "not_schema_2")]
    [InlineData("not json", "unusable")]
    public async Task Plan_only_names_why_a_state_file_was_refused(string content, string expected)
    {
        using var r = new RunRoot();
        Directory.CreateDirectory(r.State);
        File.WriteAllText(r.StatePath, content);
        var (exit, _) = await Runs.Go(r.Settings(), new NoChildAllowed(), Runs.Pin, planOnly: true);
        Assert.Equal(2, exit);
        Assert.Equal(("refuse_state", expected), (ReadPlan(r)["decision"]!.GetValue<string>(), ReadPlan(r)["state_file"]!.GetValue<string>()));
    }

    [Fact]
    public async Task Plan_only_decisions_skip_restart_and_refuse_state_and_run()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Restart = x.Restart with { DelayMinutes = 30 } });
        var (e1, _) = await Runs.Go(s, new NoChildAllowed(), Utc(2026, 9, 28, 15, 30), restart: new FixedRestart(Utc(2026, 9, 28, 15, 20)), planOnly: true);
        var skip = ReadPlan(r);
        Assert.Equal(0, e1);
        Assert.Equal("skip_restart", skip["decision"]!.GetValue<string>());
        Assert.Equal("2026-09-28T15:20:00.0000000Z", skip["restart_utc"]!.GetValue<string>());
        Assert.Empty(skip["calls"]!.AsArray());

        File.WriteAllText(r.StatePath, "{\"schema\":1}");
        var (e2, _) = await Runs.Go(s, new NoChildAllowed(), Runs.Pin, planOnly: true);
        Assert.Equal(2, e2);
        Assert.Equal("refuse_state", ReadPlan(r)["decision"]!.GetValue<string>());
        Assert.Equal("{\"schema\":1}", File.ReadAllText(r.StatePath));
    }

    [Fact]
    public async Task Plan_only_status_values_waits_for_minute_20_nothing_due_and_skipped_and_gap_entries()
    {
        using var r = new RunRoot(); r.SeedState();
        var (_, _) = await Runs.Go(Fast(r, 10, x => x), new NoChildAllowed(), Utc(2026, 9, 28, 16, 10), planOnly: true);
        var fam = ReadPlan(r)["families"]!.AsArray();
        Assert.Equal("waits_for_minute_20", fam.First(f => f!["name"]!.GetValue<string>() == "fresh")!["status"]!.GetValue<string>());
        Assert.Equal("due", fam.First(f => f!["name"]!.GetValue<string>() == "fresh_fast")!["status"]!.GetValue<string>());

        using var r2 = new RunRoot(); r2.SeedState();
        await Runs.Go(r2.Settings(), new NoChildAllowed(), Utc(2026, 9, 28, 12, 20), planOnly: true);
        Assert.All(ReadPlan(r2)["families"]!.AsArray(), f => Assert.Equal("nothing_due", f!["status"]!.GetValue<string>()));

        using var r3 = new RunRoot(); r3.SeedState("store_pg", queryStore: false);
        await Runs.Go(r3.Settings("store_pg", x => x with { Families = x.Families with { FreshTables = ["collection_log"], QueryStoreEnabled = false, FastWindowMinutes = 10 } }), new NoChildAllowed(), Runs.Pin, planOnly: true);
        var pg = ReadPlan(r3, "store_pg")["families"]!.AsArray();
        Assert.Equal("skipped", pg.First(f => f!["name"]!.GetValue<string>() == "fresh")!["status"]!.GetValue<string>());

        using var r4 = new RunRoot(); r4.SeedState(tweak: o => o["fresh"]!["last_to"] = "2026-09-27T10:00:00.0000000Z");
        await Runs.Go(r4.Settings(), new NoChildAllowed(), Utc(2026, 9, 28, 9, 20), planOnly: true);
        var entries = ReadPlan(r4)["families"]![0]!["entries"]!.AsArray();
        Assert.Contains(entries, e => e!["gap"]!.GetValue<bool>());
        Assert.DoesNotContain(ReadPlan(r4)["calls"]!.AsArray(), c => c!["family"]!.GetValue<string>() == "fresh" && c["from"]!.GetValue<string>() == "2026-09-27T10:00:00Z");
    }

    [Fact]
    public async Task Plan_only_exempt_from_cutoff_lists_query_store_at_lag_1()
    {
        using var r = new RunRoot(); r.SeedState();
        await Runs.Go(r.Settings(tweak: x => x with { Families = x.Families with { QueryStoreLagHours = 1 } }), new NoChildAllowed(), Runs.Pin, planOnly: true);
        Assert.Equal(new[] { "fresh_fast", "query_store" }, ReadPlan(r)["exempt_from_cutoff"]!.AsArray().Select(x => x!.GetValue<string>()));
    }

    [Fact]
    public async Task Plan_only_free_mb_is_null_when_the_host_cannot_say()
    {
        using var r = new RunRoot(); r.SeedState();
        await Runs.Go(r.Settings(), new NoChildAllowed(), Runs.Pin, planOnly: true, memory: new FakeHostMemory((int?)null));
        Assert.Null(ReadPlan(r)["free_mb"]);
    }
}
