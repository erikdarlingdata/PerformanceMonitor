using System.Globalization;
using System.Text;
using System.Text.Json.Nodes;
using PerformanceMonitor.Exporter.Run;
using Xunit;
using static PerformanceMonitor.Exporter.Tests.Run.RunRoot;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>R7-R9, R11, R12, R14-R17, R20, R24-R26 end to end with a fake child. State, call objects and exit codes are asserted.</summary>
public class RunSchedulerTests
{
    private static readonly DateTime Pin = Runs.Pin;   // 2026-09-28T16:30Z

    private static RunSettings Fast(RunRoot r, int n = 10, string store = "testh", Func<RunSettings, RunSettings>? more = null) =>
        r.Settings(store, s => (more is null ? s : more(s)) with { Families = (more is null ? s : more(s)).Families with { FastWindowMinutes = n } });

    private static DateTime Parse(string s) => DateTime.Parse(s, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);

    private static List<ChildCall> ByFamily(FakeChild c, string family) => c.Calls.Where(x => FakeChild.Family(x) == family).ToList();

    // ---- R7: cutoff minute ----

    [Fact]
    public async Task R7_no_hourly_window_starts_once_the_clock_minute_is_at_or_past_the_cutoff()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Schedule = x.Schedule with { CutoffMinute = 40 } });
        var child = new FakeChild();
        var (exit, output) = await Runs.Go(s, child, Utc(2026, 9, 28, 16, 45));
        Assert.Equal(0, exit);
        Assert.Contains("past minute 40; not starting any more windows for fresh this run", output);
        Assert.Empty(child.Calls);
    }

    [Theory]
    [InlineData(39, 59, true)]
    [InlineData(40, 0, false)]
    [InlineData(41, 0, false)]
    public async Task R7_minute_40_is_the_first_minute_with_no_new_hourly_window(int minute, int second, bool starts)
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Schedule = x.Schedule with { CutoffMinute = 40 } });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 16, minute, second));
        Assert.Equal(starts, child.Calls.Count > 0);
    }

    [Fact]
    public async Task R7_the_hourly_windows_run_at_16_20_and_the_cutoff_is_read_again_before_each_window()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Schedule = x.Schedule with { CutoffMinute = 40 } });
        var child = new FakeChild();
        var clock = Utc(2026, 9, 28, 16, 20);
        // The first window "takes" 25 minutes: the clock passes :40 before the second window, so it does not start.
        child.OnCall = _ => clock = clock.AddMinutes(25);
        var output = new StringWriter { NewLine = "\n" };
        var sched = new RunScheduler(s, new RunOptions(false, Utc(2026, 9, 28, 16, 20)), () => clock, child, new NoRestartDetector(), new FakeHostMemory(32768), output);
        await sched.RunAsync(CancellationToken.None);
        Assert.Single(ByFamily(child, "fresh"));
        Assert.Contains("past minute 40; not starting any more windows for fresh this run", output.ToString());
    }

    [Fact]
    public async Task R7_the_cutoff_follows_now_not_the_real_clock()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Schedule = x.Schedule with { CutoffMinute = 40 } });
        // --now says 16:45 while the "real" clock is 03:05: the cutoff must still trip (the 2026-10-22 CI bug).
        var (_, output) = await Runs.Go(s, new FakeChild(), Utc(2026, 9, 28, 16, 45), clockNow: Utc(2026, 9, 28, 3, 5));
        Assert.Contains("past minute 40", output);
    }

    [Fact]
    public async Task R7_fresh_fast_and_query_store_at_lag_1_are_exempt_from_the_cutoff()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => { o["fresh"]!["last_to"] = "2026-09-28T16:00:00.0000000Z"; o["query_store"]!["last_to"] = "2026-09-28T15:00:00.0000000Z"; });
        var s = Fast(r, 15, more: x => x with
        {
            Schedule = x.Schedule with { CutoffMinute = 40 },
            Families = x.Families with { QueryStoreLagHours = 1 },
        });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 16, 50));
        Assert.NotEmpty(ByFamily(child, "fresh_fast"));
        var qs = Assert.Single(ByFamily(child, "query_store"));
        Assert.Equal("2026-09-28T16:00:00Z", FakeChild.Arg(qs, "--to"));
        Assert.Empty(ByFamily(child, "fresh"));
    }

    // ---- R8: hourly families wait for minute 20 when fast is on ----

    [Fact]
    public async Task R8_at_16_10_with_fast_10_only_fast_calls_run()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();
        var (_, output) = await Runs.Go(Fast(r, 10), child, Utc(2026, 9, 28, 16, 10));
        Assert.NotEmpty(child.Calls);
        Assert.All(child.Calls, c => Assert.Equal("fresh_fast", FakeChild.Family(c)));
        Assert.Contains("hourly family fresh waits for minute 20", output);
        Assert.Contains("hourly family query_store waits for minute 20", output);
    }

    [Fact]
    public async Task R8_query_store_at_lag_1_is_exempt_from_the_minute_20_rule()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => o["query_store"]!["last_to"] = "2026-09-28T15:00:00.0000000Z");
        var s = Fast(r, 15, more: x => x with { Families = x.Families with { QueryStoreLagHours = 1 } });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 16, 50));
        Assert.Single(ByFamily(child, "query_store"));
    }

    [Fact]
    public async Task R8_lag_2_on_a_15_minute_box_waits_for_minute_20()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => o["query_store"]!["last_to"] = "2026-09-28T14:00:00.0000000Z");
        var s = Fast(r, 15, more: x => x with { Families = x.Families with { QueryStoreLagHours = 2 } });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 17, 5));
        Assert.Empty(ByFamily(child, "query_store"));
    }

    // ---- R9: order, and a failure stops only its own family ----

    [Fact]
    public async Task R9_order_is_fresh_fast_then_fresh_then_query_store()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();
        await Runs.Go(Fast(r, 10), child, Pin);
        var families = child.Calls.Select(FakeChild.Family).Distinct().ToArray();
        Assert.Equal(new[] { "fresh_fast", "fresh", "query_store" }, families);
    }

    [Fact]
    public async Task R9_a_failing_fast_call_holds_its_last_to_records_the_error_and_the_hourly_fresh_family_still_runs()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild { ExitFor = c => FakeChild.Has(c, "--dimension-origin") ? 0 : 1, StderrFor = c => FakeChild.Has(c, "--dimension-origin") ? null : "boom" };
        var (exit, _) = await Runs.Go(Fast(r, 10), child, Pin);
        var st = r.ReadState();
        Assert.Equal(1, exit);
        Assert.Equal("2026-09-28T12:00:00.0000000Z", st["fresh_fast"]!["last_to"]!.GetValue<string>());
        Assert.Contains("boom", st["fresh_fast"]!["last_error"]!.GetValue<string>());
        Assert.True(Parse(st["fresh"]!["last_to"]!.GetValue<string>()) > Utc(2026, 9, 28, 12, 0));
        Assert.Single(ByFamily(child, "fresh_fast"));   // stopped after the first failure
    }

    [Fact]
    public async Task R9_a_failing_fresh_family_does_not_block_query_store()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild { ExitFor = c => FakeChild.Arg(c, "--dimension-origin") == "query_stats" ? 1 : 0, StderrFor = c => "boom" };
        var (exit, _) = await Runs.Go(r.Settings(), child, Pin);
        var st = r.ReadState();
        Assert.Equal(1, exit);
        Assert.Equal("2026-09-28T12:00:00.0000000Z", st["fresh"]!["last_to"]!.GetValue<string>());
        Assert.Contains("boom", st["fresh"]!["last_error"]!.GetValue<string>());
        Assert.True(Parse(st["query_store"]!["last_to"]!.GetValue<string>()) > Utc(2026, 9, 28, 12, 0));
    }

    // ---- R10 / R20: the call objects equal the expected argument lists ----

    private static string Line(ChildCall c) => string.Join(' ', c.Args);

    [Fact]
    public async Task R20_default_config_calls_equal_the_expected_argument_line()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();
        await Runs.Go(r.Settings(), child, Pin);
        var tables = string.Join(',', Planner.DefaultFreshTables);
        var head = $"export --store testh --from 2026-09-28T12:00:00Z --to 2026-09-28T13:00:00Z --out {r.Out} --statement-timeout-seconds 120 --duckdb-memory-limit 4GB --upload s3://example-bucket/landing --tables {tables} --dimension-origin query_stats";
        Assert.Equal(head, Line(child.Calls[0]));
        Assert.Equal(4, ByFamily(child, "fresh").Count);   // [12,13) .. [15,16): the 16:30 run reaches the window due at 16:15
        var qs = ByFamily(child, "query_store");
        Assert.Equal(
            $"export --store testh --from 2026-09-28T12:00:00Z --to 2026-09-28T13:00:00Z --out {r.Out} --statement-timeout-seconds 120 --duckdb-memory-limit 4GB --upload s3://example-bucket/landing --tables query_store_stats,plan_shape,query_text --dimension-origin query_store",
            Line(qs[0]));
        Assert.All(child.Calls, c => Assert.DoesNotContain(c.Args, string.IsNullOrEmpty));
        Assert.All(child.Calls, c => Assert.Equal("child-exe", c.Exe));
    }

    [Fact]
    public async Task R20_fast_10_calls_carry_the_sub_hour_tables_and_the_fresh_call_keeps_the_hourly_tables_and_also()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = Fast(r, 10, more: x => x with { Families = x.Families with { OptionalTables = ["plan_correction"] } });
        var child = new FakeChild();
        await Runs.Go(s, child, Pin);
        var fast = ByFamily(child, "fresh_fast");
        var hourly = ByFamily(child, "fresh");
        Assert.Equal(5, fast.Count);
        Assert.Equal(("2026-09-28T12:00:00Z", "2026-09-28T13:00:00Z"), (FakeChild.Arg(fast[0], "--from"), FakeChild.Arg(fast[0], "--to")));
        Assert.Equal(("2026-09-28T16:00:00Z", "2026-09-28T16:10:00Z"), (FakeChild.Arg(fast[4], "--from"), FakeChild.Arg(fast[4], "--to")));
        Assert.All(fast, c => { Assert.False(FakeChild.Has(c, "--also")); Assert.False(FakeChild.Has(c, "--dimension-origin")); });
        Assert.Single(fast.Select(c => FakeChild.Arg(c, "--tables")).Distinct());
        Assert.Equal(string.Join(',', Planner.DefaultFreshTables.Where(t => SubHourTables.All.Contains(t))), FakeChild.Arg(fast[0], "--tables"));
        Assert.NotEmpty(hourly);
        Assert.All(hourly, c =>
        {
            Assert.Equal("query_stats_hourly,procedure_stats_hourly,plan_shape,query_text,procedure_plan", FakeChild.Arg(c, "--tables"));
            Assert.Equal("plan_correction", FakeChild.Arg(c, "--also"));
        });
    }

    [Fact]
    public async Task R20_postgres_shaped_config_has_only_collection_log_and_also_and_no_query_store_call()
    {
        using var r = new RunRoot(); r.SeedState("store_pg", queryStore: false);
        var s = r.Settings("store_pg", x => x with { Families = x.Families with { FreshTables = ["collection_log"], QueryStoreEnabled = false, OptionalTables = ["pg_stat_statements", "pg_stat_activity"] } });
        var child = new FakeChild();
        var (exit, _) = await Runs.Go(s, child, Pin);
        Assert.Equal(0, exit);
        Assert.NotEmpty(child.Calls);
        Assert.All(child.Calls, c =>
        {
            Assert.Equal("collection_log", FakeChild.Arg(c, "--tables"));
            Assert.Equal("pg_stat_statements,pg_stat_activity", FakeChild.Arg(c, "--also"));
            Assert.NotEqual("query_store", FakeChild.Arg(c, "--dimension-origin"));
        });
        Assert.False(r.ReadState().ContainsKey("query_store"));
    }

    [Fact]
    public async Task R20_postgres_with_fast_10_and_no_also_makes_only_fast_calls_on_collection_log()
    {
        using var r = new RunRoot(); r.SeedState("store_pg", queryStore: false);
        var s = Fast(r, 10, "store_pg", x => x with { Families = x.Families with { FreshTables = ["collection_log"], QueryStoreEnabled = false } });
        var child = new FakeChild();
        await Runs.Go(s, child, Pin);
        Assert.NotEmpty(child.Calls);
        Assert.All(child.Calls, c => { Assert.Equal("collection_log", FakeChild.Arg(c, "--tables")); Assert.False(FakeChild.Has(c, "--also")); Assert.False(FakeChild.Has(c, "--dimension-origin")); });
    }

    [Fact]
    public async Task R20_postgres_with_fast_10_and_also_still_names_its_tables_on_every_call()
    {
        using var r = new RunRoot(); r.SeedState("store_pg", queryStore: false);
        var s = Fast(r, 10, "store_pg", x => x with { Families = x.Families with { FreshTables = ["collection_log"], QueryStoreEnabled = false, OptionalTables = ["pg_extra"] } });
        var child = new FakeChild();
        await Runs.Go(s, child, Pin);
        Assert.All(child.Calls, c => Assert.True(FakeChild.Has(c, "--tables")));
        Assert.Contains(child.Calls, c => FakeChild.Arg(c, "--also") == "pg_extra");
    }

    [Fact]
    public async Task R20_login_plan_reuse_source_state_dir_region_and_threads_flags_and_no_empty_elements()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with
        {
            PgUser = "export_user", PgCredentialFile = "C:\\cred.dpapi", ProcedurePlanReuse = "on", QueryStoreSource = "wide",
            ExportStateDir = "C:\\exporter\\state", AwsRegion = "us-east-1", DuckdbThreads = 3,
        });
        var child = new FakeChild();
        await Runs.Go(s, child, Pin);
        foreach (var c in child.Calls)
        {
            var l = Line(c);
            Assert.Contains("--pg-user export_user --pg-credential-file C:\\cred.dpapi --procedure-plan-reuse on --query-store-source wide", l);
            Assert.Contains("--upload s3://example-bucket/landing --aws-region us-east-1 --state-dir C:\\exporter\\state --tables ", l);
            Assert.Contains("--duckdb-threads 3", l);
            Assert.DoesNotContain(c.Args, string.IsNullOrEmpty);
        }
    }

    [Fact]
    public async Task R20_defaults_pass_no_state_dir_and_no_region_so_the_boxes_defaults_apply()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();
        await Runs.Go(r.Settings(), child, Pin);
        Assert.All(child.Calls, c => { Assert.False(FakeChild.Has(c, "--state-dir")); Assert.False(FakeChild.Has(c, "--aws-region")); Assert.False(FakeChild.Has(c, "--pg-user")); });
    }

    // ---- R5 / R6 flags on the calls ----

    [Fact]
    public async Task R5_margin_5_puts_fresh_guard_minutes_on_every_fresh_fast_call_and_no_other_family()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = Fast(r, 10, more: x => x with { Families = x.Families with { LateRowMarginMinutes = 5 } });
        var child = new FakeChild();
        var (_, output) = await Runs.Go(s, child, Utc(2026, 9, 28, 16, 20));
        var fast = ByFamily(child, "fresh_fast");
        Assert.NotEmpty(fast);
        Assert.All(fast, c => Assert.Equal("5", FakeChild.Arg(c, "--fresh-guard-minutes")));
        Assert.All(child.Calls.Except(fast), c => Assert.False(FakeChild.Has(c, "--fresh-guard-minutes")));
        Assert.Contains("late-row margin for fresh_fast: 5 minutes (config)", output);
    }

    [Theory]
    [InlineData(null)] [InlineData(15)]
    public async Task R5_margin_15_or_missing_puts_the_flag_on_no_call(int? margin)
    {
        using var r = new RunRoot(); r.SeedState();
        var s = Fast(r, 10, more: x => x with { Families = x.Families with { LateRowMarginMinutes = margin } });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 16, 20));
        Assert.NotEmpty(child.Calls);
        Assert.All(child.Calls, c => Assert.False(FakeChild.Has(c, "--fresh-guard-minutes")));
    }

    [Fact]
    public async Task R6_lag_2_flag_on_query_store_calls_only_and_the_log_line_says_config()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Families = x.Families with { QueryStoreLagHours = 2 } });
        var child = new FakeChild();
        var (_, output) = await Runs.Go(s, child, Utc(2026, 9, 28, 16, 20));
        var qs = ByFamily(child, "query_store");
        Assert.Equal(3, qs.Count);
        Assert.Equal("2026-09-28T15:00:00Z", FakeChild.Arg(qs[^1], "--to"));
        Assert.All(qs, c => Assert.Equal("2", FakeChild.Arg(c, "--query-store-lag-hours")));
        Assert.All(child.Calls.Except(qs), c => Assert.False(FakeChild.Has(c, "--query-store-lag-hours")));
        Assert.Contains("query_store lag: 2 hours (config)", output);
    }

    [Fact]
    public async Task R6_no_lag_setting_is_lag_3_default_and_no_flag()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();
        var (_, output) = await Runs.Go(r.Settings(), child, Utc(2026, 9, 28, 16, 20));
        var qs = ByFamily(child, "query_store");
        Assert.Equal(2, qs.Count);
        Assert.Equal("2026-09-28T14:00:00Z", FakeChild.Arg(qs[^1], "--to"));
        Assert.All(child.Calls, c => Assert.False(FakeChild.Has(c, "--query-store-lag-hours")));
        Assert.Contains("query_store lag: 3 hours (default)", output);
    }

    [Fact]
    public async Task R6_lag_1_at_16_50_on_a_15_minute_box_reaches_15_to_16_with_the_flag()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => o["query_store"]!["last_to"] = "2026-09-28T15:00:00.0000000Z");
        var s = Fast(r, 15, more: x => x with { Families = x.Families with { QueryStoreLagHours = 1 } });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 16, 50));
        var qs = Assert.Single(ByFamily(child, "query_store"));
        Assert.Equal(("2026-09-28T15:00:00Z", "2026-09-28T16:00:00Z", "1"), (FakeChild.Arg(qs, "--from"), FakeChild.Arg(qs, "--to"), FakeChild.Arg(qs, "--query-store-lag-hours")));
    }

    [Fact]
    public async Task R6_lag_1_on_a_15_minute_box_at_16_35_makes_no_query_store_call()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => o["query_store"]!["last_to"] = "2026-09-28T15:00:00.0000000Z");
        var s = Fast(r, 15, more: x => x with { Families = x.Families with { QueryStoreLagHours = 1 } });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 16, 35));
        Assert.Empty(ByFamily(child, "query_store"));
    }

    [Fact]
    public async Task R6_lag_1_at_17_05_after_a_missed_50_run_on_a_15_minute_box_still_reaches_15_to_16()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => o["query_store"]!["last_to"] = "2026-09-28T15:00:00.0000000Z");
        var s = Fast(r, 15, more: x => x with { Families = x.Families with { QueryStoreLagHours = 1 } });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 17, 5));
        var qs = Assert.Single(ByFamily(child, "query_store"));
        Assert.Equal("2026-09-28T16:00:00Z", FakeChild.Arg(qs, "--to"));
    }

    [Theory]
    [InlineData("off")] [InlineData("shadow")] [InlineData("on")]
    public async Task R20_procedure_plan_reuse_value_rides_on_every_child_call_and_never_on_the_hook(string value)
    {
        using var r = new RunRoot(); r.SeedState();
        var hook = new PostRunSettings { Name = "alert-check", Command = [TestChildPath.Exe, "--tc-print", "x"] };
        var child = new FakeChild();
        var (exit, _) = await Runs.Go(r.Settings(tweak: x => x with { ProcedurePlanReuse = value, PostRun = hook }), child, Pin);
        Assert.Equal(0, exit);
        Assert.NotEmpty(child.Calls);
        Assert.All(child.Calls, c => Assert.Equal(value, FakeChild.Arg(c, "--procedure-plan-reuse")));
        Assert.All(child.Calls, c => Assert.False(FakeChild.Has(c, "--query-store-source")));
    }

    [Theory]
    [InlineData("raw")] [InlineData("wide")] [InlineData("compare")]
    public async Task R20_query_store_source_value_rides_on_every_child_call(string value)
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();
        var (exit, _) = await Runs.Go(r.Settings(tweak: x => x with { QueryStoreSource = value }), child, Pin);
        Assert.Equal(0, exit);
        Assert.NotEmpty(child.Calls);
        Assert.All(child.Calls, c => Assert.Equal(value, FakeChild.Arg(c, "--query-store-source")));
        Assert.All(child.Calls, c => Assert.False(FakeChild.Has(c, "--procedure-plan-reuse")));
    }

    [Theory]
    [InlineData("maybe", null)] [InlineData("ON", null)] [InlineData("Shadow", null)] [InlineData("on please", null)]
    [InlineData(null, "maybe")] [InlineData(null, "Wide")] [InlineData(null, "raw wide")]
    public async Task R26_a_procedure_plan_reuse_or_query_store_source_value_the_exporter_would_reject_is_refused_with_exit_2(string? reuse, string? source)
    {
        // Settings are data: a bad value is a refusal, not a child that fails every run.
        using var r = new RunRoot(); r.SeedState();
        var before = File.ReadAllBytes(r.StatePath);
        var (exit, output) = await Runs.Go(r.Settings(tweak: x => x with { ProcedurePlanReuse = reuse, QueryStoreSource = source }), new NoChildAllowed(), Pin);
        Assert.Equal(2, exit);
        Assert.Contains("refused:", output);
        Assert.Equal(before, File.ReadAllBytes(r.StatePath));
    }

    // ---- R11: store overrides ----

    [Fact]
    public async Task R11_query_store_disabled_writes_no_query_store_key_even_when_the_file_never_had_one()
    {
        using var r = new RunRoot(); r.SeedState("store_pg", queryStore: false);
        var s = r.Settings("store_pg", x => x with { Families = x.Families with { QueryStoreEnabled = false } });
        await Runs.Go(s, new FakeChild(), Pin);
        Assert.False(r.ReadState().ContainsKey("query_store"));
    }

    [Fact]
    public async Task R11_cold_start_with_a_start_instant_seeds_schema_2_without_query_store_for_a_store_that_has_none()
    {
        using var r = new RunRoot();   // no state file at all: cold start (store_pg cold-start seed in the Postgres-store schedule test)
        var s = r.Settings("store_pg", x => x with
        {
            Families = x.Families with { FreshTables = ["collection_log"], QueryStoreEnabled = false },
            Schedule = x.Schedule with { StartUtc = Utc(2026, 9, 28, 12, 0) },
        });
        var child = new FakeChild();
        var (exit, _) = await Runs.Go(s, child, Pin);
        var st = r.ReadState();
        Assert.Equal(0, exit);
        Assert.Equal(2, st["schema"]!.GetValue<int>());
        Assert.Equal("store_pg", st["store"]!.GetValue<string>());
        Assert.False(st.ContainsKey("query_store"));
        Assert.NotNull(st["cutover_utc"]);
        Assert.True(Parse(st["fresh"]!["last_to"]!.GetValue<string>()) > Utc(2026, 9, 28, 12, 0));
        Assert.NotEmpty(child.Calls);
    }

    [Fact]
    public async Task R3_cold_start_seeds_fresh_and_query_store_last_to_at_start_utc_and_cutover_at_the_real_clock()
    {
        // the Postgres-store schedule test "-ColdStart": fresh.last_to = the start instant, cutover_utc = the seed's own clock, schema 2, store name.
        using var r = new RunRoot();
        var s = r.Settings(tweak: x => x with { Schedule = x.Schedule with { StartUtc = Utc(2026, 9, 28, 12, 0) } });
        var child = new FakeChild();
        var real = Utc(2026, 9, 28, 12, 30, 7);
        var (exit, _) = await Runs.Go(s, child, Utc(2026, 9, 28, 12, 30), clockNow: real);   // nothing is due until 13:15
        var st = r.ReadState();
        Assert.Equal(0, exit);
        Assert.Empty(child.Calls);
        Assert.Equal(("2026-09-28T12:00:00.0000000Z", "2026-09-28T12:00:00.0000000Z"), (st["fresh"]!["last_to"]!.GetValue<string>(), st["query_store"]!["last_to"]!.GetValue<string>()));
        Assert.Equal("2026-09-28T12:30:07.0000000Z", st["cutover_utc"]!.GetValue<string>());
        Assert.Equal((2, "testh"), (st["schema"]!.GetValue<int>(), st["store"]!.GetValue<string>()));
    }

    [Fact]
    public async Task R3_an_existing_state_file_wins_over_start_utc()
    {
        using var r = new RunRoot(); r.SeedState();   // both families at 12:00
        var s = r.Settings(tweak: x => x with { Schedule = x.Schedule with { StartUtc = Utc(2026, 9, 28, 15, 0) } });
        var child = new FakeChild();
        await Runs.Go(s, child, Utc(2026, 9, 28, 13, 20));
        Assert.Equal("2026-09-28T12:00:00Z", FakeChild.Arg(child.Calls[0], "--from"));
    }

    [Fact]
    public async Task R3_a_start_utc_that_is_not_a_whole_utc_hour_is_refused_with_exit_2_and_writes_nothing()
    {
        // the Postgres-store schedule test "refuses a -LastTo not on a whole UTC hour" and "refused attempt writes nothing".
        using var r = new RunRoot();
        foreach (var bad in new[] { Utc(2026, 9, 28, 12, 30), Utc(2026, 9, 28, 12, 0, 1), Utc(2026, 9, 28, 12, 0).AddMilliseconds(5) })
        {
            var s = r.Settings(tweak: x => x with { Schedule = x.Schedule with { StartUtc = bad } });
            var (exit, output) = await Runs.Go(s, new NoChildAllowed(), Utc(2026, 9, 28, 13, 20));
            Assert.Equal(2, exit);
            Assert.Contains("schedule.start_utc must be a whole UTC hour", output);
            Assert.False(File.Exists(r.StatePath));
            Assert.False(File.Exists(RunLock.PathFor(r.State)));
        }
    }

    [Fact]
    public async Task R3_cold_start_without_a_start_instant_begins_at_the_catch_up_horizon()
    {
        using var r = new RunRoot();
        var child = new FakeChild();
        await Runs.Go(r.Settings(), child, Utc(2026, 9, 28, 9, 20));
        var first = child.Calls.First(c => FakeChild.Arg(c, "--dimension-origin") == "query_stats");
        Assert.Equal("2026-09-27T13:00:00Z", FakeChild.Arg(first, "--from"));   // newest due end 09:00 minus 19 h = 14:00, so the first window is [13:00,14:00)
    }

    // ---- R12: fresh_fast state ----

    [Fact]
    public async Task R12_first_use_seeds_fresh_fast_from_fresh_last_to_with_the_same_shape_as_the_other_families()
    {
        using var r = new RunRoot(); r.SeedState();
        await Runs.Go(Fast(r, 10), new FakeChild(), Pin);
        var st = r.ReadState();
        Assert.Equal("2026-09-28T16:10:00.0000000Z", st["fresh_fast"]!["last_to"]!.GetValue<string>());
        Assert.Equal("2026-09-28T16:00:00.0000000Z", st["fresh"]!["last_to"]!.GetValue<string>());
        Assert.Equal("last_error,last_failure_utc,last_run,last_success_utc,last_to", string.Join(',', st["fresh_fast"]!.AsObject().Select(p => p.Key).OrderBy(k => k, StringComparer.Ordinal)));
    }

    [Fact]
    public async Task R12_an_existing_fresh_fast_last_to_is_the_resume_point_and_each_next_run_makes_one_call()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => o["fresh_fast"] = new JsonObject { ["last_to"] = "2026-09-28T16:00:00.0000000Z", ["last_success_utc"] = null, ["last_failure_utc"] = null, ["last_error"] = null });
        var child = new FakeChild();
        await Runs.Go(Fast(r, 10), child, Pin);
        var fast = Assert.Single(ByFamily(child, "fresh_fast"));
        Assert.Equal(("2026-09-28T16:00:00Z", "2026-09-28T16:10:00Z"), (FakeChild.Arg(fast, "--from"), FakeChild.Arg(fast, "--to")));
        // Ten minutes later: exactly one new 10-minute call.
        var child2 = new FakeChild();
        await Runs.Go(Fast(r, 10), child2, Utc(2026, 9, 28, 16, 40));
        var next = Assert.Single(child2.Calls, c => FakeChild.Family(c) == "fresh_fast");
        Assert.Equal(("2026-09-28T16:10:00Z", "2026-09-28T16:20:00Z"), (FakeChild.Arg(next, "--from"), FakeChild.Arg(next, "--to")));
    }

    [Fact]
    public async Task R12_back_at_60_the_fresh_fast_state_is_dropped_and_fresh_re_exports_every_table_from_its_own_last_to()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => o["fresh_fast"] = new JsonObject { ["last_to"] = "2026-09-28T15:00:00.0000000Z", ["last_success_utc"] = null, ["last_failure_utc"] = null, ["last_error"] = null });
        var child = new FakeChild();
        var (_, output) = await Runs.Go(r.Settings(), child, Pin);
        Assert.False(r.ReadState().ContainsKey("fresh_fast"));
        Assert.Contains("fresh_fast state dropped", output);
        Assert.Equal(("2026-09-28T12:00:00Z", "2026-09-28T13:00:00Z"), (FakeChild.Arg(child.Calls[0], "--from"), FakeChild.Arg(child.Calls[0], "--to")));
        Assert.StartsWith("perfmon_stats", FakeChild.Arg(child.Calls[0], "--tables"));
    }

    [Fact]
    public async Task R12_default_and_explicit_60_make_identical_calls_and_state_keys_with_no_fresh_fast()
    {
        using var a = new RunRoot(); a.SeedState();
        using var b = new RunRoot(); b.SeedState();
        var ca = new FakeChild(); var cb = new FakeChild();
        await Runs.Go(a.Settings(), ca, Pin);
        await Runs.Go(b.Settings(tweak: x => x with { Families = x.Families with { FastWindowMinutes = 60 } }), cb, Pin);
        Assert.Equal(ca.Calls.Select(c => Line(c).Replace(a.Out, "OUT")), cb.Calls.Select(c => Line(c).Replace(b.Out, "OUT")));
        Assert.NotEmpty(ca.Calls);
        Assert.All(ca.Calls, c => Assert.True(FakeChild.Has(c, "--dimension-origin")));
        Assert.DoesNotContain("fresh_fast", a.ReadState().Select(p => p.Key));
        Assert.Equal(a.ReadState().Select(p => p.Key), b.ReadState().Select(p => p.Key));
    }

    // ---- R13: optional tables ----

    [Fact]
    public async Task R13_the_optional_file_is_read_every_run_and_only_the_fresh_call_carries_also_and_the_log_line_names_it()
    {
        using var r = new RunRoot(); r.SeedState();
        var file = Path.Combine(r.Root, "optional.txt");
        File.WriteAllText(file, "# header\nplan_correction\n\n  job_history # why\n");
        var s = r.Settings(tweak: x => x with { Families = x.Families with { OptionalTablesFile = file } });
        var child = new FakeChild();
        var (_, output) = await Runs.Go(s, child, Pin);
        Assert.Contains("optional tables: plan_correction,job_history", output);
        Assert.All(ByFamily(child, "fresh"), c => Assert.Equal("plan_correction,job_history", FakeChild.Arg(c, "--also")));
        Assert.All(ByFamily(child, "query_store"), c => Assert.False(FakeChild.Has(c, "--also")));
        // An edit takes effect at the next run.
        File.WriteAllText(file, "other_table\n");
        var child2 = new FakeChild();
        r.SeedState();
        await Runs.Go(s, child2, Pin);
        Assert.All(ByFamily(child2, "fresh"), c => Assert.Equal("other_table", FakeChild.Arg(c, "--also")));
    }

    [Fact]
    public async Task R13_no_file_logs_none_and_no_call_carries_also()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();
        var (_, output) = await Runs.Go(r.Settings(), child, Pin);
        Assert.Contains("optional tables: none", output);
        Assert.All(child.Calls, c => Assert.False(FakeChild.Has(c, "--also")));
    }

    // ---- R14: exit 3 ----

    private static void WriteOptionalManifest(RunRoot r, ChildCall c, string json)
    {
        var also = FakeChild.Arg(c, "--also").Split(',');
        var from = DateTime.Parse(FakeChild.Arg(c, "--from"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var to = DateTime.Parse(FakeChild.Arg(c, "--to"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var path = OutputPaths.ManifestFilePath(r.Out, "testh", from, to, also, dimensionOrigin: null);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, json);
    }

    [Fact]
    public async Task R14_exit_3_advances_last_to_and_records_failed_optional_from_the_optional_manifest()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Families = x.Families with { OptionalTables = ["plan_correction", "job_history"] } });
        var child = new FakeChild { ExitFor = c => FakeChild.Arg(c, "--dimension-origin") == "query_stats" ? 3 : 0 };
        child.OnCall = c =>
        {
            if (FakeChild.Arg(c, "--dimension-origin") != "query_stats") return;
            WriteOptionalManifest(r, c, """{"failed_optional":[{"table":"plan_correction","error":"store timeout"},{"table":"job_history","error":"connection reset"}]}""");
            // The core manifest (a different file name) must not be what is read.
            var core = OutputPaths.ManifestFilePath(r.Out, "testh", Utc(2026, 9, 28, 12, 0), Utc(2026, 9, 28, 13, 0), Planner.DefaultFreshTables, "query_stats");
            Directory.CreateDirectory(Path.GetDirectoryName(core)!);
            File.WriteAllText(core, """{"failed_optional":[{"table":"decoy","error":"core manifest"}]}""");
        };
        var (exit, output) = await Runs.Go(s, child, Utc(2026, 9, 28, 13, 20));
        var st = r.ReadState();
        Assert.Equal(0, exit);
        Assert.Equal("2026-09-28T13:00:00.0000000Z", st["fresh"]!["last_to"]!.GetValue<string>());
        Assert.Null(st["fresh"]!["last_error"]);
        var of = st["optional_failures"]!.AsArray();
        Assert.Equal(new[] { "plan_correction", "job_history" }, of.Select(e => e!["table"]!.GetValue<string>()));
        Assert.Equal(new[] { "store timeout", "connection reset" }, of.Select(e => e!["error"]!.GetValue<string>()));
        Assert.All(of, e => { Assert.Equal("2026-09-28T12:00:00Z", e!["window_from"]!.GetValue<string>()); Assert.Equal("2026-09-28T13:00:00Z", e["window_to"]!.GetValue<string>()); Assert.NotNull(e["recorded_utc"]); });
        Assert.Contains("optional table(s) failed for fresh", output);
    }

    [Fact]
    public async Task R14_exit_3_with_no_readable_optional_manifest_records_one_unknown_entry()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Families = x.Families with { OptionalTables = ["plan_correction"] } });
        var child = new FakeChild { ExitFor = c => FakeChild.Arg(c, "--dimension-origin") == "query_stats" ? 3 : 0 };
        await Runs.Go(s, child, Utc(2026, 9, 28, 13, 20));
        var e = Assert.Single(r.ReadState()["optional_failures"]!.AsArray())!;
        Assert.Equal(("unknown", "exit 3; optional manifest not readable"), (e["table"]!.GetValue<string>(), e["error"]!.GetValue<string>()));
    }

    [Fact]
    public async Task R14_optional_failures_keeps_the_newest_200()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o =>
        {
            var arr = new JsonArray();
            for (var i = 0; i < 205; i++) arr.Add(new JsonObject { ["table"] = "seed", ["error"] = "old", ["window_from"] = "x", ["window_to"] = "y", ["recorded_utc"] = "z" });
            o["optional_failures"] = arr;
        });
        var s = r.Settings(tweak: x => x with { Families = x.Families with { OptionalTables = ["plan_correction"] } });
        var child = new FakeChild { ExitFor = c => FakeChild.Arg(c, "--dimension-origin") == "query_stats" ? 3 : 0 };
        child.OnCall = c => { if (FakeChild.Arg(c, "--dimension-origin") == "query_stats") WriteOptionalManifest(r, c, """{"failed_optional":[{"table":"t","error":"newest failure"}]}"""); };
        await Runs.Go(s, child, Utc(2026, 9, 28, 13, 20));
        var of = r.ReadState()["optional_failures"]!.AsArray();
        Assert.Equal(200, of.Count);
        Assert.Equal("newest failure", of[^1]!["error"]!.GetValue<string>());
        Assert.Equal("old", of[0]!["error"]!.GetValue<string>());
    }

    [Fact]
    public async Task R14_exit_1_does_not_advance_and_an_aborted_child_is_exit_9_a_failure_not_exit_3()
    {
        using var r = new RunRoot(); r.SeedState(tweak: o => o["optional_failures"] = new JsonArray(new JsonObject { ["table"] = "seed" }));
        var child = new FakeChild
        {
            Override = c => FakeChild.Arg(c, "--dimension-origin") == "query_stats"
                ? new ChildResult { ExitCode = 9, Aborted = true, AbortFreeMb = 100, FloorMb = 1000 } : null,
        };
        var (exit, _) = await Runs.Go(r.Settings(), child, Pin);
        var st = r.ReadState();
        Assert.Equal(1, exit);
        Assert.Equal("2026-09-28T12:00:00.0000000Z", st["fresh"]!["last_to"]!.GetValue<string>());
        Assert.NotNull(st["fresh"]!["last_failure_utc"]);
        var err = st["fresh"]!["last_error"]!.GetValue<string>();
        Assert.Contains("memory guard", err);
        Assert.Contains("100 MB", err);
        Assert.Contains("1000 MB", err);
        Assert.Equal(9, st["fresh"]!["last_run"]!["exit_code"]!.GetValue<int>());
        Assert.Single(st["optional_failures"]!.AsArray());
    }

    // ---- R15: state values ----

    [Fact]
    public async Task R15_success_writes_last_run_family_last_run_exit_code_and_clears_last_error_and_unknown_keys_survive()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => { o["store_login_error"] = "keep me"; o["fresh"]!["last_error"] = "old error"; o["extra_unknown"] = new JsonObject { ["a"] = 1 }; });
        var (exit, _) = await Runs.Go(r.Settings(), new FakeChild(), Pin);
        var st = r.ReadState();
        Assert.Equal(0, exit);
        Assert.Equal("2026-09-01", st["last_exported_day"]!.GetValue<string>());
        Assert.Equal("2026-09-28T12:00:00.0000000Z", st["cutover_utc"]!.GetValue<string>());
        Assert.Equal("keep me", st["store_login_error"]!.GetValue<string>());
        Assert.Equal(1, st["extra_unknown"]!["a"]!.GetValue<int>());
        Assert.Equal(2, st["schema"]!.GetValue<int>());
        Assert.Null(st["fresh"]!["last_error"]);
        Assert.NotNull(st["fresh"]!["last_success_utc"]);
        Assert.Equal(0, st["last_exit_code"]!.GetValue<int>());
        var top = st["last_run"]!.AsObject();
        Assert.Equal("peak_private_mb,min_free_mb,elapsed_seconds,log_path,family", string.Join(',', top.Select(p => p.Key)));
        Assert.Equal("query_store", top["family"]!.GetValue<string>());
        Assert.EndsWith("export-testh-20260928.log", top["log_path"]!.GetValue<string>());
        var fam = st["fresh"]!["last_run"]!.AsObject();
        Assert.Equal("peak_private_mb,min_free_mb,elapsed_seconds,exit_code,finished_utc", string.Join(',', fam.Select(p => p.Key)));
        Assert.Equal(321, fam["peak_private_mb"]!.GetValue<long>());
        Assert.Equal(0, fam["exit_code"]!.GetValue<int>());
        Assert.NotNull(st["query_store"]!["last_run"]);
    }

    [Fact]
    public async Task R15_a_later_run_that_skips_a_family_leaves_that_familys_last_run_untouched()
    {
        using var r = new RunRoot(); r.SeedState();
        await Runs.Go(r.Settings(), new FakeChild(), Pin);
        var before = r.ReadState()["query_store"]!["last_run"]!.ToJsonString();
        // Only fresh is due again: pull its last_to back; query_store is current.
        var st = r.ReadState(); st["fresh"]!["last_to"] = "2026-09-28T12:00:00.0000000Z";
        File.WriteAllText(r.StatePath, st.ToJsonString());
        var child = new FakeChild();
        await Runs.Go(r.Settings(), child, Pin);
        Assert.Equal(before, r.ReadState()["query_store"]!["last_run"]!.ToJsonString());
        Assert.Equal("fresh", r.ReadState()["last_run"]!["family"]!.GetValue<string>());
    }

    [Fact]
    public async Task R15_failure_records_the_last_500_chars_of_stderr_else_stdout_else_the_message()
    {
        using var r = new RunRoot(); r.SeedState();
        var long700 = new string('a', 200) + new string('b', 500);
        var child = new FakeChild { ExitFor = _ => 1, StderrFor = _ => long700 };
        await Runs.Go(r.Settings(), child, Pin);
        var err = r.ReadState()["fresh"]!["last_error"]!.GetValue<string>();
        Assert.Equal(500, err.Length);
        Assert.Equal(new string('b', 500), err);
        Assert.NotNull(r.ReadState()["fresh"]!["last_failure_utc"]);

        using var r2 = new RunRoot(); r2.SeedState();
        await Runs.Go(r2.Settings(), new FakeChild { ExitFor = _ => 1 }, Pin);   // no stderr: stdout ("stub ran")
        Assert.Contains("stub ran", r2.ReadState()["fresh"]!["last_error"]!.GetValue<string>());

        using var r3 = new RunRoot(); r3.SeedState();
        var notStarted = new FakeChild { Override = _ => new ChildResult { NotStarted = "only 100 MB free; need at least 2 MB to start" } };
        var (exit, _) = await Runs.Go(r3.Settings(), notStarted, Pin);
        Assert.Equal(1, exit);
        Assert.Equal("only 100 MB free; need at least 2 MB to start", r3.ReadState()["fresh"]!["last_error"]!.GetValue<string>());
        Assert.Equal("2026-09-28T12:00:00.0000000Z", r3.ReadState()["fresh"]!["last_to"]!.GetValue<string>());
    }

    [Fact]
    public async Task R15_gaps_are_appended_with_family_from_to_recorded_utc_and_advance_last_to()
    {
        using var r = new RunRoot();
        r.SeedState(tweak: o => o["fresh"]!["last_to"] = "2026-09-27T10:00:00.0000000Z");
        var child = new FakeChild();
        await Runs.Go(r.Settings(), child, Utc(2026, 9, 28, 9, 20));
        var gaps = r.ReadState()["gaps"]!.AsArray();
        Assert.NotEmpty(gaps);
        var g = gaps[0]!.AsObject();
        Assert.Equal("family,from,to,recorded_utc", string.Join(',', g.Select(p => p.Key)));
        Assert.Equal("fresh", g["family"]!.GetValue<string>());
        // A gap is never exported.
        Assert.DoesNotContain(child.Calls, c => FakeChild.Arg(c, "--from") == g["from"]!.GetValue<string>().Replace(".0000000", ""));
    }

    [Fact]
    public async Task R15_each_family_keeps_its_own_last_run_values_and_the_top_level_follows_the_latest_call()
    {
        // Per-family last_run: the two families' elapsed_seconds differ, so a shared or copied last_run cannot pass.
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild
        {
            Override = c => FakeChild.Family(c) == "query_store"
                ? new ChildResult { ExitCode = 0, PeakPrivateMb = 700, MinFreeMb = 9000, ElapsedSeconds = 2.5, FloorMb = 1 }
                : new ChildResult { ExitCode = 0, PeakPrivateMb = 300, MinFreeMb = 12000, ElapsedSeconds = 0.5, FloorMb = 1 },
        };
        await Runs.Go(r.Settings(), child, Pin);
        var st = r.ReadState();
        var fresh = st["fresh"]!["last_run"]!;
        var qs = st["query_store"]!["last_run"]!;
        Assert.Equal((0.5, 300L, 12000L), (fresh["elapsed_seconds"]!.GetValue<double>(), fresh["peak_private_mb"]!.GetValue<long>(), fresh["min_free_mb"]!.GetValue<long>()));
        Assert.Equal((2.5, 700L, 9000L), (qs["elapsed_seconds"]!.GetValue<double>(), qs["peak_private_mb"]!.GetValue<long>(), qs["min_free_mb"]!.GetValue<long>()));
        Assert.NotNull(fresh["finished_utc"]);
        Assert.NotNull(qs["finished_utc"]);
        var top = st["last_run"]!;
        Assert.Equal("query_store", top["family"]!.GetValue<string>());
        Assert.Equal(2.5, top["elapsed_seconds"]!.GetValue<double>());
    }

    [Fact]
    public async Task R9_a_failed_window_is_retried_by_the_next_run_from_the_same_from_and_the_state_file_stays_small()
    {
        // A failing run records the error as plain text and holds; the retry run succeeds from the same window.
        using var r = new RunRoot(); r.SeedState();
        var failing = new FakeChild { ExitFor = _ => 1, StderrFor = _ => "boom: disk full" };
        var (e1, _) = await Runs.Go(r.Settings(), failing, Pin);
        var st = r.ReadState();
        Assert.Equal(1, e1);
        Assert.Equal("2026-09-28T12:00:00.0000000Z", st["fresh"]!["last_to"]!.GetValue<string>());
        Assert.Contains("boom: disk full", st["fresh"]!["last_error"]!.GetValue<string>());
        Assert.NotNull(st["fresh"]!["last_failure_utc"]);
        Assert.True(new FileInfo(r.StatePath).Length < 4096);

        var ok = new FakeChild();
        var (e2, _) = await Runs.Go(r.Settings(), ok, Pin);
        var after = r.ReadState();
        Assert.Equal(0, e2);
        Assert.Equal("2026-09-28T12:00:00Z", FakeChild.Arg(ok.Calls[0], "--from"));
        Assert.Equal("2026-09-28T16:00:00.0000000Z", after["fresh"]!["last_to"]!.GetValue<string>());
        Assert.Null(after["fresh"]!["last_error"]);
        Assert.NotNull(after["fresh"]!["last_success_utc"]);
    }

    // ---- R16: attempt stamp, atomic writes, BOM ----

    [Fact]
    public async Task R16_last_attempt_utc_is_the_scheduling_instant_and_a_utf8_bom_state_file_is_accepted_and_rewritten_without_one()
    {
        using var r = new RunRoot();
        var seeded = r.SeedState();
        var bytes = new UTF8Encoding(true).GetPreamble().Concat(File.ReadAllBytes(r.StatePath)).ToArray();
        File.WriteAllBytes(r.StatePath, bytes);
        var (exit, _) = await Runs.Go(r.Settings(), new FakeChild(), Pin);
        Assert.Equal(0, exit);
        var after = File.ReadAllBytes(r.StatePath);
        Assert.False(after.Length >= 3 && after[0] == 0xEF && after[1] == 0xBB && after[2] == 0xBF);
        Assert.Equal("2026-09-28T16:30:00.0000000Z", r.ReadState()["last_attempt_utc"]!.GetValue<string>());
    }

    [Fact]
    public async Task R16_writes_are_atomic_no_temp_files_are_left_and_the_file_is_always_valid_json()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();
        child.OnCall = _ => Assert.NotNull(JsonNode.Parse(File.ReadAllText(r.StatePath)));   // mid-run: the old-or-new version, never a torn one
        await Runs.Go(r.Settings(), child, Pin);
        Assert.Empty(Directory.GetFiles(r.State, "*.tmp-*"));
    }

    [Fact]
    public async Task R16_a_reader_polling_the_state_file_during_many_saves_never_sees_a_torn_file()
    {
        using var r = new RunRoot();
        var state = new ScheduleState(r.SeedState());
        state.Root["padding"] = new string('x', 3_000_000);   // a write this big is not one disk block, so a truncate-then-write would be seen half done
        state.Save(r.StatePath);
        var stop = false;
        var torn = 0;
        var reads = 0;
        var reader = Task.Run(() =>
        {
            while (!Volatile.Read(ref stop))
            {
                try { JsonNode.Parse(File.ReadAllText(r.StatePath)); reads++; }
                catch (System.Text.Json.JsonException) { torn++; }
                catch (IOException) { }   // a move in flight can briefly deny the open on Windows; that is not a torn file
            }
        });
        for (var i = 0; i < 60; i++)
        {
            state.Root["n"] = i;
            state.Save(r.StatePath);
        }
        Volatile.Write(ref stop, true);
        await reader;
        Assert.Equal(0, torn);
        Assert.True(reads > 0);
    }

    [Fact]
    public void R16_save_failure_leaves_the_old_file_untouched()
    {
        using var r = new RunRoot(); r.SeedState();
        var before = File.ReadAllBytes(r.StatePath);
        var (state, _) = ScheduleState.Load(r.StatePath);
        // A directory squatting on the target path makes the move fail after the temp file is written.
        var blocked = Path.Combine(r.State, "blocked.json");
        Directory.CreateDirectory(blocked);
        Assert.ThrowsAny<Exception>(() => state!.Save(blocked));
        Assert.Equal(before, File.ReadAllBytes(r.StatePath));
        Assert.Empty(Directory.GetFiles(r.State, "*.tmp-*"));
    }

    // ---- R17: restart delay ----

    private static async Task<(int Exit, string Output, int Calls, bool StateSame, string DailyLog)> Restart(DateTime? restartAt, int delay = 30)
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Restart = x.Restart with { DelayMinutes = delay } });
        var before = File.ReadAllBytes(r.StatePath);
        var child = new FakeChild();
        var detector = new FixedRestart(restartAt);
        var (exit, output) = await Runs.Go(s, child, Utc(2026, 9, 28, 15, 30), restart: detector);
        var log = Path.Combine(r.Logs, "export-testh-20260928.log");
        return (exit, output, child.Calls.Count, before.SequenceEqual(File.ReadAllBytes(r.StatePath)), File.Exists(log) ? File.ReadAllText(log) : "");
    }

    [Fact]
    public async Task R17_restart_10_minutes_ago_skips_with_the_message_three_log_lines_and_a_byte_identical_state_file()
    {
        var (exit, output, calls, same, log) = await Restart(Utc(2026, 9, 28, 15, 20));
        Assert.Equal(0, exit);
        Assert.Equal(0, calls);
        Assert.True(same);
        Assert.Matches(@"skipped: Darling restarted at .* \(10 min ago\), inside the 30-minute restart delay; the next run catches up", output);
        var lines = log.TrimEnd().Split('\n');
        Assert.Equal(3, lines.Length);
        Assert.Equal("===== run 20260928T153000Z start export testh", lines[0]);
        Assert.StartsWith("skipped: Darling restarted at ", lines[1]);
        Assert.Equal("===== run 20260928T153000Z end exit=0 elapsed=0s", lines[2]);
    }

    [Fact]
    public async Task R17_a_skipped_run_does_not_start_the_post_run_hook_either()
    {
        // A restart 10 min ago makes no exporter call, the hook included
        using var r = new RunRoot(); r.SeedState();
        var rec = Path.Combine(r.Root, "hook-ran.txt");
        var hook = new PostRunSettings { Name = "alert-check", Command = [TestChildPath.Exe, "--tc-record", rec] };
        var s = r.Settings(tweak: x => x with { PostRun = hook, Restart = x.Restart with { DelayMinutes = 30 } });
        var (exit, _) = await Runs.Go(s, new NoChildAllowed(), Utc(2026, 9, 28, 15, 30), restart: new FixedRestart(Utc(2026, 9, 28, 15, 20)));
        Assert.Equal(0, exit);
        Assert.False(File.Exists(rec));
        Assert.Empty(Directory.GetFiles(r.Logs, "alert-check-*"));
    }

    [Fact]
    public async Task R17_the_restart_skip_comes_before_the_lock_so_a_run_inside_the_delay_skips_even_while_another_run_holds_the_lock()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Restart = x.Restart with { DelayMinutes = 30 } });
        using var held = RunLock.TryAcquire(r.State);
        Assert.NotNull(held);
        var (exit, output) = await Runs.Go(s, new NoChildAllowed(), Utc(2026, 9, 28, 15, 30), restart: new FixedRestart(Utc(2026, 9, 28, 15, 20)));
        Assert.Equal(0, exit);
        Assert.Contains("skipped: Darling restarted", output);
        Assert.DoesNotContain("another run holds the lock", output);
    }

    [Fact]
    public async Task R17_restart_40_minutes_ago_runs_normally_and_writes_no_skip_log()
    {
        var (exit, output, calls, _, log) = await Restart(Utc(2026, 9, 28, 14, 50));
        Assert.Equal(0, exit);
        Assert.True(calls > 0);
        Assert.DoesNotContain("skipped: Darling restarted", output);
        Assert.DoesNotContain("skipped: Darling restarted", log);
    }

    [Fact]
    public async Task R17_delay_0_disables_the_check_and_does_not_even_read_the_restart_time()
    {
        using var r = new RunRoot(); r.SeedState();
        var detector = new FixedRestart(Utc(2026, 9, 28, 15, 20));
        var child = new FakeChild();
        var (exit, _) = await Runs.Go(r.Settings(), child, Utc(2026, 9, 28, 15, 30), restart: detector);
        Assert.Equal(0, exit);
        Assert.True(child.Calls.Count > 0);
        Assert.Equal(0, detector.Reads);
    }

    [Fact]
    public async Task R17_an_unknown_restart_time_runs_and_a_restart_in_the_future_is_clock_skew_and_runs()
    {
        Assert.True((await Restart(null)).Calls > 0);
        Assert.True((await Restart(Utc(2026, 9, 28, 15, 45))).Calls > 0);
    }

    [Theory]
    [InlineData(29, true)] [InlineData(30, false)] [InlineData(0, true)]
    public void R17_the_delay_is_strictly_less_than_the_configured_minutes(int minutesAgo, bool inside)
    {
        var now = Utc(2026, 9, 28, 15, 30);
        Assert.Equal(inside && minutesAgo < 30, RestartDelay.IsWithin(now, now.AddMinutes(-minutesAgo), 30));
        Assert.False(RestartDelay.IsWithin(now, now.AddMinutes(-1), 0));
    }

    [Fact]
    public void R17_file_detector_reads_the_last_write_time_and_a_missing_file_is_unknown()
    {
        using var r = new RunRoot();
        var f = Path.Combine(r.Root, "postmaster.pid");
        Assert.Null(new FileRestartDetector(f).RestartUtc());
        File.WriteAllText(f, "1");
        File.SetLastWriteTimeUtc(f, Utc(2026, 9, 28, 15, 20));
        Assert.Equal(Utc(2026, 9, 28, 15, 20), new FileRestartDetector(f).RestartUtc());
    }

    [Fact]
    public void R17_command_detector_reads_the_first_stdout_line_and_a_failure_timeout_or_garbage_is_unknown()
    {
        var child = TestChildPath.Exe;
        Assert.Equal(Utc(2026, 9, 28, 15, 20), new CommandRestartDetector([child, "--tc-print", "2026-09-28T15:20:00Z"], TimeSpan.FromSeconds(20)).RestartUtc());
        Assert.Null(new CommandRestartDetector([child, "--tc-print", "2026-09-28T15:20:00Z", "--tc-exit", "3"], TimeSpan.FromSeconds(20)).RestartUtc());
        Assert.Null(new CommandRestartDetector([child, "--tc-print", "not a date"], TimeSpan.FromSeconds(20)).RestartUtc());
        Assert.Null(new CommandRestartDetector(["/nonexistent/command"], TimeSpan.FromSeconds(20)).RestartUtc());
        Assert.Null(new CommandRestartDetector([child, "--tc-print", "2026-09-28T15:20:00Z", "--tc-sleep-ms", "30000"], TimeSpan.FromMilliseconds(500)).RestartUtc());
    }

    private sealed class FakeProcesses(DateTime? service, params DateTime[] postgres) : IDarlingProcessSource
    {
        public DateTime? ServiceProcessStartUtc() => service;
        public IReadOnlyList<DateTime> PostgresStartUtcs() => postgres;
    }

    [Fact]
    public void R17_darling_process_detector_takes_the_later_of_the_service_start_and_the_oldest_runtime_postgres_start()
    {
        Assert.Equal(Utc(2026, 9, 28, 15, 20), new DarlingProcessDetector(new FakeProcesses(Utc(2026, 9, 28, 15, 20), Utc(2026, 9, 28, 15, 5), Utc(2026, 9, 28, 15, 25))).RestartUtc());
        Assert.Equal(Utc(2026, 9, 28, 15, 5), new DarlingProcessDetector(new FakeProcesses(Utc(2026, 9, 28, 14, 0), Utc(2026, 9, 28, 15, 5), Utc(2026, 9, 28, 15, 25))).RestartUtc());
        Assert.Null(new DarlingProcessDetector(new FakeProcesses(null)).RestartUtc());
        Assert.Equal(Utc(2026, 9, 28, 14, 0), new DarlingProcessDetector(new FakeProcesses(Utc(2026, 9, 28, 14, 0))).RestartUtc());
    }

    [Fact]
    public void R17_off_detector_and_the_factory()
    {
        Assert.Null(new NoRestartDetector().RestartUtc());
        Assert.IsType<NoRestartDetector>(RestartDelay.Create(new RestartSettings { Detector = "off" }, TextWriter.Null));
        Assert.IsType<FileRestartDetector>(RestartDelay.Create(new RestartSettings { Detector = "file", File = "x" }, TextWriter.Null));
        Assert.IsType<CommandRestartDetector>(RestartDelay.Create(new RestartSettings { Detector = "command", Command = ["x"] }, TextWriter.Null));
        Assert.IsType<DarlingProcessDetector>(RestartDelay.Create(new RestartSettings { Detector = "darling_process" }, TextWriter.Null));
    }

    // ---- R24: post-run hook ----

    private static PostRunSettings Hook(int exit) => new() { Name = "alert-check", Command = [TestChildPath.Exe, "--tc-print", "hook output", "--tc-exit", exit.ToString(CultureInfo.InvariantCulture)] };

    [Fact]
    public async Task R24_hook_exit_5_fails_the_run_with_exit_1_and_never_writes_export_state()
    {
        using var ok = new RunRoot(); ok.SeedState();
        using var bad = new RunRoot(); bad.SeedState();
        // Nothing is due at 12:20 (both families are current), so the only state write is last_attempt_utc, and the two files must match.
        var now = Utc(2026, 9, 28, 13, 5);
        var (e0, _) = await Runs.Go(ok.Settings(tweak: x => x with { PostRun = Hook(0) }), new FakeChild(), now);
        var (e5, output) = await Runs.Go(bad.Settings(tweak: x => x with { PostRun = Hook(5) }), new FakeChild(), now);
        Assert.Equal(0, e0);
        Assert.Equal(1, e5);
        Assert.Contains("alert-check failed (exit 5)", output);
        Assert.Equal(File.ReadAllText(ok.StatePath).Replace(ok.Root, ""), File.ReadAllText(bad.StatePath).Replace(bad.Root, ""));
        var log = File.ReadAllText(Path.Combine(bad.Logs, "export-testh-20260928.log"));
        Assert.Contains("----- alert-check exit 5\nhook output\n", log);
        Assert.Contains("end exit=1", log);
        Assert.Empty(Directory.GetFiles(bad.Logs, "alert-check-*"));
    }

    [Fact]
    public async Task R24_hook_runs_even_when_an_export_failed_and_a_hook_that_cannot_start_fails_the_run()
    {
        using var r = new RunRoot(); r.SeedState();
        var (exit, output) = await Runs.Go(r.Settings(tweak: x => x with { PostRun = Hook(0) }), new FakeChild { ExitFor = _ => 1 }, Pin);
        Assert.Equal(1, exit);
        Assert.Contains("running alert-check for testh", output);
        Assert.Contains("----- alert-check exit 0", File.ReadAllText(Path.Combine(r.Logs, "export-testh-20260928.log")));

        using var r2 = new RunRoot(); r2.SeedState();
        var missing = new PostRunSettings { Name = "alert-check", Command = ["/nonexistent/alert-check"] };
        var (exit2, _) = await Runs.Go(r2.Settings(tweak: x => x with { PostRun = missing }), new FakeChild(), Utc(2026, 9, 28, 13, 5));
        Assert.Equal(1, exit2);
    }

    [Fact]
    public async Task R24_the_hook_gets_exactly_its_configured_argv_and_no_export_flag_reaches_it()
    {
        // The hook never carries --procedure-plan-reuse / --query-store-source, and export calls never carry the hook's options.
        using var r = new RunRoot(); r.SeedState();
        var rec = Path.Combine(r.Root, "hook-args.txt");
        var hook = new PostRunSettings { Name = "alert-check", Command = [TestChildPath.Exe, "alert-check", "--store", "testh", "--hook-only-flag", "1", "--tc-record", rec] };
        var child = new FakeChild();
        await Runs.Go(r.Settings(tweak: x => x with { PostRun = hook, ProcedurePlanReuse = "on", QueryStoreSource = "wide", AwsRegion = "us-east-1" }), child, Pin);
        Assert.Equal("alert-check --store testh --hook-only-flag 1 --tc-record " + rec + "\n", File.ReadAllText(rec));
        Assert.All(child.Calls, c => Assert.False(FakeChild.Has(c, "--hook-only-flag")));
    }

    [Fact]
    public async Task R24_a_hook_that_outlives_its_timeout_is_killed_and_fails_the_run()
    {
        using var r = new RunRoot(); r.SeedState();
        var slow = new PostRunSettings { Name = "alert-check", TimeoutSeconds = 1, Command = [TestChildPath.Exe, "--tc-sleep-ms", "60000"] };
        var (exit, output) = await Runs.Go(r.Settings(tweak: x => x with { PostRun = slow }), new FakeChild(), Utc(2026, 9, 28, 13, 5));
        Assert.Equal(1, exit);
        Assert.Contains($"failed (exit {PostRunHook.TimeoutExitCode})", output);
    }

    // ---- R25: --now ----

    [Fact]
    public async Task R25_scheduling_uses_now_while_recorded_stamps_stay_on_the_real_clock()
    {
        using var r = new RunRoot(); r.SeedState();
        var real = Utc(2026, 10, 29, 3, 3, 3);
        var (_, _) = await Runs.Go(r.Settings(), new FakeChild(), Pin, clockNow: real);
        var st = r.ReadState();
        Assert.Equal("2026-09-28T16:30:00.0000000Z", st["last_attempt_utc"]!.GetValue<string>());
        Assert.Equal("2026-10-29T03:03:03.0000000Z", st["fresh"]!["last_success_utc"]!.GetValue<string>());
        Assert.Equal("2026-09-28T16:00:00.0000000Z", st["fresh"]!["last_to"]!.GetValue<string>());
    }

    [Theory]
    [InlineData(2026, 9, 28, 15, 30, 1)]
    [InlineData(2026, 9, 28, 17, 30, 3)]
    public async Task R25_now_hook_1_5_hours_after_last_to_gives_one_fresh_window_and_3_5_hours_gives_three(int y, int mo, int d, int h, int mi, int expected)
    {
        using var r = new RunRoot(); r.SeedState(tweak: o => { o["fresh"]!["last_to"] = "2026-09-28T14:00:00.0000000Z"; o["query_store"]!["last_to"] = "2026-09-28T14:00:00.0000000Z"; });
        var child = new FakeChild();
        await Runs.Go(r.Settings(), child, Utc(y, mo, d, h, mi));
        Assert.Equal(expected, ByFamily(child, "fresh").Count);
    }

    // ---- R26: exit codes ----

    [Fact]
    public async Task R26_exit_0_for_ok_exit_1_for_a_failure_exit_2_for_a_refusal()
    {
        using var r = new RunRoot(); r.SeedState();
        Assert.Equal(0, (await Runs.Go(r.Settings(), new FakeChild(), Pin)).Exit);
        r.SeedState();
        Assert.Equal(1, (await Runs.Go(r.Settings(), new FakeChild { ExitFor = _ => 1 }, Pin)).Exit);
        Assert.Equal(2, (await Runs.Go(r.Settings(tweak: x => x with { Store = "Bad Name" }), new NoChildAllowed(), Pin)).Exit);
    }

    [Theory]
    [InlineData("{\"store\":\"testh\",\"fresh\":{}}")]                         // no schema key: the daily format
    [InlineData("{\"schema\":1,\"store\":\"testh\"}")]
    [InlineData("{\"schema\":3,\"store\":\"testh\"}")]
    [InlineData("{ not json")]
    [InlineData("[1,2]")]
    public async Task R26_a_state_file_that_is_not_schema_2_is_refused_with_exit_2_and_stays_byte_identical(string content)
    {
        using var r = new RunRoot();
        Directory.CreateDirectory(r.State);
        File.WriteAllText(r.StatePath, content);
        var (exit, output) = await Runs.Go(r.Settings(), new NoChildAllowed(), Pin);
        Assert.Equal(2, exit);
        Assert.Equal(content, File.ReadAllText(r.StatePath));
        Assert.Contains("nothing exported", output);
        Assert.False(File.Exists(RunLock.PathFor(r.State)));
    }

    [Fact]
    public async Task R26_settings_that_name_a_user_without_its_credential_file_exit_2_with_no_fallback_and_no_child()
    {
        using var r = new RunRoot(); r.SeedState();
        var before = File.ReadAllBytes(r.StatePath);
        var (exit, output) = await Runs.Go(r.Settings(tweak: x => x with { PgUser = "export_user" }), new NoChildAllowed(), Pin);
        Assert.Equal(2, exit);
        Assert.Contains("pg_user and pg_credential_file must be set together", output);
        Assert.Equal(before, File.ReadAllBytes(r.StatePath));
    }

    [Fact]
    public async Task R26_out_of_range_settings_are_refused_with_exit_2_before_any_state_write_or_child()
    {
        // Settings are data, so a bad value is a refusal.
        using var r = new RunRoot(); r.SeedState();
        var before = File.ReadAllBytes(r.StatePath);
        var bad = new (string Why, Func<RunSettings, RunSettings> Tweak)[]
        {
            ("lag 4", x => x with { Families = x.Families with { QueryStoreLagHours = 4 } }),
            ("lag 0", x => x with { Families = x.Families with { QueryStoreLagHours = 0 } }),
            ("margin 1", x => x with { Families = x.Families with { LateRowMarginMinutes = 1 } }),
            ("margin 16", x => x with { Families = x.Families with { LateRowMarginMinutes = 16 } }),
            ("window 7", x => x with { Families = x.Families with { FastWindowMinutes = 7 } }),
            ("cutoff 0", x => x with { Schedule = x.Schedule with { CutoffMinute = 0 } }),
            ("cutoff 61", x => x with { Schedule = x.Schedule with { CutoffMinute = 61 } }),
            ("max age 1", x => x with { Schedule = x.Schedule with { MaxWindowAgeHours = 1 } }),
            ("max windows 0", x => x with { Schedule = x.Schedule with { MaxWindowsPerFamily = 0 } }),
            ("empty fresh tables", x => x with { Families = x.Families with { FreshTables = [] } }),
            ("negative restart delay", x => x with { Restart = x.Restart with { DelayMinutes = -1 } }),
            ("unknown detector", x => x with { Restart = x.Restart with { Detector = "magic" } }),
            ("file detector without a file", x => x with { Restart = x.Restart with { Detector = "file" } }),
            ("post_run without a command", x => x with { PostRun = new PostRunSettings { Name = "alert-check" } }),
            ("store name", x => x with { Store = "1bad" }),
        };
        foreach (var (why, tweak) in bad)
        {
            var (exit, output) = await Runs.Go(r.Settings(tweak: tweak), new NoChildAllowed(), Pin);
            Assert.True(exit == 2, why + ": exit " + exit);
            Assert.Contains("refused:", output);
            Assert.Equal(before, File.ReadAllBytes(r.StatePath));
        }
    }

    [Fact]
    public void R26_settings_round_trip_through_snake_case_json_and_unknown_keys_are_refused()
    {
        using var r = new RunRoot();
        var s = r.Settings(tweak: x => x with { PostRun = Hook(0), Families = x.Families with { QueryStoreLagHours = 2 } });
        var json = s.ToJson();
        Assert.Contains("\"out_dir\"", json);
        Assert.Contains("\"query_store_lag_hours\": 2", json);
        Assert.Equal(s.ToJson(), RunSettings.FromJson(json).ToJson());
        Assert.ThrowsAny<System.Text.Json.JsonException>(() => RunSettings.FromJson("{\"store\":\"x\",\"typo_key\":1}"));
    }

    [Fact]
    public async Task R26_run_command_exit_codes_through_the_real_entry_point()
    {
        using var r = new RunRoot(); r.SeedState();
        var settingsFile = Path.Combine(r.Root, "settings.json");
        File.WriteAllText(settingsFile, r.Settings(tweak: x => x with { ChildExe = TestChildPath.Exe, ChildBaseArgs = ["--tc-print", "hi"] }).ToJson());
        var o = new StringWriter(); var e = new StringWriter();
        Assert.Equal(0, await RunCommand.MainAsync(["--settings", settingsFile, "--now", "2026-09-28T16:30:00Z"], o, e, CancellationToken.None));
        Assert.Equal(2, await RunCommand.MainAsync([], o, e, CancellationToken.None));
        Assert.Equal(2, await RunCommand.MainAsync(["--settings", Path.Combine(r.Root, "missing.json")], o, e, CancellationToken.None));
        Assert.Equal(2, await RunCommand.MainAsync(["--settings", settingsFile, "--now", "yesterday"], o, e, CancellationToken.None));
        Assert.Equal(2, await RunCommand.MainAsync(["--bogus"], o, e, CancellationToken.None));
    }
}

public static class TestChildPath
{
    public static string Exe
    {
        get
        {
            var dir = Path.Combine(AppContext.BaseDirectory, "testchild");
            var exe = Path.Combine(dir, OperatingSystem.IsWindows() ? "darling-testchild.exe" : "darling-testchild");
            return File.Exists(exe) ? exe : throw new FileNotFoundException("the test child was not built next to the tests", exe);
        }
    }
}
