using PerformanceMonitor.Exporter.Run;
using Xunit;
using static PerformanceMonitor.Exporter.Tests.Run.RunRoot;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>R1-R6, R10, R13: the pure planner. Instants and expected windows are fixed.</summary>
public class PlannerTests
{
    private static string Tos(IEnumerable<Window> w) => string.Join(',', w.Select(x => x.To.ToString("HH:mm")));
    private static string QsTos(DateTime last, DateTime now, int lag) => Tos(Planner.DueWindows("query_store", last, now, 6, 20, lag));

    // ---- R1: hourly due time ----

    [Fact]
    public void R1_normal_hour_fresh_has_two_windows_and_query_store_one()
    {
        var now = Utc(2026, 9, 28, 9, 20);
        var fresh = Planner.DueWindows("fresh", Utc(2026, 9, 28, 7, 0), now);
        Assert.Equal(2, fresh.Count);
        Assert.Equal(Utc(2026, 9, 28, 8, 0), fresh[0].To);
        var qs = Planner.DueWindows("query_store", Utc(2026, 9, 28, 6, 0), now);
        Assert.Single(qs);
        Assert.Equal(Utc(2026, 9, 28, 7, 0), qs[0].To);
    }

    [Fact]
    public void R1_fresh_window_is_not_due_at_9_14_and_is_due_at_9_20()
    {
        Assert.Empty(Planner.DueWindows("fresh", Utc(2026, 9, 28, 8, 0), Utc(2026, 9, 28, 9, 14)));
        var w = Planner.DueWindows("fresh", Utc(2026, 9, 28, 8, 0), Utc(2026, 9, 28, 9, 20));
        Assert.Single(w);
        Assert.Equal(Utc(2026, 9, 28, 9, 0), w[0].To);
    }

    [Fact]
    public void R1_hourly_due_times_to_the_second_and_no_margin_parameter_on_the_hourly_planner()
    {
        Assert.Single(Planner.DueWindows("fresh", Utc(2026, 9, 28, 8, 0), Utc(2026, 9, 28, 9, 15, 20)));
        Assert.Empty(Planner.DueWindows("fresh", Utc(2026, 9, 28, 8, 0), Utc(2026, 9, 28, 9, 14, 59)));
        var qs = Planner.DueWindows("query_store", Utc(2026, 9, 28, 6, 0), Utc(2026, 9, 28, 9, 15, 20));
        Assert.Equal(Utc(2026, 9, 28, 7, 0), Assert.Single(qs).To);
    }

    [Fact]
    public void R1_lag_2_edges()
    {
        Assert.Equal("11:00", QsTos(Utc(2026, 9, 28, 10, 0), Utc(2026, 9, 28, 12, 20), 2));
        Assert.Equal("", QsTos(Utc(2026, 9, 28, 10, 0), Utc(2026, 9, 28, 12, 14), 2));
        Assert.Equal("10:00", QsTos(Utc(2026, 9, 28, 9, 0), Utc(2026, 9, 28, 12, 14), 2));
        Assert.Equal("11:00", QsTos(Utc(2026, 9, 28, 10, 0), Utc(2026, 9, 28, 12, 15), 2));
    }

    [Theory]
    [InlineData(3, 135)]
    [InlineData(2, 75)]
    [InlineData(1, 45)]
    public void R1_gate_cross_check_window_6_to_7_is_due_exactly_at_to_plus_the_lag_margin(int lag, int dueAfterTo)
    {
        var due = Utc(2026, 9, 28, 7, 0).AddMinutes(dueAfterTo);
        Assert.Equal("07:00", QsTos(Utc(2026, 9, 28, 6, 0), due, lag));
        Assert.Equal("", QsTos(Utc(2026, 9, 28, 6, 0), due.AddMinutes(-1), lag));
    }

    [Fact]
    public void R1_default_lag_is_3_and_fresh_ignores_the_lag()
    {
        Assert.Equal("10:00", QsTos(Utc(2026, 9, 28, 9, 0), Utc(2026, 9, 28, 12, 20), 3));
        var noLagArg = Planner.DueWindows("query_store", Utc(2026, 9, 28, 9, 0), Utc(2026, 9, 28, 12, 20));
        Assert.Equal(Utc(2026, 9, 28, 10, 0), Assert.Single(noLagArg).To);
        var f2 = Planner.DueWindows("fresh", Utc(2026, 9, 28, 10, 0), Utc(2026, 9, 28, 12, 20), 6, 20, 2);
        var f1 = Planner.DueWindows("fresh", Utc(2026, 9, 28, 10, 0), Utc(2026, 9, 28, 12, 20), 6, 20, 1);
        Assert.Equal(2, f2.Count);
        Assert.Equal(Utc(2026, 9, 28, 12, 0), f2[1].To);
        Assert.Equal(2, f1.Count);
    }

    [Fact]
    public void R1_lag_1_is_due_at_h_plus_1h45m()
    {
        Assert.Equal("", QsTos(Utc(2026, 9, 28, 15, 0), Utc(2026, 9, 28, 16, 35), 1));
        Assert.Equal("", QsTos(Utc(2026, 9, 28, 15, 0), Utc(2026, 9, 28, 16, 44), 1));
        Assert.Equal("16:00", QsTos(Utc(2026, 9, 28, 15, 0), Utc(2026, 9, 28, 16, 50), 1));
        Assert.Equal("16:00", QsTos(Utc(2026, 9, 28, 15, 0), Utc(2026, 9, 28, 17, 5), 1));
        Assert.Equal("16:00", QsTos(Utc(2026, 9, 28, 15, 0), Utc(2026, 9, 28, 17, 20), 1));
    }

    [Fact]
    public void R1_switching_the_lag_3_to_1_to_3_skips_and_repeats_nothing()
    {
        Assert.Equal("", QsTos(Utc(2026, 9, 28, 14, 0), Utc(2026, 9, 28, 16, 50), 3));
        Assert.Equal("15:00,16:00", QsTos(Utc(2026, 9, 28, 14, 0), Utc(2026, 9, 28, 16, 50), 1));
        Assert.Equal("", QsTos(Utc(2026, 9, 28, 16, 0), Utc(2026, 9, 28, 17, 20), 3));
        Assert.Equal("17:00", QsTos(Utc(2026, 9, 28, 16, 0), Utc(2026, 9, 28, 19, 20), 3));
        Assert.Equal("12:00,13:00", QsTos(Utc(2026, 9, 28, 11, 0), Utc(2026, 9, 28, 14, 20), 2));
    }

    // ---- R2: catch-up and gaps ----

    [Fact]
    public void R2_a_10_hour_outage_gives_6_windows_and_no_gaps()
    {
        var w = Planner.DueWindows("fresh", Utc(2026, 9, 27, 23, 0), Utc(2026, 9, 28, 9, 20), 6, 20);
        Assert.Equal(6, w.Count);
        Assert.DoesNotContain(w, x => x.IsGap);
    }

    [Fact]
    public void R2_a_30_hour_outage_gives_6_windows_some_of_them_gaps_and_gaps_count_toward_the_cap()
    {
        var w = Planner.DueWindows("fresh", Utc(2026, 9, 27, 10, 0), Utc(2026, 9, 28, 9, 20), 6, 20);
        Assert.Equal(6, w.Count);
        Assert.Contains(w, x => x.IsGap);
        Assert.Contains(w, x => !x.IsGap);
        Assert.True(w.TakeWhile(x => x.IsGap).Count() + w.SkipWhile(x => x.IsGap).Count() == 6, "gaps come first, then exportable windows");
    }

    // ---- R3: first run ----

    [Fact]
    public void R3_hourly_first_run_starts_at_newest_due_minus_19_hours()
    {
        var now = Utc(2026, 9, 28, 9, 20);
        var w = Planner.DueWindows("fresh", null, now, 6, 20);
        Assert.Equal(Utc(2026, 9, 28, 9, 0).AddHours(-19), w[0].To);
        Assert.False(w[0].IsGap);
    }

    [Fact]
    public void R3_fast_first_run_starts_at_the_first_boundary_at_or_after_now_minus_max_age_with_no_gap()
    {
        var fw = Planner.FastDueWindows(null, Utc(2026, 9, 28, 9, 35), 15);
        Assert.Equal(Utc(2026, 9, 27, 13, 45), fw[0].From);
        Assert.Equal(6, fw.Count);
        Assert.False(fw[0].IsGap);
    }

    // ---- R4: fast windows ----

    [Fact]
    public void R4_fast_a_boundary_is_due_at_b_plus_margin()
    {
        Assert.Empty(Planner.FastDueWindows(Utc(2026, 9, 28, 9, 10), Utc(2026, 9, 28, 9, 25), 10));
        var one = Planner.FastDueWindows(Utc(2026, 9, 28, 9, 10), Utc(2026, 9, 28, 9, 35), 10);
        var w = Assert.Single(one);
        Assert.Equal(Utc(2026, 9, 28, 9, 10), w.From);
        Assert.Equal(Utc(2026, 9, 28, 9, 20), w.To);
        Assert.False(w.IsGap);
        Assert.Empty(Planner.FastDueWindows(Utc(2026, 9, 28, 9, 10), Utc(2026, 9, 28, 9, 34), 10));
    }

    [Fact]
    public void R4_fast_catch_up_splits_60_60_60_60_10_and_caps_at_max_calls()
    {
        var fw = Planner.FastDueWindows(Utc(2026, 9, 28, 12, 0), Utc(2026, 9, 28, 16, 30), 10);
        Assert.Equal(5, fw.Count);
        Assert.Equal(Utc(2026, 9, 28, 16, 10), fw[4].To);
        Assert.Equal("60,60,60,60,10", string.Join(',', fw.Select(x => (int)(x.To - x.From).TotalMinutes)));
        var capped = Planner.FastDueWindows(Utc(2026, 9, 28, 12, 0), Utc(2026, 9, 28, 16, 30), 10, maxCalls: 2);
        Assert.Equal(2, capped.Count);
        Assert.Equal(Utc(2026, 9, 28, 14, 0), capped[1].To);
    }

    [Fact]
    public void R4_fast_range_older_than_max_age_is_one_gap_entry_that_does_not_count_toward_the_cap()
    {
        var fw = Planner.FastDueWindows(Utc(2026, 9, 27, 6, 0), Utc(2026, 9, 28, 9, 35), 10, 6, 20);
        Assert.True(fw[0].IsGap);
        Assert.Equal(Utc(2026, 9, 27, 6, 0), fw[0].From);
        Assert.Equal(Utc(2026, 9, 27, 13, 30), fw[0].To);
        Assert.Equal(7, fw.Count);
        Assert.False(fw[1].IsGap);
        Assert.Equal(fw[0].To, fw[1].From);
    }

    [Fact]
    public void R4_fast_5_minute_windows_align_to_the_5_minute_boundary()
    {
        var fw = Planner.FastDueWindows(Utc(2026, 9, 28, 9, 0), Utc(2026, 9, 28, 9, 20), 5);
        Assert.Equal(Utc(2026, 9, 28, 9, 5), Assert.Single(fw).To);
    }

    // ---- R5: late-row margin ----

    [Theory]
    [InlineData("2", 2)] [InlineData("5", 5)] [InlineData("  5 \r\n", 5)] [InlineData("15", 15)]
    [InlineData("1", 15)] [InlineData("16", 15)] [InlineData("0", 15)] [InlineData("abc", 15)] [InlineData("5.5", 15)]
    [InlineData("-5", 15)] [InlineData("", 15)] [InlineData("\r\n  \r\n", 15)] [InlineData(null, 15)]
    [InlineData("\n\n  7\n99", 7)] [InlineData("abc\n5", 15)]
    public void R5_late_row_margin_parser(string? text, int expected) => Assert.Equal(expected, Planner.ParseLateRowMargin(text));

    [Fact]
    public void R5_margin_5_and_15_decide_when_a_fast_window_is_due()
    {
        static DateTime At(int h, int m, int s) => Utc(2026, 9, 28, h, m, s);
        var due = Planner.FastDueWindows(At(9, 0, 0), At(9, 20, 20), 15, lateMinutes: 5);
        Assert.Equal(At(9, 15, 0), Assert.Single(due).To);
        Assert.Empty(Planner.FastDueWindows(At(9, 0, 0), At(9, 19, 59), 15, lateMinutes: 5));
        Assert.Empty(Planner.FastDueWindows(At(9, 0, 0), At(9, 20, 20), 15, lateMinutes: 15));
        Assert.Equal(At(9, 15, 0), Assert.Single(Planner.FastDueWindows(At(9, 0, 0), At(9, 30, 20), 15)).To);
        Assert.Equal(At(9, 0, 0), Assert.Single(Planner.FastDueWindows(At(8, 0, 0), At(9, 5, 20), 60, lateMinutes: 5)).To);
        Assert.Empty(Planner.FastDueWindows(At(8, 0, 0), At(9, 4, 59), 60, lateMinutes: 5));
    }

    [Theory]
    [InlineData(2, 5)] [InlineData(2, 15)] [InlineData(2, 60)] [InlineData(5, 5)] [InlineData(5, 15)] [InlineData(5, 60)]
    [InlineData(15, 5)] [InlineData(15, 15)] [InlineData(15, 60)]
    public void R5_guard_cross_check_a_due_window_satisfies_to_le_now_minus_margin(int margin, int window)
    {
        var b = Utc(2026, 9, 28, 9, 0);
        var dueNow = b.AddMinutes(window).AddMinutes(margin);
        var fw = Planner.FastDueWindows(b, dueNow, window, lateMinutes: margin);
        Assert.NotEmpty(fw);
        Assert.True(fw[^1].To <= dueNow.AddMinutes(-margin));
        Assert.Empty(Planner.FastDueWindows(b, dueNow.AddSeconds(-1), window, lateMinutes: margin));
    }

    // ---- R6: Query Store lag parser ----

    [Theory]
    [InlineData(null, 3)] [InlineData("", 3)] [InlineData("\r\n  2 \r\n", 2)] [InlineData("1\n", 1)] [InlineData("3", 3)]
    [InlineData("4", 3)] [InlineData("0", 3)] [InlineData("02", 3)] [InlineData("two", 3)] [InlineData("2h", 3)] [InlineData("2 3", 3)] [InlineData("x\n2", 3)]
    public void R6_query_store_lag_parser(string? text, int expected) => Assert.Equal(expected, Planner.ParseQueryStoreLag(text));

    // ---- R10: table sets ----

    [Fact]
    public void R10_default_fresh_tables_are_exactly_the_literal_list()
    {
        var psm = new[]
        {
            "perfmon_stats", "wait_stats", "file_io_stats", "memory_stats", "cpu_utilization_stats", "spinlock_stats", "latch_stats", "tempdb_stats",
            "deadlocks", "blocked_process_reports", "collection_log", "query_stats_hourly", "procedure_stats_hourly", "plan_shape", "query_text", "procedure_plan",
            "system_health_cpu", "system_health_system", "system_health_memory", "system_health_query_processing", "system_health_io", "system_health_io_pending",
            "system_health_errors", "system_health_waits", "system_health_memory_broker", "system_health_memory_node_oom",
        };
        Assert.Equal(psm, Planner.DefaultFreshTables);
        Assert.Equal("query_store_stats,plan_shape,query_text", string.Join(',', Planner.QueryStoreTableList));
    }

    [Fact]
    public void R10_sub_hour_and_hourly_lists_partition_the_fresh_list_and_the_hourly_rest_is_the_five_tables()
    {
        using var root = new RunRoot();
        var s = root.Settings(tweak: x => x with { Families = x.Families with { FastWindowMinutes = 10 } });
        var m = Planner.ResolveMode(s, []);
        Assert.Equal("query_stats_hourly,procedure_stats_hourly,plan_shape,query_text,procedure_plan", m.FreshTables);
        var fast = m.FastTables.Split(',');
        Assert.All(fast, t => Assert.Contains(t, SubHourTables.All));
        Assert.Equal(Planner.DefaultFreshTables.Count, fast.Length + m.FreshTables.Split(',').Length);
    }

    [Fact]
    public void R10_call_objects_for_the_default_config_fresh_and_query_store()
    {
        using var root = new RunRoot();
        var s = root.Settings();
        var m = Planner.ResolveMode(s, []);
        var fresh = Planner.PlanFamily(s, m, "fresh", Utc(2026, 9, 28, 12, 0), Utc(2026, 9, 28, 16, 30));
        var qs = Planner.PlanFamily(s, m, "query_store", Utc(2026, 9, 28, 12, 0), Utc(2026, 9, 28, 16, 30));
        var fc = Planner.CallFor(m, fresh, fresh.Windows[0]);
        var qc = Planner.CallFor(m, qs, qs.Windows[0]);
        Assert.Equal(("query_stats", (string?)null, (int?)null, (int?)null), (fc.DimensionOrigin, fc.Also, fc.QueryStoreLagHours, fc.FreshGuardMinutes));
        Assert.Equal(string.Join(',', Planner.DefaultFreshTables), fc.Tables);
        Assert.Equal(("query_store", "query_store_stats,plan_shape,query_text"), (qc.DimensionOrigin, qc.Tables));
        Assert.Equal(("2026-09-28T12:00:00Z", "2026-09-28T13:00:00Z"), (fc.FromArg, fc.ToArg));
    }

    [Fact]
    public void R10_fresh_fast_with_no_hourly_tables_and_no_also_skips_the_fresh_call_but_keeps_the_full_list_when_also_is_set()
    {
        using var root = new RunRoot();
        var s = root.Settings("store_pg", x => x with { Families = x.Families with { FastWindowMinutes = 10, FreshTables = ["collection_log"], QueryStoreEnabled = false } });
        var noAlso = Planner.ResolveMode(s, []);
        Assert.True(noAlso.UseFast);
        Assert.True(noAlso.SkipHourlyFresh);
        Assert.Equal(new[] { "fresh_fast" }, Planner.FamilyOrder(noAlso));
        var withAlso = Planner.ResolveMode(s, ["pg_extra"]);
        Assert.False(withAlso.SkipHourlyFresh);
        Assert.Equal("collection_log", withAlso.FreshTables);
        Assert.Equal(new[] { "fresh_fast", "fresh" }, Planner.FamilyOrder(withAlso));
    }

    [Fact]
    public void R10_a_store_with_no_fast_tables_turns_fast_off()
    {
        using var root = new RunRoot();
        var s = root.Settings(tweak: x => x with { Families = x.Families with { FastWindowMinutes = 10, FreshTables = ["plan_shape"] } });
        Assert.False(Planner.ResolveMode(s, []).UseFast);
    }

    // ---- R13: optional tables ----

    [Fact]
    public void R13_optional_tables_parse_comments_blanks_and_whitespace_order_preserved()
    {
        var got = Planner.ParseOptionalTables(["# header", "", "  plan_correction  # trailing", "default_trace_events", "   ", "job_history"]);
        Assert.Equal(new[] { "plan_correction", "default_trace_events", "job_history" }, got);
        Assert.Empty(Planner.ParseOptionalTables([]));
    }
}
