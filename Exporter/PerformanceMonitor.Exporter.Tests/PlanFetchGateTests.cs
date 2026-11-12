using DuckDB.NET.Data;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// The plan-fetch gate (docs/store-read-budget.md "Plan fetch threshold"): a plan's XML is fetched only when its
/// accumulated executions reach 5 or its accumulated CPU reaches 1 s. Runs against a real on-disk StateStore; the
/// helper below plays the part of one exporter run (gate, then record after success).
/// </summary>
public class PlanFetchGateTests
{
    private static readonly DateTime T0 = new(2026, 10, 1, 0, 0, 0, DateTimeKind.Utc);

    private static string NewStateDir() =>
        Path.Combine(Path.GetTempPath(), $"darling-export-gate-test-{Guid.NewGuid():N}");

    private static PlanFetchCandidate Cand(string? server, string hash, long executions, long cpuUs, DateTime first, DateTime? last = null, string origin = "query_store") =>
        new(server, hash, origin, executions, cpuUs, first, last ?? first.AddMinutes(50));

    /// <summary>One exporter run: the gate over <paramref name="candidates"/>, then the state write a successful run does.</summary>
    private static List<PlanFetchDecision> RunWindow(StateStore store, string source, DateTime now, params PlanFetchCandidate[] candidates)
    {
        var decisions = PlanFetchGate.Apply(store, source, now, candidates);
        var exported = decisions.Where(d => d.Qualifies).Select(d => (d.ServerName, d.QueryPlanHash)).ToList();
        store.RecordExportedKeys(
            source, exported, [],
            planFetchPendingUpserts: decisions.Where(d => !d.Qualifies).ToList(), nowUtc: now);
        return decisions;
    }

    [Fact]
    public void A_plan_under_both_limits_is_held_back_and_stored_as_pending()
    {
        using var store = StateStore.Open(NewStateDir());

        var d = Assert.Single(RunWindow(store, "store_a", T0, Cand("srv", "h1", 4, 999_999, T0)));

        Assert.False(d.Qualifies);
        Assert.False(d.WasPending);
        var (row, _) = Assert.Single(store.ReadPlanFetchPending("store_a"));
        Assert.Equal((4L, 999_999L), (row.Executions, row.CpuUs));
        Assert.Contains(("srv", "h1"), store.FilterUnknownPlanKeys("store_a", [("srv", "h1")]));
    }

    [Fact]
    public void Executions_alone_qualify_at_five()
    {
        using var store = StateStore.Open(NewStateDir());

        var d = Assert.Single(RunWindow(store, "store_a", T0, Cand("srv", "h1", 5, 10, T0)));

        Assert.True(d.Qualifies);
        Assert.False(d.WasPending);
    }

    [Fact]
    public void Cpu_alone_qualifies_at_one_second()
    {
        using var store = StateStore.Open(NewStateDir());

        var d = Assert.Single(RunWindow(store, "store_a", T0, Cand("srv", "h1", 1, 1_000_000, T0)));

        Assert.True(d.Qualifies);
    }

    [Fact]
    public void Totals_accumulate_across_windows_and_cross_the_threshold_on_the_second()
    {
        using var store = StateStore.Open(NewStateDir());

        var first = Assert.Single(RunWindow(store, "store_a", T0, Cand("srv", "h1", 3, 400_000, T0)));
        Assert.False(first.Qualifies);

        var second = Assert.Single(RunWindow(store, "store_a", T0.AddHours(1), Cand("srv", "h1", 2, 100_000, T0.AddHours(1))));

        Assert.True(second.Qualifies);
        Assert.True(second.WasPending);
        Assert.Equal(5, second.Executions);
        Assert.Equal(500_000, second.CpuUs);
        Assert.Empty(store.ReadPlanFetchPending("store_a")); // exported keys leave pending
        Assert.DoesNotContain(("srv", "h1"), store.FilterUnknownPlanKeys("store_a", [("srv", "h1")]));
    }

    [Fact]
    public void Cpu_accumulates_across_windows_to_qualify()
    {
        using var store = StateStore.Open(NewStateDir());

        RunWindow(store, "store_a", T0, Cand("srv", "h1", 1, 600_000, T0));
        var second = Assert.Single(RunWindow(store, "store_a", T0.AddHours(1), Cand("srv", "h1", 1, 600_000, T0.AddHours(1))));

        Assert.True(second.Qualifies);
        Assert.Equal(2, second.Executions);
        Assert.Equal(1_200_000, second.CpuUs);
    }

    [Fact]
    public void A_deferred_key_that_later_qualifies_carries_the_earliest_first_seen_and_the_latest_last_seen()
    {
        using var store = StateStore.Open(NewStateDir());
        var w1 = T0;
        var w2 = T0.AddHours(1);
        var w3 = T0.AddHours(2);

        RunWindow(store, "store_a", w1, Cand("srv", "h1", 1, 1, w1, w1.AddMinutes(10)));
        RunWindow(store, "store_a", w2, Cand("srv", "h1", 1, 1, w2, w2.AddMinutes(20)));
        var third = Assert.Single(RunWindow(store, "store_a", w3, Cand("srv", "h1", 10, 1, w3.AddMinutes(5), w3.AddMinutes(30))));

        Assert.True(third.Qualifies);
        Assert.Equal(w1, third.FirstSeen);
        Assert.Equal(w3.AddMinutes(30), third.LastSeen);
        Assert.Equal(12, third.Executions);
    }

    [Fact]
    public void A_held_back_key_keeps_its_earliest_first_seen_in_pending()
    {
        using var store = StateStore.Open(NewStateDir());

        RunWindow(store, "store_a", T0, Cand("srv", "h1", 1, 1, T0));
        RunWindow(store, "store_a", T0.AddHours(1), Cand("srv", "h1", 1, 1, T0.AddHours(1)));

        var (row, _) = Assert.Single(store.ReadPlanFetchPending("store_a"));
        Assert.Equal(T0, row.FirstSeen);
        Assert.Equal(T0.AddHours(1).AddMinutes(50), row.LastSeen);
        Assert.Equal(2, row.Executions);
    }

    [Fact]
    public void A_failed_run_that_records_nothing_does_not_count_its_window_twice()
    {
        using var store = StateStore.Open(NewStateDir());
        var candidate = Cand("srv", "h1", 3, 1, T0);

        // Run 1 gates but fails before the state write.
        PlanFetchGate.Apply(store, "store_a", T0, [candidate]);
        // The retry sees the same window again and still starts from zero.
        var retry = Assert.Single(RunWindow(store, "store_a", T0, candidate));

        Assert.Equal(3, retry.Executions);
        Assert.False(retry.Qualifies);
    }

    [Fact]
    public void Pending_rows_not_updated_for_14_days_are_pruned_using_the_injected_clock()
    {
        using var store = StateStore.Open(NewStateDir());
        RunWindow(store, "store_a", T0, Cand("srv", "old", 1, 1, T0), Cand("srv", "newer", 1, 1, T0));
        // "newer" is touched again 10 days later; "old" is not.
        RunWindow(store, "store_a", T0.AddDays(10), Cand("srv", "newer", 1, 1, T0.AddDays(10)));

        Assert.Equal(0, store.PrunePlanFetchPending(T0.AddDays(14))); // exactly 14 days old: kept
        Assert.Equal(1, store.PrunePlanFetchPending(T0.AddDays(14).AddSeconds(1)));

        var (row, _) = Assert.Single(store.ReadPlanFetchPending("store_a"));
        Assert.Equal("newer", row.QueryPlanHash);
    }

    [Fact]
    public void Apply_prunes_stale_rows_so_a_returning_plan_starts_from_zero()
    {
        using var store = StateStore.Open(NewStateDir());
        RunWindow(store, "store_a", T0, Cand("srv", "h1", 4, 1, T0));

        var later = T0.AddDays(15);
        var d = Assert.Single(RunWindow(store, "store_a", later, Cand("srv", "h1", 1, 1, later)));

        Assert.False(d.Qualifies);
        Assert.False(d.WasPending);
        Assert.Equal(1, d.Executions);
        Assert.Equal(later, d.FirstSeen);
    }

    [Fact]
    public void A_null_server_name_is_a_key_of_its_own()
    {
        using var store = StateStore.Open(NewStateDir());

        RunWindow(store, "store_a", T0, Cand(null, "h1", 3, 1, T0), Cand("srv", "h1", 1, 1, T0));
        var second = RunWindow(store, "store_a", T0.AddHours(1), Cand(null, "h1", 2, 1, T0.AddHours(1)), Cand("srv", "h1", 1, 1, T0.AddHours(1)));

        var nullKey = Assert.Single(second, d => d.ServerName is null);
        var named = Assert.Single(second, d => d.ServerName == "srv");
        Assert.True(nullKey.Qualifies);
        Assert.Equal(5, nullKey.Executions);
        Assert.False(named.Qualifies);
        Assert.Equal(2, named.Executions);
        var (pending, _) = Assert.Single(store.ReadPlanFetchPending("store_a"));
        Assert.Equal("srv", pending.ServerName);
    }

    [Fact]
    public void Both_origins_are_gated_and_each_keeps_its_origin_in_pending()
    {
        using var store = StateStore.Open(NewStateDir());

        var decisions = RunWindow(
            store, "store_a", T0,
            Cand("srv", "qs-plan", 2, 1, T0, origin: "query_store"),
            Cand("srv", "qstats-plan", 2, 1, T0, origin: "query_stats"),
            Cand("srv", "busy-qs", 9, 1, T0, origin: "query_store"),
            Cand("srv", "busy-qstats", 9, 1, T0, origin: "query_stats"));

        Assert.Equal(["busy-qs", "busy-qstats"], decisions.Where(d => d.Qualifies).Select(d => d.QueryPlanHash).OrderBy(h => h));
        var pending = store.ReadPlanFetchPending("store_a").Select(p => (p.Row.QueryPlanHash, p.Row.Origin)).ToList();
        Assert.Equal([("qs-plan", "query_store"), ("qstats-plan", "query_stats")], pending);
    }

    [Fact]
    public void Pending_state_is_scoped_to_the_source_store()
    {
        using var store = StateStore.Open(NewStateDir());

        RunWindow(store, "store_a", T0, Cand("srv", "h1", 4, 1, T0));
        var other = Assert.Single(RunWindow(store, "store_b", T0.AddHours(1), Cand("srv", "h1", 1, 1, T0.AddHours(1))));

        Assert.False(other.Qualifies);
        Assert.Equal(1, other.Executions);
    }

    [Fact]
    public void A_qualified_key_whose_content_was_missing_keeps_its_totals_for_the_next_run()
    {
        using var store = StateStore.Open(NewStateDir());
        var d = Assert.Single(PlanFetchGate.Apply(store, "store_a", T0, [Cand("srv", "h1", 6, 1, T0)]));
        Assert.True(d.Qualifies);

        // The exporter found no plan content, so the key is not recorded as exported; its totals are kept instead.
        store.RecordExportedKeys("store_a", [], [], planFetchPendingUpserts: [d], nowUtc: T0);

        var next = Assert.Single(RunWindow(store, "store_a", T0.AddHours(1), Cand("srv", "h1", 0, 0, T0.AddHours(1))));
        Assert.True(next.Qualifies);
        Assert.Equal(6, next.Executions);
        Assert.Equal(T0, next.FirstSeen);
    }

    [Fact]
    public void Pending_totals_taken_from_the_recorder_are_written_by_RecordExportedKeys()
    {
        using var store = StateStore.Open(NewStateDir());
        var held = Assert.Single(PlanFetchGate.Apply(store, "store_a", T0, [Cand("srv", "h1", 1, 1, T0)]));
        PendingKeyRecorder.PendingPlanFetchUpserts["gate-test-store"] = [held];

        store.RecordExportedKeys("gate-test-store", [], []);

        Assert.False(PendingKeyRecorder.PendingPlanFetchUpserts.ContainsKey("gate-test-store"));
        Assert.Single(store.ReadPlanFetchPending("gate-test-store"));
    }

    [Fact]
    public void The_gate_is_off_for_ignore_state_and_for_plan_fetch_all_and_on_by_default()
    {
        ExportOptions Make(bool ignoreState = false, bool planFetchAll = false) => ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, new DateTime(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc),
            ignoreState: ignoreState, planFetchAll: planFetchAll);

        Assert.True(PlanFetchGate.IsEnabled(Make()));
        Assert.False(PlanFetchGate.IsEnabled(Make(ignoreState: true)));
        Assert.False(PlanFetchGate.IsEnabled(Make(planFetchAll: true)));
        Assert.False(Make().PlanFetchAll);
        Assert.True(Make(planFetchAll: true).PlanFetchAll);
    }

    [Fact]
    public void The_threshold_constants_match_the_agreed_rule()
    {
        Assert.Equal(5, PlanFetchThreshold.MinExecutions);
        Assert.Equal(1_000_000, PlanFetchThreshold.MinCpuUs);
        Assert.Equal(14, PlanFetchThreshold.PendingRetentionDays);
    }

    // ---- The DuckDB key queries that feed the gate (they read the window caches, not the store). ----

    [Fact]
    public void The_query_stats_key_query_sums_delta_executions_and_worker_time_per_key()
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        Exec(connection, "CREATE TABLE query_stats_raw (server_name VARCHAR, query_plan_hash VARCHAR, collection_time TIMESTAMP, " +
                         "query_plan_digest BLOB, delta_execution_count BIGINT, delta_worker_time BIGINT)");
        Exec(connection,
            "INSERT INTO query_stats_raw VALUES " +
            "('srv', 'h1', TIMESTAMP '2026-10-01 00:05:00', '\\xAA'::BLOB, 2, 300000)," +
            "('srv', 'h1', TIMESTAMP '2026-10-01 00:20:00', '\\xAB'::BLOB, 3, NULL)," +
            "(NULL, 'h1', TIMESTAMP '2026-10-01 00:05:00', '\\xAA'::BLOB, 1, 1)," +
            "('srv', 'h2', TIMESTAMP '2026-10-01 00:05:00', NULL, 9, 9)");

        using var command = connection.CreateCommand();
        command.CommandText = SqlBuilder.PlanShapeKeyDuckDbSql;
        using var reader = command.ExecuteReader();
        var rows = new List<(string? Server, string Hash, long Executions, long CpuUs)>();
        while (reader.Read())
        {
            rows.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetString(1), reader.GetInt64(5), reader.GetInt64(6)));
        }

        Assert.Equal([("srv", "h1", 5L, 300_000L), (null, "h1", 1L, 1L)], rows.OrderBy(r => r.Server is null).ToList());
    }

    [Fact]
    public void The_query_store_key_query_sums_executions_and_cpu_over_the_latest_snapshot_of_each_interval()
    {
        using var connection = new DuckDBConnection("Data Source=:memory:");
        connection.Open();
        Exec(connection,
            "CREATE TABLE query_store_raw (server_id INTEGER, server_name VARCHAR, database_name VARCHAR, query_id BIGINT, plan_id BIGINT, " +
            "execution_type_desc VARCHAR, replica_role VARCHAR, runtime_stats_interval_id BIGINT, query_plan_hash VARCHAR, " +
            "execution_count BIGINT, avg_cpu_time_us BIGINT, interval_start_time_utc TIMESTAMP, collection_time TIMESTAMP)");
        // Interval 1 was collected twice (counters grew): only the 01:05 snapshot (10 runs, 200 us) counts.
        // Interval 2 is a second interval of the same plan (4 runs, 1000 us).
        Exec(connection,
            "INSERT INTO query_store_raw VALUES " +
            "(1, 'srv', 'db1', 7, 70, 'Regular', 'PRIMARY', 1, 'qh', 4, 100, TIMESTAMP '2026-10-01 00:00:00', TIMESTAMP '2026-10-01 00:59:00')," +
            "(1, 'srv', 'db1', 7, 70, 'Regular', 'PRIMARY', 1, 'qh', 10, 200, TIMESTAMP '2026-10-01 00:00:00', TIMESTAMP '2026-10-01 01:05:00')," +
            "(1, 'srv', 'db1', 7, 70, 'Regular', 'PRIMARY', 2, 'qh', 4, 1000, TIMESTAMP '2026-10-01 01:00:00', TIMESTAMP '2026-10-01 02:05:00')," +
            "(1, 'srv', 'db1', 8, 80, 'Regular', 'PRIMARY', 1, NULL, 99, 99, TIMESTAMP '2026-10-01 00:00:00', TIMESTAMP '2026-10-01 01:05:00')");

        using var command = connection.CreateCommand();
        command.CommandText = SqlBuilder.QueryStorePlanShapeKeyDuckDbSql;
        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("qh", reader.GetString(2));
        Assert.Equal(14L, reader.GetInt64(7));
        Assert.Equal(10L * 200 + 4L * 1000, reader.GetInt64(8));
        Assert.Equal(70L, reader.GetInt64(6)); // the existing example columns are unchanged
        Assert.False(reader.Read());
    }

    private static void Exec(DuckDBConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
