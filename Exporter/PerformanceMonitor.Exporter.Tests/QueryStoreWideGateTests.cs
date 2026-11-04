using System.Text.Json;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>The wide-read gate's decision, clause by clause, plus mode parsing and the manifest fields.</summary>
public class QueryStoreWideGateTests
{
    private static readonly DateTime From = new(2026, 10, 15, 8, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime To = From.AddHours(1);
    private static readonly DateTime Now = From.AddHours(4);

    private static WideCoverageRow Covered(int serverId, DateTime? filledSince = null, DateTime? appliedThrough = null) =>
        new(serverId, filledSince ?? From.AddDays(-30), appliedThrough ?? To.AddHours(3));

    private static WideGateDecision Evaluate(
        IReadOnlyList<WideCoverageRow> coverage,
        IReadOnlyCollection<int>? pending = null, bool tablesExist = true, DateTime? from = null, DateTime? to = null, DateTime? now = null) =>
        QueryStoreWideGate.Evaluate(from ?? From, to ?? To, now ?? Now, tablesExist, coverage, pending ?? []);

    [Fact]
    public void Healthy_servers_allow_the_wide_read()
    {
        var decision = Evaluate([Covered(1), Covered(2)]);
        Assert.True(decision.UseWide);
        Assert.Null(decision.Reason);
    }

    [Fact]
    public void Missing_tables_refuse_with_table_missing()
    {
        var decision = Evaluate([], [], tablesExist: false);
        Assert.False(decision.UseWide);
        Assert.Equal("table_missing", decision.Reason);
    }

    [Fact]
    public void A_window_older_than_8_days_refuses_and_the_boundary_is_exact()
    {
        var from = new DateTime(2026, 10, 8, 8, 0, 0, DateTimeKind.Utc);
        var coverage = new[] { Covered(1, filledSince: from.AddDays(-30), appliedThrough: from.AddDays(30)) };

        // now - 8 days == from: not older, allowed.
        var atBoundary = Evaluate(coverage, from: from, to: from.AddHours(1), now: from.AddDays(8));
        Assert.True(atBoundary.UseWide);

        // One tick past: refused.
        var past = Evaluate(coverage, from: from, to: from.AddHours(1), now: from.AddDays(8).AddTicks(1));
        Assert.False(past.UseWide);
        Assert.Equal("window_too_old", past.Reason);
        Assert.Equal("window_too_old", QueryStoreWideGate.PreCheck(from, from.AddHours(1), from.AddDays(8).AddTicks(1))!.Reason);
        Assert.Null(QueryStoreWideGate.PreCheck(from, from.AddHours(1), from.AddDays(8)));
    }

    [Fact]
    public void A_window_that_is_not_on_whole_hours_refuses()
    {
        var half = Evaluate([Covered(1)], from: From.AddMinutes(30), to: To.AddMinutes(30));
        Assert.Equal("window_not_hour_aligned", half.Reason);
        var oddTo = Evaluate([Covered(1)], to: To.AddMinutes(15));
        Assert.Equal("window_not_hour_aligned", oddTo.Reason);
    }

    [Fact]
    public void No_coverage_row_that_reaches_the_window_refuses_with_no_coverage()
    {
        Assert.Equal("no_coverage", Evaluate([]).Reason);

        // A coverage row whose applied_through is before the window does not vouch for it.
        var stale = Evaluate([Covered(1, appliedThrough: From.AddTicks(-1))]);
        Assert.Equal("no_coverage", stale.Reason);
        Assert.Null(stale.ServerId);
    }

    [Fact]
    public void Filled_since_after_from_refuses_and_equal_is_allowed()
    {
        Assert.True(Evaluate([Covered(1, filledSince: From)]).UseWide);

        var late = Evaluate([Covered(1, filledSince: From.AddTicks(1))]);
        Assert.Equal("filled_late", late.Reason);
        Assert.Equal(1, late.ServerId);
    }

    [Fact]
    public void Applied_through_must_reach_the_last_interval_start_plus_90_minutes()
    {
        Assert.True(Evaluate([Covered(1, appliedThrough: To.AddMinutes(30))]).UseWide);

        var behind = Evaluate([Covered(1, appliedThrough: To.AddMinutes(30).AddTicks(-1))]);
        Assert.Equal("applied_behind", behind.Reason);
        Assert.Equal(1, behind.ServerId);
    }

    private static DateTime At(int day, int hour, int minute) => new(2026, 10, day, hour, minute, 0, DateTimeKind.Utc);

    private static WideGateDecision Lagged(DateTime from, DateTime to, DateTime now, DateTime applied) =>
        QueryStoreWideGate.Evaluate(from, to, now, true, [new WideCoverageRow(12, from.AddDays(-30), applied)], []);

    [Fact]
    public void Lag_2_window_read_at_H20_passes_when_darling_is_under_50_minutes_behind()
    {
        var from = At(22, 6, 0);
        var to = At(22, 7, 0);
        Assert.True(Lagged(from, to, At(22, 8, 20), At(22, 7, 52)).UseWide);
        var behind = Lagged(from, to, At(22, 8, 20), At(22, 7, 29));
        Assert.Equal("applied_behind", behind.Reason);
        Assert.Equal(12, behind.ServerId);
    }

    [Fact]
    public void Lag_3_window_read_at_H20_passes_with_applied_through_at_08_52()
    {
        Assert.True(Lagged(At(22, 6, 0), At(22, 7, 0), At(22, 9, 20), At(22, 8, 52)).UseWide);
    }

    [Fact]
    public void Lag_1_window_read_at_H50_needs_darling_less_than_20_minutes_behind()
    {
        var from = At(22, 7, 0);
        var to = At(22, 8, 0);
        Assert.True(Lagged(from, to, At(22, 8, 50), At(22, 8, 31)).UseWide);
        Assert.Equal("applied_behind", Lagged(from, to, At(22, 8, 50), At(22, 8, 29)).Reason);
    }

    [Fact]
    public void A_two_hour_window_needs_the_same_applied_through_as_its_last_hour()
    {
        var now = At(22, 9, 20);
        Assert.True(Lagged(At(22, 5, 0), At(22, 7, 0), now, At(22, 7, 30)).UseWide);
        Assert.Equal("applied_behind", Lagged(At(22, 5, 0), At(22, 7, 0), now, At(22, 7, 29)).Reason);
        Assert.True(Lagged(At(22, 6, 0), At(22, 7, 0), now, At(22, 7, 30)).UseWide);
    }

    /// <summary>
    /// Keeps the scheduler and the gate from drifting apart: window [h, h+1) is due at h + (lag h + 15 min), lag 1 at
    /// h + 1h45m. A change to either the planner's due times or the gate's margin breaks this test.
    /// </summary>
    [Theory]
    [InlineData(3, 135, 45)]
    [InlineData(2, 75, 45)]
    [InlineData(1, 45, 1)]
    public void The_gate_needed_applied_through_is_before_the_scheduler_due_time(int lagHours, int dueMinutesAfterTo, int minMinutesBefore)
    {
        var from = At(22, 6, 0);
        var to = from.AddHours(1);
        var due = to.AddMinutes(dueMinutesAfterTo);

        // Find the exact needed applied_through by probing the gate in whole minutes.
        var needed = Enumerable.Range(0, 24 * 60).Select(m => from.AddMinutes(m))
            .First(a => Lagged(from, to, due, a).UseWide);
        Assert.True(due - needed >= TimeSpan.FromMinutes(minMinutesBefore), $"lag {lagHours}: needed {needed:HH:mm}, due {due:HH:mm}");
    }

    [Fact]
    public void An_applied_behind_refusal_carries_a_detail_with_the_server_id_and_both_times()
    {
        var from = At(22, 6, 0);
        var decision = Lagged(from, At(22, 7, 0), At(22, 8, 20), At(22, 7, 29));
        Assert.Equal("server 12 applied_through 2026-10-22T07:29Z < needed 2026-10-22T07:30Z", decision.Detail);
        Assert.Null(Lagged(from, At(22, 7, 0), At(22, 8, 20), At(22, 7, 30)).Detail);
        Assert.Null(Evaluate([], [], tablesExist: false).Detail);
    }

    [Fact]
    public async Task The_manifest_carries_the_fallback_detail_only_when_set()
    {
        var with = await WriteAsync(new ManifestTableEntry
        {
            Table = "query_store_stats", OutputFile = "x", RowCount = 1, ByteSize = 1, Sha256 = "a", Columns = [],
            StoreQueryElapsedMs = 1, WriteElapsedMs = 1, MissingContentCount = 0,
            QueryStoreSource = new QueryStoreSourceStats("wide", "raw", "applied_behind", 12, null, 5.0, null, null, null, "server 12 applied_through 2026-10-22T07:29Z < needed 2026-10-22T07:30Z"),
        });
        Assert.Equal("server 12 applied_through 2026-10-22T07:29Z < needed 2026-10-22T07:30Z", with.GetProperty("query_store_fallback_detail").GetString());

        var report = new QueryStoreSourceReport { Mode = QueryStoreSourceMode.Wide };
        Assert.Null(report.ToStats().FallbackDetail);
        var without = await WriteAsync(new ManifestTableEntry
        {
            Table = "query_store_stats", OutputFile = "x", RowCount = 1, ByteSize = 1, Sha256 = "a", Columns = [],
            StoreQueryElapsedMs = 1, WriteElapsedMs = 1, MissingContentCount = 0, QueryStoreSource = report.ToStats(),
        });
        Assert.False(without.TryGetProperty("query_store_fallback_detail", out _));
    }

    [Fact]
    public void A_pending_row_refuses_only_for_a_server_that_is_checked()
    {
        var refused = Evaluate([Covered(1), Covered(2)], pending: [2]);
        Assert.Equal("pending", refused.Reason);
        Assert.Equal(2, refused.ServerId);

        // A pending row for a server that is not active in the window does not matter.
        var ignored = Evaluate([Covered(1), Covered(9, appliedThrough: From.AddTicks(-1))], pending: [9]);
        Assert.True(ignored.UseWide);
    }

    [Fact]
    public void A_pending_result_at_the_cap_refuses_without_a_server_id()
    {
        var pending = Enumerable.Range(1000, QueryStoreWideGate.PendingRowCap).ToArray();
        var decision = Evaluate([Covered(1)], pending: pending);
        Assert.Equal("pending", decision.Reason);
        Assert.Null(decision.ServerId);
    }

    [Fact]
    public void A_removed_server_is_ignored_when_its_applied_through_is_before_the_window_and_refuses_when_inside_it()
    {
        // applied_through before from: the server wrote nothing in the window, so it is not checked.
        var old = Evaluate([Covered(1), Covered(5, appliedThrough: From.AddDays(-3))]);
        Assert.True(old.UseWide);
        var justBefore = Evaluate([Covered(1), Covered(5, appliedThrough: From.AddTicks(-1))]);
        Assert.True(justBefore.UseWide);

        // from <= applied_through < to + 30 min: it may have data in the window that Darling has not applied.
        var atFrom = Evaluate([Covered(1), Covered(5, appliedThrough: From)]);
        Assert.Equal("applied_behind", atFrom.Reason);
        Assert.Equal(5, atFrom.ServerId);
        var inside = Evaluate([Covered(1), Covered(5, appliedThrough: From.AddMinutes(30))]);
        Assert.Equal("applied_behind", inside.Reason);
        Assert.Equal(5, inside.ServerId);
        var justShort = Evaluate([Covered(1), Covered(5, appliedThrough: To.AddMinutes(30).AddTicks(-1))]);
        Assert.Equal(5, justShort.ServerId);
    }

    [Fact]
    public void The_first_failing_clause_wins_in_server_then_clause_order()
    {
        // server 1 is applied_behind, server 2 is filled_late: server 1 comes first.
        var decision = Evaluate([Covered(2, filledSince: From.AddHours(1)), Covered(1, appliedThrough: To)]);
        Assert.Equal("applied_behind", decision.Reason);
        Assert.Equal(1, decision.ServerId);
    }

    [Theory]
    [InlineData("raw", QueryStoreSourceMode.Raw)]
    [InlineData("wide", QueryStoreSourceMode.Wide)]
    [InlineData("compare", QueryStoreSourceMode.Compare)]
    public void Modes_parse(string text, QueryStoreSourceMode expected)
    {
        Assert.True(QueryStoreSourceModes.TryParse(text, out var mode));
        Assert.Equal(expected, mode);
        Assert.Equal(text, QueryStoreSourceModes.Name(mode));
    }

    [Theory]
    [InlineData("")]
    [InlineData("Wide")]
    [InlineData("both")]
    public void Bad_modes_do_not_parse(string text) => Assert.False(QueryStoreSourceModes.TryParse(text, out _));

    private static ExportOptions Options(string? source) => ExportOptions.Validate(
        "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
        "127.0.0.1", 5641, "darling", "darling", null, 600, false, true, Now, queryStoreSource: source);

    [Fact]
    public void Options_default_to_raw_and_parse_the_flag()
    {
        Assert.Equal(QueryStoreSourceMode.Raw, Options(null).QueryStoreSource);
        Assert.Equal(QueryStoreSourceMode.Wide, Options("wide").QueryStoreSource);
        Assert.Equal(QueryStoreSourceMode.Compare, Options("compare").QueryStoreSource);
        var ex = Assert.Throws<ArgumentException>(() => Options("bogus"));
        Assert.Contains("--query-store-source", ex.Message);
    }

    [Fact]
    public void Raw_mode_cache_has_no_source_report_and_wide_mode_has_one()
    {
        var builder = new Npgsql.NpgsqlConnectionStringBuilder();
        using var rawCache = QueryStoreRawWindowCache.Create(builder, Options(null));
        Assert.Null(rawCache.SourceReport);
        using var wideCache = QueryStoreRawWindowCache.Create(builder, Options("wide"));
        Assert.Equal(QueryStoreSourceMode.Wide, wideCache.SourceReport!.Mode);
    }

    [Fact]
    public async Task Manifest_carries_the_source_fields_only_when_a_report_exists()
    {
        var entry = new ManifestTableEntry
        {
            Table = "query_store_stats", OutputFile = "x", RowCount = 1, ByteSize = 1, Sha256 = "a", Columns = [],
            StoreQueryElapsedMs = 1, WriteElapsedMs = 1, MissingContentCount = 0,
            QueryStoreSource = new QueryStoreSourceStats("compare", "raw", "pending", 7, 12.5, 30.0, 1, 2, 3),
        };
        var json = await WriteAsync(entry);
        Assert.Equal("compare", json.GetProperty("query_store_source_mode").GetString());
        Assert.Equal("raw", json.GetProperty("query_store_source").GetString());
        Assert.Equal("pending", json.GetProperty("query_store_fallback_reason").GetString());
        Assert.Equal(7, json.GetProperty("query_store_fallback_server_id").GetInt32());
        Assert.Equal(12.5, json.GetProperty("wide_read_ms").GetDouble());
        Assert.Equal(30.0, json.GetProperty("raw_read_ms").GetDouble());
        Assert.Equal(1, json.GetProperty("compare_only_raw").GetInt64());
        Assert.Equal(2, json.GetProperty("compare_only_wide").GetInt64());
        Assert.Equal(3, json.GetProperty("compare_diff_rows").GetInt64());

        var plain = await WriteAsync(new ManifestTableEntry
        {
            Table = "query_store_stats", OutputFile = "x", RowCount = 1, ByteSize = 1, Sha256 = "a", Columns = [],
            StoreQueryElapsedMs = 1, WriteElapsedMs = 1, MissingContentCount = 0,
        });
        Assert.False(plain.TryGetProperty("query_store_source_mode", out _));
        Assert.False(plain.TryGetProperty("compare_diff_rows", out _));

        var wideOnly = await WriteAsync(new ManifestTableEntry
        {
            Table = "query_store_stats", OutputFile = "x", RowCount = 1, ByteSize = 1, Sha256 = "a", Columns = [],
            StoreQueryElapsedMs = 1, WriteElapsedMs = 1, MissingContentCount = 0,
            QueryStoreSource = new QueryStoreSourceStats("wide", "wide", null, null, 9.0, null, null, null, null),
        });
        Assert.Equal("wide", wideOnly.GetProperty("query_store_source").GetString());
        Assert.False(wideOnly.TryGetProperty("query_store_fallback_reason", out _));
        Assert.False(wideOnly.TryGetProperty("raw_read_ms", out _));
    }

    private static async Task<JsonElement> WriteAsync(ManifestTableEntry entry)
    {
        var manifest = new Manifest
        {
            Store = "test-store", FromUtc = From, ToUtc = To, ExporterVersion = "0.0.0-test", DarlingSchemaVersion = null, Tables = [entry],
        };
        var path = Path.Combine(Path.GetTempPath(), $"manifest-qs-{Guid.NewGuid()}.json");
        try
        {
            await ManifestWriter.WriteAsync(manifest, path, CancellationToken.None);
            using var doc = JsonDocument.Parse(await File.ReadAllTextAsync(path));
            return doc.RootElement.GetProperty("tables")[0].Clone();
        }
        finally
        {
            File.Delete(path);
        }
    }
}
