using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Edge tests for the per-family window guard (docs/hourly-export.md §1.5): the fresh family
/// needs 15 minutes of margin before "now"; the Query Store family (query_store_stats, or
/// plan_shape/query_text under an origin that reads Query Store) needs 2h15m. Each edge is
/// tested exactly at the limit (passes) and one second over (fails).
/// </summary>
public class PerFamilyGuardTests
{
    // Now sits on a whole hour plus 15 minutes so both margins (15m and 2h15m) land exactly
    // on a whole-hour "to", since --to must parse as a whole UTC hour.
    private static readonly DateTime Now = new(2026, 9, 26, 12, 15, 0, DateTimeKind.Utc);

    // --- Fresh family: 15-minute margin ---

    [Fact]
    public void Fresh_family_at_exactly_15_minutes_of_margin_passes()
    {
        // to = 12:00Z is exactly 15 minutes before Now (12:15Z).
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T11:00:00Z", "2026-09-26T12:00:00Z", "/tmp/out", "perfmon_stats",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Equal(new DateTime(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc), options.ToUtc);
    }

    [Fact]
    public void Fresh_family_one_second_inside_15_minutes_of_margin_fails()
    {
        var now = Now.AddSeconds(-1); // to is now 15m - 1s before "now"
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T11:00:00Z", "2026-09-26T12:00:00Z", "/tmp/out", "perfmon_stats",
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, now));
    }

    [Fact]
    public void Fresh_family_dimension_only_run_under_query_stats_origin_uses_15_minute_margin()
    {
        // plan_shape/query_text under --dimension-origin query_stats is a fresh-family read
        // even though it's a dimension table, because it never touches Query Store.
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T11:00:00Z", "2026-09-26T12:00:00Z", "/tmp/out", "plan_shape,query_text",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
            dimensionOrigin: DimensionOrigins.QueryStats);

        Assert.Equal(DimensionOrigins.QueryStats, options.DimensionOrigin);
    }

    // --- Query Store family: 2h15m margin ---

    [Fact]
    public void Query_store_family_at_exactly_2h15m_of_margin_passes()
    {
        // to = 10:00Z is exactly 2h15m before Now (12:15Z).
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T09:00:00Z", "2026-09-26T10:00:00Z", "/tmp/out", "query_store_stats",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Equal(new DateTime(2026, 9, 26, 10, 0, 0, DateTimeKind.Utc), options.ToUtc);
    }

    [Fact]
    public void Query_store_family_one_second_inside_2h15m_of_margin_fails()
    {
        var now = Now.AddSeconds(-1);
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T09:00:00Z", "2026-09-26T10:00:00Z", "/tmp/out", "query_store_stats",
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, now));
    }

    [Fact]
    public void Dimension_only_run_under_query_store_origin_uses_the_2h15m_margin()
    {
        // plan_shape/query_text with no query_store_stats table, but --dimension-origin
        // query_store, still needs the Query Store family's wider margin.
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T11:00:00Z", "2026-09-26T12:00:00Z", "/tmp/out", "plan_shape,query_text",
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
                dimensionOrigin: DimensionOrigins.QueryStore));
    }

    [Fact]
    public void Dimension_only_run_under_query_store_origin_passes_at_the_2h15m_boundary()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T09:00:00Z", "2026-09-26T10:00:00Z", "/tmp/out", "plan_shape,query_text",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now,
            dimensionOrigin: DimensionOrigins.QueryStore);

        Assert.Equal(DimensionOrigins.QueryStore, options.DimensionOrigin);
    }

    [Fact]
    public void Default_both_origin_dimension_only_run_uses_the_2h15m_margin()
    {
        // The default --dimension-origin both still reads Query Store, so a dimension-only
        // run with no explicit origin keeps the wider margin (today's "both" behavior).
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T11:00:00Z", "2026-09-26T12:00:00Z", "/tmp/out", "plan_shape,query_text",
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now));
    }

    [Fact]
    public void AllowOpenWindow_bypasses_the_query_store_family_margin()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T11:00:00Z", "2026-09-26T12:00:00Z", "/tmp/out", "query_store_stats",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, true, Now);

        Assert.True(options.AllowOpenWindow);
    }
}
