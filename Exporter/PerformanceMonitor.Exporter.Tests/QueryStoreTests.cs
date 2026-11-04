using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class QueryStoreTests
{
    private static readonly DateTime Now = new(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void TimeColumn_for_query_store_stats_is_interval_start_time_utc()
    {
        Assert.Equal("interval_start_time_utc", SqlBuilder.TimeColumn(QueryStoreTables.QueryStoreStats));
    }

    [Fact]
    public void Validate_uses_a_3_hour_guard_when_query_store_stats_is_among_the_tables()
    {
        // Now is 12:00Z; "to" of 09:30Z... use whole hour 09:00Z is 3h back, exactly at the
        // Query Store boundary, so it must be rejected without the override (guard is a
        // strict "later than now minus N hours" check).
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T08:00:00Z", "2026-09-26T10:00:00Z", "/tmp/out", "query_store_stats",
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now));
    }

    [Fact]
    public void Validate_allows_a_2_hour_window_for_non_query_store_tables()
    {
        // 10:00Z is 2 hours before Now (12:00Z); the plain 2-hour guard allows this for a
        // table set that does not include query_store_stats.
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T09:00:00Z", "2026-09-26T10:00:00Z", "/tmp/out", "perfmon_stats",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Equal("store_a", options.Store);
    }

    [Fact]
    public void Merge_prefers_the_query_stats_row_when_both_sources_have_the_same_key()
    {
        var queryStats = new List<OriginRow> { new("srv1|planA", DimensionOrigin.QueryStats, ["from-query-stats"]) };
        var queryStore = new List<OriginRow>
        {
            new("srv1|planA", DimensionOrigin.QueryStore, ["from-query-store"]),
            new("srv1|planB", DimensionOrigin.QueryStore, ["only-in-query-store"]),
        };

        var merged = DimensionKeyMerge.Merge(queryStats, queryStore);

        Assert.Equal(2, merged.Count);
        Assert.Equal("from-query-stats", merged.Single(r => r.Key == "srv1|planA").Values[0]);
        Assert.Equal(DimensionOrigin.QueryStats, merged.Single(r => r.Key == "srv1|planA").Origin);
        Assert.Equal(DimensionOrigin.QueryStore, merged.Single(r => r.Key == "srv1|planB").Origin);
    }
}
