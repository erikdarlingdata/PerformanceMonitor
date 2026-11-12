using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// The per-box Query Store lag (--query-store-lag-hours 1|2|3, default 3): the open-window guard margin and the upper
/// collection_time bound of the Query Store read (docs/hourly-export.md §3.4).
/// </summary>
public class QueryStoreLagTests
{
    private static ExportOptions Validate(string to, DateTime now, int? lag, string tables = "query_store_stats") =>
        ExportOptions.Validate(
            "store_a", "2026-09-26T09:00:00Z", to, "/tmp/out", tables,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, now, queryStoreLagHours: lag);

    [Fact]
    public void Default_lag_is_3_with_today_range()
    {
        var o = Validate("2026-09-26T10:00:00Z", new DateTime(2026, 9, 26, 12, 15, 0, DateTimeKind.Utc), null);
        Assert.Equal(3, o.QueryStoreLagHours);
        Assert.Equal(o.ToUtc.AddHours(2), o.QueryStoreCollectionToUtc);
    }

    [Theory]
    [InlineData(3, 12, 15)] // to 10:00, margin 2h15m
    [InlineData(2, 11, 15)] // margin 1h15m
    [InlineData(1, 10, 45)] // margin 45m
    public void Guard_margin_follows_the_lag_at_the_edge(int lag, int hour, int minute)
    {
        var now = new DateTime(2026, 9, 26, hour, minute, 0, DateTimeKind.Utc);
        var o = Validate("2026-09-26T10:00:00Z", now, lag);
        Assert.Equal(lag, o.QueryStoreLagHours);
        Assert.Throws<ArgumentException>(() => Validate("2026-09-26T10:00:00Z", now.AddSeconds(-1), lag));
    }

    [Fact]
    public void Fresh_family_ignores_the_lag()
    {
        var now = new DateTime(2026, 9, 26, 10, 15, 0, DateTimeKind.Utc);
        var o = Validate("2026-09-26T10:00:00Z", now, 1, "perfmon_stats");
        Assert.Equal(new DateTime(2026, 9, 26, 10, 0, 0, DateTimeKind.Utc), o.ToUtc);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(4)]
    public void Bad_lag_is_rejected(int lag)
    {
        Assert.Throws<ArgumentException>(() =>
            Validate("2026-09-26T10:00:00Z", new DateTime(2026, 9, 26, 20, 0, 0, DateTimeKind.Utc), lag));
    }

    [Theory]
    [InlineData(2, 80)]  // lag 2 at H:20: window end + 1h20
    [InlineData(2, 95)]  // lag 2 at H:35
    [InlineData(1, 50)]  // lag 1 on a 15-minute box at H:50
    [InlineData(1, 80)]  // lag 1 on an hourly box: the next :20
    public void Collection_range_covers_window_start_to_run_time(int lag, int minutesAfterWindowEnd)
    {
        var to = new DateTime(2026, 9, 26, 10, 0, 0, DateTimeKind.Utc);
        var runTime = to.AddMinutes(minutesAfterWindowEnd);
        var o = Validate("2026-09-26T10:00:00Z", runTime, lag);
        Assert.True(runTime <= o.QueryStoreCollectionToUtc);
        Assert.True(o.QueryStoreCollectionToUtc >= o.ToUtc.AddHours(1).AddMinutes(12), "covers the worst measured snapshot lag");
    }
}
