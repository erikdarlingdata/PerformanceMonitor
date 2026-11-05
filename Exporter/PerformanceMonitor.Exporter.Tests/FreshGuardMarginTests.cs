using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// --fresh-guard-minutes (2..15): the open-window guard margin for the non-Query-Store family. `run` passes
/// the fresh_fast late-row margin here, so every window the planner calls due satisfies To &lt;= now - margin. Query Store keeps its own margin.
/// </summary>
public class FreshGuardMarginTests
{
    private static readonly DateTime Now = new(2026, 10, 20, 18, 5, 0, DateTimeKind.Utc);

    private static ExportOptions Validate(DateTime to, int? guard, string tables = "collection_log") =>
        ExportOptions.Validate(
            "store_a", "2026-10-20T10:00:00Z", to.ToString("yyyy-MM-ddTHH:mm:ssZ"), "/tmp/out", tables,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now, freshGuardMinutes: guard);

    [Fact]
    public void Margin_5_accepts_to_at_now_minus_5()
    {
        var o = Validate(Now.AddMinutes(-5), 5);
        Assert.Equal(Now.AddMinutes(-5), o.ToUtc);
    }

    [Fact]
    public void Margin_5_rejects_to_at_now_minus_4()
    {
        var ex = Assert.Throws<ArgumentException>(() => Validate(Now.AddMinutes(-4), 5));
        Assert.Contains("now minus 5 minutes", ex.Message);
    }

    [Fact]
    public void No_option_keeps_15_and_rejects_now_minus_5()
    {
        var ex = Assert.Throws<ArgumentException>(() => Validate(Now.AddMinutes(-5), null));
        Assert.Contains("now minus 15 minutes", ex.Message);
        Validate(Now.AddMinutes(-15), null);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(15)]
    public void Edge_values_are_accepted(int guard) => Validate(Now.AddMinutes(-guard), guard);

    [Theory]
    [InlineData(1)]
    [InlineData(16)]
    [InlineData(0)]
    [InlineData(-1)]
    public void Out_of_range_is_an_argument_error(int guard)
    {
        var ex = Assert.Throws<ArgumentException>(() => Validate(Now.AddMinutes(-60), guard));
        Assert.Contains("--fresh-guard-minutes", ex.Message);
    }

    [Fact]
    public void Query_store_family_ignores_the_option()
    {
        // Lag 3 default: margin 2h15m, whole-hour windows. At 12:20, a window ending 11:00 is refused even with
        // --fresh-guard-minutes 2 (the fresh margin would allow it); one ending 10:00 passes on the QS margin.
        var now = new DateTime(2026, 10, 20, 12, 20, 0, DateTimeKind.Utc);
        ExportOptions Qs(string to) => ExportOptions.Validate(
            "store_a", "2026-10-20T09:00:00Z", to, "/tmp/out", "query_store_stats",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, now, freshGuardMinutes: 2);
        var ex = Assert.Throws<ArgumentException>(() => Qs("2026-10-20T11:00:00Z"));
        Assert.Contains("2 hours 15 minutes", ex.Message);
        Assert.Equal(new DateTime(2026, 10, 20, 10, 0, 0, DateTimeKind.Utc), Qs("2026-10-20T10:00:00Z").ToUtc);
    }
}
