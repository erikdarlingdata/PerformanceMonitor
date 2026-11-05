using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class ExportOptionsTests
{
    private static readonly DateTime Now = new(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Validate_accepts_a_whole_hour_window()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Equal("store_a", options.Store);
        Assert.Equal(new DateTime(2026, 9, 26, 8, 0, 0, DateTimeKind.Utc), options.FromUtc);
        Assert.Equal(new DateTime(2026, 9, 26, 9, 0, 0, DateTimeKind.Utc), options.ToUtc);
    }

    [Theory]
    [InlineData("2026-09-26T08:00:01Z")]
    [InlineData("2026-09-26T08:15:00Z")]
    [InlineData("2026-09-26T08:00:00.500Z")]
    public void Validate_rejects_a_from_that_is_not_a_whole_hour(string from)
    {
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", from, "2026-09-26T09:00:00Z", "/tmp/out", null,
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now));
    }

    [Fact]
    public void Validate_rejects_to_not_later_than_from()
    {
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T09:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now));
    }

    [Fact]
    public void Validate_rejects_an_unknown_store()
    {
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "Use-3", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now));
    }

    [Fact]
    public void Validate_rejects_a_window_too_close_to_now_without_the_override()
    {
        // Now is 12:00Z; "to" of 11:00Z is only 1 hour back, inside the 2-hour guard.
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T10:00:00Z", "2026-09-26T11:00:00Z", "/tmp/out", null,
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now));
    }

    [Fact]
    public void Validate_allows_a_recent_window_with_the_override()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T10:00:00Z", "2026-09-26T11:00:00Z", "/tmp/out", null,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, true, Now);

        Assert.True(options.AllowOpenWindow);
    }

    [Fact]
    public void Validate_rejects_an_unknown_table()
    {
        Assert.Throws<ArgumentException>(() =>
            ExportOptions.Validate(
                "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "not_a_real_table",
                "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now));
    }

    [Fact]
    public void Validate_defaults_to_all_tables_when_none_given()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", null,
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Equal(ExportOptions.AllTables.Length, options.Tables.Length);
    }

    [Fact]
    public void Validate_parses_a_tables_csv()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-26T08:00:00Z", "2026-09-26T09:00:00Z", "/tmp/out", "perfmon_stats, deadlocks",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, Now);

        Assert.Equal(["perfmon_stats", "deadlocks"], options.Tables);
    }
}
