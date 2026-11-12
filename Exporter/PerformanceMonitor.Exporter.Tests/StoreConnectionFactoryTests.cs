using System.Linq;
using System.Reflection;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class StoreConnectionFactoryTests
{
    // H9 (store-role plumbing): ApplySessionGuardsAsync's "is this already inherited" decision is
    // pure logic pulled out of the pg_settings SHOW round trip so it can be unit-tested without a
    // live connection. These reflect into the private ToKb helper directly (the connection-touching
    // parts stay covered by tests/guards/test_guards.py's case (c) against a real container).
    private static long InvokeToKb(long value, string? unit)
    {
        var method = typeof(StoreConnectionFactory).GetMethod(
            "ToKb", BindingFlags.NonPublic | BindingFlags.Static)!;
        return (long)method.Invoke(null, new object?[] { value, unit })!;
    }

    [Theory]
    [InlineData(16, "MB", 16384)]
    [InlineData(256, "MB", 262144)]
    [InlineData(1024, "kB", 1024)]
    [InlineData(1, "GB", 1048576)]
    [InlineData(-1, null, -1)]
    public void ToKb_normalizes_pg_settings_units_to_kilobytes(long value, string? unit, long expectedKb)
    {
        Assert.Equal(expectedKb, InvokeToKb(value, unit));
    }

    private static ExportOptions BuildOptions(int statementTimeoutSeconds) => ExportOptions.Validate(
        store: "store_a",
        from: "2026-09-01T00:00:00Z",
        to: "2026-09-01T01:00:00Z",
        outDir: "/tmp/out",
        tablesCsv: "perfmon_stats",
        pgHost: "127.0.0.1",
        pgPort: 5641,
        pgDatabase: "darling",
        pgUser: "darling",
        pgCredentialFile: null,
        statementTimeoutSeconds: statementTimeoutSeconds,
        dryRun: false,
        allowOpenWindow: true,
        nowUtc: new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc));

    [Fact]
    public void BuildConnectionStringBuilder_applies_the_given_statement_timeout_to_the_startup_options_and_command_timeout()
    {
        // H9 item 4: the per-statement timeout (docs/store-read-budget.md §6, "sized to one slice") is applied
        // to every store command via both the startup `statement_timeout` option and Npgsql's CommandTimeout,
        // never a whole-day timeout. The scheduler's hourly path passes 120 (the hourly default).
        var options = BuildOptions(120);
        var builder = StoreConnectionFactory.BuildConnectionStringBuilder(options, "password");

        Assert.Contains("statement_timeout=120000", builder.Options);
        Assert.Equal(120, builder.CommandTimeout);
    }

    [Fact]
    public void BuildConnectionStringBuilder_keeps_lock_timeout_and_application_name_unchanged()
    {
        var options = BuildOptions(120);
        var builder = StoreConnectionFactory.BuildConnectionStringBuilder(options, "password");

        Assert.Contains("lock_timeout=5000", builder.Options);
        Assert.Equal("darling-export", builder.ApplicationName);
    }

    [Fact]
    public void BuildConnectionStringBuilder_scales_the_statement_timeout_with_the_option()
    {
        var options = BuildOptions(1800);
        var builder = StoreConnectionFactory.BuildConnectionStringBuilder(options, "password");

        Assert.Contains("statement_timeout=1800000", builder.Options);
        Assert.Equal(1800, builder.CommandTimeout);
    }
}
