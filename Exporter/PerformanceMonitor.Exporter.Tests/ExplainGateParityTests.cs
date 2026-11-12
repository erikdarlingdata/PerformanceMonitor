using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// `explain-gate --tables` accepts an optional-registry name and plans the statement the optional runner sends for
/// it: the same builder, the same table, the same window predicate, with the window bound as literals.
/// </summary>
public class ExplainGateParityTests
{
    private static readonly DateTime From = new(2026, 10, 22, 11, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void The_gate_plans_the_statement_the_optional_runner_sends_for_pg_log_events()
    {
        var table = OptionalTables.TryGet("pg_log_events");
        Assert.NotNull(table);

        var script = OptionalGateScript.Build([table!], From, From.AddHours(1), "store_a");

        // The runner projects the discovered columns through the table's policy; for the two columns the gate
        // projects (both on pg_log_events' keep-only list) that is the gate's statement with the window as literals.
        var discovered = new List<DiscoveredColumn>
        {
            new("collection_time", "timestamp without time zone", "timestamp"),
            new("server_name", "text", "text"),
        };
        var runnerStatement = SqlBuilder.BuildOptionalSelect(table!, discovered, ColumnPolicies.For(table!.Name))
            .Replace("@from", "'2026-10-22T11:00:00Z'", StringComparison.Ordinal)
            .Replace("@to", "'2026-10-22T12:00:00Z'", StringComparison.Ordinal);

        Assert.Contains(runnerStatement, script, StringComparison.Ordinal);
    }

    [Fact]
    public void The_gate_and_the_export_accept_the_same_optional_name_in_tables()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-10-22T11:00:00Z", "2026-10-22T12:00:00Z", "/tmp/out", "pg_log_events",
            "127.0.0.1", 5641, "darling", "darling", null, 600, false, false, From.AddHours(2));

        Assert.Equal(["pg_log_events"], options.Tables);
        Assert.NotNull(OptionalTables.TryGet(options.Tables[0]));
    }
}
