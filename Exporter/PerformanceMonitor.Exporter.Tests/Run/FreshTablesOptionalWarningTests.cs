using PerformanceMonitor.Exporter.Run;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>
/// `families.fresh.tables` may name an optional-registry table. The run is not refused, the child still gets the
/// name in `--tables`, and the run output says the table takes the optional path.
/// </summary>
public class FreshTablesOptionalWarningTests
{
    private static readonly DateTime Now = RunRoot.Utc(2026, 9, 28, 13, 20);

    private static RunSettings Settings(RunRoot r, params string[] freshTables) =>
        r.Settings(tweak: x => x with { Families = x.Families with { FreshTables = [.. freshTables] } });

    private static bool NamesTheOptionalPath(string line, string table) =>
        line.Contains(table, StringComparison.Ordinal) && line.Contains("optional", StringComparison.OrdinalIgnoreCase);

    [Fact]
    public async Task Run_warns_about_an_optional_table_in_fresh_tables_and_still_passes_it_to_the_child()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild();

        var (exit, output) = await Runs.Go(Settings(r, "deadlocks", "pg_log_events"), child, Now);

        Assert.Equal(RunScheduler.ExitOk, exit);
        Assert.Contains(output.Split('\n'), line => NamesTheOptionalPath(line, "pg_log_events"));
        Assert.Contains(child.Calls, c => FakeChild.Arg(c, "--tables").Split(',').Contains("pg_log_events"));
    }

    [Fact]
    public async Task Plan_only_warns_the_same_way()
    {
        using var r = new RunRoot(); r.SeedState();

        var (exit, output) = await Runs.Go(Settings(r, "deadlocks", "pg_log_events"), new NoChildAllowed(), Now, planOnly: true);

        Assert.Equal(RunScheduler.ExitOk, exit);
        Assert.Contains(output.Split('\n'), line => NamesTheOptionalPath(line, "pg_log_events"));
    }

    [Fact]
    public async Task A_core_only_fresh_list_gets_no_such_warning()
    {
        using var r = new RunRoot(); r.SeedState();

        var (exit, output) = await Runs.Go(Settings(r, "deadlocks", "wait_stats"), new FakeChild(), Now);

        Assert.Equal(RunScheduler.ExitOk, exit);
        Assert.DoesNotContain(output.Split('\n'), line => NamesTheOptionalPath(line, "deadlocks") || NamesTheOptionalPath(line, "wait_stats"));
    }
}
