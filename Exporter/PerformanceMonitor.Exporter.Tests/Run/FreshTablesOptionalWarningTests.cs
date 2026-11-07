using System.Globalization;
using PerformanceMonitor.Exporter;
using PerformanceMonitor.Exporter.Run;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>
/// `families.fresh.tables` may name an optional-registry table. The run is not refused, the child still gets the
/// name in `--tables`, the run output says the table takes the optional path, and when the table fails the
/// scheduler reads the optional manifest the child named by that table.
/// </summary>
public class FreshTablesOptionalWarningTests
{
    private static readonly DateTime Now = RunRoot.Utc(2026, 9, 28, 13, 20);

    private static RunSettings Settings(RunRoot r, params string[] freshTables) =>
        r.Settings(tweak: x => x with { Families = x.Families with { FreshTables = [.. freshTables] } });

    private static bool NamesTheOptionalPath(string line, string table) =>
        line.Contains(table, StringComparison.Ordinal) && line.Contains("optional", StringComparison.OrdinalIgnoreCase);

    private static bool IsFreshCall(ChildCall c) => FakeChild.Arg(c, "--dimension-origin") == "query_stats";

    /// <summary>Writes the optional manifest the child writes for this call: named by its optional names, which are
    /// the `--also` names plus the registry names in `--tables`.</summary>
    private static void WriteOptionalManifest(RunRoot r, ChildCall c, string[] optionalNames, string json)
    {
        var from = DateTime.Parse(FakeChild.Arg(c, "--from"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var to = DateTime.Parse(FakeChild.Arg(c, "--to"), CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
        var path = OutputPaths.ManifestFilePath(r.Out, "testh", from, to, optionalNames, dimensionOrigin: null);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, json);
    }

    [Fact]
    public async Task A_failed_optional_table_named_only_in_fresh_tables_is_recorded_by_name()
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild { ExitFor = c => IsFreshCall(c) ? 3 : 0 };
        child.OnCall = c =>
        {
            if (!IsFreshCall(c)) return;
            WriteOptionalManifest(r, c, ["pg_log_events"], """{"tables":[],"failed_optional":[{"table":"pg_log_events","error":"store timeout"}]}""");
        };

        var (exit, _) = await Runs.Go(Settings(r, "deadlocks", "pg_log_events"), child, Now);

        Assert.Equal(RunScheduler.ExitOk, exit);
        var state = r.ReadState();
        Assert.Equal("2026-09-28T13:00:00.0000000Z", state["fresh"]!["last_to"]!.GetValue<string>());
        var failure = Assert.Single(state["optional_failures"]!.AsArray())!;
        Assert.Equal("pg_log_events", failure["table"]!.GetValue<string>());
        Assert.Equal("store timeout", failure["error"]!.GetValue<string>());
    }

    [Fact]
    public async Task Failures_are_read_from_the_manifest_named_by_also_and_fresh_tables_together()
    {
        using var r = new RunRoot(); r.SeedState();
        var settings = r.Settings(tweak: x => x with
        {
            Families = x.Families with { FreshTables = ["deadlocks", "pg_log_events"], OptionalTables = ["plan_correction"] },
        });
        var child = new FakeChild { ExitFor = c => IsFreshCall(c) ? 3 : 0 };
        child.OnCall = c =>
        {
            if (!IsFreshCall(c)) return;
            WriteOptionalManifest(r, c, ["plan_correction", "pg_log_events"],
                """{"tables":[],"failed_optional":[{"table":"plan_correction","error":"connection reset"},{"table":"pg_log_events","error":"store timeout"}]}""");
        };

        var (exit, _) = await Runs.Go(settings, child, Now);

        Assert.Equal(RunScheduler.ExitOk, exit);
        var failures = r.ReadState()["optional_failures"]!.AsArray();
        Assert.Equal(["plan_correction", "pg_log_events"], failures.Select(f => f!["table"]!.GetValue<string>()));
        Assert.Equal(["connection reset", "store timeout"], failures.Select(f => f!["error"]!.GetValue<string>()));
    }

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
