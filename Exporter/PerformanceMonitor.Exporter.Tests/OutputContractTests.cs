using System.Diagnostics;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Exporter;
using PerformanceMonitor.Exporter.Tests.Run;
using Xunit;
using static PerformanceMonitor.Exporter.Tests.Run.RunRoot;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// What the tool prints: the usage text, the windows `run --plan-only` names, the log path a window prints, and how
/// much noise a run writes to stderr and stdout.
/// </summary>
public sealed class OutputContractTests
{
    private static (int Exit, string Out, string Err) RunTool(params string[] args)
    {
        var dll = Path.Combine(AppContext.BaseDirectory, "darling-export.dll");
        var psi = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true };
        psi.ArgumentList.Add(dll);
        foreach (var a in args) { psi.ArgumentList.Add(a); }
        using var p = Process.Start(psi)!;
        var err = p.StandardError.ReadToEndAsync();
        var output = p.StandardOutput.ReadToEnd();
        p.WaitForExit();
        return (p.ExitCode, output, err.GetAwaiter().GetResult());
    }

    [Fact]
    public void No_verb_prints_every_verb_and_every_export_flag_and_exits_1()
    {
        var (exit, _, err) = RunTool();
        Assert.Equal(1, exit);
        foreach (var verb in new[] { "export", "run", "check-config", "describe-tables", "explain-gate" })
        {
            Assert.Contains($"darling-export {verb}", err);
        }

        foreach (var flag in Usage.ExportFlags)
        {
            Assert.Contains(flag.Name, err);
        }

        Assert.Contains("--plan-only", err);
    }

    [Fact]
    public void Help_prints_the_same_usage_on_stdout_and_exits_0()
    {
        var (exit, output, _) = RunTool("--help");
        Assert.Equal(0, exit);
        Assert.Equal(Usage.Text().ReplaceLineEndings(), output.ReplaceLineEndings());
    }

    [Fact]
    public void Every_flag_in_the_usage_text_is_accepted_by_export()
    {
        foreach (var flag in Usage.ExportFlags)
        {
            string[] args = flag.Value is null ? [flag.Name] : [flag.Name, "1"];
            // A bad value or a missing flag is a different error; only an unknown flag is the usage text lying.
            var ex = Record.Exception(() => ExportCommand.Parse(args, DateTime.UtcNow));
            Assert.False(ex is UsageException { Message: var m } && m.StartsWith("Unknown argument", StringComparison.Ordinal), flag.Name);
        }
    }

    [Fact]
    public async Task Plan_only_prints_every_due_window_on_the_console()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Families = x.Families with { FastWindowMinutes = 10 } });
        var (exit, output) = await Runs.Go(s, new NoChildAllowed(), Runs.Pin, planOnly: true);
        Assert.Equal(0, exit);

        var plan = (JsonObject)JsonNode.Parse(File.ReadAllLines(Path.Combine(r.Logs, "plan-only-testh-20260928.jsonl"))[^1])!;
        var windows = 0;
        foreach (var family in plan["families"]!.AsArray())
        {
            foreach (var entry in family!["entries"]!.AsArray())
            {
                windows++;
                Assert.Contains($"{family["name"]}: {entry!["from"]} .. {entry["to"]}", output);
            }
        }

        Assert.True(windows > 0, "the plan names no windows");
    }

    [Fact]
    public async Task Every_log_path_a_window_prints_exists_after_the_run()
    {
        using var r = new RunRoot(); r.SeedState();
        var s = r.Settings(tweak: x => x with { Families = x.Families with { FastWindowMinutes = 10 } });
        var (exit, output) = await Runs.Go(s, new FakeChild(), Runs.Pin);
        Assert.Equal(0, exit);

        var printed = Regex.Matches(output, @"; log: (?<path>.+)\r?$", RegexOptions.Multiline).Select(m => m.Groups["path"].Value.Trim()).ToList();
        Assert.NotEmpty(printed);
        foreach (var path in printed)
        {
            Assert.True(File.Exists(path), path);
        }
    }

    [Fact]
    public void A_failure_prints_the_exception_chain_and_the_stack_frames_only_with_verbose()
    {
        ExporterLog.Reset();
        try
        {
            Exception failure;
            try { throw new InvalidOperationException("outer", new TimeoutException("inner")); }
            catch (Exception ex) { failure = ex; }

            var quiet = ExporterLog.DescribeFailure(failure);
            Assert.Contains("InvalidOperationException: outer", quiet);
            Assert.Contains("TimeoutException: inner", quiet);
            Assert.DoesNotContain("   at ", quiet);

            ExporterLog.Verbose = true;
            Assert.Contains("   at ", ExporterLog.DescribeFailure(failure));
        }
        finally
        {
            ExporterLog.Reset();
        }
    }

    [Fact]
    public void Describe_tables_json_marks_the_tables_the_registry_blocks()
    {
        var (exit, output, _) = RunTool("describe-tables", "--json");
        Assert.Equal(0, exit);
        var tables = JsonNode.Parse(output)!.AsArray().ToDictionary(t => t!["table"]!.GetValue<string>(), t => t!["blocked"]!.GetValue<bool>());
        Assert.True(tables["waiting_tasks"]);
        Assert.True(tables["query_snapshots"]);
        Assert.False(tables["perfmon_stats"]);
        Assert.False(tables["pg_statement_stats"]);
    }

    [Fact]
    public void The_store_connection_turns_GSS_encryption_off()
    {
        var options = ExportOptions.Validate(
            "store_a", "2026-09-01T00:00:00Z", "2026-09-01T01:00:00Z", "/tmp/out", "perfmon_stats", "127.0.0.1", 5641, "darling", "darling", null, 120,
            false, true, new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc));
        Assert.Equal(GssEncryptionMode.Disable, StoreConnectionFactory.BuildConnectionStringBuilder(options, "pw").GssEncryptionMode);
        Assert.Equal(GssEncryptionMode.Disable, StoreConnectionFactory.BuildFromConnectionString(options, "Host=h;Username=u;Password=p;GSS Encryption Mode=Prefer").GssEncryptionMode);
    }
}

/// <summary>
/// A login that cannot set temp_file_limit, on a real PostgreSQL: the warning appears once per process however many
/// connections open, and no DEBUG line appears unless `--verbose` is on. With no Docker the test returns without asserting.
/// </summary>
[Trait("Category", "Slow")]
[Collection("ConsoleError")]
public sealed class SessionGuardNoiseTests : IClassFixture<GenericRawTableEquivalenceTests.GenericPgFixture>
{
    private readonly GenericRawTableEquivalenceTests.GenericPgFixture _pg;

    public SessionGuardNoiseTests(GenericRawTableEquivalenceTests.GenericPgFixture pg) => _pg = pg;

    private async Task<string> OpenConnectionsAsync(int count, bool verbose)
    {
        var made = _pg.Psql("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'plain_reader') THEN CREATE ROLE plain_reader LOGIN PASSWORD 'pw'; END IF; END $$;");
        Assert.True(made.code == 0, made.output);
        var builder = new NpgsqlConnectionStringBuilder(_pg.Builder!.ConnectionString) { Username = "plain_reader", Password = "pw" };

        var original = Console.Error;
        var captured = new StringWriter();
        Console.SetError(captured);
        ExporterLog.Reset();
        ExporterLog.Verbose = verbose;
        try
        {
            for (var i = 0; i < count; i++)
            {
                await using var connection = await StoreConnectionFactory.OpenAsync(builder, CancellationToken.None);
            }
        }
        finally
        {
            Console.SetError(original);
            ExporterLog.Reset();
        }

        return captured.ToString();
    }

    [Fact]
    public async Task The_temp_file_limit_warning_is_written_once_and_no_debug_line_by_default()
    {
        if (_pg.Builder is null) { return; }
        var text = await OpenConnectionsAsync(4, verbose: false);
        Assert.Single(Regex.Matches(text, "could not SET temp_file_limit"));
        Assert.DoesNotContain("DEBUG", text);
    }

    [Fact]
    public async Task Verbose_adds_one_session_guard_line_per_connection()
    {
        if (_pg.Builder is null) { return; }
        var text = await OpenConnectionsAsync(3, verbose: true);
        Assert.Single(Regex.Matches(text, "could not SET temp_file_limit"));
        Assert.Equal(3, Regex.Matches(text, "DEBUG: session_guard applied").Count);
    }
}

[CollectionDefinition("ConsoleError", DisableParallelization = true)]
public sealed class ConsoleErrorCollection;
