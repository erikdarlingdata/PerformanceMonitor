using System.Diagnostics;
using System.Text.Json;
using PerformanceMonitor.Exporter.Run;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>
/// A configuration error is found before the first window: `check-config` and `run --plan-only` report a missing or unwritable
/// local destination folder, `run` reports an unresolvable store credential, and a child that refuses its configuration (exit 2)
/// makes the run exit 2. Each is exit 2 with one line.
/// </summary>
public class ConfigErrorsEarlyTests
{
    private static string Json(string s) => JsonSerializer.Serialize(s);

    /// <summary>A config whose credential is a file and whose destination is <paramref name="destination"/> (a JSON object, or null for none).</summary>
    private static string Config(RunRoot r, string credentialFile, string? destination) => $$"""
        {
          "config_version": 1,
          "store": { "name": "cfgearly", "host": "127.0.0.1", "port": 5641, "database": "darling", "user": "darling",
                     "password": { "source": "file", "path": {{Json(credentialFile)}} } },
          "paths": { "out": {{Json(r.Out)}}, "state": {{Json(r.State)}}, "logs": {{Json(r.Logs)}} }
          {{(destination is null ? "" : ", \"destination\": " + destination)}}
        }
        """;

    private static string Local(string path, string prefix = "") =>
        $$"""{ "type": "local", "path": {{Json(path)}}{{(prefix.Length == 0 ? "" : ", \"prefix\": " + Json(prefix))}} }""";

    private static ExporterConfig Parse(string json) => ConfigLoader.Parse(json, isWindows: false);

    private static string WriteFile(RunRoot r, string name, string content)
    {
        var path = Path.Combine(r.Root, name);
        File.WriteAllText(path, content);
        return path;
    }

    /// <summary>A credential file the resolver accepts without a permissions warning.</summary>
    private static string Credential(RunRoot r)
    {
        var path = WriteFile(r, "pw.txt", "lab-value");
        if (!OperatingSystem.IsWindows()) File.SetUnixFileMode(path, UnixFileMode.UserRead | UnixFileMode.UserWrite);
        return path;
    }

    private static void AssertOneLine(string text)
    {
        var lines = text.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        Assert.Single(lines);
    }

    // ---- DestinationCheck ----

    [Fact]
    public void A_missing_local_destination_folder_is_a_config_error_that_names_the_key_and_the_folder()
    {
        using var r = new RunRoot();
        var missing = Path.Combine(r.Root, "landing-missing");
        var ex = Assert.Throws<ConfigException>(() => DestinationCheck.Assert(Parse(Config(r, "unused", Local(missing)))));
        Assert.StartsWith("destination.path:", ex.Message);
        Assert.Contains(missing, ex.Message);
        Assert.Contains("does not exist", ex.Message);
        Assert.DoesNotContain('\n', ex.Message);
        Assert.False(Directory.Exists(missing), "the check must not create the folder");
    }

    [Fact]
    public void An_existing_writable_destination_folder_passes_and_the_check_leaves_nothing_behind()
    {
        using var r = new RunRoot();
        var landing = Path.Combine(r.Root, "landing");
        Directory.CreateDirectory(landing);
        DestinationCheck.Assert(Parse(Config(r, "unused", Local(landing))));
        Assert.Empty(Directory.GetFileSystemEntries(landing));
    }

    [Fact]
    public void The_prefix_is_part_of_the_folder_that_is_checked()
    {
        using var r = new RunRoot();
        var landing = Path.Combine(r.Root, "landing");
        Directory.CreateDirectory(landing);
        var ex = Assert.Throws<ConfigException>(() => DestinationCheck.Assert(Parse(Config(r, "unused", Local(landing, "hourly")))));
        Assert.Contains(Path.Combine(landing, "hourly"), ex.Message);

        Directory.CreateDirectory(Path.Combine(landing, "hourly"));
        DestinationCheck.Assert(Parse(Config(r, "unused", Local(landing, "hourly"))));
    }

    [Fact]
    public void A_folder_the_exporter_cannot_write_to_is_a_config_error_that_names_the_folder()
    {
        using var r = new RunRoot();
        var landing = Path.Combine(r.Root, "landing");
        Directory.CreateDirectory(landing);
        foreach (Exception denied in new Exception[] { new UnauthorizedAccessException("Access to the path is denied."), new IOException("Read-only file system.") })
        {
            var ex = Assert.Throws<ConfigException>(() => DestinationCheck.Assert(Parse(Config(r, "unused", Local(landing))), _ => throw denied));
            Assert.StartsWith("destination.path:", ex.Message);
            Assert.Contains(landing, ex.Message);
            Assert.Contains("not writable", ex.Message);
            Assert.DoesNotContain('\n', ex.Message);
        }
    }

    [Fact]
    public void With_no_destination_the_landing_folder_next_to_paths_out_is_checked()
    {
        using var r = new RunRoot();
        var landing = Path.Combine(r.Root, "landing");
        var ex = Assert.Throws<ConfigException>(() => DestinationCheck.Assert(Parse(Config(r, "unused", destination: null))));
        Assert.StartsWith("paths.out:", ex.Message);
        Assert.Contains(landing, ex.Message);

        Directory.CreateDirectory(landing);
        DestinationCheck.Assert(Parse(Config(r, "unused", destination: null)));
    }

    [Fact]
    public void A_destination_that_is_not_a_local_folder_has_nothing_to_check()
    {
        using var r = new RunRoot();
        DestinationCheck.Assert(Parse(Config(r, "unused", """{ "type": "s3", "bucket": "example-bucket", "prefix": "landing", "region": "us-east-1" }""")));
    }

    // ---- run: the credential and the destination, before the first window ----

    private static async Task<(int Exit, string Output, string Error, FakeChild Child)> RunConfig(string cfg, bool planOnly = false, FakeChild? child = null)
    {
        child ??= new FakeChild();
        var o = new StringWriter { NewLine = "\n" }; var e = new StringWriter { NewLine = "\n" };
        string[] args = planOnly ? ["--config", cfg, "--plan-only", "--now", "2026-09-28T16:30:00Z"] : ["--config", cfg, "--now", "2026-09-28T16:30:00Z"];
        var exit = await RunCommand.MainAsync(args, o, e, CancellationToken.None, child);
        return (exit, o.ToString(), e.ToString(), child);
    }

    [Fact]
    public async Task Run_with_an_unresolvable_store_credential_exits_2_with_one_line_and_starts_no_window()
    {
        using var r = new RunRoot(); r.SeedState("cfgearly");
        var landing = Path.Combine(r.Root, "landing"); Directory.CreateDirectory(landing);
        var cfg = WriteFile(r, "c.json", Config(r, Path.Combine(r.Root, "no-such-credential"), Local(landing)));
        var before = File.ReadAllBytes(r.StatePath);

        var (exit, output, error, child) = await RunConfig(cfg);

        Assert.Equal(2, exit);
        Assert.Empty(child.Calls);
        AssertOneLine(error);
        Assert.StartsWith("Config error: store.password", error);
        Assert.Contains("no-such-credential", error);
        Assert.Equal(before, File.ReadAllBytes(r.StatePath));
        Assert.DoesNotContain("export failed", output);
    }

    [Fact]
    public async Task Run_with_a_missing_destination_folder_exits_2_with_one_line_and_starts_no_window()
    {
        using var r = new RunRoot(); r.SeedState("cfgearly");
        var credential = Credential(r);
        var missing = Path.Combine(r.Root, "landing-missing");
        var cfg = WriteFile(r, "c.json", Config(r, credential, Local(missing)));

        var (exit, _, error, child) = await RunConfig(cfg);

        Assert.Equal(2, exit);
        Assert.Empty(child.Calls);
        AssertOneLine(error);
        Assert.StartsWith("Config error: destination.path:", error);
        Assert.Contains(missing, error);
    }

    [Fact]
    public async Task Plan_only_with_a_missing_destination_folder_exits_2_and_writes_no_plan()
    {
        using var r = new RunRoot();
        var missing = Path.Combine(r.Root, "landing-missing");
        var cfg = WriteFile(r, "c.json", Config(r, Path.Combine(r.Root, "no-such-credential"), Local(missing)));

        var (exit, output, error, _) = await RunConfig(cfg, planOnly: true);

        Assert.Equal(2, exit);
        AssertOneLine(error);
        Assert.Contains(missing, error);
        Assert.Empty(output);
        Assert.False(Directory.Exists(r.Logs), "a refused plan writes no plan-only log");
    }

    [Fact]
    public async Task Plan_only_does_not_resolve_the_credential()
    {
        using var r = new RunRoot();
        var landing = Path.Combine(r.Root, "landing"); Directory.CreateDirectory(landing);
        var cfg = WriteFile(r, "c.json", Config(r, Path.Combine(r.Root, "no-such-credential"), Local(landing)));
        Assert.Equal(0, (await RunConfig(cfg, planOnly: true)).Exit);
    }

    [Fact]
    public async Task Run_with_a_good_credential_and_destination_starts_its_windows()
    {
        using var r = new RunRoot(); r.SeedState("cfgearly");
        var credential = Credential(r);
        var landing = Path.Combine(r.Root, "landing"); Directory.CreateDirectory(landing);
        var cfg = WriteFile(r, "c.json", Config(r, credential, Local(landing)));

        var (exit, _, error, child) = await RunConfig(cfg);

        Assert.Equal(0, exit);
        Assert.NotEmpty(child.Calls);
        Assert.Empty(error);
    }

    // ---- run: a child's exit 2 passes through ----

    [Theory]
    [InlineData(2, 2)]
    [InlineData(1, 1)]
    public async Task A_window_that_exits_2_makes_the_run_exit_2_and_any_other_failure_stays_1(int childExit, int runExit)
    {
        using var r = new RunRoot(); r.SeedState();
        var child = new FakeChild { ExitFor = _ => childExit };

        var (exit, output) = await Runs.Go(r.Settings(), child, Runs.Pin);

        Assert.Equal(runExit, exit);
        Assert.Contains("export failed", output);
        Assert.Contains($" end exit={runExit} ", File.ReadAllText(Path.Combine(r.Logs, "export-testh-20260928.log")));
    }

    // ---- check-config, as the real executable ----

    [Fact]
    public async Task Check_config_with_a_missing_destination_folder_exits_2_with_one_line_naming_the_folder()
    {
        using var r = new RunRoot();
        var credential = Credential(r);
        var missing = Path.Combine(r.Root, "landing-missing");
        var cfg = WriteFile(r, "c.json", Config(r, credential, Local(missing)));

        var (exit, stdout, stderr) = await Exe("check-config", "--config", cfg);

        Assert.Equal(2, exit);
        AssertOneLine(stderr);
        Assert.StartsWith("Config error: destination.path:", stderr);
        Assert.Contains(missing, stderr);
        Assert.DoesNotContain("   at ", stderr);
        Assert.Empty(stdout);
    }

    [Fact]
    public async Task Check_config_with_an_existing_destination_folder_prints_the_config_and_exits_0()
    {
        using var r = new RunRoot();
        var credential = Credential(r);
        var landing = Path.Combine(r.Root, "landing"); Directory.CreateDirectory(landing);
        var cfg = WriteFile(r, "c.json", Config(r, credential, Local(landing)));

        var (exit, stdout, _) = await Exe("check-config", "--config", cfg);

        Assert.Equal(0, exit);
        Assert.Contains("cfgearly", stdout);
    }

    // ---- check-config, run and run --plan-only refuse the same destinations with the same line ----

    public static TheoryData<string, string> RefusedDestinations => new()
    {
        { """{ "type": "azure_blob", "account_url": "https://a.blob.core.windows.net", "container": "exports", "hive_store_folder": true, "auth": { "method": "default_credential" } }""", "destination.hive_store_folder: run does not support it for now" },
        { """{ "type": "s3", "bucket": "b", "endpoint_url": "http://127.0.0.1:9000", "hive_store_folder": true }""", "destination.hive_store_folder: run does not support it for now" },
        { """{ "type": "s3", "bucket": "b", "credentials": { "access_key_id_env": "LAB_AK", "secret_access_key": "LAB_SK" } }""", "destination.credentials has an unknown field 'secret_access_key'" },
        { """{ "type": "s3", "bucket": "b", "checksum": "md5" }""", "destination.checksum must be 'sha256' or 'none', got 'md5'" },
        { """{ "type": "azure_blob", "container": "exports", "auth": { "method": "default_credential" } }""", "destination.account_url is required unless auth.method is 'connection_string_env'" },
        { """{ "type": "azure_blob", "account_url": "https://a.blob.core.windows.net", "container": "exports", "auth": { "method": "shared_key" } }""", "destination.auth.method must be 'default_credential', 'connection_string_env' or 'sas_url_env', got 'shared_key'" },
        { """{ "type": "s3", "bucket": "b", "regoin": "us-east-1" }""", "destination has an unknown field 'regoin'" },
        { """{ "type": "local", "path": "/srv/landing", "prefx": "hourly" }""", "destination has an unknown field 'prefx'" },
        { """{ "type": "ftp", "host": "h" }""", "destination.type must be 'local', 's3' or 'azure_blob', got 'ftp'" },
    };

    [Theory]
    [MemberData(nameof(RefusedDestinations))]
    public async Task A_destination_run_refuses_is_refused_by_check_config_and_plan_only_with_the_same_line(string destination, string message)
    {
        using var r = new RunRoot(); r.SeedState("cfgearly");
        var cfg = WriteFile(r, "c.json", Config(r, Credential(r), destination));
        var expected = $"Config error: {message}";

        var run = await RunConfig(cfg);
        var plan = await RunConfig(cfg, planOnly: true);
        var o = new StringWriter { NewLine = "\n" }; var e = new StringWriter { NewLine = "\n" };
        var check = await CliVerbs.CheckConfigAsync(["check-config", "--config", cfg], o, e, CancellationToken.None);

        Assert.Equal((2, expected), (run.Exit, run.Error.Trim()));
        Assert.Empty(run.Child.Calls);
        Assert.Equal((2, expected), (plan.Exit, plan.Error.Trim()));
        Assert.Equal((2, expected), (check, e.ToString().Trim()));
        Assert.Empty(o.ToString());
    }

    private static async Task<(int Exit, string Stdout, string Stderr)> Exe(params string[] args)
    {
        var exe = Path.Combine(AppContext.BaseDirectory, OperatingSystem.IsWindows() ? "darling-export.exe" : "darling-export");
        Assert.True(File.Exists(exe), "the exporter was not built next to the tests: " + exe);
        var psi = new ProcessStartInfo(exe) { RedirectStandardOutput = true, RedirectStandardError = true };
        foreach (var a in args) psi.ArgumentList.Add(a);
        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        await p.WaitForExitAsync();
        return (p.ExitCode, await stdout, await stderr);
    }
}
