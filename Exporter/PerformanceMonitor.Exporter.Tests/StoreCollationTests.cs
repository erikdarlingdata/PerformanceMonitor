using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Npgsql;
using PerformanceMonitor.Exporter.Run;
using PerformanceMonitor.Exporter.Tests.Run;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// A store that is not C-collated is a configuration error with a one-line remedy: `export`, `check-config` and
/// `run --plan-only` exit 2 and print no stack trace. The live tests start a PostgreSQL container with the image's
/// default locale (so its default database is not C) and a second database created with the C collation; with no
/// Docker they return without asserting, as the other live tests do.
/// </summary>
public sealed class StoreCollationTests(StoreCollationTests.NonCStoreFixture store) : IClassFixture<StoreCollationTests.NonCStoreFixture>
{
    public sealed class NonCStoreFixture : IDisposable
    {
        private readonly string _container = "darling-export-collation-" + Guid.NewGuid().ToString("N")[..8];
        public const string CDatabase = "c_store";
        public const string NonCDatabase = "postgres";
        public int Port { get; }
        public bool Available { get; }

        public NonCStoreFixture()
        {
            try
            {
                var run = Sh("docker", $"run -d --rm --name {_container} -e POSTGRES_PASSWORD=pw -p 127.0.0.1::5432 postgres:17");
                if (run.code != 0) { return; }
                var deadline = DateTime.UtcNow.AddSeconds(90);
                var ok = 0;
                while (DateTime.UtcNow < deadline && ok < 3)
                {
                    ok = Psql("SELECT 1").code == 0 ? ok + 1 : 0;
                    Thread.Sleep(1000);
                }
                if (ok < 3) { Dispose(); return; }
                Port = int.Parse(Sh("docker", $"port {_container} 5432/tcp").output.Trim().Split(':')[^1]);
                var create = Psql($"CREATE DATABASE {CDatabase} TEMPLATE template0 ENCODING 'UTF8' LOCALE 'C'");
                Assert.True(create.code == 0, create.output);
                var stub = Psql(StubSchema);
                Assert.True(stub.code == 0, stub.output);
                Available = true;
            }
            catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException)
            {
                Available = false; // no docker CLI
            }
        }

        /// <summary>The two catalog objects `export` reads before it reaches the collation check: the schema version table and the chunk view, empty.</summary>
        private const string StubSchema = """
            CREATE SCHEMA collect;
            CREATE TABLE collect.darling_schema_version (version int NOT NULL);
            INSERT INTO collect.darling_schema_version VALUES (1);
            CREATE SCHEMA timescaledb_information;
            CREATE VIEW timescaledb_information.chunks AS
              SELECT ''::name AS chunk_name, false AS is_compressed, ''::name AS hypertable_schema, ''::name AS hypertable_name,
                     now()::timestamp AS range_start, now()::timestamp AS range_end WHERE false;
            """;

        private (int code, string output) Psql(string sql)
        {
            var psi = new ProcessStartInfo("docker", $"exec -i {_container} psql -U postgres -d postgres -v ON_ERROR_STOP=1 -q")
            {
                RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true,
            };
            using var p = Process.Start(psi)!;
            try
            {
                p.StandardInput.Write(sql);
                p.StandardInput.Close();
            }
            catch (IOException)
            {
                // `docker exec` exited before it read the statement (the container is not accepting sessions yet): the exit code says so.
            }
            var o = p.StandardOutput.ReadToEnd() + p.StandardError.ReadToEnd();
            p.WaitForExit();
            return (p.ExitCode, o);
        }

        private static (int code, string output) Sh(string file, string args)
        {
            var psi = new ProcessStartInfo(file, args) { RedirectStandardOutput = true, RedirectStandardError = true };
            using var p = Process.Start(psi)!;
            var o = p.StandardOutput.ReadToEnd() + p.StandardError.ReadToEnd();
            p.WaitForExit();
            return (p.ExitCode, o);
        }

        public void Dispose()
        {
            try { Sh("docker", $"rm -f {_container}"); } catch (Exception) { /* best effort */ }
        }
    }

    private const string PasswordVariable = "DARLING_EXPORT_COLLATION_TEST_PASSWORD";

    /// <summary>A config for <paramref name="database"/> on the fixture's container (or a closed port when <paramref name="port"/> is given), with the password in an environment variable.</summary>
    private static string Config(RunRoot root, int port, string database) => $$"""
        {
          "config_version": 1,
          "store": { "name": "coll_test", "host": "127.0.0.1", "port": {{port}}, "database": "{{database}}", "user": "postgres",
                     "password": { "source": "env", "name": "{{PasswordVariable}}" } },
          "paths": { "out": {{System.Text.Json.JsonSerializer.Serialize(root.Out)}}, "state": {{System.Text.Json.JsonSerializer.Serialize(root.State)}}, "logs": {{System.Text.Json.JsonSerializer.Serialize(root.Logs)}} }
        }
        """;

    private string WriteConfig(RunRoot root, string database, int? port = null)
    {
        Environment.SetEnvironmentVariable(PasswordVariable, "pw");
        // The default local destination must exist, or check-config and run stop at the destination check before the store.
        Directory.CreateDirectory(Path.Combine(root.Root, "landing"));
        var path = Path.Combine(root.Root, "c.json");
        File.WriteAllText(path, Config(root, port ?? store.Port, database));
        return path;
    }

    private static void AssertOneLineRemedy(string error)
    {
        var line = Assert.Single(error.Split('\n', StringSplitOptions.RemoveEmptyEntries));
        Assert.StartsWith("Config error: store: the store database uses the collation 'en_US.utf8'", line);
        Assert.Contains("LOCALE 'C'", line);
        Assert.Contains("new Darling store on a new volume", line);
        Assert.DoesNotContain(" at ", line);
    }

    [Fact]
    public async Task The_collation_guard_throws_a_config_error_for_a_non_C_database_and_passes_a_C_database()
    {
        if (!store.Available) { return; }
        var password = "pw";
        foreach (var (database, ok) in new[] { (NonCStoreFixture.NonCDatabase, false), (NonCStoreFixture.CDatabase, true) })
        {
            var builder = new NpgsqlConnectionStringBuilder
            {
                Host = "127.0.0.1", Port = store.Port, Username = "postgres", Password = password, Database = database,
            };
            await using var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync();
            if (ok)
            {
                await QueryStoreWindow.AssertCCollationAsync(connection, CancellationToken.None);
            }
            else
            {
                var ex = await Assert.ThrowsAsync<ConfigException>(() => QueryStoreWindow.AssertCCollationAsync(connection, CancellationToken.None));
                Assert.Contains("'en_US.utf8'", ex.Message);
            }
        }
    }

    [Fact]
    public async Task Check_config_exits_2_with_one_line_for_a_non_C_database()
    {
        if (!store.Available) { return; }
        using var root = new RunRoot();
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await CliVerbs.CheckConfigAsync(["check-config", "--config", WriteConfig(root, NonCStoreFixture.NonCDatabase)], output, error, CancellationToken.None);
        Assert.Equal(2, exit);
        AssertOneLineRemedy(error.ToString());
        Assert.Equal("", output.ToString());
    }

    [Fact]
    public async Task Check_config_passes_a_C_database_and_says_so()
    {
        if (!store.Available) { return; }
        using var root = new RunRoot();
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await CliVerbs.CheckConfigAsync(["check-config", "--config", WriteConfig(root, NonCStoreFixture.CDatabase)], output, error, CancellationToken.None);
        Assert.Equal(0, exit);
        Assert.Equal("", error.ToString());
        Assert.Contains("store: collation C", output.ToString());
    }

    [Fact]
    public async Task Plan_only_exits_2_with_one_line_for_a_non_C_database_and_writes_no_plan()
    {
        if (!store.Available) { return; }
        using var root = new RunRoot();
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await RunCommand.MainAsync(["--config", WriteConfig(root, NonCStoreFixture.NonCDatabase), "--plan-only", "--now", "2026-09-28T16:30:00Z"], output, error, CancellationToken.None);
        Assert.Equal(2, exit);
        AssertOneLineRemedy(error.ToString());
        Assert.False(Directory.Exists(root.Logs), "plan-only must write no plan when the store cannot be read");
    }

    [Fact]
    public async Task Plan_only_plans_a_C_database()
    {
        if (!store.Available) { return; }
        using var root = new RunRoot();
        var error = new StringWriter();
        var exit = await RunCommand.MainAsync(["--config", WriteConfig(root, NonCStoreFixture.CDatabase), "--plan-only", "--now", "2026-09-28T16:30:00Z"], new StringWriter(), error, CancellationToken.None);
        Assert.Equal(0, exit);
        Assert.DoesNotContain("collation", error.ToString());
        Assert.True(File.Exists(Path.Combine(root.Logs, "plan-only-coll_test-20260928.jsonl")));
    }

    [Fact]
    public async Task Export_exits_2_with_one_line_and_no_stack_trace_for_a_non_C_database()
    {
        if (!store.Available) { return; }
        using var root = new RunRoot();
        var config = WriteConfig(root, NonCStoreFixture.NonCDatabase);
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await CliVerbs.ExportAsync(
            ["--config", config, "--from", "2026-09-28T10:00:00Z", "--to", "2026-09-28T11:00:00Z", "--tables", "query_stats_hourly"],
            output, error, CancellationToken.None);
        Assert.True(exit == 2, $"exit {exit}; stderr: {error}; stdout: {output}");
        AssertOneLineRemedy(error.ToString());
        Assert.DoesNotContain("   at ", output.ToString());
    }

    [Fact]
    public async Task An_unreachable_store_is_a_warning_in_check_config_and_does_not_fail_it()
    {
        using var root = new RunRoot();
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var closedPort = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        var output = new StringWriter();
        var error = new StringWriter();
        var exit = await CliVerbs.CheckConfigAsync(["check-config", "--config", WriteConfig(root, "darling", closedPort)], output, error, CancellationToken.None);
        Assert.Equal(0, exit);
        var line = Assert.Single(error.ToString().Split('\n', StringSplitOptions.RemoveEmptyEntries));
        Assert.StartsWith("warning: store collation not checked: the store could not be read", line);
        Assert.DoesNotContain("store: collation C", output.ToString());
    }
}
