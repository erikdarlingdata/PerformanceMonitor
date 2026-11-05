using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Config;

public class ExportCommandTests
{
    private static readonly DateTime Now = new(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc);

    /// <summary>A script-built argv with every optional flag present.</summary>
    private static readonly string[] ScriptArgv =
    [
        "--store", "store_a", "--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--out", @"C:\exporter\out",
        "--statement-timeout-seconds", "120", "--duckdb-memory-limit", "4GB", "--upload", "s3://bucket/landing",
        "--tables", "perfmon_stats,wait_stats", "--dimension-origin", "query_stats", "--also", "database_size_stats",
        "--query-store-lag-hours", "2", "--fresh-guard-minutes", "5",
        "--pg-user", "export_user", "--pg-credential-file", @"C:\ProgramData\x\pg-export-credential.dpapi",
        "--ignore-state", "--procedure-plan-reuse", "on", "--query-store-source", "wide",
        "--aws-region", "us-east-1", "--state-dir", @"C:\exporter\state", "--application-name", "box-exporter",
    ];

    [Fact]
    public void Every_flag_a_script_passes_still_parses_to_the_same_options()
    {
        // --dimension-origin with a whole-hour window and --tables that are all hourly-capable: use a table set the
        // validator accepts together with --also.
        var args = ScriptArgv.ToArray();
        var inv = ExportCommand.Parse(args, Now);
        var o = inv.Options;

        Assert.Equal("store_a", o.Store);
        Assert.Equal(new DateTime(2026, 9, 1, 0, 0, 0, DateTimeKind.Utc), o.FromUtc);
        Assert.Equal(new DateTime(2026, 9, 1, 1, 0, 0, DateTimeKind.Utc), o.ToUtc);
        Assert.Equal(@"C:\exporter\out", o.OutDir);
        Assert.Equal(120, o.StatementTimeoutSeconds);
        Assert.Equal("4GB", inv.DuckDbMemoryLimit);
        Assert.Equal(DuckDbSession.DefaultThreads, inv.DuckDbThreads);
        Assert.Equal("s3://bucket/landing", o.Upload);
        Assert.Equal(["perfmon_stats", "wait_stats"], o.Tables);
        Assert.Equal("query_stats", o.DimensionOrigin);
        Assert.Equal(["database_size_stats"], o.Also);
        Assert.Equal(2, o.QueryStoreLagHours);
        Assert.Equal("export_user", o.PgUser);
        Assert.Equal(@"C:\ProgramData\x\pg-export-credential.dpapi", o.PgCredentialFile);
        Assert.True(o.IgnoreState);
        Assert.Equal(ProcedurePlanReuseMode.On, o.ProcedurePlanReuse);
        Assert.Equal(QueryStoreSourceMode.Wide, o.QueryStoreSource);

        // The three values the boxes now pass explicitly.
        Assert.Equal(@"C:\exporter\state", o.StateDir);
        Assert.Equal("us-east-1", o.AwsRegion);
        Assert.Equal("box-exporter", o.ApplicationName);
        Assert.Null(o.Credential);
        Assert.Equal("127.0.0.1", o.PgHost);
        Assert.Equal(5641, o.PgPort);
        Assert.Equal("darling", o.PgDatabase);
        Assert.False(o.DryRun);
        Assert.False(o.AllowCompressed);
    }

    [Fact]
    public void Fresh_guard_minutes_flag_still_sets_the_open_window_margin()
    {
        // 15 min margin by default; with 5 a window ending 10 min ago is accepted. Proves the flag is wired, not dropped.
        string[] withFlag = ["--store", "store_a", "--from", "2026-09-01T23:40:00Z", "--to", "2026-09-01T23:50:00Z", "--out", "/o", "--tables", "wait_stats", "--state-dir", "/s", "--fresh-guard-minutes", "5"];
        var now = new DateTime(2026, 9, 2, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(new DateTime(2026, 9, 1, 23, 50, 0, DateTimeKind.Utc), ExportCommand.Parse(withFlag, now).Options.ToUtc);
        Assert.Throws<ArgumentException>(() => ExportCommand.Parse(withFlag[..^2], now));
    }

    [Fact]
    public void The_manual_flags_still_parse()
    {
        string[] argv =
        [
            "--store", "store_pg", "--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--out", "/o", "--tables", "wait_stats",
            "--pg-host", "10.0.0.1", "--pg-port", "6000", "--pg-database", "db", "--aws-region", "eu-west-1", "--state-dir", "/state",
            "--duckdb-threads", "4", "--dry-run", "--allow-open-window", "--allow-compressed", "--pg-text-fetch-all", "--plan-fetch-all",
        ];
        var inv = ExportCommand.Parse(argv, Now);
        var o = inv.Options;
        Assert.Equal("10.0.0.1", o.PgHost);
        Assert.Equal(6000, o.PgPort);
        Assert.Equal("db", o.PgDatabase);
        Assert.Equal("eu-west-1", o.AwsRegion);
        Assert.Equal("/state", o.StateDir);
        Assert.Equal(4, inv.DuckDbThreads);
        Assert.True(o.DryRun && o.AllowOpenWindow && o.AllowCompressed && o.PgTextFetchAll && o.PlanFetchAll);
    }

    [Fact]
    public void An_unknown_flag_or_a_flag_with_no_value_is_a_usage_error()
    {
        Assert.Throws<UsageException>(() => ExportCommand.Parse(["--bogus"], Now));
        Assert.Throws<UsageException>(() => ExportCommand.Parse(["--store"], Now));
        Assert.Throws<UsageException>(() => ExportCommand.Parse(["--pg-port", "x"], Now));
    }

    private static ExporterConfig Config(string extraStore = "", string? families = null)
    {
        var json = $$"""
            { "config_version": 1,
              "store": { "name": "store_a", "host": "db.internal", "port": 6000, "database": "d", "user": "u",
                         "password": { "source": "env", "name": "X_PW" }, "application_name": "cfg-app", "statement_timeout_seconds": 90 {{extraStore}} },
              "paths": { "out": "/cfg/out", "state": "/cfg/state", "logs": "/cfg/logs" },
              "families": { "query_store": { "lag_hours": 2, "source": "wide" }, "fast": { "late_row_margin_minutes": 10 } },
              "export": { "duckdb_memory_limit": "2GB", "duckdb_threads": 3, "procedure_plan_reuse": "off" } }
            """;
        return ConfigLoader.Parse(json, isWindows: false);
    }

    [Fact]
    public void A_config_supplies_the_defaults()
    {
        var inv = ExportCommand.Parse(["--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--tables", "wait_stats"], Now, Config());
        var o = inv.Options;
        Assert.Equal("store_a", o.Store);
        Assert.Equal("/cfg/out", o.OutDir);
        Assert.Equal("/cfg/state", o.StateDir);
        Assert.Equal("db.internal", o.PgHost);
        Assert.Equal(6000, o.PgPort);
        Assert.Equal("d", o.PgDatabase);
        Assert.Equal("u", o.PgUser);
        Assert.Equal(90, o.StatementTimeoutSeconds);
        Assert.Equal("cfg-app", o.ApplicationName);
        Assert.Equal("env", o.Credential!.Source);
        Assert.Equal(2, o.QueryStoreLagHours);
        Assert.Equal(QueryStoreSourceMode.Wide, o.QueryStoreSource);
        Assert.Equal(ProcedurePlanReuseMode.Off, o.ProcedurePlanReuse);
        Assert.Equal("2GB", inv.DuckDbMemoryLimit);
        Assert.Equal(3, inv.DuckDbThreads);
    }

    [Fact]
    public void A_flag_overrides_the_config()
    {
        var inv = ExportCommand.Parse(
            ["--store", "other", "--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--tables", "wait_stats",
             "--out", "/x", "--state-dir", "/y", "--statement-timeout-seconds", "30", "--query-store-lag-hours", "1",
             "--query-store-source", "raw", "--procedure-plan-reuse", "on", "--duckdb-memory-limit", "8GB", "--duckdb-threads", "5",
             "--pg-host", "other-host"],
            Now, Config());
        var o = inv.Options;
        Assert.Equal("other", o.Store);
        Assert.Equal("/x", o.OutDir);
        Assert.Equal("/y", o.StateDir);
        Assert.Equal(30, o.StatementTimeoutSeconds);
        Assert.Equal(1, o.QueryStoreLagHours);
        Assert.Equal(QueryStoreSourceMode.Raw, o.QueryStoreSource);
        Assert.Equal(ProcedurePlanReuseMode.On, o.ProcedurePlanReuse);
        Assert.Equal("8GB", inv.DuckDbMemoryLimit);
        Assert.Equal(5, inv.DuckDbThreads);
        Assert.Equal("other-host", o.PgHost);
    }

    [Fact]
    public void Pg_credential_file_is_refused_with_a_config()
    {
        var ex = Assert.Throws<ConfigException>(() =>
            ExportCommand.Parse(["--pg-credential-file", "/x", "--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z"], Now, Config()));
        Assert.StartsWith("--pg-credential-file:", ex.Message);
    }

    [Fact]
    public void Connection_flags_are_refused_with_connection_string_env()
    {
        var cfg = ConfigLoader.Parse("""
            { "config_version": 1, "store": { "name": "store_a", "connection_string_env": "CS" },
              "paths": { "out": "/o", "state": "/s", "logs": "/l" } }
            """, false);
        var ex = Assert.Throws<ConfigException>(() =>
            ExportCommand.Parse(["--pg-host", "x", "--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z"], Now, cfg));
        Assert.StartsWith("--pg-host", ex.Message);
        var ok = ExportCommand.Parse(["--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--tables", "wait_stats"], Now, cfg);
        Assert.Equal("connection_string_env", ok.Options.Credential!.Source);
    }

    [Fact]
    public void A_config_name_that_is_not_a_store_name_is_refused_by_the_loader_and_a_flag_one_by_validate()
    {
        Assert.Throws<ArgumentException>(() =>
            ExportCommand.Parse(["--store", "Bad-Name", "--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--out", "/o"], Now));
    }

    private static readonly string[] MinimalArgs = ["--store", "store_a", "--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--out", "/o", "--tables", "wait_stats"];

    [Fact]
    public void Without_a_config_a_missing_state_dir_is_a_usage_error()
    {
        var ex = Assert.Throws<ConfigException>(() => ExportCommand.Parse(MinimalArgs, Now));
        Assert.Contains("--state-dir", ex.Message);
    }

    [Fact]
    public void Without_flags_the_application_name_and_region_are_the_neutral_defaults()
    {
        var o = ExportCommand.Parse([.. MinimalArgs, "--state-dir", "/s"], Now).Options;
        Assert.Equal("darling-export", o.ApplicationName);
        Assert.Null(o.AwsRegion);
    }

    [Fact]
    public void The_application_name_flag_sets_the_store_session_name_and_overrides_a_config()
    {
        var o = ExportCommand.Parse([.. MinimalArgs, "--state-dir", "/s", "--application-name", "MyTool"], Now).Options;
        Assert.Equal("MyTool", o.ApplicationName);
        var withConfig = ExportCommand.Parse(["--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--tables", "wait_stats", "--application-name", "MyTool"], Now, Config());
        Assert.Equal("MyTool", withConfig.Options.ApplicationName);
        Assert.Throws<ArgumentException>(() => ExportCommand.Parse([.. MinimalArgs, "--state-dir", "/s", "--application-name", " "], Now));
    }

    [Fact]
    public void Any_store_name_that_follows_the_rule_is_accepted_by_export()
    {
        var inv = ExportCommand.Parse(["--store", "store_b2", "--from", "2026-09-01T00:00:00Z", "--to", "2026-09-01T01:00:00Z", "--out", "/o", "--tables", "wait_stats", "--state-dir", "/s"], Now);
        Assert.Equal("store_b2", inv.Options.Store);
    }
}
