using PerformanceMonitor.Exporter.Run;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Run;

/// <summary>
/// `run --config F`: the config maps to the RunSettings the scheduler runs, and the child calls it produces are the
/// flags-only `export` calls. The three configs are the shape a deploy script writes for a store_a-like, a store_b-like
/// and a Postgres-store-like box.
/// </summary>
public class RunConfigMappingTests
{
    private const string Root = @"C:\exporter";
    private const string DefaultCred = @"C:\ProgramData\PerformanceMonitorDarling\pg-credential.dpapi";

    /// <summary>A box config as JSON, with the parts that differ per box as parameters.</summary>
    private static string BoxConfig(string store, string user = "darling", string cred = DefaultCred, string queryStore = """{ "enabled": true, "lag_hours": 3, "source": "raw" }""",
        string freshTables = "null", string optionalFile = "null", int window = 60, int margin = 15, string reuse = "shadow",
        string destination = """{ "type": "s3", "bucket": "example-bucket", "prefix": "landing", "region": "us-east-1" }""", int cutoff = 40) =>
        $$"""
        {
          "config_version": 1,
          "store": { "name": "{{store}}", "host": "127.0.0.1", "port": 5641, "database": "darling", "user": "{{user}}",
                     "password": { "source": "dpapi_file", "path": "{{cred.Replace(@"\", @"\\")}}" },
                     "application_name": "box-exporter", "statement_timeout_seconds": 120 },
          "paths": { "out": "C:\\exporter\\out", "state": "C:\\exporter\\state", "logs": "C:\\exporter\\logs" },
          "destination": {{destination}},
          "families": {
            "fresh": { "tables": {{freshTables}}, "optional_tables": [], "optional_tables_file": {{optionalFile}} },
            "query_store": {{queryStore}},
            "fast": { "window_minutes": {{window}}, "late_row_margin_minutes": {{margin}} }
          },
          "schedule": { "max_windows_per_family": 6, "max_window_age_hours": 20, "cutoff_minute": {{cutoff}}, "hourly_start_minute": 20, "start_utc": null },
          "export": { "duckdb_memory_limit": "4GB", "duckdb_threads": null, "procedure_plan_reuse": "{{reuse}}" },
          "memory_guard": { "enabled": true, "min_free_mb": 6144, "start_multiplier": 2 },
          "restart_delay": { "minutes": 30, "detector": "darling_process", "darling_service_name": "PerformanceMonitor Darling",
                             "darling_runtime_path": "C:\\PerformanceMonitorDarling\\pg-runtime", "file": null, "command": null },
          "retention": { "log_days": 14, "leftover_hours": 24, "out_hours": 48, "temp_hours": 24 },
          "post_run": null
        }
        """;

    private static RunSettings Map(string json) => RunConfigMapping.ToRunSettings(ConfigLoader.Parse(json, isWindows: true), @"C:\exporter\config\darling-export.json");

    private static string[] ChildArgs(RunSettings s, string family = Planner.Fresh)
    {
        var mode = Planner.ResolveMode(s, []);
        var plan = Planner.PlanFamily(s, mode, family, new DateTime(2026, 9, 28, family == Planner.QueryStore ? 9 : 12, 0, 0, DateTimeKind.Utc), new DateTime(2026, 9, 28, 14, 20, 0, DateTimeKind.Utc));
        return [.. Planner.BuildExportArgs(s, Planner.CallFor(mode, plan, plan.Windows[0]))];
    }

    [Fact]
    public void A_store_a_like_config_maps_to_15_minute_windows_margin_5_lag_2_and_the_dedicated_login()
    {
        var s = Map(BoxConfig("store_a", user: "export_user", cred: @"C:\ProgramData\x\pg-export-credential.dpapi", window: 15, margin: 5,
            queryStore: """{ "enabled": true, "lag_hours": 2, "source": "raw" }"""));
        Assert.Null(s.Validate());
        Assert.Equal(("store_a", Root + @"\out", Root + @"\state", Root + @"\logs"), (s.Store, s.OutDir, s.StateDir, s.LogDir));
        Assert.Equal((15, 5, 2), (s.Families.FastWindowMinutes, s.Families.LateRowMarginMinutes, s.Families.QueryStoreLagHours));
        Assert.Equal((6, 20, 40), (s.Schedule.MaxWindowsPerFamily, s.Schedule.MaxWindowAgeHours, s.Schedule.CutoffMinute));
        Assert.Equal((true, 6144, 2), (s.MemoryGuard.Enabled, s.MemoryGuard.MinFreeMb, s.MemoryGuard.StartMultiplier));
        Assert.Equal((30, "darling_process"), (s.Restart.DelayMinutes, s.Restart.Detector));
        Assert.Equal(("export_user", @"C:\ProgramData\x\pg-export-credential.dpapi"), (s.PgUser, s.PgCredentialFile));
        Assert.Null(s.Families.FreshTables);
        Assert.True(s.Families.QueryStoreEnabled);
    }

    [Fact]
    public void A_store_b_like_config_with_the_default_lag_and_margin_reports_them_as_defaults()
    {
        var s = Map(BoxConfig("store_b", window: 15, margin: 5));
        Assert.Null(s.Validate());
        Assert.Null(s.Families.QueryStoreLagHours);
        var mode = Planner.ResolveMode(s, []);
        Assert.Equal((3, "default", 5, "config"), (mode.QueryStoreLag, mode.QueryStoreLagSource, mode.LateMargin, mode.LateSource));
        Assert.Equal((null, null), (s.PgUser, s.PgCredentialFile));
    }

    [Fact]
    public void A_postgres_like_config_has_no_query_store_and_only_collection_log()
    {
        var s = Map(BoxConfig("store_pg", queryStore: """{ "enabled": false }""", freshTables: """["collection_log"]""",
            optionalFile: @"""C:\\exporter\\config\\optional-tables.txt"""));
        Assert.Null(s.Validate());
        Assert.False(s.Families.QueryStoreEnabled);
        Assert.Equal(["collection_log"], s.Families.FreshTables!);
        Assert.Equal(@"C:\exporter\config\optional-tables.txt", s.Families.OptionalTablesFile);
        Assert.Equal([Planner.Fresh], Planner.FamilyOrder(Planner.ResolveMode(s, [])));
        Assert.Null(s.QueryStoreSource);
    }

    [Fact]
    public void The_child_call_from_a_box_config_is_the_flags_only_export_in_the_fixed_order()
    {
        var args = ChildArgs(Map(BoxConfig("store_a", user: "export_user", cred: @"C:\c.dpapi")));
        Assert.Equal(
            "export --store store_a --from 2026-09-28T12:00:00Z --to 2026-09-28T13:00:00Z --out C:\\exporter\\out --statement-timeout-seconds 120 " +
            "--duckdb-memory-limit 4GB --upload s3://example-bucket/landing --aws-region us-east-1 --state-dir C:\\exporter\\state --application-name box-exporter --tables " +
            string.Join(',', Planner.DefaultFreshTables) + " --dimension-origin query_stats --pg-user export_user --pg-credential-file C:\\c.dpapi",
            string.Join(' ', args));
    }

    [Fact]
    public void A_flags_only_config_passes_its_own_application_name_whatever_it_is()
    {
        var args = ChildArgs(Map(BoxConfig("store_a").Replace("\"application_name\": \"box-exporter\"", "\"application_name\": \"my-exporter\"")));
        Assert.DoesNotContain("--config", args);
        Assert.Equal("my-exporter", args[Array.IndexOf(args, "--application-name") + 1]);
    }

    [Fact]
    public void The_default_darling_login_passes_no_login_flags_as_an_absent_store_login_json_does_today()
    {
        var args = ChildArgs(Map(BoxConfig("store_b")));
        Assert.DoesNotContain("--pg-user", args);
        Assert.DoesNotContain("--pg-credential-file", args);
        Assert.DoesNotContain("--config", args);
        Assert.Contains("--state-dir", args);
    }

    [Fact]
    public void Plan_reuse_and_query_store_source_pass_flags_only_when_not_the_exporter_default()
    {
        Assert.Null(Map(BoxConfig("store_a")).ProcedurePlanReuse);
        Assert.Null(Map(BoxConfig("store_a")).QueryStoreSource);
        var s = Map(BoxConfig("store_a", reuse: "on", queryStore: """{ "enabled": true, "lag_hours": 3, "source": "wide" }"""));
        Assert.Equal(("on", "wide"), (s.ProcedurePlanReuse, s.QueryStoreSource));
        var qs = ChildArgs(s, Planner.QueryStore);
        Assert.Contains("--procedure-plan-reuse", qs);
        Assert.Equal("wide", qs[Array.IndexOf(qs, "--query-store-source") + 1]);
    }

    [Fact]
    public void A_config_today_flags_cannot_express_runs_the_child_as_export_with_the_same_config()
    {
        var json = BoxConfig("store_a").Replace("\"host\": \"127.0.0.1\"", "\"host\": \"db.example\"");
        var args = ChildArgs(Map(json));
        Assert.Equal(["export", "--config", Path.GetFullPath(@"C:\exporter\config\darling-export.json"), "--store", "store_a"], args[..5]);
        Assert.DoesNotContain("--pg-user", args);
    }

    [Fact]
    public void Destinations_map_to_the_upload_flag_and_the_region()
    {
        var s3 = Map(BoxConfig("store_a", destination: """{ "type": "s3", "bucket": "b", "prefix": "/p/q/", "region": "us-east-2" }"""));
        Assert.Equal(("s3://b/p/q", "us-east-2"), (s3.Upload, s3.AwsRegion));
        var bare = Map(BoxConfig("store_a", destination: """{ "type": "s3", "bucket": "b" }"""));
        Assert.Equal(("s3://b", (string?)null), (bare.Upload, bare.AwsRegion));   // no region: the AWS SDK chain decides
        var local = Map(BoxConfig("store_a", destination: """{ "type": "local", "path": "/srv/landing" }"""));
        Assert.Equal("/srv/landing", local.Upload);
        Assert.Null(local.AwsRegion);
        var none = RunConfigMapping.ToRunSettings(ConfigLoader.Parse(BoxConfig("store_a").Replace("\"destination\": { \"type\": \"s3\", \"bucket\": \"example-bucket\", \"prefix\": \"landing\", \"region\": \"us-east-1\" },", ""), true), "x.json");
        Assert.EndsWith("landing", none.Upload);
    }

    [Theory]
    [InlineData("""{ "type": "azure_blob", "account_url": "https://a.blob.core.windows.net", "container": "c", "auth": { "method": "default_credential" } }""", "destination.type")]
    [InlineData("""{ "type": "s3", "bucket": "b", "endpoint_url": "http://minio:9000" }""", "destination")]
    [InlineData("""{ "type": "s3" }""", "destination.bucket")]
    public void Destinations_run_cannot_pass_are_a_config_error(string destination, string key)
    {
        var ex = Assert.Throws<ConfigException>(() => Map(BoxConfig("store_a", destination: destination)));
        Assert.StartsWith(key, ex.Message);
    }

    [Fact]
    public async Task Run_config_with_a_bad_file_exits_2_and_names_the_key()
    {
        using var r = new RunRoot();
        var bad = Path.Combine(r.Root, "bad.json");
        File.WriteAllText(bad, BoxConfig("store_a", window: 7));
        var err = new StringWriter();
        Assert.Equal(2, await RunCommand.MainAsync(["--config", bad, "--plan-only"], new StringWriter(), err, CancellationToken.None));
        Assert.Contains("families.fast.window_minutes", err.ToString());

        var both = new StringWriter();
        Assert.Equal(2, await RunCommand.MainAsync(["--config", bad, "--settings", bad], new StringWriter(), both, CancellationToken.None));
        Assert.Contains("Usage", both.ToString());
    }

    [Fact]
    public async Task Run_config_plan_only_plans_from_the_config_paths()
    {
        using var r = new RunRoot();
        var json = BoxConfig("store_b", window: 15, margin: 5).Replace(@"C:\\exporter\\out", r.Out.Replace(@"\", @"\\"))
            .Replace(@"C:\\exporter\\state", r.State.Replace(@"\", @"\\")).Replace(@"C:\\exporter\\logs", r.Logs.Replace(@"\", @"\\"));
        var cfg = Path.Combine(r.Root, "c.json");
        File.WriteAllText(cfg, json);
        var output = new StringWriter();
        var exit = await RunCommand.MainAsync(["--config", cfg, "--plan-only", "--now", "2026-09-28T16:30:00Z"], output, new StringWriter(), CancellationToken.None);
        Assert.Equal(0, exit);
        var line = File.ReadAllLines(Path.Combine(r.Logs, "plan-only-store_b-20260928.jsonl")).Single();
        Assert.Contains("\"late_margin\":{\"value\":5,\"source\":\"config\"}", line);
        Assert.False(File.Exists(r.StatePath));
    }
}
