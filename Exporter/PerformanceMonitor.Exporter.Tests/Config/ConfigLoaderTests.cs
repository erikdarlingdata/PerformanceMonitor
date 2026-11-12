using System.Text.Json;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Config;

public class ConfigLoaderTests
{
    /// <summary>A valid config as a mutable tree. Tests change one key and check the one-line error that names it.</summary>
    internal static System.Text.Json.Nodes.JsonObject Valid() => (System.Text.Json.Nodes.JsonObject)System.Text.Json.Nodes.JsonNode.Parse("""
        {
          "config_version": 1,
          "store": { "name": "store_a", "password": { "source": "env", "name": "SOME_PW" } },
          "paths": { "out": "/o", "state": "/s", "logs": "/l" }
        }
        """)!;

    private static ExporterConfig ParseOk(System.Text.Json.Nodes.JsonObject o, bool windows = false) =>
        ConfigLoader.Parse(o.ToJsonString(), windows);

    private static string Err(System.Text.Json.Nodes.JsonObject o, bool windows = false) =>
        Assert.Throws<ConfigException>(() => ConfigLoader.Parse(o.ToJsonString(), windows)).Message;

    [Fact]
    public void Defaults_match_the_design()
    {
        var c = ParseOk(Valid());
        Assert.Equal("darling-export", c.Store.ApplicationName);
        Assert.Equal(120, c.Store.StatementTimeoutSeconds);
        Assert.Equal(5641, c.Store.Port);
        Assert.Equal("127.0.0.1", c.Store.Host);
        Assert.Equal(6, c.Schedule.MaxWindowsPerFamily);
        Assert.Equal(20, c.Schedule.MaxWindowAgeHours);
        Assert.Equal(40, c.Schedule.CutoffMinute);
        Assert.Equal(20, c.Schedule.HourlyStartMinute);
        Assert.Equal(60, c.Families.Fast.WindowMinutes);
        Assert.Equal(15, c.Families.Fast.LateRowMarginMinutes);
        Assert.True(c.Families.QueryStore.Enabled);
        Assert.Equal(3, c.Families.QueryStore.LagHours);
        Assert.Equal("raw", c.Families.QueryStore.Source);
        Assert.Equal("4GB", c.Export.DuckdbMemoryLimit);
        Assert.Equal("shadow", c.Export.ProcedurePlanReuse);
        Assert.Equal(6144, c.MemoryGuard.MinFreeMb);
        Assert.Equal(2, c.MemoryGuard.StartMultiplier);
        Assert.Equal(30, c.RestartDelay.Minutes);
        Assert.Equal(14, c.Retention.LogDays);
        Assert.Equal(24, c.Retention.LeftoverHours);
        Assert.Equal(48, c.Retention.OutHours);
        Assert.Equal(24, c.Retention.TempHours);
        Assert.Null(c.PostRun);
        Assert.Null(c.Destination);
    }

    [Fact]
    public void Restart_detector_defaults_to_the_platform()
    {
        Assert.Equal("off", ParseOk(Valid(), windows: false).RestartDelay.Detector);
        Assert.Equal("darling_process", ParseOk(Valid(), windows: true).RestartDelay.Detector);
    }

    [Fact]
    public void Destination_is_kept_raw_and_not_validated()
    {
        var o = Valid();
        o["destination"] = JsonNode("""{ "type": "anything", "whatever": [1, 2] }""");
        var c = ParseOk(o);
        Assert.Equal("anything", c.Destination!.Value.GetProperty("type").GetString());
        Assert.Equal(2, c.Destination.Value.GetProperty("whatever").GetArrayLength());
    }

    [Fact]
    public void The_full_design_example_parses()
    {
        var o = JsonNode("""
            {
              "config_version": 1,
              "store": { "name": "store_a", "host": "127.0.0.1", "port": 5641, "database": "darling", "user": "darling_export",
                "password": { "source": "dpapi_file", "path": "C:\\x\\pg.dpapi" }, "application_name": "darling-export", "statement_timeout_seconds": 120 },
              "paths": { "out": "D:\\o", "state": "D:\\s", "logs": "D:\\l" },
              "destination": { "type": "local", "path": "\\\\fs\\x", "prefix": "landing", "hive_store_folder": false },
              "families": {
                "fresh": { "tables": null, "optional_tables": ["database_size_stats"], "optional_tables_file": null },
                "query_store": { "enabled": true, "lag_hours": 3, "source": "raw" },
                "fast": { "window_minutes": 60, "late_row_margin_minutes": 15 } },
              "schedule": { "max_windows_per_family": 6, "max_window_age_hours": 20, "cutoff_minute": 40, "hourly_start_minute": 20, "start_utc": null },
              "export": { "duckdb_memory_limit": "4GB", "duckdb_threads": null, "procedure_plan_reuse": "shadow" },
              "memory_guard": { "enabled": true, "min_free_mb": 6144, "start_multiplier": 2 },
              "restart_delay": { "minutes": 30, "detector": "darling_process", "darling_service_name": "PerformanceMonitor Darling",
                "darling_runtime_path": "C:\\r", "file": null, "command": null },
              "retention": { "log_days": 14, "leftover_hours": 24, "out_hours": 48, "temp_hours": 24 },
              "post_run": null
            }
            """);
        var c = ParseOk(o, windows: true);
        Assert.Equal("darling_export", c.Store.User);
        Assert.Equal(["database_size_stats"], c.Families.Fresh.OptionalTables);
    }

    private static System.Text.Json.Nodes.JsonObject JsonNode(string json) =>
        (System.Text.Json.Nodes.JsonObject)System.Text.Json.Nodes.JsonNode.Parse(json)!;

    public static IEnumerable<object[]> BadCases() =>
    [
        // (mutation, expected key at the start of the message)
        Case("config_version", "1.5", "config_version"),
        Case("config_version", "2", "config_version"),
        Case("config_version", "\"1\"", "config_version"),
        Case("store.name", "\"Store_A\"", "store.name"),
        Case("store.name", "\"1abc\"", "store.name"),
        Case("store.name", "\"has-dash\"", "store.name"),
        Case("store.name", "\"has space\"", "store.name"),
        Case("store.name", "\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"", "store.name"), // 32 chars
        Case("store.name", "\"\"", "store.name"),
        Case("store.name", "7", "store.name"),
        Case("store.port", "0", "store.port"),
        Case("store.port", "70000", "store.port"),
        Case("store.statement_timeout_seconds", "0", "store.statement_timeout_seconds"),
        Case("store.password.source", "\"carrier_pigeon\"", "store.password.source"),
        Case("store.password", "\"hunter2\"", "store.password"),
        Case("store.bogus", "1", "store.bogus"),
        Case("bogus", "1", "bogus"),
        Case("paths.bogus", "1", "paths.bogus"),
        Case("families.fast.window_minutes", "7", "families.fast.window_minutes"),
        Case("families.fast.window_minutes", "0", "families.fast.window_minutes"),
        Case("families.fast.window_minutes", "120", "families.fast.window_minutes"),
        Case("families.fast.late_row_margin_minutes", "1", "families.fast.late_row_margin_minutes"),
        Case("families.fast.late_row_margin_minutes", "16", "families.fast.late_row_margin_minutes"),
        Case("families.query_store.lag_hours", "0", "families.query_store.lag_hours"),
        Case("families.query_store.lag_hours", "4", "families.query_store.lag_hours"),
        Case("families.query_store.source", "\"fast\"", "families.query_store.source"),
        Case("families.query_store.enabled", "\"yes\"", "families.query_store.enabled"),
        Case("families.fresh.tables", "[\"not_a_table\"]", "families.fresh.tables"),
        Case("families.fresh.tables", "[\"servers_password\"]", "families.fresh.tables"),
        Case("families.fresh.optional_tables", "[\"not_a_table\"]", "families.fresh.optional_tables"),
        Case("families.fresh.optional_tables", "\"database_size_stats\"", "families.fresh.optional_tables"),
        Case("families.typo", "{}", "families.typo"),
        Case("schedule.cutoff_minute", "0", "schedule.cutoff_minute"),
        Case("schedule.cutoff_minute", "61", "schedule.cutoff_minute"),
        Case("schedule.max_window_age_hours", "1", "schedule.max_window_age_hours"),
        Case("schedule.max_windows_per_family", "0", "schedule.max_windows_per_family"),
        Case("schedule.hourly_start_minute", "60", "schedule.hourly_start_minute"),
        Case("schedule.start_utc", "\"2026-01-01T00:00:30Z\"", "schedule.start_utc"),
        Case("schedule.start_utc", "\"nope\"", "schedule.start_utc"),
        Case("export.duckdb_memory_limit", "\"lots\"", "export.duckdb_memory_limit"),
        Case("export.duckdb_threads", "0", "export.duckdb_threads"),
        Case("export.procedure_plan_reuse", "\"maybe\"", "export.procedure_plan_reuse"),
        Case("memory_guard.min_free_mb", "0", "memory_guard.min_free_mb"),
        Case("memory_guard.start_multiplier", "0", "memory_guard.start_multiplier"),
        Case("restart_delay.minutes", "-1", "restart_delay.minutes"),
        Case("restart_delay.detector", "\"magic\"", "restart_delay.detector"),
        Case("restart_delay.detector", "\"file\"", "restart_delay.file"),          // file detector with no file
        Case("restart_delay.detector", "\"command\"", "restart_delay.command"),    // command detector with no command
        Case("retention.log_days", "0", "retention.log_days"),
        Case("retention.out_hours", "\"48\"", "retention.out_hours"),
        Case("post_run", "{\"name\":\"x\"}", "post_run.command"),
        Case("post_run", "{\"command\":[\"x\"]}", "post_run.name"),
        Case("post_run", "{\"name\":\"x\",\"command\":[]}", "post_run.command"),
        Case("destination", "\"s3://x\"", "destination"),
    ];

    private static object[] Case(string path, string valueJson, string expectedKey) => [path, valueJson, expectedKey];

    [Theory]
    [MemberData(nameof(BadCases))]
    public void A_bad_value_is_one_line_naming_the_key(string path, string valueJson, string expectedKey)
    {
        var o = Valid();
        var parts = path.Split('.');
        var node = o;
        foreach (var part in parts[..^1])
        {
            node = (System.Text.Json.Nodes.JsonObject)(node[part] ??= new System.Text.Json.Nodes.JsonObject());
        }

        node[parts[^1]] = System.Text.Json.Nodes.JsonNode.Parse(valueJson);
        var message = Err(o);
        Assert.StartsWith(expectedKey + ":", message);
        Assert.DoesNotContain('\n', message);
    }

    [Theory]
    [InlineData("config_version")]
    [InlineData("store")]
    [InlineData("paths")]
    public void A_missing_required_top_level_key_is_named(string key)
    {
        var o = Valid();
        o.Remove(key);
        Assert.StartsWith(key + ":", Err(o));
    }

    [Theory]
    [InlineData("out")]
    [InlineData("state")]
    [InlineData("logs")]
    public void Every_path_is_required(string key)
    {
        var o = Valid();
        ((System.Text.Json.Nodes.JsonObject)o["paths"]!).Remove(key);
        Assert.StartsWith($"paths.{key}:", Err(o));
    }

    [Fact]
    public void Store_name_and_password_are_required()
    {
        var o = Valid();
        ((System.Text.Json.Nodes.JsonObject)o["store"]!).Remove("name");
        Assert.StartsWith("store.name:", Err(o));

        o = Valid();
        ((System.Text.Json.Nodes.JsonObject)o["store"]!).Remove("password");
        Assert.StartsWith("store.password:", Err(o));
    }

    [Fact]
    public void An_optional_table_also_named_in_tables_is_refused()
    {
        var o = Valid();
        o["families"] = JsonNode("""{ "fresh": { "tables": ["wait_stats", "database_size_stats"], "optional_tables": ["database_size_stats"] } }""");
        Assert.StartsWith("families.fresh.optional_tables:", Err(o));
    }

    [Fact]
    public void A_registry_optional_table_is_allowed_in_tables()
    {
        var o = Valid();
        o["families"] = JsonNode("""{ "fresh": { "tables": ["collection_log"] } }""");
        Assert.Equal(["collection_log"], ParseOk(o).Families.Fresh.Tables);
    }

    [Theory]
    [InlineData(5)]
    [InlineData(10)]
    [InlineData(15)]
    [InlineData(20)]
    [InlineData(30)]
    [InlineData(60)]
    public void Every_documented_window_size_is_accepted(int minutes)
    {
        var o = Valid();
        o["families"] = JsonNode($$"""{ "fast": { "window_minutes": {{minutes}} } }""");
        Assert.Equal(minutes, ParseOk(o).Families.Fast.WindowMinutes);
    }

    [Fact]
    public void A_postgres_target_config_parses()
    {
        var o = Valid();
        o["families"] = JsonNode("""{ "query_store": { "enabled": false }, "fresh": { "tables": ["collection_log"] } }""");
        var c = ParseOk(o);
        Assert.False(c.Families.QueryStore.Enabled);
    }

    [Fact]
    public void A_dpapi_file_credential_is_refused_off_Windows_and_accepted_on_Windows()
    {
        var o = Valid();
        o["store"]!["password"] = JsonNode("""{ "source": "dpapi_file", "path": "C:\\x.dpapi" }""");
        Assert.StartsWith("store.password.source:", Err(o, windows: false));
        Assert.Equal("dpapi_file", ParseOk(o, windows: true).Store.Password!.Source);
    }

    [Fact]
    public void Each_password_source_needs_its_own_key()
    {
        var o = Valid();
        o["store"]!["password"] = JsonNode("""{ "source": "env" }""");
        Assert.StartsWith("store.password.name:", Err(o));
        o["store"]!["password"] = JsonNode("""{ "source": "file" }""");
        Assert.StartsWith("store.password.path:", Err(o));
        o["store"]!["password"] = JsonNode("""{ "source": "env", "name": "X", "path": "/p" }""");
        Assert.StartsWith("store.password.path:", Err(o));
    }

    [Fact]
    public void Connection_string_env_excludes_the_connection_keys()
    {
        var o = Valid();
        var store = (System.Text.Json.Nodes.JsonObject)o["store"]!;
        store.Remove("password");
        store["connection_string_env"] = "CS_ENV";
        var c = ParseOk(o);
        Assert.Equal("connection_string_env", c.Store.Credential.Source);
        Assert.Equal("CS_ENV", c.Store.Credential.Name);

        foreach (var key in new[] { "host", "port", "database", "user" })
        {
            var o2 = Valid();
            var s2 = (System.Text.Json.Nodes.JsonObject)o2["store"]!;
            s2["connection_string_env"] = "CS_ENV";
            s2[key] = key == "port" ? 1 : "x";
            Assert.StartsWith($"store.{key}:", Err(o2));
        }

        // A password block next to it is refused too.
        var o3 = Valid();
        o3["store"]!["connection_string_env"] = "CS_ENV";
        Assert.StartsWith("store.password:", Err(o3));
    }

    [Fact]
    public void Invalid_json_and_a_non_object_are_config_errors()
    {
        Assert.StartsWith("config:", Assert.Throws<ConfigException>(() => ConfigLoader.Parse("{ nope", false)).Message);
        Assert.StartsWith("config:", Assert.Throws<ConfigException>(() => ConfigLoader.Parse("[1]", false)).Message);
    }

    [Fact]
    public void A_missing_file_is_a_config_error()
    {
        var ex = Assert.Throws<ConfigException>(() => ConfigLoader.Load(Path.Combine(Path.GetTempPath(), "no-such-config-" + Guid.NewGuid() + ".json")));
        Assert.StartsWith("--config:", ex.Message);
    }

    [Theory]
    [InlineData("store_a", true)]
    [InlineData("store_pg", true)]
    [InlineData("a", true)]
    [InlineData("a23456789012345678901234567890b", true)]   // 31 chars
    [InlineData("a234567890123456789012345678901b", false)] // 32 chars
    [InlineData("_a", false)]
    [InlineData("A", false)]
    [InlineData("a-b", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void Store_name_rule(string? name, bool valid) => Assert.Equal(valid, ExportOptions.IsValidStoreName(name));
}
