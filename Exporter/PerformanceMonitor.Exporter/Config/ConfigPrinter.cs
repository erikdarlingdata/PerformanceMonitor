using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Exporter;

/// <summary>Renders the resolved config as JSON for `check-config`. A secret is never printed: only where it comes from, and whether it resolved.</summary>
public static partial class ConfigPrinter
{
    public const string Redacted = "<redacted>";

    [GeneratedRegex(@"(?i)(password|secret|token|sas|connection_string)")]
    private static partial Regex SecretKeyPattern();

    public static string Render(ExporterConfig config, bool credentialResolved)
    {
        var store = config.Store;
        var root = new JsonObject
        {
            ["config_version"] = config.ConfigVersion,
            ["store"] = new JsonObject
            {
                ["name"] = store.Name,
                ["host"] = store.ConnectionStringEnv is null ? store.Host : null,
                ["port"] = store.ConnectionStringEnv is null ? store.Port : null,
                ["database"] = store.ConnectionStringEnv is null ? store.Database : null,
                ["user"] = store.ConnectionStringEnv is null ? store.User : null,
                ["credential"] = new JsonObject
                {
                    ["source"] = store.Credential.Source,
                    ["name"] = store.Credential.Name,
                    ["path"] = store.Credential.Path,
                    ["value"] = Redacted,
                    ["resolved"] = credentialResolved,
                },
                ["application_name"] = store.ApplicationName,
                ["statement_timeout_seconds"] = store.StatementTimeoutSeconds,
            },
            ["paths"] = new JsonObject { ["out"] = config.Paths.Out, ["state"] = config.Paths.State, ["logs"] = config.Paths.Logs },
            ["destination"] = config.Destination is { } d ? RedactSecrets(JsonNode.Parse(d.GetRawText())) : null,
            ["families"] = new JsonObject
            {
                ["fresh"] = new JsonObject
                {
                    ["tables"] = config.Families.Fresh.Tables is { } t ? ToArray(t) : null,
                    ["optional_tables"] = ToArray(config.Families.Fresh.OptionalTables),
                    ["optional_tables_file"] = config.Families.Fresh.OptionalTablesFile,
                },
                ["query_store"] = new JsonObject
                {
                    ["enabled"] = config.Families.QueryStore.Enabled,
                    ["lag_hours"] = config.Families.QueryStore.LagHours,
                    ["source"] = config.Families.QueryStore.Source,
                },
                ["fast"] = new JsonObject
                {
                    ["window_minutes"] = config.Families.Fast.WindowMinutes,
                    ["late_row_margin_minutes"] = config.Families.Fast.LateRowMarginMinutes,
                },
            },
            ["schedule"] = new JsonObject
            {
                ["max_windows_per_family"] = config.Schedule.MaxWindowsPerFamily,
                ["max_window_age_hours"] = config.Schedule.MaxWindowAgeHours,
                ["cutoff_minute"] = config.Schedule.CutoffMinute,
                ["hourly_start_minute"] = config.Schedule.HourlyStartMinute,
                ["start_utc"] = config.Schedule.StartUtc,
            },
            ["export"] = new JsonObject
            {
                ["duckdb_memory_limit"] = config.Export.DuckdbMemoryLimit,
                ["duckdb_threads"] = config.Export.DuckdbThreads,
                ["procedure_plan_reuse"] = config.Export.ProcedurePlanReuse,
            },
            ["memory_guard"] = new JsonObject
            {
                ["enabled"] = config.MemoryGuard.Enabled,
                ["min_free_mb"] = config.MemoryGuard.MinFreeMb,
                ["start_multiplier"] = config.MemoryGuard.StartMultiplier,
            },
            ["restart_delay"] = new JsonObject
            {
                ["minutes"] = config.RestartDelay.Minutes,
                ["detector"] = config.RestartDelay.Detector,
                ["darling_service_name"] = config.RestartDelay.DarlingServiceName,
                ["darling_runtime_path"] = config.RestartDelay.DarlingRuntimePath,
                ["file"] = config.RestartDelay.File,
                ["command"] = config.RestartDelay.Command is { } c ? ToArray(c) : null,
            },
            ["retention"] = new JsonObject
            {
                ["log_days"] = config.Retention.LogDays,
                ["leftover_hours"] = config.Retention.LeftoverHours,
                ["out_hours"] = config.Retention.OutHours,
                ["temp_hours"] = config.Retention.TempHours,
            },
            ["post_run"] = config.PostRun is { } p
                ? new JsonObject { ["name"] = p.Name, ["command"] = ToArray(p.Command), ["timeout_seconds"] = p.TimeoutSeconds }
                : null,
        };

        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = true, Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
    }

    private static JsonArray ToArray(IEnumerable<string> items) => new([.. items.Select(i => (JsonNode?)JsonValue.Create(i))]);

    /// <summary>Blanks every string value whose key looks like a secret. A key ending `_env` names an env var, not a secret, so it stays.</summary>
    public static JsonNode? RedactSecrets(JsonNode? node)
    {
        switch (node)
        {
            case JsonObject obj:
                foreach (var key in obj.Select(kv => kv.Key).ToList())
                {
                    if (obj[key] is JsonValue && !key.EndsWith("_env", StringComparison.OrdinalIgnoreCase) && SecretKeyPattern().IsMatch(key))
                    {
                        obj[key] = Redacted;
                    }
                    else
                    {
                        obj[key] = RedactSecrets(obj[key]);
                    }
                }

                return obj;
            case JsonArray arr:
                for (var i = 0; i < arr.Count; i++)
                {
                    arr[i] = RedactSecrets(arr[i]);
                }

                return arr;
            default:
                return node;
        }
    }
}
