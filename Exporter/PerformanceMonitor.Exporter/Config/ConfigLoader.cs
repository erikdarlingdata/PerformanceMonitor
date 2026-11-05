using System.Text.Json;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Exporter;

/// <summary>Reads and validates the exporter JSON config. Every failure is a <see cref="ConfigException"/> naming one key.</summary>
public static partial class ConfigLoader
{
    public const int SupportedConfigVersion = 1;

    public static readonly int[] WindowMinutesValues = [5, 10, 15, 20, 30, 60];
    public static readonly string[] RestartDetectors = ["darling_process", "file", "command", "off"];
    public const string DefaultApplicationName = "darling-export";

    [GeneratedRegex(@"^\d+(\.\d+)?\s*(KB|MB|GB|KiB|MiB|GiB)$", RegexOptions.IgnoreCase)]
    private static partial Regex MemoryLimitPattern();

    public static ExporterConfig Load(string path, bool? isWindows = null)
    {
        string text;
        try
        {
            text = File.ReadAllText(path);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            throw new ConfigException($"--config: cannot read '{path}': {ex.Message}");
        }

        return Parse(text, isWindows ?? OperatingSystem.IsWindows());
    }

    public static ExporterConfig Parse(string json, bool isWindows)
    {
        JsonDocument doc;
        try
        {
            doc = JsonDocument.Parse(json);
        }
        catch (JsonException ex)
        {
            throw new ConfigException($"config: not valid JSON: {ex.Message.Split('\n')[0]}");
        }

        using (doc)
        {
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                throw new ConfigException("config: the top level must be a JSON object");
            }

            var root = new ConfigObject(doc.RootElement, "");

            var version = root.Int("config_version", null, 1, int.MaxValue, required: true)!.Value;
            if (version != SupportedConfigVersion)
            {
                throw new ConfigException($"config_version: must be {SupportedConfigVersion}, got {version}");
            }

            var store = ReadStore(root.Object("store", required: true)!, isWindows);
            var paths = ReadPaths(root.Object("paths", required: true)!);

            JsonElement? destination = null;
            if (root.Raw("destination") is { } dest)
            {
                if (dest.ValueKind != JsonValueKind.Object)
                {
                    throw new ConfigException("destination: must be an object");
                }

                destination = dest.Clone();
            }

            var families = ReadFamilies(root.Object("families"));
            var schedule = ReadSchedule(root.Object("schedule"));
            var export = ReadExport(root.Object("export"));
            var memoryGuard = ReadMemoryGuard(root.Object("memory_guard"));
            var restartDelay = ReadRestartDelay(root.Object("restart_delay"), isWindows);
            var retention = ReadRetention(root.Object("retention"));
            var postRun = ReadPostRun(root.Object("post_run"));
            root.Finish();

            return new ExporterConfig(
                version, store, paths, destination, families, schedule, export, memoryGuard, restartDelay, retention, postRun);
        }
    }

    private static StoreConfig ReadStore(ConfigObject o, bool isWindows)
    {
        var name = o.String("name", null, required: true)!;
        if (!ExportOptions.IsValidStoreName(name))
        {
            throw new ConfigException($"store.name: must match {ExportOptions.StoreNamePattern}, got '{name}'");
        }

        var connectionStringEnv = o.String("connection_string_env", null);
        if (connectionStringEnv is not null)
        {
            foreach (var key in new[] { "host", "port", "database", "user", "password" })
            {
                if (o.Has(key))
                {
                    throw new ConfigException($"store.{key}: not allowed together with store.connection_string_env (the connection string carries it)");
                }
            }
        }

        var host = o.String("host", "127.0.0.1")!;
        var port = o.Int("port", 5641, 1, 65535)!.Value;
        var database = o.String("database", "darling")!;
        var user = o.String("user", "darling")!;

        CredentialSpec? password = null;
        if (connectionStringEnv is null)
        {
            var pw = o.Object("password", required: true)!;
            var source = pw.String("source", null, required: true)!;
            if (!CredentialSources.PasswordSources.Contains(source))
            {
                throw new ConfigException($"store.password.source: must be one of {string.Join(", ", CredentialSources.PasswordSources)}, got '{source}'");
            }

            switch (source)
            {
                case CredentialSources.Env:
                    password = new CredentialSpec(source, Name: pw.String("name", null, required: true));
                    break;
                case CredentialSources.File:
                    password = new CredentialSpec(source, Path: pw.String("path", null, required: true));
                    break;
                default:
                    password = new CredentialSpec(source, Path: pw.String("path", null, required: true));
                    if (!isWindows)
                    {
                        throw new ConfigException("store.password.source: dpapi_file can only be used on Windows");
                    }

                    break;
            }

            pw.Finish();
        }

        var applicationName = o.String("application_name", DefaultApplicationName)!;
        var timeout = o.Int("statement_timeout_seconds", 120, 1, int.MaxValue)!.Value;
        o.Finish();
        return new StoreConfig(name, host, port, database, user, password, connectionStringEnv, applicationName, timeout);
    }

    private static PathsConfig ReadPaths(ConfigObject o)
    {
        var result = new PathsConfig(
            o.String("out", null, required: true)!,
            o.String("state", null, required: true)!,
            o.String("logs", null, required: true)!);
        o.Finish();
        return result;
    }

    private static FamiliesConfig ReadFamilies(ConfigObject? o)
    {
        o ??= ConfigObject.Empty("families");
        var fresh = ReadFresh(o.Object("fresh"));
        var qs = ReadQueryStore(o.Object("query_store"));
        var fast = ReadFast(o.Object("fast"));
        o.Finish();
        return new FamiliesConfig(fresh, qs, fast);
    }

    private static FreshFamilyConfig ReadFresh(ConfigObject? o)
    {
        o ??= ConfigObject.Empty("families.fresh");
        var tables = o.StringList("tables");
        var optional = o.StringList("optional_tables") ?? [];
        var optionalFile = o.String("optional_tables_file", null);
        o.Finish();

        if (tables is not null)
        {
            foreach (var t in tables)
            {
                if (ColumnPolicies.IsDenied(t))
                {
                    throw new ConfigException($"families.fresh.tables: '{t}' is denied and can never be exported");
                }

                if (!ExportOptions.AllTables.Contains(t) && OptionalTables.TryGet(t) is null)
                {
                    throw new ConfigException($"families.fresh.tables: unknown table '{t}'");
                }
            }
        }

        foreach (var t in optional)
        {
            if (OptionalTables.TryGet(t) is null)
            {
                throw new ConfigException($"families.fresh.optional_tables: '{t}' is not in the optional-table registry");
            }

            if (tables is not null && tables.Contains(t))
            {
                throw new ConfigException($"families.fresh.optional_tables: '{t}' is also named in families.fresh.tables; name it in one place only");
            }
        }

        return new FreshFamilyConfig(tables, optional, optionalFile);
    }

    private static QueryStoreFamilyConfig ReadQueryStore(ConfigObject? o)
    {
        o ??= ConfigObject.Empty("families.query_store");
        var enabled = o.Bool("enabled", true);
        var lag = o.Int("lag_hours", 3, 1, 3)!.Value;
        var source = o.String("source", "raw")!;
        o.Finish();
        if (!QueryStoreSourceModes.Names.Contains(source))
        {
            throw new ConfigException($"families.query_store.source: must be one of {string.Join(", ", QueryStoreSourceModes.Names)}, got '{source}'");
        }

        return new QueryStoreFamilyConfig(enabled, lag, source);
    }

    private static FastFamilyConfig ReadFast(ConfigObject? o)
    {
        o ??= ConfigObject.Empty("families.fast");
        var window = o.Int("window_minutes", 60, 1, int.MaxValue)!.Value;
        var margin = o.Int("late_row_margin_minutes", 15, 2, 15)!.Value;
        o.Finish();
        if (!WindowMinutesValues.Contains(window))
        {
            throw new ConfigException($"families.fast.window_minutes: must be one of {string.Join(", ", WindowMinutesValues)}, got {window}");
        }

        return new FastFamilyConfig(window, margin);
    }

    private static ScheduleConfig ReadSchedule(ConfigObject? o)
    {
        o ??= ConfigObject.Empty("schedule");
        var maxWindows = o.Int("max_windows_per_family", 6, 1, int.MaxValue)!.Value;
        var maxAge = o.Int("max_window_age_hours", 20, 2, int.MaxValue)!.Value;
        var cutoff = o.Int("cutoff_minute", 40, 1, 60)!.Value;
        var startMinute = o.Int("hourly_start_minute", 20, 0, 59)!.Value;
        var startUtc = o.String("start_utc", null);
        o.Finish();
        if (startUtc is not null && !ExportOptions.TryParseUtcMinute(startUtc, out _))
        {
            throw new ConfigException("schedule.start_utc: must be an ISO-8601 UTC timestamp on a whole minute, or null");
        }

        return new ScheduleConfig(maxWindows, maxAge, cutoff, startMinute, startUtc);
    }

    private static ExportSectionConfig ReadExport(ConfigObject? o)
    {
        o ??= ConfigObject.Empty("export");
        var limit = o.String("duckdb_memory_limit", DuckDbSession.DefaultMemoryLimit)!;
        var threads = o.Int("duckdb_threads", null, 1, 64);
        var reuse = o.String("procedure_plan_reuse", "shadow")!;
        o.Finish();
        if (!MemoryLimitPattern().IsMatch(limit.Trim()))
        {
            throw new ConfigException($"export.duckdb_memory_limit: must look like 2GB or 512MB, got '{limit}'");
        }

        if (!ProcedurePlanReuseModes.Names.Contains(reuse))
        {
            throw new ConfigException($"export.procedure_plan_reuse: must be one of {string.Join(", ", ProcedurePlanReuseModes.Names)}, got '{reuse}'");
        }

        return new ExportSectionConfig(limit.Trim(), threads, reuse);
    }

    private static MemoryGuardConfig ReadMemoryGuard(ConfigObject? o)
    {
        o ??= ConfigObject.Empty("memory_guard");
        var result = new MemoryGuardConfig(
            o.Bool("enabled", true),
            o.Int("min_free_mb", 6144, 1, int.MaxValue)!.Value,
            o.Int("start_multiplier", 2, 1, 100)!.Value);
        o.Finish();
        return result;
    }

    private static RestartDelayConfig ReadRestartDelay(ConfigObject? o, bool isWindows)
    {
        o ??= ConfigObject.Empty("restart_delay");
        var minutes = o.Int("minutes", 30, 0, int.MaxValue)!.Value;
        var detector = o.String("detector", isWindows ? "darling_process" : "off")!;
        var service = o.String("darling_service_name", "PerformanceMonitor Darling")!;
        var runtime = o.String("darling_runtime_path", @"C:\PerformanceMonitorDarling\pg-runtime")!;
        var file = o.String("file", null);
        var command = o.StringList("command");
        o.Finish();
        if (!RestartDetectors.Contains(detector))
        {
            throw new ConfigException($"restart_delay.detector: must be one of {string.Join(", ", RestartDetectors)}, got '{detector}'");
        }

        if (detector == "file" && file is null)
        {
            throw new ConfigException("restart_delay.file: required when restart_delay.detector is 'file'");
        }

        if (detector == "command" && (command is null || command.Count == 0))
        {
            throw new ConfigException("restart_delay.command: required (a non-empty argv list) when restart_delay.detector is 'command'");
        }

        return new RestartDelayConfig(minutes, detector, service, runtime, file, command);
    }

    private static RetentionConfig ReadRetention(ConfigObject? o)
    {
        o ??= ConfigObject.Empty("retention");
        var result = new RetentionConfig(
            o.Int("log_days", 14, 1, int.MaxValue)!.Value,
            o.Int("leftover_hours", 24, 1, int.MaxValue)!.Value,
            o.Int("out_hours", 48, 1, int.MaxValue)!.Value,
            o.Int("temp_hours", 24, 1, int.MaxValue)!.Value);
        o.Finish();
        return result;
    }

    private static PostRunConfig? ReadPostRun(ConfigObject? o)
    {
        if (o is null)
        {
            return null;
        }

        var name = o.String("name", null, required: true)!;
        var command = o.StringList("command");
        var timeout = o.Int("timeout_seconds", 600, 1, int.MaxValue)!.Value;
        o.Finish();
        if (command is null || command.Count == 0)
        {
            throw new ConfigException("post_run.command: required (a non-empty argv list)");
        }

        return new PostRunConfig(name, command, timeout);
    }
}
