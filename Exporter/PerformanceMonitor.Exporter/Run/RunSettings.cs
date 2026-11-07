using System.Text.Json;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Exporter.Run;

/// <summary>
/// Everything `darling-export run` needs, as plain data. A later step maps the JSON config to this record;
/// until then <c>run --settings &lt;file&gt;</c> reads a snake_case JSON serialization of it. Tests build it directly.
/// </summary>
public sealed record RunSettings
{
    public string Store { get; init; } = "";
    public string OutDir { get; init; } = "";
    public string StateDir { get; init; } = "";
    public string LogDir { get; init; } = "";

    /// <summary>The child's `--upload` value. Null passes no `--upload` at all.</summary>
    public string? Upload { get; init; }

    /// <summary>The child executable. Null means this process's own exe (`Environment.ProcessPath`).</summary>
    public string? ChildExe { get; init; }

    /// <summary>Arguments before the per-call flags. The default is the `export` verb.</summary>
    public List<string> ChildBaseArgs { get; init; } = ["export"];

    /// <summary>`--pg-user` and `--pg-credential-file` ride together; one without the other is a refusal (exit 2).</summary>
    public string? PgUser { get; init; }
    public string? PgCredentialFile { get; init; }
    public string? ProcedurePlanReuse { get; init; }
    public string? QueryStoreSource { get; init; }

    /// <summary>When set, every child call carries `--state-dir`. Null passes nothing.</summary>
    public string? ExportStateDir { get; init; }

    /// <summary>When set, every child call carries `--application-name` (the flags-only call has no config to carry it).</summary>
    public string? ApplicationName { get; init; }

    /// <summary>When set, every child call carries `--aws-region`. Null passes nothing.</summary>
    public string? AwsRegion { get; init; }

    public int StatementTimeoutSeconds { get; init; } = 120;
    public string DuckdbMemoryLimit { get; init; } = "4GB";
    public int? DuckdbThreads { get; init; }

    public FamilySettings Families { get; init; } = new();
    public ScheduleSettings Schedule { get; init; } = new();
    public MemoryGuardSettings MemoryGuard { get; init; } = new();
    public RestartSettings Restart { get; init; } = new();
    public RetentionSettings Retention { get; init; } = new();
    public PostRunSettings? PostRun { get; init; }

    public static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        WriteIndented = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        UnmappedMemberHandling = System.Text.Json.Serialization.JsonUnmappedMemberHandling.Disallow,
    };

    public static RunSettings FromJson(string json) =>
        JsonSerializer.Deserialize<RunSettings>(json, JsonOptions) ?? throw new JsonException("settings file is empty");

    public string ToJson() => JsonSerializer.Serialize(this, JsonOptions);

    /// <summary>The one-line refusal reason (exit 2), or null when the settings are usable.</summary>
    public string? Validate()
    {
        if (!Regex.IsMatch(Store ?? "", "^[a-z][a-z0-9_]{0,30}$")) return "store must match ^[a-z][a-z0-9_]{0,30}$";
        if (string.IsNullOrWhiteSpace(OutDir)) return "out_dir is required";
        if (string.IsNullOrWhiteSpace(StateDir)) return "state_dir is required";
        if (string.IsNullOrWhiteSpace(LogDir)) return "log_dir is required";
        if (string.IsNullOrWhiteSpace(PgUser) != string.IsNullOrWhiteSpace(PgCredentialFile))
            return "pg_user and pg_credential_file must be set together";
        if (!ScheduleSettings.AllowedWindowMinutes.Contains(Families.FastWindowMinutes))
            return "families.fast_window_minutes must be one of 5, 10, 15, 20, 30, 60";
        if (Families.LateRowMarginMinutes is < 2 or > 15) return "families.late_row_margin_minutes must be 2..15";
        if (Families.QueryStoreLagHours is < 1 or > 3) return "families.query_store_lag_hours must be 1..3";
        if (Schedule.CutoffMinute is < 1 or > 60) return "schedule.cutoff_minute must be 1..60";
        if (Schedule.StartUtc is { } start && start.Ticks % TimeSpan.TicksPerHour != 0) return "schedule.start_utc must be a whole UTC hour";
        if (Schedule.MaxWindowAgeHours <= 1) return "schedule.max_window_age_hours must be greater than 1";
        if (Schedule.MaxWindowsPerFamily < 1) return "schedule.max_windows_per_family must be at least 1";
        if (Families.FreshTables is { Count: 0 }) return "families.fresh_tables must not be an empty list";
        if (Restart.DelayMinutes < 0) return "restart.delay_minutes must not be negative";
        if (!new[] { "darling_process", "file", "command", "off" }.Contains(Restart.Detector))
            return "restart.detector must be darling_process, file, command or off";
        if (Restart.Detector == "file" && string.IsNullOrWhiteSpace(Restart.File)) return "restart.file is required for the file detector";
        if (Restart.Detector == "command" && Restart.Command is not { Count: > 0 }) return "restart.command is required for the command detector";
        // An empty prefix matches every entry under temp_dir, so the sweep would delete files that are not the exporter's.
        if (Retention.TempPrefixes is null || Retention.TempPrefixes.Any(string.IsNullOrWhiteSpace))
            return "retention.temp_prefixes must not contain an empty or whitespace entry (it would match every entry under temp_dir)";
        if (ProcedurePlanReuse is not null && !ProcedurePlanReuseModes.Names.Contains(ProcedurePlanReuse))
            return $"procedure_plan_reuse must be one of: {string.Join(", ", ProcedurePlanReuseModes.Names)}";
        if (QueryStoreSource is not null && !QueryStoreSourceModes.Names.Contains(QueryStoreSource))
            return $"query_store_source must be one of: {string.Join(", ", QueryStoreSourceModes.Names)}";
        if (PostRun is not null && (string.IsNullOrWhiteSpace(PostRun.Name) || PostRun.Command is not { Count: > 0 }))
            return "post_run needs a name and a command";
        return null;
    }

    /// <summary>
    /// Settings that are usable but worth a line in the run output. Today: an optional-registry table in
    /// `families.fresh.tables`, which the export routes through the optional path (column policy, schema gate, chunk
    /// guard, the optional manifest) exactly as if it were in `optional_tables`.
    /// </summary>
    public IReadOnlyList<string> Warnings()
    {
        var warnings = new List<string>();
        foreach (var table in Families.FreshTables ?? [])
        {
            if (!ExportOptions.AllTables.Contains(table) && OptionalTables.TryGet(table) is not null)
            {
                warnings.Add($"families.fresh.tables names the optional table {table}; it is exported through the optional path, with its column policy, schema gate and chunk guard, and listed in the optional manifest, as if it were in optional_tables");
            }
        }

        return warnings;
    }

    public string ResolvedChildExe => ChildExe ?? Environment.ProcessPath ?? "darling-export";
    public string StatePath => Path.Combine(StateDir, "export-schedule.json");
}

public sealed record FamilySettings
{
    /// <summary>Replaces the fresh family's table list (a store without the SQL Server tables uses ["collection_log"]). Null = the default list.</summary>
    public List<string>? FreshTables { get; init; }

    /// <summary>False means no query_store family and no query_store key in the state file, ever.</summary>
    public bool QueryStoreEnabled { get; init; } = true;

    public List<string> OptionalTables { get; init; } = [];

    /// <summary>One table per line, `#` starts a comment; read on every run and added to <see cref="OptionalTables"/>.</summary>
    public string? OptionalTablesFile { get; init; }

    /// <summary>1, 2 or 3. Null means the default 3 and is reported as source "default".</summary>
    public int? QueryStoreLagHours { get; init; }

    /// <summary>5, 10, 15, 20, 30 or 60. Below 60 adds the fresh_fast family.</summary>
    public int FastWindowMinutes { get; init; } = 60;

    /// <summary>2..15. Null means the default 15 and is reported as source "default".</summary>
    public int? LateRowMarginMinutes { get; init; }
}

public sealed record ScheduleSettings
{
    public static readonly int[] AllowedWindowMinutes = [5, 10, 15, 20, 30, 60];

    public int MaxWindowsPerFamily { get; init; } = 6;
    public int MaxWindowAgeHours { get; init; } = 20;
    public int CutoffMinute { get; init; } = 40;

    /// <summary>With no state file, both families start here instead of at the catch-up horizon.</summary>
    public DateTime? StartUtc { get; init; }
}

public sealed record MemoryGuardSettings
{
    public bool Enabled { get; init; } = true;
    public int MinFreeMb { get; init; } = 6144;
    public int StartMultiplier { get; init; } = 2;
}

public sealed record RestartSettings
{
    public int DelayMinutes { get; init; } = 30;
    public string Detector { get; init; } = "off";
    public string DarlingServiceName { get; init; } = "PerformanceMonitor Darling";
    public string DarlingRuntimePath { get; init; } = @"C:\PerformanceMonitorDarling\pg-runtime";
    public string? File { get; init; }
    public List<string>? Command { get; init; }
}

public sealed record RetentionSettings
{
    public int LogDays { get; init; } = 14;
    public int LeftoverHours { get; init; } = 24;
    public int OutHours { get; init; } = 48;
    public int TempHours { get; init; } = 24;

    /// <summary>Temp folder to sweep. Null is this process's temp folder.</summary>
    public string? TempDir { get; init; }

    /// <summary>Entries directly under the temp folder with one of these prefixes are swept (the exporter's scratch files).</summary>
    public List<string> TempPrefixes { get; init; } = ["darling-export-"];
}

public sealed record PostRunSettings
{
    public string Name { get; init; } = "";
    public List<string> Command { get; init; } = [];
    public int TimeoutSeconds { get; init; } = 600;
}
