using System.Text.Json;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// The exporter's JSON config (`config_version` 1). Plain immutable records; <see cref="ConfigLoader"/> builds and
/// validates them. `Destination` stays a raw <see cref="JsonElement"/>: the uploader owns its keys.
/// The family, schedule, memory-guard, restart-delay, retention and post-run sections are read by the `run` verb.
/// </summary>
public sealed record ExporterConfig(
    int ConfigVersion,
    StoreConfig Store,
    PathsConfig Paths,
    JsonElement? Destination,
    FamiliesConfig Families,
    ScheduleConfig Schedule,
    ExportSectionConfig Export,
    MemoryGuardConfig MemoryGuard,
    RestartDelayConfig RestartDelay,
    RetentionConfig Retention,
    PostRunConfig? PostRun);

/// <summary>`store`: the connection. Either <see cref="Password"/> with host/port/database/user, or <see cref="ConnectionStringEnv"/> alone.</summary>
public sealed record StoreConfig(
    string Name,
    string Host,
    int Port,
    string Database,
    string User,
    CredentialSpec? Password,
    string? ConnectionStringEnv,
    string ApplicationName,
    int StatementTimeoutSeconds)
{
    /// <summary>The credential spec the connection factory resolves.</summary>
    public CredentialSpec Credential => ConnectionStringEnv is { } env
        ? new CredentialSpec(CredentialSources.ConnectionStringEnv, Name: env)
        : Password!;
}

public sealed record PathsConfig(string Out, string State, string Logs);

public sealed record FamiliesConfig(FreshFamilyConfig Fresh, QueryStoreFamilyConfig QueryStore, FastFamilyConfig Fast);

/// <summary>`families.fresh`. <see cref="Tables"/> null means every core table except query_store_stats.</summary>
public sealed record FreshFamilyConfig(IReadOnlyList<string>? Tables, IReadOnlyList<string> OptionalTables, string? OptionalTablesFile);

public sealed record QueryStoreFamilyConfig(bool Enabled, int LagHours, string Source);

public sealed record FastFamilyConfig(int WindowMinutes, int LateRowMarginMinutes);

public sealed record ScheduleConfig(
    int MaxWindowsPerFamily, int MaxWindowAgeHours, int CutoffMinute, int HourlyStartMinute, string? StartUtc);

public sealed record ExportSectionConfig(string DuckdbMemoryLimit, int? DuckdbThreads, string ProcedurePlanReuse);

public sealed record MemoryGuardConfig(bool Enabled, int MinFreeMb, int StartMultiplier);

public sealed record RestartDelayConfig(
    int Minutes,
    string Detector,
    string DarlingServiceName,
    string DarlingRuntimePath,
    string? File,
    IReadOnlyList<string>? Command);

public sealed record RetentionConfig(int LogDays, int LeftoverHours, int OutHours, int TempHours);

public sealed record PostRunConfig(string Name, IReadOnlyList<string> Command, int TimeoutSeconds);

/// <summary>A config file is invalid. The message is one line and starts with the key it names. The CLI maps it to exit 2.</summary>
public sealed class ConfigException(string message) : Exception(message);
