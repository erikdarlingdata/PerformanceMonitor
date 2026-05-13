namespace PerformanceMonitor.Headless.Models;

public sealed record ServerHealthDto(
    string ServerId,
    string DisplayName,
    bool IsEnabled,
    DateTime? LastSeenTime,
    string LastStatus,
    string? LastError,
    string? ProductVersion,
    string? Edition,
    int? SqlMajorVersion,
    string HealthState,
    string HealthReason,
    int ActiveAlertCount);

public sealed record CollectionLogDto(
    DateTime CollectionTime,
    string ServerId,
    string ServerName,
    string CollectorName,
    string Status,
    int RowsCollected,
    int DurationMs,
    string? ErrorMessage);

public sealed record TopWaitDto(
    string WaitType,
    long WaitTimeDeltaMs,
    long SignalWaitTimeDeltaMs,
    long WaitingTasksDelta);

public sealed record CpuSampleDto(
    DateTime SampleTime,
    int SqlServerCpuUtilization,
    int OtherProcessCpuUtilization);

public sealed record EstateSummaryDto(
    int ServerCount,
    int GreenCount,
    int YellowCount,
    int RedCount,
    int ErrorCount,
    int DisabledCount,
    DateTime GeneratedAt,
    IReadOnlyList<ServerHealthDto> Servers);

public sealed record ServerPropertiesSnapshot(
    string MachineName,
    string? InstanceName,
    string ProductVersion,
    string ProductLevel,
    string Edition,
    int EngineEdition,
    int SqlMajorVersion,
    int CpuCount,
    long PhysicalMemoryMb,
    DateTime SqlServerStartTime);

public sealed record WaitStatSnapshot(
    string WaitType,
    long WaitingTasksCount,
    long WaitTimeMs,
    long SignalWaitTimeMs);

public sealed record CpuSample(
    DateTime SampleTime,
    int SqlServerCpuUtilization,
    int OtherProcessCpuUtilization);
