namespace PerformanceMonitor.Headless.Models;

public sealed class MonitorOptions
{
    public string StoragePath { get; set; } = "data\\headless\\performance-monitor.duckdb";
    public string ArchiveDirectory { get; set; } = "data\\headless\\parquet";
    public int CollectionIntervalSeconds { get; set; } = 60;
    public int MaxConcurrentServers { get; set; } = 8;
    public int CommandTimeoutSeconds { get; set; } = 30;
    public int ArchiveIntervalMinutes { get; set; } = 60;
    public int HotDataDays { get; set; } = 7;
    public List<CollectorScheduleOptions> Collectors { get; set; } = [];
    public List<MonitoredServerOptions> Servers { get; set; } = [];

    public IReadOnlyList<CollectorScheduleOptions> GetEffectiveCollectors()
    {
        if (Collectors.Count > 0)
        {
            return Collectors;
        }

        return
        [
            new() { Name = "server_properties", FrequencySeconds = 3600 },
            new() { Name = "wait_stats", FrequencySeconds = 60 },
            new() { Name = "cpu_utilization", FrequencySeconds = 60 }
        ];
    }
}
