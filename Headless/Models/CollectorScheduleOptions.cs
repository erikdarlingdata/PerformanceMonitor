namespace PerformanceMonitor.Headless.Models;

public sealed class CollectorScheduleOptions
{
    public string Name { get; set; } = "";
    public bool Enabled { get; set; } = true;
    public int FrequencySeconds { get; set; } = 60;
}
