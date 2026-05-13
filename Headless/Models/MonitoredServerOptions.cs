namespace PerformanceMonitor.Headless.Models;

public sealed class MonitoredServerOptions
{
    public string Id { get; set; } = "";
    public string DisplayName { get; set; } = "";
    public string? ConnectionString { get; set; }
    public string? ConnectionStringEnvironmentVariable { get; set; }
    public bool Enabled { get; set; } = true;

    public string ServerNameForStorage => string.IsNullOrWhiteSpace(DisplayName) ? Id : DisplayName;

    public string ResolveConnectionString()
    {
        if (!string.IsNullOrWhiteSpace(ConnectionStringEnvironmentVariable))
        {
            var fromEnvironment = Environment.GetEnvironmentVariable(ConnectionStringEnvironmentVariable);
            if (!string.IsNullOrWhiteSpace(fromEnvironment))
            {
                return Environment.ExpandEnvironmentVariables(fromEnvironment);
            }
        }

        return Environment.ExpandEnvironmentVariables(ConnectionString ?? "");
    }
}
