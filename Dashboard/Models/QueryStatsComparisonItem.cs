namespace PerformanceMonitorDashboard.Models
{
    public class QueryStatsComparisonItem : ComparisonItemBase
    {
        public string QueryHash { get; set; } = "";
        public string? ObjectName { get; set; }
        public string? SchemaName { get; set; }
        public string? ObjectType { get; set; }
        public string QueryText { get; set; } = "";
    }
}
