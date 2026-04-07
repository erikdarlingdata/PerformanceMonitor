namespace PerformanceMonitorDashboard.Models
{
    public class ProcedureStatsComparisonItem : ComparisonItemBase
    {
        public string SchemaName { get; set; } = "";
        public string ObjectName { get; set; } = "";
        public string FullName => string.IsNullOrEmpty(SchemaName) ? ObjectName : $"{SchemaName}.{ObjectName}";
    }
}
