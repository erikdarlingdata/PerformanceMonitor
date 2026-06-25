using System;

namespace PerformanceMonitorDashboard.Models
{
    public class PlanCacheStatsItem
    {
        public long CollectionId { get; set; }
        public DateTime CollectionTime { get; set; }
        public string CacheObjType { get; set; } = string.Empty;
        public string ObjType { get; set; } = string.Empty;

        // Plan counts
        public int TotalPlans { get; set; }
        public int TotalSizeMb { get; set; }
        public int SingleUsePlans { get; set; }
        public int SingleUseSizeMb { get; set; }
        public int MultiUsePlans { get; set; }
        public int MultiUseSizeMb { get; set; }

        // Averages
        public decimal AvgUseCount { get; set; }
        public int AvgSizeKb { get; set; }

        // Plan cache stability
        public DateTime? OldestPlanCreateTime { get; set; }

        // Computed helpers
        public decimal SingleUsePlanPercentage => TotalPlans > 0
            ? (decimal)SingleUsePlans * 100.0m / TotalPlans
            : 0;
        public decimal SingleUseSizePercentage => TotalSizeMb > 0
            ? (decimal)SingleUseSizeMb * 100.0m / TotalSizeMb
            : 0;

        // Analysis columns (from report.plan_cache_bloat view logic)
        public string BloatLevel { get; set; } = string.Empty;
        public string Recommendation { get; set; } = string.Empty;
    }
}
